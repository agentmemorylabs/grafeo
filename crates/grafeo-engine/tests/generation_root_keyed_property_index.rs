//! Regression: AMH production incident 2026-08-22 (Lane A WritePlan
//! `code_intel_source_hash_mismatch: stale base ... stored None` on the
//! m26pipeline generation-root sidecar).
//!
//! Reproduces the EXACT production sequence on a real generation root:
//! publish a base with keyed nodes → open RW through the production
//! constructor → `create_property_index` (as AMH `ensure_property_indexes`
//! does) → the same keyed map-pattern Cypher the WritePlan preflight uses
//! MUST find base rows. At pin 9ebcf388 the preflight read `stored: None`
//! while the bytes sat in the base generation.
//!
//! Discriminating chain asserted step by step so a RED pinpoints the exact
//! broken link (index postings / planner routing / property read) instead of
//! just the final symptom.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap"
))]

use grafeo_common::types::Value;
use grafeo_engine::{generation_build_request, GrafeoDB};
use tempfile::tempdir;

/// Build a generation root whose base holds `CodeDocument`-shaped nodes keyed
/// by (repo_id, path), open it writable exactly like the AMH owner slot, then
/// verify the AMH index-ensure + keyed-lookup sequence.
#[test]
fn generation_root_keyed_lookup_after_create_property_index_finds_base_rows() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("keyed-lookup.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    // --- Publish a base that mimics the AMH code-index shape ---
    let source = GrafeoDB::new_in_memory();
    for path in ["a.py", "b.py", "ports.md"] {
        source
            .create_node_with_props(
                &["CodeDocument"],
                [
                    ("repo_id", Value::from("repo-x")),
                    ("path", Value::from(path)),
                    ("source_hash", Value::from(format!("hash-{path}"))),
                ],
            )
            .expect("create base doc");
    }
    // A second repo's doc must not leak into repo-x lookups.
    source
        .create_node_with_props(
            &["CodeDocument"],
            [
                ("repo_id", Value::from("repo-y")),
                ("path", Value::from("a.py")),
                ("source_hash", Value::from("hash-other-repo")),
            ],
        )
        .expect("create other-repo doc");
    source
        .build_and_publish_generation(generation_build_request(&root, "keyed-g1"))
        .expect("publish generation");
    drop(source);

    // --- Open writable exactly like the AMH owner slot ---
    let db = GrafeoDB::open_generation_root(&root, false).expect("open generation root writable");

    // Step 0 (sanity): before any index exists, the keyed map pattern works —
    // this is the shape that DID work in production before the first ensure.
    let keyed = |db: &GrafeoDB, repo: &str, path: &str| -> Option<String> {
        let mut params = std::collections::HashMap::new();
        params.insert("repo_id".to_string(), Value::from(repo));
        params.insert("path".to_string(), Value::from(path));
        let result = db
            .session()
            .execute_language(
                "MATCH (d:CodeDocument {repo_id: $repo_id, path: $path}) \
                 RETURN d.source_hash AS h LIMIT 1",
                "cypher",
                Some(params),
            )
            .expect("keyed lookup query");
        result
            .rows()
            .first()
            .and_then(|row| row.first())
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    assert_eq!(
        keyed(&db, "repo-x", "a.py").as_deref(),
        Some("hash-a.py"),
        "Step 0: pre-index keyed lookup must find base rows"
    );

    // Step 1: merged visibility — the store must serve base nodes at all.
    let store = db.graph_store();
    let base_ids = store.nodes_by_label("CodeDocument");
    assert_eq!(
        base_ids.len(),
        4,
        "Step 1: merged view serves all 4 base docs"
    );

    // Step 2: the AMH ensure sequence — create_property_index("repo_id").
    db.create_property_index("repo_id");
    assert!(
        db.has_property_index("repo_id"),
        "Step 2: index registered after create"
    );

    // Step 3 (THE production symptom): the same keyed lookup must STILL find
    // base rows. If postings were seeded overlay-only, G-E1.RO's
    // exclusive-overlay rule makes this return None → stale-base mismatch.
    assert_eq!(
        keyed(&db, "repo-x", "ports.md").as_deref(),
        Some("hash-ports.md"),
        "Step 3: keyed lookup AFTER create_property_index must find base rows"
    );

    // Step 4: direct API path — find_nodes_by_property must serve base rows.
    let found = db.find_nodes_by_property("repo_id", &Value::from("repo-x"));
    assert_eq!(
        found.len(),
        3,
        "Step 4: find_nodes_by_property serves all 3 repo-x docs from base"
    );

    // Step 5: no cross-repo leak — repo-y's a.py is a distinct row.
    assert_eq!(
        keyed(&db, "repo-y", "a.py").as_deref(),
        Some("hash-other-repo"),
        "Step 5: cross-repo isolation holds"
    );

    // Step 6: writes after index creation stay visible to the keyed path
    // (overlay rows auto-maintain the index).
    let mut params = std::collections::HashMap::new();
    params.insert("repo_id".to_string(), Value::from("repo-x"));
    params.insert("path".to_string(), Value::from("new.py"));
    params.insert("source_hash".to_string(), Value::from("hash-new"));
    use grafeo_engine::Role;
    db.session_with_role(Role::Admin)
        .execute_language(
            "CREATE (:CodeDocument {repo_id: $repo_id, path: $path, source_hash: $source_hash})",
            "cypher",
            Some(params),
        )
        .expect("create overlay doc");
    assert_eq!(
        keyed(&db, "repo-x", "new.py").as_deref(),
        Some("hash-new"),
        "Step 6: overlay write after index creation is keyed-visible"
    );

    // Step 7: durability across reopen. RW drop checkpoints the database;
    // a fresh open must still serve every base row through the keyed path.
    // (At pin 9ebcf388 index REGISTRATION is not restored on generation-root
    // reopen — WAL CreateIndex replay is a no-op and no PropertyIndexSection
    // restore exists — so post-reopen this exercises the merged-scan serving
    // path. When PropertyIndexSection emit/restore lands, this same step will
    // drive MappedPropertyIndex postings; the data contract asserted here is
    // identical.)
    drop(db);
    let reopened = GrafeoDB::open_generation_root(&root, false)
        .expect("reopen generation root writable");
    assert_eq!(
        keyed(&reopened, "repo-x", "ports.md").as_deref(),
        Some("hash-ports.md"),
        "Step 7: keyed lookup finds base rows after checkpoint + reopen"
    );
    assert_eq!(
        keyed(&reopened, "repo-x", "new.py").as_deref(),
        Some("hash-new"),
        "Step 7: overlay row also survives reopen"
    );
}
