//! D10 slice 1: on a writable generation root, a write to a base node keeps
//! an overlay *diff row* (labels plus the written properties) instead of
//! copying the whole node, embedding included, into the overlay.
//!
//! These tests pin the merged view across every path that used to rely on
//! the copy: Cypher and direct reads, property removal (tombstones), rollback,
//! property lookups (with and without an index), relation creates, reopen
//! (WAL replay), an epoch handoff (the capture materializes whole rows) and a
//! ForceDisk reopen (a diff row's new vector is not spilled behind a stale
//! base vector). The last test measures what one small update now costs.
//! Slice 2 adds the same for a base edge's properties.
//!
//! See `docs/architecture/storage/layered-overlay-diff.md`.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::path::{Path, PathBuf};

use grafeo_common::types::{NodeId, PropertyKey, Value};
use grafeo_core::graph::traits::GraphStore;
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::TempDir;

const DIMS: usize = 16;

fn vector(seed: usize, dims: usize) -> Value {
    Value::Vector(
        (0..dims)
            .map(|d| (seed * 31 + d) as f32 * 0.001)
            .collect::<Vec<f32>>()
            .into(),
    )
}

/// Publishes `count` `MemoryEntity` nodes `e0..` (AMH's shape: identity,
/// observations, provenance, an embedding of `dims`) plus one relation
/// `e0 -> e1`.
fn publish(root: &Path, count: usize, dims: usize) {
    std::fs::create_dir_all(root).expect("create root");
    let source = GrafeoDB::new_in_memory();
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let id = source
            .create_node_with_props(
                &["MemoryEntity"],
                [
                    ("name", Value::from(format!("e{i}"))),
                    ("account_id", Value::from("acct")),
                    ("observations_json", Value::from("[\"o0\"]")),
                    ("embedding_provider", Value::from("voyage")),
                    ("updated_at_ms", Value::Int64(1_000 + i as i64)),
                    ("embedding", vector(i, dims)),
                ],
            )
            .expect("create entity");
        ids.push(id);
    }
    if count > 1 {
        source.create_edge_with_props(
            ids[0],
            ids[1],
            "MemoryEntityRelation",
            [
                ("rel_type", Value::from("base")),
                ("since", Value::Int64(2020)),
                ("weight", Value::Float64(0.5)),
            ],
        );
    }
    source
        .build_and_publish_generation(generation_build_request(root, "g1"))
        .expect("publish base generation");
}

fn fresh_root(count: usize, dims: usize) -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("diff.grafeo.d");
    publish(&root, count, dims);
    (dir, root)
}

fn open(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, false).expect("open generation root writable")
}

fn handoff(db: &GrafeoDB, root: &Path, generation: &str) {
    let report = db
        .run_epoch_handoff(generation_build_request(root, generation))
        .expect("epoch handoff");
    db.publish_and_install_handoff(report)
        .expect("publish and install handoff");
}

fn id_of(db: &GrafeoDB, name: &str) -> NodeId {
    let r = db
        .execute_cypher(&format!(
            "MATCH (n:MemoryEntity {{name: '{name}'}}) RETURN id(n)"
        ))
        .expect("lookup");
    assert_eq!(r.row_count(), 1, "exactly one {name}");
    match &r.rows()[0][0] {
        Value::Int64(v) => NodeId::new(*v as u64),
        other => panic!("id: {other:?}"),
    }
}

fn count(db: &GrafeoDB, query: &str) -> i64 {
    match &db.execute_cypher(query).expect(query).rows()[0][0] {
        Value::Int64(v) => *v,
        other => panic!("{query}: {other:?}"),
    }
}

fn prop(db: &GrafeoDB, id: NodeId, key: &str) -> Option<Value> {
    db.graph_store()
        .get_node_property(id, &PropertyKey::new(key))
}

/// Every base property of `e{i}` as published, except those in `skip`.
fn assert_base_props(db: &GrafeoDB, id: NodeId, i: usize, skip: &[&str], stage: &str) {
    let want = [
        ("name", Value::from(format!("e{i}"))),
        ("account_id", Value::from("acct")),
        ("observations_json", Value::from("[\"o0\"]")),
        ("embedding_provider", Value::from("voyage")),
        ("updated_at_ms", Value::Int64(1_000 + i as i64)),
        ("embedding", vector(i, DIMS)),
    ];
    let node = db.get_node(id).unwrap_or_else(|| panic!("[{stage}] node"));
    for (key, value) in want.iter().filter(|(k, _)| !skip.contains(k)) {
        assert_eq!(
            prop(db, id, key).as_ref(),
            Some(value),
            "[{stage}] get_node_property {key}"
        );
        assert_eq!(
            node.properties.get(&PropertyKey::new(*key)),
            Some(value),
            "[{stage}] get_node {key}"
        );
    }
}

/// The diff row of a base node holds only what was written.
fn assert_not_copied(db: &GrafeoDB, id: NodeId, key: &str, stage: &str) {
    let overlay = db.layered_store().expect("layered").overlay_store();
    assert!(
        overlay
            .get_node_property(id, &PropertyKey::new(key))
            .is_none(),
        "[{stage}] base property {key} was copied into the overlay"
    );
}

/// Merged reads after a SET; the overlay holds only the SET property, and
/// the merged view survives reopen and an epoch handoff.
#[test]
fn set_keeps_unchanged_base_properties() {
    let (_dir, root) = fresh_root(3, DIMS);
    {
        let db = open(&root);
        let e0 = id_of(&db, "e0");
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e0'}) SET m.observations_json = 'new'")
            .expect("SET");
        assert_eq!(prop(&db, e0, "observations_json"), Some(Value::from("new")));
        assert_base_props(&db, e0, 0, &["observations_json"], "after SET");
        assert_not_copied(&db, e0, "embedding", "after SET");
        assert_eq!(
            count(
                &db,
                "MATCH (m:MemoryEntity) WHERE m.observations_json = 'new' AND m.name = 'e0' RETURN count(m)"
            ),
            1
        );
        db.close().expect("close");
    }
    let db = open(&root);
    let e0 = id_of(&db, "e0");
    assert_eq!(
        prop(&db, e0, "observations_json"),
        Some(Value::from("new")),
        "[reopen]"
    );
    assert_base_props(&db, e0, 0, &["observations_json"], "reopen");
    assert_not_copied(&db, e0, "embedding", "reopen");
    handoff(&db, &root, "g2");
    assert_eq!(
        prop(&db, e0, "observations_json"),
        Some(Value::from("new")),
        "[handoff]"
    );
    assert_base_props(&db, e0, 0, &["observations_json"], "handoff");
    db.close().expect("close");
    // The root lock is released when the handle drops, not at close.
    drop(db);
    let db = open(&root);
    let e0 = id_of(&db, "e0");
    assert_eq!(
        prop(&db, e0, "observations_json"),
        Some(Value::from("new")),
        "[reopen 2]"
    );
    assert_base_props(&db, e0, 0, &["observations_json"], "reopen 2");
}

/// AMH's provenance `REMOVE` on a base entity: the property is gone from
/// every view, nothing else is, and the removal survives reopen and an
/// epoch handoff.
#[test]
fn remove_of_a_base_property_is_a_tombstone() {
    let (_dir, root) = fresh_root(3, DIMS);
    let check = |db: &GrafeoDB, stage: &str| {
        let e1 = id_of(db, "e1");
        assert_eq!(
            prop(db, e1, "embedding_provider"),
            None,
            "[{stage}] removed"
        );
        let node = db.get_node(e1).expect("node");
        assert!(
            node.properties
                .get(&PropertyKey::new("embedding_provider"))
                .is_none(),
            "[{stage}] get_node still has the removed property"
        );
        assert_eq!(
            count(
                db,
                "MATCH (m:MemoryEntity {name: 'e1'}) WHERE m.embedding_provider IS NULL RETURN count(m)"
            ),
            1,
            "[{stage}] Cypher IS NULL"
        );
        assert_base_props(db, e1, 1, &["embedding_provider"], stage);
    };
    {
        let db = open(&root);
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e1'}) REMOVE m.embedding_provider")
            .expect("REMOVE");
        check(&db, "after REMOVE");
        assert_not_copied(&db, id_of(&db, "e1"), "embedding", "after REMOVE");
        db.close().expect("close");
    }
    let db = open(&root);
    check(&db, "reopen");
    handoff(&db, &root, "g2");
    check(&db, "handoff");
    db.close().expect("close");
    drop(db);
    check(&open(&root), "reopen 2");
}

/// A rolled-back SET and REMOVE on a base node leave its base values.
#[test]
fn rollback_restores_the_base_view() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    let e2 = id_of(&db, "e2");
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher(
            "MATCH (m:MemoryEntity {name: 'e2'}) SET m.observations_json = 'tx' \
             REMOVE m.embedding_provider",
        )
        .expect("SET + REMOVE");
    session.rollback().expect("rollback");
    assert_base_props(&db, e2, 2, &[], "rollback");
    drop(session);
    db.close().expect("close");
    drop(db);
    assert_base_props(&open(&root), e2, 2, &[], "reopen");
}

/// Property lookups merge base postings of diff rows (the base value still
/// holds) with overlay values, with and without a property index.
#[test]
fn property_lookups_see_the_merged_values() {
    for indexed in [false, true] {
        let (_dir, root) = fresh_root(3, DIMS);
        let db = open(&root);
        if indexed {
            db.create_property_index("name");
        }
        let stage = if indexed { "indexed" } else { "scan" };
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e0'}) SET m.observations_json = 'x'")
            .expect("SET observations");
        assert_eq!(
            count(&db, "MATCH (m:MemoryEntity {name: 'e0'}) RETURN count(m)"),
            1,
            "[{stage}] unchanged key of a diff row"
        );
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e0'}) SET m.name = 'e0b'")
            .expect("SET name");
        assert_eq!(
            count(&db, "MATCH (m:MemoryEntity {name: 'e0'}) RETURN count(m)"),
            0,
            "[{stage}] old"
        );
        assert_eq!(
            count(&db, "MATCH (m:MemoryEntity {name: 'e0b'}) RETURN count(m)"),
            1,
            "[{stage}] new"
        );
        assert_eq!(
            count(
                &db,
                "MATCH (m:MemoryEntity {account_id: 'acct', name: 'e0b'}) RETURN count(m)"
            ),
            1,
            "[{stage}] multi-key, one key changed"
        );
        assert_eq!(
            count(
                &db,
                "MATCH (m:MemoryEntity {account_id: 'acct'}) RETURN count(m)"
            ),
            3,
            "[{stage}] multi-key base value of every row"
        );
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e1'}) SET m.updated_at_ms = 5000")
            .expect("SET updated_at");
        assert_eq!(
            count(
                &db,
                "MATCH (m:MemoryEntity) WHERE m.updated_at_ms >= 1001 AND m.updated_at_ms < 2000 RETURN count(m)"
            ),
            1,
            "[{stage}] range: only e2 (e1 moved out)"
        );
        assert_eq!(
            count(
                &db,
                "MATCH (m:MemoryEntity) WHERE m.updated_at_ms >= 5000 RETURN count(m)"
            ),
            1,
            "[{stage}] range: e1 moved in"
        );
    }
}

/// A relation between two base entities leaves both whole (it used to copy
/// both up, embeddings included).
#[test]
fn relation_create_between_base_entities_keeps_both() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    db.execute_cypher(
        "MATCH (a:MemoryEntity {name: 'e1'}), (b:MemoryEntity {name: 'e2'}) \
         CREATE (a)-[:MemoryEntityRelation {rel_type: 'x'}]->(b)",
    )
    .expect("create relation");
    let (e1, e2) = (id_of(&db, "e1"), id_of(&db, "e2"));
    assert_base_props(&db, e1, 1, &[], "src");
    assert_base_props(&db, e2, 2, &[], "dst");
    assert_not_copied(&db, e1, "embedding", "src");
    assert_not_copied(&db, e2, "embedding", "dst");
    // Slice 3: an endpoint whose only change is a new edge gets no overlay
    // row at all.
    let overlay = db.layered_store().expect("layered").overlay_store();
    assert!(overlay.get_node(e1).is_none(), "src has an overlay row");
    assert!(overlay.get_node(e2).is_none(), "dst has an overlay row");
    assert_eq!(
        count(
            &db,
            "MATCH (:MemoryEntity {name: 'e1'})-[r:MemoryEntityRelation]->(:MemoryEntity {name: 'e2'}) RETURN count(r)"
        ),
        1
    );
    handoff(&db, &root, "g2");
    assert_base_props(&db, e1, 1, &[], "handoff src");
    assert_base_props(&db, e2, 2, &[], "handoff dst");
}

/// The DB-level label and property-removal APIs edit a base node through
/// its diff row.
#[test]
fn database_label_and_removal_apis_on_base_nodes() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    let e0 = id_of(&db, "e0");
    assert!(db.add_node_label(e0, "Pinned"), "add_node_label");
    assert!(
        db.remove_node_property(e0, "embedding_provider"),
        "remove_node_property"
    );
    assert_eq!(prop(&db, e0, "embedding_provider"), None);
    let mut labels = db.get_node_labels(e0).expect("labels");
    labels.sort();
    assert_eq!(
        labels,
        vec!["MemoryEntity".to_string(), "Pinned".to_string()]
    );
    assert_base_props(&db, e0, 0, &["embedding_provider"], "after label + remove");
    assert!(db.remove_node_label(e0, "Pinned"), "remove_node_label");
    assert_eq!(
        db.get_node_labels(e0).expect("labels"),
        vec!["MemoryEntity".to_string()]
    );
}

/// A diff row's new vector survives a ForceDisk reopen. The open spills
/// overlay vectors; a diff row's must stay in the overlay, or the merged
/// read would serve the stale base vector.
#[cfg(all(feature = "vector-index", not(feature = "temporal")))]
#[test]
fn force_disk_reopen_keeps_a_diff_row_vector() {
    use grafeo_common::storage::{SectionType, TierOverride};
    use grafeo_engine::Config;

    let (dir, root) = fresh_root(3, DIMS);
    let spill = dir.path().join("spill");
    let force_disk = || {
        GrafeoDB::open_generation_root_with_config(
            Config::persistent(&root)
                .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk)
                .with_spill_path(&spill),
        )
        .expect("ForceDisk open")
    };
    let new_vector = vector(99, DIMS);
    {
        let db = force_disk();
        db.create_vector_index(
            "MemoryEntity",
            "embedding",
            Some(DIMS),
            Some("cosine"),
            None,
            None,
            None,
        )
        .expect("vector index");
        let e0 = id_of(&db, "e0");
        db.session()
            .set_node_property(e0, "embedding", new_vector.clone())
            .expect("new vector");
        db.close().expect("close");
    }
    let db = force_disk();
    let e0 = id_of(&db, "e0");
    assert_eq!(
        prop(&db, e0, "embedding"),
        Some(new_vector),
        "diff vector after ForceDisk reopen"
    );
    assert_base_props(&db, e0, 0, &["embedding"], "ForceDisk reopen");
    assert_base_props(&db, id_of(&db, "e1"), 1, &[], "untouched base node");
}

/// What one small update of a base entity costs. Before D10 every touched
/// entity copied its whole row (here a 2048-dim, 8 KiB embedding) into the
/// overlay; now the overlay holds the written property and a labels-only
/// row. WAL bytes were already per-property (the copy was never logged);
/// this pins that too.
#[test]
fn small_update_of_a_base_entity_does_not_copy_its_embedding() {
    const N: usize = 400;
    const WIDE: usize = 2048;
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("cost.grafeo.d");
    publish(&root, N, WIDE);
    let db = open(&root);
    let layered = db.layered_store().expect("layered").clone();
    let wal_bytes = || dir_bytes(&root.join("wal"));
    let (overlay_before, wal_before, rss_before) =
        (layered.overlay_memory_bytes(), wal_bytes(), rss_anon_kib());

    for i in 0..N {
        db.execute_cypher(&format!(
            "MATCH (m:MemoryEntity {{name: 'e{i}'}}) SET m.observations_json = '[\"o0\",\"o1\"]'"
        ))
        .expect("append observation");
    }

    let overlay_per = (layered.overlay_memory_bytes() - overlay_before) / N;
    let wal_per = (wal_bytes() - wal_before) / N as u64;
    let rss_per_kib = rss_anon_kib().saturating_sub(rss_before) as f64 / N as f64;
    eprintln!(
        "D10 cost per small update of a base entity with a {WIDE}-dim embedding \
         ({} B vector): overlay {overlay_per} B, WAL {wal_per} B, RssAnon {rss_per_kib:.2} KiB",
        WIDE * 4
    );
    assert!(
        overlay_per < 1024,
        "overlay grows {overlay_per} B per small update; the {} B embedding is being copied",
        WIDE * 4
    );
    assert!(wal_per < 1024, "WAL grows {wal_per} B per small update");
    assert_not_copied(&db, id_of(&db, "e7"), "embedding", "cost");
    assert_eq!(
        prop(&db, id_of(&db, "e7"), "embedding"),
        Some(vector(7, WIDE))
    );
}

fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok()?.metadata().ok())
                .filter(|m| m.is_file())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

/// `RssAnon` of this process in KiB (Linux), 0 elsewhere. Reported, not
/// asserted: the allocator makes it noisy at this size.
fn rss_anon_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("RssAnon:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

// ── Slice 2: edge property diffs ──────────────────────────────────────

const BASE_REL: &str = "MATCH (:MemoryEntity {name: 'e0'})-[r:MemoryEntityRelation {rel_type: 'base'}]->(:MemoryEntity {name: 'e1'})";

fn base_rel_id(db: &GrafeoDB) -> grafeo_common::types::EdgeId {
    let r = db
        .execute_cypher(&format!("{BASE_REL} RETURN id(r)"))
        .expect("base relation id");
    assert_eq!(r.row_count(), 1, "one base relation");
    match &r.rows()[0][0] {
        Value::Int64(v) => grafeo_common::types::EdgeId::new(*v as u64),
        other => panic!("id: {other:?}"),
    }
}

fn edge_prop(db: &GrafeoDB, key: &str) -> Option<Value> {
    db.graph_store()
        .get_edge_property(base_rel_id(db), &PropertyKey::new(key))
}

/// The base relation's properties, as published, except those in `skip`.
fn assert_base_rel(db: &GrafeoDB, skip: &[&str], stage: &str) {
    let want = [
        ("rel_type", Value::from("base")),
        ("since", Value::Int64(2020)),
        ("weight", Value::Float64(0.5)),
    ];
    let edge = db
        .graph_store()
        .get_edge(base_rel_id(db))
        .unwrap_or_else(|| panic!("[{stage}] edge"));
    for (key, value) in want.iter().filter(|(k, _)| !skip.contains(k)) {
        assert_eq!(
            edge_prop(db, key).as_ref(),
            Some(value),
            "[{stage}] get_edge_property {key}"
        );
        assert_eq!(
            edge.properties.get(&PropertyKey::new(*key)),
            Some(value),
            "[{stage}] get_edge {key}"
        );
    }
}

/// SET and REMOVE on a base relation keep its other properties, never copy
/// them into the overlay, and survive reopen and an epoch handoff.
#[test]
fn edge_set_and_remove_keep_the_other_base_properties() {
    let (_dir, root) = fresh_root(3, DIMS);
    let check = |db: &GrafeoDB, stage: &str| {
        assert_eq!(
            edge_prop(db, "weight"),
            Some(Value::Float64(0.9)),
            "[{stage}] weight"
        );
        assert_eq!(edge_prop(db, "since"), None, "[{stage}] since removed");
        assert_eq!(
            count(
                db,
                &format!("{BASE_REL} WHERE r.since IS NULL AND r.weight = 0.9 RETURN count(r)")
            ),
            1,
            "[{stage}] Cypher"
        );
        assert_base_rel(db, &["weight", "since"], stage);
    };
    {
        let db = open(&root);
        db.execute_cypher(&format!("{BASE_REL} SET r.weight = 0.9 REMOVE r.since"))
            .expect("SET + REMOVE");
        check(&db, "after write");
        let overlay = db.layered_store().expect("layered").overlay_store();
        assert!(
            overlay
                .get_edge_property(base_rel_id(&db), &PropertyKey::new("rel_type"))
                .is_none(),
            "base edge property rel_type was copied into the overlay"
        );
        db.close().expect("close");
    }
    let db = open(&root);
    check(&db, "reopen");
    handoff(&db, &root, "g2");
    check(&db, "handoff");
    db.close().expect("close");
    drop(db);
    check(&open(&root), "reopen 2");
}

/// A rolled-back write on a base relation leaves its base properties.
#[test]
fn edge_rollback_restores_the_base_view() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher(&format!("{BASE_REL} SET r.weight = 0.1 REMOVE r.since"))
        .expect("SET + REMOVE");
    session.rollback().expect("rollback");
    assert_base_rel(&db, &[], "rollback");
}

/// The DB-level edge property removal goes through the diff row too.
#[test]
fn database_edge_property_removal_on_a_base_relation() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    assert!(
        db.remove_edge_property(base_rel_id(&db), "weight"),
        "remove_edge_property"
    );
    assert_eq!(edge_prop(&db, "weight"), None);
    assert_base_rel(&db, &["weight"], "after removal");
}

/// What one relation between two base entities costs. Before D10 it copied
/// both endpoints whole (2 × the 8 KiB embedding); slice 1 left two
/// labels-only endpoint rows (~206 B each); slice 3 drops them, so the
/// overlay holds only the edge.
#[test]
fn relation_between_base_entities_does_not_copy_their_embeddings() {
    const N: usize = 400;
    const WIDE: usize = 2048;
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("rel-cost.grafeo.d");
    publish(&root, N, WIDE);
    let db = open(&root);
    let layered = db.layered_store().expect("layered").clone();
    let overlay_before = layered.overlay_memory_bytes();
    for i in 0..N / 2 {
        db.execute_cypher(&format!(
            "MATCH (a:MemoryEntity {{name: 'e{}'}}), (b:MemoryEntity {{name: 'e{}'}}) \
             CREATE (a)-[:MemoryEntityRelation {{rel_type: 'r'}}]->(b)",
            2 * i,
            2 * i + 1
        ))
        .expect("relate");
    }
    let per = (layered.overlay_memory_bytes() - overlay_before) / (N / 2);
    eprintln!(
        "D10 cost per relation between two base entities with a {WIDE}-dim embedding: overlay {per} B"
    );
    assert!(
        per < 2048,
        "overlay grows {per} B per relation; endpoints are being copied"
    );
}
