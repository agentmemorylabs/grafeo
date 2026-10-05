//! Commit and rollback cost what the transaction changed, not what the store
//! holds (#410), on a plain store and on a generation root.
//!
//! Upstream's `transaction_changes.rs` pins this with wall-clock timings.
//! These tests count work instead: `LpgStore::transaction_versions_walked`
//! is the number of node and edge version entries that commit, rollback and
//! the statistics refresh after a rollback have visited. The same
//! transaction, run against a small and a large store, must walk the same
//! number, and must still undo everything.
//!
//! ```bash
//! cargo test -p grafeo-engine --test transaction_changes_cost \
//!   --features generation,generation-streaming,compact-store,lpg,mmap,wal,cypher,text-index
//! ```

use std::collections::BTreeMap;

use grafeo_common::types::Value;
use grafeo_core::graph::lpg::LpgStore;
use grafeo_engine::GrafeoDB;

fn insert_bulk(db: &GrafeoDB, size: i64) {
    for first in (1..=size).step_by(10_000) {
        let last = (first + 9_999).min(size);
        db.execute(&format!(
            "UNWIND range({first}, {last}) AS i INSERT (:Bulk {{i: i}})"
        ))
        .unwrap();
    }
}

fn int(db: &GrafeoDB, query: &str) -> i64 {
    match db.execute(query).unwrap().rows()[0][0] {
        Value::Int64(n) => n,
        ref other => panic!("{query}: expected an integer, got {other:?}"),
    }
}

/// What the plain-store transaction touches, as readers see it.
fn plain_state(db: &GrafeoDB) -> BTreeMap<&'static str, String> {
    let store = db.store();
    store.ensure_statistics_fresh();
    let stats = store.statistics();
    let mut state = BTreeMap::new();
    state.insert("nodes", db.node_count().to_string());
    state.insert("edges", db.edge_count().to_string());
    state.insert(
        "stats",
        format!(
            "{} {} {:?} {:?}",
            stats.total_nodes,
            stats.total_edges,
            stats.get_label("Bulk").map(|l| l.node_count),
            stats.get_label("Extra").map(|l| l.node_count),
        ),
    );
    state.insert(
        "bulk_1",
        format!(
            "{:?}",
            db.execute("MATCH (b:Bulk {i: 1}) RETURN b.flag, labels(b)")
                .unwrap()
                .rows()
        ),
    );
    state.insert(
        "bulk_2",
        int(db, "MATCH (b:Bulk {i: 2}) RETURN count(b)").to_string(),
    );
    state.insert(
        "new_nodes",
        int(db, "MATCH (t:New) RETURN count(t)").to_string(),
    );
    state.insert(
        "tmp_lookup",
        db.find_nodes_by_property("name", &Value::from("tmp"))
            .len()
            .to_string(),
    );
    #[cfg(feature = "text-index")]
    state.insert(
        "tmp_text",
        db.text_search("New", "name", "tmp", 10)
            .unwrap()
            .len()
            .to_string(),
    );
    state
}

/// Runs one transaction that creates, changes and deletes, then commits or
/// rolls it back and reads once (the read pays for any statistics refresh
/// the rollback left behind). Returns the versions walked meanwhile.
fn plain_transaction(db: &GrafeoDB, store: &LpgStore, commit: bool) -> u64 {
    let walked = store.transaction_versions_walked();
    let mut session = db.session();
    session.begin_transaction().unwrap();
    session
        .execute("INSERT (:New {name: 'tmp'})-[:REL]->(:New {name: 'other'})")
        .unwrap();
    session
        .execute("MATCH (b:Bulk {i: 1}) SET b.flag = true, b:Extra")
        .unwrap();
    session
        .execute("MATCH (b:Bulk {i: 2}) DETACH DELETE b")
        .unwrap();
    if commit {
        session.commit().unwrap();
    } else {
        session.rollback().unwrap();
    }
    session.execute("MATCH (b:Bulk) RETURN count(b)").unwrap();
    store.transaction_versions_walked() - walked
}

fn plain_db(size: i64) -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    insert_bulk(&db, size);
    db.create_property_index("name");
    #[cfg(feature = "text-index")]
    db.create_text_index("New", "name").unwrap();
    db
}

#[test]
fn plain_store_rollback_and_commit_walk_only_the_changes() {
    let mut walked = Vec::new();
    for size in [200, 20_000] {
        let db = plain_db(size);
        let store = std::sync::Arc::clone(db.store());
        let before = plain_state(&db);

        let rollback = plain_transaction(&db, &store, false);
        assert_eq!(plain_state(&db), before, "rollback at {size} nodes");

        let commit = plain_transaction(&db, &store, true);
        let after = plain_state(&db);
        assert_eq!(after["nodes"], (size + 1).to_string(), "commit at {size}");
        assert_eq!(after["bulk_2"], "0", "commit at {size}");
        assert_eq!(after["tmp_lookup"], "1", "commit at {size}");

        walked.push((size, rollback, commit));
    }
    let [(_, small_rollback, small_commit), (_, large_rollback, large_commit)] = walked[..] else {
        unreachable!()
    };
    assert_eq!(
        (large_rollback, large_commit),
        (small_rollback, small_commit),
        "versions walked by (rollback, commit) must not grow with the store: {walked:?}"
    );
    assert!(
        small_rollback <= 16 && small_commit <= 16,
        "a 3-statement transaction walked {walked:?} versions"
    );
}

#[cfg(all(
    feature = "generation",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]
mod generation_root {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::Arc;

    use grafeo_common::types::Value;
    use grafeo_core::graph::GraphStore;
    use grafeo_engine::{GrafeoDB, generation_build_request};
    use tempfile::tempdir;

    const NAMES: [&str; 5] = ["alix", "gus", "vincent", "jules", "mia"];

    /// `alix -KNOWS-> gus -KNOWS-> vincent -KNOWS-> jules -LIKES-> mia`
    fn publish_base(root: &Path) {
        std::fs::create_dir_all(root).unwrap();
        let source = GrafeoDB::new_in_memory();
        let ids: Vec<_> = NAMES
            .iter()
            .enumerate()
            .map(|(i, name)| {
                source
                    .create_node_with_props(
                        &["Person"],
                        [
                            ("name", Value::from(*name)),
                            ("age", Value::from(30 + i as i64)),
                        ],
                    )
                    .unwrap()
            })
            .collect();
        for (s, d, t) in [(0, 1, "KNOWS"), (1, 2, "KNOWS"), (2, 3, "KNOWS"), (3, 4, "LIKES")] {
            source.create_edge(ids[s], ids[d], t);
        }
        source
            .build_and_publish_generation(generation_build_request(root, "g-base"))
            .unwrap();
    }

    /// Base tombstones (a DETACH DELETE and an edge delete), a copy-up with
    /// a SET and a label, a new node with an edge onto a base node, and a
    /// SET plus label on an overlay row.
    const MUTATIONS: [&str; 5] = [
        "MATCH (n:Person {name: 'alix'}) DETACH DELETE n",
        "MATCH (n:Person {name: 'gus'}) SET n.age = 99, n.name = 'gus-renamed', n:Extra",
        "MATCH (v:Person {name: 'vincent'}) CREATE (:Temp {name: 'tmp'})-[:NEW]->(v)",
        "MATCH (:Person {name: 'jules'})-[r:LIKES]->() DELETE r",
        "MATCH (b:Bulk {i: 1}) SET b.flag = true, b:Extra",
    ];

    fn state(db: &GrafeoDB) -> BTreeMap<String, String> {
        let layered = db.layered_store().unwrap();
        let store: &dyn GraphStore = layered.as_ref();
        let mut state = BTreeMap::new();
        state.insert("nodes".into(), store.node_count().to_string());
        state.insert("edges".into(), store.edge_count().to_string());
        for label in ["Person", "Temp", "Extra"] {
            let mut ids: Vec<u64> = store
                .nodes_by_label(label)
                .into_iter()
                .map(|id| id.as_u64())
                .collect();
            ids.sort_unstable();
            state.insert(format!("label {label}"), format!("{ids:?}"));
        }
        for name in NAMES.iter().chain(&["gus-renamed", "tmp"]) {
            let ids = store.find_nodes_by_property("name", &Value::from(*name));
            state.insert(format!("lookup {name}"), format!("{ids:?}"));
        }
        let rows = db
            .execute("MATCH (a)-[r]->(b) RETURN a.name, type(r), b.name ORDER BY a.name, b.name")
            .unwrap();
        state.insert("edges seen".into(), format!("{:?}", rows.rows()));
        let rows = db
            .execute("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.name")
            .unwrap();
        state.insert("people".into(), format!("{:?}", rows.rows()));
        let rows = db
            .execute("MATCH (b:Bulk {i: 1}) RETURN b.flag")
            .unwrap();
        state.insert("bulk 1".into(), format!("{:?}", rows.rows()));
        state
    }

    #[test]
    fn generation_root_rollback_walks_only_the_changes() {
        let mut walked = Vec::new();
        for overlay_rows in [100, 5_000] {
            let dir = tempdir().unwrap();
            let root = dir.path().join("root");
            publish_base(&root);
            let db = GrafeoDB::open_generation_root(&root, false).unwrap();
            db.create_property_index("name");
            // Committed overlay rows: the old rollback scanned all of them.
            super::insert_bulk(&db, overlay_rows);
            let overlay = Arc::clone(db.layered_store().unwrap()).overlay_store();
            let before = state(&db);

            let start = overlay.transaction_versions_walked();
            let mut session = db.session();
            session.begin_transaction().unwrap();
            for query in MUTATIONS {
                session.execute(query).expect(query);
            }
            session.rollback().unwrap();
            drop(session);
            db.execute("MATCH (p:Person) RETURN count(p)").unwrap();
            let rollback = overlay.transaction_versions_walked() - start;

            assert_eq!(state(&db), before, "rollback with {overlay_rows} overlay rows");
            walked.push(rollback);

            // The same changes still commit.
            let mut session = db.session();
            session.begin_transaction().unwrap();
            for query in MUTATIONS {
                session.execute(query).expect(query);
            }
            session.commit().unwrap();
            let after = state(&db);
            assert_eq!(after["lookup alix"], "[]");
            assert_eq!(after["lookup gus-renamed"].matches("NodeId").count(), 1);
            assert_eq!(after["lookup tmp"].matches("NodeId").count(), 1);
            db.close().unwrap();
        }
        assert_eq!(
            walked[0], walked[1],
            "versions walked by the rollback must not grow with the overlay: {walked:?}"
        );
        assert!(walked[0] <= 16, "the rollback walked {walked:?} versions");
    }
}
