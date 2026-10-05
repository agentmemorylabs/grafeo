//! A compacted single-file `.grafeo` must survive repeated close + reopen.
//!
//! The first reopen loads the v5 CompactStore section with *mapped* id
//! lookup tables (`set_mapped_id_indexes`), which clears the heap id maps
//! while `preserves_ids()` stays true. The next close re-serializes the base:
//! both v5 writers (the eager `section_v5::serialize_v5_with_string_order`
//! when `generation-streaming` is off, the streaming
//! `generation::emit::segments` when it is on) emitted the id tables from the
//! heap maps only, i.e. empty. The second reopen then resolved no base id:
//! queries saw no base nodes or edges while `node_count()` still counted them.
//!
//! Run under both writers:
//!
//! ```bash
//! cargo test -p grafeo-engine --features compact-store --test compact_store_reopen_cycles
//! cargo test -p grafeo-engine --features compact-store,generation-streaming --test compact_store_reopen_cycles
//! ```

#![cfg(all(
    feature = "compact-store",
    feature = "lpg",
    feature = "gql",
    feature = "grafeo-file",
    feature = "wal"
))]

use std::path::Path;

use grafeo_common::types::{NodeId, Value};
use grafeo_core::graph::traits::GraphStore;
use grafeo_engine::{Config, GrafeoDB};
use tempfile::tempdir;

const CYCLES: usize = 3;

fn count(db: &GrafeoDB, query: &str) -> i64 {
    let result = db
        .execute(query)
        .unwrap_or_else(|e| panic!("`{query}` failed: {e}"));
    match &result.rows()[0][0] {
        Value::Int64(v) => *v,
        other => panic!("expected integer count from `{query}`, got {other:?}"),
    }
}

fn open(path: &Path) -> GrafeoDB {
    GrafeoDB::with_config(Config::persistent(path)).expect("open single-file db")
}

/// Builds `(a:P {name:'a', n:1})-[:K {w:10}]->(b:P {name:'b', n:2})-[:K {w:20}]->(c:Q {name:'c', n:3})`,
/// compacts it, optionally runs `after_compact`, and closes. Returns the ids
/// of a, b and c.
fn build(path: &Path, after_compact: Option<&str>) -> [NodeId; 3] {
    let mut db = open(path);
    let a = db
        .create_node_with_props(
            &["P"],
            [("name", Value::from("a")), ("n", Value::from(1i64))],
        )
        .expect("a");
    let b = db
        .create_node_with_props(
            &["P"],
            [("name", Value::from("b")), ("n", Value::from(2i64))],
        )
        .expect("b");
    let c = db
        .create_node_with_props(
            &["Q"],
            [("name", Value::from("c")), ("n", Value::from(3i64))],
        )
        .expect("c");
    db.create_edge_with_props(a, b, "K", [("w", Value::from(10i64))]);
    db.create_edge_with_props(b, c, "K", [("w", Value::from(20i64))]);
    db.compact().expect("compact");
    if let Some(query) = after_compact {
        db.execute(query)
            .unwrap_or_else(|e| panic!("`{query}` failed: {e}"));
    }
    db.close().expect("close");
    [a, b, c]
}

/// Checks the whole graph after a reopen: label scans, edge counts,
/// property-anchored traversals and point reads by id.
fn assert_graph(db: &GrafeoDB, ids: [NodeId; 3], c_n: i64, ctx: &str) {
    assert_eq!(
        count(db, "MATCH (n) RETURN count(n)"),
        3,
        "{ctx}: MATCH (n)"
    );
    assert_eq!(
        count(db, "MATCH (n:P) RETURN count(n)"),
        2,
        "{ctx}: MATCH (n:P)"
    );
    assert_eq!(
        count(db, "MATCH ()-[r]->() RETURN count(r)"),
        2,
        "{ctx}: edge count"
    );
    assert_eq!(
        count(
            db,
            "MATCH (:P {name: 'a'})-[r:K]->(:P {name: 'b'}) WHERE r.w = 10 RETURN count(r)"
        ),
        1,
        "{ctx}: a-[w=10]->b"
    );
    assert_eq!(
        count(
            db,
            &format!("MATCH (:P {{name: 'b'}})-[:K]->(c:Q) WHERE c.n = {c_n} RETURN count(c)")
        ),
        1,
        "{ctx}: b->c"
    );
    assert_eq!(
        count(
            db,
            &format!("MATCH (n:Q {{name: 'c'}}) WHERE n.n = {c_n} RETURN count(n)")
        ),
        1,
        "{ctx}: c.n = {c_n}"
    );

    let [a, b, c] = ids;
    for (id, name) in [(a, "a"), (b, "b"), (c, "c")] {
        let node = db
            .get_node(id)
            .unwrap_or_else(|| panic!("{ctx}: get_node({name})"));
        assert_eq!(
            node.properties.get(&"name".into()),
            Some(&Value::from(name)),
            "{ctx}: get_node({name}).name"
        );
    }
    // `GrafeoDB::node_count` reports the overlay LpgStore after a reopen, so
    // read the counts from the layered store that serves the queries.
    let layered = db.layered_store().expect("compacted base is loaded");
    assert_eq!(layered.node_count(), 3, "{ctx}: node_count");
    assert_eq!(layered.edge_count(), 2, "{ctx}: edge_count");
}

fn reopen_cycles(path: &Path, ids: [NodeId; 3], c_n: i64) {
    for cycle in 1..=CYCLES {
        let db = open(path);
        assert_graph(&db, ids, c_n, &format!("reopen #{cycle}"));
        db.close().expect("close");
    }
}

#[test]
fn compacted_file_survives_three_reopens_without_writes() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("cycles.grafeo");
    let ids = build(&path, None);
    reopen_cycles(&path, ids, 3);
}

#[test]
fn compacted_file_survives_three_reopens_after_one_set() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("cycles-set.grafeo");
    // SET on `c`, which has no outgoing edges: a SET on an edge's source
    // hides its base edges on this base (fixed separately by PR #15).
    let ids = build(&path, Some("MATCH (n:Q {name: 'c'}) SET n.n = 100"));
    reopen_cycles(&path, ids, 100);
}

/// A write in the first reopened session (after the mapped load) is also
/// carried through the following reopens.
#[test]
fn compacted_file_survives_reopens_with_a_write_after_mapped_load() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("cycles-write.grafeo");
    let ids = build(&path, None);
    {
        let db = open(&path);
        assert_graph(&db, ids, 3, "first reopen");
        db.execute("MATCH (n:Q {name: 'c'}) SET n.n = 7")
            .expect("set after mapped load");
        db.close().expect("close");
    }
    reopen_cycles(&path, ids, 7);
}
