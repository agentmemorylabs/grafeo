//! Indexed property lookups with a label (`MATCH (n:L) WHERE n.k = v`,
//! `n.k IN [...]`, `MATCH (n:L {k: v})`) check the label on each index hit
//! (`Planner::visible_with_label`, upstream #459) instead of intersecting
//! with every node of the label.
//!
//! Correctness is pinned against a label-scan oracle and explicit expected
//! results on an in-memory store, inside a transaction and after rollback,
//! and on a reopened generation root (layered store) with overlay writes,
//! label changes and a deleted base node. `label_lookup_cost_probe` (ignored)
//! prints timings that show the lookup no longer scales with the label.

#![cfg(feature = "cypher")]

use std::collections::HashMap;

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;
use grafeo_engine::database::QueryResult;

/// Sorted first-column strings of `result`.
fn names(result: &QueryResult) -> Vec<String> {
    let mut out: Vec<String> = result
        .rows()
        .iter()
        .map(|row| row[0].as_str().expect("name").to_string())
        .collect();
    out.sort();
    out
}

fn run(db: &GrafeoDB, query: &str) -> Vec<String> {
    names(&db.execute_cypher(query).expect(query))
}

/// Oracle: every node with `label`, filtered on `k` in Rust (no index path).
fn oracle(exec: &dyn Fn(&str) -> QueryResult, label: &str, k: i64) -> Vec<String> {
    let result = exec(&format!("MATCH (n:{label}) RETURN n.name, n.k"));
    let mut out: Vec<String> = result
        .rows()
        .iter()
        .filter(|row| row[1].as_int64() == Some(k))
        .map(|row| row[0].as_str().expect("name").to_string())
        .collect();
    out.sort();
    out
}

/// Every labelled lookup shape (`=`, `IN`, `IN` with a miss, inline map)
/// agrees with the oracle, through `exec` (a database or a session).
fn assert_shapes_with(exec: &dyn Fn(&str) -> QueryResult, stage: &str) {
    for label in ["A", "B"] {
        for k in [1_i64, 2] {
            let want = oracle(exec, label, k);
            for query in [
                format!("MATCH (n:{label}) WHERE n.k = {k} RETURN n.name"),
                format!("MATCH (n:{label}) WHERE n.k IN [{k}] RETURN n.name"),
                format!("MATCH (n:{label}) WHERE n.k IN [{k}, 99] RETURN n.name"),
                format!("MATCH (n:{label} {{k: {k}}}) RETURN n.name"),
            ] {
                assert_eq!(names(&exec(&query)), want, "[{stage}] {query}");
            }
        }
    }
}

fn assert_shapes(db: &GrafeoDB, stage: &str) {
    assert_shapes_with(&|q: &str| db.execute_cypher(q).expect(q), stage);
}

/// Every lookup shape for label A and `k = 1` returns exactly `want`.
fn assert_a1(exec: &dyn Fn(&str) -> QueryResult, want: &[&str], stage: &str) {
    for query in [
        "MATCH (n:A) WHERE n.k = 1 RETURN n.name",
        "MATCH (n:A) WHERE n.k IN [1] RETURN n.name",
        "MATCH (n:A) WHERE n.k IN [1, 99] RETURN n.name",
        "MATCH (n:A {k: 1}) RETURN n.name",
    ] {
        assert_eq!(names(&exec(query)), want, "[{stage}] {query}");
    }
}

fn seed(db: &GrafeoDB) {
    db.create_property_index("k");
    db.execute_cypher(
        "CREATE (:A {name: 'a1', k: 1}), (:B {name: 'b1', k: 1}), \
                (:A:B {name: 'ab1', k: 1}), (:A {name: 'a2', k: 2}), \
                (:C {name: 'c1', k: 1})",
    )
    .expect("seed");
}

#[test]
fn labelled_index_lookup_matches_label_scan_in_memory() {
    let db = GrafeoDB::new_in_memory();
    seed(&db);
    assert_eq!(
        run(&db, "MATCH (n:A) WHERE n.k IN [1] RETURN n.name"),
        vec!["a1", "ab1"]
    );
    assert_shapes(&db, "seeded");

    db.execute_cypher("MATCH (n {name: 'ab1'}) REMOVE n:A")
        .expect("remove label");
    db.execute_cypher("MATCH (n {name: 'c1'}) SET n:A")
        .expect("add label");
    db.execute_cypher("MATCH (n {name: 'a1'}) DETACH DELETE n")
        .expect("delete");
    assert_eq!(
        run(&db, "MATCH (n:A) WHERE n.k = 1 RETURN n.name"),
        vec!["c1"]
    );
    assert_shapes(&db, "after label changes");
}

#[test]
fn labelled_index_lookup_inside_transaction() {
    let db = GrafeoDB::new_in_memory();
    seed(&db);
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher("CREATE (:A {name: 'a-tx', k: 1})")
        .expect("create in tx");
    session
        .execute_cypher("MATCH (n {name: 'b1'}) SET n:A")
        .expect("add label in tx");
    session
        .execute_cypher("MATCH (n {name: 'ab1'}) REMOVE n:A")
        .expect("remove label in tx");
    {
        let exec = |q: &str| session.execute_cypher(q).expect(q);
        // Explicit expectations, so a label change that silently failed
        // (and so is missing from the scan as well) still fails the test.
        assert_eq!(
            oracle(&exec, "A", 1),
            vec!["a-tx", "a1", "b1"],
            "label scan in tx"
        );
        assert_a1(&exec, &["a-tx", "a1", "b1"], "in tx");
        assert_shapes_with(&exec, "in tx");
    }
    session.rollback().expect("rollback");
    drop(session);

    let exec = |q: &str| db.execute_cypher(q).expect(q);
    assert_eq!(
        oracle(&exec, "A", 1),
        vec!["a1", "ab1"],
        "label scan after rollback"
    );
    assert_a1(&exec, &["a1", "ab1"], "after rollback");
    assert_shapes(&db, "after rollback");
}

#[cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal"
))]
#[test]
fn labelled_index_lookup_on_reopened_generation_root() {
    use grafeo_engine::generation_build_request;

    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("labels.grafeo.d");
    std::fs::create_dir_all(&root).expect("root");
    let source = GrafeoDB::new_in_memory();
    seed(&source);
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .expect("publish");
    drop(source);

    let db = GrafeoDB::open_generation_root(&root, false).expect("open");
    assert!(db.layered_store().is_some());
    assert_shapes(&db, "reopened");

    // Overlay rows, a label added to a base node and one removed from it.
    db.execute_cypher("CREATE (:A {name: 'a-new', k: 1}), (:B {name: 'b-new', k: 2})")
        .expect("overlay creates");
    db.execute_cypher("MATCH (n {name: 'c1'}) SET n:B")
        .expect("label base node");
    db.execute_cypher("MATCH (n {name: 'ab1'}) REMOVE n:B")
        .expect("unlabel base node");
    assert_eq!(
        run(&db, "MATCH (n:B) WHERE n.k IN [1] RETURN n.name"),
        vec!["b1", "c1"]
    );
    assert_shapes(&db, "after overlay writes");

    // A deleted base node: the property index still holds its mapped
    // posting, so this is what the removed label intersection used to drop.
    db.execute_cypher("MATCH (n {name: 'a1'}) DETACH DELETE n")
        .expect("delete base node");
    let exec = |q: &str| db.execute_cypher(q).expect(q);
    assert_a1(&exec, &["a-new", "ab1"], "after base delete");
    assert_shapes(&db, "after base delete");

    drop(db);
    let db = GrafeoDB::open_generation_root(&root, false).expect("reopen");
    let exec = |q: &str| db.execute_cypher(q).expect(q);
    assert_a1(&exec, &["a-new", "ab1"], "after reopen");
    assert_shapes(&db, "after reopen");
}

/// Timing probe, not an assertion: an indexed lookup that hits one node
/// should not scale with the label's size. Run with
/// `cargo test --release --features cypher --test indexed_lookup_label_check
/// -- --ignored --nocapture label_lookup_cost_probe`.
#[test]
#[ignore = "timing probe; prints numbers"]
fn label_lookup_cost_probe() {
    for size in [10_000_usize, 100_000, 400_000] {
        let db = GrafeoDB::new_in_memory();
        db.create_property_index("k");
        for i in 0..size {
            db.create_node_with_props(&["L"], [("k", Value::from(i as i64))])
                .expect("create");
        }
        let target = (size / 2) as i64;
        let query = format!("MATCH (n:L) WHERE n.k IN [{target}] RETURN count(n)");
        let _ = db.execute_cypher(&query).expect("warm");
        let rounds = 20;
        let start = std::time::Instant::now();
        for _ in 0..rounds {
            let count = db.execute_cypher(&query).expect("lookup").rows()[0][0]
                .as_int64()
                .expect("count");
            assert_eq!(count, 1);
        }
        let per_query = start.elapsed() / rounds;
        println!("label size {size:>7}: {per_query:?} per indexed IN lookup");
    }
}

// The two tests below are upstream's #459 tests (GrafeoDB/grafeo 4a3c5e6e4,
// `node_seek.rs`), with the helpers they need from that file. The fork does
// not have upstream's `node_seek.rs`, so they live here.

/// Twelve `Doc` nodes with ids `d0`..`d11`, numbers 0..11 and a few edges;
/// the same data with and without a property index on `id`.
fn docs(indexed: bool) -> GrafeoDB {
    let db = GrafeoDB::new_in_memory();
    if indexed {
        db.create_property_index("id");
        db.create_property_index("n");
    }
    // One node per statement: the fork's planner takes no computed values in
    // a pattern's property map (upstream seeds with
    // `UNWIND range(0, 11) AS i INSERT (:Doc {id: 'd' + toString(i), n: i})`).
    for i in 0..12 {
        db.execute(&format!("INSERT (:Doc {{id: 'd{i}', n: {i}}})"))
            .unwrap();
    }
    db.execute("INSERT (:Other {id: 'd1'})").unwrap();
    db.execute_cypher(
        "MATCH (a:Doc), (b:Doc) WHERE b.n = a.n + 1 AND a.n < 11 CREATE (a)-[:NEXT {w: a.n}]->(b)",
    )
    .unwrap();
    db
}

fn rows(db: &GrafeoDB, query: &str, params: &[(&str, Value)]) -> Vec<Vec<Value>> {
    let params: HashMap<String, Value> = params
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.clone()))
        .collect();
    let mut rows = db
        .execute_with_params(query, params)
        .unwrap()
        .rows()
        .to_vec();
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

/// A literal key is looked up in the index once; of what it finds, only the
/// visible nodes with the pattern's label count (`:Other {id: 'd1'}` shares the
/// key), also for labels set or removed in the transaction.
#[test]
fn a_literal_key_keeps_the_nodes_with_the_label() {
    let (sought, scanned) = (docs(true), docs(false));
    for query in [
        "MATCH (n:Doc {id: 'd1'}) RETURN n.n",
        "MATCH (n:Other {id: 'd1'}) RETURN n.id",
        "MATCH (n:Doc) WHERE n.id IN ['d1', 'd2', 'x'] RETURN n.id",
        "MATCH (n:Other) WHERE n.id IN ['d1', 'd2'] RETURN n.id",
    ] {
        assert_eq!(
            rows(&sought, query, &[]),
            rows(&scanned, query, &[]),
            "{query}"
        );
    }
    assert_eq!(
        rows(&sought, "MATCH (n:Doc {id: 'd1'}) RETURN n.n", &[]),
        [vec![Value::Int64(1)]]
    );

    let mut session = sought.session();
    session.begin_transaction().unwrap();
    session
        .execute("MATCH (n:Other {id: 'd1'}) SET n:Doc")
        .unwrap();
    session
        .execute("MATCH (n:Doc {id: 'd2'}) REMOVE n:Doc")
        .unwrap();
    let count = |query: &str| session.execute(query).unwrap().rows().len();
    assert_eq!(count("MATCH (n:Doc {id: 'd1'}) RETURN n"), 2);
    assert_eq!(count("MATCH (n:Doc {id: 'd2'}) RETURN n"), 0);
    assert_eq!(
        count("MATCH (n:Doc) WHERE n.id IN ['d1', 'd2'] RETURN n"),
        2
    );
    session.rollback().unwrap();
    assert_eq!(
        rows(&sought, "MATCH (n:Doc {id: 'd2'}) RETURN n.n", &[]),
        [vec![Value::Int64(2)]]
    );
}

/// A labeled point lookup costs about what an unlabeled one does, however many
/// nodes have the label: the label is checked on the index's results, not by
/// collecting every node with it (which took 3 ms per lookup at 60,000 nodes).
#[cfg(not(debug_assertions))]
#[test]
fn a_labeled_point_lookup_does_not_grow_with_the_label() {
    use std::time::{Duration, Instant};

    let db = GrafeoDB::new_in_memory();
    for i in 0..60_000 {
        db.create_node_with_props(&["Graph", "File"], [("id", Value::from(format!("n{i}")))])
            .unwrap();
    }
    db.create_property_index("id");
    // The fastest of five batches, to keep a busy machine out of the ratio.
    let time = |query: &str| -> Duration {
        (0..5)
            .map(|_| {
                let start = Instant::now();
                for _ in 0..100 {
                    db.execute(query).unwrap();
                }
                start.elapsed()
            })
            .min()
            .unwrap()
    };
    let unlabeled = time("MATCH (s {id: 'n10'}) RETURN s.id");
    let labeled = time("MATCH (s:File {id: 'n10'}) RETURN s.id");
    assert!(
        labeled < unlabeled * 5,
        "labeled {labeled:?} vs unlabeled {unlabeled:?} per 100 lookups"
    );
}
