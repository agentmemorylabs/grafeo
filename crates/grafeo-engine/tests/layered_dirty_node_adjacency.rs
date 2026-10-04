//! D4 — on a generation root, touching a base node must not hide its base
//! links.
//!
//! A generation root serves a read-only `CompactStore` *base* under a
//! writable `LpgStore` *overlay* (`LayeredStore`). Writing to a base node
//! (a property SET, or becoming an endpoint of a new edge) copies the node
//! into the overlay and marks it *dirty*. Before the fix,
//! `LayeredStore::edges_from` / `neighbors` skipped every base edge of a
//! dirty node, so the node's pre-existing links vanished in both directions,
//! and stayed gone after reopen because WAL replay re-dirties the node.
//!
//! Every test publishes a base, reopens the root so the overlay starts
//! empty, writes through Cypher, and checks adjacency both through Cypher
//! `MATCH` and through `LayeredStore::edges_from` / `neighbors` directly.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::path::Path;

use grafeo_common::types::{EdgeId, NodeId, Value};
use grafeo_core::graph::Direction;
use grafeo_core::graph::compact::layered::LayeredStore;
use grafeo_core::graph::traits::GraphStore;
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::tempdir;

/// Publishes the base generation:
///
/// ```text
/// (f1:File)-[:DEFINES]->(S:Symbol)<-[:CALLS]-(f2:File)
/// (S)-[:USES {w: 0}]->(T:Symbol)
/// (S)-[:USES {w: 0}]->(T2:Symbol)
/// ```
fn publish_base(root: &Path) {
    let source = GrafeoDB::new_in_memory();
    let node = |label: &str, name: &str| {
        source
            .create_node_with_props(&[label], [("name", Value::from(name))])
            .expect("create node")
    };
    let f1 = node("File", "f1");
    let f2 = node("File", "f2");
    let s = node("Symbol", "S");
    let t = node("Symbol", "T");
    let t2 = node("Symbol", "T2");
    source.create_edge(f1, s, "DEFINES");
    source.create_edge(f2, s, "CALLS");
    source.create_edge_with_props(s, t, "USES", [("w", Value::from(0i64))]);
    source.create_edge_with_props(s, t2, "USES", [("w", Value::from(0i64))]);
    source
        .build_and_publish_generation(generation_build_request(root, "d4-g1"))
        .expect("publish base generation");
    drop(source);
}

/// Publishes the base, then reopens the root writable with an empty overlay.
fn fresh_root() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("d4.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");
    publish_base(&root);
    (dir, root)
}

fn open(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, false).expect("open generation root writable")
}

fn layered(db: &GrafeoDB) -> &LayeredStore {
    db.layered_store()
        .expect("a generation root is served by a LayeredStore")
}

fn cypher(db: &GrafeoDB, query: &str) {
    db.session()
        .execute_cypher(query)
        .unwrap_or_else(|e| panic!("cypher `{query}` failed: {e}"));
}

fn cypher_count(db: &GrafeoDB, query: &str) -> i64 {
    let result = db
        .session()
        .execute_cypher(query)
        .unwrap_or_else(|e| panic!("cypher `{query}` failed: {e}"));
    match &result.rows()[0][0] {
        Value::Int64(v) => *v,
        other => panic!("expected integer count from `{query}`, got {other:?}"),
    }
}

fn node_named(store: &LayeredStore, name: &str) -> NodeId {
    store
        .node_ids()
        .into_iter()
        .find(|&id| store.get_node_property(id, &"name".into()) == Some(Value::from(name)))
        .unwrap_or_else(|| panic!("node `{name}` should be visible"))
}

fn names(store: &LayeredStore, ids: impl IntoIterator<Item = NodeId>) -> Vec<String> {
    let mut out: Vec<String> = ids
        .into_iter()
        .map(|id| match store.get_node_property(id, &"name".into()) {
            Some(Value::String(s)) => s.as_str().to_string(),
            other => panic!("node {id:?} has no string name: {other:?}"),
        })
        .collect();
    out.sort_unstable();
    out
}

fn edge_targets(store: &LayeredStore, node: NodeId, dir: Direction) -> Vec<String> {
    names(
        store,
        store.edges_from(node, dir).into_iter().map(|(t, _)| t),
    )
}

fn neighbor_names(store: &LayeredStore, node: NodeId, dir: Direction) -> Vec<String> {
    names(store, store.neighbors(node, dir))
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| (*s).to_string()).collect()
}

/// Asserts S's adjacency through the store API and through Cypher, from
/// both anchors. `incoming` / `outgoing` are the expected neighbor names.
fn assert_s_adjacency(db: &GrafeoDB, incoming: &[&str], outgoing: &[&str], ctx: &str) {
    let store = layered(db);
    let s = node_named(store, "S");
    assert_eq!(
        edge_targets(store, s, Direction::Incoming),
        strs(incoming),
        "{ctx}: edges_from(S, Incoming)"
    );
    assert_eq!(
        neighbor_names(store, s, Direction::Incoming),
        strs(incoming),
        "{ctx}: neighbors(S, Incoming)"
    );
    assert_eq!(
        edge_targets(store, s, Direction::Outgoing),
        strs(outgoing),
        "{ctx}: edges_from(S, Outgoing)"
    );
    assert_eq!(
        neighbor_names(store, s, Direction::Outgoing),
        strs(outgoing),
        "{ctx}: neighbors(S, Outgoing)"
    );

    let incoming_len = i64::try_from(incoming.len()).unwrap();
    let outgoing_len = i64::try_from(outgoing.len()).unwrap();
    assert_eq!(
        cypher_count(db, "MATCH (f)-[]->(s:Symbol {name: 'S'}) RETURN count(f)"),
        incoming_len,
        "{ctx}: Cypher incoming links of S"
    );
    assert_eq!(
        cypher_count(db, "MATCH (s:Symbol {name: 'S'})-[]->(t) RETURN count(t)"),
        outgoing_len,
        "{ctx}: Cypher outgoing links of S"
    );
    // Anchored on the far side, so the planner walks from the untouched node.
    for name in incoming {
        assert_eq!(
            cypher_count(
                db,
                &format!(
                    "MATCH (f {{name: '{name}'}})-[]->(s:Symbol {{name: 'S'}}) RETURN count(*)"
                )
            ),
            1,
            "{ctx}: Cypher {name} -> S"
        );
    }
    for name in outgoing {
        assert_eq!(
            cypher_count(
                db,
                &format!(
                    "MATCH (s:Symbol {{name: 'S'}})-[]->(t {{name: '{name}'}}) RETURN count(*)"
                )
            ),
            1,
            "{ctx}: Cypher S -> {name}"
        );
    }
}

const BASE_IN: &[&str] = &["f1", "f2"];
const BASE_OUT: &[&str] = &["T", "T2"];

#[test]
fn baseline_reopened_root_serves_base_adjacency() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    assert_s_adjacency(&db, BASE_IN, BASE_OUT, "untouched base");
}

/// (a) A property SET on a base node keeps its outgoing AND incoming base
/// edges visible.
#[test]
fn set_property_on_base_node_keeps_its_base_edges() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    cypher(&db, "MATCH (s:Symbol {name: 'S'}) SET s.touched = true");
    assert_eq!(
        cypher_count(
            &db,
            "MATCH (s:Symbol {name: 'S'}) WHERE s.touched = true RETURN count(s)"
        ),
        1
    );
    assert_s_adjacency(&db, BASE_IN, BASE_OUT, "after SET on S");
}

/// (b) Creating new edges onto / out of a base node keeps that node's other
/// base edges visible, in both directions.
#[test]
fn new_edge_on_base_node_keeps_its_other_base_edges() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    // New incoming edge: S becomes the destination of an overlay edge.
    cypher(
        &db,
        "MATCH (s:Symbol {name: 'S'}) CREATE (:File {name: 'f3'})-[:CALLS]->(s)",
    );
    assert_s_adjacency(&db, &["f1", "f2", "f3"], BASE_OUT, "after new f3 -> S");

    // New outgoing edge: S becomes the source of an overlay edge.
    cypher(
        &db,
        "MATCH (s:Symbol {name: 'S'}) CREATE (s)-[:USES]->(:Symbol {name: 'U'})",
    );
    assert_s_adjacency(
        &db,
        &["f1", "f2", "f3"],
        &["T", "T2", "U"],
        "after new S -> U",
    );
}

/// (b') The base-only far endpoint of a new edge is dirtied too; its other
/// base edges must stay visible (f1 gains an edge to a new node).
#[test]
fn new_edge_on_neighbor_keeps_its_base_edge_to_s() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    cypher(
        &db,
        "MATCH (f:File {name: 'f1'}) CREATE (f)-[:DEFINES]->(:Symbol {name: 'V'})",
    );
    let store = layered(&db);
    let f1 = node_named(store, "f1");
    assert_eq!(
        edge_targets(store, f1, Direction::Outgoing),
        strs(&["S", "V"]),
        "edges_from(f1, Outgoing) after new f1 -> V"
    );
    assert_s_adjacency(&db, BASE_IN, BASE_OUT, "after new f1 -> V");
}

/// (c) A SET on a base EDGE keeps the source node's sibling base edges (and
/// the destination's) visible.
#[test]
fn set_on_base_edge_keeps_sibling_base_edges() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    cypher(
        &db,
        "MATCH (:Symbol {name: 'S'})-[r:USES]->(:Symbol {name: 'T'}) SET r.w = 7",
    );
    assert_eq!(
        cypher_count(
            &db,
            "MATCH (:Symbol {name: 'S'})-[r:USES]->(:Symbol {name: 'T'}) WHERE r.w = 7 RETURN count(r)"
        ),
        1,
        "the edge SET is visible"
    );
    assert_s_adjacency(&db, BASE_IN, BASE_OUT, "after SET on S-[:USES]->T");
    // The promoted edge must be reported once, not twice (base + overlay copy).
    let store = layered(&db);
    let s = node_named(store, "S");
    let t = node_named(store, "T");
    let out: Vec<(NodeId, EdgeId)> = store.edges_from(s, Direction::Outgoing);
    assert_eq!(out.iter().filter(|(n, _)| *n == t).count(), 1);
    assert_eq!(
        cypher_count(
            &db,
            "MATCH (:Symbol {name: 'S'})-[r:USES]->() RETURN count(r)"
        ),
        2
    );
}

/// (d) All of the above survives close + reopen (WAL replay re-dirties the
/// touched nodes and edges).
#[test]
fn touched_base_adjacency_survives_reopen() {
    let (_dir, root) = fresh_root();
    {
        let db = open(&root);
        cypher(&db, "MATCH (s:Symbol {name: 'S'}) SET s.touched = true");
        cypher(
            &db,
            "MATCH (s:Symbol {name: 'S'}) CREATE (:File {name: 'f3'})-[:CALLS]->(s)",
        );
        cypher(
            &db,
            "MATCH (:Symbol {name: 'S'})-[r:USES]->(:Symbol {name: 'T'}) SET r.w = 7",
        );
        assert_s_adjacency(&db, &["f1", "f2", "f3"], BASE_OUT, "before close");
        db.close().expect("close");
    }
    for cycle in 0..2 {
        let db = open(&root);
        let ctx = format!("after reopen #{cycle}");
        assert_eq!(
            cypher_count(
                &db,
                "MATCH (s:Symbol {name: 'S'}) WHERE s.touched = true RETURN count(s)"
            ),
            1,
            "{ctx}: SET on S replayed"
        );
        assert_eq!(
            cypher_count(
                &db,
                "MATCH (:Symbol {name: 'S'})-[r:USES]->(:Symbol {name: 'T'}) WHERE r.w = 7 RETURN count(r)"
            ),
            1,
            "{ctx}: SET on S-[:USES]->T replayed"
        );
        assert_s_adjacency(&db, &["f1", "f2", "f3"], BASE_OUT, &ctx);
        db.close().expect("close");
    }
}

/// (e) A base edge copied into the overlay (by a SET) and then deleted stays
/// deleted: the base copy must not resurrect it, before or after reopen.
#[test]
fn promoted_then_deleted_base_edge_stays_deleted() {
    let (_dir, root) = fresh_root();
    {
        let db = open(&root);
        cypher(
            &db,
            "MATCH (:Symbol {name: 'S'})-[r:USES]->(:Symbol {name: 'T'}) SET r.w = 7",
        );
        cypher(
            &db,
            "MATCH (:Symbol {name: 'S'})-[r:USES]->(:Symbol {name: 'T'}) DELETE r",
        );
        assert_s_adjacency(&db, BASE_IN, &["T2"], "after promote + delete");
        let store = layered(&db);
        let t = node_named(store, "T");
        assert!(
            store.edges_from(t, Direction::Incoming).is_empty(),
            "edges_from(T, Incoming) must not resurrect the deleted edge"
        );
        assert!(
            store.neighbors(t, Direction::Incoming).is_empty(),
            "neighbors(T, Incoming) must not resurrect the deleted edge"
        );
        db.close().expect("close");
    }
    let db = open(&root);
    assert_s_adjacency(&db, BASE_IN, &["T2"], "after reopen");
    assert_eq!(
        cypher_count(&db, "MATCH ()-[r]->() RETURN count(r)"),
        3,
        "edge count after reopen"
    );
}

/// (e') A plain base edge delete (no promotion) is honoured by `neighbors`
/// too, not only by `edges_from`.
#[test]
fn deleted_base_edge_hidden_from_neighbors() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    cypher(
        &db,
        "MATCH (:Symbol {name: 'S'})-[r:USES]->(:Symbol {name: 'T'}) DELETE r",
    );
    assert_s_adjacency(&db, BASE_IN, &["T2"], "after base edge delete");
}
