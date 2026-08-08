//! G-GEM0.SRV1 Gap A RED repro (toy scale): Cypher edge traversal over a
//! base-published generation root must serve the published edges.
//!
//! The direct GraphStore API serves the base (this test proves it), but the
//! identical Cypher queries return zero rows — the query path does not
//! consult the layered base's edge sections.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap"
))]

use grafeo_common::types::Value;
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::TempDir;

#[test]
fn cypher_edge_traversal_serves_base_published_edges() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("srv1-gap-a.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    // Publish a base with nodes AND edges.
    let source = GrafeoDB::new_in_memory();
    let ada = source
        .create_node_with_props(&["Person"], [("name", Value::from("Ada"))])
        .expect("create Ada");
    let grace = source
        .create_node_with_props(&["Person"], [("name", Value::from("Grace"))])
        .expect("create Grace");
    let knows =
        source.create_edge_with_props(ada, grace, "KNOWS", [("since", Value::from(2020i64))]);
    let _works =
        source.create_edge_with_props(grace, ada, "WORKS_WITH", [("role", Value::from("peer"))]);
    source
        .build_and_publish_generation(generation_build_request(&root, "srv1-gap-a-g1"))
        .expect("publish generation");
    drop(source);

    // RO reopen — the serving path under test.
    let db = GrafeoDB::open_generation_root(&root, true).expect("open generation root RO");
    let store = db.graph_store();

    // Ground truth: the direct API serves the base edges.
    let out = store.edges_from(ada, grafeo_core::graph::Direction::Outgoing);
    assert_eq!(
        out.len(),
        1,
        "direct API must serve the base KNOWS edge (id {:?})",
        knows
    );

    // Gap A: the Cypher path must serve them too.
    let all = db
        .session()
        .execute("MATCH ()-[r]->() RETURN count(r)")
        .expect("count all edges");
    let got_all: i64 = all
        .rows()
        .first()
        .and_then(|r| r.first())
        .and_then(Value::as_int64)
        .unwrap_or(-1);
    assert_eq!(
        got_all, 2,
        "Cypher full-edge scan must count base-published edges"
    );

    let typed = db
        .session()
        .execute("MATCH ()-[r:KNOWS]->() RETURN count(r)")
        .expect("count typed edges");
    let got_typed: i64 = typed
        .rows()
        .first()
        .and_then(|r| r.first())
        .and_then(Value::as_int64)
        .unwrap_or(-1);
    assert_eq!(
        got_typed, 1,
        "Cypher typed-edge scan must count base-published edges"
    );
}
