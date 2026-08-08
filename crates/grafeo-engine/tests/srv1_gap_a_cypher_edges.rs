//! G-GEM0.SRV1 Gap A RED repro — PUBLISH ONLY SERVES EDGES WHOSE SOURCE IS
//! IN NODE TABLE 0 (H-ADOPT.7 Wave 4 G2 run 6 frontier finding).
//!
//! Empirical rule (20-variant bisection, 2026-08-07, pin+`87809a9b`):
//! after `build_and_publish_generation` + RO generation-root reopen, Cypher
//! edge traversal returns ONLY edges whose source node is in node table 0
//! (the first-created label). Edges from any other source table are
//! invisible to Cypher while still readable through the direct GraphStore
//! edge-record API (`get_edge`), proving the records ARE published and the
//! traversal/directory path drops them.
//!
//! Bisection table (node tables numbered by first-creation order):
//!   - 1 label (src always table 0): PASS — this is why ALL pre-existing
//!     engine generation tests pass (single-label or table-0-source only).
//!   - v6 two labels, src=table-0 label: PASS.
//!   - v13 two labels/two types: table-0-src type survives, other lost.
//!   - v9/v12/v14/v15 three labels, src=table 1 or 2: ALL edges lost.
//!   - x2 same edge TYPE from tables 0 and 1: only the table-0-src edge
//!     survives (the type itself is not the trigger).
//!   - Frontier code-index graph: CodeDocument = table 0 (no outgoing
//!     edges); every one of the 4.7M edges sources at CodeSymbol/
//!     RetrievalUnit → zero edges servable. G1 parity cannot pass.
//!
//! The suspect surface is the generation build's rel-table directory /
//! CSR emission (`generation_builder::write_directory_segments` rel_tables
//! and `csr_pass`), which must emit one rel table per edge type with
//! correct (src_table_id, dst_table_id) pairs for ALL source tables.

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

fn open_ro_and_count(root: &std::path::Path, q: &str) -> i64 {
    let db = GrafeoDB::open_generation_root(root, true).expect("open generation root RO");
    db.session()
        .execute(q)
        .expect("cypher")
        .rows()
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_int64)
        .unwrap_or(-1)
}

/// Minimal frontier shape: 3 labels created in document order, one edge
/// sourced at table 1 and one at table 2. Both must survive publish+reopen.
#[test]
fn publish_serves_edges_from_all_source_tables() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("srv1-red.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    let source = GrafeoDB::new_in_memory();
    // Table 0: CodeDocument (created first). No outgoing edges.
    let doc = source
        .create_node_with_props(&["CodeDocument"], [("path", Value::from("a.rs"))])
        .expect("doc");
    // Table 1: CodeSymbol.
    let sym = source
        .create_node_with_props(&["CodeSymbol"], [("name", Value::from("f1"))])
        .expect("sym");
    // Table 2: RetrievalUnit.
    let ru = source
        .create_node_with_props(&["RetrievalUnit"], [("name", Value::from("u1"))])
        .expect("ru");
    // Edge from table 1 (frontier: OCCURRENCE_OF).
    source.create_edge(sym, doc, "OCCURRENCE_OF");
    // Edge from table 2 (frontier shape: RU -> symbol relations).
    source.create_edge_with_props(ru, sym, "ENCLOSED_BY", [("k", Value::from("v"))]);
    source
        .build_and_publish_generation(generation_build_request(&root, "srv1-red-g1"))
        .expect("publish generation");
    drop(source);

    // Direct API ground truth: the records are published.
    let db = GrafeoDB::open_generation_root(&root, true).expect("open RO for direct reads");
    let store = db.graph_store();
    let from_sym = store.edges_from(sym, grafeo_core::graph::Direction::Outgoing);
    let from_ru = store.edges_from(ru, grafeo_core::graph::Direction::Outgoing);
    assert_eq!(from_sym.len(), 1, "direct API serves the table-1-src edge");
    assert_eq!(from_ru.len(), 1, "direct API serves the table-2-src edge");
    drop(db);

    // Cypher must serve BOTH — fails pre-fix (rule: only table-0-src edges).
    let all = open_ro_and_count(&root, "MATCH ()-[r]->() RETURN count(r)");
    let occ = open_ro_and_count(&root, "MATCH ()-[r:OCCURRENCE_OF]->() RETURN count(r)");
    let enc = open_ro_and_count(&root, "MATCH ()-[r:ENCLOSED_BY]->() RETURN count(r)");
    assert_eq!(occ, 1, "OCCURRENCE_OF from table 1 must be servable");
    assert_eq!(enc, 1, "ENCLOSED_BY from table 2 must be servable");
    assert_eq!(all, 2, "all base edges must be servable via Cypher");
}

/// Same edge TYPE from tables 0 and 1: both must survive (rules out a
/// per-type-directory explanation — the trigger is the source table).
#[test]
fn publish_serves_same_type_from_multiple_source_tables() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("srv1-red-sametype.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    let source = GrafeoDB::new_in_memory();
    let a = source
        .create_node_with_props(&["Doc"], [("p", Value::from("x"))])
        .expect("a");
    let b = source
        .create_node_with_props(&["Sym"], [("n", Value::from("f1"))])
        .expect("b");
    let c = source
        .create_node_with_props(&["Unit"], [("n", Value::from("f2"))])
        .expect("c");
    source.create_edge_with_props(a, c, "LINKS", [("k", Value::from("from-table0"))]);
    source.create_edge_with_props(b, c, "LINKS", [("k", Value::from("from-table1"))]);
    source
        .build_and_publish_generation(generation_build_request(&root, "srv1-red-sametype-g1"))
        .expect("publish generation");
    drop(source);

    let links = open_ro_and_count(&root, "MATCH ()-[r:LINKS]->() RETURN count(r)");
    assert_eq!(
        links, 2,
        "both LINKS edges must survive regardless of source table"
    );
}
