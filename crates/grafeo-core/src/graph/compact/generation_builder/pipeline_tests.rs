//! Bounded pipeline composition tests (G-EM0.5b D0.8.2/D0.8.6).
//!
//! Drives the bounded node pass + occurrence explosion + column geometry
//! through the in-memory `RunStore`, proving the streaming multi-pass shape
//! produces the same schema/geometry the eager path derives — without any
//! graph-sized heap container.

#![cfg(feature = "generation-streaming")]

use crate::graph::compact::generation::{
    GenerationBudget, GenerationInput, GenerationNode, InMemoryRunStore, RunStore,
};
use crate::graph::compact::generation_builder::column_pass::compute_column_geometries;
use crate::graph::compact::generation_builder::node_pass::NodePass;
use grafeo_common::types::Value;

fn budget() -> GenerationBudget {
    GenerationBudget::for_tests()
}

#[test]
fn bounded_node_pass_schema_and_geometry() {
    let input = GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "Person")
                .with_prop("name", "Ada")
                .with_prop("age", Value::Int64(30)),
        )
        .node(GenerationNode::new(2u64, "Person").with_prop("name", "Bob"))
        .node(GenerationNode::new(100u64, "Project").with_prop("title", "Grafeo"));

    let b = budget();
    let mut store = InMemoryRunStore::new();
    let pass = NodePass::new(&b, None);
    let mut metrics = crate::graph::compact::generation::GenerationMetrics::default();

    // Stage once.
    let mut out = pass
        .stage(&mut input.node_source(), &mut store)
        .expect("stage");

    // Schema: Person=table 0, Project=table 1 (lexicographic).
    assert_eq!(out.schema.labels, vec!["Person", "Project"]);
    assert_eq!(out.schema.total_nodes, 3);

    // Duplicate-ID rejection passes (no dups here).
    let mut id_merger = store.merger("node-ids").unwrap();
    pass.reject_duplicate_ids(&out, id_merger.as_mut(), &mut metrics)
        .expect("no duplicates");

    // Explode occurrences.
    let mut occ_sink = store.sink("occ", &b).unwrap();
    let mut id_index_bytes = Vec::new();
    let mut node_row_merger = store.merger("node-rows").unwrap();
    let counts = pass
        .explode_occurrences(
            &mut out,
            node_row_merger.as_mut(),
            occ_sink.as_mut(),
            &mut id_index_bytes,
            &mut metrics,
        )
        .expect("explode");
    assert_eq!(counts, vec![2, 1], "Person has 2 rows, Project 1");
    let occ_lease = occ_sink.finish().expect("occ lease");

    // ID index: resolves original → (table, offset).
    let id_index = crate::graph::compact::mapped::id_index::MappedNodeIdIndex::new(
        bytes::Bytes::from(id_index_bytes),
    )
    .expect("id index");
    assert_eq!(id_index.lookup(1), Some((0, 0)));
    assert_eq!(id_index.lookup(2), Some((0, 1)));
    assert_eq!(id_index.lookup(100), Some((1, 0)));

    // Column geometry from the occurrence run.
    let mut occ_merger = store.merger("occ").unwrap();
    let geometries = compute_column_geometries(
        &occ_lease,
        occ_merger.as_mut(),
        &b,
        &mut metrics,
        None,
        &|tid| counts[tid as usize],
    )
    .expect("geometries");

    // Columns: Person.name (2 present), Person.age (1 present, 1 absent),
    // Project.title (1 present).
    let by_key: std::collections::HashMap<(u16, &str), _> = geometries
        .iter()
        .map(|g| ((g.table_id, g.key.as_str()), g))
        .collect();
    let name = by_key[&(0u16, "name")];
    assert_eq!(name.present_count, 2);
    assert!(!name.needs_presence(), "name on every Person row");
    let age = by_key[&(0u16, "age")];
    assert_eq!(age.present_count, 1);
    assert!(
        age.needs_presence(),
        "age absent on row 1 → presence bitmap"
    );
    let title = by_key[&(1u16, "title")];
    assert_eq!(title.present_count, 1);
}

#[test]
fn bounded_node_pass_rejects_duplicate_ids() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "P"))
        .node(GenerationNode::new(1u64, "P"));
    let b = budget();
    let mut store = InMemoryRunStore::new();
    let pass = NodePass::new(&b, None);
    let mut metrics = crate::graph::compact::generation::GenerationMetrics::default();
    let out = pass
        .stage(&mut input.node_source(), &mut store)
        .expect("stage");
    let mut id_merger = store.merger("node-ids").unwrap();
    let err = pass
        .reject_duplicate_ids(&out, id_merger.as_mut(), &mut metrics)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::graph::compact::generation::GenerationError::DuplicateNodeId(1)
    ));
}
