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

    // Explode occurrences + sortable ID-index runs (D0.8.4).
    let mut occ_sink = store.sink("occ", &b).unwrap();
    let mut id_index_sink = store.sink("id-index", &b).unwrap();
    let mut node_row_merger = store.merger("node-rows").unwrap();
    let counts = pass
        .explode_occurrences(
            &mut out,
            node_row_merger.as_mut(),
            occ_sink.as_mut(),
            id_index_sink.as_mut(),
            &mut metrics,
        )
        .expect("explode");
    assert_eq!(counts, vec![2, 1], "Person has 2 rows, Project 1");
    let occ_lease = occ_sink.finish().expect("occ lease");
    let id_index_lease = id_index_sink.finish().expect("id-index lease");

    // Materialize sorted fixed-width file and open via RunStore mapper.
    use crate::graph::compact::mapped::id_index::{ID_INDEX_RECORD_LEN, id_index_record_bytes};
    use std::io::Write;
    let tmp = tempfile::tempdir().expect("tmpdir");
    let id_path = tmp.path().join("id-index.bin");
    let mut file = std::fs::File::create(&id_path).expect("create");
    let mut merger = store.merger("id-index").unwrap();
    merger
        .merge_all(
            &id_index_lease.handles,
            &b,
            &mut metrics,
            None,
            &mut |rec| {
                let original_id = u64::from_be_bytes(rec.key[0..8].try_into().unwrap());
                let table_id = u16::from_le_bytes(rec.payload[0..2].try_into().unwrap());
                let dense_offset = u64::from_le_bytes(rec.payload[2..10].try_into().unwrap());
                let bytes = id_index_record_bytes(original_id, table_id, dense_offset);
                file.write_all(&bytes).unwrap();
                assert_eq!(bytes.len(), ID_INDEX_RECORD_LEN);
                Ok(())
            },
        )
        .expect("merge id-index");
    drop(file);
    let id_index = store.map_id_index_file(&id_path).expect("id index");
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
