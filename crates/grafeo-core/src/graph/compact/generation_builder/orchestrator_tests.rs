//! Orchestrator end-to-end test (G-EM0.5b Phase 2b).

#![cfg(feature = "generation-streaming")]

use crate::graph::compact::generation::{
    GenerationBudget, GenerationInput, GenerationNode, GenerationEdge, InMemoryRunStore,
};
use crate::graph::compact::generation_builder::orchestrator::{
    BoundedBuildConfig, BoundedGenerationBuilder,
};
use crate::graph::compact::section_v5::deserialize_v5;
use crate::graph::traits::GraphStore;
use grafeo_common::types::{NodeId, Value};
use tempfile::TempDir;

fn budget() -> GenerationBudget {
    GenerationBudget::for_tests()
}

fn config(temp: &std::path::Path) -> BoundedBuildConfig {
    BoundedBuildConfig {
        budget: budget(),
        temp_dir: temp.to_path_buf(),
        correlation_id: "test".into(),
        spool_buf_cap: 64 * 1024,
    }
}

#[test]
fn orchestrator_produces_deserializable_payload() {
    let tmp = TempDir::new().unwrap();
    let input = GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "Person")
                .with_prop("name", "Ada")
                .with_prop("age", Value::Int64(30)),
        )
        .node(
            GenerationNode::new(2u64, "Person")
                .with_prop("name", "Bob")
                .with_prop("age", Value::Int64(25)),
        )
        .node(
            GenerationNode::new(100u64, "Project")
                .with_prop("title", "Grafeo"),
        )
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "KNOWS"))
        .edge(GenerationEdge::new(11u64, 1u64, 100u64, "WORKS_ON"));

    let mut store = InMemoryRunStore::new();
    let mut builder = BoundedGenerationBuilder::new(config(tmp.path()));
    let mut lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect("build");

    // Stream the payload.
    let mut payload = Vec::new();
    lease
        .stream_to(&mut payload)
        .expect("stream_to");

    // Deserialize.
    let bytes = bytes::Bytes::from(payload);
    let compact = deserialize_v5(&bytes).expect("deserialize_v5");

    // Verify round-trip.
    assert_eq!(compact.total_nodes(), 3);
    assert_eq!(compact.total_edges(), 2);
    assert!(compact.get_node(NodeId::new(1)).is_some());
    assert!(compact.get_node(NodeId::new(2)).is_some());
    assert!(compact.get_node(NodeId::new(100)).is_some());
    assert!(compact.get_node(NodeId::new(999)).is_none());
}

#[test]
fn orchestrator_sparse_columns() {
    let tmp = TempDir::new().unwrap();
    let input = GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "Person")
                .with_prop("name", "Ada")
                .with_prop("age", Value::Int64(30)),
        )
        .node(GenerationNode::new(2u64, "Person").with_prop("name", "Bob")); // no age

    let mut store = InMemoryRunStore::new();
    let mut builder = BoundedGenerationBuilder::new(config(tmp.path()));
    let mut lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect("build");

    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");
    let bytes = bytes::Bytes::from(payload);
    let compact = deserialize_v5(&bytes).expect("deserialize_v5");

    assert_eq!(compact.total_nodes(), 2);
    let n1 = compact.get_node(NodeId::new(1)).unwrap();
    let n2 = compact.get_node(NodeId::new(2)).unwrap();
    assert_eq!(n1.get_property("age"), Some(&Value::Int64(30)));
    assert_eq!(n2.get_property("age"), None); // sparse
}
