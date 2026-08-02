//! G-EM0.5b D0.8.0 source-true fixtures.
//!
//! Asserts label-membership and sparse-property/null behavior through the
//! public bounded generation path (`BoundedGenerationBuilder`) and production
//! `CompactStore` reader (`deserialize_v5` fresh reopen).

#![cfg(feature = "generation-streaming")]

use bytes::Bytes;
use grafeo_common::types::Value;
use grafeo_core::graph::GraphStore;
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_core::graph::compact::generation::{
    GenerationBudget, GenerationInput, GenerationNode, InMemoryRunStore,
};
use grafeo_core::graph::compact::generation_builder::orchestrator::{
    BoundedBuildConfig, BoundedGenerationBuilder,
};
use grafeo_core::graph::compact::mapped::layout_flags;
use tempfile::TempDir;

fn budget() -> GenerationBudget {
    GenerationBudget::for_tests()
}

/// Bounded writer + fresh-reopen reader round trip.
fn bounded_round_trip(
    input: &GenerationInput,
) -> std::sync::Arc<grafeo_core::graph::compact::CompactStore> {
    let tmp = TempDir::new().unwrap();
    let mut store = InMemoryRunStore::new();
    let config = BoundedBuildConfig {
        budget: budget(),
        temp_dir: tmp.path().to_path_buf(),
        correlation_id: "source-true".into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
    };
    let mut builder = BoundedGenerationBuilder::new(config);
    let mut lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect("bounded build");
    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");
    drop(lease);
    let mut section = CompactStoreSection::empty();
    section
        .deserialize_from_bytes(Bytes::from(payload))
        .expect("fresh reopen");
    section.store().expect("store")
}

// ── Multi-label membership (D0.8.0 items 1–3) ──────────────────────

#[test]
fn multi_label_node_survives_round_trip() {
    let input = GenerationInput::new()
        .node(GenerationNode::with_labels(1u64, ["Person", "Employee"]).unwrap())
        .node(GenerationNode::new(2u64, "Person"));
    let store = bounded_round_trip(&input);

    let by_person = store.nodes_by_label("Person");
    let by_employee = store.nodes_by_label("Employee");
    assert_eq!(by_person.len(), 2, "Person must see both nodes");
    assert_eq!(by_employee.len(), 1, "Employee must see node 1");

    let n1 = store
        .get_node(grafeo_common::types::NodeId::new(1))
        .expect("node 1");
    let labels: Vec<String> = n1.labels.iter().map(|l| l.to_string()).collect();
    assert!(labels.contains(&"Person".to_string()));
    assert!(labels.contains(&"Employee".to_string()));

    let all = store.all_labels();
    assert!(all.iter().any(|l| l == "Person"));
    assert!(all.iter().any(|l| l == "Employee"));
    assert_eq!(store.node_count(), 2);
}

#[test]
fn three_label_node_keeps_all_memberships() {
    let input = GenerationInput::new()
        .node(GenerationNode::with_labels(7u64, ["A", "B", "C"]).unwrap());
    let store = bounded_round_trip(&input);
    for label in ["A", "B", "C"] {
        assert_eq!(
            store.nodes_by_label(label).len(),
            1,
            "label {label} must resolve the node"
        );
    }
}

// ── Sparse property / null three-way distinction (D0.8.0 item 4) ────

#[test]
fn sparse_property_absence_preserved() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person").with_prop("age", Value::Int64(30)))
        .node(GenerationNode::new(2u64, "Person").with_prop("name", "Bob"));
    let store = bounded_round_trip(&input);

    let n2 = store
        .get_node(grafeo_common::types::NodeId::new(2))
        .expect("node 2");
    assert_eq!(n2.get_property("age"), None, "absent age must read None");
    assert!(n2.get_property("name").is_some());
}

#[test]
fn present_null_preserved_distinct_from_absent() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "T").with_prop("key", Value::Null))
        .node(GenerationNode::new(2u64, "T"));
    let store = bounded_round_trip(&input);
    let n1 = store.get_node(grafeo_common::types::NodeId::new(1)).expect("n1");
    let n2 = store.get_node(grafeo_common::types::NodeId::new(2)).expect("n2");
    assert_eq!(n1.get_property("key"), Some(&Value::Null));
    assert_eq!(n2.get_property("key"), None);
}

// ── Old-v5 default (backward compatibility, D0.8.0 item 6) ─────────

#[test]
fn old_v5_default_single_label_semantics() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person").with_prop("name", "Ada"))
        .node(GenerationNode::new(2u64, "Person").with_prop("name", "Bob"));
    let store = bounded_round_trip(&input);
    assert_eq!(store.nodes_by_label_count("Person"), 2);
    let n1 = store.get_node(grafeo_common::types::NodeId::new(1)).expect("n1");
    assert_eq!(n1.labels.len(), 1, "single physical label, no membership");
}

// ── Extended-payload fail-closed (D0.8.0 item 5) ───────────────────

#[test]
fn missing_required_membership_segment_fails_closed() {
    // Single-label payload: no membership segment, layout_flags = 0.
    let input = GenerationInput::new().node(GenerationNode::new(1u64, "Person"));
    let tmp = TempDir::new().unwrap();
    let mut store = InMemoryRunStore::new();
    let config = BoundedBuildConfig {
        budget: budget(),
        temp_dir: tmp.path().to_path_buf(),
        correlation_id: "fail-closed".into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
    };
    let mut builder = BoundedGenerationBuilder::new(config);
    let mut lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .unwrap();
    let mut payload = Vec::new();
    lease.stream_to(&mut payload).unwrap();
    drop(lease);

    // Tamper: claim membership is required without emitting the segment.
    let tampered_flags = layout_flags::from_companion_segments(true, false, false);
    payload[12..16].copy_from_slice(&tampered_flags.to_le_bytes());
    let tail = payload.len() - 4;
    let outer_crc = crc32fast::hash(&payload[..tail]);
    payload[tail..].copy_from_slice(&outer_crc.to_le_bytes());

    let mut section = CompactStoreSection::empty();
    let err = section
        .deserialize_from_bytes(Bytes::from(payload))
        .expect_err("must fail closed");
    let msg = err.to_string();
    assert!(
        msg.contains("NodeLabelMembership") && msg.contains("absent"),
        "unexpected error: {msg}"
    );
}
