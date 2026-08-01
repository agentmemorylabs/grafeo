//! G-EM0.5b D0.8.0 source-true RED fixtures.
//!
//! These tests assert the source-true label-membership and sparse-property/
//! null behavior that the bounded generation writer + production reader must
//! deliver. They are authored through the existing public generation call
//! (`generate_compact_store`) and the production `CompactStore` reader.
//!
//! Status: **RED by design** until the bounded writer emits the v5
//! label-membership / presence / null companion segments and the reader
//! honors them. Each test documents the exact source-true expectation that
//! the current eager path violates (multi-label dropped to one physical
//! label; sparse/absent properties conflated with present-null). They are
//! `#[ignore]`d so the suite stays green while the format extension lands;
//! un-ignore them as the D0.8.x writer/reader companions come online.

use grafeo_common::types::Value;
use grafeo_core::graph::GraphStore;
use grafeo_core::graph::compact::generation::{
    GenerationBudget, GenerationInput, GenerationNode, generate_compact_store,
};

fn budget() -> GenerationBudget {
    GenerationBudget::for_tests()
}

// ── Multi-label membership (D0.8.0 items 1–3) ──────────────────────

/// A node carrying two logical labels must survive generation and reopen
/// with BOTH labels visible through `get_node`, `nodes_by_label`, and
/// `all_labels` — without duplicating the node.
///
/// RED: the eager path groups the node under `physical_label()` only and
/// emits no membership segment, so the second label is lost.
#[test]
#[ignore = "D0.8.0 RED: bounded writer/reader companions not yet wired"]
fn multi_label_node_survives_round_trip() {
    let input = GenerationInput::new()
        .node(GenerationNode::with_labels(1u64, ["Person", "Employee"]).unwrap())
        .node(GenerationNode::new(2u64, "Person"));
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &budget(),
    )
    .unwrap();
    let store = generated.store;

    // Both labels resolve to the same single node.
    let by_person = store.nodes_by_label("Person");
    let by_employee = store.nodes_by_label("Employee");
    assert_eq!(by_person.len(), 2, "Person must see both nodes");
    assert_eq!(by_employee.len(), 1, "Employee must see node 1");

    // get_node returns the complete label set.
    let n1 = store
        .get_node(grafeo_common::types::NodeId::new(1))
        .expect("node 1");
    let labels: Vec<String> = n1.labels.iter().map(|l| l.to_string()).collect();
    assert!(labels.contains(&"Person".to_string()));
    assert!(labels.contains(&"Employee".to_string()));

    // all_labels is the union of logical labels.
    let all = store.all_labels();
    assert!(all.iter().any(|l| l == "Person"));
    assert!(all.iter().any(|l| l == "Employee"));

    // Node is stored once (two memberships, one physical row).
    assert_eq!(store.node_count(), 2);
}

/// A three-label node must keep all three memberships across publication and
/// fresh reopen.
#[test]
#[ignore = "D0.8.0 RED: bounded writer/reader companions not yet wired"]
fn three_label_node_keeps_all_memberships() {
    let input = GenerationInput::new()
        .node(GenerationNode::with_labels(7u64, ["A", "B", "C"]).unwrap());
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &budget(),
    )
    .unwrap();
    let store = generated.store;
    for label in ["A", "B", "C"] {
        assert_eq!(store.nodes_by_label_count(label), 1, "label {label}");
    }
}

// ── Sparse property / null three-way distinction (D0.8.0 item 4) ────

/// Sparse fixed-width properties: a row that LACKS a property key must read
/// back as absent (`None`), distinct from a row that stores a present value.
/// Zone-map pruning and scans must preserve the absence.
///
/// RED: the eager column path returns `GenerationError::NullValue` for an
/// absent row value instead of emitting a presence bitmap.
#[test]
#[ignore = "D0.8.0 RED: sparse column presence not yet emitted"]
fn sparse_property_absence_preserved() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person").with_prop("age", Value::Int64(30)))
        // Node 2 has NO "age" property.
        .node(GenerationNode::new(2u64, "Person").with_prop("name", "Bob"));
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &budget(),
    )
    .unwrap();
    let store = generated.store;

    let n2 = store
        .get_node(grafeo_common::types::NodeId::new(2))
        .expect("node 2");
    assert_eq!(n2.get_property("age"), None, "absent age must read None");
    assert!(n2.get_property("name").is_some());
}

/// A present `Value::Null` must round-trip as `Some(Value::Null)`, distinct
/// from absence, in the non-temporal source view (source-locked by
/// `grafeo-engine/tests/error_paths.rs::test_property_with_null_value`).
///
/// RED: the eager path rejects present-null with `NullValue`.
#[test]
#[ignore = "D0.8.0 RED: present-null not yet emitted"]
fn present_null_preserved_distinct_from_absent() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "T").with_prop("key", Value::Null))
        .node(GenerationNode::new(2u64, "T")); // absent
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &budget(),
    )
    .unwrap();
    let store = generated.store;
    let n1 = store.get_node(grafeo_common::types::NodeId::new(1)).expect("n1");
    let n2 = store.get_node(grafeo_common::types::NodeId::new(2)).expect("n2");
    assert_eq!(n1.get_property("key"), Some(&Value::Null));
    assert_eq!(n2.get_property("key"), None);
}

// ── Old-v5 default (backward compatibility, D0.8.0 item 6) ─────────

/// A payload with no companion segments keeps historical semantics: one
/// physical label per node, every encoded row present and non-null. This is
/// the GREEN control that must keep passing — it proves the reader's
/// old-v5-default branch.
#[test]
fn old_v5_default_single_label_semantics() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person").with_prop("name", "Ada"))
        .node(GenerationNode::new(2u64, "Person").with_prop("name", "Bob"));
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &budget(),
    )
    .unwrap();
    let store = generated.store;
    assert_eq!(store.nodes_by_label_count("Person"), 2);
    let n1 = store.get_node(grafeo_common::types::NodeId::new(1)).expect("n1");
    assert_eq!(n1.labels.len(), 1, "single physical label, no membership");
}
