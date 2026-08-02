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
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
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

/// B6 (bounded writer): a multi-label node must round-trip through the bounded
/// orchestrator + production reader with ALL logical labels visible, while the
/// node is stored exactly once. Exercises the NodeLabelMembership companion
/// segment (kind 21) emitted via the external-sort membership pass.
#[test]
fn orchestrator_multi_label_membership_round_trip() {
    let tmp = TempDir::new().unwrap();
    let input = GenerationInput::new()
        .node(GenerationNode::with_labels(1u64, ["Person", "Employee"]).unwrap())
        .node(GenerationNode::new(2u64, "Person"));

    let mut store = InMemoryRunStore::new();
    let mut builder = BoundedGenerationBuilder::new(config(tmp.path()));
    let mut lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect("build");

    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");
    let bytes = bytes::Bytes::from(payload);
    let compact = deserialize_v5(&bytes).expect("deserialize_v5");

    // Node stored once per physical row (2 nodes total, not 3).
    assert_eq!(compact.total_nodes(), 2);

    // Both labels resolve the correct node sets.
    let by_person = compact.nodes_by_label("Person");
    let by_employee = compact.nodes_by_label("Employee");
    assert_eq!(by_person.len(), 2, "Person must see both nodes");
    assert_eq!(by_employee.len(), 1, "Employee must see node 1 only");

    // get_node returns the complete logical label set for the multi-label node.
    let n1 = compact.get_node(NodeId::new(1)).expect("node 1");
    let labels: Vec<String> = n1.labels.iter().map(|l| l.to_string()).collect();
    assert!(labels.contains(&"Person".to_string()), "labels: {labels:?}");
    assert!(labels.contains(&"Employee".to_string()), "labels: {labels:?}");

    // all_labels is the union of logical labels.
    let all = compact.all_labels();
    assert!(all.iter().any(|l| l == "Person"));
    assert!(all.iter().any(|l| l == "Employee"));
}

/// B6: a three-label node keeps all three memberships through the bounded
/// writer + reader round-trip.
#[test]
fn orchestrator_three_label_membership_round_trip() {
    let tmp = TempDir::new().unwrap();
    let input =
        GenerationInput::new().node(GenerationNode::with_labels(7u64, ["A", "B", "C"]).unwrap());

    let mut store = InMemoryRunStore::new();
    let mut builder = BoundedGenerationBuilder::new(config(tmp.path()));
    let mut lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect("build");

    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");
    let bytes = bytes::Bytes::from(payload);
    let compact = deserialize_v5(&bytes).expect("deserialize_v5");

    assert_eq!(compact.total_nodes(), 1);
    for label in ["A", "B", "C"] {
        assert_eq!(
            compact.nodes_by_label(label).len(),
            1,
            "label {label} must resolve the node"
        );
    }
}

/// The eager lexicographic v5 reference payload for an input (D0.8.10 oracle).
fn eager_lexicographic_payload(input: &GenerationInput) -> Vec<u8> {
    use crate::graph::compact::generation::generate_compact_store;
    use crate::graph::compact::section_v5::{StringCodeOrder, serialize_v5_with_string_order};
    let budget = GenerationBudget::for_tests();
    let mut nodes = input.node_source();
    let mut edges = input.edge_source();
    let generated =
        generate_compact_store(&mut nodes, &mut edges, &input.rel_schemas, &budget).unwrap();
    serialize_v5_with_string_order(&generated.store, StringCodeOrder::Lexicographic).unwrap()
}

/// The bounded orchestrator payload for an input.
fn bounded_payload(input: &GenerationInput, temp: &std::path::Path) -> Vec<u8> {
    let mut store = InMemoryRunStore::new();
    let mut builder = BoundedGenerationBuilder::new(config(temp));
    let mut lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect("bounded build");
    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");
    payload
}

/// D0.8.10 core acceptance: the bounded orchestrator emits a v5 payload
/// **byte-identical** to the eager lexicographic reference for the dense
/// single-label domain (the reference's supported semantic domain).
///
/// The golden fixtures are frozen at SHA ca0d6069 (D0.8.10 #2: no
/// regenerating the oracle at test time). The test loads the committed
/// binary fixtures and compares against the bounded output.
#[test]
fn bounded_byte_parity_with_eager_lexicographic() {
    let tmp = TempDir::new().unwrap();
    let input = gem0_5b_parity_input();

    let bounded = bounded_payload(&input, tmp.path());
    let golden = golden_fixture("parity_dense");

    assert_eq!(
        golden.len(),
        bounded.len(),
        "payload length mismatch: golden {} vs bounded {}",
        golden.len(),
        bounded.len()
    );
    assert_eq!(
        golden, bounded,
        "bounded payload not byte-identical to frozen golden fixture (SHA ca0d6069)"
    );
}

/// The shared parity test input (3 Person nodes, 2 KNOWS edges, all with
/// properties). Matches the golden fixture generated at SHA ca0d6069.
#[allow(clippy::needless_pass_by_value)]
fn gem0_5b_parity_input() -> GenerationInput {
    GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "Person")
                .with_prop("name", "Alice")
                .with_prop("age", Value::Int64(30)),
        )
        .node(
            GenerationNode::new(2u64, "Person")
                .with_prop("name", "Bob")
                .with_prop("age", Value::Int64(25)),
        )
        .node(
            GenerationNode::new(3u64, "Person")
                .with_prop("name", "Carol")
                .with_prop("age", Value::Int64(40)),
        )
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "KNOWS"))
        .edge(GenerationEdge::new(11u64, 2u64, 3u64, "KNOWS"))
}

/// Loads a committed golden fixture from `tests/golden/gem0_5b/`.
fn golden_fixture(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/gem0_5b")
        .join(format!("{name}.v5"));
    std::fs::read(&path)
        .unwrap_or_else(|e| panic!("read golden fixture {}: {e}", path.display()))
}

/// One-time fixture generator: writes the golden payloads to
/// `tests/golden/gem0_5b/`. Run with `cargo test write_golden_fixtures
/// -- --ignored --nocapture` then commit the output. This test is
/// `#[ignore]` so it does NOT run in normal CI (D0.8.10 #2 forbids
/// regenerating the oracle at test time).
#[test]
#[ignore = "fixture generator: run manually, commit output (D0.8.10 #2)"]
fn write_golden_fixtures() {
    let input = gem0_5b_parity_input();
    let eager = eager_lexicographic_payload(&input);
    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/gem0_5b");
    std::fs::create_dir_all(&out_dir).expect("create golden dir");
    let path = out_dir.join("parity_dense.v5");
    std::fs::write(&path, &eager).expect("write golden fixture");
    eprintln!("wrote {} bytes to {}", eager.len(), path.display());
}

/// RAII: spool files and temp dir are cleaned up when the lease is dropped
/// after a successful build+stream (D0.8.10 #10: zero leftover artifacts).
#[test]
fn raii_cleanup_on_success() {
    let tmp = TempDir::new().unwrap();
    let build_tmp = tmp.path().join("build-tmp");
    let input = gem0_5b_parity_input();
    let mut store = InMemoryRunStore::new();
    let config = BoundedBuildConfig {
        budget: budget(),
        temp_dir: build_tmp.clone(),
        correlation_id: "raii-success".into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
    };
    let mut builder = BoundedGenerationBuilder::new(config);
    let mut lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect("build");

    // Stream the payload (consumes spool files).
    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");

    // Drop the lease → should clean up temp dir + spool files.
    drop(lease);

    // Verify the build-tmp directory is gone.
    assert!(
        !build_tmp.exists(),
        "build-tmp directory should be removed after lease drop"
    );
}

/// RAII: spool files and temp dir are cleaned up even when the lease is
/// dropped WITHOUT streaming (simulating a build error after lease creation).
#[test]
fn raii_cleanup_on_drop_without_stream() {
    let tmp = TempDir::new().unwrap();
    let build_tmp = tmp.path().join("build-tmp");
    let input = gem0_5b_parity_input();
    let mut store = InMemoryRunStore::new();
    let config = BoundedBuildConfig {
        budget: budget(),
        temp_dir: build_tmp.clone(),
        correlation_id: "raii-nostream".into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
    };
    let mut builder = BoundedGenerationBuilder::new(config);
    let lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect("build");
    drop(lease);
    assert!(
        !build_tmp.exists(),
        "build-tmp directory should be removed even without streaming"
    );
}

/// Counts regular files under `root` (recursive).
fn count_files_under(root: &std::path::Path) -> usize {
    if !root.exists() {
        return 0;
    }
    let mut count = 0;
    for entry in std::fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            count += count_files_under(&path);
        } else {
            count += 1;
        }
    }
    count
}

/// Asserts the job temp root and optional run root have zero leftover artifacts.
fn assert_zero_job_artifacts(build_tmp: &std::path::Path, build_runs: Option<&std::path::Path>) {
    assert!(
        !build_tmp.exists(),
        "build-tmp must be removed after failure, found: {:?}",
        build_tmp
    );
    if let Some(runs) = build_runs {
        assert_eq!(
            count_files_under(runs),
            0,
            "build-runs must have zero leftover files after failure: {}",
            runs.display()
        );
    }
}

fn large_parity_input() -> GenerationInput {
    let mut input = GenerationInput::new();
    for id in 1..=256u64 {
        input = input.node(
            GenerationNode::new(id, "Person")
                .with_prop("name", format!("node-{id}"))
                .with_prop("age", grafeo_common::types::Value::Int64((id % 100) as i64)),
        );
    }
    for id in 0..255u64 {
        input = input.edge(GenerationEdge::new(
            10_000 + id,
            id + 1,
            id + 2,
            "KNOWS",
        ));
    }
    input
}

/// Pre-lease failure (duplicate node id) must not orphan job temp artifacts
/// (spools, dictchunks.cat, id-index.bin).
#[test]
fn failure_before_payload_lease_cleans_job_temp() {
    let tmp = TempDir::new().unwrap();
    let build_tmp = tmp.path().join("build-tmp");
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person").with_prop("name", "Ada"))
        .node(GenerationNode::new(1u64, "Person").with_prop("name", "Dup"));
    let mut store = InMemoryRunStore::new();
    let config = BoundedBuildConfig {
        budget: budget(),
        temp_dir: build_tmp.clone(),
        correlation_id: "pre-lease-dup".into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
    };
    let mut builder = BoundedGenerationBuilder::new(config);
    let err = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect_err("duplicate node id must fail before payload lease");
    assert!(
        matches!(err, crate::graph::compact::generation::GenerationError::DuplicateNodeId(1)),
        "unexpected error: {err}"
    );
    assert_zero_job_artifacts(&build_tmp, None);
}

/// Tiny temp budget must fail mid-build and leave zero job temp artifacts.
#[test]
fn failure_tiny_budget_cleans_job_temp() {
    let tmp = TempDir::new().unwrap();
    let build_tmp = tmp.path().join("build-tmp");
    let input = large_parity_input();
    let mut store = InMemoryRunStore::new();
    let tiny = GenerationBudget {
        max_temp_bytes: 512,
        sort_run_bytes: 32,
        ..GenerationBudget::for_tests()
    };
    let config = BoundedBuildConfig {
        budget: tiny,
        temp_dir: build_tmp.clone(),
        correlation_id: "tiny-budget".into(),
        spool_buf_cap: 256,
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
    };
    let mut builder = BoundedGenerationBuilder::new(config);
    let err = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect_err("tiny budget must fail mid-build");
    assert!(
        matches!(
            err,
            crate::graph::compact::generation::GenerationError::BudgetExceeded { .. }
        ),
        "unexpected error: {err}"
    );
    assert_zero_job_artifacts(&build_tmp, None);
}

/// Cancellation mid-build must leave zero job temp artifacts.
#[test]
fn failure_cancel_mid_build_cleans_job_temp() {
    use crate::graph::compact::generation::CancelToken;
    use std::thread;
    use std::time::Duration;

    let tmp = TempDir::new().unwrap();
    let build_tmp = tmp.path().join("build-tmp");
    let input = large_parity_input();
    let mut store = InMemoryRunStore::new();
    let token = CancelToken::new();
    let cancel = token.clone();
    let killer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(1));
        cancel.cancel();
    });
    let config = BoundedBuildConfig {
        budget: budget(),
        temp_dir: build_tmp.clone(),
        correlation_id: "cancel-mid".into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
    };
    let mut builder = BoundedGenerationBuilder::new(config).with_cancel(token);
    let err = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect_err("cancel mid-build must fail");
    killer.join().unwrap();
    assert!(
        matches!(
            err,
            crate::graph::compact::generation::GenerationError::Cancelled
        ),
        "unexpected error: {err}"
    );
    assert_zero_job_artifacts(&build_tmp, None);
}
