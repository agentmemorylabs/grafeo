//! G-EM0.5b D0.8.10 — DiskRunStore byte parity and two-job determinism.
//!
//! Proves the production `DiskRunStore` path emits payloads byte-identical to
//! the committed golden fixture and that two independent job directories
//! produce identical payload bytes and CRC32.

#![cfg(feature = "generation-streaming")]

use grafeo_core::graph::compact::generation::{
    GenerationBudget, GenerationEdge, GenerationInput, GenerationNode,
};
use grafeo_core::graph::compact::generation_builder::orchestrator::{
    BoundedBuildConfig, BoundedGenerationBuilder,
};
use grafeo_storage::generation::run_adapter::DiskRunStore;
use tempfile::TempDir;

fn budget() -> GenerationBudget {
    GenerationBudget::for_tests()
}

fn gem0_5b_parity_input() -> GenerationInput {
    GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "Person")
                .with_prop("name", "Alice")
                .with_prop("age", grafeo_common::types::Value::Int64(30)),
        )
        .node(
            GenerationNode::new(2u64, "Person")
                .with_prop("name", "Bob")
                .with_prop("age", grafeo_common::types::Value::Int64(25)),
        )
        .node(
            GenerationNode::new(3u64, "Person")
                .with_prop("name", "Carol")
                .with_prop("age", grafeo_common::types::Value::Int64(40)),
        )
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "KNOWS"))
        .edge(GenerationEdge::new(11u64, 2u64, 3u64, "KNOWS"))
}

fn golden_fixture(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../grafeo-core/tests/golden/gem0_5b")
        .join(format!("{name}.v5"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("read golden fixture {}: {e}", path.display()))
}

fn disk_run_store_payload(input: &GenerationInput, tmp: &TempDir, job: &str) -> Vec<u8> {
    let build_tmp = tmp.path().join(format!("{job}-tmp"));
    let runs_dir = tmp.path().join(format!("{job}-runs"));
    let mut run_store = DiskRunStore::new(&runs_dir, budget(), job).expect("DiskRunStore");
    let config = BoundedBuildConfig {
        budget: budget(),
        temp_dir: build_tmp,
        correlation_id: job.into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
    };
    let mut builder = BoundedGenerationBuilder::new(config);
    let mut lease = builder
        .build(
            &mut input.node_source(),
            &mut input.edge_source(),
            &mut run_store,
        )
        .expect("bounded build");
    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");
    drop(lease);
    drop(run_store);
    payload
}

#[test]
fn disk_run_store_byte_parity_with_golden() {
    let input = gem0_5b_parity_input();
    let tmp = TempDir::new().unwrap();
    let bounded = disk_run_store_payload(&input, &tmp, "parity-a");
    let golden = golden_fixture("parity_dense");

    assert_eq!(golden.len(), bounded.len(), "payload length mismatch");
    assert_eq!(
        golden, bounded,
        "DiskRunStore payload != golden parity_dense"
    );
}

#[test]
fn disk_run_store_two_job_determinism() {
    let input = gem0_5b_parity_input();
    let tmp = TempDir::new().unwrap();
    let payload_a = disk_run_store_payload(&input, &tmp, "job-a");
    let payload_b = disk_run_store_payload(&input, &tmp, "job-b");

    assert_eq!(
        payload_a, payload_b,
        "two job dirs must yield identical payloads"
    );

    let hash_a = crc32fast::hash(&payload_a);
    let hash_b = crc32fast::hash(&payload_b);
    assert_eq!(hash_a, hash_b, "payload CRC32 must match across jobs");
    eprintln!("payload CRC32={hash_a:#010x} len={}", payload_a.len());
}
