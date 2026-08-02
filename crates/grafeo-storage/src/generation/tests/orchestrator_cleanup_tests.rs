//! Bounded orchestrator failure cleanup with disk-backed runs (D0.8.10).

#![cfg(feature = "generation-streaming")]

use grafeo_core::graph::compact::generation::{
    CancelToken, GenerationBudget, GenerationEdge, GenerationInput, GenerationNode,
};
use grafeo_core::graph::compact::generation_builder::orchestrator::{
    BoundedBuildConfig, BoundedGenerationBuilder,
};
use tempfile::TempDir;

use super::support::count_files_under;
use crate::generation::DiskRunStore;

fn large_input() -> GenerationInput {
    let mut input = GenerationInput::new();
    for id in 1..=128u64 {
        input = input.node(
            GenerationNode::new(id, "Person").with_prop("name", format!("node-{id}")),
        );
    }
    for id in 0..127u64 {
        input = input.edge(GenerationEdge::new(10_000 + id, id + 1, id + 2, "KNOWS"));
    }
    input
}

#[test]
fn disk_run_store_failure_leaves_zero_artifacts() {
    let tmp = TempDir::new().unwrap();
    let build_tmp = tmp.path().join("build-tmp");
    let build_runs = tmp.path().join("build-runs");
    let budget = GenerationBudget {
        max_temp_bytes: 512,
        sort_run_bytes: 32,
        ..GenerationBudget::for_tests()
    };
    let config = BoundedBuildConfig {
        budget,
        temp_dir: build_tmp.clone(),
        correlation_id: "disk-fail".into(),
        spool_buf_cap: 256,
        rel_schemas: Vec::new(),
    };
    let mut run_store =
        DiskRunStore::new(&build_runs, budget, "disk-fail").expect("run store");
    let input = large_input();
    let mut builder = BoundedGenerationBuilder::new(config);
    let err = builder
        .build(
            &mut input.node_source(),
            &mut input.edge_source(),
            &mut run_store,
        )
        .expect_err("tiny budget must fail");
    assert!(
        matches!(
            err,
            grafeo_core::graph::compact::generation::GenerationError::BudgetExceeded { .. }
        ),
        "unexpected error: {err}"
    );
    assert!(
        !build_tmp.exists(),
        "build-tmp must be removed after failure"
    );
    assert_eq!(
        count_files_under(&build_runs),
        0,
        "build-runs must have zero leftover files"
    );
}

#[test]
fn disk_run_store_cancel_leaves_zero_artifacts() {
    let tmp = TempDir::new().unwrap();
    let build_tmp = tmp.path().join("build-tmp");
    let build_runs = tmp.path().join("build-runs");
    let budget = GenerationBudget::for_tests();
    let config = BoundedBuildConfig {
        budget,
        temp_dir: build_tmp.clone(),
        correlation_id: "disk-cancel".into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: Vec::new(),
    };
    let mut run_store =
        DiskRunStore::new(&build_runs, budget, "disk-cancel").expect("run store");
    let token = CancelToken::new();
    token.cancel();
    let input = large_input();
    let mut builder = BoundedGenerationBuilder::new(config).with_cancel(token);
    let err = builder
        .build(
            &mut input.node_source(),
            &mut input.edge_source(),
            &mut run_store,
        )
        .expect_err("cancel must fail before payload lease");
    assert!(
        matches!(
            err,
            grafeo_core::graph::compact::generation::GenerationError::Cancelled
        ),
        "unexpected error: {err}"
    );
    assert!(!build_tmp.exists(), "build-tmp must be removed after cancel");
    assert_eq!(
        count_files_under(&build_runs),
        0,
        "build-runs must have zero leftover files after cancel"
    );
}
