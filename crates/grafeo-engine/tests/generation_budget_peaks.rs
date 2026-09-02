//! G-FRZ.1 — `BuildPublication` carries measured budget peaks.
//!
//! Proves the streaming engine path surfaces the build's real peak charges:
//! the publication's `budget_peaks` must be non-zero (the orchestrator charges
//! anonymous read buffers for any build) and must stay within the request
//! budget on both axes. Zero peaks would mean the metrics tally never reached
//! the publication (the G-FRZ.1 plumbing gap this packet closes).

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "compact-store",
    feature = "lpg",
    feature = "mmap"
))]

use grafeo_common::types::Value;
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::TempDir;

/// Populate the DB with `n` labeled node pairs + one `KNOWS` edge each, so the
/// streaming orchestrator has real work that charges anon buffers (records
/// pass through the budget ledger before any spill).
fn populate(db: &GrafeoDB, tag: &str, n: usize) {
    for i in 0..n {
        let a = db
            .create_node_with_props(&["Person"], [("name", Value::from(format!("{tag}-a-{i}")))])
            .expect("node a");
        let b = db
            .create_node_with_props(&["Person"], [("name", Value::from(format!("{tag}-b-{i}")))])
            .expect("node b");
        let _e = db.create_edge(a, b, "KNOWS");
    }
}

/// A small streaming build reports measured (non-zero) peaks inside budget.
#[test]
fn streaming_build_publication_carries_measured_peaks() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "peaks", 64);

    let request = generation_build_request(&gen_root, "g-frz-peaks");
    // `GenerationBudget` is Copy — snapshot the limits for assertions.
    let budget = request.budget;
    let publication = db
        .build_and_publish_generation(request)
        .expect("publish generation");

    let peaks = publication.budget_peaks;
    // The orchestrator charges anonymous working-set for its read buffers, so
    // a real measured build must report a positive anon peak. Zero would mean
    // the lease metrics never reached the publication (G-FRZ.1 default path).
    assert!(
        peaks.anon_bytes_peak > 0,
        "streaming build must charge anonymous buffers (got 0)"
    );
    // Both axes stay within the safety budget (the fail-closed property).
    assert!(
        peaks.anon_bytes_peak <= budget.max_anon_bytes,
        "anon peak {} exceeds budget {}",
        peaks.anon_bytes_peak,
        budget.max_anon_bytes
    );
    assert!(
        peaks.temp_bytes_peak <= budget.max_temp_bytes,
        "temp peak {} exceeds budget {}",
        peaks.temp_bytes_peak,
        budget.max_temp_bytes
    );
}
