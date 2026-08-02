//! Disk-backed `RunStore` adapter lifecycle tests (G-EM0.5b D0.8.1).
//!
//! Proves the artifact-ownership contract: a disk run must safely outlive
//! its sink (via the RAII [`RunSetLease`]), the merge consumes live files,
//! and dropping the lease cleans the job scope on success, error, and
//! abandon.

use grafeo_core::graph::compact::generation::{GenerationBudget, RunStore, SortRecord};
use tempfile::TempDir;

use super::super::run_adapter::DiskRunStore;

fn budget() -> GenerationBudget {
    // Small run cap so a handful of records spills to multiple disk runs.
    let mut b = GenerationBudget::for_tests();
    b.sort_run_bytes = 128;
    b
}

fn rec(i: u64) -> SortRecord {
    SortRecord::new(i.to_be_bytes().to_vec(), vec![0u8; 16])
}

#[test]
fn disk_run_outlives_sink_and_merges() {
    let tmp = TempDir::new().unwrap();
    let mut store = DiskRunStore::new(tmp.path(), budget(), "job1").unwrap();

    // Stage ~30 records into several disk runs, then finish → lease.
    let lease = {
        let mut sink = store.sink("nodes", &budget()).unwrap();
        for i in 0..30u64 {
            sink.push(rec(i)).unwrap();
        }
        sink.finish().unwrap()
        // sink is consumed by finish; the lease owns the live DiskRunSink.
    };
    assert!(!lease.is_empty(), "runs must be flushed to disk");
    // Files still exist (lease holds them) — mergeable.
    let mut merged = Vec::new();
    let mut metrics = grafeo_core::graph::compact::generation::GenerationMetrics::default();
    let mut merger = store.merger("nodes").unwrap();
    merger
        .merge_all(&lease.handles, &budget(), &mut metrics, None, &mut |r| {
            merged.push(r.key.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(merged.len(), 30, "all records merged from live disk runs");
    // Sorted ascending.
    assert!(merged.windows(2).all(|w| w[1] >= w[0]));
    drop(lease);
}

#[test]
fn lease_drop_cleans_run_files() {
    let tmp = TempDir::new().unwrap();
    let job = tmp.path().join("job2");
    let mut store = DiskRunStore::new(&job, budget(), "job2").unwrap();
    let lease = {
        let mut sink = store.sink("edges", &budget()).unwrap();
        for i in 0..20u64 {
            sink.push(rec(i)).unwrap();
        }
        sink.finish().unwrap()
    };
    let paths: Vec<_> = lease
        .handles
        .iter()
        .map(|h| std::path::PathBuf::from(&h.id))
        .collect();
    assert!(
        paths.iter().all(|p| p.exists()),
        "run files live under lease"
    );
    drop(lease);
    // After lease drop, the owned DiskRunSink's Drop deleted the files.
    assert!(
        paths.iter().all(|p| !p.exists()),
        "run files removed on lease drop"
    );
}

#[test]
fn double_finish_fails_closed() {
    let tmp = TempDir::new().unwrap();
    let mut store = DiskRunStore::new(tmp.path(), budget(), "job3").unwrap();
    let mut sink = store.sink("nodes", &budget()).unwrap();
    sink.push(rec(1)).unwrap();
    let _lease = sink.finish().unwrap();
    let err = sink.finish().unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("finish"),
        "double finish must fail closed: {msg}"
    );
}
