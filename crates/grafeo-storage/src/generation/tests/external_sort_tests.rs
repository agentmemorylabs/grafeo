//! External sort tests (W0-A1).
//!
//! Covers: run write, merge, recursive fan-in, cancellation cleanup,
//! budget exhaustion, deterministic output, temp accounting honesty,
//! merge peak includes both input and output sides.

#![allow(clippy::cast_possible_truncation)]

use crate::generation::budget::ExternalSortBudget;
use crate::generation::external_sort::{
    CancelToken, DiskRunMerger, DiskRunSink, merge_runs_recursive,
};
use crate::generation::metrics::{ExternalSortMetrics, ExternalSortMetricsError, JobAnonLedger};
use crate::generation::records::FramedRecord;
use std::sync::Arc;
use tempfile::tempdir;

/// Fresh shared job anon ledger for test sinks (64 MiB default limit).
fn job_ledger() -> Arc<JobAnonLedger> {
    Arc::new(JobAnonLedger::new(64 * 1024 * 1024))
}

/// Create a budget with small run size to force multiple runs.
fn small_budget(sort_run_bytes: u64, fan_in: u32) -> ExternalSortBudget {
    ExternalSortBudget {
        sort_run_bytes,
        max_temp_bytes: 64 * 1024 * 1024,
        merge_fan_in: fan_in,
        ..ExternalSortBudget::for_tests()
    }
}

#[test]
fn run_write_and_merge_basic() {
    let dir = tempdir().unwrap();
    let budget = small_budget(1024, 4);
    let mut sink =
        DiskRunSink::new(dir.path().join("runs"), budget, "basic", job_ledger()).unwrap();
    for i in (0..20u32).rev() {
        sink.push(FramedRecord::new(
            format!("{i:04}").into_bytes(),
            vec![i as u8],
        ))
        .unwrap();
    }
    let runs = sink.finish().unwrap();
    assert!(!runs.is_empty());

    let mut merger =
        DiskRunMerger::new(dir.path().join("merge"), budget, "basic", job_ledger()).unwrap();
    let mut metrics = ExternalSortMetrics::default();
    let mut out = Vec::new();
    merge_runs_recursive(&mut merger, &runs, &mut metrics, None, &mut |r| {
        out.push(r.clone());
        Ok(())
    })
    .unwrap();

    assert_eq!(out.len(), 20);
    // Verify sorted order
    for w in out.windows(2) {
        assert!(
            w[0].key <= w[1].key,
            "unsorted: {:?} > {:?}",
            w[0].key,
            w[1].key
        );
    }
    sink.cleanup();
    merger.cleanup();
}

#[test]
fn recursive_fan_in_triggers_merge_passes() {
    // 200 runs with fan_in=32 → multiple recursive merge passes.
    let dir = tempdir().unwrap();
    let budget = small_budget(48, 32);
    let mut sink = DiskRunSink::new(dir.path().join("runs"), budget, "fan", job_ledger()).unwrap();
    for i in (0..200u32).rev() {
        let key = format!("{i:08}").into_bytes();
        sink.push(FramedRecord::new(key, vec![i as u8])).unwrap();
    }
    let runs = sink.finish().unwrap();
    assert!(
        runs.len() > 32,
        "expected >32 runs for recursive merge, got {}",
        runs.len()
    );

    let mut merger =
        DiskRunMerger::new(dir.path().join("merge"), budget, "fan", job_ledger()).unwrap();
    let mut metrics = ExternalSortMetrics::default();
    let mut out = Vec::new();
    merge_runs_recursive(&mut merger, &runs, &mut metrics, None, &mut |r| {
        out.push(r.clone());
        Ok(())
    })
    .unwrap();

    assert_eq!(out.len(), 200);
    assert!(
        metrics.merge_passes >= 2,
        "expected recursive passes, got {}",
        metrics.merge_passes
    );
    // Verify fully sorted
    for w in out.windows(2) {
        assert!(w[0].key <= w[1].key);
    }
    sink.cleanup();
    merger.cleanup();
}

#[test]
fn cancel_mid_merge_leaves_no_temp_files() {
    let dir = tempdir().unwrap();
    let runs_dir = dir.path().join("runs");
    let merge_dir = dir.path().join("merge");
    let budget = small_budget(48, 2);
    let mut sink = DiskRunSink::new(runs_dir.clone(), budget, "cx", job_ledger()).unwrap();
    for i in (0..30u32).rev() {
        sink.push(FramedRecord::new(
            format!("{i:04}").into_bytes(),
            vec![i as u8],
        ))
        .unwrap();
    }
    let runs = sink.finish().unwrap();

    let token = CancelToken::new();
    let mut merger = DiskRunMerger::new(merge_dir.clone(), budget, "cx", job_ledger()).unwrap();
    let mut metrics = ExternalSortMetrics::default();

    // Cancel immediately — should fail before or during the first emit.
    token.cancel();
    let result = merge_runs_recursive(&mut merger, &runs, &mut metrics, Some(&token), &mut |_| {
        Ok(())
    });
    assert!(result.is_err());

    sink.cleanup();
    merger.cleanup();

    // Runs directory should be empty after cleanup.
    let run_leftovers: Vec<_> = std::fs::read_dir(&runs_dir)
        .map(|rd| rd.filter_map(|e| e.ok()).collect())
        .unwrap_or_default();
    assert!(
        run_leftovers.is_empty(),
        "run files left after cleanup: {run_leftovers:?}"
    );
}

#[test]
fn cancellation_during_push() {
    let dir = tempdir().unwrap();
    let budget = small_budget(32, 2);
    let token = CancelToken::new();
    let mut sink = DiskRunSink::new(dir.path().join("runs"), budget, "cp", job_ledger())
        .unwrap()
        .with_cancel(token.clone());
    sink.push(FramedRecord::new(b"a", b"1")).unwrap();
    token.cancel();
    let result = sink.push(FramedRecord::new(b"b", b"2"));
    assert!(result.is_err());
    sink.cleanup();
}

#[test]
fn temp_disk_budget_rejection() {
    let dir = tempdir().unwrap();
    let budget = ExternalSortBudget {
        sort_run_bytes: 24,
        max_temp_bytes: 80,
        merge_fan_in: 4,
        ..ExternalSortBudget::for_tests()
    };
    let mut sink = DiskRunSink::new(dir.path().join("runs"), budget, "tb", job_ledger()).unwrap();
    let mut hit_budget_error = false;
    for i in 0..40u32 {
        let key = format!("{i:08}").into_bytes();
        match sink.push(FramedRecord::new(key, vec![0u8; 8])) {
            Ok(()) => {}
            Err(ExternalSortMetricsError::BudgetExceeded { .. }) => {
                hit_budget_error = true;
                break;
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    assert!(hit_budget_error, "expected budget exhaustion");
    sink.cleanup();
}

#[test]
fn rejected_reservation_leaves_no_untracked_file() {
    // Reserve-before-write: when a run flush is rejected, no file is left.
    let dir = tempdir().unwrap();
    let runs_dir = dir.path().join("runs");
    let budget = ExternalSortBudget {
        sort_run_bytes: 24,
        max_temp_bytes: 80,
        merge_fan_in: 4,
        ..ExternalSortBudget::for_tests()
    };
    let mut sink = DiskRunSink::new(runs_dir.clone(), budget, "rb", job_ledger()).unwrap();
    let mut rejected = false;
    for i in 0..40u32 {
        let key = format!("{i:08}").into_bytes();
        if sink.push(FramedRecord::new(key, vec![0u8; 8])).is_err() {
            rejected = true;
            break;
        }
    }
    assert!(rejected, "expected temp-budget rejection");

    // Every live file must be accounted for.
    let live_files: u64 =
        std::fs::read_dir(&runs_dir).map_or(0, |rd| rd.filter_map(|e| e.ok()).count() as u64);
    let charged_runs = sink.metrics().run_count;
    assert_eq!(
        live_files, charged_runs,
        "live files must equal accounted runs (no untracked file)"
    );
    sink.cleanup();
    let after: u64 =
        std::fs::read_dir(&runs_dir).map_or(0, |rd| rd.filter_map(|e| e.ok()).count() as u64);
    assert_eq!(after, 0, "cleanup must remove all run files");
}

#[test]
fn deterministic_output_across_runs() {
    // Two identical input sets (pushed in reverse) should produce
    // byte-identical merged output.
    let make = || {
        let dir = tempdir().unwrap();
        let budget = small_budget(48, 4);
        let mut sink =
            DiskRunSink::new(dir.path().join("runs"), budget, "det", job_ledger()).unwrap();
        for i in (0..20u32).rev() {
            sink.push(FramedRecord::new(
                format!("{i:04}").into_bytes(),
                vec![i as u8],
            ))
            .unwrap();
        }
        let runs = sink.finish().unwrap();
        let mut merger =
            DiskRunMerger::new(dir.path().join("merge"), budget, "det", job_ledger()).unwrap();
        let mut metrics = ExternalSortMetrics::default();
        let mut out = Vec::new();
        merge_runs_recursive(&mut merger, &runs, &mut metrics, None, &mut |r| {
            out.push(r.clone());
            Ok(())
        })
        .unwrap();
        sink.cleanup();
        merger.cleanup();
        // dir must stay alive for tempdir lifetime
        std::mem::forget(dir);
        out
    };

    let out1 = make();
    let out2 = make();
    assert_eq!(out1.len(), out2.len());
    assert_eq!(out1, out2, "outputs must be byte-identical");
}

#[test]
fn merge_peak_includes_both_input_and_output() {
    // During a merge pass, peak temp must include both input and output
    // (sync-before-delete), not release inputs before output is charged.
    let dir = tempdir().unwrap();
    let budget = ExternalSortBudget {
        sort_run_bytes: 48,
        merge_fan_in: 2,
        max_temp_bytes: 64 * 1024 * 1024,
        ..ExternalSortBudget::for_tests()
    };
    let mut sink = DiskRunSink::new(dir.path().join("runs"), budget, "pk", job_ledger()).unwrap();
    for i in (0..30u32).rev() {
        sink.push(FramedRecord::new(
            format!("{i:04}").into_bytes(),
            vec![i as u8],
        ))
        .unwrap();
    }
    let runs = sink.finish().unwrap();
    assert!(runs.len() > 2, "need multiple runs");
    let input_bytes: u64 = runs.iter().map(|r| r.byte_len).sum();

    let mut merger =
        DiskRunMerger::new(dir.path().join("merge"), budget, "pk", job_ledger()).unwrap();
    let mut metrics = ExternalSortMetrics::default();
    // Seed merger metrics with the sink's live input charge so peak reflects
    // concurrent input + output, matching the packet formula.
    metrics.temp_bytes_current = sink.metrics().temp_bytes_current;
    metrics.temp_bytes_peak = sink.metrics().temp_bytes_peak;
    let mut out = Vec::new();
    merge_runs_recursive(&mut merger, &runs, &mut metrics, None, &mut |r| {
        out.push(r.clone());
        Ok(())
    })
    .unwrap();
    assert_eq!(out.len(), 30);
    assert!(
        metrics.temp_bytes_peak > input_bytes,
        "merge peak {} must exceed input-only bytes {} (both sides charged)",
        metrics.temp_bytes_peak,
        input_bytes
    );
    sink.cleanup();
    merger.cleanup();
}

#[test]
fn peak_temp_matches_measured_file_sizes() {
    // After producing runs, the temp_bytes_peak should not exceed the sum
    // of all run file sizes on disk (measured via fs::metadata).
    let dir = tempdir().unwrap();
    let runs_dir = dir.path().join("runs");
    let budget = small_budget(64, 4);
    let mut sink = DiskRunSink::new(runs_dir.clone(), budget, "mc", job_ledger()).unwrap();
    for i in (0..50u32).rev() {
        sink.push(FramedRecord::new(
            format!("{i:08}").into_bytes(),
            vec![i as u8],
        ))
        .unwrap();
    }
    let runs = sink.finish().unwrap();

    let measured: u64 = runs
        .iter()
        .filter_map(|r| std::fs::metadata(&r.path).ok().map(|m| m.len()))
        .sum();
    let peak = sink.metrics().temp_bytes_peak;
    assert!(
        peak >= measured,
        "peak temp {} should be >= measured file sizes {}",
        peak,
        measured
    );
    // Peak shouldn't be wildly larger than measured (no over-counting).
    assert!(
        peak <= measured * 2,
        "peak temp {} wildly exceeds measured {}",
        peak,
        measured
    );
    sink.cleanup();
}

#[test]
fn empty_input_merges_cleanly() {
    let dir = tempdir().unwrap();
    let budget = small_budget(64, 4);
    let mut merger =
        DiskRunMerger::new(dir.path().join("merge"), budget, "empty", job_ledger()).unwrap();
    let mut metrics = ExternalSortMetrics::default();
    let mut out = Vec::new();
    merge_runs_recursive(&mut merger, &[], &mut metrics, None, &mut |r| {
        out.push(r.clone());
        Ok(())
    })
    .unwrap();
    assert!(out.is_empty());
    merger.cleanup();
}

#[test]
fn single_run_no_merge_needed() {
    let dir = tempdir().unwrap();
    let budget = small_budget(1024, 4); // large arena → single run
    let mut sink = DiskRunSink::new(dir.path().join("runs"), budget, "one", job_ledger()).unwrap();
    for i in (0..5u32).rev() {
        sink.push(FramedRecord::new(
            format!("{i:04}").into_bytes(),
            vec![i as u8],
        ))
        .unwrap();
    }
    let runs = sink.finish().unwrap();
    assert_eq!(runs.len(), 1, "expected single run");

    let mut merger =
        DiskRunMerger::new(dir.path().join("merge"), budget, "one", job_ledger()).unwrap();
    let mut metrics = ExternalSortMetrics::default();
    let mut out = Vec::new();
    merge_runs_recursive(&mut merger, &runs, &mut metrics, None, &mut |r| {
        out.push(r.clone());
        Ok(())
    })
    .unwrap();
    assert_eq!(out.len(), 5);
    assert_eq!(metrics.merge_passes, 1, "single-run final emit is one pass");
    sink.cleanup();
    merger.cleanup();
}
