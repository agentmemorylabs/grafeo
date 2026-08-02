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

// ── R2 adversarial tests ──────────────────────────────────────────────────

/// R2-B1: effective flush threshold must equal configured `sort_run_bytes`,
/// NOT `sort_run_bytes / 2`. With a tiny budget, push records and assert no
/// flush fires while the arena charge is below `sort_run_bytes` (i.e. the
/// threshold was not halved).
#[test]
fn r2_b1_flush_threshold_equals_configured_sort_run_bytes() {
    let dir = tempdir().unwrap();
    // sort_run_bytes = 512. Each record charges ~64 bytes (48 struct + 16
    // heap). After 4 pushes the charge is ~448 (below 512). With the OLD
    // halved threshold (256), a flush would fire at push 1 (charge 320 > 256).
    // With the TRUE threshold (512), no flush fires until push 4 (charge 704).
    let budget = small_budget(512, 4);
    let ledger = job_ledger();
    let mut sink =
        DiskRunSink::new(dir.path().join("runs"), budget, "b1", Arc::clone(&ledger)).unwrap();

    let mut flush_count_at_4 = 0u64;
    for i in 0..8u32 {
        sink.push(FramedRecord::new(
            format!("{i:08}").into_bytes(),
            vec![i as u8; 8],
        ))
        .unwrap();
        // After 4 pushes the arena charge is ~448 (below 512, above 256).
        // With the halved threshold a flush would have fired by now.
        if i == 3 {
            flush_count_at_4 = sink.metrics().run_count;
        }
    }
    let runs = sink.finish().unwrap();

    // With the TRUE 512 threshold, no flush should have fired by push 3
    // (arena charge ~448 < 512). With the halved 256 threshold, at least
    // one flush would have fired by push 1.
    assert_eq!(
        flush_count_at_4, 0,
        "R2-B1: flush fired at ~448 bytes with sort_run_bytes=512 — threshold was halved!"
    );
    assert!(
        runs.len() <= 3,
        "R2-B1: too many runs ({}) — threshold may be halved",
        runs.len()
    );
    sink.cleanup();
}

/// R2-M3: a budget-exceeded error must occur BEFORE `Vec::reserve` grows
/// anonymous memory. With a tiny anon limit, the first push that would
/// exceed the limit must fail at the pre-charge step, not post-reserve.
#[test]
fn r2_m3_admission_fails_before_vec_reserve() {
    let dir = tempdir().unwrap();
    // Tiny anon limit: 100 bytes. First record's predicted growth will
    // exceed this, so the pre-charge must reject it.
    let budget = ExternalSortBudget {
        max_anon_bytes: 100,
        max_temp_bytes: 64 * 1024 * 1024,
        sort_run_bytes: 64 * 1024,
        io_buffer_bytes: 4096,
        merge_fan_in: 4,
        max_record_bytes: 8 * 1024 * 1024,
    };
    let ledger = Arc::new(JobAnonLedger::new(100));
    let mut sink =
        DiskRunSink::new(dir.path().join("runs"), budget, "m3", Arc::clone(&ledger)).unwrap();

    // The first push should fail at pre-charge (predicted growth > 100).
    let result = sink.push(FramedRecord::new(b"key".to_vec(), b"payload".to_vec()));
    assert!(
        result.is_err(),
        "R2-M3: push should fail at pre-charge with 100-byte anon limit"
    );
    // The ledger must NOT have been charged (pre-charge failed).
    assert_eq!(
        ledger.current(),
        0,
        "R2-M3: ledger charged despite pre-charge failure"
    );
    sink.cleanup();
}

/// R2-M2: merge heap seed/refill must admit BEFORE allocating record Vecs.
/// With a tiny anon limit that allows I/O buffers but not record heap, the
/// merge must fail at admission, not after allocation.
#[test]
fn r2_m2_merge_admits_before_allocation() {
    let dir = tempdir().unwrap();
    let budget = small_budget(1024, 4);
    let ledger = Arc::new(JobAnonLedger::new(64 * 1024 * 1024));

    // First, create a run with some records.
    let mut sink =
        DiskRunSink::new(dir.path().join("runs"), budget, "m2", Arc::clone(&ledger)).unwrap();
    for i in 0..5u32 {
        sink.push(FramedRecord::new(
            format!("{i:04}").into_bytes(),
            vec![i as u8],
        ))
        .unwrap();
    }
    let runs = sink.finish().unwrap();
    sink.cleanup();

    // Now merge with a ledger that has only enough for I/O buffers but NOT
    // for record heap. I/O = 2 * io_buffer_bytes (1 reader + 1 writer).
    // With io_buffer_bytes=4096, I/O = 8192. Set limit to 8192 + 1 so I/O
    // fits but the first record's heap (key+payload ~12 bytes) does not.
    let tight_ledger = Arc::new(JobAnonLedger::new(8192 + 1));
    let mut merger = DiskRunMerger::new(
        dir.path().join("merge"),
        budget,
        "m2",
        Arc::clone(&tight_ledger),
    )
    .unwrap();
    let mut metrics = ExternalSortMetrics::default();
    let result = merge_runs_recursive(&mut merger, &runs, &mut metrics, None, &mut |_| Ok(()));
    // The merge should fail because the record heap cannot be admitted.
    assert!(
        result.is_err(),
        "R2-M2: merge should fail when record heap cannot be admitted"
    );
    // The tight ledger must be back to zero (I/O guard dropped on error).
    assert_eq!(
        tight_ledger.current(),
        0,
        "R2-M2: ledger not reconciled after merge failure"
    );
    merger.cleanup();
}

/// R2-M5 (storage-side): after a successful sink+merge cycle, the shared
/// job ledger must have zero current charges.
#[test]
fn r2_m5_storage_ledger_zero_after_success() {
    let dir = tempdir().unwrap();
    let budget = small_budget(1024, 4);
    let ledger = job_ledger();
    let mut sink =
        DiskRunSink::new(dir.path().join("runs"), budget, "m5", Arc::clone(&ledger)).unwrap();
    for i in 0..10u32 {
        sink.push(FramedRecord::new(
            format!("{i:04}").into_bytes(),
            vec![i as u8],
        ))
        .unwrap();
    }
    let runs = sink.finish().unwrap();

    let mut merger =
        DiskRunMerger::new(dir.path().join("merge"), budget, "m5", Arc::clone(&ledger)).unwrap();
    let mut metrics = ExternalSortMetrics::default();
    merge_runs_recursive(&mut merger, &runs, &mut metrics, None, &mut |_| Ok(())).unwrap();

    // After success, the shared ledger must be zero.
    assert_eq!(
        ledger.current(),
        0,
        "R2-M5: shared ledger has nonzero current after successful merge"
    );
    sink.cleanup();
    merger.cleanup();
}

/// R2-M5 (storage-side): after a cancelled merge, the shared ledger must
/// have zero current charges.
#[test]
fn r2_m5_storage_ledger_zero_after_cancel() {
    let dir = tempdir().unwrap();
    let budget = small_budget(1024, 4);
    let ledger = job_ledger();
    let mut sink =
        DiskRunSink::new(dir.path().join("runs"), budget, "m5c", Arc::clone(&ledger)).unwrap();
    for i in 0..10u32 {
        sink.push(FramedRecord::new(
            format!("{i:04}").into_bytes(),
            vec![i as u8],
        ))
        .unwrap();
    }
    let runs = sink.finish().unwrap();

    let cancel = CancelToken::new();
    cancel.cancel(); // cancel before merge starts

    let mut merger =
        DiskRunMerger::new(dir.path().join("merge"), budget, "m5c", Arc::clone(&ledger)).unwrap();
    let mut metrics = ExternalSortMetrics::default();
    let result = merge_runs_recursive(&mut merger, &runs, &mut metrics, Some(&cancel), &mut |_| {
        Ok(())
    });
    assert!(result.is_err(), "cancelled merge must fail");

    // After cancellation, the shared ledger must be zero.
    assert_eq!(
        ledger.current(),
        0,
        "R2-M5: shared ledger has nonzero current after cancelled merge"
    );
    sink.cleanup();
    merger.cleanup();
}
