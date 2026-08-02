//! Bounded external run generation + recursive fan-in k-way merge
//! (G-EM0.W0-A1, W0 contract §8).
//!
//! ## Design
//!
//! - **DiskRunSink**: accumulates records in a bounded in-memory arena
//!   (`sort_run_bytes`). When the arena fills, it sorts, reserves the run's
//!   bytes against `max_temp_bytes`, writes + syncs, and records a
//!   [`RunHandle`]. Reservation happens **before** the file is created, so a
//!   rejected reservation leaves no untracked file.
//! - **merge_runs_recursive**: uses a min-heap of run heads (one buffer per
//!   run). When run count exceeds `merge_fan_in`, performs recursive merge
//!   passes — intermediate files are synced before consumed inputs are
//!   deleted and released, so the peak charge includes both sides.
//! - **Cancellation**: checked during push, flush, merge, and emit. Cleanup
//!   drops all readers before unlinking files (Windows-portable).
//!
//! Ported from the failed Stage A candidate after RED tests proved the
//! mechanics sound. No graph semantics, no v5 codecs, no wire aliases.

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use super::budget::ExternalSortBudget;
use super::merge::{kway_merge_emit, kway_merge_to_file};
use super::metrics::{
    AnonLedgerError, AnonReservation, ExternalSortMetrics, ExternalSortMetricsError, JobAnonLedger,
};
use super::records::FramedRecord;

/// Shared cancellation token (thread-safe).
#[derive(Debug, Clone, Default)]
pub struct CancelToken {
    flag: Arc<AtomicBool>,
}

impl CancelToken {
    /// Fresh uncancelled token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Shares an existing atomic flag (bridges core ↔ storage cancel tokens).
    #[must_use]
    pub fn from_shared(flag: Arc<AtomicBool>) -> Self {
        Self { flag }
    }

    /// Request cancellation.
    pub fn cancel(&self) {
        self.flag.store(true, AtomicOrdering::SeqCst);
    }

    /// Is cancellation requested?
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(AtomicOrdering::SeqCst)
    }
}

/// Handle to a flushed sorted run file.
#[derive(Debug, Clone)]
pub struct RunHandle {
    /// Path to the run file on disk.
    pub path: PathBuf,
    /// Number of records in this run.
    pub record_count: u64,
    /// Total encoded byte length of the run.
    pub byte_len: u64,
}

/// Disk-backed run sink: accumulates records in a bounded arena, flushes
/// sorted runs to disk.
///
/// On drop, all tracked run files are removed and temp charges released.
pub struct DiskRunSink {
    dir: PathBuf,
    budget: ExternalSortBudget,
    metrics: ExternalSortMetrics,
    cancel: Option<CancelToken>,
    arena: Vec<FramedRecord>,
    /// Logical arena bytes (sum of per-record `arena_len`) — drives the
    /// flush threshold against `sort_run_bytes`. Unchanged from D0.8.10.
    arena_bytes: u64,
    /// Sum of `key.capacity() + payload.capacity()` across all live records.
    heap_bytes_total: u64,
    /// RAII guard holding the anonymous charge against the shared job ledger.
    /// Covers `arena.capacity() * size_of::<FramedRecord>() + heap_bytes_total`.
    /// Released automatically on drop (flush/cleanup/sink drop).
    anon_guard: Option<AnonReservation>,
    /// Shared job-level concurrent anon ledger. Every sink in the job charges
    /// here so the whole-job peak reflects concurrently live arenas (the node
    /// pass holds row + id + membership sinks at once), not just this sink's
    /// own high-water mark.
    job_anon: Arc<JobAnonLedger>,
    runs: Vec<RunHandle>,
    next_id: u64,
    correlation: String,
}

impl DiskRunSink {
    /// Create a sink writing runs under `dir`.
    ///
    /// # Errors
    /// Returns an error if budget validation fails or directory creation fails.
    pub fn new(
        dir: impl Into<PathBuf>,
        budget: ExternalSortBudget,
        correlation: impl Into<String>,
        job_anon: Arc<JobAnonLedger>,
    ) -> Result<Self, io::Error> {
        budget
            .validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            budget,
            metrics: ExternalSortMetrics::default(),
            cancel: None,
            arena: Vec::new(),
            arena_bytes: 0,
            heap_bytes_total: 0,
            anon_guard: None,
            job_anon,
            runs: Vec::new(),
            next_id: 0,
            correlation: correlation.into(),
        })
    }

    /// Attach a cancellation token.
    #[must_use]
    pub fn with_cancel(mut self, token: CancelToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Borrow the current metrics.
    #[must_use]
    pub fn metrics(&self) -> &ExternalSortMetrics {
        &self.metrics
    }

    /// Access tracked runs (for merger setup).
    #[must_use]
    pub fn runs(&self) -> &[RunHandle] {
        &self.runs
    }

    fn check_cancel(&self) -> Result<(), ExternalSortMetricsError> {
        if let Some(c) = &self.cancel
            && c.is_cancelled()
        {
            return Err(ExternalSortMetricsError::Overflow);
        }
        Ok(())
    }

    /// Push one record into the arena. If the arena exceeds `sort_run_bytes`,
    /// a run is flushed automatically.
    ///
    /// # Errors
    /// Returns an error if the record exceeds `max_record_bytes`, the temp
    pub fn push(&mut self, record: FramedRecord) -> Result<(), ExternalSortMetricsError> {
        self.check_cancel()?;
        let enc = record.encoded_len();
        if enc > self.budget.max_record_bytes {
            return Err(ExternalSortMetricsError::BudgetExceeded {
                requested: enc,
                limit: self.budget.max_record_bytes,
            });
        }
        // --- R2: reserve BEFORE allocation ---
        // Predict backing-array growth. Vec doubles capacity when len == cap.
        let old_cap = self.arena.capacity() as u64;
        let old_len = self.arena.len();
        let predicted_cap = if self.arena.len() == self.arena.capacity() {
            old_cap.saturating_mul(2).max(4)
        } else {
            old_cap
        };
        // R2-M3: add one extra element of headroom so the post-reserve
        // reconciliation grow() can never be the FIRST admission failure.
        // The pre-charge covers predicted growth + rounding headroom; the
        // post-reserve grow() only reconciles actual vs predicted (accuracy),
        // never gates admission for the first time.
        let backing_delta = predicted_cap
            .saturating_sub(old_cap)
            .saturating_add(1) // allocator rounding headroom
            .saturating_mul(std::mem::size_of::<FramedRecord>() as u64);
        let heap_delta =
            (record.key.capacity() as u64).saturating_add(record.payload.capacity() as u64);
        let total_delta = backing_delta.saturating_add(heap_delta);

        // R2-B1: flush based on the CONFIGURED sort_run_bytes — true 64 MiB
        // arena semantics. Concurrent arenas are coordinated by the shared
        // enforcing JobAnonLedger (max_anon_bytes = 128 MiB under the
        // acceptance profile), NOT by halving the per-sink flush threshold.
        // Two coordination triggers:
        //   1. Per-sink: this sink's charge exceeds sort_run_bytes → flush.
        //   2. Whole-job: the job ledger current + this growth would exceed
        //      max_anon_bytes → flush this sink to make room (admission
        //      coordination via the shared ledger).
        let flush_threshold = self.budget.sort_run_bytes;
        let current_charge = self.anon_guard.as_ref().map_or(0, AnonReservation::bytes);
        let job_current = self.job_anon.current();
        if (current_charge > 0
            && current_charge.saturating_add(total_delta) > flush_threshold)
            || job_current.saturating_add(total_delta) > self.budget.max_anon_bytes
        {
            self.flush_run()?;
        }

        if total_delta > 0 {
            // Charge the shared enforcing ledger BEFORE any allocation.
            match &mut self.anon_guard {
                Some(guard) => guard.grow(total_delta).map_err(anon_to_sort_err)?,
                None => {
                    self.anon_guard = Some(
                        self.job_anon
                            .reserve(total_delta)
                            .map_err(anon_to_sort_err)?,
                    );
                }
            }
            // Mirror onto per-sink observational metrics.
            self.metrics
                .reserve_anon(total_delta, self.budget.max_anon_bytes)?;
        }

        // Now allocate (reservation is held).
        self.arena.reserve(1);

        // Reconcile: actual capacity may exceed prediction (allocator rounding).
        // R2-M3: this grow() is a reconciliation of allocator rounding AFTER a
        // successful pre-charge that includes rounding headroom. It can only
        // fail if the allocator exceeded the headroom — in that case we undo
        // the allocation and propagate the error (first fail was pre-charge).
        let actual_backing = (self.arena.capacity() as u64)
            .saturating_mul(std::mem::size_of::<FramedRecord>() as u64);
        let actual_heap = self.heap_bytes_total.saturating_add(heap_delta);
        let actual_total = actual_backing.saturating_add(actual_heap);
        let predicted_total = (old_cap.saturating_mul(std::mem::size_of::<FramedRecord>() as u64))
            .saturating_add(self.heap_bytes_total)
            .saturating_add(total_delta);
        if actual_total > predicted_total {
            let extra = actual_total - predicted_total;
            if let Some(guard) = &mut self.anon_guard {
                if let Err(e) = guard.grow(extra) {
                    // Undo the allocation: truncate back to old length.
                    // The pre-charged reservation covers the predicted amount;
                    // the extra was never successfully charged.
                    self.arena.truncate(old_len);
                    self.metrics.release_anon(total_delta);
                    return Err(anon_to_sort_err(e));
                }
            }
            self.metrics
                .reserve_anon(extra, self.budget.max_anon_bytes)?;
        }

        self.heap_bytes_total = actual_heap;
        self.arena_bytes = self.arena_bytes.saturating_add(record.arena_len());
        self.metrics.record_count += 1;
        self.arena.push(record);
        Ok(())
    }

    fn flush_run(&mut self) -> Result<(), ExternalSortMetricsError> {
        self.check_cancel()?;
        if self.arena.is_empty() {
            return Ok(());
        }
        // Take the arena AND its RAII guard. The guard keeps the charge alive
        // while `run` is physically live (R2: run+I/O overlap).
        let mut run = std::mem::take(&mut self.arena);
        let run_guard = self.anon_guard.take();
        let anon_to_release = run_guard.as_ref().map_or(0, AnonReservation::bytes);
        self.heap_bytes_total = 0;
        self.arena_bytes = 0;

        run.sort_unstable();

        // Reserve run bytes against max_temp_bytes BEFORE writing the file.
        let byte_len: u64 = run.iter().map(FramedRecord::encoded_len).sum();
        self.metrics
            .reserve_temp(byte_len, self.budget.max_temp_bytes)
            .inspect_err(|_| run.clear())?;

        // Reserve I/O buffer charge — overlaps with the still-live run charge.
        let io_buf = self.budget.io_buffer_bytes as u64;
        let io_guard = self.job_anon.reserve(io_buf).map_err(anon_to_sort_err);
        let io_guard = match io_guard {
            Ok(g) => {
                if let Err(e) = self
                    .metrics
                    .reserve_anon(io_buf, self.budget.max_anon_bytes)
                {
                    drop(g);
                    self.metrics.release_temp(byte_len);
                    run.clear();
                    return Err(e);
                }
                Some(g)
            }
            Err(e) => {
                self.metrics.release_temp(byte_len);
                run.clear();
                return Err(e);
            }
        };

        let path = self
            .dir
            .join(format!("run-{}-{:06}.bin", self.correlation, self.next_id));
        self.next_id += 1;
        let write_result = (|| -> io::Result<()> {
            let file = File::create(&path)?;
            let mut w = BufWriter::with_capacity(self.budget.io_buffer_bytes, file);
            for rec in &run {
                rec.write_to(&mut w)?;
            }
            w.flush()?;
            w.into_inner().map_err(|e| e.into_error())?.sync_all()?;
            Ok(())
        })();

        // Release the transient I/O buffer charge (BufWriter is gone).
        drop(io_guard);
        self.metrics.release_anon(io_buf);

        // Release the run's anon charge (run Vec is about to drop).
        drop(run_guard);
        self.metrics.release_anon(anon_to_release);

        if let Err(e) = write_result {
            self.metrics.release_temp(byte_len);
            let _ = fs::remove_file(&path);
            return Err(ExternalSortMetricsError::from_io(e));
        }
        self.metrics.run_count += 1;
        self.runs.push(RunHandle {
            path,
            record_count: run.len() as u64,
            byte_len,
        });
        Ok(())
    }

    /// Flush remaining arena and return run handles.
    ///
    /// # Errors
    /// Returns an error on I/O failure or cancellation.
    pub fn finish(&mut self) -> Result<Vec<RunHandle>, ExternalSortMetricsError> {
        self.check_cancel()?;
        self.flush_run()?;
        Ok(self.runs.clone())
    }

    /// Peak anonymous (in-memory arena) bytes observed across all flushes.
    #[must_use]
    pub fn anon_peak(&self) -> u64 {
        self.metrics.anon_bytes_peak
    }

    /// Peak concurrent anonymous bytes across the whole job (shared ledger).
    #[must_use]
    pub fn job_anon_peak(&self) -> u64 {
        self.job_anon.peak()
    }

    /// Idempotent cleanup of all tracked run files.
    pub fn cleanup(&mut self) {
        for r in self.runs.drain(..) {
            self.metrics.release_temp(r.byte_len);
            let _ = fs::remove_file(&r.path);
        }
        // Release any live arena anon charge (unflushed records) via RAII.
        if let Some(guard) = self.anon_guard.take() {
            self.metrics.release_anon(guard.bytes());
            // guard drops here, releasing the shared ledger charge.
        }
        self.arena.clear();
        self.arena_bytes = 0;
        self.heap_bytes_total = 0;
    }
}

/// Map an enforcing-ledger error into the sort metrics error type.
fn anon_to_sort_err(e: AnonLedgerError) -> ExternalSortMetricsError {
    match e {
        AnonLedgerError::BudgetExceeded { requested, limit } => {
            ExternalSortMetricsError::BudgetExceeded { requested, limit }
        }
        AnonLedgerError::Overflow => ExternalSortMetricsError::Overflow,
    }
}

impl Drop for DiskRunSink {
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// Manages intermediate merge files across recursive passes.
pub struct DiskRunMerger {
    dir: PathBuf,
    budget: ExternalSortBudget,
    correlation: String,
    next_id: u64,
    intermediates: Vec<PathBuf>,
    /// Shared job-level anon ledger for merge I/O buffer charging (R2).
    job_anon: Arc<JobAnonLedger>,
}

impl DiskRunMerger {
    /// Create a merger writing intermediates under `dir`.
    ///
    /// # Errors
    /// Returns an error if budget validation fails or directory creation fails.
    pub fn new(
        dir: impl Into<PathBuf>,
        budget: ExternalSortBudget,
        correlation: impl Into<String>,
        job_anon: Arc<JobAnonLedger>,
    ) -> Result<Self, io::Error> {
        budget
            .validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            budget,
            correlation: correlation.into(),
            next_id: 0,
            intermediates: Vec::new(),
            job_anon,
        })
    }

    /// Cleanup all intermediate merge files.
    pub fn cleanup(&mut self) {
        for p in self.intermediates.drain(..) {
            let _ = fs::remove_file(p);
        }
    }
}

impl Drop for DiskRunMerger {
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// Recursive fan-in merge of `runs`, emitting sorted records via `emit`.
///
/// When `runs.len() > merge_fan_in`, intermediate merge passes reduce the
/// run count. Each intermediate is synced before consumed inputs are deleted.
/// The final pass emits records directly without writing another file.
///
/// # Truthful concurrent temp accounting (W0 §8)
///
/// Each merge output's bytes are reserved against `max_temp_bytes` BEFORE the
/// output file is extended. An output file is synced before any consumed
/// merger-owned intermediate is deleted and released, so the peak charge
/// includes both sides of a pass and always matches live files on disk.
///
/// # Errors
/// Returns an error on I/O failure, budget exhaustion, or cancellation.
pub fn merge_runs_recursive(
    merger: &mut DiskRunMerger,
    runs: &[RunHandle],
    metrics: &mut ExternalSortMetrics,
    cancel: Option<&CancelToken>,
    emit: &mut dyn FnMut(&FramedRecord) -> Result<(), ExternalSortMetricsError>,
) -> Result<(), ExternalSortMetricsError> {
    if runs.is_empty() {
        return Ok(());
    }
    let fan_in = merger.budget.merge_fan_in as usize;
    let mut level: Vec<RunHandle> = runs.to_vec();

    while level.len() > fan_in {
        if let Some(c) = cancel
            && c.is_cancelled()
        {
            return Err(ExternalSortMetricsError::Overflow);
        }
        metrics.merge_passes += 1;
        let mut next = Vec::new();
        for chunk in level.chunks(fan_in) {
            let out_bytes: u64 = chunk.iter().map(|r| r.byte_len).sum();
            metrics.reserve_temp(out_bytes, merger.budget.max_temp_bytes)?;
            let out_path = merger.dir.join(format!(
                "merge-{}-{:06}.bin",
                merger.correlation, merger.next_id
            ));
            merger.next_id += 1;
            match kway_merge_to_file(
                chunk,
                &out_path,
                merger.budget.io_buffer_bytes,
                cancel,
                metrics,
                &merger.job_anon,
            ) {
                Ok((count, bytes)) => {
                    debug_assert_eq!(bytes, out_bytes, "merge output size must match reservation");
                    merger.intermediates.push(out_path.clone());
                    next.push(RunHandle {
                        path: out_path,
                        record_count: count,
                        byte_len: bytes,
                    });
                }
                Err(e) => {
                    metrics.release_temp(out_bytes);
                    let _ = fs::remove_file(&out_path);
                    return Err(e);
                }
            }
        }
        // Pass outputs are synced. Now delete and release consumed inputs.
        for prev in level.drain(..) {
            if let Some(pos) = merger.intermediates.iter().position(|p| p == &prev.path) {
                merger.intermediates.swap_remove(pos);
                let _ = fs::remove_file(&prev.path);
            }
            metrics.release_temp(prev.byte_len);
        }
        level = next;
    }

    metrics.merge_passes += 1;
    kway_merge_emit(
        &level,
        merger.budget.io_buffer_bytes,
        cancel,
        metrics,
        emit,
        &merger.job_anon,
    )
}
