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
use super::metrics::{ExternalSortMetrics, ExternalSortMetricsError, JobAnonLedger};
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
    /// Complete anonymous charge currently held against the ledger:
    /// `arena.capacity() * size_of::<FramedRecord>() + heap_bytes_total`.
    /// Includes Vec growth-doubling excess. Released on flush/cleanup.
    anon_charged: u64,
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
            anon_charged: 0,
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
    /// budget is exhausted, or cancellation is requested.
    pub fn push(&mut self, record: FramedRecord) -> Result<(), ExternalSortMetricsError> {
        self.check_cancel()?;
        let enc = record.encoded_len();
        if enc > self.budget.max_record_bytes {
            return Err(ExternalSortMetricsError::BudgetExceeded {
                requested: enc,
                limit: self.budget.max_record_bytes,
            });
        }
        // Flush threshold: the logical arena footprint (sum of per-record
        // `arena_len`), unchanged from D0.8.10. This is NOT the ledger charge.
        let mem = record.arena_len();
        if self.arena_bytes > 0 && self.arena_bytes.saturating_add(mem) > self.budget.sort_run_bytes
        {
            self.flush_run()?;
        }
        // Grow the backing array first so `arena.capacity()` reflects the
        // allocation we are about to charge (reserve mirrors push's own
        // amortized growth, so behavior is unchanged).
        self.arena.reserve(1);
        // Complete live anonymous charge for THIS sink:
        //   backing array  = arena.capacity() * size_of::<FramedRecord>()
        //                    (includes Vec growth-doubling excess)
        //   + record heaps = key.capacity() + payload.capacity()
        // Charged to the authoritative job ledger BEFORE the push so an
        // exhausted anon budget fails closed with no partial state.
        let new_heap = self
            .heap_bytes_total
            .saturating_add(record.key.capacity() as u64)
            .saturating_add(record.payload.capacity() as u64);
        let backing = (self.arena.capacity() as u64)
            .saturating_mul(std::mem::size_of::<FramedRecord>() as u64);
        let new_complete = backing.saturating_add(new_heap);
        if new_complete > self.anon_charged {
            let delta = new_complete - self.anon_charged;
            self.metrics
                .reserve_anon(delta, self.budget.max_anon_bytes)?;
            // Mirror the charge onto the shared job ledger so concurrent
            // sinks are summed into the whole-job peak.
            self.job_anon.reserve(delta);
        } else {
            let delta = self.anon_charged - new_complete;
            self.metrics.release_anon(delta);
            self.job_anon.release(delta);
        }
        self.anon_charged = new_complete;
        self.heap_bytes_total = new_heap;
        self.arena_bytes = self.arena_bytes.saturating_add(mem);
        self.metrics.record_count += 1;
        self.arena.push(record);
        Ok(())
    }

    fn flush_run(&mut self) -> Result<(), ExternalSortMetricsError> {
        self.check_cancel()?;
        if self.arena.is_empty() {
            return Ok(());
        }
        // Release the complete anon charge for the arena we are about to
        // consume. `run` (the taken Vec) still physically holds the memory
        // until it drops at function exit; the ledger tracks the job-level
        // live charge, and the peak is already captured in anon_bytes_peak.
        let anon_to_release = self.anon_charged;
        self.metrics.release_anon(anon_to_release);
        self.job_anon.release(anon_to_release);
        self.anon_charged = 0;
        self.heap_bytes_total = 0;
        self.arena_bytes = 0;

        let mut run = std::mem::take(&mut self.arena);
        run.sort_unstable();
        // Truthful accounting (W0 §8): reserve run bytes against max_temp_bytes
        // BEFORE writing the file. A rejected reservation never leaves a file.
        let byte_len: u64 = run.iter().map(FramedRecord::encoded_len).sum();
        self.metrics
            .reserve_temp(byte_len, self.budget.max_temp_bytes)
            .inspect_err(|_| run.clear())?;
        // The write path allocates an `io_buffer_bytes` BufWriter on top of
        // the still-live `run`. Charge it to the anon ledger so the flush peak
        // (run backing + heaps + I/O buffer) is truthful, then release it.
        let io_buf = self.budget.io_buffer_bytes as u64;
        self.metrics
            .reserve_anon(io_buf, self.budget.max_anon_bytes)
            .inspect_err(|_| {
                self.metrics.release_temp(byte_len);
                run.clear();
            })?;
        self.job_anon.reserve(io_buf);
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
        // Release the transient I/O buffer charge (the BufWriter is gone).
        self.metrics.release_anon(io_buf);
        self.job_anon.release(io_buf);
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
        // Release any live arena anon charge (unflushed records).
        if self.anon_charged > 0 {
            self.metrics.release_anon(self.anon_charged);
            self.job_anon.release(self.anon_charged);
            self.anon_charged = 0;
        }
        self.arena.clear();
        self.arena_bytes = 0;
        self.heap_bytes_total = 0;
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
    kway_merge_emit(&level, merger.budget.io_buffer_bytes, cancel, metrics, emit)
}
