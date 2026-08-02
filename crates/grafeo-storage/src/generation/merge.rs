//! Internal k-way merge helpers (G-EM0.W0-A1).
//!
//! Heap-based merge of sorted run files: one [`RunReader`] per run, one
//! [`HeapEntry`] per current head record. Min-heap ordering by
//! `FramedRecord::cmp` (key then payload), tie-broken by `run_index`.
//!
//! Exposes two entry points:
//! - [`kway_merge_to_file`] — merges runs into a single output file.
//! - [`kway_merge_emit`] — merges runs emitting records via a callback.

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use super::metrics::{
    AnonReservation, ExternalSortMetrics, ExternalSortMetricsError, JobAnonLedger,
};
use super::records::FramedRecord;

/// Buffered reader for one sorted run file.
pub(super) struct RunReader {
    reader: BufReader<File>,
    exhausted: bool,
}

impl RunReader {
    /// Open a run file for buffered reading.
    pub(super) fn open(path: &Path, io_buffer_bytes: usize) -> io::Result<Self> {
        Ok(Self {
            reader: BufReader::with_capacity(io_buffer_bytes, File::open(path)?),
            exhausted: false,
        })
    }

    /// Read the next record, or `None` at clean EOF.
    pub(super) fn next_record(&mut self) -> io::Result<Option<FramedRecord>> {
        if self.exhausted {
            return Ok(None);
        }
        match FramedRecord::read_next(&mut self.reader) {
            Ok(Some(r)) => Ok(Some(r)),
            Ok(None) => {
                self.exhausted = true;
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}

/// One heap entry: current head record from run `run_index`.
///
/// # R3 (MAJOR-1): per-record heap charging
///
/// Each entry holds an [`AnonReservation`] (`record_guard`) covering its
/// record's owned heap (`key.capacity() + payload.capacity()`). The charge is
/// admitted **before** the entry is pushed (see [`charge_record`]) and released
/// automatically when the entry is popped and dropped. This keeps the merge
/// min-heap — which holds up to `fan_in` live `FramedRecord`s — on the shared
/// enforcing ledger on a *per-record* basis. Worst-case charging
/// (`fan_in × max_record_bytes`) is deliberately avoided: it would reserve
/// 256 MiB under the acceptance profile and break the 128 MiB gate.
struct HeapEntry {
    record: FramedRecord,
    run_index: usize,
    /// RAII charge for this record's key+payload heap. Drops on pop.
    record_guard: AnonReservation,
}

// `AnonReservation` is not `PartialEq`/`Eq`; compare only the ordering keys.
impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.record == other.record && self.run_index == other.run_index
    }
}
impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap via reverse
        other
            .record
            .cmp(&self.record)
            .then_with(|| other.run_index.cmp(&self.run_index))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Map an enforcing-ledger error into the sort metrics error type.
///
/// Duplicated from `external_sort::anon_to_sort_err` (which is private to that
/// module) so merge.rs can fail closed on per-record reservation without a
/// cross-module visibility change.
fn anon_to_merge_err(e: super::metrics::AnonLedgerError) -> ExternalSortMetricsError {
    match e {
        super::metrics::AnonLedgerError::BudgetExceeded { requested, limit } => {
            ExternalSortMetricsError::BudgetExceeded { requested, limit }
        }
        super::metrics::AnonLedgerError::Overflow => ExternalSortMetricsError::Overflow,
    }
}

/// Reserve the shared ledger charge for one record's owned heap.
///
/// Covers `key.capacity() + payload.capacity()` — the anonymous bytes the
/// record holds while it sits in the merge heap. Called **before** the
/// `HeapEntry` is pushed, so the charge is held for the record's entire
/// residence in the heap and released when the popped entry drops.
///
/// NOTE (accepted window): the record's `Vec`s are allocated by
/// `FramedRecord::read_next` *just before* this charge is admitted. That
/// bounded window (≤ `max_record_bytes` per record) is accepted; the ledger is
/// deliberately NOT threaded into `records.rs`.
fn charge_record(
    job_anon: &Arc<JobAnonLedger>,
    rec: &FramedRecord,
) -> Result<AnonReservation, ExternalSortMetricsError> {
    let bytes = (rec.key.capacity() as u64).saturating_add(rec.payload.capacity() as u64);
    job_anon.reserve(bytes).map_err(anon_to_merge_err)
}

/// Merge `runs` into `out_path`. Returns `(record_count, byte_len)`.
///
/// Syncs the output file before returning so callers may safely delete
/// consumed input runs.
///
/// # R2: merge I/O buffer charging
///
/// Each `RunReader` allocates a `BufReader` of `io_buffer_bytes` and the
/// output `BufWriter` allocates another. All are charged against the shared
/// job ledger **before** allocation and released after the merge completes.
pub(super) fn kway_merge_to_file(
    runs: &[super::external_sort::RunHandle],
    out_path: &Path,
    io_buffer_bytes: usize,
    cancel: Option<&super::external_sort::CancelToken>,
    metrics: &mut ExternalSortMetrics,
    job_anon: &Arc<JobAnonLedger>,
) -> Result<(u64, u64), ExternalSortMetricsError> {
    // Charge all merge I/O buffers before allocation:
    //   readers: runs.len() * io_buffer_bytes
    //   writer:  1 * io_buffer_bytes
    let io_per_buf = io_buffer_bytes as u64;
    let total_io = io_per_buf
        .saturating_mul(runs.len() as u64)
        .saturating_add(io_per_buf);
    let io_guard = job_anon.reserve(total_io).map_err(|e| match e {
        super::metrics::AnonLedgerError::BudgetExceeded { requested, limit } => {
            ExternalSortMetricsError::BudgetExceeded { requested, limit }
        }
        super::metrics::AnonLedgerError::Overflow => ExternalSortMetricsError::Overflow,
    })?;
    metrics.reserve_anon(total_io, u64::MAX)?;

    let result = (|| -> Result<(u64, u64), ExternalSortMetricsError> {
        let mut readers = Vec::with_capacity(runs.len());
        for r in runs {
            readers.push(RunReader::open(&r.path, io_buffer_bytes).map_err(io_to_metrics_err)?);
        }
        metrics.observe_open_runs(readers.len());
        let mut heap = seed_heap(&mut readers, job_anon)?;

        let file = File::create(out_path).map_err(io_to_metrics_err)?;
        let mut w = BufWriter::with_capacity(io_buffer_bytes, file);
        let mut count = 0u64;
        let mut bytes = 0u64;
        while let Some(entry) = heap.pop() {
            if let Some(c) = cancel
                && c.is_cancelled()
            {
                drop(readers);
                let _ = fs::remove_file(out_path);
                return Err(ExternalSortMetricsError::Overflow);
            }
            entry.record.write_to(&mut w).map_err(io_to_metrics_err)?;
            bytes += entry.record.encoded_len();
            count += 1;
            let i = entry.run_index;
            // `entry` (and its record_guard) drops here, releasing the popped
            // record's charge before the refill record is charged below.
            drop(entry);
            if let Some(rec) = readers[i].next_record().map_err(io_to_metrics_err)? {
                // R3 MAJOR-1: charge the refill record's heap before push.
                let record_guard = charge_record(job_anon, &rec)?;
                heap.push(HeapEntry {
                    record: rec,
                    run_index: i,
                    record_guard,
                });
            }
        }
        w.flush().map_err(io_to_metrics_err)?;
        w.into_inner()
            .map_err(|e| io_to_metrics_err(e.into_error()))
            .and_then(|f| f.sync_all().map_err(io_to_metrics_err))?;
        drop(readers);
        Ok((count, bytes))
    })();

    // Release merge I/O charges.
    drop(io_guard);
    metrics.release_anon(total_io);

    result
}

/// Merge `runs` and emit each record via `emit`.
///
/// # R2: merge I/O buffer charging
///
/// Each `RunReader` allocates a `BufReader` of `io_buffer_bytes`. All are
/// charged against the shared job ledger before allocation.
#[allow(clippy::too_many_arguments)]
pub(super) fn kway_merge_emit(
    runs: &[super::external_sort::RunHandle],
    io_buffer_bytes: usize,
    cancel: Option<&super::external_sort::CancelToken>,
    metrics: &mut ExternalSortMetrics,
    emit: &mut dyn FnMut(&FramedRecord) -> Result<(), ExternalSortMetricsError>,
    job_anon: &Arc<JobAnonLedger>,
) -> Result<(), ExternalSortMetricsError> {
    // Charge reader I/O buffers before allocation.
    let io_per_buf = io_buffer_bytes as u64;
    let total_io = io_per_buf.saturating_mul(runs.len() as u64);
    let io_guard = job_anon.reserve(total_io).map_err(|e| match e {
        super::metrics::AnonLedgerError::BudgetExceeded { requested, limit } => {
            ExternalSortMetricsError::BudgetExceeded { requested, limit }
        }
        super::metrics::AnonLedgerError::Overflow => ExternalSortMetricsError::Overflow,
    })?;
    metrics.reserve_anon(total_io, u64::MAX)?;

    let result = (|| -> Result<(), ExternalSortMetricsError> {
        let mut readers = Vec::with_capacity(runs.len());
        for r in runs {
            readers.push(RunReader::open(&r.path, io_buffer_bytes).map_err(io_to_metrics_err)?);
        }
        metrics.observe_open_runs(readers.len());
        let mut heap = seed_heap(&mut readers, job_anon)?;
        while let Some(entry) = heap.pop() {
            if let Some(c) = cancel
                && c.is_cancelled()
            {
                return Err(ExternalSortMetricsError::Overflow);
            }
            emit(&entry.record)?;
            let i = entry.run_index;
            // Release the popped record's charge before charging the refill.
            drop(entry);
            if let Some(rec) = readers[i].next_record().map_err(io_to_metrics_err)? {
                // R3 MAJOR-1: charge the refill record's heap before push.
                let record_guard = charge_record(job_anon, &rec)?;
                heap.push(HeapEntry {
                    record: rec,
                    run_index: i,
                    record_guard,
                });
            }
        }
        drop(readers);
        Ok(())
    })();

    // Release merge I/O charges.
    drop(io_guard);
    metrics.release_anon(total_io);

    result
}

/// Seed the heap with the first record from each reader.
///
/// Each seeded record's heap is charged against the shared ledger (R3
/// MAJOR-1) before its `HeapEntry` is pushed; the guard lives on the entry.
fn seed_heap(
    readers: &mut [RunReader],
    job_anon: &Arc<JobAnonLedger>,
) -> Result<BinaryHeap<HeapEntry>, ExternalSortMetricsError> {
    let mut heap = BinaryHeap::new();
    for (i, r) in readers.iter_mut().enumerate() {
        if let Some(rec) = r.next_record().map_err(io_to_metrics_err)? {
            // Reserve BEFORE push; fail closed if the ledger rejects it.
            let record_guard = charge_record(job_anon, &rec)?;
            heap.push(HeapEntry {
                record: rec,
                run_index: i,
                record_guard,
            });
        }
    }
    Ok(heap)
}

/// Wrap an `io::Error` into the metrics error enum.
fn io_to_metrics_err(e: io::Error) -> ExternalSortMetricsError {
    ExternalSortMetricsError::from_io(e)
}
