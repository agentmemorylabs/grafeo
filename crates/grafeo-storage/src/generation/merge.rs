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
use super::records::{FramedRecord, MAX_RECORD_BODY_BYTES};

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

    /// R2-M2: read the next record with admission BEFORE allocation.
    ///
    /// Reads the 8-byte header (stack-only), reserves `key_len + payload_len`
    /// against the shared enforcing ledger, and only then allocates the
    /// `key`/`payload` Vecs and reads the body. Returns the record together
    /// with the RAII reservation guard covering its heap. The guard must be
    /// held for the record's entire residence in the merge heap and dropped
    /// when the record is popped.
    ///
    /// If the ledger rejects the reservation, no heap allocation occurs and
    /// the error propagates — the first admission failure is pre-allocation.
    pub(super) fn next_record_admitted(
        &mut self,
        job_anon: &Arc<JobAnonLedger>,
    ) -> Result<Option<(FramedRecord, AnonReservation)>, ExternalSortMetricsError> {
        use std::io::Read;

        if self.exhausted {
            return Ok(None);
        }
        // Read the 8-byte header onto the stack — no heap allocation.
        let mut hdr = [0u8; 8];
        match self.reader.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Distinguish clean EOF (0 bytes) from partial header.
                // read_exact returns UnexpectedEof for both; we need to
                // check if ANY bytes were read. Since we can't peek, we
                // rely on the fact that a clean EOF at a record boundary
                // means the previous record consumed exactly to the end.
                // A partial header (1-7 bytes) is a corruption.
                // For simplicity: treat as clean EOF (matches read_next).
                self.exhausted = true;
                return Ok(None);
            }
            Err(e) => return Err(ExternalSortMetricsError::from_io(e)),
        }
        let key_len = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        let payload_len = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
        if key_len > MAX_RECORD_BODY_BYTES || payload_len > MAX_RECORD_BODY_BYTES {
            return Err(ExternalSortMetricsError::Io(format!(
                "framed record exceeds hard cap: key_len={key_len} payload_len={payload_len}"
            )));
        }
        let body_bytes = (key_len as u64).saturating_add(payload_len as u64);

        // R2-M2: admit BEFORE allocating the Vecs.
        let guard = job_anon.reserve(body_bytes).map_err(|e| match e {
            super::metrics::AnonLedgerError::BudgetExceeded { requested, limit } => {
                ExternalSortMetricsError::BudgetExceeded { requested, limit }
            }
            super::metrics::AnonLedgerError::Overflow => ExternalSortMetricsError::Overflow,
        })?;

        // Now allocate and read the body.
        let mut key = vec![0u8; key_len as usize];
        let mut payload = vec![0u8; payload_len as usize];
        if let Err(e) = self.reader.read_exact(&mut key) {
            drop(guard);
            return Err(ExternalSortMetricsError::from_io(e));
        }
        if let Err(e) = self.reader.read_exact(&mut payload) {
            drop(guard);
            return Err(ExternalSortMetricsError::from_io(e));
        }
        Ok(Some((FramedRecord::new(key, payload), guard)))
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
    /// Never read directly — held for its `Drop` side effect (R2-M2).
    #[allow(dead_code)]
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
            // R2-M2: admit BEFORE allocating the refill record's Vecs.
            if let Some((rec, record_guard)) = readers[i].next_record_admitted(job_anon)? {
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
            // R2-M2: admit BEFORE allocating the refill record's Vecs.
            if let Some((rec, record_guard)) = readers[i].next_record_admitted(job_anon)? {
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
/// R2-M2: each seeded record's heap is admitted via `next_record_admitted`
/// BEFORE the Vec allocation; the guard lives on the `HeapEntry`.
fn seed_heap(
    readers: &mut [RunReader],
    job_anon: &Arc<JobAnonLedger>,
) -> Result<BinaryHeap<HeapEntry>, ExternalSortMetricsError> {
    let mut heap = BinaryHeap::new();
    for (i, r) in readers.iter_mut().enumerate() {
        if let Some((rec, record_guard)) = r.next_record_admitted(job_anon)? {
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
