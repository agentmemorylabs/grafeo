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

use super::metrics::{ExternalSortMetrics, ExternalSortMetricsError};
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
#[derive(Eq, PartialEq)]
struct HeapEntry {
    record: FramedRecord,
    run_index: usize,
}

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
pub(super) fn kway_merge_to_file(
    runs: &[super::external_sort::RunHandle],
    out_path: &Path,
    io_buffer_bytes: usize,
    cancel: Option<&super::external_sort::CancelToken>,
    metrics: &mut ExternalSortMetrics,
) -> Result<(u64, u64), ExternalSortMetricsError> {
    let mut readers = Vec::with_capacity(runs.len());
    for r in runs {
        readers.push(RunReader::open(&r.path, io_buffer_bytes).map_err(io_to_metrics_err)?);
    }
    metrics.observe_open_runs(readers.len());
    let mut heap = seed_heap(&mut readers)?;

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
        if let Some(rec) = readers[i].next_record().map_err(io_to_metrics_err)? {
            heap.push(HeapEntry {
                record: rec,
                run_index: i,
            });
        }
    }
    w.flush().map_err(io_to_metrics_err)?;
    w.into_inner()
        .map_err(|e| io_to_metrics_err(e.into_error()))
        .and_then(|f| f.sync_all().map_err(io_to_metrics_err))?;
    drop(readers);
    Ok((count, bytes))
}

/// Merge `runs` and emit each record via `emit`.
#[allow(clippy::too_many_arguments)]
pub(super) fn kway_merge_emit(
    runs: &[super::external_sort::RunHandle],
    io_buffer_bytes: usize,
    cancel: Option<&super::external_sort::CancelToken>,
    metrics: &mut ExternalSortMetrics,
    emit: &mut dyn FnMut(&FramedRecord) -> Result<(), ExternalSortMetricsError>,
) -> Result<(), ExternalSortMetricsError> {
    let mut readers = Vec::with_capacity(runs.len());
    for r in runs {
        readers.push(RunReader::open(&r.path, io_buffer_bytes).map_err(io_to_metrics_err)?);
    }
    metrics.observe_open_runs(readers.len());
    let mut heap = seed_heap(&mut readers)?;
    while let Some(entry) = heap.pop() {
        if let Some(c) = cancel
            && c.is_cancelled()
        {
            return Err(ExternalSortMetricsError::Overflow);
        }
        emit(&entry.record)?;
        let i = entry.run_index;
        if let Some(rec) = readers[i].next_record().map_err(io_to_metrics_err)? {
            heap.push(HeapEntry {
                record: rec,
                run_index: i,
            });
        }
    }
    drop(readers);
    Ok(())
}

/// Seed the heap with the first record from each reader.
fn seed_heap(readers: &mut [RunReader]) -> Result<BinaryHeap<HeapEntry>, ExternalSortMetricsError> {
    let mut heap = BinaryHeap::new();
    for (i, r) in readers.iter_mut().enumerate() {
        if let Some(rec) = r.next_record().map_err(io_to_metrics_err)? {
            heap.push(HeapEntry {
                record: rec,
                run_index: i,
            });
        }
    }
    Ok(heap)
}

/// Wrap an `io::Error` into the metrics error enum.
fn io_to_metrics_err(e: io::Error) -> ExternalSortMetricsError {
    ExternalSortMetricsError::from_io(e)
}
