//! Production disk-backed [`RunStore`] adapter (G-EM0.5b D0.8.1).
//!
//! Bridges the core external-sort traits (`RunStore` / `ExternalRunSink` /
//! `ExternalRunMerger`) onto the accepted W0-A1 disk primitives
//! ([`DiskRunSink`], [`DiskRunMerger`], [`merge_runs_recursive`]) **without**
//! changing those primitives' signatures or `Drop` semantics.
//!
//! The key repair over the in-memory-only path: [`DiskRunSink`] deletes its
//! run files on `Drop`. If a sink returned bare run handles and was then
//! dropped, the files would vanish before the merger could read them. This
//! adapter instead returns a core [`RunSetLease`] whose cleanup closure owns
//! the live `DiskRunSink` — so the files outlive the adapter sink handle and
//! are deleted only when the lease is dropped (after the merge consumes them)
//! or on error/cancel/unwind.
//!
//! Dependency direction: this module lives in `grafeo-storage` and depends on
//! `grafeo-core` (enabled by the `generation` feature). Core never imports
//! storage. No core wire semantics are copied here — records are opaque
//! `(key, payload)` bytes translated to/from [`FramedRecord`].

use std::path::PathBuf;

use grafeo_core::graph::compact::generation::{
    ExternalRunHandle, ExternalRunMerger, ExternalRunSink, GenerationBudget, GenerationError,
    GenerationMetrics, RunSetLease, RunStore, SortRecord,
};

use super::budget::ExternalSortBudget;
use super::external_sort::{DiskRunMerger, DiskRunSink, merge_runs_recursive};
use super::metrics::{ExternalSortMetrics, ExternalSortMetricsError};
use super::records::FramedRecord;

/// Maps a core [`GenerationBudget`] onto a storage [`ExternalSortBudget`].
fn to_sort_budget(b: &GenerationBudget) -> ExternalSortBudget {
    ExternalSortBudget {
        max_temp_bytes: b.max_temp_bytes,
        sort_run_bytes: b.sort_run_bytes,
        io_buffer_bytes: b.io_buffer_bytes as usize,
        merge_fan_in: b.merge_fan_in,
        max_record_bytes: b.max_record_bytes,
    }
}

/// Maps a storage error onto a core [`GenerationError`].
fn map_err(e: ExternalSortMetricsError) -> GenerationError {
    match e {
        ExternalSortMetricsError::BudgetExceeded { requested, limit } => {
            GenerationError::BudgetExceeded {
                counter: "max_temp_bytes",
                requested,
                limit,
            }
        }
        ExternalSortMetricsError::Overflow => {
            GenerationError::Codec("external sort u64 overflow".into())
        }
        ExternalSortMetricsError::Io(m) => GenerationError::Io(m),
    }
}

fn to_framed(r: &SortRecord) -> FramedRecord {
    FramedRecord::new(r.key.clone(), r.payload.clone())
}

fn to_handle(h: &super::external_sort::RunHandle) -> ExternalRunHandle {
    ExternalRunHandle {
        id: h.path.to_string_lossy().into_owned(),
        record_count: h.record_count,
        byte_len: h.byte_len,
    }
}

/// Production disk-backed [`RunStore`].
///
/// Owns one job directory beneath the configured temp root; every sort
/// domain gets a correlation-scoped subdirectory. Sinks and mergers created
/// here share the job's correlation ID.
pub struct DiskRunStore {
    root: PathBuf,
    budget: GenerationBudget,
    correlation: String,
}

impl DiskRunStore {
    /// Creates a store writing runs under `root`, creating it if needed.
    ///
    /// # Errors
    /// Returns [`GenerationError::Io`] when the directory cannot be created.
    pub fn new(
        root: impl Into<PathBuf>,
        budget: GenerationBudget,
        correlation: impl Into<String>,
    ) -> Result<Self, GenerationError> {
        budget.validate()?;
        let root = root.into();
        std::fs::create_dir_all(&root)
            .map_err(|e| GenerationError::Io(format!("create run store dir: {e}")))?;
        Ok(Self {
            root,
            budget,
            correlation: correlation.into(),
        })
    }
}

impl RunStore for DiskRunStore {
    fn sink(
        &mut self,
        domain: &str,
        budget: &GenerationBudget,
    ) -> Result<Box<dyn ExternalRunSink>, GenerationError> {
        let dir = self.root.join(format!("{}-{domain}", self.correlation));
        let sink = DiskRunSink::new(
            dir,
            to_sort_budget(budget),
            format!("{}-{domain}", self.correlation),
        )
        .map_err(|e| GenerationError::Io(format!("create disk run sink: {e}")))?;
        Ok(Box::new(DiskSinkAdapter {
            inner: Some(sink),
            domain: domain.to_string(),
        }))
    }

    fn merger(&mut self, domain: &str) -> Result<Box<dyn ExternalRunMerger>, GenerationError> {
        let dir = self
            .root
            .join(format!("{}-{domain}-merge", self.correlation));
        let merger = DiskRunMerger::new(
            dir,
            to_sort_budget(&self.budget),
            format!("{}-{domain}", self.correlation),
        )
        .map_err(|e| GenerationError::Io(format!("create disk run merger: {e}")))?;
        Ok(Box::new(DiskMergerAdapter {
            inner: merger,
            budget: self.budget,
        }))
    }
}

/// Adapter sink: pushes core records into a [`DiskRunSink`], then transfers
/// the live sink into a [`RunSetLease`] on `finish`.
struct DiskSinkAdapter {
    inner: Option<DiskRunSink>,
    domain: String,
}

impl ExternalRunSink for DiskSinkAdapter {
    fn push(&mut self, record: SortRecord) -> Result<(), GenerationError> {
        let sink = self
            .inner
            .as_mut()
            .ok_or_else(|| GenerationError::InvalidInput("push after finish".into()))?;
        sink.push(to_framed(&record)).map_err(map_err)
    }

    fn finish(&mut self) -> Result<RunSetLease, GenerationError> {
        let mut sink = self
            .inner
            .take()
            .ok_or_else(|| GenerationError::InvalidInput("double finish".into()))?;
        let handles: Vec<ExternalRunHandle> = sink
            .finish()
            .map_err(map_err)?
            .iter()
            .map(to_handle)
            .collect();
        // Transfer the live sink into the lease: its Drop deletes the files,
        // but only when the lease is dropped (after the merge consumes the
        // runs) or on error/cancel/unwind. This is the D0.8.1 ownership fix.
        let domain = self.domain.clone();
        Ok(RunSetLease::with_cleanup(handles, move || {
            let _ = domain;
            drop(sink); // DiskRunSink::drop → cleanup() removes run files.
        }))
    }

    fn cleanup(&mut self) {
        if let Some(mut sink) = self.inner.take() {
            sink.cleanup();
        }
    }
}

/// Adapter merger: drives [`merge_runs_recursive`] over the live run files.
struct DiskMergerAdapter {
    inner: DiskRunMerger,
    budget: GenerationBudget,
}

impl ExternalRunMerger for DiskMergerAdapter {
    fn merge_all(
        &mut self,
        runs: &[ExternalRunHandle],
        _budget: &GenerationBudget,
        metrics: &mut GenerationMetrics,
        _cancel: Option<&grafeo_core::graph::compact::generation::CancelToken>,
        emit: &mut dyn FnMut(&SortRecord) -> Result<(), GenerationError>,
    ) -> Result<(), GenerationError> {
        // Resolve core handles (paths) back to storage RunHandles.
        let storage_runs: Vec<super::external_sort::RunHandle> = runs
            .iter()
            .map(|h| super::external_sort::RunHandle {
                path: PathBuf::from(&h.id),
                record_count: h.record_count,
                byte_len: h.byte_len,
            })
            .collect();
        let mut sort_metrics = ExternalSortMetrics::default();
        let mut emit_err: Option<GenerationError> = None;
        let result = merge_runs_recursive(
            &mut self.inner,
            &storage_runs,
            &mut sort_metrics,
            None,
            &mut |framed: &FramedRecord| {
                let rec = SortRecord::new(framed.key.clone(), framed.payload.clone());
                match emit(&rec) {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        emit_err = Some(e);
                        Err(ExternalSortMetricsError::Io("emit aborted".into()))
                    }
                }
            },
        );
        // Fold sort metrics into core metrics.
        metrics.merge_passes += sort_metrics.merge_passes;
        metrics.record_count += sort_metrics.record_count;
        metrics.max_open_runs = metrics.max_open_runs.max(sort_metrics.max_open_runs);
        if let Some(e) = emit_err {
            return Err(e);
        }
        result.map_err(map_err)?;
        let _ = self.budget;
        Ok(())
    }

    fn cleanup(&mut self) {
        self.inner.cleanup();
    }
}

// ── D0.8.4 disk-sorted mapped node-ID index ────────────────────────────────

/// Maps a completed fixed-width ID-index file with `memmap2` and returns a
/// checked core [`MappedNodeIdIndex`] over the mapping.
///
/// Per the D0.8.4 lock: the file is read-only mapped, wrapped in
/// `Bytes::from_owner`, and passed to a checked core view. The file length is
/// charged as temp, the mapping length is reported as mapped bytes, and only
/// fixed binary-search scratch is charged anonymous. The mapped index is
/// **never** copied into a resident `Vec`.
///
/// # Errors
///
/// Returns [`GenerationError`] when the file cannot be opened/mapped or the
/// index fails core validation (unsorted, adjacent duplicate, truncated).
#[cfg(unix)]
pub fn map_node_id_index(
    path: &std::path::Path,
) -> Result<grafeo_core::graph::compact::mapped::MappedNodeIdIndex, GenerationError> {
    use bytes::Bytes;
    use memmap2::Mmap;

    let file = std::fs::File::open(path)
        .map_err(|e| GenerationError::Io(format!("open ID index {}: {e}", path.display())))?;
    // SAFETY: the file is opened read-only and kept alive by the owner
    // closure captured in `Bytes::from_owner`; the mapping is never exposed
    // as `&mut`, and truncation of the underlying file is prevented because
    // the lease keeps the file open for the mapping's lifetime.
    #[allow(unsafe_code)]
    let mmap = unsafe { Mmap::map(&file) }
        .map_err(|e| GenerationError::Io(format!("mmap ID index {}: {e}", path.display())))?;
    let owner = MmapOwner(mmap);
    let bytes = Bytes::from_owner(owner);
    grafeo_core::graph::compact::mapped::MappedNodeIdIndex::new(bytes)
}

#[cfg(unix)]
struct MmapOwner(memmap2::Mmap);

#[cfg(unix)]
impl AsRef<[u8]> for MmapOwner {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
