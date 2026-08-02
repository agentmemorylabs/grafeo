#![allow(clippy::cast_possible_truncation)]
//! Narrow external-run traits owned by core; storage implements them.

use super::budget::{GenerationBudget, GenerationMetrics};
use super::error::GenerationError;
use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

/// Default merge fan-in used when a budget is not yet applied.
pub const fn default_merge_fan_in() -> u32 {
    32
}

/// One sortable external-run record (opaque payload).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortRecord {
    /// Full sort key (byte-lexicographic).
    pub key: Vec<u8>,
    /// Opaque body retained with the key through sort/merge.
    pub payload: Vec<u8>,
}

impl SortRecord {
    /// Constructs a record.
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            payload: payload.into(),
        }
    }

    /// Total encoded size used for run-byte accounting.
    #[must_use]
    pub fn encoded_len(&self) -> u64 {
        // u32 key_len + key + u32 payload_len + payload
        8u64.saturating_add(self.key.len() as u64)
            .saturating_add(self.payload.len() as u64)
    }
}

impl PartialOrd for SortRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.payload.cmp(&other.payload))
    }
}

/// Opaque handle to a flushed sorted run.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExternalRunHandle {
    /// Implementation-defined identifier (path or in-memory id).
    pub id: String,
    /// Record count in the run.
    pub record_count: u64,
    /// Byte length of the run file / buffer.
    pub byte_len: u64,
}

/// Cancellation token shared across run/merge/emission.
#[derive(Debug, Clone, Default)]
pub struct CancelToken {
    flag: Arc<AtomicBool>,
}

impl CancelToken {
    /// Fresh token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation.
    pub fn cancel(&self) {
        self.flag.store(true, AtomicOrdering::SeqCst);
    }

    /// Returns true when cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(AtomicOrdering::SeqCst)
    }

    /// Shared cancellation flag (for bridging to storage-layer tokens).
    #[must_use]
    pub fn shared_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.flag)
    }

    /// Fail closed if cancelled.
    ///
    /// # Errors
    ///
    /// [`GenerationError::Cancelled`].
    pub fn check(&self) -> Result<(), GenerationError> {
        if self.is_cancelled() {
            Err(GenerationError::Cancelled)
        } else {
            Ok(())
        }
    }
}

/// Sink that accepts unsorted records and flushes sorted runs.
pub trait ExternalRunSink {
    /// Append one record into the current sort arena.
    ///
    /// # Errors
    /// Budget, cancel, or I/O failures.
    fn push(&mut self, record: SortRecord) -> Result<(), GenerationError>;

    /// Flush remaining arena as a final run (if non-empty) and return the
    /// RAII run-set lease owning every flushed run's storage.
    ///
    /// The lease keeps the runs alive (disk files are not deleted) until the
    /// caller explicitly consumes them via the merger and drops the lease.
    /// This is the D0.8.1 repair: a disk-backed sink deletes its files on
    /// `Drop`, so `finish` must transfer ownership into a lease rather than
    /// returning bare handles whose storage vanishes with the sink.
    ///
    /// # Errors
    /// Budget, cancel, or I/O failures.
    fn finish(&mut self) -> Result<RunSetLease, GenerationError>;

    /// Drop all unpublished runs for this sink (idempotent).
    fn cleanup(&mut self);

    /// Peak anonymous (in-memory arena) bytes observed by this sink.
    /// Callers fold this into the job-level anon ledger after `finish()`.
    /// Default: 0 (in-memory sinks that don't track anon separately).
    fn anon_peak(&self) -> u64 {
        0
    }
}

/// RAII ownership over one sort domain's flushed runs (G-EM0.5b D0.8.1).
///
/// Owns the run storage (files or buffers) until the runs are fully consumed
/// by a merger and the lease is dropped. On drop it cleans up every run it
/// still owns — on success, error, cancellation, unwind, or abandon.
///
/// The in-memory implementation is a no-op guard (run bodies live in the
/// shared registry). The storage implementation owns the live `DiskRunSink`
/// and deletes files on drop.
pub struct RunSetLease {
    /// Opaque handles to the runs in creation order.
    pub handles: Vec<ExternalRunHandle>,
    /// Cleanup closure invoked once on drop. `None` after explicit disarm.
    on_drop: Option<Box<dyn FnOnce() + Send>>,
}

impl std::fmt::Debug for RunSetLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunSetLease")
            .field("handles", &self.handles)
            .finish_non_exhaustive()
    }
}

impl RunSetLease {
    /// A lease with no cleanup responsibility (in-memory runs).
    #[must_use]
    pub fn in_memory(handles: Vec<ExternalRunHandle>) -> Self {
        Self {
            handles,
            on_drop: None,
        }
    }

    /// A lease that runs `cleanup` on drop.
    #[must_use]
    pub fn with_cleanup(
        handles: Vec<ExternalRunHandle>,
        cleanup: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            handles,
            on_drop: Some(Box::new(cleanup)),
        }
    }

    /// Number of runs owned.
    #[must_use]
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// True when no runs are owned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Disarm the drop cleanup, transferring responsibility to the caller.
    /// Used on the success path after the runs are fully consumed and the
    /// caller has taken over their lifecycle.
    pub fn disarm(&mut self) {
        self.on_drop = None;
    }
}

impl Drop for RunSetLease {
    fn drop(&mut self) {
        if let Some(cleanup) = self.on_drop.take() {
            cleanup();
        }
    }
}

/// Merger that performs recursive fan-in k-way merge.
pub trait ExternalRunMerger {
    /// Merge `runs` into a single sorted stream invoking `emit` per record.
    ///
    /// # Errors
    /// Budget, cancel, I/O, or emit failures.
    fn merge_all(
        &mut self,
        runs: &[ExternalRunHandle],
        budget: &GenerationBudget,
        metrics: &mut GenerationMetrics,
        cancel: Option<&CancelToken>,
        emit: &mut dyn FnMut(&SortRecord) -> Result<(), GenerationError>,
    ) -> Result<(), GenerationError>;

    /// Cleanup any temp outputs owned by this merger.
    fn cleanup(&mut self);
}

/// Factory for per-sort-domain sinks and mergers (G-EM0.5b Phase 2).
///
/// The streaming builder drives every external-sort domain through this seam:
/// production wires the storage-backed disk implementation; core unit tests
/// wire [`InMemoryRunStore`]. A fresh sink/merger pair is requested per
/// domain so concurrent domains hold independent arenas.
///
/// D0.8.1: both factories are **fallible** — a disk-backed store can fail to
/// create its job directory or validate its budget before any record is
/// pushed, and that failure must surface as a typed error, not a panic.
pub trait RunStore {
    /// Create a sink for one sort domain under the (possibly projected) budget.
    ///
    /// # Errors
    /// Returns [`GenerationError`] when the sink cannot be constructed (I/O,
    /// budget validation).
    fn sink(
        &mut self,
        domain: &str,
        budget: &GenerationBudget,
    ) -> Result<Box<dyn ExternalRunSink>, GenerationError>;

    /// Create a merger for one sort domain.
    ///
    /// # Errors
    /// Returns [`GenerationError`] when the merger cannot be constructed.
    fn merger(&mut self, domain: &str) -> Result<Box<dyn ExternalRunMerger>, GenerationError>;

    /// Open a completed fixed-width node-ID index file as a checked mapped view
    /// (D0.8.4).
    ///
    /// Production (`DiskRunStore`) must mmap the file and wrap it in
    /// `Bytes::from_owner` — never copy the index into a resident `Vec`.
    /// The in-memory test store may load small fixtures via `fs::read`.
    ///
    /// # Errors
    ///
    /// I/O or index-validation failure.
    fn map_id_index_file(
        &self,
        path: &std::path::Path,
    ) -> Result<crate::graph::compact::mapped::MappedNodeIdIndex, GenerationError>;

    /// Remove job-scoped run/merge artifacts after a successful build.
    ///
    /// Disk-backed stores delete correlation-scoped directories under the run
    /// root; in-memory stores are no-ops. Called on the success path after
    /// every run lease has been dropped.
    fn cleanup_job_artifacts(&mut self) -> Result<(), GenerationError> {
        let _ = self;
        Ok(())
    }
}

/// Shared run-body registry connecting in-memory sinks to in-memory mergers
/// (G-EM0.5b Phase 2 core tests). Handle ids map to their run bodies so a
/// merger can resolve handles produced by any sink sharing the registry.
pub type InMemoryRegistry =
    std::rc::Rc<std::cell::RefCell<grafeo_common::utils::hash::FxHashMap<String, Vec<SortRecord>>>>;

/// In-memory sink used by core unit tests and small fixtures.
#[derive(Debug)]
pub struct InMemoryRunSink {
    budget: GenerationBudget,
    metrics: GenerationMetrics,
    cancel: Option<CancelToken>,
    arena: Vec<SortRecord>,
    arena_bytes: u64,
    runs: Vec<Vec<SortRecord>>,
    run_handles: Vec<ExternalRunHandle>,
    next_id: u64,
    finished: bool,
    domain: Option<String>,
    registry: Option<InMemoryRegistry>,
}

impl InMemoryRunSink {
    /// Creates a sink with the given budget.
    #[must_use]
    pub fn new(budget: GenerationBudget) -> Self {
        Self {
            budget,
            metrics: GenerationMetrics::default(),
            cancel: None,
            arena: Vec::new(),
            arena_bytes: 0,
            runs: Vec::new(),
            run_handles: Vec::new(),
            next_id: 0,
            finished: false,
            domain: None,
            registry: None,
        }
    }

    /// Attaches a cancel token.
    #[must_use]
    pub fn with_cancel(mut self, token: CancelToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Attaches a shared registry and domain prefix (test wiring).
    ///
    /// Run handles are named `{domain}-run-{n}` and their bodies are
    /// registered on flush so an [`InMemoryRunMerger`] sharing the registry
    /// can resolve them in [`ExternalRunMerger::merge_all`].
    #[must_use]
    pub fn with_registry(mut self, registry: InMemoryRegistry, domain: &str) -> Self {
        self.domain = Some(domain.to_string());
        self.registry = Some(registry);
        self
    }

    /// Metrics snapshot.
    #[must_use]
    pub fn metrics(&self) -> &GenerationMetrics {
        &self.metrics
    }

    fn check_cancel(&self) -> Result<(), GenerationError> {
        if let Some(c) = &self.cancel {
            c.check()?;
        }
        Ok(())
    }

    fn flush_run(&mut self) -> Result<(), GenerationError> {
        self.check_cancel()?;
        if self.arena.is_empty() {
            return Ok(());
        }
        let mut run = std::mem::take(&mut self.arena);
        run.sort();
        let byte_len: u64 = run.iter().map(SortRecord::encoded_len).sum();
        self.metrics
            .reserve_temp(byte_len, self.budget.max_temp_bytes)?;
        let id = match &self.domain {
            Some(d) => format!("{d}-run-{}", self.next_id),
            None => format!("mem-run-{}", self.next_id),
        };
        self.next_id += 1;
        let handle = ExternalRunHandle {
            id: id.clone(),
            record_count: run.len() as u64,
            byte_len,
        };
        self.metrics.run_count += 1;
        if let Some(reg) = &self.registry {
            reg.borrow_mut().insert(id, run.clone());
        }
        self.runs.push(run);
        self.run_handles.push(handle);
        self.arena_bytes = 0;
        Ok(())
    }
}

impl ExternalRunSink for InMemoryRunSink {
    fn push(&mut self, record: SortRecord) -> Result<(), GenerationError> {
        if self.finished {
            return Err(GenerationError::InvalidInput(
                "push after finish on InMemoryRunSink".into(),
            ));
        }
        self.check_cancel()?;
        let enc = record.encoded_len();
        if enc > self.budget.max_record_bytes {
            return Err(GenerationError::BudgetExceeded {
                counter: "max_record_bytes",
                requested: enc,
                limit: self.budget.max_record_bytes,
            });
        }
        if self.arena_bytes > 0 && self.arena_bytes.saturating_add(enc) > self.budget.sort_run_bytes
        {
            self.flush_run()?;
        }
        self.arena_bytes = self.arena_bytes.saturating_add(enc);
        self.metrics.record_count += 1;
        self.arena.push(record);
        Ok(())
    }

    fn finish(&mut self) -> Result<RunSetLease, GenerationError> {
        self.check_cancel()?;
        self.flush_run()?;
        self.finished = true;
        Ok(RunSetLease::in_memory(self.run_handles.clone()))
    }

    fn cleanup(&mut self) {
        for h in &self.run_handles {
            self.metrics.release_temp(h.byte_len);
            if let Some(reg) = &self.registry {
                reg.borrow_mut().remove(&h.id);
            }
        }
        self.runs.clear();
        self.run_handles.clear();
        self.arena.clear();
        self.arena_bytes = 0;
    }
}

/// In-memory recursive fan-in merger.
#[derive(Debug, Default)]
pub struct InMemoryRunMerger {
    /// Owned intermediate merge outputs pending cleanup.
    intermediate: Vec<Vec<SortRecord>>,
    /// Shared run-body registry (test wiring).
    registry: Option<InMemoryRegistry>,
}

impl InMemoryRunMerger {
    /// Fresh merger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attaches a shared run-body registry so [`Self::merge_all`] can resolve
    /// handles produced by registry-attached [`InMemoryRunSink`]s.
    #[must_use]
    pub fn with_registry(mut self, registry: InMemoryRegistry) -> Self {
        self.registry = Some(registry);
        self
    }
}

impl ExternalRunMerger for InMemoryRunMerger {
    fn merge_all(
        &mut self,
        runs: &[ExternalRunHandle],
        budget: &GenerationBudget,
        metrics: &mut GenerationMetrics,
        cancel: Option<&CancelToken>,
        emit: &mut dyn FnMut(&SortRecord) -> Result<(), GenerationError>,
    ) -> Result<(), GenerationError> {
        let Some(reg) = &self.registry else {
            let _ = (runs, budget, metrics, cancel, emit);
            return Err(GenerationError::InvalidInput(
                "InMemoryRunMerger::merge_all requires merge_records with concrete run bodies"
                    .into(),
            ));
        };
        let store = reg.borrow();
        let mut bodies: Vec<Vec<SortRecord>> = Vec::with_capacity(runs.len());
        for h in runs {
            let body = store.get(&h.id).ok_or_else(|| {
                GenerationError::InvalidInput(format!(
                    "run handle {} not found in shared registry",
                    h.id
                ))
            })?;
            bodies.push(body.clone());
        }
        drop(store);
        self.merge_records(&bodies, budget, metrics, cancel, emit)
    }

    fn cleanup(&mut self) {
        self.intermediate.clear();
    }
}

/// In-memory [`RunStore`] for core unit tests and small fixtures.
///
/// Sinks and mergers created here share one registry, so the streaming
/// builder's trait-object pipeline runs end to end without disk I/O.
#[derive(Debug)]
pub struct InMemoryRunStore {
    registry: InMemoryRegistry,
    cancel: Option<CancelToken>,
}

impl InMemoryRunStore {
    /// Fresh store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: std::rc::Rc::new(std::cell::RefCell::new(
                grafeo_common::utils::hash::FxHashMap::default(),
            )),
            cancel: None,
        }
    }

    /// Attaches a cancel token propagated to every created sink.
    #[must_use]
    pub fn with_cancel(mut self, token: CancelToken) -> Self {
        self.cancel = Some(token);
        self
    }
}

impl Default for InMemoryRunStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RunStore for InMemoryRunStore {
    fn sink(
        &mut self,
        domain: &str,
        budget: &GenerationBudget,
    ) -> Result<Box<dyn ExternalRunSink>, GenerationError> {
        let mut sink =
            InMemoryRunSink::new(*budget).with_registry(std::rc::Rc::clone(&self.registry), domain);
        if let Some(c) = &self.cancel {
            sink = sink.with_cancel(c.clone());
        }
        Ok(Box::new(sink))
    }

    fn merger(&mut self, _domain: &str) -> Result<Box<dyn ExternalRunMerger>, GenerationError> {
        Ok(Box::new(
            InMemoryRunMerger::new().with_registry(std::rc::Rc::clone(&self.registry)),
        ))
    }

    fn map_id_index_file(
        &self,
        path: &std::path::Path,
    ) -> Result<crate::graph::compact::mapped::MappedNodeIdIndex, GenerationError> {
        // Test-only path: small fixtures may load the completed file into
        // heap `Bytes`. Production must use DiskRunStore's mmap path.
        let bytes = std::fs::read(path)
            .map_err(|e| GenerationError::Io(format!("read ID index {}: {e}", path.display())))?;
        crate::graph::compact::mapped::MappedNodeIdIndex::new(bytes::Bytes::from(bytes))
    }
}

impl InMemoryRunMerger {
    /// Merge concrete run bodies with recursive fan-in.
    ///
    /// # Errors
    ///
    /// Cancel or emit failures.
    pub fn merge_records(
        &mut self,
        run_bodies: &[Vec<SortRecord>],
        budget: &GenerationBudget,
        metrics: &mut GenerationMetrics,
        cancel: Option<&CancelToken>,
        emit: &mut dyn FnMut(&SortRecord) -> Result<(), GenerationError>,
    ) -> Result<(), GenerationError> {
        budget.validate()?;
        if run_bodies.is_empty() {
            return Ok(());
        }
        let fan_in = budget.merge_fan_in as usize;
        let mut level: Vec<Vec<SortRecord>> = run_bodies.to_vec();
        while level.len() > fan_in {
            if let Some(c) = cancel {
                c.check()?;
            }
            metrics.merge_passes += 1;
            let mut next = Vec::new();
            for chunk in level.chunks(fan_in) {
                let merged = kway_merge_once(chunk, cancel, metrics)?;
                let byte_len: u64 = merged.iter().map(SortRecord::encoded_len).sum();
                metrics.reserve_temp(byte_len, budget.max_temp_bytes)?;
                next.push(merged);
            }
            // Release prior level after next is fully formed.
            for prev in level.drain(..) {
                let byte_len: u64 = prev.iter().map(SortRecord::encoded_len).sum();
                metrics.release_temp(byte_len);
            }
            level = next;
            self.intermediate.clone_from(&level);
        }
        metrics.merge_passes += 1;
        let final_run = kway_merge_once(&level, cancel, metrics)?;
        for rec in &final_run {
            if let Some(c) = cancel {
                c.check()?;
            }
            emit(rec)?;
        }
        Ok(())
    }
}

fn kway_merge_once(
    runs: &[Vec<SortRecord>],
    cancel: Option<&CancelToken>,
    metrics: &mut GenerationMetrics,
) -> Result<Vec<SortRecord>, GenerationError> {
    use std::collections::BinaryHeap;

    #[derive(Eq, PartialEq)]
    struct HeapEntry {
        key: Vec<u8>,
        payload: Vec<u8>,
        run_index: usize,
        idx_in_run: usize,
    }
    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            other
                .key
                .cmp(&self.key)
                .then_with(|| other.payload.cmp(&self.payload))
                .then_with(|| other.run_index.cmp(&self.run_index))
        }
    }
    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let open = runs.iter().filter(|r| !r.is_empty()).count() as u64;
    metrics.max_open_runs = metrics.max_open_runs.max(open);

    let mut heap = BinaryHeap::new();
    for (ri, run) in runs.iter().enumerate() {
        if let Some(rec) = run.first() {
            heap.push(HeapEntry {
                key: rec.key.clone(),
                payload: rec.payload.clone(),
                run_index: ri,
                idx_in_run: 0,
            });
        }
    }
    let mut out = Vec::new();
    while let Some(entry) = heap.pop() {
        if let Some(c) = cancel {
            c.check()?;
        }
        out.push(SortRecord {
            key: entry.key,
            payload: entry.payload,
        });
        let next_idx = entry.idx_in_run + 1;
        if let Some(rec) = runs[entry.run_index].get(next_idx) {
            heap.push(HeapEntry {
                key: rec.key.clone(),
                payload: rec.payload.clone(),
                run_index: entry.run_index,
                idx_in_run: next_idx,
            });
        }
    }
    Ok(out)
}
