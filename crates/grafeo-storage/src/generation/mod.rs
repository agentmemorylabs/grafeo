//! Bounded external-sort run/merge primitives for generation builds (G-EM0.W0-A1).
//!
//! This module owns the storage-layer mechanics of disk-backed sorted runs:
//! framed record I/O, bounded memory allocation via [`budget::GenerationBudget`],
//! recursive fan-in k-way merge, truthful concurrent temp-disk accounting, and
//! a cancellation/cleanup protocol that drops readers before unlinking.
//!
//! ## No graph semantics
//!
//! Per the W0 bridge plan §4, this module contains **zero** graph-semantic
//! types. There is no `NodeId`, `EdgeId`, `CompactStore`, or `SegmentKind`
//! anywhere in these files. Records are opaque `(key, payload)` byte pairs
//! owned by the caller. Graph identity resolution, CSR construction, and v5
//! codec emission live in `grafeo-core` (W0-A2 / W0-A3).
//!
//! ## Dependency direction
//!
//! `grafeo-storage` must not import `grafeo-core`. This module depends only
//! on `std`, `tempfile` (dev), and the `generation` feature gate (which pulls
//! in `grafeo-file` for `fs2`).

pub mod budget;
pub mod external_sort;
pub mod merge;
pub mod metrics;
pub mod records;

pub use budget::{ExternalSortBudget, GenerationBudget, GenerationBudgetError};
pub use external_sort::{CancelToken, DiskRunMerger, DiskRunSink, RunHandle, merge_runs_recursive};
pub use metrics::{ExternalSortMetrics, ExternalSortMetricsError, RssAnonSample, RssAnonSampler};
pub use records::{FramedRecord, FramedRecordError, MAX_RECORD_BODY_BYTES};

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
