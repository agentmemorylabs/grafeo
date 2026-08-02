//! Source-true CompactStore generation on bounded runs (G-EM0.W0-A2).
//!
//! Owns original-ID → dense offset assignment, complete edge records,
//! per-table forward/reverse CSR with true `ForwardPositions`, one global
//! UTF-8 lexicographic string dictionary, and emission through production
//! v5 codecs.

mod budget;
mod build;
mod columns;
pub mod emit;
mod error;
mod input;
pub mod ledger;
mod runs;
mod segment_source;
mod strings;
mod v5_emitter;

#[cfg(test)]
mod tests;

pub use budget::{GenerationBudget, GenerationMetrics};
#[cfg(test)]
pub use build::generate_v5_payload;
pub use ledger::{AnonLedgerError, AnonReservation, JobAnonLedger, JobAnonSnapshot};
// When `generation-streaming` is on, the eager `generate_compact_store` path
// is sealed: it is not re-exported from the generation module, so external
// crates (e.g. the engine) cannot reach it. The engine cutover uses the
// bounded orchestrator instead. The eager path remains available internally
// for test helpers (golden fixture generation) and when the feature is off.
#[cfg(not(feature = "generation-streaming"))]
pub use build::{GeneratedCompact, generate_compact_store};
#[cfg(feature = "generation-streaming")]
#[allow(unused_imports)]
pub(crate) use build::{GeneratedCompact, generate_compact_store};
pub(crate) use columns::encode_column;
pub use error::GenerationError;
pub use input::{
    EdgeRecordSource, GenerationEdge, GenerationInput, GenerationNode, NodeRecordSource,
    OriginalEdgeId, OriginalNodeId, RelSchemaDecl, SliceEdgeSource, SliceNodeSource,
};
pub use runs::{
    CancelToken, ExternalRunHandle, ExternalRunMerger, ExternalRunSink, InMemoryRunMerger,
    InMemoryRunSink, InMemoryRunStore, RunSetLease, RunStore, SortRecord, default_merge_fan_in,
};
#[cfg(test)]
pub use segment_source::assemble_v5_payload_from_source;
pub use segment_source::{CompactV5SegmentSource, V5Segment, V5SegmentSource};
pub use strings::{GlobalStringDictionary, collect_and_assign_global_codes};
pub use v5_emitter::emit_v5_segments;
