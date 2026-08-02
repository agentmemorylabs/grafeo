//! Canonical bounded-emission surface for CompactStore v5 (G-EM0.5b Phase 0).
//!
//! One canonical emitter; two sink implementations behind a single
//! [`SegmentSink`] trait. Core owns all wire semantics; the caller passes a
//! `temp_dir` path into core (core does plain `std::fs` into that dir; core
//! never imports storage).
//!
//! Phase 0 delivers the foundation: sink/descriptor, assembler, external
//! dictionary, and incremental column encoder — all byte-checked against the
//! existing eager helpers on small fixtures. Phase 1 converges `serialize_v5`
//! and `emit_v5_segments` onto these primitives. Phase 2 drives them from the
//! streaming builder.

pub mod assembler;
pub mod column;
pub mod descriptor;
#[cfg(feature = "generation-streaming")]
pub mod dict_column_lookup;
pub mod dictionary;
pub mod global_dict;
pub mod payload_lease;
pub mod segments;
pub mod sink;
#[cfg(feature = "generation-streaming")]
pub mod streaming_column;

pub use assembler::V5PayloadAssembler;
pub use column::ColumnEncoder;
pub use descriptor::{SegmentBody, SegmentDescriptor};
#[cfg(feature = "generation-streaming")]
pub use dict_column_lookup::{DictChunkCatalog, DictCodeLookup, DictColumnLookup, EmptyDictLookup};
pub use dictionary::{BoundedDictionary, DictionaryPassDriver, StringOccurrence, StringUseKind};
pub use payload_lease::V5PayloadLease;
pub use segments::emit_canonical_descriptors;
pub use sink::{SegmentSink, SpoolSegmentSink};
#[cfg(feature = "generation-streaming")]
pub use streaming_column::StreamingBodyWriter;
// MemorySegmentSink is the legacy/test-only in-memory sink. When
// `generation-streaming` is on, it is NOT re-exported from the emit
// module — the bounded builder path must use SpoolSegmentSink. This
// is the B8 module-visibility seal.
#[cfg(not(feature = "generation-streaming"))]
pub use sink::MemorySegmentSink;
#[cfg(feature = "generation-streaming")]
#[allow(unused_imports)]
pub(crate) use sink::MemorySegmentSink;

#[cfg(test)]
mod tests;
