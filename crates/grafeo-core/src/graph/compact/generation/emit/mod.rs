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
pub mod dictionary;
pub mod global_dict;
pub mod segments;
pub mod sink;

pub use assembler::V5PayloadAssembler;
pub use column::ColumnEncoder;
pub use descriptor::{SegmentBody, SegmentDescriptor};
pub use dictionary::{BoundedDictionary, DictionaryPassDriver, StringOccurrence, StringUseKind};
pub use segments::emit_canonical_descriptors;
pub use sink::{MemorySegmentSink, SegmentSink, SpoolSegmentSink};

#[cfg(test)]
mod tests;
