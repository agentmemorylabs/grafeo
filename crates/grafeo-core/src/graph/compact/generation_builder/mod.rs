//! Streaming bounded generation builder (G-EM0.5b Phase 2).
//!
//! This module implements the end-to-end bounded generation build: freeze
//! overlay epoch → stream merged base+overlay → external dictionary → bounded
//! column/CSR/ID-lookup emission through spool sinks → payload assembly.
//!
//! The builder never holds a database-proportional structure in anonymous
//! memory. Every large segment body is emitted through a [`SpoolSegmentSink`],
//! and the final payload is streamed through [`V5PayloadAssembler::stream_to`].

pub mod builder;
pub mod column_pass;
pub mod csr_pass;
pub mod edge_pass;
pub mod emit_columns;
pub mod emit_ids;
pub mod emit_meta;
pub mod freeze;
pub mod node_pass;
pub mod orchestrator;
pub mod staging;

pub use builder::{StreamingBuildConfig, StreamingGenerationBuilder, StreamingGenerationOutput};
pub use freeze::{
    BaseEdgeCursor, BaseNodeCursor, EmptyEdgeSource, EmptyNodeSource, FrozenOverlayEpoch,
    MergedEdgeSource, MergedNodeSource,
};

#[cfg(test)]
mod csr_tests;
#[cfg(test)]
mod orchestrator_tests;
#[cfg(test)]
mod pipeline_tests;
#[cfg(test)]
mod tests;
