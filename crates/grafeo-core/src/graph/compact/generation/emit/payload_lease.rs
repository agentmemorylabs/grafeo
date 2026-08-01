//! Streaming payload lease (G-EM0.5b D0.8.7).
//!
//! The bounded builder returns a spool-owning [`V5PayloadLease`] instead of a
//! complete-payload `Vec<u8>`. The lease owns the ordered segment descriptors
//! (whose bodies are resident or spilled), exposes the exact payload length,
//! counts, preserve-ID flag, and final metrics, and streams the assembled
//! payload to any `std::io::Write` via [`V5PayloadLease::stream_to`].
//!
//! `grafeo-storage` adapts the lease to `ExactSectionSource` and passes it to
//! `create_versioned_sections_streaming` / `publish_generation`. Dropping the
//! lease after the outer copy cleans the transferred spools on success or
//! failure. The lease never exposes or constructs a complete-payload
//! `Vec<u8>`.

use crate::graph::compact::generation::GenerationMetrics;
use crate::graph::compact::generation::emit::assembler::V5PayloadAssembler;
use crate::graph::compact::generation::emit::descriptor::SegmentDescriptor;
use crate::graph::compact::generation::error::GenerationError;
use std::path::PathBuf;

/// A spool-owning streaming v5 payload lease.
pub struct V5PayloadLease {
    assembler: V5PayloadAssembler,
    descriptors: Vec<SegmentDescriptor>,
    total_nodes: u64,
    total_edges: u64,
    preserves_ids: bool,
    metrics: GenerationMetrics,
    /// Job temp directory (spools, build-tmp). Cleaned on Drop.
    temp_dir: Option<PathBuf>,
}

impl std::fmt::Debug for V5PayloadLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("V5PayloadLease")
            .field("total_nodes", &self.total_nodes)
            .field("total_edges", &self.total_edges)
            .field("segment_count", &self.descriptors.len())
            .finish_non_exhaustive()
    }
}

impl V5PayloadLease {
    /// Creates a lease from ordered descriptors and build results.
    ///
    /// Descriptors must be pre-sorted by `kind.as_u16()` ascending.
    #[must_use]
    pub fn new(
        descriptors: Vec<SegmentDescriptor>,
        total_nodes: u64,
        total_edges: u64,
        preserves_ids: bool,
        metrics: GenerationMetrics,
    ) -> Self {
        Self::with_temp_dir(descriptors, total_nodes, total_edges, preserves_ids, metrics, None)
    }

    /// Creates a lease that owns the job temp directory for RAII cleanup.
    ///
    /// When `temp_dir` is `Some`, the directory is removed recursively
    /// when the lease is dropped (after assembly on success, or on
    /// error/cancel/unwind). Spilled segment body files are cleaned by
    /// [`SegmentBody`]'s `Drop`.
    #[must_use]
    pub fn with_temp_dir(
        descriptors: Vec<SegmentDescriptor>,
        total_nodes: u64,
        total_edges: u64,
        preserves_ids: bool,
        metrics: GenerationMetrics,
        temp_dir: Option<PathBuf>,
    ) -> Self {
        let assembler = V5PayloadAssembler::new(total_nodes, total_edges, preserves_ids);
        Self {
            assembler,
            descriptors,
            total_nodes,
            total_edges,
            preserves_ids,
            metrics,
            temp_dir,
        }
    }

    /// Exact total payload byte length (computed from descriptor metadata
    /// without materializing the payload).
    ///
    /// # Errors
    ///
    /// [`GenerationError`] on segment-count overflow or body stat failure.
    pub fn exact_len(&self) -> Result<u64, GenerationError> {
        self.assembler.payload_len(&self.descriptors)
    }

    /// Total logical node count.
    #[must_use]
    pub fn total_nodes(&self) -> u64 {
        self.total_nodes
    }

    /// Total logical edge count.
    #[must_use]
    pub fn total_edges(&self) -> u64 {
        self.total_edges
    }

    /// Whether original IDs are preserved.
    #[must_use]
    pub fn preserves_ids(&self) -> bool {
        self.preserves_ids
    }

    /// Final build metrics.
    #[must_use]
    pub fn metrics(&self) -> &GenerationMetrics {
        &self.metrics
    }

    /// Streams the assembled payload to `sink` in bounded chunks.
    ///
    /// Spilled segment bodies are read through a fixed buffer; the payload is
    /// never fully resident.
    ///
    /// # Errors
    ///
    /// [`GenerationError`] on body read or sink write failure.
    pub fn stream_to(&mut self, sink: &mut dyn std::io::Write) -> Result<(), GenerationError> {
        self.assembler.stream_to(&self.descriptors, sink)
    }
}

impl Drop for V5PayloadLease {
    fn drop(&mut self) {
        // 1. SegmentBody::Drop deletes each spilled spool file.
        //    This happens automatically when descriptors drop.
        // 2. Clean up the job temp directory (spools, build-tmp, any
        //    other artifacts). Best-effort: ignore errors.
        if let Some(ref dir) = self.temp_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}
