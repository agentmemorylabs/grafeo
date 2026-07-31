//! Unified flush: one code path for checkpoint, eviction, and explicit CHECKPOINT.
//!
//! The [`FlushManager`] replaces separate checkpoint/snapshot/close paths with
//! a single `flush()` method. Three triggers, one implementation:
//!
//! | Trigger | What gets written | RAM after flush |
//! |---------|-------------------|-----------------|
//! | Periodic checkpoint | All dirty sections | Kept |
//! | Memory pressure | Lowest-priority section | Mmap back, release RAM |
//! | Explicit CHECKPOINT | All sections | Kept |
//!
//! Future phases will add memory pressure integration with BufferManager.
//!
//! ## Generation-root seam (G-EM0.3a)
//!
//! Legacy flush writes into a standalone mapped `.grafeo` container. Writable
//! E-M0 generation builds use [`super::generation_build`] and publish into an
//! explicit generation root. Flush must never transparently rewrite a
//! generation root or convert a legacy standalone snapshot into the generation
//! layout.

use std::path::Path;

use grafeo_common::storage::{Section, SectionType};
use grafeo_common::utils::error::Result;

#[cfg(feature = "grafeo-file")]
use grafeo_storage::file::GrafeoFileManager;

/// Returns true when `path` is (or already contains) an E-M0 generation root.
///
/// Used to keep the legacy single-file flush path fail-closed against
/// accidental in-place conversion of generation layouts.
#[cfg(feature = "generation")]
#[must_use]
pub(crate) fn is_generation_root(path: &Path) -> bool {
    if path.join("root.lock").is_file() || path.join("manifest.bin").is_file() {
        return true;
    }
    if path.join("generations").is_dir() {
        return true;
    }
    // Conventional layout name: `<logical>.grafeo.d/`
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|name| name.ends_with(".grafeo.d"))
}

/// Placeholder when the generation feature is off — always false.
#[cfg(not(feature = "generation"))]
#[must_use]
pub(crate) fn is_generation_root(_path: &Path) -> bool {
    false
}

/// Reason for triggering a flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlushReason {
    /// Periodic checkpoint (timer-driven) or database close.
    #[allow(dead_code)] // Used by async_ops (async-storage feature)
    Checkpoint,
    /// User-initiated `CHECKPOINT` command or `wal_checkpoint()` API.
    Explicit,
}

/// Context needed by each section during serialization.
pub(super) struct FlushContext {
    pub epoch: u64,
    pub transaction_id: u64,
    pub node_count: u64,
    pub edge_count: u64,
}

/// Result of a flush operation.
pub(super) struct FlushResult {
    /// Number of sections written to the container.
    pub sections_written: usize,
}

/// Executes the unified flush: serialize dirty sections, write to container, truncate WAL.
///
/// This is the single write path for all persistence operations.
///
/// # Errors
///
/// Returns an error if serialization or I/O fails.
#[cfg(feature = "grafeo-file")]
pub(super) fn flush(
    fm: &GrafeoFileManager,
    sections: &[&dyn Section],
    context: &FlushContext,
    reason: FlushReason,
    #[cfg(feature = "wal")] wal: Option<&grafeo_storage::wal::LpgWal>,
) -> Result<FlushResult> {
    use grafeo_common::testing::crash::maybe_crash;

    maybe_crash("flush:before_serialize");

    // Collect sections to write based on flush reason
    // Write all sections (dirty or not for Explicit, only dirty for Checkpoint).
    // Retain each section's declared format version for the directory entry
    // (G-F0.1); do not hard-code version 1 at the shared writer.
    let mut targets: Vec<(SectionType, u8, Vec<u8>)> = Vec::new();
    for section in sections {
        if reason == FlushReason::Explicit || section.is_dirty() {
            targets.push((
                section.section_type(),
                section.version(),
                section.serialize()?,
            ));
        }
    }
    // If nothing is dirty on a periodic checkpoint, skip the write entirely.
    // Previous sections remain intact in the container.
    if targets.is_empty() {
        return Ok(FlushResult {
            sections_written: 0,
        });
    }

    let sections_written = targets.len();

    maybe_crash("flush:after_serialize");

    // Write sections to container with truthful per-section directory versions.
    let section_refs: Vec<(SectionType, u8, &[u8])> = targets
        .iter()
        .map(|(t, v, d)| (*t, *v, d.as_slice()))
        .collect();

    fm.write_versioned_sections(
        &section_refs,
        context.epoch,
        context.transaction_id,
        context.node_count,
        context.edge_count,
    )?;

    // Mark all written sections as clean
    for section in sections {
        if targets.iter().any(|(t, _, _)| *t == section.section_type()) {
            section.mark_clean();
        }
    }

    maybe_crash("flush:after_write");

    // Sync WAL to disk (all data is now in the container)
    #[cfg(feature = "wal")]
    if let Some(wal) = wal {
        wal.sync()?;
    }

    Ok(FlushResult { sections_written })
}

/// Builds the flush context from the current database state.
#[cfg(feature = "lpg")]
pub(super) fn build_context(
    store: &grafeo_core::graph::lpg::LpgStore,
    transaction_manager: &crate::transaction::TransactionManager,
) -> FlushContext {
    FlushContext {
        epoch: store.current_epoch().0,
        transaction_id: transaction_manager
            .last_assigned_transaction_id()
            .map_or(0, |t| t.0),
        node_count: store.node_count() as u64,
        edge_count: store.edge_count() as u64,
    }
}

/// Builds a minimal flush context when no LPG store is available.
#[cfg(not(feature = "lpg"))]
pub(super) fn build_context_minimal(
    transaction_manager: &crate::transaction::TransactionManager,
) -> FlushContext {
    FlushContext {
        epoch: 0,
        transaction_id: transaction_manager
            .last_assigned_transaction_id()
            .map_or(0, |t| t.0),
        node_count: 0,
        edge_count: 0,
    }
}
