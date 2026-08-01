//! Manifest publication orchestration surface (G-EM0.3b).
//!
//! This module owns the engine-facing publication contract: the ordered
//! publication phases, the extended published-generation descriptor that
//! carries the durable WAL boundary and overlay epoch, and the typed
//! publication error surface. It **delegates** the actual 11-step durability
//! ordering to W0 [`grafeo_storage::generation::publication::publish_generation`]
//! — the manifest fsync remains the commit point, and WAL truncation /
//! unpublished-path cleanup run only after that point.
//!
//! What this module adds on top of W0 (and what G-EM0.3a did not expose):
//!
//! - [`PublicationPhase`]: the exact ordered phases a publication moves
//!   through, so callers and tests can observe where a failure occurred.
//! - [`PublicationPhaseError`]: a phase-tagged error that preserves the W0
//!   [`PublicationError`] identity while naming the phase that failed.
//! - [`PublishedGeneration`]: an extended descriptor that retains the WAL
//!   replay boundary, overlay epoch, and parent linkage that 3a's
//!   `PublishedGenerationDescriptor` discarded.
//!
//! This module does not select a published generation (selection is W0
//! recovery) and does not advance/truncate the WAL before the manifest
//! selection is durable.

use grafeo_storage::generation::publication::PublicationError;
use grafeo_storage::generation::wal_cursor::WalReplayCursor;

use super::manifest::WalBoundary;

/// The ordered phases a manifest publication moves through.
///
/// These map 1:1 onto the W0 §11 publication ordering. The commit point is
/// [`PublicationPhase::ManifestSync`]: once that phase completes, the
/// generation is selected/durable and WAL truncation + cleanup may run.
/// Every phase before the commit point must leave the prior selected
/// generation untouched on failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PublicationPhase {
    /// Cut the WAL generation boundary (sync + rotate); freezes replay input.
    WalBoundaryCut = 0,
    /// Create the correlation-scoped unpublished build directory.
    UnpublishedDir = 1,
    /// Stream the complete container to the partial generation file.
    StreamSections = 2,
    /// fsync the partial generation file.
    SyncGeneration = 3,
    /// Fresh-reopen validate the unpublished generation.
    ReopenValidate = 4,
    /// Atomically rename the partial file to the immutable generation path.
    RenameImmutable = 5,
    /// fsync the immutable generation file and the generations directory.
    SyncGenerationDir = 6,
    /// Write the inactive manifest slot (full 4096 bytes).
    WriteSlot = 7,
    /// fsync the manifest — **the commit point**.
    ManifestSync = 8,
    /// Truncate the WAL at the recorded boundary (post-commit only).
    WalTruncate = 9,
    /// Best-effort cleanup of unpublished paths (post-commit only).
    Cleanup = 10,
}

impl PublicationPhase {
    /// All phases in publication order.
    pub const ALL: [PublicationPhase; 11] = [
        PublicationPhase::WalBoundaryCut,
        PublicationPhase::UnpublishedDir,
        PublicationPhase::StreamSections,
        PublicationPhase::SyncGeneration,
        PublicationPhase::ReopenValidate,
        PublicationPhase::RenameImmutable,
        PublicationPhase::SyncGenerationDir,
        PublicationPhase::WriteSlot,
        PublicationPhase::ManifestSync,
        PublicationPhase::WalTruncate,
        PublicationPhase::Cleanup,
    ];

    /// True when this phase is at or after the durable commit point.
    ///
    /// Phases before the commit point must not have advanced/truncated the
    /// WAL or reset overlay state; phases at/after it may.
    #[must_use]
    pub fn is_post_commit(self) -> bool {
        self >= PublicationPhase::ManifestSync
    }

    /// Human-readable phase name for diagnostics.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            PublicationPhase::WalBoundaryCut => "wal_boundary_cut",
            PublicationPhase::UnpublishedDir => "unpublished_dir",
            PublicationPhase::StreamSections => "stream_sections",
            PublicationPhase::SyncGeneration => "sync_generation",
            PublicationPhase::ReopenValidate => "reopen_validate",
            PublicationPhase::RenameImmutable => "rename_immutable",
            PublicationPhase::SyncGenerationDir => "sync_generation_dir",
            PublicationPhase::WriteSlot => "write_slot",
            PublicationPhase::ManifestSync => "manifest_sync",
            PublicationPhase::WalTruncate => "wal_truncate",
            PublicationPhase::Cleanup => "cleanup",
        }
    }
}

/// A phase-tagged publication error.
///
/// Preserves the underlying W0 [`PublicationError`] while naming the exact
/// [`PublicationPhase`] that failed, so an operator can tell a pre-commit
/// validation failure (previous generation still selected, WAL intact) from
/// a post-commit cleanup failure (new generation already durable).
#[derive(Debug, thiserror::Error)]
#[error("publication failed in phase {phase_name}: {source}")]
pub struct PublicationPhaseError {
    /// The phase that failed.
    pub phase: PublicationPhase,
    /// Static phase name (denormalized for `thiserror` display).
    phase_name: &'static str,
    /// The underlying W0 publication error.
    pub source: PublicationError,
}

impl PublicationPhaseError {
    /// Tag a W0 publication error with the phase that produced it.
    #[must_use]
    pub fn new(phase: PublicationPhase, source: PublicationError) -> Self {
        Self {
            phase,
            phase_name: phase.name(),
            source,
        }
    }

    /// True when the failure occurred at/after the durable commit point —
    /// i.e. the new generation is already selected and the failure is in
    /// post-commit WAL truncation or cleanup.
    #[must_use]
    pub fn is_post_commit(&self) -> bool {
        self.phase.is_post_commit()
    }
}

/// An extended, validated published-generation descriptor.
///
/// This is the G-EM0.3b descriptor: it retains everything 3a's
/// `PublishedGenerationDescriptor` exposed **plus** the durable WAL boundary
/// and overlay epoch the generation represents, and the parent linkage needed
/// for selection transition. It is produced only after a fresh-reopen-validated
/// immutable generation has been published and its manifest slot is durable.
#[derive(Debug, Clone)]
pub struct PublishedGeneration {
    /// Monotonic publication sequence.
    pub publication_sequence: u64,
    /// Publication sequence of the parent/previous generation (0 = genesis).
    pub parent_publication_sequence: u64,
    /// Root-relative generation path (`generations/g-…grafeo`).
    pub generation_path: String,
    /// SHA-256 of the complete generation file.
    pub generation_sha256: [u8; 32],
    /// Byte length of the generation file.
    pub generation_length: u64,
    /// Caller-supplied generation identifier.
    pub generation_id: String,
    /// Parent generation identifier (empty for genesis).
    pub parent_generation_id: String,
    /// The precise durable WAL boundary this generation represents.
    pub wal_boundary: WalBoundary,
    /// Overlay epoch represented by this generation.
    pub overlay_epoch: u64,
}

impl PublishedGeneration {
    /// Assemble the extended descriptor from a W0 publication result plus the
    /// parent linkage and overlay epoch known at publication time.
    #[must_use]
    pub fn from_publication(
        result: &grafeo_storage::generation::publication::PublicationResult,
        generation_id: String,
        parent_generation_id: String,
        parent_publication_sequence: u64,
        overlay_epoch: u64,
    ) -> Self {
        Self {
            publication_sequence: result.publication_sequence,
            parent_publication_sequence,
            generation_path: result.generation_path.clone(),
            generation_sha256: result.generation_sha256,
            generation_length: result.generation_length,
            generation_id,
            parent_generation_id,
            wal_boundary: WalBoundary::from_cursor(&result.wal_cursor),
            overlay_epoch,
        }
    }

    /// The WAL replay cursor this generation recorded, as the W0 storage type.
    #[must_use]
    pub fn wal_cursor(&self) -> WalReplayCursor {
        self.wal_boundary.to_cursor()
    }
}
