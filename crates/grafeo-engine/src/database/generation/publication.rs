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
/// These mirror the W0 §11 publication ordering and its commit point. The
/// mapping is faithful about **ordering and the commit boundary** but is not
/// a literal 1:1 step rename: W0's "sync final file" + "sync generations
/// directory" (one logical step) is a single phase here, and W0's genesis-only
/// layout/root-directory sync has no distinct phase. The commit point is
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

    /// Tag a W0 publication error with the phase inferred from its variant.
    ///
    /// This is the producer used by the live build path: it preserves the W0
    /// error identity and names the (conservative) failing phase so a caller
    /// can learn whether the failure was pre-commit (new generation not
    /// durable) or post-commit.
    #[must_use]
    pub fn from_publication(source: PublicationError) -> Self {
        let phase = phase_for_error(&source);
        Self::new(phase, source)
    }

    /// True when the failure occurred at/after the durable commit point —
    /// i.e. the new generation is already selected and the failure is in
    /// post-commit WAL truncation or cleanup.
    #[must_use]
    pub fn is_post_commit(&self) -> bool {
        self.phase.is_post_commit()
    }
}

impl From<PublicationPhaseError> for grafeo_common::utils::error::Error {
    fn from(e: PublicationPhaseError) -> Self {
        grafeo_common::utils::error::Error::Internal(e.to_string())
    }
}

/// Map a W0 [`PublicationError`] to the most precise publication phase.
///
/// W0's `publish_generation` does not return its fault-hook position on
/// failure, so the phase is inferred from the error variant. The mapping is
/// conservative: several variants are ambiguous (an I/O error can occur at
/// any step), so the returned phase is the **latest** phase the failing step
/// could represent — always accurate about the pre/post-commit boundary,
/// which is the distinction callers need (is the new generation durable?).
///
/// Boundary accuracy:
/// - `WalCut` is the pre-commit boundary cut (step 0) or post-commit truncate
///   (step 10); both are `>= WalBoundaryCut`, and the truncate case is
///   post-commit. We report `WalBoundaryCut` (pre-commit) as the conservative
///   floor — a post-commit truncate failure is indistinguishable at this seam.
/// - `TargetExists` / `ValidationFailed` are unambiguous pre-commit phases.
/// - `ManifestWrite` / `Io` / `NoLock` can arise at many steps; we report
///   `StreamSections` (the earliest fallible step after the cut) — never
///   falsely claiming the commit point was reached.
fn phase_for_error(err: &PublicationError) -> PublicationPhase {
    match err {
        PublicationError::NoLock => PublicationPhase::WalBoundaryCut,
        PublicationError::WalCut(_) => PublicationPhase::WalBoundaryCut,
        PublicationError::ValidationFailed(_) => PublicationPhase::ReopenValidate,
        PublicationError::TargetExists(_) => PublicationPhase::RenameImmutable,
        PublicationError::ManifestWrite(_) | PublicationError::Io(_) => {
            PublicationPhase::StreamSections
        }
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

/// Result of a G-EM0.3b build+publish: the extended publication descriptor
/// plus the absolute path of the immutable generation container.
///
/// The [`PublishedGeneration`] carries the durable WAL boundary, overlay
/// epoch, and parent linkage recorded in the manifest slot; the absolute path
/// is kept alongside for callers that need the on-disk container location.
#[derive(Debug, Clone)]
pub struct BuildPublication {
    /// The extended published-generation descriptor (WAL boundary + epoch).
    pub publication: PublishedGeneration,
    /// Absolute path to the immutable generation container.
    pub generation_abs_path: std::path::PathBuf,
}

/// Assemble a [`BuildPublication`] from a successful W0 publication, reading
/// back the durable manifest slot so the descriptor agrees with the on-disk
/// manifest by construction.
///
/// The manifest slot is the durable source of truth for parent linkage and
/// the WAL boundary. A read-back failure downgrades provenance to the
/// caller-supplied parent values and the pre-commit WAL-cut cursor (whose
/// `transaction_id`/`epoch` come from WAL checkpoint metadata, not the durable
/// slot); the downgrade is logged via `grafeo_warn!`, never silent.
///
/// `overlay_epoch` is the engine epoch captured at build time (used only when
/// the read-back fails); `parent_generation_id`/`parent_publication_sequence`
/// are the caller-supplied fallbacks.
#[must_use]
pub fn assemble_build_publication(
    root: &std::path::Path,
    result: &grafeo_storage::generation::publication::PublicationResult,
    generation_id: String,
    parent_generation_id: Option<String>,
    parent_publication_sequence: Option<u64>,
    overlay_epoch: u64,
) -> BuildPublication {
    let recorded = super::manifest::read_manifest_state(root);
    if let Err(ref downgrade) = recorded {
        grafeo_common::grafeo_warn!(
            "manifest read-back after publication failed ({downgrade}); \
             descriptor provenance downgraded to caller/pre-commit values"
        );
    }
    let recorded = recorded.ok();
    let (parent_id, parent_seq, recorded_boundary, recorded_epoch) = recorded.as_ref().map_or_else(
        || {
            (
                parent_generation_id.unwrap_or_default(),
                parent_publication_sequence.unwrap_or(0),
                WalBoundary::from_cursor(&result.wal_cursor),
                overlay_epoch,
            )
        },
        |state| {
            (
                state.selected.parent_generation_id.clone(),
                state.selected.parent_publication_sequence,
                state.selected.wal_boundary,
                state.selected.overlay_epoch,
            )
        },
    );

    let mut publication = PublishedGeneration::from_publication(
        result,
        generation_id,
        parent_id,
        parent_seq,
        recorded_epoch,
    );
    // Prefer the manifest-recorded boundary so descriptor and on-disk
    // manifest agree by construction.
    publication.wal_boundary = recorded_boundary;

    BuildPublication {
        publication,
        generation_abs_path: root.join(&result.generation_path),
    }
}
