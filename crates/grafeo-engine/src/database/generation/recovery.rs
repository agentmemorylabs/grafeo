//! Engine-side runtime recovery and publication fault surface (G-EM0.3c).
//!
//! This module owns the engine-facing recovery contract: a typed view over
//! the W0 triple-validation recovery result ([`grafeo_storage::generation::
//! recovery::recover`]) plus the orphan classification the operator surface
//! needs. It **delegates** all selection authority to W0 recovery — the
//! engine never selects by mtime, filename, PID, or partial decode, and never
//! promotes an unreferenced generation file.
//!
//! What this module adds on top of W0 (and what G-EM0.3b did not expose):
//!
//! - [`recover_generation_root`]: acquire the exclusive root lock, run W0
//!   recovery, and return a typed [`RootRecovery`] carrying the selected
//!   generation, its replayable WAL boundary, and every surviving
//!   on-disk artifact's [`OrphanClassification`].
//! - [`RecoveryViewError`]: a phase-aware typed error surface preserving the
//!   W0 [`RecoveryError`] identity (both-causes `NoValidGeneration` detail is
//!   never flattened away).
//! - [`PublicationCrashPoint`]: the engine-level names of the accepted W0
//!   fault-injection boundaries, with their commit-point classification, so
//!   the fresh-process crash matrix and its expectations are locked at the
//!   engine boundary without re-implementing the injection (which stays in
//!   W0's `#[cfg(test)]` fault hooks).
//!
//! WAL **replay** (applying frames from the selected boundary into a live
//! overlay) is a later packet (G-EM0.5b/5c); this module stops at validated
//! selection + exact replay boundary + orphan classification.

use std::path::Path;

use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::recovery::{RecoveryError, SelectedGeneration, recover};

use super::manifest::WalBoundary;

/// Classification of a surviving on-disk artifact under a generation root.
///
/// Recovery never deletes or promotes anything; it only classifies. Garbage
/// collection of classified orphans is a later packet (G-EM0.4b).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrphanClassification {
    /// The immutable generation file recovery selected as the live base.
    SelectedGeneration {
        /// Root-relative path recorded in the manifest slot.
        path: String,
    },
    /// The immutable generation file of the still-retained previous slot
    /// (the explicit W0 fallback; must be preserved while its slot survives).
    PreviousGeneration {
        /// Root-relative path recorded in the previous slot.
        path: String,
    },
    /// A correlation-scoped unpublished build directory (`.unpublished-*`)
    /// left by a pre-commit crash. Never promoted; safe to collect later.
    UnpublishedBuildDir {
        /// Directory name (no path separators).
        name: String,
    },
    /// An immutable generation file not referenced by any valid manifest
    /// slot. Never promoted (W0 selection is manifest-authority only).
    UnreferencedGeneration {
        /// File name under `generations/`.
        name: String,
    },
}

/// A fully validated engine-level recovery result.
#[derive(Debug)]
pub struct RootRecovery {
    /// The root lock, held for the database lifetime (Option S). Returned to
    /// the caller because recovery runs under the lock and ownership must not
    /// silently drop.
    pub lock: RootLock,
    /// The selected generation (slot, path, replay cursor), validated by W0
    /// recovery against structure + generation bytes + WAL replayability.
    pub selected: SelectedGeneration,
    /// The selected generation's durable WAL boundary (the exact replay
    /// range start) as the engine boundary type.
    pub wal_boundary: WalBoundary,
    /// The retained previous slot's root-relative generation path, when a
    /// second valid slot exists (the explicit W0 fallback).
    pub previous_generation_path: Option<String>,
    /// Classification of every surviving artifact observed under the root.
    pub orphans: Vec<OrphanClassification>,
}

/// Errors from engine-level recovery.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryViewError {
    /// The exclusive root lock could not be acquired.
    #[error("root lock: {0}")]
    Lock(String),
    /// W0 recovery failed; the source identity (including both-causes
    /// `NoValidGeneration` detail) is preserved.
    #[error("recovery: {0}")]
    Recovery(#[from] RecoveryError),
    /// Underlying I/O error while classifying orphans.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

impl From<RecoveryViewError> for grafeo_common::utils::error::Error {
    fn from(e: RecoveryViewError) -> Self {
        grafeo_common::utils::error::Error::Internal(e.to_string())
    }
}

/// Recover a generation root at the engine boundary: acquire the exclusive
/// root lock, run W0 triple-validation recovery, and classify every surviving
/// on-disk artifact.
///
/// Selection is entirely W0's: the highest-sequence slot whose manifest slot,
/// referenced generation file (length + SHA-256 + production open), and WAL
/// replay cursor all validate. Fallback goes only to the explicitly retained
/// previous slot; when neither validates, the W0 both-causes error is
/// preserved. No mtime, filename, or PID is consulted.
///
/// # Errors
///
/// Returns [`RecoveryViewError::Lock`] when the root is already owned,
/// [`RecoveryViewError::Recovery`] for every W0 recovery failure branch, and
/// [`RecoveryViewError::Io`] when orphan classification cannot read the root.
pub fn recover_generation_root(root: &Path) -> Result<RootRecovery, RecoveryViewError> {
    let lock = RootLock::try_acquire(root).map_err(|e| RecoveryViewError::Lock(e.to_string()))?;
    let selected = recover(&lock)?;
    let wal_boundary = WalBoundary::from_cursor(&selected.wal_cursor);

    let canonical = lock.canonical_root().to_path_buf();
    // Read the retained previous slot once; shared by the descriptor field
    // and orphan classification (a torn/absent previous slot yields None).
    let previous_generation_path = previous_slot_path(&canonical, &selected);
    let orphans =
        classify_root_artifacts(&canonical, &selected, previous_generation_path.as_deref())?;

    Ok(RootRecovery {
        lock,
        selected,
        wal_boundary,
        previous_generation_path,
        orphans,
    })
}

/// Read the retained previous slot's generation path when it decodes.
/// Best-effort: a non-decoding previous slot simply yields `None` (W0
/// recovery already validated the selected slot).
fn previous_slot_path(root: &Path, selected: &SelectedGeneration) -> Option<String> {
    let [slot0, slot1] =
        grafeo_storage::generation::manifest::read_both_slots(&root.join("manifest.bin")).ok()?;
    let other = match (selected.slot_index, slot0, slot1) {
        (0, _, Ok(prev)) => Some(prev),
        (1, Ok(prev), _) => Some(prev),
        _ => None,
    }?;
    Some(other.generation_path)
}

/// Classify surviving artifacts under the root without deleting anything.
fn classify_root_artifacts(
    root: &Path,
    selected: &SelectedGeneration,
    previous_path: Option<&str>,
) -> Result<Vec<OrphanClassification>, std::io::Error> {
    let mut out = vec![OrphanClassification::SelectedGeneration {
        path: selected.slot.generation_path.clone(),
    }];

    // Retained previous generation (explicit W0 fallback slot).
    if let Some(prev_path) = previous_path {
        out.push(OrphanClassification::PreviousGeneration {
            path: prev_path.to_string(),
        });
    }

    // Unpublished build directories at the root level.
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(".unpublished-") && entry.file_type()?.is_dir() {
            out.push(OrphanClassification::UnpublishedBuildDir { name });
        }
    }

    // Unreferenced immutable generations: present under `generations/` but
    // named by no valid slot.
    let generations_dir = root.join("generations");
    if generations_dir.is_dir() {
        let referenced: Vec<String> = out
            .iter()
            .filter_map(|c| match c {
                OrphanClassification::SelectedGeneration { path }
                | OrphanClassification::PreviousGeneration { path } => Some(path.clone()),
                _ => None,
            })
            .collect();
        for entry in std::fs::read_dir(&generations_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let rel = format!("generations/{name}");
            if !referenced.contains(&rel) {
                out.push(OrphanClassification::UnreferencedGeneration { name });
            }
        }
    }

    Ok(out)
}

/// The engine-level names of the accepted W0 publication fault-injection
/// boundaries, in publication order.
///
/// These are the points where a fresh-process crash proof (W0
/// `fresh_process_faults`, plus the engine crash matrix in
/// `tests/compact_store_manifest_recovery.rs`) injects a hard abort. The
/// classification here is the locked expectation contract:
///
/// - [`PublicationCrashPoint::is_post_commit`] is true exactly when the point
///   is at/after the manifest sync (the durable commit point); a crash there
///   must recover the NEW generation.
/// - A crash at a pre-commit point must recover the prior selected
///   generation, except `DuringSlotWrite`, where recovery may surface the old
///   or the fully-written new slot — but never a torn one (CRC-gated).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PublicationCrashPoint {
    /// After the correlation-scoped unpublished build directory exists.
    AfterUnpublishedDir = 0,
    /// After the container finished streaming to the partial file.
    AfterStreaming = 1,
    /// Before the partial generation file's fsync.
    BeforeGenSync = 2,
    /// After the partial generation file's fsync.
    AfterGenSync = 3,
    /// After fresh-reopen validation of the unpublished generation.
    AfterReopen = 4,
    /// After the atomic rename to the immutable generation path.
    AfterRename = 5,
    /// After the immutable generation file + generations directory fsync.
    AfterGenDirSync = 6,
    /// Before the inactive manifest slot write.
    BeforeSlotWrite = 7,
    /// Mid-way through the two-part inactive slot write (torn slot point).
    DuringSlotWrite = 8,
    /// After the complete inactive slot write, before its fsync.
    AfterSlotWrite = 9,
    /// Before the manifest fsync.
    BeforeManifestSync = 10,
    /// After the manifest fsync — **the durable commit point**.
    AfterManifestSync = 11,
    /// During post-commit WAL truncation.
    DuringWalCleanup = 12,
    /// After post-commit WAL truncation completed.
    AfterWalCleanup = 13,
}

impl PublicationCrashPoint {
    /// All crash points in publication order.
    pub const ALL: [PublicationCrashPoint; 14] = [
        PublicationCrashPoint::AfterUnpublishedDir,
        PublicationCrashPoint::AfterStreaming,
        PublicationCrashPoint::BeforeGenSync,
        PublicationCrashPoint::AfterGenSync,
        PublicationCrashPoint::AfterReopen,
        PublicationCrashPoint::AfterRename,
        PublicationCrashPoint::AfterGenDirSync,
        PublicationCrashPoint::BeforeSlotWrite,
        PublicationCrashPoint::DuringSlotWrite,
        PublicationCrashPoint::AfterSlotWrite,
        PublicationCrashPoint::BeforeManifestSync,
        PublicationCrashPoint::AfterManifestSync,
        PublicationCrashPoint::DuringWalCleanup,
        PublicationCrashPoint::AfterWalCleanup,
    ];

    /// The W0 fault-hook name this point injects at.
    #[must_use]
    pub fn hook_name(self) -> &'static str {
        match self {
            PublicationCrashPoint::AfterUnpublishedDir => "after_unpublished_dir",
            PublicationCrashPoint::AfterStreaming => "after_streaming",
            PublicationCrashPoint::BeforeGenSync => "before_gen_sync",
            PublicationCrashPoint::AfterGenSync => "after_gen_sync",
            PublicationCrashPoint::AfterReopen => "after_reopen",
            PublicationCrashPoint::AfterRename => "after_rename",
            PublicationCrashPoint::AfterGenDirSync => "after_gen_dir_sync",
            PublicationCrashPoint::BeforeSlotWrite => "before_slot_write",
            PublicationCrashPoint::DuringSlotWrite => "during_slot_write",
            PublicationCrashPoint::AfterSlotWrite => "after_slot_write",
            PublicationCrashPoint::BeforeManifestSync => "before_manifest_sync",
            PublicationCrashPoint::AfterManifestSync => "after_manifest_sync",
            PublicationCrashPoint::DuringWalCleanup => "during_wal_cleanup",
            PublicationCrashPoint::AfterWalCleanup => "after_wal_cleanup",
        }
    }

    /// True when a crash at this point is at/after the durable commit point
    /// and must therefore recover the NEW generation.
    #[must_use]
    pub fn is_post_commit(self) -> bool {
        self >= PublicationCrashPoint::AfterManifestSync
    }

    /// The expected selected publication sequence after a crash at this
    /// point, given `prior` as the last durable sequence before publication.
    ///
    /// - Pre-commit points: always `prior`.
    /// - `DuringSlotWrite`: `prior` or `prior + 1` (never torn).
    /// - Post-commit points: always `prior + 1`.
    #[must_use]
    pub fn expected_selection(self, prior: u64) -> ExpectedSelection {
        if self.is_post_commit() {
            ExpectedSelection::Exactly(prior + 1)
        } else if self == PublicationCrashPoint::DuringSlotWrite {
            ExpectedSelection::PriorOrNew { prior }
        } else {
            ExpectedSelection::Exactly(prior)
        }
    }
}

/// The locked recovery expectation for one crash point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedSelection {
    /// Recovery must select exactly this publication sequence.
    Exactly(u64),
    /// Recovery may select the prior generation or the fully-written new one
    /// (the torn-slot boundary); a torn slot must never be selected.
    PriorOrNew {
        /// The last durable sequence before the crashed publication.
        prior: u64,
    },
}

impl ExpectedSelection {
    /// Assert the expectation against an observed selected sequence.
    #[must_use]
    pub fn admits(self, observed: u64) -> bool {
        match self {
            ExpectedSelection::Exactly(seq) => observed == seq,
            ExpectedSelection::PriorOrNew { prior } => observed == prior || observed == prior + 1,
        }
    }
}
