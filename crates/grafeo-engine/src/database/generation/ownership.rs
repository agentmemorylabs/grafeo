//! Process ownership of a writable generation root (G-EM0.4b).
//!
//! This module owns the engine-facing **process ownership** contract for the
//! writable generation mode (packet requirement 1):
//!
//! - The accepted W0 Option-S exclusive root lock
//!   ([`grafeo_storage::generation::lock::RootLock`]) is acquired **before**
//!   the manifest is read, and it is held for the entire lifetime of the
//!   open root. This was already the 3c recovery order
//!   ([`recover_generation_root`]); [`RootOwnership`] makes the holding
//!   explicit: the lock cannot be silently dropped while the root is in use.
//! - Every second-process live-root open is rejected, **including read-only
//!   mode**: [`RootOwnership::open_read_only`] takes the same exclusive
//!   kernel lock as the writable open. External readers are never given a
//!   live-root path at all — they receive independent immutable snapshots
//!   published through the W0 contract (packet requirement 2, exercised via
//!   `grafeo_storage::generation::snapshot::publish_snapshot`).
//! - Process exit releases the kernel lock (handle close); recovery after a
//!   crash validates manifest/WAL state and never trusts a stale lease file
//!   (there is no PID file, no lease timeout, no timestamp fencing — the W0
//!   lock contract).
//!
//! What this module adds on top of 3c ([`recover_generation_root`]):
//!
//! - [`RootOwnership`]: an owned open root that bundles the held lock, the
//!   validated selected generation, its WAL boundary, and the open
//!   [`OpenMode`] for the database lifetime.
//! - [`RootLockOwnerState`]: the packet requirement-5 lock-owner
//!   observability surface (mode, canonical root, lock path, hold start).
//!   Full selected/previous/pinned/leased/eligible/retired reporting lives
//!   in [`super::retirement`].

use std::path::{Path, PathBuf};

use grafeo_storage::generation::lock::{RootLock, RootLockError};
use grafeo_storage::generation::recovery::{RecoveryError, SelectedGeneration};

use super::manifest::WalBoundary;
use super::recovery::{
    OrphanClassification, RecoveryViewError, RootRecovery, recover_generation_root,
};

/// How an owned generation root was opened.
///
/// Both modes take the **same** exclusive kernel lock (packet requirement
/// 1): a read-only open is still a live-root open and rejects every second
/// process. The mode tag exists for observability and for future
/// write-gating; it never weakens the lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Writable open: the owner may publish generations, back up, and retire.
    Writable,
    /// Read-only open: the owner serves the selected generation but performs
    /// no writes. The exclusive lock is still held, so a second process
    /// (writer or reader) is rejected.
    ReadOnly,
}

/// Errors opening an owned generation root.
///
/// The lock/recovery identities are preserved exactly (never flattened into
/// an opaque I/O error) so a caller can distinguish "another process owns
/// this root" from "the root is corrupt".
#[derive(Debug, thiserror::Error)]
pub enum OwnershipError {
    /// The exclusive root lock could not be acquired (already owned,
    /// non-canonical path, or unsupported filesystem).
    #[error("root lock: {0}")]
    Lock(#[from] RootLockError),
    /// W0 triple-validation recovery failed (manifest, generation bytes, or
    /// WAL replayability). The both-causes `NoValidGeneration` detail is
    /// preserved.
    #[error("recovery: {0}")]
    Recovery(#[from] RecoveryError),
    /// Underlying I/O error.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

impl From<RecoveryViewError> for OwnershipError {
    fn from(e: RecoveryViewError) -> Self {
        match e {
            RecoveryViewError::Lock(lock) => OwnershipError::Lock(lock),
            RecoveryViewError::Recovery(recovery) => OwnershipError::Recovery(recovery),
            RecoveryViewError::Io(io) => OwnershipError::Io(io),
        }
    }
}

impl From<OwnershipError> for grafeo_common::utils::error::Error {
    fn from(e: OwnershipError) -> Self {
        grafeo_common::utils::error::Error::Internal(e.to_string())
    }
}

/// Root-lock owner state for observability (packet requirement 5).
///
/// This is a plain data snapshot: observing the owner state never acquires
/// or releases the lock and never keeps a generation alive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootLockOwnerState {
    /// The mode this root was opened with.
    pub mode: OpenMode,
    /// The canonical root the lock is bound to.
    pub canonical_root: PathBuf,
    /// The lock file path (`<root>/root.lock`).
    pub lock_path: PathBuf,
    /// Wall-clock milliseconds (UNIX epoch) when ownership was acquired.
    pub held_since_ms: u64,
}

/// An owned writable generation root: the exclusive root lock held for the
/// database lifetime plus the validated recovery view.
///
/// A `RootOwnership` value existing **is** the proof of ownership: it can
/// only be constructed by acquiring the W0 Option-S lock and running W0
/// triple-validation recovery under it. Dropping it closes the lock handle;
/// process exit does the same at the kernel level. Recovery never trusts a
/// stale lease file — it re-validates manifest structure, generation bytes
/// (length + SHA-256 + production open), and WAL replayability on every
/// open.
#[derive(Debug)]
pub struct RootOwnership {
    /// The held recovery view (owns the [`RootLock`]).
    recovery: RootRecovery,
    /// The mode this root was opened with.
    mode: OpenMode,
    /// Wall-clock milliseconds (UNIX epoch) when ownership was acquired.
    held_since_ms: u64,
}

impl RootOwnership {
    /// Open a writable generation root, acquiring the exclusive root lock
    /// **before** reading the manifest and holding it until drop.
    ///
    /// # Errors
    ///
    /// Returns [`OwnershipError::Lock`] when another process owns the root
    /// (any mode), the path is non-canonical, or the filesystem is
    /// unsupported; [`OwnershipError::Recovery`] when no valid generation
    /// exists; [`OwnershipError::Io`] for read failures.
    pub fn open(root: &Path) -> Result<Self, OwnershipError> {
        Self::open_with_mode(root, OpenMode::Writable)
    }

    /// Open a generation root in read-only mode.
    ///
    /// This takes the **same** exclusive kernel lock as
    /// [`RootOwnership::open`]: packet requirement 1 rejects every
    /// second-process live-root open including read-only mode. External
    /// readers must use an independent immutable snapshot published through
    /// the W0 snapshot contract, never a live-root path.
    ///
    /// # Errors
    ///
    /// Identical to [`RootOwnership::open`].
    pub fn open_read_only(root: &Path) -> Result<Self, OwnershipError> {
        Self::open_with_mode(root, OpenMode::ReadOnly)
    }

    /// Shared open body: 3c recovery acquires the lock first, then validates.
    fn open_with_mode(root: &Path, mode: OpenMode) -> Result<Self, OwnershipError> {
        let recovery = recover_generation_root(root)?;
        Ok(Self {
            recovery,
            mode,
            held_since_ms: super::now_ms(),
        })
    }

    /// Wrap an already-recovered root (lock held) in ownership.
    ///
    /// Used by restore, which acquires the lock on a **new** root itself,
    /// publishes the restored generation under it, and then hands the owned
    /// root to the caller (packet requirement 4).
    #[must_use]
    pub fn from_recovery(recovery: RootRecovery, mode: OpenMode) -> Self {
        Self {
            recovery,
            mode,
            held_since_ms: super::now_ms(),
        }
    }

    /// The mode this root was opened with.
    #[must_use]
    pub fn mode(&self) -> OpenMode {
        self.mode
    }

    /// The canonical root this ownership is bound to.
    #[must_use]
    pub fn canonical_root(&self) -> &Path {
        self.recovery.lock.canonical_root()
    }

    /// The held exclusive root lock.
    #[must_use]
    pub fn lock(&self) -> &RootLock {
        &self.recovery.lock
    }

    /// The validated selected generation (manifest slot, absolute path,
    /// replay cursor).
    #[must_use]
    pub fn selected(&self) -> &SelectedGeneration {
        &self.recovery.selected
    }

    /// The selected generation's durable WAL boundary.
    #[must_use]
    pub fn wal_boundary(&self) -> WalBoundary {
        self.recovery.wal_boundary
    }

    /// The retained previous slot's root-relative generation path, when a
    /// second structurally valid slot exists. Slot-authority only (the
    /// referenced bytes are NOT guaranteed valid).
    #[must_use]
    pub fn previous_generation_path(&self) -> Option<&str> {
        self.recovery.previous_generation_path.as_deref()
    }

    /// Classification of every surviving artifact observed under the root at
    /// open time.
    #[must_use]
    pub fn orphans(&self) -> &[OrphanClassification] {
        &self.recovery.orphans
    }

    /// Consume ownership, returning the underlying recovery view (the lock
    /// travels with it; ownership is never silently dropped).
    #[must_use]
    pub fn into_recovery(self) -> RootRecovery {
        self.recovery
    }

    /// Root-lock owner state for observability (packet requirement 5).
    ///
    /// A plain data snapshot; observing never acquires, releases, or
    /// weakens the lock.
    #[must_use]
    pub fn owner_state(&self) -> RootLockOwnerState {
        RootLockOwnerState {
            mode: self.mode,
            canonical_root: self.recovery.lock.canonical_root().to_path_buf(),
            lock_path: self.recovery.lock.lock_path().to_path_buf(),
            held_since_ms: self.held_since_ms,
        }
    }
}

/// Writable generation-root ownership for a live `GrafeoDB` (H-ADOPT.2).
///
/// Bundles the two things a live production database must retain for its
/// entire open lifetime so nothing is released early:
///
/// - [`RootOwnership`]: the W0 Option-S exclusive root lock (`root.lock`) and
///   the validated selected generation / WAL boundary. Held until drop.
/// - [`GenerationLeaseRegistry`]: the in-process owner of the selected
///   mmap-backed base generation. Retiring/leasing correctness depends on the
///   registry (and thus the base mapping) outliving every reader.
///
/// A `GrafeoDB` opened as a generation root carries one of these in
/// `GrafeoDB::generation_root`; legacy and in-memory databases carry `None`.
/// Dropping it (with the database) releases the lock and lets the base mapping
/// be unmapped once the last lease drains — exactly the W0 close contract.
#[cfg(feature = "mmap")]
#[derive(Debug)]
pub struct GenerationRootOwnership {
    /// Process ownership of the root (lock + validated selected generation).
    ownership: RootOwnership,
    /// The in-process lease registry holding the selected mmap-backed base.
    registry: std::sync::Arc<super::lease::GenerationLeaseRegistry>,
}

#[cfg(feature = "mmap")]
impl GenerationRootOwnership {
    /// Open a generation root for a live database: acquire the exclusive lock,
    /// validate the selected generation, then install the lease registry on it.
    ///
    /// `mode` controls observability only — both modes take the same exclusive
    /// kernel lock (packet requirement 1).
    ///
    /// # Errors
    ///
    /// Returns [`OwnershipError`] when the root cannot be locked or no valid
    /// generation exists; returns `grafeo_common::utils::error::Error` when the
    /// selected generation container cannot be opened, mapped, or deserialized
    /// (never serves a torn generation).
    pub fn open(root: &Path, mode: OpenMode) -> Result<Self, grafeo_common::utils::error::Error> {
        let ownership = match mode {
            OpenMode::Writable => RootOwnership::open(root),
            OpenMode::ReadOnly => RootOwnership::open_read_only(root),
        }
        .map_err(|e| grafeo_common::utils::error::Error::Internal(e.to_string()))?;

        let selected = ownership.selected();
        let registry = super::lease::GenerationLeaseRegistry::from_selected(
            selected.slot.publication_sequence,
            selected.slot.generation_id.clone(),
            selected.generation_abs_path.clone(),
        )?;

        Ok(Self { ownership, registry })
    }

    /// The process ownership (root lock + validated selected generation).
    #[must_use]
    pub fn ownership(&self) -> &RootOwnership {
        &self.ownership
    }

    /// The in-process lease registry holding the selected base.
    #[must_use]
    pub fn registry(&self) -> &std::sync::Arc<super::lease::GenerationLeaseRegistry> {
        &self.registry
    }
}
