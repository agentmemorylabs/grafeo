//! Live-root retirement, backup, and garbage collection (G-EM0.4b).
//!
//! This module owns the engine-facing **retirement** contract for the
//! writable generation mode (packet requirements 2–5):
//!
//! - **Retention classes** ([`RetentionClass`]): a live-root generation may
//!   be deleted only when it is **not** selected, previous-recovery
//!   retained, backup-pinned, in-process leased, or referenced by a valid
//!   manifest/WAL recovery state. Slot authority (the dual-slot manifest) is
//!   the selection reference; the classes are evaluated in strict precedence
//!   so a generation protected by any class is never collected.
//! - **Backup pinning** ([`RetirementAuthority::pin_for_backup`] /
//!   [`backup::backup_generation_root`]): a backup pins an exact selected
//!   manifest sequence plus all referenced immutable generation/WAL bytes
//!   *before* copying; the pin is an in-process, ref-counted guard, so GC
//!   can never delete a generation out from under an in-flight backup.
//! - **External snapshots** are *not* tracked here: they are published to
//!   independent immutable paths outside the live root by the W0 snapshot
//!   contract, so live-root GC has no authority over them and never needs to
//!   track external readers (packet requirement 3).
//! - **Observability** ([`RetirementAuthority::lifecycle_report`]): root-lock
//!   owner state plus selected/previous/pinned/in-process-leased/eligible/
//!   retired IDs and reasons. The report is plain data — it holds no strong
//!   reference to any generation base, so observability never prevents
//!   retirement (packet requirement 5).
//!
//! What this module deliberately does **not** do:
//!
//! - It never deletes the selected or previous slot-referenced generation,
//!   even when its bytes are torn (slot authority, never bytes authority —
//!   recovery fallback depends on the retained slot).
//! - It never tracks or validates external snapshot consumers; a returned
//!   snapshot path belongs to its publisher/consumer contract.
//! - It never rewrites the legacy single-file backup surface in
//!   [`crate::database::backup`]; the generation-root backup/restore API is
//!   new and root-scoped (`backup` submodule).

mod backup;
mod gc;
mod pins;
mod restore;

pub use backup::{
    BACKUP_MANIFEST_NAME, BackedUpWalFile, GenerationBackupManifest, GenerationBackupReceipt,
    backup_generation_root,
};
pub use gc::{RetirementPlan, collect_retirement, plan_retirement};
pub use pins::{BackupPin, BackupPinGuard};
pub use restore::restore_generation_root;

use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use super::ownership::RootOwnership;
use pins::PinRegistry;

/// The retention class of one artifact under the live root (packet
/// requirement 3). Evaluated in strict precedence — the first matching
/// class wins, so a slot-referenced generation is never reported as leased
/// or eligible even when both also apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionClass {
    /// Selected by a valid manifest slot (the live base). Never collectable.
    Selected,
    /// Referenced by the still-retained other manifest slot (the explicit
    /// recovery fallback). Never collectable while its slot survives.
    PreviousRecoveryRetained,
    /// Pinned by an in-flight backup. Never collectable until the pin drops.
    BackupPinned,
    /// Held by a live in-process read lease (strong refs > 0). Never
    /// collectable while the lease survives.
    InProcessLeased,
    /// Referenced by no valid slot, pin, or lease: eligible for collection.
    Eligible,
    /// Collected by this process (recorded with its deletion reason).
    Retired,
}

/// One classified artifact: identity, retention class, and the operator
/// reason for that class (packet requirement 5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifiedGeneration {
    /// Root-relative path (`generations/<name>.grafeo` or a root-level
    /// unpublished build-artifact name).
    pub path: String,
    /// The generation identifier, when known from a manifest slot.
    pub generation_id: Option<String>,
    /// The retention class.
    pub class: RetentionClass,
    /// Why this class applies (operator-facing reason).
    pub reason: String,
}

/// Errors from retirement, backup, and restore operations.
///
/// Every failure branch is typed and fail-closed: GC deletes nothing on any
/// doubt, and a backup/restore never publishes unvalidated bytes.
#[derive(Debug, thiserror::Error)]
pub enum RetirementError {
    /// The backup destination is inside the live root.
    #[error("destination inside live root")]
    DestinationInsideRoot,
    /// A caller-supplied name is not a single safe path component.
    #[error("name is not a single safe path component")]
    UnsafeName,
    /// The restore target root is not empty (restore never overwrites).
    #[error("restore root is not empty")]
    NonEmptyRoot,
    /// The retirement plan no longer matches the live manifest state; the
    /// collection was aborted and nothing was deleted.
    #[error("retirement plan is stale: {0}")]
    SelectionChanged(String),
    /// A generation or backup artifact failed validation (length, hash, or
    /// container structure).
    #[error("validation failed: {0}")]
    ValidationFailed(String),
    /// The backup manifest could not be encoded or decoded.
    #[error("backup manifest: {0}")]
    BackupManifest(String),
    /// The exclusive root lock could not be acquired.
    #[error("root lock: {0}")]
    Lock(#[from] grafeo_storage::generation::lock::RootLockError),
    /// W0 recovery failed (restore-time validation).
    #[error("recovery: {0}")]
    Recovery(#[from] grafeo_storage::generation::recovery::RecoveryError),
    /// The W0 dual-slot manifest could not be read or decoded.
    #[error("manifest: {0}")]
    StorageManifest(#[from] grafeo_storage::generation::manifest::ManifestError),
    /// The engine manifest view could not be read.
    #[error("manifest state: {0}")]
    ManifestState(#[from] super::manifest::ManifestStateError),
    /// Underlying I/O error.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

impl From<grafeo_common::utils::error::Error> for RetirementError {
    fn from(e: grafeo_common::utils::error::Error) -> Self {
        match e {
            grafeo_common::utils::error::Error::Io(io) => RetirementError::Io(io),
            other => RetirementError::ValidationFailed(other.to_string()),
        }
    }
}

impl From<RetirementError> for grafeo_common::utils::error::Error {
    fn from(e: RetirementError) -> Self {
        grafeo_common::utils::error::Error::Internal(e.to_string())
    }
}

/// The retirement authority for one owned generation root.
///
/// Owns the in-process backup-pin registry and the retired-artifact log for
/// the root. The authority holds **no strong reference** to any generation
/// base: in-process lease state is read through the lease registry's
/// weak-probe statistics (G-EM0.4a), and pins are plain data. Observability
/// through the authority therefore never prevents retirement (packet
/// requirement 5).
///
/// Construct exactly one authority per [`RootOwnership`]; the pin registry
/// is per-authority, so two authorities on one root would not share pins.
///
/// ## Why eligibility is a fresh manifest read (not recovery-anchored)
///
/// GC runs in the owner process under the exclusive root lock, and every
/// publication needs that same lock — so no build is in flight while GC
/// plans or collects. Under that model an unreferenced generation or a
/// `.unpublished-*` directory observed by GC is, by construction, a crash
/// leftover (a pre-commit build that aborted before this owner acquired the
/// lock), never a live build. Eligibility is therefore a fresh read of the
/// manifest plus the in-process pin/lease state; nothing is deferred to a
/// later recovery.
#[derive(Debug)]
pub struct RetirementAuthority {
    /// The canonical root this authority serves.
    root: PathBuf,
    /// Active backup pins, ref-counted (a path stays pinned until the last
    /// guard covering it drops).
    pins: PinRegistry,
    /// Artifacts retired (deleted) by this process, with reasons.
    retired: Mutex<Vec<ClassifiedGeneration>>,
    /// The in-process lease registry (weak-probe observability only).
    #[cfg(feature = "mmap")]
    lease_registry: Option<std::sync::Arc<super::lease::GenerationLeaseRegistry>>,
}

impl RetirementAuthority {
    /// Create the retirement authority for an owned root.
    #[must_use]
    pub fn new(ownership: &RootOwnership) -> Self {
        Self {
            root: ownership.canonical_root().to_path_buf(),
            pins: PinRegistry::default(),
            retired: Mutex::new(Vec::new()),
            #[cfg(feature = "mmap")]
            lease_registry: None,
        }
    }

    /// Attach the in-process lease registry so GC honors live read leases
    /// (packet requirement 3) and observability reports lease counts
    /// (packet requirement 5). Only weak-probe statistics are read; the
    /// authority never takes a strong reference to a base.
    #[cfg(feature = "mmap")]
    #[must_use]
    pub fn with_lease_registry(
        mut self,
        registry: std::sync::Arc<super::lease::GenerationLeaseRegistry>,
    ) -> Self {
        self.lease_registry = Some(registry);
        self
    }

    /// The canonical root this authority serves.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Pin a generation for backup (packet requirement 4). The returned
    /// guard holds the pin; GC treats the generation as [`RetentionClass::
    /// BackupPinned`] until the guard drops. Pins are ref-counted, so two
    /// concurrent backups of the same generation each keep it pinned.
    ///
    /// Callers backing up through [`backup::backup_generation_root`] get the
    /// pin automatically; this entry point exists for integrations that pin
    /// first and copy with their own streaming logic.
    #[must_use]
    pub fn pin_for_backup(
        &self,
        generation_path: String,
        publication_sequence: u64,
    ) -> BackupPinGuard<'_> {
        self.pins.pin(generation_path, publication_sequence)
    }

    /// The currently active backup pins (plain data snapshot).
    #[must_use]
    pub fn active_pins(&self) -> Vec<BackupPin> {
        self.pins.active()
    }

    /// In-process leased generations with live strong references, as
    /// `(root-relative path, publication sequence, strong refs)`. Empty when
    /// no lease registry is attached (or `mmap` is disabled); the empty case
    /// is honest: without a registry there are no in-process leases to
    /// honor.
    #[cfg(feature = "mmap")]
    pub(crate) fn leased_generations(&self) -> Vec<(String, u64, usize)> {
        let Some(registry) = self.lease_registry.as_ref() else {
            return Vec::new();
        };
        registry
            .lease_stats()
            .into_iter()
            .filter(|s| s.strong_refs > 0)
            .filter_map(|s| {
                let rel = s
                    .generation_abs_path
                    .strip_prefix(&self.root)
                    .ok()?
                    .to_string_lossy()
                    .replace('\\', "/");
                Some((rel, s.publication_sequence, s.strong_refs))
            })
            .collect()
    }

    /// Non-mmap builds have no lease registry: no in-process leases exist.
    #[cfg(not(feature = "mmap"))]
    pub(crate) fn leased_generations(&self) -> Vec<(String, u64, usize)> {
        Vec::new()
    }

    /// Record retired artifacts (GC internal).
    pub(crate) fn record_retired(&self, entries: Vec<ClassifiedGeneration>) {
        self.retired.lock().extend(entries);
    }

    /// The full lifecycle observability report (packet requirement 5):
    /// root-lock owner state plus selected/previous/pinned/
    /// in-process-leased/eligible/retired IDs and reasons.
    ///
    /// The report is plain data — no strong reference to any generation
    /// base is taken, so producing it never prevents retirement.
    ///
    /// # Errors
    ///
    /// Returns [`RetirementError`] when the live manifest state cannot be
    /// read (a corrupt manifest fails closed rather than reporting a guess).
    pub fn lifecycle_report(
        &self,
        ownership: &RootOwnership,
    ) -> Result<RootLifecycleReport, RetirementError> {
        let owner = ownership.owner_state();
        let plan = plan_retirement(self)?;
        let leased = self.leased_generations();
        let mut leased_report = Vec::new();
        for (rel, _seq, strong) in &leased {
            leased_report.push(ClassifiedGeneration {
                path: rel.clone(),
                generation_id: None,
                class: RetentionClass::InProcessLeased,
                reason: format!("{strong} live in-process strong reference(s)"),
            });
        }
        let selected = plan
            .protected
            .iter()
            .find(|c| c.class == RetentionClass::Selected)
            .cloned();
        let previous = plan
            .protected
            .iter()
            .find(|c| c.class == RetentionClass::PreviousRecoveryRetained)
            .cloned();
        let pinned = plan
            .protected
            .iter()
            .filter(|c| c.class == RetentionClass::BackupPinned)
            .cloned()
            .collect();
        Ok(RootLifecycleReport {
            mode: owner.mode,
            canonical_root: owner.canonical_root,
            lock_path: owner.lock_path,
            selected,
            previous,
            backup_pinned: pinned,
            in_process_leased: leased_report,
            eligible: plan.eligible,
            retired: self.retired.lock().clone(),
        })
    }
}

/// The packet requirement-5 observability report: root-lock owner state
/// plus every retention class with IDs and reasons. Plain data only.
#[derive(Debug, Clone)]
pub struct RootLifecycleReport {
    /// The mode the root was opened with.
    pub mode: super::ownership::OpenMode,
    /// The canonical, exclusively locked root.
    pub canonical_root: PathBuf,
    /// The lock file path (`<root>/root.lock`).
    pub lock_path: PathBuf,
    /// The manifest-selected generation (absent on a genesis root).
    pub selected: Option<ClassifiedGeneration>,
    /// The previous-recovery-retained generation, when a second valid slot
    /// exists.
    pub previous: Option<ClassifiedGeneration>,
    /// Generations pinned by in-flight backups.
    pub backup_pinned: Vec<ClassifiedGeneration>,
    /// Generations held by live in-process read leases.
    pub in_process_leased: Vec<ClassifiedGeneration>,
    /// Artifacts eligible for collection right now.
    pub eligible: Vec<ClassifiedGeneration>,
    /// Artifacts already retired by this process, with deletion reasons.
    pub retired: Vec<ClassifiedGeneration>,
}
