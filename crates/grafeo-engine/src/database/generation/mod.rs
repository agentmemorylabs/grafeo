//! Engine-side generation lifecycle surface (G-EM0.3b–G-EM0.5c).
//!
//! This module groups the publication/recovery/lease/handoff contracts:
//!
//! - [`manifest`]: a read-only typed view over the W0 dual-slot manifest,
//!   exposing the selected/previous generation, publication sequence, durable
//!   WAL boundary, and overlay epoch.
//! - [`publication`]: the ordered publication phases, phase-tagged errors,
//!   and the extended [`publication::PublishedGeneration`] descriptor that
//!   retains the WAL boundary and overlay epoch a published generation
//!   represents.
//!
//! - [`recovery`]: runtime recovery + publication fault proof (G-EM0.3c) —
//!   typed recovery view with orphan classification, and the locked
//!   crash-point expectation surface for the fresh-process fault matrix.
//!
//! - [`lease`]: in-process generation leases and base transition (G-EM0.4a) —
//!   an explicit [`lease::GenerationLease`] owns the selected generation's
//!   mmap-backed base bytes; a [`lease::GenerationLeaseRegistry`] atomically
//!   redirects new snapshots on publication while existing readers finish
//!   against the old immutable bytes. Compiled only when `mmap` is enabled
//!   (the lease wraps [`crate::database::compact_tiered::CompactStoreTiered`]).
//!
//! - [`epoch_handoff`]: concurrent overlay epoch + WAL handoff (G-EM0.5c) —
//!   freeze epoch N at WAL boundary B, admit bounded N+1 writes, build/publish
//!   G(N) with the pre-cut boundary, retire only the frozen overlay/WAL prefix.
//!
//! Neither module re-implements the W0 manifest schema, the 11-step
//! publication ordering, WAL-cursor mechanics, or recovery selection — those
//! stay in `grafeo-storage` (W0). This module only re-exposes them at the
//! engine boundary and adds the observability G-EM0.3a did not provide.

#[cfg(all(
    feature = "generation",
    feature = "lpg",
    feature = "compact-store",
    feature = "generation-streaming"
))]
pub mod epoch_handoff;
#[cfg(feature = "mmap")]
pub mod lease;
pub mod manifest;
pub mod ownership;
pub mod publication;
pub mod recovery;
#[cfg(all(
    feature = "wal",
    feature = "lpg",
    feature = "generation",
    feature = "compact-store"
))]
pub mod replay;
pub mod retirement;

#[cfg(all(
    feature = "generation",
    feature = "lpg",
    feature = "compact-store",
    feature = "generation-streaming"
))]
pub use epoch_handoff::{
    EpochHandoffCoordinator, EpochHandoffPhase, EpochHandoffReport, FrozenEpochHandle,
};
#[cfg(feature = "mmap")]
pub use lease::{
    BaseGeneration, GenerationLease, GenerationLeaseRegistry, GenerationLeaseStats,
    GenerationTransitionError, TransitionReport,
};
pub use manifest::{
    ManifestSelection, ManifestState, ManifestStateError, WalBoundary, read_manifest_state,
};
#[cfg(feature = "mmap")]
pub use ownership::GenerationRootOwnership;
pub use ownership::{OpenMode, OwnershipError, RootLockOwnerState, RootOwnership};
pub use publication::{
    BuildPublication, PublicationPhase, PublicationPhaseError, PublishedGeneration,
};
pub use recovery::{
    ExpectedSelection, OrphanClassification, PublicationCrashPoint, RecoveryViewError,
    RootRecovery, recover_generation_root,
};
pub use retirement::{
    BACKUP_MANIFEST_NAME, BackedUpWalFile, BackupPin, BackupPinGuard, ClassifiedGeneration,
    GenerationBackupManifest, GenerationBackupReceipt, RetentionClass, RetirementAuthority,
    RetirementError, RetirementPlan, RootLifecycleReport, backup_generation_root,
    collect_retirement, plan_retirement, restore_generation_root,
};

/// Wall-clock milliseconds since the UNIX epoch, for observability
/// timestamps (ownership hold-since, backup-pin start, backup creation).
///
/// Kept in this module rather than reusing [`crate::database::backup::now_ms`]
/// so the generation-lifecycle surface does not depend on the legacy backup
/// module (which is gated behind the `wal` feature). Saturates to 0 before
/// the epoch.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| {
            // reason: millis since the UNIX epoch fits u64 for centuries
            #[allow(clippy::cast_possible_truncation)]
            let ms = d.as_millis() as u64;
            ms
        })
}
