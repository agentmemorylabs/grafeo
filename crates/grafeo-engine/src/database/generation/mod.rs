//! Engine-side manifest publication and WAL boundary surface (G-EM0.3b).
//!
//! This module groups the G-EM0.3b publication contract:
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
//! Neither module re-implements the W0 manifest schema, the 11-step
//! publication ordering, WAL-cursor mechanics, or recovery selection — those
//! stay in `grafeo-storage` (W0). This module only re-exposes them at the
//! engine boundary and adds the observability G-EM0.3a did not provide.

#[cfg(feature = "mmap")]
pub mod lease;
pub mod manifest;
pub mod publication;
pub mod recovery;

#[cfg(feature = "mmap")]
pub use lease::{
    BaseGeneration, GenerationLease, GenerationLeaseRegistry, GenerationLeaseStats,
    GenerationTransitionError, TransitionReport,
};
pub use manifest::{
    ManifestSelection, ManifestState, ManifestStateError, WalBoundary, read_manifest_state,
};
pub use publication::{
    BuildPublication, PublicationPhase, PublicationPhaseError, PublishedGeneration,
};
pub use recovery::{
    ExpectedSelection, OrphanClassification, PublicationCrashPoint, RecoveryViewError,
    RootRecovery, recover_generation_root,
};
