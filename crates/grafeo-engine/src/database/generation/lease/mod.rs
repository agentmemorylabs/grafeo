//! In-process generation leases and base transition (G-EM0.4a).
//!
//! This module owns the engine-facing **lease** contract for the writable
//! generation mode: an explicit in-process [`GenerationLease`] owns the
//! selected generation's mmap-backed base bytes, and a
//! [`GenerationLeaseRegistry`] atomically redirects new snapshots on
//! publication while existing readers finish against the old immutable bytes.
//!
//! ## What this adds on top of W0/3a/3b/3c (and what it does not do)
//!
//! - The **manifest** (3b) records *which* generation is selected on disk;
//!   [`recover_generation_root`](super::recovery::recover_generation_root)
//!   (3c) validates and selects it after a restart. Neither tracks the
//!   *in-process* mapping lifetime of the base bytes a live database is
//!   actually serving from. This module closes that gap.
//! - [`BaseGeneration`] owns one immutable generation's CompactStore-section
//!   mapping (via `GrafeoFileManager::mmap_section`, CRC-validated) plus the
//!   zero-copy store deserialized from it. The lease wraps that base, tagging
//!   it with the selected generation's identity so a reader can prove which
//!   immutable base it holds.
//! - The registry's [`GenerationLeaseRegistry::publish`] redirects new readers
//!   to a new base with a single atomic `ArcSwap` store; it never mutates or
//!   deletes the old generation's bytes.
//!
//! ## Three reference classes (packet requirement 2)
//!
//! The implementation keeps three reference classes **distinct** so their
//! lifetimes can be proven independently:
//!
//! 1. **Database-owner** — the registry's strong `Arc<BaseGeneration>` for
//!    the currently-selected base. Held by the registry itself.
//! 2. **Selected-manifest** — the durable on-disk manifest slot naming the
//!    selected generation (owned by 3b/3c, *not* by this module). The lease
//!    carries the manifest's `publication_sequence` as identity.
//! 3. **External read-snapshot** — a [`GenerationLease`] cloned by a reader.
//!    Each holds a strong `Arc<BaseGeneration>` to a possibly-stale base.
//!
//! A lease count of zero for a non-selected base is proven by **weak
//! downgrade/drop evidence**: the registry downgrades its own strong
//! reference and asserts it cannot be upgraded once every external snapshot
//! has dropped. An `ArcSwap::swap` or a changed generation ID alone is never
//! accepted as proof of release (packet requirement 2).
//!
//! ## Platform note (`ERROR_USER_MAPPED_FILE`)
//!
//! The registry **never modifies or deletes** an old generation's bytes while
//! any in-process lease on it exists (packet requirement 3), so the Windows
//! `ERROR_USER_MAPPED_FILE` hazard cannot be triggered by this code path.
//! Deletion/GC of retired generations is explicitly out of scope (G-EM0.4b).
//! The exercised code paths (open/mmap/redirect) are OS-independent; the test
//! evidence here is Linux-only, and the old-reader → publish → new-reader →
//! old-reader-completion → final-mapping-release sequence keeps the old bytes
//! intact and readable throughout.

mod base;
mod registry;

pub use base::BaseGeneration;
pub use registry::{GenerationLeaseRegistry, GenerationLeaseStats, TransitionReport};

use std::path::Path;
use std::sync::{Arc, Weak};

use grafeo_common::utils::error::Error;
use grafeo_core::graph::compact::CompactStore;

/// A typed base-transition error (packet requirement 5).
///
/// Today this is constructed only by [`GenerationLeaseRegistry::publish`]
/// (the transition surface that can actually fail). `checkpoint_transition` /
/// `close_transition` return `Result` to reserve the same typed surface for
/// the G-EM0.4b retirement/GC validation, but are infallible today (always
/// `Ok`). Unlike `Drop` (which can only log), these surfaces return the
/// transition failure to the caller.
#[derive(Debug, thiserror::Error)]
pub enum GenerationTransitionError {
    /// A publish was attempted on a closed transition surface.
    #[error("publish on a closed generation lease registry (seq {sequence})")]
    PublishOnClosed {
        /// The publication sequence that was rejected.
        sequence: u64,
    },
    /// The new generation being published failed to open.
    #[error("failed to open generation {sequence} for publication: {source}")]
    OpenFailed {
        /// The publication sequence that failed to open.
        sequence: u64,
        /// The underlying open/decode error.
        source: Box<Error>,
    },
}

impl From<GenerationTransitionError> for Error {
    fn from(e: GenerationTransitionError) -> Self {
        Error::Internal(e.to_string())
    }
}

/// An in-process read-snapshot lease on one immutable base generation.
///
/// A `GenerationLease` holds a strong `Arc<BaseGeneration>`; while it is
/// alive the wrapped OS mapping cannot be released. Dropping the lease is the
/// read-completion signal. Leases are cheap `Arc` clones, safe on the query
/// hot path.
#[derive(Debug, Clone)]
pub struct GenerationLease {
    base: Arc<BaseGeneration>,
}

impl GenerationLease {
    /// Wrap a base in a read-snapshot lease (registry-internal).
    pub(super) fn new(base: Arc<BaseGeneration>) -> Self {
        Self { base }
    }

    /// The base store this lease serves (mmap-backed, immutable).
    #[must_use]
    pub fn store(&self) -> Arc<CompactStore> {
        self.base.store()
    }

    /// Manifest publication sequence of the leased base.
    #[must_use]
    pub fn publication_sequence(&self) -> u64 {
        self.base.publication_sequence()
    }

    /// The generation identifier of the leased base.
    #[must_use]
    pub fn generation_id(&self) -> &str {
        self.base.generation_id()
    }

    /// Absolute path of the leased generation container.
    #[must_use]
    pub fn generation_abs_path(&self) -> &Path {
        self.base.generation_abs_path()
    }

    /// A weak handle to the leased base, for drop/release evidence.
    ///
    /// The returned `Weak` upgrades while any strong reference (this lease,
    /// the registry, or another snapshot) survives and fails to upgrade once
    /// the final mapping has been released.
    #[must_use]
    pub fn downgrade(&self) -> Weak<BaseGeneration> {
        Arc::downgrade(&self.base)
    }

    /// Number of strong references to the leased base (owner + snapshots).
    #[must_use]
    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.base)
    }
}
