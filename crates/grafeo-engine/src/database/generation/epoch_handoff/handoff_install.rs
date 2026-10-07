//! Combined production handoff install (H-ADOPT.2 item 4).
//!
//! [`GrafeoDB::publish_and_install_handoff`] is the ONE engine surface a
//! quiesced maintenance caller uses to install a completed epoch handoff as
//! the live base of a writable generation-root database:
//!
//! 1. **Preconditions** (fail-closed, before anything is published): the
//!    report must be `EpochRetired` and carry its publication descriptor,
//!    and the layered store must hold the retired handoff's live state.
//!    Writers keep running from the freeze to here (DESIGN G2); the install
//!    stops them briefly — the handoff gate and the commit-order lock — and
//!    refuses (retryable) while a write transaction is open.
//! 2. **Leases publish**: the generation container is already durable (the
//!    handoff's publication committed it to the manifest); the registry's
//!    [`GenerationLeaseRegistry::publish`] atomically redirects new
//!    snapshots to it while existing readers finish against the old
//!    immutable bytes. Durable-first: a crash between publish and swap loses
//!    nothing (reopen re-derives the registry from the manifest).
//! 3. **Base swap**: `lease.store()` becomes the layered store's new base
//!    via [`LayeredStore::install_handoff_base`], which repairs the overlay
//!    with every write made since the freeze: N+1 wins over the new base.
//!
//! The install runs entirely under the DB-lifetime exclusive root lock held
//! by [`super::super::GenerationRootOwnership`] (the handoff's freeze/build
//! reuse that lock rather than re-acquiring — see `handoff.rs`).

use std::path::PathBuf;
use std::sync::Arc;

use grafeo_common::types::PropertyKey;
use grafeo_common::utils::error::{Error, Result};
use grafeo_common::utils::hash::FxHashSet;

use super::types::EpochHandoffPhase;
use super::types::EpochHandoffReport;
use crate::database::GrafeoDB;

/// Result of a successful publish → install handoff.
///
/// Carries the identity and size of the newly-installed base generation for
/// operator logging (AMH maintenance path): the generation that the lease
/// registry now redirects to, and the base's node/edge counts as served by
/// the swapped-in container.
#[derive(Debug, Clone)]
pub struct HandoffInstallReport {
    /// Manifest publication sequence of the newly-selected generation.
    pub publication_sequence: u64,
    /// Caller-supplied identifier of the newly-selected generation.
    pub generation_id: String,
    /// Absolute path of the newly-installed generation container.
    pub generation_abs_path: PathBuf,
    /// Total node count of the newly-installed base store.
    pub base_node_count: u64,
    /// Total edge count of the newly-installed base store.
    pub base_edge_count: u64,
}

/// Typed failure modes of [`GrafeoDB::publish_and_install_handoff`].
///
/// Every variant is produced by the named precondition; none is ever
/// constructed speculatively. The variant names are the fail-closed contract
/// for the maintenance caller.
#[derive(Debug, thiserror::Error)]
pub enum HandoffInstallError {
    /// `GrafeoDB::generation_root` is `None` (not opened as a generation
    /// root), so no lease registry exists to publish through.
    #[error(
        "publish_and_install_handoff requires a generation-root database \
         (generation_root is None); open via GrafeoDB::open_generation_root"
    )]
    NotGenerationRoot,
    /// The database is not in the retired state (a handoff is active, or
    /// none has completed), so there is nothing safe to install.
    #[error(
        "publish_and_install_handoff requires the DB handoff phase EpochRetired, got {phase:?}"
    )]
    NotRetired {
        /// The observed database handoff phase.
        phase: EpochHandoffPhase,
    },
    /// The report does not carry its publication descriptor, so there is no
    /// durable generation to publish/install.
    #[error(
        "publish_and_install_handoff requires a report with a publication \
         descriptor (phase {phase:?} carries none)"
    )]
    MissingPublication {
        /// The report's phase (pre-publication phase).
        phase: EpochHandoffPhase,
    },
    /// The layered store holds no retired handoff waiting for its install
    /// (the report was already installed, or the handoff was cancelled).
    #[error("publish_and_install_handoff: no retired handoff is waiting for its install")]
    NothingToInstall,
    /// The generation-root database has no layered store (never happens
    /// through [`crate::database::GrafeoDB::open_generation_root`], which
    /// installs one fail-closed).
    #[error("publish_and_install_handoff requires the layered store")]
    NoLayeredStore,
}

impl From<HandoffInstallError> for Error {
    fn from(e: HandoffInstallError) -> Self {
        Error::Internal(e.to_string())
    }
}

impl GrafeoDB {
    /// Combined production handoff install: publish the handoff generation
    /// through the lease registry and swap it in as the layered base,
    /// repairing the overlay with the writes made since the freeze.
    ///
    /// The caller must have completed an epoch handoff on THIS database
    /// (freeze → build → publish → retire, e.g. via
    /// [`GrafeoDB::run_epoch_handoff`]) and pass the returned
    /// [`EpochHandoffReport`]. Writers may run throughout (DESIGN G2): the
    /// install stops them only for the publish and swap.
    ///
    /// # Errors
    ///
    /// Returns [`HandoffInstallError::NotGenerationRoot`] when the database
    /// was not opened as a writable generation root,
    /// [`HandoffInstallError::NotRetired`] when the database phase is not
    /// `EpochRetired`, [`HandoffInstallError::MissingPublication`] when the
    /// report carries no publication descriptor,
    /// [`HandoffInstallError::NothingToInstall`] when the layered store holds
    /// no retired handoff, or [`HandoffInstallError::NoLayeredStore`] when the
    /// layered store is absent. Returns [`Error::AdmissionRetryable`] while a
    /// write transaction is open (nothing is published or swapped).
    /// Propagates lease-registry publication errors as `Error::Internal` (typed
    /// [`crate::database::generation::lease::GenerationTransitionError`]
    /// inside).
    #[cfg(all(
        feature = "generation",
        feature = "lpg",
        feature = "compact-store",
        feature = "generation-streaming",
        feature = "mmap"
    ))]
    pub fn publish_and_install_handoff(
        &self,
        report: EpochHandoffReport,
    ) -> Result<HandoffInstallReport> {
        // ── (a) Fail-closed assertions, BEFORE any publication ────────────
        let ownership = self
            .generation_root
            .as_ref()
            .ok_or(HandoffInstallError::NotGenerationRoot)?;
        if self.epoch_handoff_phase() != EpochHandoffPhase::EpochRetired {
            return Err(HandoffInstallError::NotRetired {
                phase: self.epoch_handoff_phase(),
            }
            .into());
        }
        if report.phase != EpochHandoffPhase::EpochRetired {
            return Err(HandoffInstallError::NotRetired {
                phase: report.phase,
            }
            .into());
        }
        let publication =
            report
                .publication
                .as_ref()
                .ok_or(HandoffInstallError::MissingPublication {
                    phase: report.phase,
                })?;

        let layered = self
            .layered_store
            .as_ref()
            .ok_or(HandoffInstallError::NoLayeredStore)?;

        // Brief writer stop (DESIGN G2): the gate holds direct writes, the
        // commit-order lock holds commits, and the merge guard holds store
        // mutations, from the checks below through the swap. The repair
        // reads committed state only, so an open transaction with in-place
        // writes defers the install (retryable). Lock order as the freeze.
        let _gate = self.handoff_gate.write();
        #[cfg(feature = "wal")]
        let _commit_order = self.wal_commit_order.lock();
        let barrier = layered.freeze_write_barrier();
        self.refuse_open_write_transactions("install")?;
        if !layered.handoff_live().is_some_and(|live| live.retired) {
            return Err(HandoffInstallError::NothingToInstall.into());
        }
        // Spilled vectors live outside the overlay rows (ForceDisk). Held for
        // the whole swap: a spill drains columns under its own upgradable
        // read, which a plain read would not exclude, and a reload takes the
        // write lock.
        #[cfg(all(feature = "vector-index", feature = "mmap", not(feature = "temporal")))]
        let spill_guard = self
            .vector_spill_storages
            .as_ref()
            .map(|r| r.upgradable_read());
        #[cfg(all(feature = "vector-index", feature = "mmap", not(feature = "temporal")))]
        let spilled_properties: FxHashSet<PropertyKey> = spill_guard
            .as_deref()
            .into_iter()
            .flat_map(|registry| registry.keys())
            .filter_map(|key| {
                key.split_once(':')
                    .map(|(_, property)| PropertyKey::new(property))
            })
            .collect();
        #[cfg(not(all(feature = "vector-index", feature = "mmap", not(feature = "temporal"))))]
        let spilled_properties: FxHashSet<PropertyKey> = FxHashSet::default();
        grafeo_common::testing::crash::park_point("handoff_install");

        // ── (b) Leases publish (durable-first: the container is already on
        //        disk and manifest-selected; reopen re-derives the registry
        //        from the manifest, so a crash before the swap loses
        //        nothing) ──────────────────────────────────────────────────
        let publication_sequence = publication.publication.publication_sequence;
        let generation_id = publication.publication.generation_id.clone();
        let generation_abs_path = publication.generation_abs_path.clone();
        let lease = ownership.registry().publish(
            publication_sequence,
            generation_id.clone(),
            generation_abs_path.clone(),
        )?;

        // ── (c) Base swap + repair (N+1 wins over the new base) ───────────
        let new_base = lease.store();
        let (_old_base, _live) = layered
            .install_handoff_base(&barrier, Arc::clone(&new_base), &|_, key| {
                spilled_properties.contains(key)
            })
            .map_err(Error::Internal)?;

        Ok(HandoffInstallReport {
            publication_sequence,
            generation_id,
            generation_abs_path,
            base_node_count: new_base.total_nodes(),
            base_edge_count: new_base.total_edges(),
        })
    }
}
