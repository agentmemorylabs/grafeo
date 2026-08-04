//! Combined production handoff install (H-ADOPT.2 item 4).
//!
//! [`GrafeoDB::publish_and_install_handoff`] is the ONE engine surface a
//! quiesced maintenance caller uses to install a completed epoch handoff as
//! the live base of a writable generation-root database:
//!
//! 1. **Real zero-writer assertion** (fail-closed, before anything is
//!    published): the report must be `EpochRetired`, carry its publication
//!    descriptor, and its `post_freeze_*` sets — the live handoff's
//!    post-freeze identity snapshotted under the handoff lock at retire —
//!    must be EMPTY. Any write recorded between freeze and retire is a real
//!    invariant violation (the caller's maintenance window must quiesce
//!    writers); nothing is published or swapped.
//! 2. **Leases publish**: the generation container is already durable (the
//!    handoff's publication committed it to the manifest); the registry's
//!    [`GenerationLeaseRegistry::publish`] atomically redirects new
//!    snapshots to it while existing readers finish against the old
//!    immutable bytes. Durable-first: a crash between publish and swap loses
//!    nothing (reopen re-derives the registry from the manifest).
//! 3. **Base swap**: `lease.store()` becomes the layered store's new base
//!    via [`LayeredStore::swap_base_and_repair_overlay`] with the report's
//!    freeze/post-freeze identity sets (selective undirty — G-EM0.5d).
//!
//! The install runs entirely under the DB-lifetime exclusive root lock held
//! by [`super::super::GenerationRootOwnership`] (the handoff's freeze/build
//! reuse that lock rather than re-acquiring — see `handoff.rs`).

use std::path::PathBuf;
use std::sync::Arc;

use grafeo_common::types::{EdgeId, NodeId};
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
    /// THE zero-writer assertion: writes were recorded between freeze and
    /// retire, so the published generation does not represent the live
    /// overlay and the repair swap could shadow accepted N+1 values.
    #[error(
        "writes occurred during the handoff window: {nodes} post-freeze node(s) \
         and {edges} post-freeze edge(s) were recorded between freeze and \
         retire; quiesce writers before publish_and_install_handoff"
    )]
    WritesDuringWindow {
        /// Number of post-freeze node ids recorded on the report.
        nodes: usize,
        /// Number of post-freeze edge ids recorded on the report.
        edges: usize,
    },
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
    /// Combined production handoff install: assert the quiesced zero-writer
    /// invariant, publish the handoff generation through the lease registry,
    /// and swap it in as the layered base.
    ///
    /// The caller must have completed an epoch handoff on THIS database
    /// (freeze → build → publish → retire, e.g. via
    /// [`GrafeoDB::run_epoch_handoff`]) inside a quiesced maintenance window
    /// and pass the returned [`EpochHandoffReport`].
    ///
    /// # Errors
    ///
    /// Returns [`HandoffInstallError::NotGenerationRoot`] when the database
    /// was not opened as a writable generation root,
    /// [`HandoffInstallError::NotRetired`] when the database phase is not
    /// `EpochRetired`, [`HandoffInstallError::MissingPublication`] when the
    /// report carries no publication descriptor,
    /// [`HandoffInstallError::WritesDuringWindow`] when the report's
    /// post-freeze identity sets are non-empty (a real zero-writer
    /// violation — nothing is published or swapped), or
    /// [`HandoffInstallError::NoLayeredStore`] when the layered store is
    /// absent. Propagates lease-registry publication errors as
    /// `Error::Internal` (typed
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

        // THE zero-writer assertion. The report's post-freeze identity is
        // snapshotted from the live handoff state under the handoff lock at
        // retire (complete_epoch_handoff), so any non-empty set is proof that
        // a write landed between freeze and retire — an invariant violation
        // the repair swap cannot honor. Fail closed: no publish, no swap.
        if !report.post_freeze_nodes.is_empty() || !report.post_freeze_edges.is_empty() {
            return Err(HandoffInstallError::WritesDuringWindow {
                nodes: report.post_freeze_nodes.len(),
                edges: report.post_freeze_edges.len(),
            }
            .into());
        }
        // Defense-in-depth: if a live handoff slot still exists (a state the
        // retire path should have cleared), its post-freeze identity must
        // also be empty — read under the handoff lock.
        if let Some(layered) = self.layered_store.as_ref()
            && let Some(live) = layered.handoff_live()
            && (!live.post_freeze_nodes.is_empty() || !live.post_freeze_edges.is_empty())
        {
            return Err(HandoffInstallError::WritesDuringWindow {
                nodes: live.post_freeze_nodes.len(),
                edges: live.post_freeze_edges.len(),
            }
            .into());
        }
        let layered = self
            .layered_store
            .as_ref()
            .ok_or(HandoffInstallError::NoLayeredStore)?;

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

        // ── (c) Base swap (selective undirty, G-EM0.5d) ───────────────────
        let new_base = lease.store();
        let frozen_node_ids = to_node_ids(&report.freeze_node_ids);
        let frozen_edge_ids = to_edge_ids(&report.freeze_edge_ids);
        let post_freeze_node_ids = to_node_ids(&report.post_freeze_nodes);
        let post_freeze_edge_ids = to_edge_ids(&report.post_freeze_edges);
        let _old_base = layered.swap_base_and_repair_overlay(
            Arc::clone(&new_base),
            &frozen_node_ids,
            &frozen_edge_ids,
            &post_freeze_node_ids,
            &post_freeze_edge_ids,
        );

        Ok(HandoffInstallReport {
            publication_sequence,
            generation_id,
            generation_abs_path,
            base_node_count: new_base.total_nodes(),
            base_edge_count: new_base.total_edges(),
        })
    }
}

/// Convert a report's raw `u64` id set to `NodeId`s (freeze/post-freeze
/// identity is stored raw on the report; the repair swap keys on `NodeId`).
fn to_node_ids(raw: &FxHashSet<u64>) -> FxHashSet<NodeId> {
    raw.iter().map(|id| NodeId::new(*id)).collect()
}

/// Edge variant of [`to_node_ids`].
fn to_edge_ids(raw: &FxHashSet<u64>) -> FxHashSet<EdgeId> {
    raw.iter().map(|id| EdgeId::new(*id)).collect()
}
