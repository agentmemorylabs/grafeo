//! Concurrent overlay epoch and WAL handoff (G-EM0.5c).
//!
//! # Contract
//!
//! 1. **Freeze** epoch N at exact WAL boundary B. New writes either enter
//!    bounded epoch N+1 (charged to `RetainedCategory::NextEpoch`) or block
//!    under G-EM0.5a admission.
//! 2. **Build** generation G(N) from the frozen input (materialized overlay
//!    payloads + mapped base) and **publish** it with boundary B through the
//!    G-EM0.3b path (`pre_cut_cursor`).
//! 3. **Retire** only the represented prefix of epoch N (overlay entities not
//!    re-mutated after freeze + frozen retained bytes). Epoch N+1 remains
//!    applied exactly once.
//!
//! # Linearization points
//!
//! | Phase | Linearization |
//! |-------|---------------|
//! | `FreezeCaptured` | WAL cut B durable; freeze id/payload snapshot taken; next epoch open |
//! | `Building` | G(N) streams only frozen input; N+1 writers concurrent |
//! | `Published` | Manifest sync (3b commit); selected generation = G(N) |
//! | `EpochRetired` | Frozen overlay prefix drained; WAL truncated at B (post-commit) |
//! | `Cancelled` / `Failed` | No WAL advance, no overlay retire, freeze slot cleared |
//!
//! Checkpoint/close while handoff is active return a typed error rather than
//! racing freeze/publication. Drop is best-effort cancel only.
//!
//! # Fault injection
//!
//! Under `debug_assertions`, `GRAFEO_5C_ABORT` may be set to one of:
//! `after_freeze`, `after_build`, `after_publication`, `after_retire` — the
//! process aborts at that boundary so a fresh-process parent can prove recovery.

use grafeo_common::utils::hash::FxHashSet;
use grafeo_core::graph::compact::generation::{GenerationEdge, GenerationNode};
use grafeo_core::graph::compact::generation_builder::FrozenOverlayEpoch;
use grafeo_core::graph::compact::overlay_budget::RetainedCategory;
use parking_lot::Mutex;

use super::super::manifest::WalBoundary;
use super::super::publication::BuildPublication;

/// Ordered phases of a concurrent epoch handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EpochHandoffPhase {
    /// No handoff in progress.
    Idle = 0,
    /// Epoch N frozen at WAL boundary B; N+1 open for writes.
    FreezeCaptured = 1,
    /// G(N) build running against frozen input.
    Building = 2,
    /// Manifest selection durable for G(N).
    Published = 3,
    /// Frozen overlay prefix retired; handoff complete.
    EpochRetired = 4,
    /// Handoff cancelled before commit; prior state intact.
    Cancelled = 5,
    /// Handoff failed; prior selected generation + full WAL recoverable.
    Failed = 6,
}

impl EpochHandoffPhase {
    /// Human-readable phase name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::FreezeCaptured => "freeze_captured",
            Self::Building => "building",
            Self::Published => "published",
            Self::EpochRetired => "epoch_retired",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    /// True after the 3b manifest commit point.
    #[must_use]
    pub fn is_post_commit(self) -> bool {
        matches!(self, Self::Published | Self::EpochRetired)
    }
}

/// Materialized freeze of epoch N ready for concurrent build + N+1 writes.
#[derive(Debug, Clone)]
pub struct FrozenEpochHandle {
    /// Frozen overlay epoch N.
    pub frozen_epoch: u64,
    /// Next epoch opened for concurrent writes.
    pub next_epoch: u64,
    /// Exact WAL boundary B cut at freeze.
    pub wal_boundary: WalBoundary,
    /// Identity freeze sets (for live layered bookkeeping).
    pub freeze: FrozenOverlayEpoch,
    /// Materialized overlay node payloads at freeze (budget-bounded).
    pub frozen_nodes: Vec<GenerationNode>,
    /// Materialized overlay edge payloads at freeze (budget-bounded).
    pub frozen_edges: Vec<GenerationEdge>,
    /// Selected base identity path when known (generation root relative).
    pub base_identity: Option<String>,
    /// Frozen retained-byte snapshot by category.
    pub frozen_category_bytes: [u64; RetainedCategory::COUNT],
    /// Aggregate frozen retained bytes.
    pub frozen_retained_bytes: u64,
}

/// Result of a complete freeze → build → publish → retire cycle.
#[derive(Debug, Clone)]
pub struct EpochHandoffReport {
    /// Final phase reached.
    pub phase: EpochHandoffPhase,
    /// Frozen epoch N.
    pub frozen_epoch: u64,
    /// Next epoch N+1 that remained applied.
    pub next_epoch: u64,
    /// WAL boundary B recorded in the published generation.
    pub wal_boundary: WalBoundary,
    /// Extended publication descriptor (present from `Published` onward).
    pub publication: Option<BuildPublication>,
    /// Count of frozen dirty nodes absorbed (not re-mutated after freeze).
    pub absorbed_nodes: u64,
    /// Count of frozen dirty edges absorbed.
    pub absorbed_edges: u64,
    /// Count of post-freeze node mutations retained in the overlay.
    pub retained_next_epoch_nodes: u64,
    /// Count of post-freeze edge mutations retained in the overlay.
    pub retained_next_epoch_edges: u64,
    /// Frozen-identity node ids: every overlay-resident/dirty node captured at
    /// freeze, all absorbed into the published generation (G-EM0.5d repair
    /// swap input).
    pub freeze_node_ids: FxHashSet<u64>,
    /// Frozen-identity edge ids (see [`Self::freeze_node_ids`]).
    pub freeze_edge_ids: FxHashSet<u64>,
    /// Node ids mutated AFTER the freeze (epoch N+1), as recorded by the
    /// layered store's mutation paths during the handoff.
    /// `swap_base_and_repair_overlay` retains dirty for exactly these so
    /// N+1 writes and deletions survive the base swap (G-EM0.5d hardening
    /// 2026-08-02: previously only the COUNT was propagated and the repair
    /// predicate could not distinguish re-mutated frozen entities).
    pub post_freeze_nodes: FxHashSet<u64>,
    /// Edge ids mutated after the freeze (epoch N+1); see
    /// [`Self::post_freeze_nodes`].
    pub post_freeze_edges: FxHashSet<u64>,
}

/// In-process handoff coordinator state (at most one active handoff per DB).
#[derive(Debug, Default)]
pub(super) struct HandoffSlot {
    pub(super) phase: EpochHandoffPhase,
    pub(super) handle: Option<FrozenEpochHandle>,
}

impl Default for EpochHandoffPhase {
    fn default() -> Self {
        Self::Idle
    }
}

/// Engine-side dual-epoch handoff surface attached to [`GrafeoDB`].
pub struct EpochHandoffCoordinator {
    pub(super) slot: Mutex<HandoffSlot>,
}

impl Default for EpochHandoffCoordinator {
    fn default() -> Self {
        Self {
            slot: Mutex::new(HandoffSlot::default()),
        }
    }
}

impl EpochHandoffCoordinator {
    /// Creates an idle coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Current phase (observability).
    #[must_use]
    pub fn phase(&self) -> EpochHandoffPhase {
        self.slot.lock().phase
    }

    /// True when a freeze is held and not yet fully retired/cancelled.
    #[must_use]
    pub fn is_active(&self) -> bool {
        !matches!(
            self.phase(),
            EpochHandoffPhase::Idle
                | EpochHandoffPhase::EpochRetired
                | EpochHandoffPhase::Cancelled
                | EpochHandoffPhase::Failed
        )
    }
}
