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

use std::path::Path;
use std::sync::Arc;

use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};
use grafeo_common::utils::error::{Error, Result};
use grafeo_common::utils::hash::FxHashMap;
use grafeo_core::graph::compact::generation::{
    EdgeRecordSource, GenerationEdge, GenerationError, GenerationNode, NodeRecordSource,
    OriginalEdgeId, OriginalNodeId,
};
use grafeo_core::graph::compact::generation_builder::FrozenOverlayEpoch;
use grafeo_core::graph::compact::layered::OverlayHandoffLive;
use grafeo_core::graph::compact::overlay_budget::RetainedCategory;
use grafeo_core::graph::lpg::{Edge, Node};
use grafeo_storage::file::generation_writer::{
    ExactSectionSource, GenerationContainerHeader, OsGenerationFileOps,
};
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::publication::{PublicationInput, publish_generation};
use grafeo_storage::generation::wal_cursor::cut_generation_boundary;
use grafeo_storage::wal::WalManager;
use parking_lot::Mutex;

use super::manifest::WalBoundary;
use super::publication::{BuildPublication, PublicationPhaseError, assemble_build_publication};
use crate::database::GrafeoDB;
use crate::database::generation_build::GenerationBuildRequest;

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
}

/// In-process handoff coordinator state (at most one active handoff per DB).
#[derive(Debug, Default)]
struct HandoffSlot {
    phase: EpochHandoffPhase,
    handle: Option<FrozenEpochHandle>,
}

impl Default for EpochHandoffPhase {
    fn default() -> Self {
        Self::Idle
    }
}

/// Engine-side dual-epoch handoff surface attached to [`GrafeoDB`].
pub struct EpochHandoffCoordinator {
    slot: Mutex<HandoffSlot>,
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
        )
    }
}

// ── Frozen record sources (build path) ─────────────────────────────

struct FrozenNodeSource {
    nodes: Vec<GenerationNode>,
    pos: usize,
}

impl NodeRecordSource for FrozenNodeSource {
    fn next_node(&mut self) -> std::result::Result<Option<GenerationNode>, GenerationError> {
        if self.pos >= self.nodes.len() {
            return Ok(None);
        }
        let n = self.nodes[self.pos].clone();
        self.pos += 1;
        Ok(Some(n))
    }
}

struct FrozenEdgeSource {
    edges: Vec<GenerationEdge>,
    pos: usize,
}

impl EdgeRecordSource for FrozenEdgeSource {
    fn next_edge(&mut self) -> std::result::Result<Option<GenerationEdge>, GenerationError> {
        if self.pos >= self.edges.len() {
            return Ok(None);
        }
        let e = self.edges[self.pos].clone();
        self.pos += 1;
        Ok(Some(e))
    }
}

fn map_generation_error(err: GenerationError) -> Error {
    Error::Internal(format!("generation build: {err}"))
}

fn map_publication_error(err: grafeo_storage::generation::publication::PublicationError) -> Error {
    PublicationPhaseError::from_publication(err).into()
}

fn node_to_generation(node: &Node) -> GenerationNode {
    let mut labels: Vec<String> = node.labels.iter().map(|l| l.to_string()).collect();
    labels.sort();
    labels.dedup();
    let properties: FxHashMap<PropertyKey, Value> = node
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    GenerationNode {
        id: OriginalNodeId::new(node.id.as_u64()),
        labels,
        properties,
    }
}

fn edge_to_generation(edge: &Edge) -> GenerationEdge {
    let properties: FxHashMap<PropertyKey, Value> = edge
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    GenerationEdge {
        id: OriginalEdgeId::new(edge.id.as_u64()),
        src: OriginalNodeId::new(edge.src.as_u64()),
        dst: OriginalNodeId::new(edge.dst.as_u64()),
        edge_type: edge.edge_type.to_string(),
        properties,
    }
}

#[cfg(debug_assertions)]
fn maybe_abort(point: &str) {
    if std::env::var("GRAFEO_5C_ABORT").ok().as_deref() == Some(point) {
        std::process::abort();
    }
}

#[cfg(not(debug_assertions))]
fn maybe_abort(_point: &str) {}

impl GrafeoDB {
    /// Freeze epoch N at WAL boundary B and open bounded epoch N+1 for writes.
    ///
    /// # Errors
    ///
    /// Returns when the root lock cannot be acquired, the WAL cut fails, a
    /// handoff is already active, or the live graph cannot be snapshotted.
    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    pub fn freeze_epoch_for_handoff(&self, generation_root: &Path) -> Result<FrozenEpochHandle> {
        let mut slot = self.epoch_handoff.slot.lock();
        if !matches!(
            slot.phase,
            EpochHandoffPhase::Idle
                | EpochHandoffPhase::EpochRetired
                | EpochHandoffPhase::Cancelled
        ) {
            return Err(Error::Internal(format!(
                "epoch handoff already active in phase {}",
                slot.phase.name()
            )));
        }

        if generation_root.is_file() {
            return Err(Error::Internal(
                "refusing to freeze into a standalone .grafeo file; use a generation root directory"
                    .into(),
            ));
        }
        std::fs::create_dir_all(generation_root)?;
        let generation_root = std::fs::canonicalize(generation_root)
            .map_err(|e| Error::Internal(format!("canonicalize generation root: {e}")))?;

        let wal_boundary = {
            let _lock = RootLock::try_acquire(&generation_root)
                .map_err(|e| Error::Internal(format!("generation root lock: {e}")))?;
            let wal_dir = generation_root.join("wal");
            std::fs::create_dir_all(&wal_dir)?;
            let wal = WalManager::open(&wal_dir)?;
            // Cut boundary B first so concurrent N+1 writes land after B.
            let cut = cut_generation_boundary(&wal)
                .map_err(|e| Error::Internal(format!("WAL freeze cut: {e}")))?;
            WalBoundary::from_cursor(&cut.cursor)
            // RootLock drops here before N+1 writers and the later build re-acquire.
        };

        let frozen_epoch = self.transaction_manager.current_epoch().0;
        let next_epoch = frozen_epoch.saturating_add(1);

        // Advance engine + overlay epoch so new commits land at N+1.
        self.transaction_manager
            .sync_epoch(grafeo_common::types::EpochId::new(next_epoch));
        #[cfg(feature = "lpg")]
        {
            if let Some(layered) = self.layered_store.as_ref() {
                layered
                    .overlay_store()
                    .sync_epoch(grafeo_common::types::EpochId::new(next_epoch));
            } else if let Some(store) = self.store.as_ref() {
                store.sync_epoch(grafeo_common::types::EpochId::new(next_epoch));
            }
        }

        // Capture freeze identity + materialize overlay payloads.
        let (freeze, frozen_nodes, frozen_edges) =
            self.capture_frozen_overlay_payloads(frozen_epoch)?;

        // Snapshot frozen retained accounting (both frozen + next count later).
        let mut frozen_category_bytes = [0u64; RetainedCategory::COUNT];
        let mut frozen_retained_bytes = 0u64;
        if let Some(ctl) = self.overlay_admission() {
            let snap = ctl.snapshot();
            for cat in [
                RetainedCategory::MutationPayload,
                RetainedCategory::DirtySets,
                RetainedCategory::DeletionSets,
            ] {
                let b = snap.categories[cat.index()].current_bytes;
                frozen_category_bytes[cat.index()] = b;
                frozen_retained_bytes = frozen_retained_bytes.saturating_add(b);
            }
            ctl.set_active_epoch(next_epoch);
        }

        if let Some(layered) = self.layered_store.as_ref() {
            let live = OverlayHandoffLive {
                frozen_epoch,
                next_epoch,
                freeze_node_ids: freeze.overlay_node_ids.clone(),
                freeze_edge_ids: freeze.overlay_edge_ids.clone(),
                freeze_deleted_nodes: freeze.deleted_base_node_ids.clone(),
                freeze_deleted_edges: freeze.deleted_base_edge_ids.clone(),
                post_freeze_nodes: Default::default(),
                post_freeze_edges: Default::default(),
                frozen_retained_bytes,
                frozen_category_bytes,
            };
            layered.begin_epoch_handoff(live).map_err(Error::Internal)?;
        }

        let handle = FrozenEpochHandle {
            frozen_epoch,
            next_epoch,
            wal_boundary,
            freeze,
            frozen_nodes,
            frozen_edges,
            base_identity: None,
            frozen_category_bytes,
            frozen_retained_bytes,
        };

        slot.phase = EpochHandoffPhase::FreezeCaptured;
        slot.handle = Some(handle.clone());
        drop(slot);

        maybe_abort("after_freeze");
        Ok(handle)
    }

    /// Build G(N) from a freeze handle, publish with boundary B, and retire
    /// the frozen overlay/WAL prefix. Epoch N+1 remains applied once.
    ///
    /// # Errors
    ///
    /// Returns phase-tagged errors on build/publication failure. On pre-commit
    /// failure the freeze is cancelled and prior state remains recoverable.
    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    pub fn complete_epoch_handoff(
        &self,
        handle: FrozenEpochHandle,
        request: GenerationBuildRequest,
    ) -> Result<EpochHandoffReport> {
        {
            let mut slot = self.epoch_handoff.slot.lock();
            if slot.phase != EpochHandoffPhase::FreezeCaptured {
                return Err(Error::Internal(format!(
                    "complete_epoch_handoff requires FreezeCaptured, got {}",
                    slot.phase.name()
                )));
            }
            slot.phase = EpochHandoffPhase::Building;
        }

        let build_result = self.build_publish_frozen(handle.clone(), request);
        let publication = match build_result {
            Ok(p) => p,
            Err(e) => {
                self.cancel_epoch_handoff_inner(EpochHandoffPhase::Failed);
                return Err(e);
            }
        };

        maybe_abort("after_publication");

        {
            let mut slot = self.epoch_handoff.slot.lock();
            slot.phase = EpochHandoffPhase::Published;
        }

        // Retire frozen overlay prefix; N+1 remains.
        let (absorbed_nodes, absorbed_edges, retained_n, retained_e) =
            self.retire_frozen_prefix(&handle)?;

        maybe_abort("after_retire");

        {
            let mut slot = self.epoch_handoff.slot.lock();
            slot.phase = EpochHandoffPhase::EpochRetired;
            slot.handle = None;
        }

        Ok(EpochHandoffReport {
            phase: EpochHandoffPhase::EpochRetired,
            frozen_epoch: handle.frozen_epoch,
            next_epoch: handle.next_epoch,
            wal_boundary: publication.publication.wal_boundary,
            publication: Some(publication),
            absorbed_nodes,
            absorbed_edges,
            retained_next_epoch_nodes: retained_n,
            retained_next_epoch_edges: retained_e,
        })
    }

    /// One-shot freeze → build → publish → retire.
    ///
    /// # Errors
    ///
    /// Propagates freeze or complete errors.
    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    pub fn run_epoch_handoff(&self, request: GenerationBuildRequest) -> Result<EpochHandoffReport> {
        let root = request.generation_root.clone();
        let handle = self.freeze_epoch_for_handoff(&root)?;
        self.complete_epoch_handoff(handle, request)
    }

    /// Cancel an in-progress pre-commit handoff. No-op when idle/retired.
    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    pub fn cancel_epoch_handoff(&self) {
        self.cancel_epoch_handoff_inner(EpochHandoffPhase::Cancelled);
    }

    /// Current handoff phase.
    #[must_use]
    pub fn epoch_handoff_phase(&self) -> EpochHandoffPhase {
        self.epoch_handoff.phase()
    }

    /// True when freeze is held and not fully retired/cancelled.
    #[must_use]
    pub fn epoch_handoff_active(&self) -> bool {
        self.epoch_handoff.is_active()
    }

    // ── internals ──────────────────────────────────────────────────

    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    fn cancel_epoch_handoff_inner(&self, phase: EpochHandoffPhase) {
        if let Some(layered) = self.layered_store.as_ref() {
            layered.end_epoch_handoff();
        }
        let mut slot = self.epoch_handoff.slot.lock();
        slot.phase = phase;
        slot.handle = None;
    }

    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    fn capture_frozen_overlay_payloads(
        &self,
        frozen_epoch: u64,
    ) -> Result<(FrozenOverlayEpoch, Vec<GenerationNode>, Vec<GenerationEdge>)> {
        if let Some(layered) = self.layered_store.as_ref() {
            // Freeze identity = every entity currently resident in the overlay
            // (created-or-modified), plus base-deletion tombstones. Dirty-set
            // alone is insufficient: after compact(), GrafeoDB::create_node*
            // writes the shared overlay Arc directly and may not mark dirty.
            let overlay = layered.overlay_store();
            let mut overlay_node_ids: grafeo_common::utils::hash::FxHashSet<u64> = overlay
                .all_node_ids()
                .into_iter()
                .map(|id| id.as_u64())
                .collect();
            for id in layered.snapshot_dirty_node_ids() {
                overlay_node_ids.insert(id.as_u64());
            }
            let mut overlay_edge_ids: grafeo_common::utils::hash::FxHashSet<u64> =
                Default::default();
            for e in overlay.all_edges() {
                overlay_edge_ids.insert(e.id.as_u64());
            }
            for id in layered.snapshot_dirty_edge_ids() {
                overlay_edge_ids.insert(id.as_u64());
            }
            let deleted_base_node_ids: grafeo_common::utils::hash::FxHashSet<u64> = layered
                .snapshot_deleted_node_ids()
                .into_iter()
                .map(|id| id.as_u64())
                .collect();
            let deleted_base_edge_ids: grafeo_common::utils::hash::FxHashSet<u64> = layered
                .snapshot_deleted_edge_ids()
                .into_iter()
                .map(|id| id.as_u64())
                .collect();
            let freeze = FrozenOverlayEpoch {
                epoch: frozen_epoch,
                overlay_node_ids: overlay_node_ids.clone(),
                overlay_edge_ids: overlay_edge_ids.clone(),
                deleted_base_node_ids,
                deleted_base_edge_ids,
            };
            let mut nodes = Vec::with_capacity(overlay_node_ids.len());
            let mut node_ids: Vec<u64> = overlay_node_ids.into_iter().collect();
            node_ids.sort_unstable();
            for raw in node_ids {
                if let Some(n) = overlay.get_node(NodeId::new(raw)) {
                    nodes.push(node_to_generation(&n));
                }
            }
            let mut edges = Vec::with_capacity(overlay_edge_ids.len());
            let mut edge_ids: Vec<u64> = overlay_edge_ids.into_iter().collect();
            edge_ids.sort_unstable();
            for raw in edge_ids {
                if let Some(e) = overlay.get_edge(EdgeId::new(raw)) {
                    edges.push(edge_to_generation(&e));
                }
            }
            return Ok((freeze, nodes, edges));
        }

        // Pure LPG: whole store is the overlay.
        if let Some(store) = self.store.as_ref() {
            let mut overlay_node_ids = grafeo_common::utils::hash::FxHashSet::default();
            let mut nodes = Vec::new();
            let mut node_ids = store.all_node_ids();
            node_ids.sort_unstable();
            for id in node_ids {
                overlay_node_ids.insert(id.as_u64());
                if let Some(n) = store.get_node(id) {
                    nodes.push(node_to_generation(&n));
                }
            }
            let mut overlay_edge_ids = grafeo_common::utils::hash::FxHashSet::default();
            let mut edges = Vec::new();
            let mut edge_list: Vec<Edge> = store.all_edges().collect();
            edge_list.sort_by_key(|e| e.id.as_u64());
            for e in edge_list {
                overlay_edge_ids.insert(e.id.as_u64());
                edges.push(edge_to_generation(&e));
            }
            let freeze = FrozenOverlayEpoch {
                epoch: frozen_epoch,
                overlay_node_ids,
                overlay_edge_ids,
                deleted_base_node_ids: Default::default(),
                deleted_base_edge_ids: Default::default(),
            };
            return Ok((freeze, nodes, edges));
        }

        Err(Error::Internal(
            "no live graph store available for epoch freeze".into(),
        ))
    }

    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    fn build_publish_frozen(
        &self,
        handle: FrozenEpochHandle,
        request: GenerationBuildRequest,
    ) -> Result<BuildPublication> {
        std::fs::create_dir_all(&request.generation_root)?;
        let root_buf = std::fs::canonicalize(&request.generation_root)
            .map_err(|e| Error::Internal(format!("canonicalize generation root: {e}")))?;
        let root = root_buf.as_path();
        let lock = RootLock::try_acquire(root)
            .map_err(|e| Error::Internal(format!("generation root lock: {e}")))?;

        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&wal_dir)?;
        let wal = WalManager::open(&wal_dir)?;

        // Build from frozen base (if layered) + materialized frozen overlay.
        #[cfg(feature = "generation-streaming")]
        let (section, node_count, edge_count) = {
            use grafeo_core::graph::compact::generation_builder::orchestrator::{
                BoundedBuildConfig, BoundedGenerationBuilder,
            };
            use grafeo_storage::file::generation_writer::StreamingPayloadSectionSource;
            use grafeo_storage::generation::DiskRunStore;

            let base = self.layered_store.as_ref().map(|l| l.base_store_arc());
            // Overlay side uses ONLY the freeze handle's materialized nodes/edges
            // via empty live overlay + frozen sources composed below.
            let node_src = FrozenNodeSource {
                nodes: handle.frozen_nodes.clone(),
                pos: 0,
            };
            let edge_src = FrozenEdgeSource {
                edges: handle.frozen_edges.clone(),
                pos: 0,
            };

            // When a base exists, merge base (honoring freeze shadows) + frozen overlay.
            let mut sources = if let Some(b) = base {
                let freeze = handle.freeze.clone();
                // Build merged sources: base cursor + frozen overlay records.
                // live_graph_sources_bounded with overlay=None for base-only,
                // then we chain frozen overlay separately via Merged* sources.
                use grafeo_core::graph::compact::generation_builder::freeze::{
                    BaseEdgeCursor, BaseNodeCursor, MergedEdgeSource, MergedNodeSource,
                };
                let base_nodes = BaseNodeCursor::new(Arc::clone(&b), freeze.clone());
                let base_edges = BaseEdgeCursor::new(b, freeze);
                let nodes: Box<dyn NodeRecordSource> =
                    Box::new(MergedNodeSource::new(base_nodes, node_src));
                let edges: Box<dyn EdgeRecordSource> =
                    Box::new(MergedEdgeSource::new(base_edges, edge_src));
                grafeo_core::graph::compact::generation_builder::live_graph::LiveGraphSources {
                    nodes,
                    edges,
                }
            } else {
                grafeo_core::graph::compact::generation_builder::live_graph::LiveGraphSources {
                    nodes: Box::new(node_src),
                    edges: Box::new(edge_src),
                }
            };

            let temp_dir = root.join("build-tmp");
            let config = BoundedBuildConfig {
                budget: request.budget,
                temp_dir,
                correlation_id: request.generation_id.clone(),
                spool_buf_cap: usize::try_from(request.budget.io_buffer_bytes)
                    .unwrap_or(1024 * 1024),
                rel_schemas: request.rel_schemas.clone(),
                frozen_epoch: handle.frozen_epoch,
            };
            let mut run_store = DiskRunStore::new(
                root.join("build-runs"),
                request.budget,
                request.generation_id.clone(),
            )
            .map_err(map_generation_error)?;
            let mut builder = BoundedGenerationBuilder::new(config);
            let lease = builder
                .build(
                    sources.nodes.as_mut(),
                    sources.edges.as_mut(),
                    &mut run_store,
                )
                .map_err(map_generation_error)?;
            let node_count = lease.total_nodes();
            let edge_count = lease.total_edges();
            let section: Box<dyn ExactSectionSource> =
                Box::new(StreamingPayloadSectionSource::new(lease));
            (section, node_count, edge_count)
        };

        #[cfg(not(feature = "generation-streaming"))]
        let (section, node_count, edge_count) = {
            use grafeo_core::graph::compact::generation::generate_compact_store;
            use grafeo_storage::file::generation_writer::CompactStoreSectionSource;

            let mut nodes = FrozenNodeSource {
                nodes: handle.frozen_nodes.clone(),
                pos: 0,
            };
            let mut edges = FrozenEdgeSource {
                edges: handle.frozen_edges.clone(),
                pos: 0,
            };
            // Feature-off path: overlay-only freeze is the full graph for pure LPG tests.
            let generated = generate_compact_store(
                &mut nodes,
                &mut edges,
                &request.rel_schemas,
                &request.budget,
            )
            .map_err(map_generation_error)?;
            let node_count = generated.store.total_nodes();
            let edge_count = generated.store.total_edges();
            let section: Box<dyn ExactSectionSource> = Box::new(
                CompactStoreSectionSource::new(generated.store, generated.global_strings)
                    .map_err(|e| Error::Internal(format!("generation section source: {e}")))?,
            );
            (section, node_count, edge_count)
        };

        maybe_abort("after_build");

        let header = GenerationContainerHeader {
            epoch: handle.frozen_epoch,
            transaction_id: self
                .transaction_manager
                .last_assigned_transaction_id()
                .map_or(0, |t| t.0),
            node_count,
            edge_count,
        };

        let parent_generation_id = request.parent_generation_id.clone();
        let parent_publication_sequence = request.parent_publication_sequence;
        let generation_id = request.generation_id.clone();

        let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![section];
        let result = publish_generation(
            &lock,
            PublicationInput {
                header,
                sections: &mut sections,
                generation_id: generation_id.clone(),
                parent_generation_id: parent_generation_id.clone(),
                parent_publication_sequence,
                pre_cut_cursor: Some(handle.wal_boundary.to_cursor()),
            },
            &wal,
            &OsGenerationFileOps,
        )
        .map_err(map_publication_error)?;

        Ok(assemble_build_publication(
            root,
            &result,
            generation_id,
            parent_generation_id,
            parent_publication_sequence,
            handle.frozen_epoch,
        ))
    }

    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    fn retire_frozen_prefix(&self, handle: &FrozenEpochHandle) -> Result<(u64, u64, u64, u64)> {
        if let Some(layered) = self.layered_store.as_ref() {
            let live = layered
                .retire_frozen_overlay_prefix()
                .map_err(Error::Internal)?;
            let absorbed_nodes = handle
                .freeze
                .overlay_node_ids
                .iter()
                .filter(|id| !live.post_freeze_nodes.contains(*id))
                .count() as u64;
            let absorbed_edges = handle
                .freeze
                .overlay_edge_ids
                .iter()
                .filter(|id| !live.post_freeze_edges.contains(*id))
                .count() as u64;
            return Ok((
                absorbed_nodes,
                absorbed_edges,
                live.post_freeze_nodes.len() as u64,
                live.post_freeze_edges.len() as u64,
            ));
        }

        // Pure LPG / no layered: nothing to strip; N+1 writes are already on the store.
        if let Some(ctl) = self.overlay_admission() {
            for cat in [
                RetainedCategory::MutationPayload,
                RetainedCategory::DirtySets,
                RetainedCategory::DeletionSets,
            ] {
                let bytes = handle.frozen_category_bytes[cat.index()];
                if bytes > 0 {
                    ctl.release(cat, bytes);
                }
            }
            ctl.set_active_epoch(handle.next_epoch);
            ctl.complete_generation_build();
        }
        Ok((
            handle.freeze.overlay_node_ids.len() as u64,
            handle.freeze.overlay_edge_ids.len() as u64,
            0,
            0,
        ))
    }
}
