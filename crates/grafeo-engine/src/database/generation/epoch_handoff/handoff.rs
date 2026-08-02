//! Freeze → build → publish → retire orchestration (G-EM0.5c).
//!
//! The `impl GrafeoDB` block here owns the handoff state machine:
//! [`GrafeoDB::freeze_epoch_for_handoff`] cuts WAL boundary B and opens bounded
//! epoch N+1, [`GrafeoDB::complete_epoch_handoff`] builds/publishes G(N) with
//! the pre-cut boundary and retires only the frozen overlay prefix, and
//! [`GrafeoDB::run_epoch_handoff`] is the one-shot composition. Phase state
//! lives in [`EpochHandoffCoordinator`] (see `types.rs`); build-time record
//! sources and conversions live in `records.rs`.

use std::path::Path;
use std::sync::Arc;

use grafeo_common::types::{EdgeId, NodeId};
use grafeo_common::utils::error::{Error, Result};
use grafeo_core::graph::compact::generation::{
    EdgeRecordSource, GenerationEdge, GenerationNode, NodeRecordSource,
};
use grafeo_core::graph::compact::generation_builder::FrozenOverlayEpoch;
use grafeo_core::graph::compact::layered::OverlayHandoffLive;
use grafeo_core::graph::compact::overlay_budget::RetainedCategory;
use grafeo_storage::file::generation_writer::{
    ExactSectionSource, GenerationContainerHeader, OsGenerationFileOps,
};
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::publication::{PublicationInput, publish_generation};
use grafeo_storage::generation::wal_cursor::cut_generation_boundary;
use grafeo_storage::wal::WalManager;

use super::super::manifest::WalBoundary;
use super::super::publication::{BuildPublication, assemble_build_publication};
use super::records::{
    FrozenEdgeSource, FrozenNodeSource, edge_to_generation, map_generation_error,
    map_publication_error, maybe_abort, node_to_generation, pure_lpg_freeze_capture,
};
use super::types::{EpochHandoffPhase, EpochHandoffReport, FrozenEpochHandle};
use crate::database::GrafeoDB;
use crate::database::generation_build::GenerationBuildRequest;

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

        // Pure LPG: whole store is the overlay (no layered base).
        if let Some(store) = self.store.as_ref() {
            return pure_lpg_freeze_capture(store, frozen_epoch);
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
