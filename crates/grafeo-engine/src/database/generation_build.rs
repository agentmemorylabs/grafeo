//! Live engine orchestration for immutable generation build (G-EM0.3a).
//!
//! Wires the live engine's frozen graph view into W0's bounded record-source
//! generator, streams a CompactStore section through W0's container writer,
//! and publishes via W0's 11-step [`publish_generation`] ordering.
//!
//! This module does **not** own publication, manifest, lock, WAL-cursor, or
//! container-writer machinery — those stay in `grafeo-storage` (W0). It also
//! does not select a published generation (selection belongs to W0 recovery).
use std::path::{Path, PathBuf};
use std::sync::Arc;

use grafeo_common::types::{EdgeId, NodeId};
use grafeo_common::utils::error::{Error, Result};
use grafeo_common::utils::hash::FxHashMap;
use grafeo_core::graph::Direction;
use grafeo_core::graph::GraphStore;
#[cfg(not(feature = "generation-streaming"))]
use grafeo_core::graph::compact::generation::generate_compact_store;
#[cfg(not(feature = "generation-streaming"))]
use grafeo_core::graph::compact::generation::{EdgeRecordSource, NodeRecordSource};
use grafeo_core::graph::compact::generation::{
    GenerationBudget, GenerationBudgetPeaks, GenerationEdge, GenerationError, GenerationNode,
    OriginalEdgeId, OriginalNodeId, RelSchemaDecl,
};
#[cfg(not(feature = "generation-streaming"))]
use grafeo_storage::file::generation_writer::CompactStoreSectionSource;
use grafeo_storage::file::generation_writer::{
    ExactSectionSource, GenerationContainerHeader, OsGenerationFileOps,
};
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::publication::{
    PublicationError, PublicationInput, publish_generation,
};
use grafeo_storage::wal::WalManager;

use super::GrafeoDB;
use super::flush::is_generation_root;
use super::generation::publication::{BuildPublication, PublicationPhaseError};

/// Request to build and publish one immutable generation from the live graph.
#[derive(Debug, Clone)]
pub struct GenerationBuildRequest {
    /// Writable E-M0 generation root (`*.grafeo.d/` layout).
    pub generation_root: PathBuf,
    /// Stable generation identifier recorded in the manifest slot.
    pub generation_id: String,
    /// Explicit generation budget (non-optional).
    pub budget: GenerationBudget,
    /// Optional relationship schema declarations for endpoint table checks.
    pub rel_schemas: Vec<RelSchemaDecl>,
    /// Parent generation id (None = derive from previous slot / genesis).
    pub parent_generation_id: Option<String>,
    /// Parent publication sequence (None = derive from previous slot / genesis).
    pub parent_publication_sequence: Option<u64>,
}

/// Validated published generation descriptor (not yet selected as active).
#[derive(Debug, Clone)]
pub struct PublishedGenerationDescriptor {
    /// Monotonic publication sequence.
    pub publication_sequence: u64,
    /// Root-relative generation path (`generations/g-…grafeo`).
    pub generation_path: String,
    /// Absolute path to the immutable generation container.
    pub generation_abs_path: PathBuf,
    /// SHA-256 of the complete generation file.
    pub generation_sha256: [u8; 32],
    /// Byte length of the generation file.
    pub generation_length: u64,
    /// Caller-supplied generation identifier.
    pub generation_id: String,
}

/// Frozen ID snapshot used by streaming live record sources (feature-off path).
///
/// Holds only identity keys (not `GenerationNode` / `GenerationEdge` payloads).
/// Payloads are loaded one-at-a-time in `next_*`.
#[cfg(not(feature = "generation-streaming"))]
struct FrozenLiveGraph {
    store: Arc<dyn GraphStore>,
    node_ids: Vec<NodeId>,
    edge_ids: Vec<EdgeId>,
}

#[cfg(not(feature = "generation-streaming"))]
impl FrozenLiveGraph {
    fn freeze(store: Arc<dyn GraphStore>) -> Self {
        let node_ids = store.node_ids();
        let mut edge_ids = Vec::new();
        for nid in &node_ids {
            for (_dst, eid) in store.edges_from(*nid, Direction::Outgoing) {
                edge_ids.push(eid);
            }
        }
        edge_ids.sort_unstable();
        edge_ids.dedup();
        Self {
            store,
            node_ids,
            edge_ids,
        }
    }
}

/// Streams nodes from a frozen live-graph snapshot (feature-off path).
#[cfg(not(feature = "generation-streaming"))]
struct LiveNodeRecordSource {
    graph: Arc<FrozenLiveGraph>,
    cursor: usize,
}

#[cfg(not(feature = "generation-streaming"))]
impl NodeRecordSource for LiveNodeRecordSource {
    fn next_node(&mut self) -> std::result::Result<Option<GenerationNode>, GenerationError> {
        while self.cursor < self.graph.node_ids.len() {
            let id = self.graph.node_ids[self.cursor];
            self.cursor += 1;
            let Some(node) = self.graph.store.get_node(id) else {
                continue;
            };
            // D0.8.0 item 1: carry the node's complete canonical label set.
            // Never select one primary label and discard the rest.
            let mut labels: Vec<String> = node.labels.iter().map(|l| l.to_string()).collect();
            labels.sort();
            labels.dedup();
            if labels.is_empty() {
                return Err(GenerationError::InvalidInput(format!(
                    "node {} has no labels",
                    node.id.as_u64()
                )));
            }
            let mut properties = FxHashMap::default();
            for (key, value) in node.properties.iter() {
                properties.insert(key.clone(), value.clone());
            }
            return Ok(Some(GenerationNode {
                id: OriginalNodeId::new(node.id.as_u64()),
                labels,
                properties,
            }));
        }
        Ok(None)
    }
}

/// Streams edges from a frozen live-graph snapshot (feature-off path).
#[cfg(not(feature = "generation-streaming"))]
struct LiveEdgeRecordSource {
    graph: Arc<FrozenLiveGraph>,
    cursor: usize,
}

#[cfg(not(feature = "generation-streaming"))]
impl EdgeRecordSource for LiveEdgeRecordSource {
    fn next_edge(&mut self) -> std::result::Result<Option<GenerationEdge>, GenerationError> {
        while self.cursor < self.graph.edge_ids.len() {
            let id = self.graph.edge_ids[self.cursor];
            self.cursor += 1;
            let Some(edge) = self.graph.store.get_edge(id) else {
                continue;
            };
            let mut properties = FxHashMap::default();
            for (key, value) in edge.properties.iter() {
                properties.insert(key.clone(), value.clone());
            }
            return Ok(Some(GenerationEdge {
                id: OriginalEdgeId::new(edge.id.as_u64()),
                src: OriginalNodeId::new(edge.src.as_u64()),
                dst: OriginalNodeId::new(edge.dst.as_u64()),
                edge_type: edge.edge_type.to_string(),
                properties,
            }));
        }
        Ok(None)
    }
}

fn map_generation_error(err: GenerationError) -> Error {
    Error::Internal(format!("generation build: {err}"))
}

/// Map a W0 publication failure to a phase-tagged engine error.
///
/// Preserves the W0 [`PublicationError`] identity and tags the conservative
/// failing [`super::generation::publication::PublicationPhase`] so a caller
/// can learn whether the failure was pre-commit or post-commit, satisfying
/// the packet requirement to surface every publication phase/error.
fn map_publication_error(err: PublicationError) -> Error {
    PublicationPhaseError::from_publication(err).into()
}

#[cfg(not(feature = "generation-streaming"))]
fn map_section_error(err: Error) -> Error {
    Error::Internal(format!("generation section source: {err}"))
}

impl GrafeoDB {
    /// Build an immutable generation from the live frozen graph and publish it
    /// under `request.generation_root` via W0 publication.
    ///
    /// Requires an exclusive [`RootLock`] on the generation root. Never rewrites
    /// a standalone `.grafeo` snapshot or renames over a mapped active container.
    /// Returns a validated descriptor; does **not** select the generation.
    ///
    /// # Single-writer assumption
    ///
    /// The freeze captures identity keys only; node/edge payloads (labels, edge
    /// types, properties) are read live from the graph while the build streams.
    /// Callers must ensure **no concurrent writes** mutate the graph during the
    /// build. A mid-build node or edge deletion fails closed (the record is
    /// skipped and endpoint validation rejects dangling edges), but a
    /// concurrent property mutation does **not** fail closed — the published
    /// payload reflects whatever the stream read live.
    ///
    /// # Errors
    ///
    /// Returns an error when the root lock cannot be acquired, the live graph
    /// cannot be streamed, generation/publication fails, or the caller points
    /// at a legacy standalone `.grafeo` file path.
    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    pub fn build_immutable_generation(
        &self,
        request: GenerationBuildRequest,
    ) -> Result<PublishedGenerationDescriptor> {
        let published = self.build_generation_inner(request)?;
        let generation_abs_path = published.generation_abs_path.clone();
        Ok(PublishedGenerationDescriptor {
            publication_sequence: published.publication.publication_sequence,
            generation_path: published.publication.generation_path.clone(),
            generation_abs_path,
            generation_sha256: published.publication.generation_sha256,
            generation_length: published.publication.generation_length,
            generation_id: published.publication.generation_id.clone(),
        })
    }

    /// Build and publish one immutable generation, returning the extended
    /// G-EM0.3b descriptor that retains the durable WAL boundary, overlay
    /// epoch, and parent linkage.
    ///
    /// This is the manifest-publication entry point: it publishes only a
    /// fresh-reopen-validated immutable generation, records the precise
    /// durable WAL boundary and overlay epoch in the manifest slot, and does
    /// not truncate/advance the WAL or reset overlay state before the manifest
    /// selection is durable (the manifest fsync is the commit point).
    ///
    /// # Errors
    ///
    /// Returns a phase-tagged [`Error`] when the root lock cannot be acquired,
    /// the live graph cannot be streamed, generation/publication fails, or
    /// the caller points at a legacy standalone `.grafeo` file path.
    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    pub fn build_and_publish_generation(
        &self,
        request: GenerationBuildRequest,
    ) -> Result<BuildPublication> {
        self.build_generation_inner(request)
    }

    /// Shared build+publish body for the 3a legacy descriptor and the 3b
    /// extended descriptor. Runs the full W0 publication ordering once and
    /// assembles the extended [`PublishedGeneration`] from the result.
    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    fn build_generation_inner(&self, request: GenerationBuildRequest) -> Result<BuildPublication> {
        let root = request.generation_root.as_path();
        if root.is_file() {
            return Err(Error::Internal(
                "refusing to build a generation into a standalone .grafeo file; \
                 use an explicit generation root directory"
                    .into(),
            ));
        }

        // Acquire exclusive process ownership of the writable generation root.
        let lock = RootLock::try_acquire(root)
            .map_err(|e| Error::Internal(format!("generation root lock: {e}")))?;

        // Generation-root WAL (W0 layout), distinct from any legacy LpgWal.
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&wal_dir)?;
        let wal = WalManager::open(&wal_dir)?;

        #[cfg(feature = "generation-streaming")]
        let mut live_sources = self.live_graph_sources_bounded(request.budget.max_record_bytes)?;

        // G-GEM0.SRV1 Gap B (publish side): materialize ForceDisk-spilled
        // vectors back into the property store BEFORE the freeze, mirroring
        // `checkpoint_to_file`. Spill sidecars are derived runtime state and
        // are NOT part of the generation container — freezing a drained
        // source would publish a base whose embedding columns are missing,
        // so the reopened RO index could never serve vectors.
        #[cfg(all(
            feature = "lpg",
            feature = "vector-index",
            feature = "mmap",
            not(feature = "temporal")
        ))]
        let vector_was_on_disk =
            self.buffer_manager
                .snapshot_consumer_tiers()
                .iter()
                .any(|(name, tier)| {
                    name == "section:VectorStore"
                        && *tier == grafeo_common::memory::StorageTier::OnDisk
                });
        #[cfg(all(
            feature = "lpg",
            feature = "vector-index",
            feature = "mmap",
            not(feature = "temporal")
        ))]
        if vector_was_on_disk {
            self.buffer_manager
                .reload_consumer_by_name("section:VectorStore")
                .map_err(|error| {
                    Error::Internal(format!(
                        "failed to reload spilled vectors before generation publish: {error}"
                    ))
                })?;
        }

        #[cfg(not(feature = "generation-streaming"))]
        let store = self.live_graph_store()?;
        #[cfg(not(feature = "generation-streaming"))]
        let frozen = Arc::new(FrozenLiveGraph::freeze(store));
        #[cfg(not(feature = "generation-streaming"))]
        let mut nodes = LiveNodeRecordSource {
            graph: Arc::clone(&frozen),
            cursor: 0,
        };
        #[cfg(not(feature = "generation-streaming"))]
        let mut edges = LiveEdgeRecordSource {
            graph: Arc::clone(&frozen),
            cursor: 0,
        };

        // H-ADOPT.6 decision 3: capture catalog + index section state at the
        // SAME instant as the payload source — right after the live-graph
        // freeze above, inside the quiesced build window. Emission happens
        // before publication from this captured state.
        let section_capture = self.capture_generation_sections()?;

        // ── Generation build + section source ─────────────────────────────
        // With `generation-streaming` the engine drives the bounded
        // out-of-core orchestrator and streams a payload lease; otherwise it
        // falls back to the eager heap `generate_compact_store` path. The
        // accepted 3a commit/rollback/lease/publication state machine below
        // is verbatim in both cases.
        #[cfg(feature = "generation-streaming")]
        let (section, node_count, edge_count, budget_peaks) = {
            use grafeo_core::graph::compact::generation_builder::orchestrator::{
                BoundedBuildConfig, BoundedGenerationBuilder,
            };
            use grafeo_storage::file::generation_writer::StreamingPayloadSectionSource;
            use grafeo_storage::generation::DiskRunStore;

            // Job temp root: a scratch dir under the generation root, removed
            // when the run-set leases and payload lease drop (success or
            // failure). Correlation id = generation id for traceability.
            let temp_dir = root.join("build-tmp");
            // R1.6: capture the frozen epoch before the build so the payload
            // lease carries the same epoch as the container header.
            let frozen_epoch = self.transaction_manager.current_epoch().0;
            let config = BoundedBuildConfig {
                budget: request.budget,
                temp_dir,
                correlation_id: request.generation_id.clone(),
                spool_buf_cap: usize::try_from(request.budget.io_buffer_bytes)
                    .unwrap_or(1024 * 1024),
                rel_schemas: request.rel_schemas.clone(),
                frozen_epoch,
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
                    live_sources.nodes.as_mut(),
                    live_sources.edges.as_mut(),
                    &mut run_store,
                )
                .map_err(map_generation_error)?;
            let node_count = lease.total_nodes();
            let edge_count = lease.total_edges();
            // G-FRZ.1: snapshot the lease's final metrics BEFORE the lease is
            // moved into the streaming source — after this point the tally
            // becomes unreachable downstream.
            let budget_peaks = GenerationBudgetPeaks::from(lease.metrics());
            let section: Box<dyn ExactSectionSource> =
                Box::new(StreamingPayloadSectionSource::new(lease));
            (section, node_count, edge_count, budget_peaks)
        };

        #[cfg(not(feature = "generation-streaming"))]
        let (section, node_count, edge_count, budget_peaks) = {
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
                    .map_err(map_section_error)?,
            );
            // The eager fallback owns no live metrics tally; report zeros
            // rather than fabricating peaks (G-FRZ.1).
            (
                section,
                node_count,
                edge_count,
                GenerationBudgetPeaks::default(),
            )
        };

        // `#[doc(hidden)]` test-only fault seam (G-EM0.3c crash matrix): in
        // debug/test builds, `GRAFEO_3C_ABORT` hard-aborts the process at the
        // named engine-owned boundary — `after_generation_build` (pre-commit)
        // or `after_publication` (post-commit, manifest fsync durable) — so a
        // fresh-process parent can prove recovery. `debug_assertions`-gated
        // like W0's `#[cfg(test)]` fault hooks: compiled out of release
        // builds, so a leaked env var can never abort a shipped process.
        #[cfg(debug_assertions)]
        let abort_point = std::env::var("GRAFEO_3C_ABORT").ok();
        #[cfg(debug_assertions)]
        if abort_point.as_deref() == Some("after_generation_build") {
            std::process::abort();
        }

        let overlay_epoch = self.transaction_manager.current_epoch().0;
        let header = GenerationContainerHeader {
            epoch: overlay_epoch,
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

        let mut sections = super::generation::sections::generation_section_sources(section_capture);
        sections.insert(0, section);
        let result = publish_generation(
            &lock,
            PublicationInput {
                header,
                sections: &mut sections,
                generation_id: generation_id.clone(),
                parent_generation_id: parent_generation_id.clone(),
                parent_publication_sequence,
                pre_cut_cursor: None,
            },
            &wal,
            &OsGenerationFileOps,
        )
        .map_err(map_publication_error)?;

        // G-GEM0.SRV1 Gap B (publish side): the reload above restored the
        // complete embedding columns into the source's property store for
        // the freeze. Re-spill now to preserve the ForceDisk memory profile
        // (mirror checkpoint_to_file's post-serialization re-spill).
        #[cfg(all(
            feature = "lpg",
            feature = "vector-index",
            feature = "mmap",
            not(feature = "temporal")
        ))]
        {
            let vector_force_disk = self
                .config
                .section_configs
                .get(&grafeo_common::storage::SectionType::VectorStore)
                .is_some_and(|config| {
                    config.tier == grafeo_common::storage::TierOverride::ForceDisk
                });
            if vector_was_on_disk || vector_force_disk {
                self.buffer_manager
                    .spill_consumer_by_name("section:VectorStore");
            }
        }

        // Post-commit crash point (3c): manifest fsync durable → select NEW.
        // Same `debug_assertions` gate as the pre-commit point above.
        #[cfg(debug_assertions)]
        if abort_point.as_deref() == Some("after_publication") {
            std::process::abort();
        }

        debug_assert!(
            is_generation_root(root),
            "publish_generation must leave a recognizable generation root"
        );

        // Assemble the extended descriptor from the durable manifest slot
        // (read-back + fallback live in the generation module).
        Ok(super::generation::publication::assemble_build_publication(
            root,
            &result,
            generation_id,
            parent_generation_id,
            parent_publication_sequence,
            overlay_epoch,
            budget_peaks,
        ))
    }

    /// Returns the merged live graph store (layered when compacted, else LPG).
    #[cfg(all(
        not(feature = "generation-streaming"),
        feature = "generation",
        feature = "lpg",
        feature = "compact-store"
    ))]
    fn live_graph_store(&self) -> Result<Arc<dyn GraphStore>> {
        if let Some(layered) = self.layered_store.as_ref() {
            return Ok(Arc::clone(layered) as Arc<dyn GraphStore>);
        }
        if let Some(store) = self.store.as_ref() {
            return Ok(Arc::clone(store) as Arc<dyn GraphStore>);
        }
        Err(Error::Internal(
            "no live graph store available for generation build".into(),
        ))
    }

    /// Builds bounded live record sources from the concrete base/overlay stores.
    ///
    /// Extracts the `CompactStore` base and `LpgStore` overlay from the layered
    /// store (when present), freezes the overlay epoch, and returns bounded
    /// row-by-row cursors. Falls back to the overlay-only path when the store
    /// is not layered (pure LPG).
    ///
    /// Builder mode (G-MIDFLUSH.1 M2): when `mid_build_tiers` is non-empty, the
    /// tiers+overlay are presented as a single unified source via `TierChainView`
    ///-backed cursors — no `live_graph_sources_bounded` rebuild of the tiers.
    #[cfg(all(
        feature = "generation-streaming",
        feature = "generation",
        feature = "lpg",
        feature = "compact-store"
    ))]
    fn live_graph_sources_bounded(
        &self,
        max_record_bytes: u64,
    ) -> Result<grafeo_core::graph::compact::generation_builder::live_graph::LiveGraphSources> {
        use grafeo_common::utils::hash::FxHashSet;
        use grafeo_core::graph::compact::generation_builder::FrozenOverlayEpoch;
        use grafeo_core::graph::compact::generation_builder::live_graph_sources_bounded;

        // Builder-mode branch: tiers + overlay as a unified chain.
        #[cfg(all(feature = "mmap", feature = "compact-store"))]
        {
            if !self.mid_build_tiers.read().is_empty() {
                if let Some(layered) = self.layered_store.as_ref() {
                    let tiers = self.mid_build_tiers.read().clone();
                    let overlay = layered.overlay_store();
                    let chain = std::sync::Arc::new(
                        grafeo_core::graph::compact::tier_chain::TierChainView::new(
                            tiers, overlay.clone(),
                        ),
                    );
                    // Chain sources are self-contained: ChainNodeSource walks
                    // chain.node_ids() (tiers row-by-row + overlay) and fetches
                    // each node through the chain view; the external merge sort
                    // in NodePass::stage restores canonical (label,id) order, so
                    // enumeration order need not be pre-sorted.
                    let chain_clone = std::sync::Arc::clone(&chain) as std::sync::Arc<dyn grafeo_core::graph::traits::GraphStore>;
                    let edge_chain_clone = std::sync::Arc::clone(&chain) as std::sync::Arc<dyn grafeo_core::graph::traits::GraphStore>;
                    let mr_nodes = crate::database::tier_chain_sources::ChainNodeSource::new(chain_clone, max_record_bytes);
                    let mr_edges = crate::database::tier_chain_sources::ChainEdgeSource::new(edge_chain_clone, max_record_bytes);
                    return Ok(grafeo_core::graph::compact::generation_builder::live_graph::LiveGraphSources {
                        nodes: Box::new(mr_nodes),
                        edges: Box::new(mr_edges),
                    });
                }
            }
        }

        if let Some(layered) = self.layered_store.as_ref() {
            let base = layered.base_store_arc();
            let overlay = layered.overlay_store();
            // Snapshot dirty/deleted sets via the layered store's freeze helper,
            // then override the epoch with the transaction manager's authoritative value.
            let mut freeze = layered.generation_freeze_epoch();
            freeze.epoch = self.transaction_manager.current_epoch().0;
            return Ok(live_graph_sources_bounded(
                Some(base),
                Some(overlay),
                freeze,
                max_record_bytes,
            ));
        }
        if let Some(store) = self.store.as_ref() {
            // Pure LPG store — no base; all data is overlay.
            let overlay_node_ids: FxHashSet<u64> = store
                .all_node_ids()
                .into_iter()
                .map(|id| id.as_u64())
                .collect();
            let overlay_edge_ids: FxHashSet<u64> = store
                .all_edges()
                .into_iter()
                .map(|e| e.id.as_u64())
                .collect();
            let freeze = FrozenOverlayEpoch {
                epoch: self.transaction_manager.current_epoch().0,
                overlay_node_ids,
                overlay_edge_ids,
                deleted_base_node_ids: FxHashSet::default(),
                deleted_base_edge_ids: FxHashSet::default(),
            };
            return Ok(live_graph_sources_bounded(
                None,
                Some(Arc::clone(store)),
                freeze,
                max_record_bytes,
            ));
        }
        Err(Error::Internal(
            "no live graph store available for generation build".into(),
        ))
    }
}

/// TEST-ONLY convenience constructor: builds a request with the test-scale
/// [`GenerationBudget::for_tests`] budget so integration tests keep one-line
/// call sites. Not a production entry point: production call sites construct
/// [`GenerationBuildRequest`] directly with an explicit production budget
/// (`GenerationBuildRequest::budget` is non-optional).
#[must_use]
pub fn generation_build_request(
    generation_root: impl Into<PathBuf>,
    generation_id: impl Into<String>,
) -> GenerationBuildRequest {
    GenerationBuildRequest {
        generation_root: generation_root.into(),
        generation_id: generation_id.into(),
        budget: GenerationBudget::for_tests(),
        rel_schemas: Vec::new(),
        parent_generation_id: None,
        parent_publication_sequence: None,
    }
}

/// True when `path` is already (or must be treated as) an E-M0 generation root.
#[must_use]
pub fn path_is_generation_root(path: &Path) -> bool {
    is_generation_root(path)
}
