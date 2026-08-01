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
use grafeo_core::graph::compact::generation::{
    EdgeRecordSource, GenerationBudget, GenerationEdge, GenerationError, GenerationNode,
    NodeRecordSource, OriginalEdgeId, OriginalNodeId, RelSchemaDecl,
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

/// Frozen ID snapshot used by streaming live record sources.
///
/// Holds only identity keys (not `GenerationNode` / `GenerationEdge` payloads).
/// Payloads are loaded one-at-a-time in `next_*`.
struct FrozenLiveGraph {
    store: Arc<dyn GraphStore>,
    node_ids: Vec<NodeId>,
    edge_ids: Vec<EdgeId>,
}

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

/// Streams nodes from a frozen live-graph snapshot.
struct LiveNodeRecordSource {
    graph: Arc<FrozenLiveGraph>,
    cursor: usize,
}

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

/// Streams edges from a frozen live-graph snapshot.
struct LiveEdgeRecordSource {
    graph: Arc<FrozenLiveGraph>,
    cursor: usize,
}

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

        let store = self.live_graph_store()?;
        let frozen = Arc::new(FrozenLiveGraph::freeze(store));
        let mut nodes = LiveNodeRecordSource {
            graph: Arc::clone(&frozen),
            cursor: 0,
        };
        let mut edges = LiveEdgeRecordSource {
            graph: Arc::clone(&frozen),
            cursor: 0,
        };

        // ── Generation build + section source ─────────────────────────────
        // With `generation-streaming` the engine drives the bounded
        // out-of-core orchestrator and streams a payload lease; otherwise it
        // falls back to the eager heap `generate_compact_store` path. The
        // accepted 3a commit/rollback/lease/publication state machine below
        // is verbatim in both cases.
        #[cfg(feature = "generation-streaming")]
        let (section, node_count, edge_count) = {
            use grafeo_core::graph::compact::generation_builder::orchestrator::{
                BoundedBuildConfig, BoundedGenerationBuilder,
            };
            use grafeo_storage::file::generation_writer::StreamingPayloadSectionSource;
            use grafeo_storage::generation::DiskRunStore;

            // Job temp root: a scratch dir under the generation root, removed
            // when the run-set leases and payload lease drop (success or
            // failure). Correlation id = generation id for traceability.
            let temp_dir = root.join("build-tmp");
            let config = BoundedBuildConfig {
                budget: request.budget,
                temp_dir,
                correlation_id: request.generation_id.clone(),
                spool_buf_cap: usize::try_from(request.budget.io_buffer_bytes)
                    .unwrap_or(1024 * 1024),
                rel_schemas: request.rel_schemas.clone(),
            };
            let mut run_store = DiskRunStore::new(
                root.join("build-runs"),
                request.budget,
                request.generation_id.clone(),
            )
            .map_err(map_generation_error)?;
            let mut builder = BoundedGenerationBuilder::new(config);
            let lease = builder
                .build(&mut nodes, &mut edges, &mut run_store)
                .map_err(map_generation_error)?;
            let node_count = lease.total_nodes();
            let edge_count = lease.total_edges();
            let section: Box<dyn ExactSectionSource> =
                Box::new(StreamingPayloadSectionSource::new(lease));
            (section, node_count, edge_count)
        };

        #[cfg(not(feature = "generation-streaming"))]
        let (section, node_count, edge_count) = {
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
            (section, node_count, edge_count)
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

        let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![section];
        let result = publish_generation(
            &lock,
            PublicationInput {
                header,
                sections: &mut sections,
                generation_id: generation_id.clone(),
                parent_generation_id: parent_generation_id.clone(),
                parent_publication_sequence,
            },
            &wal,
            &OsGenerationFileOps,
        )
        .map_err(map_publication_error)?;

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
        ))
    }

    /// Returns the merged live graph store (layered when compacted, else LPG).
    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
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
