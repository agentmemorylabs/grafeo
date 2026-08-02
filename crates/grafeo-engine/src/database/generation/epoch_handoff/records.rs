//! Frozen build-time record sources and record conversion (G-EM0.5c).
//!
//! The generation build consumes materialized freeze payloads through these
//! one-shot cursors; it never reads the live overlay. `node_to_generation` /
//! `edge_to_generation` normalize overlay records into canonical generation
//! form (sorted/deduped labels, cloned property maps). The pure-LPG freeze
//! fallback (no layered base) also lives here so the handoff orchestrator
//! stays focused on the state machine.

use grafeo_common::types::{PropertyKey, Value};
use grafeo_common::utils::error::{Error, Result};
use grafeo_common::utils::hash::FxHashMap;
use grafeo_core::graph::compact::generation::{
    EdgeRecordSource, GenerationEdge, GenerationError, GenerationNode, NodeRecordSource,
    OriginalEdgeId, OriginalNodeId,
};
use grafeo_core::graph::compact::generation_builder::FrozenOverlayEpoch;
use grafeo_core::graph::lpg::{Edge, LpgStore, Node};

use super::super::publication::PublicationPhaseError;

pub(super) struct FrozenNodeSource {
    pub(super) nodes: Vec<GenerationNode>,
    pub(super) pos: usize,
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

pub(super) struct FrozenEdgeSource {
    pub(super) edges: Vec<GenerationEdge>,
    pub(super) pos: usize,
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

pub(super) fn map_generation_error(err: GenerationError) -> Error {
    Error::Internal(format!("generation build: {err}"))
}

pub(super) fn map_publication_error(
    err: grafeo_storage::generation::publication::PublicationError,
) -> Error {
    PublicationPhaseError::from_publication(err).into()
}

pub(super) fn node_to_generation(node: &Node) -> GenerationNode {
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

pub(super) fn edge_to_generation(edge: &Edge) -> GenerationEdge {
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

/// Pure-LPG freeze capture: the whole store is the overlay (no layered base).
///
/// Used when the database was never compacted into a layered base+overlay;
/// every live entity enters the freeze snapshot.
pub(super) fn pure_lpg_freeze_capture(
    store: &LpgStore,
    frozen_epoch: u64,
) -> Result<(FrozenOverlayEpoch, Vec<GenerationNode>, Vec<GenerationEdge>)> {
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
    Ok((freeze, nodes, edges))
}

#[cfg(debug_assertions)]
pub(super) fn maybe_abort(point: &str) {
    if std::env::var("GRAFEO_5C_ABORT").ok().as_deref() == Some(point) {
        std::process::abort();
    }
}

#[cfg(not(debug_assertions))]
pub(super) fn maybe_abort(_point: &str) {}
