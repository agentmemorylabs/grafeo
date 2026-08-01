//! Bounded live [`GraphStore`] record sources for generation freeze (D0.8.2).
//!
//! Streams every node and edge exactly once without retaining O(N) or O(E)
//! identity vectors. Payloads are read live via `get_node` / `get_edge`.

use std::sync::Arc;

use grafeo_common::types::{EdgeId, NodeId};
use grafeo_common::utils::hash::FxHashMap;
use grafeo_common::utils::hash::FxHashSet;

use crate::graph::Direction;
use crate::graph::GraphStore;
use crate::graph::compact::generation::{
    EdgeRecordSource, GenerationEdge, GenerationError, GenerationNode, NodeRecordSource,
    OriginalEdgeId, OriginalNodeId,
};

/// Bounded node + edge sources over a live graph store.
pub struct LiveGraphSources {
    /// Node record source.
    pub nodes: Box<dyn NodeRecordSource>,
    /// Edge record source.
    pub edges: Box<dyn EdgeRecordSource>,
}

/// Builds bounded live record sources for one generation freeze.
pub fn live_graph_sources(store: Arc<dyn GraphStore>) -> LiveGraphSources {
    LiveGraphSources {
        nodes: Box::new(LiveGraphNodeCursor::new(Arc::clone(&store))),
        edges: Box::new(LiveGraphEdgeCursor::new(store)),
    }
}

/// Generic live node cursor: label batches + per-node dedup (no O(N) id vector).
struct LiveGraphNodeCursor {
    store: Arc<dyn GraphStore>,
    labels: Vec<String>,
    label_idx: usize,
    nodes: std::vec::IntoIter<NodeId>,
    seen: FxHashSet<u64>,
}

impl LiveGraphNodeCursor {
    fn new(store: Arc<dyn GraphStore>) -> Self {
        let mut labels = store.all_labels();
        labels.sort();
        Self {
            store,
            labels,
            label_idx: 0,
            nodes: Vec::new().into_iter(),
            seen: FxHashSet::default(),
        }
    }

    fn refill_nodes(&mut self) -> bool {
        while self.label_idx < self.labels.len() {
            let label = self.labels[self.label_idx].clone();
            self.label_idx += 1;
            let batch = self.store.nodes_by_label(&label);
            if batch.is_empty() {
                continue;
            }
            self.nodes = batch.into_iter();
            return true;
        }
        false
    }
}

impl NodeRecordSource for LiveGraphNodeCursor {
    fn next_node(&mut self) -> Result<Option<GenerationNode>, GenerationError> {
        loop {
            if let Some(id) = self.nodes.next() {
                if !self.seen.insert(id.as_u64()) {
                    continue;
                }
                let Some(node) = self.store.get_node(id) else {
                    continue;
                };
                let mut labels: Vec<String> = node.labels.iter().map(|l| l.to_string()).collect();
                labels.sort();
                labels.dedup();
                if labels.is_empty() {
                    return Err(GenerationError::InvalidInput(format!(
                        "node {} has no labels",
                        id.as_u64()
                    )));
                }
                return Ok(Some(GenerationNode {
                    id: OriginalNodeId::new(id.as_u64()),
                    labels,
                    properties: node
                        .properties
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                }));
            }
            if !self.refill_nodes() {
                return Ok(None);
            }
        }
    }
}

/// Generic live edge cursor: label batches + per-node dedup (no O(E) edge-id vec).
struct LiveGraphEdgeCursor {
    store: Arc<dyn GraphStore>,
    labels: Vec<String>,
    label_idx: usize,
    nodes: std::vec::IntoIter<NodeId>,
    pending: std::vec::IntoIter<(NodeId, EdgeId)>,
    seen_nodes: FxHashSet<u64>,
}

impl LiveGraphEdgeCursor {
    fn new(store: Arc<dyn GraphStore>) -> Self {
        let mut labels = store.all_labels();
        labels.sort();
        Self {
            store,
            labels,
            label_idx: 0,
            nodes: Vec::new().into_iter(),
            pending: Vec::new().into_iter(),
            seen_nodes: FxHashSet::default(),
        }
    }

    fn refill_nodes(&mut self) -> bool {
        while self.label_idx < self.labels.len() {
            let label = self.labels[self.label_idx].clone();
            self.label_idx += 1;
            let batch = self.store.nodes_by_label(&label);
            if batch.is_empty() {
                continue;
            }
            self.nodes = batch.into_iter();
            return true;
        }
        false
    }
}

impl EdgeRecordSource for LiveGraphEdgeCursor {
    fn next_edge(&mut self) -> Result<Option<GenerationEdge>, GenerationError> {
        loop {
            if let Some((_dst, eid)) = self.pending.next() {
                let Some(edge) = self.store.get_edge(eid) else {
                    continue;
                };
                return Ok(Some(GenerationEdge {
                    id: OriginalEdgeId::new(eid.as_u64()),
                    src: OriginalNodeId::new(edge.src.as_u64()),
                    dst: OriginalNodeId::new(edge.dst.as_u64()),
                    edge_type: edge.edge_type.to_string(),
                    properties: edge
                        .properties
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                }));
            }
            loop {
                if let Some(nid) = self.nodes.next() {
                    if self.seen_nodes.insert(nid.as_u64()) {
                        self.pending = self
                            .store
                            .edges_from(nid, Direction::Outgoing)
                            .into_iter();
                        break;
                    }
                    continue;
                }
                if !self.refill_nodes() {
                    return Ok(None);
                }
            }
        }
    }
}

/// Schema-bounded lookup: physical label per table + multi-label extras.
pub(crate) struct LogicalLabelLookup {
    physical: Vec<String>,
    multi: FxHashMap<u64, Vec<String>>,
}

impl LogicalLabelLookup {
    /// Builds from node schema labels and an optional membership run.
    pub(crate) fn build(
        physical: &[String],
        membership: Option<&crate::graph::compact::generation::RunSetLease>,
        run_store: &mut dyn crate::graph::compact::generation::RunStore,
        budget: &crate::graph::compact::generation::GenerationBudget,
        metrics: &mut crate::graph::compact::generation::GenerationMetrics,
        cancel: Option<&crate::graph::compact::generation::CancelToken>,
    ) -> Result<Self, GenerationError> {
        use crate::graph::compact::generation_builder::staging;
        let mut multi = FxHashMap::default();
        if let Some(lease) = membership {
            let mut merger = run_store.merger("membership")?;
            merger.merge_all(&lease.handles, budget, metrics, cancel, &mut |rec| {
                let (_physical, original_id) = staging::split_node_row_key(&rec.key)?;
                let labels = staging::decode_labels(&rec.payload)?;
                multi.insert(original_id, labels);
                Ok(())
            })?;
        }
        Ok(Self {
            physical: physical.to_vec(),
            multi,
        })
    }

    /// True when `label` is in the node's complete logical label set.
    #[must_use]
    pub(crate) fn has_label(&self, original_id: u64, table_id: u16, label: &str) -> bool {
        if let Some(labels) = self.multi.get(&original_id) {
            return labels.iter().any(|l| l == label);
        }
        self.physical
            .get(table_id as usize)
            .is_some_and(|l| l == label)
    }
}
