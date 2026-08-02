//! Bounded live record sources for generation freeze (R1).
//!
//! Streams every node and edge exactly once via row-by-row base traversal
//! (`BaseNodeCursor` / `BaseEdgeCursor`) and budget-bounded overlay cursors.
//! No O(N) or O(E) identity retention: base tables are walked row-by-row,
//! overlay dirty-id sets are bounded by the admission budget.

use std::sync::Arc;

use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};
use grafeo_common::utils::hash::FxHashMap;

use crate::graph::compact::CompactStore;
use crate::graph::compact::generation::{
    EdgeRecordSource, GenerationEdge, GenerationError, GenerationNode, NodeRecordSource,
    OriginalEdgeId, OriginalNodeId,
};
use crate::graph::compact::generation_builder::freeze::{
    BaseEdgeCursor, BaseNodeCursor, EmptyEdgeSource, EmptyNodeSource, FrozenOverlayEpoch,
    MergedEdgeSource, MergedNodeSource,
};
use crate::graph::compact::generation_builder::membership_index::MappedLabelMembershipIndex;
use crate::graph::compact::generation_builder::source_view::{
    estimate_node_bytes, estimate_props_bytes,
};
use crate::graph::lpg::LpgStore;

/// Bounded node + edge sources over a live graph store.
pub struct LiveGraphSources {
    /// Node record source.
    pub nodes: Box<dyn NodeRecordSource>,
    /// Edge record source.
    pub edges: Box<dyn EdgeRecordSource>,
}

/// Builds bounded live record sources from frozen base/overlay components.
///
/// Base rows are walked row-by-row through `BaseNodeCursor`/`BaseEdgeCursor`;
/// overlay rows iterate the budget-bounded dirty-id sets (sorted for
/// determinism). This is the R1 bounded path.
pub fn live_graph_sources_bounded(
    base: Option<Arc<CompactStore>>,
    overlay: Option<Arc<LpgStore>>,
    freeze: FrozenOverlayEpoch,
    max_record_bytes: u64,
) -> LiveGraphSources {
    let nodes: Box<dyn NodeRecordSource> = match (&base, &overlay) {
        (Some(b), Some(o)) => {
            let base_cursor = BaseNodeCursor::new(Arc::clone(b), freeze.clone());
            let mut ids: Vec<u64> = freeze.overlay_node_ids.iter().copied().collect();
            ids.sort_unstable();
            let overlay_cursor = OverlayNodeCursor::new(Arc::clone(o), ids, max_record_bytes);
            Box::new(MergedNodeSource::new(base_cursor, overlay_cursor))
        }
        (Some(b), None) => Box::new(BaseNodeCursor::new(Arc::clone(b), freeze.clone())),
        (None, Some(o)) => {
            let mut ids: Vec<u64> = freeze.overlay_node_ids.iter().copied().collect();
            ids.sort_unstable();
            Box::new(OverlayNodeCursor::new(Arc::clone(o), ids, max_record_bytes))
        }
        (None, None) => Box::new(EmptyNodeSource),
    };

    let edges: Box<dyn EdgeRecordSource> = match (&base, &overlay) {
        (Some(b), Some(o)) => {
            let base_cursor = BaseEdgeCursor::new(Arc::clone(b), freeze.clone());
            let mut ids: Vec<u64> = freeze.overlay_edge_ids.iter().copied().collect();
            ids.sort_unstable();
            let overlay_cursor = OverlayEdgeCursor::new(Arc::clone(o), ids, max_record_bytes);
            Box::new(MergedEdgeSource::new(base_cursor, overlay_cursor))
        }
        (Some(b), None) => Box::new(BaseEdgeCursor::new(Arc::clone(b), freeze.clone())),
        (None, Some(o)) => {
            let mut ids: Vec<u64> = freeze.overlay_edge_ids.iter().copied().collect();
            ids.sort_unstable();
            Box::new(OverlayEdgeCursor::new(Arc::clone(o), ids, max_record_bytes))
        }
        (None, None) => Box::new(EmptyEdgeSource),
    };

    LiveGraphSources { nodes, edges }
}

// ── Overlay cursors (budget-bounded, R1) ──────────────────────────

/// Streams overlay (LpgStore) nodes by iterating the frozen dirty-id set.
///
/// The id set is bounded by the admission budget, not graph-proportional.
/// Sorted at construction for deterministic output order.
struct OverlayNodeCursor {
    overlay: Arc<LpgStore>,
    ids: Vec<u64>,
    pos: usize,
    max_record_bytes: u64,
}

impl OverlayNodeCursor {
    fn new(overlay: Arc<LpgStore>, ids: Vec<u64>, max_record_bytes: u64) -> Self {
        Self {
            overlay,
            ids,
            pos: 0,
            max_record_bytes,
        }
    }
}

impl NodeRecordSource for OverlayNodeCursor {
    fn next_node(&mut self) -> Result<Option<GenerationNode>, GenerationError> {
        while self.pos < self.ids.len() {
            let raw_id = self.ids[self.pos];
            self.pos += 1;
            let Some(node) = self.overlay.get_node(NodeId::new(raw_id)) else {
                continue;
            };
            let mut labels: Vec<String> = node.labels.iter().map(|l| l.to_string()).collect();
            labels.sort();
            labels.dedup();
            if labels.is_empty() {
                return Err(GenerationError::InvalidInput(format!(
                    "overlay node {raw_id} has no labels"
                )));
            }
            let properties: FxHashMap<PropertyKey, Value> = node
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let prop_bytes = estimate_props_bytes(properties.iter());
            let estimated = estimate_node_bytes(&labels, prop_bytes);
            if estimated > self.max_record_bytes {
                return Err(GenerationError::BudgetExceeded {
                    counter: "max_record_bytes",
                    requested: estimated,
                    limit: self.max_record_bytes,
                });
            }
            return Ok(Some(GenerationNode {
                id: OriginalNodeId::new(raw_id),
                labels,
                properties,
            }));
        }
        Ok(None)
    }
}

/// Streams overlay (LpgStore) edges by iterating the frozen dirty-id set.
struct OverlayEdgeCursor {
    overlay: Arc<LpgStore>,
    ids: Vec<u64>,
    pos: usize,
    max_record_bytes: u64,
}

impl OverlayEdgeCursor {
    fn new(overlay: Arc<LpgStore>, ids: Vec<u64>, max_record_bytes: u64) -> Self {
        Self {
            overlay,
            ids,
            pos: 0,
            max_record_bytes,
        }
    }
}

impl EdgeRecordSource for OverlayEdgeCursor {
    fn next_edge(&mut self) -> Result<Option<GenerationEdge>, GenerationError> {
        while self.pos < self.ids.len() {
            let raw_id = self.ids[self.pos];
            self.pos += 1;
            let Some(edge) = self.overlay.get_edge(EdgeId::new(raw_id)) else {
                continue;
            };
            let properties: FxHashMap<PropertyKey, Value> = edge
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let edge_type = edge.edge_type.to_string();
            let prop_bytes = estimate_props_bytes(properties.iter());
            let estimated = (edge_type.len() + prop_bytes) as u64;
            if estimated > self.max_record_bytes {
                return Err(GenerationError::BudgetExceeded {
                    counter: "max_record_bytes",
                    requested: estimated,
                    limit: self.max_record_bytes,
                });
            }
            return Ok(Some(GenerationEdge {
                id: OriginalEdgeId::new(raw_id),
                src: OriginalNodeId::new(edge.src.as_u64()),
                dst: OriginalNodeId::new(edge.dst.as_u64()),
                edge_type,
                properties,
            }));
        }
        Ok(None)
    }
}

// ── LogicalLabelLookup (bounded membership index) ─────────────────

/// Schema-bounded lookup: physical label per table + multi-label extras.
///
/// The multi-label membership is stored in a compact
/// [`MappedLabelMembershipIndex`] (binary-search over sorted byte buffers)
/// instead of an O(N) `FxHashMap<u64, Vec<String>>`.
pub(crate) struct LogicalLabelLookup {
    physical: Vec<String>,
    /// Bounded mapped membership index (binary search over sorted run).
    /// None when no multi-label nodes exist.
    membership: Option<MappedLabelMembershipIndex>,
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
        let mut entries: Vec<(u64, Vec<String>)> = Vec::new();
        if let Some(lease) = membership {
            let mut merger = run_store.merger("membership")?;
            merger.merge_all(&lease.handles, budget, metrics, cancel, &mut |rec| {
                let (_physical, original_id) = staging::split_node_row_key(&rec.key)?;
                let labels = staging::decode_labels(&rec.payload)?;
                entries.push((original_id, labels));
                Ok(())
            })?;
        }
        // Re-sort by original_id (merge output is sorted by (label, id)).
        entries.sort_unstable_by_key(|(id, _)| *id);
        let membership = if entries.is_empty() {
            None
        } else {
            Some(MappedLabelMembershipIndex::from_sorted(&entries)?)
        };
        Ok(Self {
            physical: physical.to_vec(),
            membership,
        })
    }

    /// True when `label` is in the node's complete logical label set.
    #[must_use]
    pub(crate) fn has_label(&self, original_id: u64, table_id: u16, label: &str) -> bool {
        // If the node has a membership entry, it carries all its labels there.
        if let Some(ref membership) = self.membership {
            if membership.contains_node(original_id) {
                return membership.has_label(original_id, label);
            }
        }
        // No membership entry: node has only its physical label.
        self.physical
            .get(table_id as usize)
            .is_some_and(|l| l == label)
    }
}
