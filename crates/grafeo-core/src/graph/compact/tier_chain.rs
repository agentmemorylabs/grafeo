use std::sync::Arc;

use arcstr::ArcStr;
use grafeo_common::types::{EdgeId, EpochId, NodeId, PropertyKey, TransactionId, Value};
use grafeo_common::utils::hash::FxHashMap;

use crate::graph::compact::CompactStore;
use crate::graph::lpg::LpgStore;
use crate::graph::traits::{GraphStore, GraphStoreSearch};
use crate::graph::Direction;
#[allow(unused_imports)]
use crate::index::vector::DistanceMetric;
use crate::statistics::Statistics;

/// Builder-scoped tier chain: `tiers + overlay`.
///
/// Tiers hold DISJOINT CONTIGUOUS id ranges in allocation order and, per the
/// G-MIDFLUSH.1 scope guardrail, node-phase tiers contain ZERO edges — all
/// edges live in the live overlay until final publish. Where a method needs
/// edge data it delegates to the overlay; where a method needs node counts the
/// tiers+overlay are disjoint so concat is exact.
///
/// This is NOT the general serving N-tier merge (that needs shadow/tombstone
/// logic); it is scoped to the builder single-writer phase.
pub struct TierChainView {
    tiers: Vec<Arc<CompactStore>>,
    overlay: Arc<LpgStore>,
    // Cached per-tier [min_node_id, max_node_id] for binary search routing.
    tier_ranges: Vec<(u64, u64)>,
}

impl TierChainView {
    pub fn new(tiers: Vec<Arc<CompactStore>>, overlay: Arc<LpgStore>) -> Self {
        let mut tier_ranges = Vec::with_capacity(tiers.len());
        for t in &tiers {
            let min = tier_min_node_id(t);
            let max = tier_max_node_id(t);
            tier_ranges.push((min, max));
        }
        Self {
            tiers,
            overlay,
            tier_ranges,
        }
    }

    fn tier_for_node(&self, id: NodeId) -> Option<&Arc<CompactStore>> {
        let target = id.as_u64();
        // Tiers are allocation-ordered disjoint contiguous ranges → binary search
        let mut lo = 0usize;
        let mut hi = self.tier_ranges.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (min, max) = self.tier_ranges[mid];
            if target < min {
                hi = mid;
            } else if target > max {
                lo = mid + 1;
            } else {
                return Some(&self.tiers[mid]);
            }
        }
        None
    }

    fn tier_for_edge(&self, id: EdgeId) -> Option<&Arc<CompactStore>> {
        let target = id.as_u64();
        // Edge tiers are not used during build (node-phase tiers have zero
        // edges) but keep the same routing for future correctness if ever
        // called — linear fallback is fine edge-wise.
        for (idx, t) in self.tiers.iter().enumerate() {
            let (min, max) = tier_edge_range(t);
            if min <= target && target <= max && min != u64::MAX {
                let _ = idx;
                return Some(t);
            }
        }
        None
    }
}

fn tier_min_node_id(store: &CompactStore) -> u64 {
    if store.total_nodes() == 0 {
        return u64::MAX;
    }
    store
        .node_ids()
        .first()
        .map(|id| id.as_u64())
        .unwrap_or(u64::MAX)
}

fn tier_max_node_id(store: &CompactStore) -> u64 {
    if store.total_nodes() == 0 {
        return u64::MAX;
    }
    store
        .node_ids()
        .last()
        .map(|id| id.as_u64())
        .unwrap_or(0)
}

fn tier_edge_range(store: &CompactStore) -> (u64, u64) {
    if store.total_edges() == 0 {
        return (u64::MAX, 0);
    }
    // CompactStore edge ids are not directly enumerable without scanning;
    // approximate by scanning node_ids -> edges_from when needed.
    // For the builder case where tiers have zero edges, this is trivially (MAX,0).
    // We enumerate once via edge scan if non-empty (bounded by window).
    let mut min_eid = u64::MAX;
    let mut max_eid = 0u64;
    for nid in store.node_ids() {
        for (_, eid) in store.edges_from(nid, Direction::Outgoing) {
            let v = eid.as_u64();
            if v < min_eid {
                min_eid = v;
            }
            if v > max_eid {
                max_eid = v;
            }
        }
    }
    (min_eid, max_eid)
}

impl GraphStore for TierChainView {
    fn get_node(&self, id: NodeId) -> Option<crate::graph::lpg::Node> {
        if let Some(t) = self.tier_for_node(id) {
            if let Some(n) = t.get_node(id) {
                return Some(n);
            }
        }
        self.overlay.get_node(id)
    }

    fn get_edge(&self, id: EdgeId) -> Option<crate::graph::lpg::Edge> {
        // Node-phase tiers have zero edges → overlay only in practice.
        if let Some(t) = self.tier_for_edge(id) {
            if let Some(e) = t.get_edge(id) {
                return Some(e);
            }
        }
        self.overlay.get_edge(id)
    }

    fn get_node_versioned(
        &self,
        id: NodeId,
        epoch: EpochId,
        transaction_id: TransactionId,
    ) -> Option<crate::graph::lpg::Node> {
        if let Some(t) = self.tier_for_node(id) {
            if let Some(n) = t.get_node_versioned(id, epoch, transaction_id) {
                return Some(n);
            }
        }
        self.overlay.get_node_versioned(id, epoch, transaction_id)
    }

    fn get_edge_versioned(
        &self,
        id: EdgeId,
        epoch: EpochId,
        transaction_id: TransactionId,
    ) -> Option<crate::graph::lpg::Edge> {
        if let Some(t) = self.tier_for_edge(id) {
            if let Some(e) = t.get_edge_versioned(id, epoch, transaction_id) {
                return Some(e);
            }
        }
        self.overlay.get_edge_versioned(id, epoch, transaction_id)
    }

    fn get_node_at_epoch(&self, id: NodeId, epoch: EpochId) -> Option<crate::graph::lpg::Node> {
        if let Some(t) = self.tier_for_node(id) {
            if let Some(n) = t.get_node_at_epoch(id, epoch) {
                return Some(n);
            }
        }
        self.overlay.get_node_at_epoch(id, epoch)
    }

    fn get_edge_at_epoch(&self, id: EdgeId, epoch: EpochId) -> Option<crate::graph::lpg::Edge> {
        if let Some(t) = self.tier_for_edge(id) {
            if let Some(e) = t.get_edge_at_epoch(id, epoch) {
                return Some(e);
            }
        }
        self.overlay.get_edge_at_epoch(id, epoch)
    }

    fn get_node_property(&self, id: NodeId, key: &PropertyKey) -> Option<Value> {
        if let Some(t) = self.tier_for_node(id) {
            if let Some(v) = t.get_node_property(id, key) {
                return Some(v);
            }
        }
        self.overlay.get_node_property(id, key)
    }

    fn get_edge_property(&self, id: EdgeId, key: &PropertyKey) -> Option<Value> {
        if let Some(t) = self.tier_for_edge(id) {
            if let Some(v) = t.get_edge_property(id, key) {
                return Some(v);
            }
        }
        self.overlay.get_edge_property(id, key)
    }

    fn get_node_property_batch(&self, ids: &[NodeId], key: &PropertyKey) -> Vec<Option<Value>> {
        ids.iter().map(|id| self.get_node_property(*id, key)).collect()
    }

    fn get_nodes_properties_batch(&self, ids: &[NodeId]) -> Vec<FxHashMap<PropertyKey, Value>> {
        ids.iter()
            .map(|id| {
                self.get_node(*id)
                    .map(|n| n.properties.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .unwrap_or_default()
            })
            .collect()
    }

    fn get_nodes_properties_selective_batch(
        &self,
        ids: &[NodeId],
        keys: &[PropertyKey],
    ) -> Vec<FxHashMap<PropertyKey, Value>> {
        ids.iter()
            .map(|id| {
                let mut map = FxHashMap::default();
                for key in keys {
                    if let Some(v) = self.get_node_property(*id, key) {
                        map.insert(key.clone(), v);
                    }
                }
                map
            })
            .collect()
    }

    fn get_edges_properties_selective_batch(
        &self,
        ids: &[EdgeId],
        keys: &[PropertyKey],
    ) -> Vec<FxHashMap<PropertyKey, Value>> {
        ids.iter()
            .map(|id| {
                let mut map = FxHashMap::default();
                for key in keys {
                    if let Some(v) = self.get_edge_property(*id, key) {
                        map.insert(key.clone(), v);
                    }
                }
                map
            })
            .collect()
    }

    fn neighbors(&self, node: NodeId, direction: Direction) -> Vec<NodeId> {
        let mut out = Vec::new();
        for t in &self.tiers {
            out.extend(t.neighbors(node, direction));
        }
        out.extend(<LpgStore as GraphStore>::neighbors(self.overlay.as_ref(), node, direction));
        out
    }

    fn edges_from(&self, node: NodeId, direction: Direction) -> Vec<(NodeId, EdgeId)> {
        let mut out = Vec::new();
        for t in &self.tiers {
            out.extend(t.edges_from(node, direction));
        }
        out.extend(<LpgStore as GraphStore>::edges_from(self.overlay.as_ref(), node, direction));
        out
    }

    fn out_degree(&self, node: NodeId) -> usize {
        let mut n = 0;
        for t in &self.tiers {
            n += t.out_degree(node);
        }
        n + self.overlay.out_degree(node)
    }

    fn in_degree(&self, node: NodeId) -> usize {
        let mut n = 0;
        for t in &self.tiers {
            n += t.in_degree(node);
        }
        n + self.overlay.in_degree(node)
    }

    fn has_backward_adjacency(&self) -> bool {
        self.overlay.has_backward_adjacency()
    }

    fn node_ids(&self) -> Vec<NodeId> {
        let mut out = Vec::new();
        for t in &self.tiers {
            out.extend(t.node_ids());
        }
        out.extend(self.overlay.node_ids());
        // Tiers are disjoint + overlay disjoint from tiers by watermark swap,
        // so no dedup needed. Keep allocation order (tier 0..N then overlay).
        out
    }

    fn nodes_by_label(&self, label: &str) -> Vec<NodeId> {
        let mut out = Vec::new();
        for t in &self.tiers {
            out.extend(t.nodes_by_label(label));
        }
        out.extend(self.overlay.nodes_by_label(label));
        out
    }

    fn nodes_by_label_count(&self, label: &str) -> usize {
        let mut n = 0;
        for t in &self.tiers {
            n += t.nodes_by_label_count(label);
        }
        n + self.overlay.nodes_by_label_count(label)
    }

    fn node_count(&self) -> usize {
        let mut n = 0;
        for t in &self.tiers {
            n += t.node_count();
        }
        n + self.overlay.node_count()
    }

    fn edge_count(&self) -> usize {
        // Tiers have zero edges during node phase; overlay holds all edges.
        let mut n = 0;
        for t in &self.tiers {
            n += t.edge_count();
        }
        n + self.overlay.edge_count()
    }

    fn edge_type(&self, id: EdgeId) -> Option<ArcStr> {
        if let Some(t) = self.tier_for_edge(id) {
            if let Some(v) = t.edge_type(id) {
                return Some(v);
            }
        }
        self.overlay.edge_type(id)
    }

    fn find_nodes_by_property(&self, property: &str, value: &Value) -> Vec<NodeId> {
        let mut out = Vec::new();
        for t in &self.tiers {
            out.extend(t.find_nodes_by_property(property, value));
        }
        out.extend(self.overlay.find_nodes_by_property(property, value));
        out
    }

    fn find_nodes_by_properties(&self, conditions: &[(&str, Value)]) -> Vec<NodeId> {
        let mut out = Vec::new();
        for t in &self.tiers {
            out.extend(t.find_nodes_by_properties(conditions));
        }
        out.extend(self.overlay.find_nodes_by_properties(conditions));
        out
    }

    fn find_nodes_in_range(
        &self,
        property: &str,
        min: Option<&Value>,
        max: Option<&Value>,
        min_inclusive: bool,
        max_inclusive: bool,
    ) -> Vec<NodeId> {
        let mut out = Vec::new();
        for t in &self.tiers {
            out.extend(t.find_nodes_in_range(property, min, max, min_inclusive, max_inclusive));
        }
        out.extend(
            self.overlay
                .find_nodes_in_range(property, min, max, min_inclusive, max_inclusive),
        );
        out
    }

    fn node_property_might_match(
        &self,
        property: &PropertyKey,
        op: crate::graph::lpg::CompareOp,
        value: &Value,
    ) -> bool {
        for t in &self.tiers {
            if t.node_property_might_match(property, op, value) {
                return true;
            }
        }
        self.overlay.node_property_might_match(property, op, value)
    }

    fn edge_property_might_match(
        &self,
        property: &PropertyKey,
        op: crate::graph::lpg::CompareOp,
        value: &Value,
    ) -> bool {
        for t in &self.tiers {
            if t.edge_property_might_match(property, op, value) {
                return true;
            }
        }
        self.overlay.edge_property_might_match(property, op, value)
    }

    fn statistics(&self) -> Arc<Statistics> {
        // Merge tier statistics with overlay statistics. For builder correctness
        // the cheapest faithful merge is to delegate to overlay for now — the
        // builder's statistics are only used for cost-based optimizer hints
        // during the final validate/HNSW phases, not for correctness.
        // Merging per-tier statistics would require a Statistics merge helper
        // that does not exist; using overlay keeps behavior simple.
        // Callers that need exact counts use node_count/edge_count above.
        self.overlay.statistics()
    }

    fn estimate_label_cardinality(&self, label: &str) -> f64 {
        let mut sum = 0.0;
        for t in &self.tiers {
            sum += t.estimate_label_cardinality(label);
        }
        sum + self.overlay.estimate_label_cardinality(label)
    }

    fn estimate_avg_degree(&self, edge_type: &str, outgoing: bool) -> f64 {
        // Edges only in overlay during build, but include tier contribution.
        let mut total = 0.0;
        let mut count = 0usize;
        for t in &self.tiers {
            total += t.estimate_avg_degree(edge_type, outgoing);
            count += 1;
        }
        total += self.overlay.estimate_avg_degree(edge_type, outgoing);
        if count == 0 {
            total
        } else {
            // Weighted average would need edge counts per tier; simple sum is
            // fine since tiers have zero edges.
            total
        }
    }

    fn current_epoch(&self) -> EpochId {
        self.overlay.current_epoch()
    }

    fn all_labels(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for t in &self.tiers {
            for l in t.all_labels() {
                if seen.insert(l.clone()) {
                    out.push(l);
                }
            }
        }
        for l in self.overlay.all_labels() {
            if seen.insert(l.clone()) {
                out.push(l);
            }
        }
        out
    }

    fn all_edge_types(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for t in &self.tiers {
            for e in t.all_edge_types() {
                if seen.insert(e.clone()) {
                    out.push(e);
                }
            }
        }
        for e in self.overlay.all_edge_types() {
            if seen.insert(e.clone()) {
                out.push(e);
            }
        }
        out
    }

    fn all_property_keys(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for t in &self.tiers {
            for k in t.all_property_keys() {
                if seen.insert(k.clone()) {
                    out.push(k);
                }
            }
        }
        for k in self.overlay.all_property_keys() {
            if seen.insert(k.clone()) {
                out.push(k);
            }
        }
        out
    }

    fn is_node_visible_at_epoch(&self, id: NodeId, epoch: EpochId) -> bool {
        if let Some(t) = self.tier_for_node(id) {
            if t.is_node_visible_at_epoch(id, epoch) {
                return true;
            }
        }
        self.overlay.is_node_visible_at_epoch(id, epoch)
    }

    fn is_node_visible_versioned(
        &self,
        id: NodeId,
        epoch: EpochId,
        transaction_id: TransactionId,
    ) -> bool {
        if let Some(t) = self.tier_for_node(id) {
            if t.is_node_visible_versioned(id, epoch, transaction_id) {
                return true;
            }
        }
        self.overlay
            .is_node_visible_versioned(id, epoch, transaction_id)
    }

    fn is_edge_visible_at_epoch(&self, id: EdgeId, epoch: EpochId) -> bool {
        if let Some(t) = self.tier_for_edge(id) {
            if t.is_edge_visible_at_epoch(id, epoch) {
                return true;
            }
        }
        self.overlay.is_edge_visible_at_epoch(id, epoch)
    }

    fn is_edge_visible_versioned(
        &self,
        id: EdgeId,
        epoch: EpochId,
        transaction_id: TransactionId,
    ) -> bool {
        if let Some(t) = self.tier_for_edge(id) {
            if t.is_edge_visible_versioned(id, epoch, transaction_id) {
                return true;
            }
        }
        self.overlay
            .is_edge_visible_versioned(id, epoch, transaction_id)
    }
}

impl GraphStoreSearch for TierChainView {
    #[cfg(feature = "text-index")]
    fn has_text_index(&self, _label: &str, _property: &str) -> bool {
        false
    }

    #[cfg(feature = "text-index")]
    fn score_text(
        &self,
        _node_id: NodeId,
        _label: &str,
        _property: &str,
        _query: &str,
    ) -> Option<f64> {
        None
    }

    #[cfg(feature = "text-index")]
    fn text_search(
        &self,
        _label: &str,
        _property: &str,
        _query: &str,
        _k: usize,
    ) -> Vec<(NodeId, f64)> {
        Vec::new()
    }

    #[cfg(feature = "text-index")]
    fn text_search_with_threshold(
        &self,
        _label: &str,
        _property: &str,
        _query: &str,
        _threshold: f64,
    ) -> Vec<(NodeId, f64)> {
        Vec::new()
    }

    #[cfg(feature = "vector-index")]
    fn has_vector_index(&self, _label: &str, _property: &str) -> bool {
        false
    }

    #[cfg(feature = "vector-index")]
    fn vector_index_metric(&self, _label: &str, _property: &str) -> Option<DistanceMetric> {
        None
    }

    #[cfg(feature = "vector-index")]
    fn vector_search(
        &self,
        _label: Option<&str>,
        _property: &str,
        _query: &[f32],
        _k: usize,
        _metric: DistanceMetric,
    ) -> Vec<(NodeId, f64)> {
        Vec::new()
    }

    #[cfg(feature = "vector-index")]
    fn vector_search_with_threshold(
        &self,
        _label: Option<&str>,
        _property: &str,
        _query: &[f32],
        _threshold: f64,
        _metric: DistanceMetric,
    ) -> Vec<(NodeId, f64)> {
        Vec::new()
    }
}
