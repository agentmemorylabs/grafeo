//! Chain-backed record sources for G-MIDFLUSH.1 builder publish.
//!
//! When `mid_build_tiers` is non-empty, the final publish must stream all
//! tiers + overlay through `BoundedGenerationBuilder` without re-streaming
//! tiers via the normal base+overlay merge (that would be O(graph) per drain).
//! `TierChainView` already unions tiers+overlay for reads; these sources walk
//! it row-by-row as the bounded `NodeRecordSource`/`EdgeRecordSource` the
//! orchestrator consumes. The external merge sort in `NodePass::stage`
//! restores canonical `(label,id)` order, so input order is irrelevant.

use std::sync::Arc;

use grafeo_common::types::{PropertyKey, Value};
use grafeo_common::utils::hash::FxHashMap;
use grafeo_core::graph::compact::generation::{
    EdgeRecordSource, GenerationEdge, GenerationError, GenerationNode, NodeRecordSource,
    OriginalEdgeId, OriginalNodeId,
};
use grafeo_core::graph::traits::GraphStore;
use grafeo_core::graph::Direction;

/// Walks a `GraphStore` (the `TierChainView`) row-by-row.
pub struct ChainNodeSource {
    store: Arc<dyn GraphStore>,
    ids: Vec<grafeo_common::types::NodeId>,
    pos: usize,
    max_record_bytes: u64,
}

impl ChainNodeSource {
    pub fn new(store: Arc<dyn GraphStore>, max_record_bytes: u64) -> Self {
        let ids = store.node_ids();
        Self {
            store,
            ids,
            pos: 0,
            max_record_bytes,
        }
    }
}

impl NodeRecordSource for ChainNodeSource {
    fn next_node(&mut self) -> Result<Option<GenerationNode>, GenerationError> {
        while self.pos < self.ids.len() {
            let id = self.ids[self.pos];
            self.pos += 1;
            let Some(node) = self.store.get_node(id) else {
                continue;
            };
            let mut labels: Vec<String> = node.labels.iter().map(|l| l.to_string()).collect();
            labels.sort();
            labels.dedup();
            if labels.is_empty() {
                return Err(GenerationError::InvalidInput(format!(
                    "chain node {} has no labels",
                    id.as_u64()
                )));
            }
            let mut properties = FxHashMap::default();
            for (k, v) in node.properties.iter() {
                properties.insert(k.clone(), v.clone());
            }
            // Budget check: estimate record bytes like live_graph does.
            let prop_bytes: usize = properties
                .iter()
                .map(|(k, v)| k.as_str().len() + estimate_value_bytes(v))
                .sum();
            let label_bytes: usize = labels.iter().map(|l| l.len()).sum();
            let estimated = (label_bytes + prop_bytes + 64) as u64;
            if estimated > self.max_record_bytes {
                return Err(GenerationError::BudgetExceeded {
                    counter: "max_record_bytes",
                    requested: estimated,
                    limit: self.max_record_bytes,
                });
            }
            return Ok(Some(GenerationNode {
                id: OriginalNodeId::new(id.as_u64()),
                labels,
                properties,
            }));
        }
        Ok(None)
    }
}

pub struct ChainEdgeSource {
    store: Arc<dyn GraphStore>,
    edges: Vec<(grafeo_common::types::NodeId, grafeo_common::types::EdgeId)>,
    pos: usize,
    max_record_bytes: u64,
}

impl ChainEdgeSource {
    pub fn new(store: Arc<dyn GraphStore>, max_record_bytes: u64) -> Self {
        let mut edges = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for nid in store.node_ids() {
            for (dst, eid) in store.edges_from(nid, Direction::Outgoing) {
                if seen.insert(eid.as_u64()) {
                    edges.push((nid, eid));
                    let _ = dst;
                }
            }
        }
        // Deduplicate and sort for determinism (like FrozenLiveGraph).
        edges.sort_by_key(|(_, eid)| eid.as_u64());
        edges.dedup_by_key(|(_, eid)| *eid);
        Self {
            store,
            edges,
            pos: 0,
            max_record_bytes,
        }
    }
}

impl EdgeRecordSource for ChainEdgeSource {
    fn next_edge(&mut self) -> Result<Option<GenerationEdge>, GenerationError> {
        while self.pos < self.edges.len() {
            let (_src_hint, eid) = self.edges[self.pos];
            self.pos += 1;
            let Some(edge) = self.store.get_edge(eid) else {
                continue;
            };
            let mut properties = FxHashMap::default();
            for (k, v) in edge.properties.iter() {
                properties.insert(k.clone(), v.clone());
            }
            let estimated = (edge.edge_type.len() + properties.len() * 32 + 64) as u64;
            if estimated > self.max_record_bytes {
                return Err(GenerationError::BudgetExceeded {
                    counter: "max_record_bytes",
                    requested: estimated,
                    limit: self.max_record_bytes,
                });
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

fn estimate_value_bytes(v: &Value) -> usize {
    match v {
        Value::Null => 1,
        Value::Bool(_) => 1,
        Value::Int64(_) => 8,
        Value::Float64(_) => 8,
        Value::String(s) => s.len(),
        Value::Vector(vec) => vec.len() * 4,
        _ => 32,
    }
}
