//! Secondary-structure cleanup for transaction rollback.
//!
//! `discard_uncommitted_versions` removes PENDING primary version chains, but
//! create paths (row and batch) also eagerly publish into non-versioned
//! secondary structures: `label_index`, `node_labels`, property columns /
//! indexes, adjacency, edge-type live counts, and live counters. The W1
//! contract requires those staged entries to disappear on rollback rather than
//! remaining as filtered-but-orphaned residue.
//!
//! This module walks entities that were *created* inside the rolled-back
//! transaction (their version chain becomes empty) and erases the matching
//! secondary state. Property/label *mutations* on pre-existing entities remain
//! covered by the property undo log.

use super::LpgStore;
use grafeo_common::types::{EdgeId, HashableValue, NodeId, PropertyKey, TransactionId};
use std::sync::atomic::Ordering;

/// Edge identity needed to reverse adjacency / type-count publication.
#[derive(Clone, Copy)]
pub(super) struct DiscardedEdge {
    pub id: EdgeId,
    pub src: NodeId,
    pub dst: NodeId,
    pub type_id: u32,
}

impl LpgStore {
    /// Removes every secondary structure entry published for nodes that were
    /// created inside `transaction_id` and are therefore fully discarded.
    pub(super) fn cleanup_discarded_node_secondaries(&self, node_ids: &[NodeId]) {
        self.cleanup_node_secondaries(node_ids, true);
    }

    /// Body of [`Self::cleanup_discarded_node_secondaries`]. With
    /// `remove_index_postings == false` the property-index postings are kept
    /// (see [`Self::purge_copied_node`]).
    fn cleanup_node_secondaries(&self, node_ids: &[NodeId], remove_index_postings: bool) {
        if node_ids.is_empty() {
            return;
        }

        // Capture label sets and property maps before tearing anything down.
        let label_sets: Vec<(NodeId, Vec<u32>)> = {
            let labels = self.node_labels.read();
            node_ids
                .iter()
                .filter_map(|&id| {
                    #[cfg(not(feature = "temporal"))]
                    let set = labels.get(&id)?;
                    #[cfg(feature = "temporal")]
                    let set = labels.get(&id)?.latest()?;
                    Some((id, set.iter().copied().collect()))
                })
                .collect()
        };

        let prop_maps: Vec<(NodeId, Vec<(PropertyKey, grafeo_common::types::Value)>)> = node_ids
            .iter()
            .map(|&id| {
                let props: Vec<_> = self.node_properties.get_all(id).into_iter().collect();
                (id, props)
            })
            .collect();

        // label_index + node_labels
        {
            let mut index = self.label_index.write();
            for (id, label_ids) in &label_sets {
                for &label_id in label_ids {
                    if let Some(set) = index.get_mut(label_id as usize) {
                        set.remove(id);
                    }
                }
            }
        }
        {
            let mut labels = self.node_labels.write();
            for &id in node_ids {
                labels.remove(&id);
            }
        }

        // Property indexes, then property columns.
        if remove_index_postings {
            let indexes = self.property_indexes.read();
            for (id, props) in &prop_maps {
                for (key, value) in props {
                    if let Some(index) = indexes.get(key) {
                        let hv = HashableValue::new(value.clone());
                        if let Some(mut nodes) = index.get_mut(&hv) {
                            nodes.remove(id);
                            if nodes.is_empty() {
                                drop(nodes);
                                index.remove(&hv);
                            }
                        }
                    }
                }
            }
        }
        for &id in node_ids {
            #[cfg(not(feature = "temporal"))]
            self.node_properties.remove_all(id);
            // Erase, don't tombstone: the whole history belongs to the
            // discarded row, and a tombstone at the current epoch would land
            // behind the transaction's PENDING entries.
            #[cfg(feature = "temporal")]
            self.node_properties.erase_all_history(id);
        }

        // reason: discarded batch sizes are bounded by practical graph limits
        #[allow(clippy::cast_possible_wrap)]
        let count = node_ids.len() as i64;
        self.live_node_count.fetch_sub(count, Ordering::Relaxed);
    }

    /// Removes every secondary structure entry published for edges that were
    /// created inside `transaction_id` and are therefore fully discarded.
    pub(super) fn cleanup_discarded_edge_secondaries(&self, edges: &[DiscardedEdge]) {
        if edges.is_empty() {
            return;
        }

        let fwd: Vec<(NodeId, EdgeId)> = edges.iter().map(|e| (e.src, e.id)).collect();
        self.forward_adj.batch_remove_edges(&fwd);
        if let Some(ref backward) = self.backward_adj {
            let back: Vec<(NodeId, EdgeId)> = edges.iter().map(|e| (e.dst, e.id)).collect();
            backward.batch_remove_edges(&back);
        }

        for edge in edges {
            self.decrement_edge_type_count(edge.type_id);
            #[cfg(not(feature = "temporal"))]
            self.edge_properties.remove_all(edge.id);
            // See `cleanup_node_secondaries`.
            #[cfg(feature = "temporal")]
            self.edge_properties.erase_all_history(edge.id);
        }

        // reason: discarded batch sizes are bounded by practical graph limits
        #[allow(clippy::cast_possible_wrap)]
        let count = edges.len() as i64;
        self.live_edge_count.fetch_sub(count, Ordering::Relaxed);
    }

    /// Collects node IDs whose version chains are solely owned by `transaction_id`.
    #[cfg(not(feature = "tiered-storage"))]
    pub(super) fn collect_solely_created_nodes(
        &self,
        transaction_id: TransactionId,
    ) -> Vec<NodeId> {
        self.nodes
            .read()
            .iter()
            .filter(|(_, chain)| chain.solely_created_by(transaction_id))
            .map(|(&id, _)| id)
            .collect()
    }

    /// Collects edge metadata for chains solely owned by `transaction_id`.
    #[cfg(not(feature = "tiered-storage"))]
    pub(super) fn collect_solely_created_edges(
        &self,
        transaction_id: TransactionId,
    ) -> Vec<DiscardedEdge> {
        self.edges
            .read()
            .iter()
            .filter_map(|(&id, chain)| {
                if !chain.solely_created_by(transaction_id) {
                    return None;
                }
                let record = chain.latest()?;
                Some(DiscardedEdge {
                    id,
                    src: record.src,
                    dst: record.dst,
                    type_id: record.type_id,
                })
            })
            .collect()
    }

    /// Collects node IDs whose version indexes are solely owned by `transaction_id`.
    #[cfg(feature = "tiered-storage")]
    pub(super) fn collect_solely_created_nodes(
        &self,
        transaction_id: TransactionId,
    ) -> Vec<NodeId> {
        self.node_versions
            .read()
            .iter()
            .filter(|(_, index)| index.solely_created_by(transaction_id))
            .map(|(&id, _)| id)
            .collect()
    }

    /// Collects edge metadata for indexes solely owned by `transaction_id`.
    #[cfg(feature = "tiered-storage")]
    pub(super) fn collect_solely_created_edges(
        &self,
        transaction_id: TransactionId,
    ) -> Vec<DiscardedEdge> {
        let versions = self.edge_versions.read();
        let mut out = Vec::new();
        for (&id, index) in versions.iter() {
            if !index.solely_created_by(transaction_id) {
                continue;
            }
            // Own PENDING versions are visible to the creating transaction.
            let Some(vref) = index
                .visible_to(grafeo_common::types::EpochId::PENDING, transaction_id)
                .or_else(|| index.latest())
            else {
                continue;
            };
            let Some(record) = self.read_edge_record(&vref) else {
                continue;
            };
            out.push(DiscardedEdge {
                id,
                src: record.src,
                dst: record.dst,
                type_id: record.type_id,
            });
        }
        out
    }
}

impl LpgStore {
    /// Removes the overlay copy of a base node outright, whatever
    /// transaction wrote it: its version chain, labels, label index entries,
    /// property columns and live counter.
    ///
    /// Used by `LayeredStore` to undo a rolled-back copy-up: the copy was
    /// created at epoch 0 by the system transaction (so concurrent readers
    /// keep seeing the base values), so the MVCC discard cannot remove it.
    /// Returns whether the node existed.
    ///
    /// With `temporal`, the copy's property history is erased rather than
    /// tombstoned: the copy existed only to stand for the base row, and a
    /// later copy-up seeds that history again at epoch 0.
    ///
    /// Property-index postings are deliberately KEPT: on a layered store the
    /// overlay's property indexes also carry the base nodes' postings
    /// (`GrafeoDB::create_property_index` builds them from the merged view)
    /// and are the only path lookups use. By the time of the purge the
    /// overlay's undo has restored the copy to the base values, so its
    /// postings are exactly the base node's.
    #[doc(hidden)]
    pub fn purge_copied_node(&self, id: NodeId) -> bool {
        #[cfg(not(feature = "tiered-storage"))]
        let existed = self.nodes.write().remove(&id).is_some();
        #[cfg(feature = "tiered-storage")]
        let existed = self.node_versions.write().remove(&id).is_some();
        if existed {
            self.cleanup_node_secondaries(&[id], false);
        }
        existed
    }

    /// Edge variant of [`Self::purge_copied_node`]: removes the version chain, both
    /// adjacency entries, the edge-type count and the property columns (with
    /// `temporal`, their whole history).
    #[doc(hidden)]
    pub fn purge_edge(&self, id: EdgeId) -> bool {
        #[cfg(not(feature = "tiered-storage"))]
        let discarded = {
            let removed = self.edges.write().remove(&id);
            removed.and_then(|chain| {
                chain.latest().map(|record| DiscardedEdge {
                    id,
                    src: record.src,
                    dst: record.dst,
                    type_id: record.type_id,
                })
            })
        };
        #[cfg(feature = "tiered-storage")]
        let discarded = {
            let removed = self.edge_versions.write().remove(&id);
            removed.and_then(|index| {
                let vref = index.latest()?;
                let record = self.read_edge_record(&vref)?;
                Some(DiscardedEdge {
                    id,
                    src: record.src,
                    dst: record.dst,
                    type_id: record.type_id,
                })
            })
        };
        let Some(edge) = discarded else {
            return false;
        };
        self.cleanup_discarded_edge_secondaries(&[edge]);
        true
    }
}
