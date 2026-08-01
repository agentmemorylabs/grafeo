//! CompactStore: a read-only columnar store for memory-constrained environments.
//!
//! Implements [`GraphStore`](crate::graph::traits::GraphStore) using per-label
//! columnar tables and double-indexed CSR adjacency. Designed for static
//! snapshot data in WASM, edge workers, and embedded devices.
//! Fully behind `#[cfg(feature = "compact-store")]`.

/// Builder API for constructing a [`CompactStore`] from raw data.
pub mod builder;
/// Columnar codecs for node and edge properties.
pub mod column;
/// Compressed Sparse Row (CSR) adjacency representation.
pub mod csr;
/// Container section serialization for the layered overlay deletion log.
#[cfg(feature = "lpg")]
pub mod deletions_section;
/// Source-true CompactStore generation on bounded runs (G-EM0.W0-A2).
pub mod generation;
mod graph_store_impl;
/// Node/edge ID encoding and decoding helpers.
pub mod id;
/// Two-layer store: columnar base + mutable LPG overlay.
#[cfg(feature = "lpg")]
pub mod layered;
/// Mapped CompactStore v5 views and accounting (G-EM0.2).
pub mod mapped;
/// Per-label node tables with columnar property storage.
pub mod node_table;
/// Overlay retained-capacity accounting, admission, and backpressure (G-EM0.5a).
#[cfg(feature = "lpg")]
pub mod overlay_budget;
/// Retained-capacity cost estimation for overlay mutations (G-EM0.5a).
#[cfg(feature = "lpg")]
pub mod overlay_cost;
/// Per-type relationship tables backed by forward/backward CSR.
pub mod rel_table;
/// Schema definitions for node tables and edge schemas.
pub mod schema;
/// Container section serialization for CompactStore.
pub mod section;
/// CompactStore payload version 5 codec.
pub(crate) mod section_v5;
#[cfg(test)]
mod tests;
/// Zone maps for skip-pruning predicate evaluation.
pub mod zone_map;

pub use builder::{CompactStoreBuilder, from_graph_store, from_graph_store_preserving_ids};

use std::sync::Arc;

use arcstr::ArcStr;
use bytes::Bytes;
use grafeo_common::types::{EdgeId, NodeId};
use grafeo_common::utils::hash::FxHashMap;

use self::node_table::NodeTable;
use self::rel_table::RelTable;
use crate::graph::Direction;
use crate::statistics::Statistics;

/// Proportional anonymous heap retained by a column codec.
fn codec_proportional_heap(codec: &column::ColumnCodec, mapped_open: bool) -> usize {
    match codec {
        column::ColumnCodec::Dict(d) => {
            // Codes: charge only inline (heap) codes.
            let codes = if d.as_codes_slice().is_some() {
                d.code_count() * 4
            } else {
                0
            };
            d.dictionary_heap_bytes().saturating_add(codes)
        }
        // On a mapped open, non-dict bodies are Bytes slices into the mapping.
        _ if mapped_open => 0,
        other => other.heap_bytes(),
    }
}

/// A read-only columnar graph store.
///
/// Node data is stored in per-label [`NodeTable`]s and edge data in per-type
/// [`RelTable`]s. The store is immutable after construction: use
/// [`CompactStoreBuilder`] to populate it from raw data.
pub struct CompactStore {
    /// Node tables indexed by table_id for O(1) lookup from NodeId.
    node_tables_by_id: Vec<NodeTable>,
    /// table_id lookup from label string (for nodes_by_label).
    label_to_table_id: FxHashMap<ArcStr, u16>,
    /// Relationship tables indexed by rel_table_id for O(1) lookup from EdgeId.
    rel_tables_by_id: Vec<RelTable>,
    /// rel_table_id lookup from edge type string (one edge type may span
    /// multiple src/dst label combinations, so the value is a Vec).
    edge_type_to_rel_id: FxHashMap<ArcStr, Vec<u16>>,
    /// Lookup: table ID -> label.
    table_id_to_label: Vec<ArcStr>,
    /// Lookup: rel table ID -> edge type.
    rel_table_id_to_type: Vec<ArcStr>,
    /// Pre-computed: for each node table_id, the rel_table_ids where it is the source.
    src_rel_table_ids: Vec<Vec<u16>>,
    /// Pre-computed: for each node table_id, the rel_table_ids where it is the destination.
    dst_rel_table_ids: Vec<Vec<u16>>,
    /// Cached statistics.
    statistics: Arc<Statistics>,

    // ── ID-preserving maps (for layered store integration) ──────────
    /// Maps original `NodeId` to (table_id, row_offset). Present when the
    /// store was built with [`from_graph_store_preserving_ids`].
    node_id_map: Option<FxHashMap<NodeId, (u16, u64)>>,
    /// Maps original `EdgeId` to (rel_table_id, csr_position).
    edge_id_map: Option<FxHashMap<EdgeId, (u16, u64)>>,
    /// Reverse: table_id index -> vec of original `NodeId` per row offset.
    node_offset_to_id: Option<Vec<Vec<NodeId>>>,
    /// Reverse: rel_table_id index -> vec of original `EdgeId` per CSR position.
    edge_offset_to_id: Option<Vec<Vec<EdgeId>>>,
    /// Full container mapping retained for a direct mapped read-only reopen.
    ///
    /// This is an owner handle only: it does not copy the mapped payload and
    /// is intentionally excluded from [`Self::memory_bytes`]. The mapped
    /// graph-view work in G-EM0.2 replaces the remaining proportional decoded
    /// structures; retaining this handle guarantees no codec-free snapshot can
    /// accidentally unmap while readers still hold the CompactStore.
    mapped_backing: Option<Bytes>,
    /// Mapped sorted NodeId lookup (v5). Mutually exclusive with `node_id_map`.
    mapped_node_id_lookup: Option<mapped::MappedNodeIdLookup>,
    /// Mapped sorted EdgeId lookup (v5).
    mapped_edge_id_lookup: Option<mapped::MappedEdgeIdLookup>,
    /// Mapped reverse original node IDs (concatenated per table).
    mapped_node_original_ids: Option<Bytes>,
    /// Per-table base index into `mapped_node_original_ids`.
    mapped_node_original_bases: Option<Vec<usize>>,
    /// Mapped reverse original edge IDs (concatenated per rel table).
    mapped_edge_original_ids: Option<Bytes>,
    /// Per-rel-table base index into `mapped_edge_original_ids`.
    mapped_edge_original_bases: Option<Vec<usize>>,
    /// Split memory accounting for Milestone R evidence.
    memory_accounting: Option<mapped::CompactMemoryAccounting>,
}

impl std::fmt::Debug for CompactStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactStore")
            .field("node_tables_by_id", &self.node_tables_by_id)
            .field("rel_tables_by_id", &self.rel_tables_by_id)
            .field("table_id_to_label", &self.table_id_to_label)
            .field("rel_table_id_to_type", &self.rel_table_id_to_type)
            .finish_non_exhaustive()
    }
}

impl CompactStore {
    /// Creates a new `CompactStore` from pre-built components.
    ///
    /// Prefer using [`CompactStoreBuilder`] which validates schemas and
    /// computes statistics automatically. This constructor is `pub(crate)`
    /// because it assumes all invariants are already satisfied.
    #[must_use]
    pub(crate) fn new(
        node_tables_by_id: Vec<NodeTable>,
        label_to_table_id: FxHashMap<ArcStr, u16>,
        rel_tables_by_id: Vec<RelTable>,
        edge_type_to_rel_id: FxHashMap<ArcStr, Vec<u16>>,
        table_id_to_label: Vec<ArcStr>,
        rel_table_id_to_type: Vec<ArcStr>,
        statistics: Statistics,
    ) -> Self {
        // Pre-compute src/dst rel_table_id mappings per node table_id.
        let node_table_count = node_tables_by_id.len();
        let mut src_rel_table_ids = vec![Vec::new(); node_table_count];
        let mut dst_rel_table_ids = vec![Vec::new(); node_table_count];

        debug_assert!(
            rel_tables_by_id.len() <= usize::from(id::MAX_TABLE_ID) + 1,
            "rel table count {} exceeds 15-bit limit; caller must validate",
            rel_tables_by_id.len()
        );
        for (rel_idx, rt) in rel_tables_by_id.iter().enumerate() {
            // Caller (CompactStoreBuilder::build) validates table count fits u16.
            let rel_id = u16::try_from(rel_idx).expect("caller validated table count");

            let src_tid = rt.src_table_id() as usize;
            let dst_tid = rt.dst_table_id() as usize;
            if src_tid < node_table_count {
                src_rel_table_ids[src_tid].push(rel_id);
            }
            if dst_tid < node_table_count {
                dst_rel_table_ids[dst_tid].push(rel_id);
            }
        }

        Self {
            node_tables_by_id,
            label_to_table_id,
            rel_tables_by_id,
            edge_type_to_rel_id,
            table_id_to_label,
            rel_table_id_to_type,
            src_rel_table_ids,
            dst_rel_table_ids,
            statistics: Arc::new(statistics),
            node_id_map: None,
            edge_id_map: None,
            node_offset_to_id: None,
            edge_offset_to_id: None,
            mapped_backing: None,
            mapped_node_id_lookup: None,
            mapped_edge_id_lookup: None,
            mapped_node_original_ids: None,
            mapped_node_original_bases: None,
            mapped_edge_original_ids: None,
            mapped_edge_original_bases: None,
            memory_accounting: None,
        }
    }

    /// Retains the owner for a verified direct container mapping.
    ///
    /// The mapping is released when this `CompactStore` and every clone of the
    /// owner `Bytes` have been dropped. Callers must only pass bytes produced
    /// from a successfully validated immutable container section.
    pub fn retain_mapped_backing(&mut self, mapped_bytes: Bytes) {
        self.mapped_backing = Some(mapped_bytes);
    }

    /// Returns the direct-container mapping length when this store was opened
    /// from one, excluding it from anonymous heap accounting.
    #[must_use]
    pub fn mapped_backing_bytes(&self) -> Option<usize> {
        self.mapped_backing.as_ref().map(Bytes::len)
    }

    /// Resolves a table_id to its [`NodeTable`].
    #[inline]
    fn resolve_node_table(&self, table_id: u16) -> Option<&NodeTable> {
        self.node_tables_by_id.get(table_id as usize)
    }

    /// Resolves a rel_table_id to its [`RelTable`].
    #[inline]
    fn resolve_rel_table(&self, rel_table_id: u16) -> Option<&RelTable> {
        self.rel_tables_by_id.get(rel_table_id as usize)
    }

    /// Returns a reference to the node table for the given label, if any.
    #[must_use]
    pub fn node_table(&self, label: &str) -> Option<&NodeTable> {
        let &tid = self.label_to_table_id.get(label)?;
        self.node_tables_by_id.get(tid as usize)
    }

    /// Returns a reference to the first relationship table for the given edge type.
    ///
    /// When an edge type spans multiple label pairs, use [`Self::rel_tables_for_type`]
    /// to get all matching tables.
    #[must_use]
    pub fn rel_table(&self, edge_type: &str) -> Option<&RelTable> {
        let rids = self.edge_type_to_rel_id.get(edge_type)?;
        let &rid = rids.first()?;
        self.rel_tables_by_id.get(rid as usize)
    }

    /// Returns all relationship tables for the given edge type.
    #[must_use]
    pub fn rel_tables_for_type(&self, edge_type: &str) -> Vec<&RelTable> {
        self.edge_type_to_rel_id
            .get(edge_type)
            .map(|rids| {
                rids.iter()
                    .filter_map(|&rid| self.rel_tables_by_id.get(rid as usize))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns the label for a given table ID, if valid.
    #[must_use]
    pub fn label_for_table_id(&self, table_id: u16) -> Option<&ArcStr> {
        self.table_id_to_label.get(table_id as usize)
    }

    /// Returns the edge type for a given rel table ID, if valid.
    #[must_use]
    pub fn edge_type_for_rel_table_id(&self, rel_table_id: u16) -> Option<&ArcStr> {
        self.rel_table_id_to_type.get(rel_table_id as usize)
    }

    /// Collects edges from snapshot RelTables for a given node in a direction.
    ///
    /// When ID-preserving, the returned `NodeId`/`EdgeId` values are translated
    /// back to the original IDs from the source store.
    fn collect_edges(
        &self,
        node_table_id: u16,
        node_offset: u32,
        direction: Direction,
    ) -> Vec<(NodeId, EdgeId)> {
        let tid = node_table_id as usize;
        let mut results = Vec::new();

        if matches!(direction, Direction::Outgoing | Direction::Both)
            && let Some(rel_ids) = self.src_rel_table_ids.get(tid)
        {
            for &rel_id in rel_ids {
                let rt = &self.rel_tables_by_id[rel_id as usize];
                results.extend(rt.edges_from_source(node_offset));
            }
        }

        if matches!(direction, Direction::Incoming | Direction::Both)
            && let Some(rel_ids) = self.dst_rel_table_ids.get(tid)
        {
            for &rel_id in rel_ids {
                let rt = &self.rel_tables_by_id[rel_id as usize];
                if let Some(edges) = rt.edges_to_target(node_offset) {
                    results.extend(edges);
                }
            }
        }

        // Translate compact-encoded IDs to original IDs when preserving.
        if self.preserves_ids() {
            for (target_id, edge_id) in &mut results {
                *target_id = self.to_original_node_id(*target_id);
                *edge_id = self.to_original_edge_id(*edge_id);
            }
        }

        results
    }

    /// Returns the total logical node count across all node tables.
    #[must_use]
    pub fn total_nodes(&self) -> u64 {
        self.node_tables_by_id
            .iter()
            .map(NodeTable::len)
            .sum::<usize>() as u64
    }

    /// Returns the total logical edge count across all rel tables.
    #[must_use]
    pub fn total_edges(&self) -> u64 {
        self.rel_tables_by_id
            .iter()
            .map(RelTable::num_edges)
            .sum::<usize>() as u64
    }

    /// Returns a rough estimate of heap memory used by the snapshot data
    /// (node columns + CSR structures + edge property columns), in bytes.
    ///
    /// Does not include `FxHashMap` overhead or schema metadata. For precise
    /// measurement, use a heap profiler.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        let node_bytes: usize = self
            .node_tables_by_id
            .iter()
            .map(|nt| nt.memory_bytes())
            .sum();
        let rel_bytes: usize = self
            .rel_tables_by_id
            .iter()
            .map(|rt| rt.memory_bytes())
            .sum();
        let id_map_bytes = self.id_map_memory_bytes();
        node_bytes + rel_bytes + id_map_bytes
    }

    // ── ID-preserving accessors ────────────────────────────────────

    /// Returns `true` if original IDs are preserved (built via
    /// [`from_graph_store_preserving_ids`]).
    #[must_use]
    pub fn preserves_ids(&self) -> bool {
        self.node_id_map.is_some() || self.mapped_node_id_lookup.is_some()
    }

    /// Attaches ID maps to an already-built `CompactStore`.
    pub(crate) fn set_id_maps(
        &mut self,
        node_id_map: FxHashMap<NodeId, (u16, u64)>,
        edge_id_map: FxHashMap<EdgeId, (u16, u64)>,
        node_offset_to_id: Vec<Vec<NodeId>>,
        edge_offset_to_id: Vec<Vec<EdgeId>>,
    ) {
        self.node_id_map = Some(node_id_map);
        self.edge_id_map = Some(edge_id_map);
        self.node_offset_to_id = Some(node_offset_to_id);
        self.edge_offset_to_id = Some(edge_offset_to_id);
    }

    /// Resolves an input `NodeId` to (table_id, offset).
    ///
    /// When ID-preserving, looks up the original ID in the map.
    /// Otherwise, decodes the compact-encoded bits.
    #[inline]
    pub(crate) fn resolve_node(&self, id: NodeId) -> Option<(u16, u64)> {
        if let Some(ref map) = self.node_id_map {
            map.get(&id).copied()
        } else if let Some(ref lookup) = self.mapped_node_id_lookup {
            lookup.lookup(id)
        } else {
            Some(id::decode_node_id(id))
        }
    }

    /// Resolves an input `EdgeId` to (rel_table_id, csr_position).
    #[inline]
    pub(crate) fn resolve_edge(&self, id: EdgeId) -> Option<(u16, u64)> {
        if let Some(ref map) = self.edge_id_map {
            map.get(&id).copied()
        } else if let Some(ref lookup) = self.mapped_edge_id_lookup {
            lookup.lookup(id)
        } else {
            Some(id::decode_edge_id(id))
        }
    }

    /// Translates a compact-encoded `NodeId` (from internal CSR/table lookups)
    /// back to the original preserved ID. No-op when not ID-preserving.
    #[inline]
    pub(crate) fn to_original_node_id(&self, compact_id: NodeId) -> NodeId {
        if let Some(ref offsets) = self.node_offset_to_id {
            let (table_id, offset) = id::decode_node_id(compact_id);
            offsets
                .get(table_id as usize)
                .and_then(|v| v.get(usize::try_from(offset).ok()?))
                .copied()
                .unwrap_or(compact_id)
        } else if let (Some(bytes), Some(bases)) = (
            &self.mapped_node_original_ids,
            &self.mapped_node_original_bases,
        ) {
            let (table_id, offset) = id::decode_node_id(compact_id);
            let base = *bases.get(table_id as usize).unwrap_or(&0);
            let idx = base.saturating_add(usize::try_from(offset).unwrap_or(0));
            let start = idx.saturating_mul(8);
            if start + 8 <= bytes.len() {
                let raw = u64::from_le_bytes([
                    bytes[start],
                    bytes[start + 1],
                    bytes[start + 2],
                    bytes[start + 3],
                    bytes[start + 4],
                    bytes[start + 5],
                    bytes[start + 6],
                    bytes[start + 7],
                ]);
                NodeId::new(raw)
            } else {
                compact_id
            }
        } else {
            compact_id
        }
    }

    /// Translates a compact-encoded `EdgeId` back to the original preserved ID.
    #[inline]
    pub(crate) fn to_original_edge_id(&self, compact_id: EdgeId) -> EdgeId {
        if let Some(ref offsets) = self.edge_offset_to_id {
            let (rel_table_id, csr_pos) = id::decode_edge_id(compact_id);
            offsets
                .get(rel_table_id as usize)
                .and_then(|v| v.get(usize::try_from(csr_pos).ok()?))
                .copied()
                .unwrap_or(compact_id)
        } else if let (Some(bytes), Some(bases)) = (
            &self.mapped_edge_original_ids,
            &self.mapped_edge_original_bases,
        ) {
            let (rel_table_id, csr_pos) = id::decode_edge_id(compact_id);
            let base = *bases.get(rel_table_id as usize).unwrap_or(&0);
            let idx = base.saturating_add(usize::try_from(csr_pos).unwrap_or(0));
            let start = idx.saturating_mul(8);
            if start + 8 <= bytes.len() {
                let raw = u64::from_le_bytes([
                    bytes[start],
                    bytes[start + 1],
                    bytes[start + 2],
                    bytes[start + 3],
                    bytes[start + 4],
                    bytes[start + 5],
                    bytes[start + 6],
                    bytes[start + 7],
                ]);
                EdgeId::new(raw)
            } else {
                compact_id
            }
        } else {
            compact_id
        }
    }

    /// Installs mapped ID indexes from a v5 payload (G-EM0.2).
    pub(crate) fn set_mapped_id_indexes(
        &mut self,
        node_lookup: mapped::MappedNodeIdLookup,
        edge_lookup: mapped::MappedEdgeIdLookup,
        node_orig: Bytes,
        edge_orig: Bytes,
        meta_node_counts: &[usize],
        meta_edge_counts: &[usize],
    ) {
        let mut node_bases = Vec::with_capacity(meta_node_counts.len());
        let mut cursor = 0usize;
        for &c in meta_node_counts {
            node_bases.push(cursor);
            cursor = cursor.saturating_add(c);
        }
        let mut edge_bases = Vec::with_capacity(meta_edge_counts.len());
        cursor = 0;
        for &c in meta_edge_counts {
            edge_bases.push(cursor);
            cursor = cursor.saturating_add(c);
        }
        self.mapped_node_id_lookup = Some(node_lookup);
        self.mapped_edge_id_lookup = Some(edge_lookup);
        self.mapped_node_original_ids = Some(node_orig);
        self.mapped_node_original_bases = Some(node_bases);
        self.mapped_edge_original_ids = Some(edge_orig);
        self.mapped_edge_original_bases = Some(edge_bases);
        // Clear heap maps so accounting sees zero proportional anonymous.
        self.node_id_map = None;
        self.edge_id_map = None;
        self.node_offset_to_id = None;
        self.edge_offset_to_id = None;
    }

    /// Records split memory accounting for this store.
    pub(crate) fn set_memory_accounting(&mut self, accounting: mapped::CompactMemoryAccounting) {
        self.memory_accounting = Some(accounting);
    }

    /// Returns split memory accounting when recorded (mapped v5 open).
    #[must_use]
    pub fn memory_accounting(&self) -> Option<&mapped::CompactMemoryAccounting> {
        self.memory_accounting.as_ref()
    }

    /// Anonymous bytes from proportional structures (CSR heap, dict Arc, ID maps).
    ///
    /// Mapped-backed structures contribute zero. A successful v5 mapped open
    /// must report zero.
    #[must_use]
    pub fn proportional_anonymous_bytes(&self) -> usize {
        // Heap ID maps always count when present.
        let mut total = self.id_map_memory_bytes();

        // When opened from a retained container mapping, column codec bodies
        // and CSR arrays are views into that mapping. Charge only residual
        // heap dictionary Arc tables and inline CSR variants.
        let mapped_open = self.mapped_backing.is_some();
        for nt in &self.node_tables_by_id {
            for codec in nt.columns().values() {
                total = total.saturating_add(codec_proportional_heap(codec, mapped_open));
            }
        }
        for rt in &self.rel_tables_by_id {
            if !rt.fwd().is_mapped() {
                total = total.saturating_add(rt.fwd().memory_bytes());
            }
            if let Some(bwd) = rt.bwd()
                && !bwd.is_mapped()
            {
                total = total.saturating_add(bwd.memory_bytes());
            }
            for codec in rt.properties().values() {
                total = total.saturating_add(codec_proportional_heap(codec, mapped_open));
            }
        }
        total
    }

    /// Approximate heap cost of the ID maps.
    fn id_map_memory_bytes(&self) -> usize {
        // ~24 bytes per entry (key + value) in FxHashMap, plus Vec overhead.
        let node_map = self.node_id_map.as_ref().map_or(0, |m| m.len() * 24);
        let edge_map = self.edge_id_map.as_ref().map_or(0, |m| m.len() * 24);
        let node_rev = self
            .node_offset_to_id
            .as_ref()
            .map_or(0, |v| v.iter().map(|inner| inner.len() * 8).sum());
        let edge_rev = self
            .edge_offset_to_id
            .as_ref()
            .map_or(0, |v| v.iter().map(|inner| inner.len() * 8).sum());
        node_map + edge_map + node_rev + edge_rev
    }
}
