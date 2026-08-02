//! Bounded row-by-row source view for generation traversal (R1).
//!
//! Unlike `GraphStore`, this trait never returns collection-sized Vecs.
//! Each accessor reads exactly one row or returns fixed-size metadata.

use crate::graph::compact::generation::{GenerationEdge, GenerationError, GenerationNode};

/// Bounded table/row access for generation source traversal.
///
/// Implementors provide deterministic row-by-row access to node and edge
/// tables without materializing table-sized or adjacency-sized collections.
pub trait GenerationSourceView: Send + Sync {
    /// Number of physical node tables.
    fn node_table_count(&self) -> usize;
    /// Label for node table at `table_idx`.
    fn node_table_label(&self, table_idx: usize) -> &str;
    /// Number of rows in node table at `table_idx`.
    fn node_table_len(&self, table_idx: usize) -> usize;
    /// Read one node row. Returns `None` if `offset >= node_table_len(table_idx)`.
    ///
    /// # Errors
    /// Returns `GenerationError` on decode failure or if `max_record_bytes`
    /// would be exceeded by the node's labels + properties.
    fn node_row(
        &self,
        table_idx: usize,
        offset: usize,
        max_record_bytes: u64,
    ) -> Result<Option<GenerationNode>, GenerationError>;

    /// Number of physical edge/rel tables.
    fn edge_table_count(&self) -> usize;
    /// Edge type for rel table at `rel_idx`.
    fn edge_table_type(&self, rel_idx: usize) -> &str;
    /// Number of edges in rel table at `rel_idx`.
    fn edge_table_len(&self, rel_idx: usize) -> usize;
    /// Read one edge row. Returns `None` if `pos >= edge_table_len(rel_idx)`.
    ///
    /// # Errors
    /// Returns `GenerationError` on decode failure or if `max_record_bytes`
    /// would be exceeded.
    fn edge_row(
        &self,
        rel_idx: usize,
        pos: usize,
        max_record_bytes: u64,
    ) -> Result<Option<GenerationEdge>, GenerationError>;
}

// ── Shared reverse-ID translation helpers ─────────────────────────
//
// These mirror the logic in `freeze.rs` `BaseNodeCursor::original_node_id`
// and `BaseEdgeCursor::original_edge_id` / `original_node_id`. They are
// factored here so the `GenerationSourceView` impl can reuse them without
// duplicating the reverse-map walk.

use crate::graph::compact::CompactStore;
use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};

/// Resolve the original node id for a dense `(table_id, offset)` pair.
pub(crate) fn reverse_node_id(store: &CompactStore, table_id: u16, offset: u64) -> Option<u64> {
    // Preserve-id reverse map (heap-built base) first.
    if let Some(ref rev) = store.node_offset_to_id {
        return rev
            .get(table_id as usize)
            .and_then(|v| v.get(offset as usize))
            .map(NodeId::as_u64);
    }
    // Mapped reverse original-id array (deserialized v5 base).
    if let (Some(bytes), Some(bases)) = (
        store.mapped_node_original_ids.as_ref(),
        store.mapped_node_original_bases.as_ref(),
    ) {
        let base_off = *bases.get(table_id as usize)?;
        let idx = base_off + offset as usize;
        let start = idx.checked_mul(8)?;
        let end = start.checked_add(8)?;
        if end > bytes.len() {
            return None;
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[start..end]);
        return Some(u64::from_le_bytes(arr));
    }
    None
}

/// Resolve the original edge id for a dense `(rel_table_id, csr_pos)` pair.
pub(crate) fn reverse_edge_id(
    store: &CompactStore,
    rel_table_id: u16,
    csr_pos: u64,
) -> Option<u64> {
    if let Some(ref rev) = store.edge_offset_to_id {
        return rev
            .get(rel_table_id as usize)
            .and_then(|v| v.get(csr_pos as usize))
            .map(EdgeId::as_u64);
    }
    if let (Some(bytes), Some(bases)) = (
        store.mapped_edge_original_ids.as_ref(),
        store.mapped_edge_original_bases.as_ref(),
    ) {
        let base_off = *bases.get(rel_table_id as usize)?;
        let idx = base_off + csr_pos as usize;
        let start = idx.checked_mul(8)?;
        let end = start.checked_add(8)?;
        if end > bytes.len() {
            return None;
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[start..end]);
        return Some(u64::from_le_bytes(arr));
    }
    None
}

/// Resolve a compact `NodeId` endpoint to its original id.
pub(crate) fn reverse_endpoint_id(store: &CompactStore, compact: NodeId) -> Option<u64> {
    let (table_id, offset) = crate::graph::compact::id::decode_node_id(compact);
    reverse_node_id(store, table_id, offset as u64)
}

// ── Record-size estimation ────────────────────────────────────────

/// Upper-bound byte estimate for a property map (keys + values).
pub(crate) fn estimate_props_bytes<'a>(
    props: impl Iterator<Item = (&'a PropertyKey, &'a Value)>,
) -> usize {
    props
        .map(|(k, v)| {
            k.as_str().len()
                + match v {
                    Value::String(s) => s.len() + 5,
                    Value::Vector(vec) => vec.len() * 4 + 3,
                    _ => 9,
                }
        })
        .sum()
}

/// Upper-bound byte estimate for a node record (labels + properties).
pub(crate) fn estimate_node_bytes(labels: &[String], prop_bytes: usize) -> u64 {
    (labels.iter().map(|l| l.len()).sum::<usize>() + prop_bytes) as u64
}

// ── CompactStore impl ─────────────────────────────────────────────

use crate::graph::compact::generation::{OriginalEdgeId, OriginalNodeId};

impl GenerationSourceView for CompactStore {
    fn node_table_count(&self) -> usize {
        self.node_tables_by_id.len()
    }

    fn node_table_label(&self, table_idx: usize) -> &str {
        self.node_tables_by_id[table_idx].label()
    }

    fn node_table_len(&self, table_idx: usize) -> usize {
        self.node_tables_by_id[table_idx].len()
    }

    fn node_row(
        &self,
        table_idx: usize,
        offset: usize,
        max_record_bytes: u64,
    ) -> Result<Option<GenerationNode>, GenerationError> {
        let nt = self.node_tables_by_id.get(table_idx).ok_or_else(|| {
            GenerationError::InvalidInput(format!("node table_idx {table_idx} OOB"))
        })?;
        if offset >= nt.len() {
            return Ok(None);
        }
        let table_id = nt.table_id();
        let original_id = reverse_node_id(self, table_id, offset as u64).ok_or_else(|| {
            GenerationError::Codec(format!(
                "base node table {table_id} offset {offset} has no original id"
            ))
        })?;
        let properties = nt.get_all_properties(offset);
        let labels = vec![nt.label().to_string()];
        let prop_bytes = estimate_props_bytes(properties.iter());
        let estimated = estimate_node_bytes(&labels, prop_bytes);
        if estimated > max_record_bytes {
            return Err(GenerationError::BudgetExceeded {
                counter: "max_record_bytes",
                requested: estimated,
                limit: max_record_bytes,
            });
        }
        Ok(Some(GenerationNode {
            id: OriginalNodeId::new(original_id),
            labels,
            properties,
        }))
    }

    fn edge_table_count(&self) -> usize {
        self.rel_tables_by_id.len()
    }

    fn edge_table_type(&self, rel_idx: usize) -> &str {
        self.rel_tables_by_id[rel_idx].edge_type()
    }

    fn edge_table_len(&self, rel_idx: usize) -> usize {
        self.rel_tables_by_id[rel_idx].num_edges()
    }

    fn edge_row(
        &self,
        rel_idx: usize,
        pos: usize,
        max_record_bytes: u64,
    ) -> Result<Option<GenerationEdge>, GenerationError> {
        let rt = self.rel_tables_by_id.get(rel_idx).ok_or_else(|| {
            GenerationError::InvalidInput(format!("rel_idx {rel_idx} OOB"))
        })?;
        if pos >= rt.num_edges() {
            return Ok(None);
        }
        let rel_table_id = rt.rel_table_id();
        let original_id = reverse_edge_id(self, rel_table_id, pos as u64).ok_or_else(|| {
            GenerationError::Codec(format!(
                "base rel table {rel_table_id} pos {pos} has no original id"
            ))
        })?;
        let pos_u32 = pos as u32;
        let src_compact = rt.source_node_id(pos_u32).ok_or_else(|| {
            GenerationError::Codec(format!("rel {rel_table_id} pos {pos} missing src"))
        })?;
        let dst_compact = rt.dest_node_id(pos_u32).ok_or_else(|| {
            GenerationError::Codec(format!("rel {rel_table_id} pos {pos} missing dst"))
        })?;
        let src = reverse_endpoint_id(self, src_compact).ok_or_else(|| {
            GenerationError::Codec(format!(
                "rel {rel_table_id} pos {pos} src has no original id"
            ))
        })?;
        let dst = reverse_endpoint_id(self, dst_compact).ok_or_else(|| {
            GenerationError::Codec(format!(
                "rel {rel_table_id} pos {pos} dst has no original id"
            ))
        })?;
        let properties = rt.get_all_edge_properties(pos);
        let edge_type = rt.edge_type().to_string();
        let prop_bytes = estimate_props_bytes(properties.iter());
        let estimated = (edge_type.len() + prop_bytes) as u64;
        if estimated > max_record_bytes {
            return Err(GenerationError::BudgetExceeded {
                counter: "max_record_bytes",
                requested: estimated,
                limit: max_record_bytes,
            });
        }
        Ok(Some(GenerationEdge {
            id: OriginalEdgeId::new(original_id),
            src: OriginalNodeId::new(src),
            dst: OriginalNodeId::new(dst),
            edge_type,
            properties,
        }))
    }
}
