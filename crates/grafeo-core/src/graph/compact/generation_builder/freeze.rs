//! Frozen overlay epoch + merged base/overlay record sources
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
//! (G-EM0.5b Phase 2).
//!
//! A generation build must freeze **one** accepted overlay epoch before it
//! streams, then present the merged logical graph (immutable base minus
//! tombstoned/overwritten rows, plus the frozen overlay's created/updated rows)
//! as bounded [`NodeRecordSource`]/[`EdgeRecordSource`]s.
//!
//! ## Ordering contract (LWW-by-epoch)
//!
//! Base rows (epoch < E, skipping ids present in the overlay dirty set or the
//! base-deletion tombstone set) stream first, then overlay rows (epoch E).
//! Because the base is immutable and only the overlay sets need an atomic
//! capture, capturing `E = current_epoch()` **before** any streaming — and
//! snapshotting the dirty/deleted id sets under that capture — gives a
//! consistent last-writer-wins merge without `merge_overlay_in_place` and
//! without reconstructing a heap-backed base.
//!
//! ## Boundedness
//!
//! The base cursor walks CompactStore tables row-by-row through the public
//! per-row accessors (`NodeTable::get_all_properties`, `RelTable::*_node_id`,
//! `get_all_edge_properties`) and translates dense offsets back to original
//! ids through the preserve-id maps. It never materializes all base rows. The
//! overlay is bounded by the admission budget, so its id snapshot is charged to
//! that budget rather than the base size.

use crate::graph::compact::CompactStore;
use crate::graph::compact::generation::GenerationError;
use crate::graph::compact::generation::{
    EdgeRecordSource, GenerationEdge, GenerationNode, NodeRecordSource, OriginalEdgeId,
    OriginalNodeId,
};
use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};
use grafeo_common::utils::hash::FxHashSet;
use std::sync::Arc;

/// A consistent point-in-time capture of the overlay mutation sets for one
/// generation build.
///
/// Constructed **before** streaming begins. The `epoch` is the engine's
/// current epoch at capture time; the id sets are the overlay's dirty
/// (created + modified) and base-deletion tombstone sets at that instant.
#[derive(Debug, Clone)]
pub struct FrozenOverlayEpoch {
    /// Captured overlay epoch (engine `TransactionManager::current_epoch`).
    pub epoch: u64,
    /// Original node ids created or modified in the overlay (skip in base scan).
    pub overlay_node_ids: FxHashSet<u64>,
    /// Original edge ids created or modified in the overlay (skip in base scan).
    pub overlay_edge_ids: FxHashSet<u64>,
    /// Original node ids deleted from the base (tombstones; skip in base scan).
    pub deleted_base_node_ids: FxHashSet<u64>,
    /// Original edge ids deleted from the base (tombstones; skip in base scan).
    pub deleted_base_edge_ids: FxHashSet<u64>,
}

impl FrozenOverlayEpoch {
    /// An empty freeze (epoch 0, no overlay, no tombstones) for base-only
    /// builds and fixtures.
    #[must_use]
    pub fn base_only() -> Self {
        Self {
            epoch: 0,
            overlay_node_ids: FxHashSet::default(),
            overlay_edge_ids: FxHashSet::default(),
            deleted_base_node_ids: FxHashSet::default(),
            deleted_base_edge_ids: FxHashSet::default(),
        }
    }

    /// True when a base node row with this original id is shadowed by the
    /// frozen overlay (created/updated) or tombstoned (deleted).
    #[must_use]
    pub fn node_shadowed(&self, original_id: u64) -> bool {
        self.overlay_node_ids.contains(&original_id)
            || self.deleted_base_node_ids.contains(&original_id)
    }

    /// True when a base edge row with this original id is shadowed or deleted.
    #[must_use]
    pub fn edge_shadowed(&self, original_id: u64) -> bool {
        self.overlay_edge_ids.contains(&original_id)
            || self.deleted_base_edge_ids.contains(&original_id)
    }
}

/// Streams base CompactStore rows that survive the frozen overlay, translating
/// dense offsets to original ids and skipping shadowed/tombstoned rows.
///
/// Holds an `Arc<CompactStore>` so the base stays mapped/shared; rows are read
/// one at a time through the public per-row accessors.
pub struct BaseNodeCursor {
    base: Arc<CompactStore>,
    freeze: FrozenOverlayEpoch,
    table: usize,
    offset: usize,
}

impl BaseNodeCursor {
    /// Creates a cursor over `base` honoring `freeze`.
    #[must_use]
    pub fn new(base: Arc<CompactStore>, freeze: FrozenOverlayEpoch) -> Self {
        Self {
            base,
            freeze,
            table: 0,
            offset: 0,
        }
    }

    fn original_node_id(&self, table_id: u16, offset: u64) -> Option<u64> {
        // Preserve-id reverse map (heap-built base) first.
        if let Some(ref rev) = self.base.node_offset_to_id {
            return rev
                .get(table_id as usize)
                .and_then(|v| v.get(offset as usize))
                .map(NodeId::as_u64);
        }
        // Mapped reverse original-id array (deserialized v5 base).
        if let (Some(bytes), Some(bases)) = (
            self.base.mapped_node_original_ids.as_ref(),
            self.base.mapped_node_original_bases.as_ref(),
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
}

impl NodeRecordSource for BaseNodeCursor {
    fn next_node(&mut self) -> Result<Option<GenerationNode>, GenerationError> {
        while self.table < self.base.node_tables_by_id.len() {
            let nt = &self.base.node_tables_by_id[self.table];
            if self.offset >= nt.len() {
                self.table += 1;
                self.offset = 0;
                continue;
            }
            let off = self.offset;
            self.offset += 1;
            let table_id = nt.table_id();
            let Some(original_id) = self.original_node_id(table_id, off as u64) else {
                return Err(GenerationError::Codec(format!(
                    "base node table {table_id} offset {off} has no original id"
                )));
            };
            if self.freeze.node_shadowed(original_id) {
                continue;
            }
            let properties = nt.get_all_properties(off);
            return Ok(Some(GenerationNode {
                id: OriginalNodeId::new(original_id),
                labels: vec![nt.label().to_string()],
                properties,
            }));
        }
        Ok(None)
    }
}

/// Streams base CompactStore edge rows surviving the frozen overlay.
pub struct BaseEdgeCursor {
    base: Arc<CompactStore>,
    freeze: FrozenOverlayEpoch,
    rel: usize,
    pos: usize,
}

impl BaseEdgeCursor {
    /// Creates a cursor over `base` honoring `freeze`.
    #[must_use]
    pub fn new(base: Arc<CompactStore>, freeze: FrozenOverlayEpoch) -> Self {
        Self {
            base,
            freeze,
            rel: 0,
            pos: 0,
        }
    }

    fn original_edge_id(&self, rel_table_id: u16, csr_pos: u64) -> Option<u64> {
        if let Some(ref rev) = self.base.edge_offset_to_id {
            return rev
                .get(rel_table_id as usize)
                .and_then(|v| v.get(csr_pos as usize))
                .map(EdgeId::as_u64);
        }
        if let (Some(bytes), Some(bases)) = (
            self.base.mapped_edge_original_ids.as_ref(),
            self.base.mapped_edge_original_bases.as_ref(),
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

    fn original_node_id(&self, compact: NodeId) -> Option<u64> {
        // Reuse the node reverse translation for endpoint ids.
        let (table_id, offset) = crate::graph::compact::id::decode_node_id(compact);
        if let Some(ref rev) = self.base.node_offset_to_id {
            return rev
                .get(table_id as usize)
                .and_then(|v| v.get(offset as usize))
                .map(NodeId::as_u64);
        }
        if let (Some(bytes), Some(bases)) = (
            self.base.mapped_node_original_ids.as_ref(),
            self.base.mapped_node_original_bases.as_ref(),
        ) {
            let base_off = *bases.get(table_id as usize)?;
            let idx = base_off + offset as usize;
            let start = idx.checked_mul(8)?;
            let end = start.checked_add(8)?;
            if end <= bytes.len() {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[start..end]);
                return Some(u64::from_le_bytes(arr));
            }
        }
        None
    }
}

impl EdgeRecordSource for BaseEdgeCursor {
    fn next_edge(&mut self) -> Result<Option<GenerationEdge>, GenerationError> {
        while self.rel < self.base.rel_tables_by_id.len() {
            let rt = &self.base.rel_tables_by_id[self.rel];
            if self.pos >= rt.num_edges() {
                self.rel += 1;
                self.pos = 0;
                continue;
            }
            let pos = self.pos;
            self.pos += 1;
            let rel_table_id = rt.rel_table_id();
            let Some(original_id) = self.original_edge_id(rel_table_id, pos as u64) else {
                return Err(GenerationError::Codec(format!(
                    "base rel table {rel_table_id} pos {pos} has no original id"
                )));
            };
            if self.freeze.edge_shadowed(original_id) {
                continue;
            }
            let pos_u32 = pos as u32;
            let src_compact = rt.source_node_id(pos_u32).ok_or_else(|| {
                GenerationError::Codec(format!("rel {rel_table_id} pos {pos} missing src"))
            })?;
            let dst_compact = rt.dest_node_id(pos_u32).ok_or_else(|| {
                GenerationError::Codec(format!("rel {rel_table_id} pos {pos} missing dst"))
            })?;
            let src = self.original_node_id(src_compact).ok_or_else(|| {
                GenerationError::Codec(format!(
                    "rel {rel_table_id} pos {pos} src has no original id"
                ))
            })?;
            let dst = self.original_node_id(dst_compact).ok_or_else(|| {
                GenerationError::Codec(format!(
                    "rel {rel_table_id} pos {pos} dst has no original id"
                ))
            })?;
            let properties = rt.get_all_edge_properties(pos);
            return Ok(Some(GenerationEdge {
                id: OriginalEdgeId::new(original_id),
                src: OriginalNodeId::new(src),
                dst: OriginalNodeId::new(dst),
                edge_type: rt.edge_type().to_string(),
                properties,
            }));
        }
        Ok(None)
    }
}

/// Chains a base cursor with an overlay record source so the merged stream is
/// base-first then overlay (LWW-by-epoch ordering).
pub struct MergedNodeSource<B: NodeRecordSource, O: NodeRecordSource> {
    base: B,
    overlay: O,
    base_done: bool,
}

impl<B: NodeRecordSource, O: NodeRecordSource> MergedNodeSource<B, O> {
    /// Creates a merged source (base first, then overlay).
    #[must_use]
    pub fn new(base: B, overlay: O) -> Self {
        Self {
            base,
            overlay,
            base_done: false,
        }
    }
}

impl<B: NodeRecordSource, O: NodeRecordSource> NodeRecordSource for MergedNodeSource<B, O> {
    fn next_node(&mut self) -> Result<Option<GenerationNode>, GenerationError> {
        if !self.base_done {
            if let Some(n) = self.base.next_node()? {
                return Ok(Some(n));
            }
            self.base_done = true;
        }
        self.overlay.next_node()
    }
}

/// Chains a base edge cursor with an overlay edge source.
pub struct MergedEdgeSource<B: EdgeRecordSource, O: EdgeRecordSource> {
    base: B,
    overlay: O,
    base_done: bool,
}

impl<B: EdgeRecordSource, O: EdgeRecordSource> MergedEdgeSource<B, O> {
    /// Creates a merged source (base first, then overlay).
    #[must_use]
    pub fn new(base: B, overlay: O) -> Self {
        Self {
            base,
            overlay,
            base_done: false,
        }
    }
}

impl<B: EdgeRecordSource, O: EdgeRecordSource> EdgeRecordSource for MergedEdgeSource<B, O> {
    fn next_edge(&mut self) -> Result<Option<GenerationEdge>, GenerationError> {
        if !self.base_done {
            if let Some(e) = self.base.next_edge()? {
                return Ok(Some(e));
            }
            self.base_done = true;
        }
        self.overlay.next_edge()
    }
}

/// An empty node source (no overlay rows).
#[derive(Debug, Clone, Copy, Default)]
pub struct EmptyNodeSource;

impl NodeRecordSource for EmptyNodeSource {
    fn next_node(&mut self) -> Result<Option<GenerationNode>, GenerationError> {
        Ok(None)
    }
}

/// An empty edge source (no overlay rows).
#[derive(Debug, Clone, Copy, Default)]
pub struct EmptyEdgeSource;

impl EdgeRecordSource for EmptyEdgeSource {
    fn next_edge(&mut self) -> Result<Option<GenerationEdge>, GenerationError> {
        Ok(None)
    }
}

/// A property-map helper used by overlay adapters: clones a borrowed map into
/// an owned generation property map.
#[must_use]
pub fn clone_properties(
    src: impl Iterator<Item = (PropertyKey, Value)>,
) -> grafeo_common::utils::hash::FxHashMap<PropertyKey, Value> {
    src.collect()
}
