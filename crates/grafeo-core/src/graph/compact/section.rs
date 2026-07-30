//! [`Section`](grafeo_common::storage::section::Section) implementation for [`CompactStore`].
//!
//! Serializes/deserializes a CompactStore to/from the `.grafeo` container
//! format with versioned headers and CRC32 integrity.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use grafeo_common::storage::section::{Section, SectionType};
use grafeo_common::types::{EdgeId, NodeId, PropertyKey};
use grafeo_common::utils::hash::FxHashMap;
use parking_lot::RwLock;

use super::CompactStore;
use super::column::ColumnCodec;
use super::csr::CsrAdjacency;
use super::node_table::NodeTable;
use super::rel_table::RelTable;
use super::schema::{ColumnDef, ColumnType, EdgeSchema, TableSchema};
use super::zone_map::ZoneMap;
use crate::statistics::{EdgeTypeStatistics, LabelStatistics, Statistics};

/// Magic bytes identifying a CompactStore section.
const MAGIC: [u8; 4] = *b"GCST";

/// Current section format version. E-0 bumped this from 3 to 4 to encode
/// section-level strings with `u32` lengths (production-sized property
/// zone-map min/max values).
const FORMAT_VERSION: u8 = 4;

/// v3 (Phase 2c) layout: per-block zone maps in the column index for
/// skip pruning; section-level strings still use `u16` lengths.
/// Retained as a read-only compat path.
const FORMAT_VERSION_V3: u8 = 3;

/// v2 (Phase 2b) layout: per-block index + bodies, no per-block stats.
/// Retained as a read-only compat path for one release.
const FORMAT_VERSION_V2: u8 = 2;

/// v1 layout: flat columns, no blocks. Retained as a read-only compat
/// path for one release. Files written by 0.5.41 and earlier carry
/// this byte; current writers always emit [`FORMAT_VERSION`].
const FORMAT_VERSION_V1: u8 = 1;

/// Wraps a [`CompactStore`] as a container [`Section`].
pub struct CompactStoreSection {
    store: RwLock<Option<Arc<CompactStore>>>,
    dirty: AtomicBool,
}

impl CompactStoreSection {
    /// Creates a new section wrapping an existing store.
    #[must_use]
    pub fn new(store: Arc<CompactStore>) -> Self {
        Self {
            store: RwLock::new(Some(store)),
            dirty: AtomicBool::new(false),
        }
    }

    /// Creates an empty section (for deserialization).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            store: RwLock::new(None),
            dirty: AtomicBool::new(false),
        }
    }

    /// Marks this section as dirty.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    /// Returns a reference to the inner store, if any.
    #[must_use]
    pub fn store(&self) -> Option<Arc<CompactStore>> {
        self.store.read().clone()
    }

    /// Deserializes from a refcounted [`Bytes`] buffer (Phase 3c).
    ///
    /// This is the zero-copy entry point: when `data` wraps a mmap
    /// region (via [`bytes::Bytes::from_owner`]), column codec storage
    /// is constructed via `data.slice(range)` rather than copying. The
    /// trait [`Section::deserialize`] entry point still works on
    /// `&[u8]` and incurs one heap copy (a single `Bytes::copy_from_slice`
    /// at the boundary).
    ///
    /// # Errors
    ///
    /// Same error semantics as [`Section::deserialize`].
    pub fn deserialize_from_bytes(
        &mut self,
        data: bytes::Bytes,
    ) -> grafeo_common::utils::error::Result<()> {
        let store = deserialize_compact_store(&data).map_err(|e| {
            grafeo_common::utils::error::Error::Internal(format!(
                "CompactStore deserialization failed: {e}"
            ))
        })?;
        *self.store.write() = Some(Arc::new(store));
        Ok(())
    }

    /// Deserializes from a verified direct container mapping.
    ///
    /// Unlike [`Self::deserialize_from_bytes`], this retains one owner handle
    /// on the resulting [`CompactStore`] so a mapping survives even when a
    /// particular snapshot happens not to contain a column codec slice.
    /// `data` must originate from the checked `GrafeoFileManager::mmap_section`
    /// path; callers cannot use this to bypass container CRC validation.
    ///
    /// # Errors
    ///
    /// Returns an error when the mapped payload is truncated, CRC-invalid at
    /// the CompactStore layer, or otherwise fails the standard v1–v4 codec
    /// reader. Failures do not expose unchecked slices to the caller.
    pub fn deserialize_from_mapped_bytes(
        &mut self,
        data: bytes::Bytes,
    ) -> grafeo_common::utils::error::Result<()> {
        let mut store = deserialize_compact_store(&data).map_err(|e| {
            grafeo_common::utils::error::Error::Internal(format!(
                "CompactStore deserialization failed: {e}"
            ))
        })?;
        store.retain_mapped_backing(data);
        *self.store.write() = Some(Arc::new(store));
        Ok(())
    }

    /// Serializes at the requested format version.
    ///
    /// The default [`Section::serialize`] always writes [`FORMAT_VERSION`].
    /// This crate-private entry point supports compatibility tests that need
    /// deterministic legacy payloads. Immutable exact-pin v1-v3 fixtures
    /// separately guard the historical on-disk bytes.
    pub(crate) fn serialize_with_version(
        &self,
        version: u8,
    ) -> grafeo_common::utils::error::Result<Vec<u8>> {
        match version {
            FORMAT_VERSION_V1 | FORMAT_VERSION_V2 | FORMAT_VERSION_V3 | FORMAT_VERSION => {}
            other => {
                return Err(grafeo_common::utils::error::Error::Serialization(format!(
                    "unsupported CompactStore section version {other} (supported: {FORMAT_VERSION_V1}, {FORMAT_VERSION_V2}, {FORMAT_VERSION_V3}, {FORMAT_VERSION})"
                )));
            }
        }

        let guard = self.store.read();
        let store = guard.as_ref().ok_or_else(|| {
            grafeo_common::utils::error::Error::Internal("no CompactStore to serialize".into())
        })?;

        let mut buf = Vec::with_capacity(store.memory_bytes());

        // Header.
        buf.extend_from_slice(&MAGIC);
        buf.push(version);
        let flags: u8 = u8::from(store.preserves_ids());
        buf.push(flags);

        // Node tables.
        write_len(&mut buf, store.node_tables_by_id.len());
        for nt in &store.node_tables_by_id {
            write_string(&mut buf, nt.label(), version)?;
            write_len(&mut buf, nt.len());
            let columns = nt.columns();
            let zone_maps = nt.zone_maps();
            write_len(&mut buf, columns.len());
            for (key, codec) in columns {
                write_string(&mut buf, key.as_str(), version)?;
                // Zone map for this column.
                if let Some(zm) = zone_maps.get(key) {
                    buf.push(1);
                    write_zone_map(&mut buf, zm, version, Some((nt.label(), key.as_str())))?;
                } else {
                    buf.push(0);
                }
                write_codec(
                    codec,
                    &mut buf,
                    version,
                    nt.block_zone_maps().get(key).map(Vec::as_slice),
                )?;
            }
        }

        // Relationship tables.
        write_len(&mut buf, store.rel_tables_by_id.len());
        for rt in &store.rel_tables_by_id {
            write_string(&mut buf, rt.edge_type().as_str(), version)?;
            write_u16(&mut buf, rt.src_table_id());
            write_u16(&mut buf, rt.dst_table_id());
            rt.fwd().write_to(&mut buf);
            if let Some(bwd) = rt.bwd() {
                buf.push(1);
                bwd.write_to(&mut buf);
            } else {
                buf.push(0);
            }
            let properties = rt.properties();
            write_len(&mut buf, properties.len());
            for (key, codec) in properties {
                write_string(&mut buf, key.as_str(), version)?;
                // Edge property columns don't track per-block zone maps
                // yet; v3 will compute them inline during write.
                write_codec(codec, &mut buf, version, None)?;
            }
        }
        // Continue building buf in `serialize()` epilogue.
        Ok(self.append_id_maps_and_crc(buf, store, version))
    }

    /// Appends ID maps (if applicable) and trailing CRC to the buffer.
    fn append_id_maps_and_crc(
        &self,
        mut buf: Vec<u8>,
        store: &CompactStore,
        _version: u8,
    ) -> Vec<u8> {
        // ID maps.
        if store.preserves_ids() {
            if let Some(ref node_map) = store.node_id_map {
                write_len(&mut buf, node_map.len());
                for (&nid, &(tid, off)) in node_map {
                    write_u64(&mut buf, nid.as_u64());
                    write_u16(&mut buf, tid);
                    write_u64(&mut buf, off);
                }
            }
            if let Some(ref edge_map) = store.edge_id_map {
                write_len(&mut buf, edge_map.len());
                for (&eid, &(rtid, pos)) in edge_map {
                    write_u64(&mut buf, eid.as_u64());
                    write_u16(&mut buf, rtid);
                    write_u64(&mut buf, pos);
                }
            }
        }

        // CRC32 at end.
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }
}

/// Writes a single column codec body using the layout matching the
/// section's format version.
///
/// - v1 = flat columns (legacy)
/// - v2 = per-block index + concatenated bodies, no stats
/// - v3/v4 = v2 layout + inline per-block zone map per index entry
///
/// `block_stats_hint` is consulted only at v3/v4; when `None` or with a
/// mismatched length, [`ColumnCodec::write_to_v3`] computes the stats
/// from the column itself.
fn write_codec(
    codec: &ColumnCodec,
    buf: &mut Vec<u8>,
    version: u8,
    block_stats_hint: Option<&[ZoneMap]>,
) -> grafeo_common::utils::error::Result<()> {
    match version {
        FORMAT_VERSION_V1 => codec.write_to(buf),
        FORMAT_VERSION_V2 => codec.write_to_v2(buf),
        FORMAT_VERSION_V3 | FORMAT_VERSION => codec.write_to_v3(buf, block_stats_hint),
        other => {
            return Err(grafeo_common::utils::error::Error::Serialization(format!(
                "unsupported CompactStore section version {other} for column codec write"
            )));
        }
    }
    Ok(())
}

impl Section for CompactStoreSection {
    fn section_type(&self) -> SectionType {
        SectionType::CompactStore
    }

    fn version(&self) -> u8 {
        FORMAT_VERSION
    }

    fn serialize(&self) -> grafeo_common::utils::error::Result<Vec<u8>> {
        self.serialize_with_version(FORMAT_VERSION)
    }

    fn deserialize(&mut self, data: &[u8]) -> grafeo_common::utils::error::Result<()> {
        // Heap-copy entry point (Section trait). Phase 3c adds
        // [`deserialize_from_bytes`](Self::deserialize_from_bytes) which
        // skips the copy on the mmap path.
        let owned = bytes::Bytes::copy_from_slice(data);
        self.deserialize_from_bytes(owned)
    }

    fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    fn memory_usage(&self) -> usize {
        self.store.read().as_ref().map_or(0, |s| s.memory_bytes())
    }
}

// ── Deserialization ────────────────────────────────────────────────

/// Reads a single column codec body, dispatching by section version.
///
/// - v1 → [`ColumnCodec::read_from`] (flat layout, no per-block stats)
/// - v2 → [`ColumnCodec::read_from_v2`] (block index, no stats)
/// - v3/v4 → [`ColumnCodec::read_from_v3`] (block index + per-block stats)
///
/// Returns the codec and an `Option<Vec<ZoneMap>>` carrying per-block
/// stats when the v3/v4 path was taken.
fn read_codec(
    data: &Bytes,
    pos: &mut usize,
    version: u8,
) -> Result<(ColumnCodec, Option<Vec<ZoneMap>>), String> {
    match version {
        FORMAT_VERSION_V1 => ColumnCodec::read_from(data, pos)
            .map(|c| (c, None))
            .map_err(|e| e.to_string()),
        FORMAT_VERSION_V2 => ColumnCodec::read_from_v2(data, pos)
            .map(|c| (c, None))
            .map_err(|e| e.to_string()),
        FORMAT_VERSION_V3 | FORMAT_VERSION => ColumnCodec::read_from_v3(data, pos)
            .map(|(c, stats)| (c, Some(stats)))
            .map_err(|e| e.to_string()),
        _ => Err(format!("unsupported CompactStore version {version}")),
    }
}

fn deserialize_compact_store(data_bytes: &bytes::Bytes) -> Result<CompactStore, String> {
    let data: &[u8] = data_bytes.as_ref();
    if data.len() < 10 {
        return Err("data too short for CompactStore section".into());
    }

    // Verify CRC32.
    let payload = &data[..data.len() - 4];
    let stored_crc = u32::from_le_bytes([
        data[data.len() - 4],
        data[data.len() - 3],
        data[data.len() - 2],
        data[data.len() - 1],
    ]);
    let computed_crc = crc32fast::hash(payload);
    if stored_crc != computed_crc {
        return Err(format!(
            "CRC32 mismatch: stored {stored_crc:#010X}, computed {computed_crc:#010X}"
        ));
    }

    let mut pos = 0;

    // Header.
    if data[pos..pos + 4] != MAGIC {
        return Err("bad magic".into());
    }
    pos += 4;
    let version = data[pos];
    pos += 1;
    if version != FORMAT_VERSION
        && version != FORMAT_VERSION_V3
        && version != FORMAT_VERSION_V2
        && version != FORMAT_VERSION_V1
    {
        return Err(format!(
            "unsupported CompactStore section version {version} (supported: {FORMAT_VERSION_V1}, {FORMAT_VERSION_V2}, {FORMAT_VERSION_V3}, {FORMAT_VERSION})"
        ));
    }
    let flags = data[pos];
    pos += 1;
    let preserves_ids = flags & 0x01 != 0;

    // Node tables.
    let num_node_tables = read_u32(data, &mut pos)? as usize;
    if num_node_tables > usize::from(u16::MAX) {
        return Err(format!(
            "node_tables count {num_node_tables} exceeds u16::MAX"
        ));
    }
    checked_count_for_alloc(
        num_node_tables,
        pos,
        data.len(),
        min_node_table_wire_bytes(version),
        "node_tables",
    )?;
    let mut node_tables = Vec::with_capacity(num_node_tables);
    let mut label_to_table_id: FxHashMap<arcstr::ArcStr, u16> = FxHashMap::default();
    let mut table_id_to_label: Vec<arcstr::ArcStr> = Vec::with_capacity(num_node_tables);

    for table_idx in 0..num_node_tables {
        let table_id = u16::try_from(table_idx).unwrap_or(0);
        let label = read_string(data, &mut pos, version)?;
        let label = arcstr::ArcStr::from(label.as_str());
        let row_count = read_u32(data, &mut pos)? as usize;
        let num_cols = read_u32(data, &mut pos)? as usize;
        checked_count_for_alloc(
            num_cols,
            pos,
            data.len(),
            min_column_entry_wire_bytes(version),
            "node_table columns",
        )?;

        let mut columns: FxHashMap<PropertyKey, ColumnCodec> = FxHashMap::default();
        let mut zone_maps: FxHashMap<PropertyKey, ZoneMap> = FxHashMap::default();
        let mut block_zone_maps: FxHashMap<PropertyKey, Vec<ZoneMap>> = FxHashMap::default();
        let mut col_defs = Vec::with_capacity(num_cols);

        for _ in 0..num_cols {
            let key_str = read_string(data, &mut pos, version)?;
            let key = PropertyKey::new(&key_str);

            let has_zm = *data.get(pos).ok_or("truncated zone map flag")?;
            pos += 1;
            if has_zm == 1 {
                let zm = read_zone_map(data, &mut pos, version)?;
                zone_maps.insert(key.clone(), zm);
            }

            let (codec, maybe_block_stats) =
                read_codec(data_bytes, &mut pos, version).map_err(|e| format!("codec: {e}"))?;
            if let Some(stats) = maybe_block_stats {
                block_zone_maps.insert(key.clone(), stats);
            }
            let col_type = infer_column_type_from_codec(&codec);
            col_defs.push(ColumnDef::new(&key_str, col_type));
            columns.insert(key, codec);
        }

        let schema = TableSchema::new(label.as_str(), table_id, col_defs);
        let table = NodeTable::from_columns_with_block_stats(
            schema,
            columns,
            zone_maps,
            block_zone_maps,
            row_count,
        );
        node_tables.push(table);
        label_to_table_id.insert(label.clone(), table_id);
        table_id_to_label.push(label);
    }

    // Relationship tables.
    let num_rel_tables = read_u32(data, &mut pos)? as usize;
    if num_rel_tables > usize::from(u16::MAX) {
        return Err(format!(
            "rel_tables count {num_rel_tables} exceeds u16::MAX"
        ));
    }
    checked_count_for_alloc(
        num_rel_tables,
        pos,
        data.len(),
        min_rel_table_wire_bytes(version),
        "rel_tables",
    )?;
    let mut rel_tables = Vec::with_capacity(num_rel_tables);
    let mut edge_type_to_rel_id: FxHashMap<arcstr::ArcStr, Vec<u16>> = FxHashMap::default();
    let mut rel_table_id_to_type: Vec<arcstr::ArcStr> = Vec::with_capacity(num_rel_tables);

    for rel_idx in 0..num_rel_tables {
        let rel_table_id = u16::try_from(rel_idx).unwrap_or(0);
        let edge_type = read_string(data, &mut pos, version)?;
        let edge_type = arcstr::ArcStr::from(edge_type.as_str());
        let src_tid = read_u16(data, &mut pos)?;
        let dst_tid = read_u16(data, &mut pos)?;

        let fwd = CsrAdjacency::read_from(data, &mut pos).map_err(|e| format!("fwd CSR: {e}"))?;

        let has_bwd = *data.get(pos).ok_or("truncated bwd flag")?;
        pos += 1;
        let bwd = if has_bwd == 1 {
            Some(CsrAdjacency::read_from(data, &mut pos).map_err(|e| format!("bwd CSR: {e}"))?)
        } else {
            None
        };

        let num_props = read_u32(data, &mut pos)? as usize;
        checked_count_for_alloc(
            num_props,
            pos,
            data.len(),
            min_column_entry_wire_bytes(version),
            "rel_table properties",
        )?;
        let mut properties: FxHashMap<PropertyKey, ColumnCodec> = FxHashMap::default();
        let mut prop_defs = Vec::with_capacity(num_props);
        for _ in 0..num_props {
            let key_str = read_string(data, &mut pos, version)?;
            let key = PropertyKey::new(&key_str);
            let (codec, _block_stats) = read_codec(data_bytes, &mut pos, version)
                .map_err(|e| format!("edge codec: {e}"))?;
            let col_type = infer_column_type_from_codec(&codec);
            prop_defs.push(ColumnDef::new(&key_str, col_type));
            properties.insert(key, codec);
        }

        let src_label = table_id_to_label
            .get(src_tid as usize)
            .cloned()
            .unwrap_or_default();
        let dst_label = table_id_to_label
            .get(dst_tid as usize)
            .cloned()
            .unwrap_or_default();

        let schema = EdgeSchema::new(
            edge_type.as_str(),
            rel_table_id,
            src_label.as_str(),
            dst_label.as_str(),
            prop_defs,
        );

        let table = RelTable::new(schema, fwd, bwd, properties, src_tid, dst_tid);
        edge_type_to_rel_id
            .entry(edge_type.clone())
            .or_default()
            .push(rel_table_id);
        rel_table_id_to_type.push(edge_type);
        rel_tables.push(table);
    }

    // Compute statistics.
    let mut stats = Statistics::new();
    let mut total_nodes = 0u64;
    let mut total_edges = 0u64;
    for (idx, nt) in node_tables.iter().enumerate() {
        let c = nt.len() as u64;
        total_nodes += c;
        stats.update_label(table_id_to_label[idx].as_str(), LabelStatistics::new(c));
    }
    let mut edge_counts: FxHashMap<&str, u64> = FxHashMap::default();
    for (idx, rt) in rel_tables.iter().enumerate() {
        let c = rt.num_edges() as u64;
        total_edges += c;
        *edge_counts
            .entry(rel_table_id_to_type[idx].as_str())
            .or_default() += c;
    }
    for (et, count) in edge_counts {
        stats.update_edge_type(et, EdgeTypeStatistics::new(count, 0.0, 0.0));
    }
    stats.total_nodes = total_nodes;
    stats.total_edges = total_edges;

    let mut store = CompactStore::new(
        node_tables,
        label_to_table_id,
        rel_tables,
        edge_type_to_rel_id,
        table_id_to_label,
        rel_table_id_to_type,
        stats,
    );

    // ID maps.
    if preserves_ids {
        let node_map_len = read_u32(data, &mut pos)? as usize;
        checked_count_for_alloc(
            node_map_len,
            pos,
            data.len(),
            ID_MAP_ENTRY_WIRE_BYTES,
            "node_id_map",
        )?;
        let mut node_id_map = FxHashMap::with_capacity_and_hasher(node_map_len, Default::default());
        let num_tables = store.node_tables_by_id.len();
        let mut node_offset_to_id: Vec<Vec<NodeId>> = vec![Vec::new(); num_tables];
        for _ in 0..node_map_len {
            let nid = NodeId::new(read_u64(data, &mut pos)?);
            let tid = read_u16(data, &mut pos)?;
            let off = read_u64(data, &mut pos)?;
            // Fail closed on unknown table ids before forward-map insert or
            // reverse-map growth — an out-of-range tid must not leave a
            // dangling forward entry while skipping reverse validation.
            let tid_idx = usize::from(tid);
            let Some(rev) = node_offset_to_id.get_mut(tid_idx) else {
                return Err(format!(
                    "node_id_map unknown table id {tid} (num_tables {num_tables})"
                ));
            };
            // Cap reverse-map fill by the owning table length so a single
            // malicious offset cannot request unbounded growth. (Dense
            // growth up to a trusted table len remains O(table rows).)
            let off_idx = usize::try_from(off).unwrap_or(usize::MAX);
            let max_len = store
                .node_tables_by_id
                .get(tid_idx)
                .map_or(0, NodeTable::len);
            if off_idx >= max_len {
                return Err(format!(
                    "node_id_map offset {off} out of range for table {tid} (len {max_len})"
                ));
            }
            while rev.len() <= off_idx {
                rev.push(NodeId::INVALID);
            }
            rev[off_idx] = nid;
            node_id_map.insert(nid, (tid, off));
        }

        let edge_map_len = read_u32(data, &mut pos)? as usize;
        checked_count_for_alloc(
            edge_map_len,
            pos,
            data.len(),
            ID_MAP_ENTRY_WIRE_BYTES,
            "edge_id_map",
        )?;
        let mut edge_id_map = FxHashMap::with_capacity_and_hasher(edge_map_len, Default::default());
        let num_rel = store.rel_tables_by_id.len();
        let mut edge_offset_to_id: Vec<Vec<EdgeId>> = vec![Vec::new(); num_rel];
        for _ in 0..edge_map_len {
            let eid = EdgeId::new(read_u64(data, &mut pos)?);
            let rtid = read_u16(data, &mut pos)?;
            let csr_pos = read_u64(data, &mut pos)?;
            // Fail closed on unknown rel-table ids before forward-map insert
            // or reverse-map growth.
            let rtid_idx = usize::from(rtid);
            let Some(rev) = edge_offset_to_id.get_mut(rtid_idx) else {
                return Err(format!(
                    "edge_id_map unknown rel table id {rtid} (num_rel_tables {num_rel})"
                ));
            };
            let pos_idx = usize::try_from(csr_pos).unwrap_or(usize::MAX);
            let max_len = store
                .rel_tables_by_id
                .get(rtid_idx)
                .map_or(0, RelTable::num_edges);
            if pos_idx >= max_len {
                return Err(format!(
                    "edge_id_map csr_pos {csr_pos} out of range for rel table {rtid} (len {max_len})"
                ));
            }
            while rev.len() <= pos_idx {
                rev.push(EdgeId::INVALID);
            }
            rev[pos_idx] = eid;
            edge_id_map.insert(eid, (rtid, csr_pos));
        }

        store.set_id_maps(
            node_id_map,
            edge_id_map,
            node_offset_to_id,
            edge_offset_to_id,
        );
    }

    Ok(store)
}

// ── Write helpers ──────────────────────────────────────────────────

fn write_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_len(buf: &mut Vec<u8>, v: usize) {
    let n = u32::try_from(v).expect("length exceeds u32::MAX in compact section");
    buf.extend_from_slice(&n.to_le_bytes());
}

/// Writes a section-level string length prefix for the given payload version.
///
/// - v1/v2/v3: `u16` little-endian (limit 65_535)
/// - v4: `u32` little-endian (limit `u32::MAX`)
///
/// Overflow returns [`Error::Serialization`] (`GRAFEO-X002`) without panicking.
fn write_string_len(
    buf: &mut Vec<u8>,
    len: usize,
    version: u8,
) -> grafeo_common::utils::error::Result<()> {
    match version {
        FORMAT_VERSION_V1 | FORMAT_VERSION_V2 | FORMAT_VERSION_V3 => {
            let n = u16::try_from(len).map_err(|_| {
                grafeo_common::utils::error::Error::Serialization(format!(
                    "string length {len} exceeds u16::MAX (65535) for CompactStore payload version {version}"
                ))
            })?;
            write_u16(buf, n);
            Ok(())
        }
        FORMAT_VERSION => {
            let n = u32::try_from(len).map_err(|_| {
                grafeo_common::utils::error::Error::Serialization(format!(
                    "string length {len} exceeds u32::MAX for CompactStore payload version {version}"
                ))
            })?;
            write_u32(buf, n);
            Ok(())
        }
        other => Err(grafeo_common::utils::error::Error::Serialization(format!(
            "unsupported CompactStore section version {other} for string length write"
        ))),
    }
}

/// Writes a section-level UTF-8 string with a version-aware length prefix.
fn write_string(
    buf: &mut Vec<u8>,
    value: &str,
    version: u8,
) -> grafeo_common::utils::error::Result<()> {
    let bytes = value.as_bytes();
    write_string_len(buf, bytes.len(), version)?;
    buf.extend_from_slice(bytes);
    Ok(())
}

fn write_zone_map(
    buf: &mut Vec<u8>,
    zm: &ZoneMap,
    version: u8,
    context: Option<(&str, &str)>,
) -> grafeo_common::utils::error::Result<()> {
    write_len(buf, zm.null_count);
    write_len(buf, zm.row_count);
    // Encode min/max as (tag, value) pairs.
    write_optional_value(buf, &zm.min, version)
        .map_err(|e| wrap_zone_map_context(e, context, "min"))?;
    write_optional_value(buf, &zm.max, version)
        .map_err(|e| wrap_zone_map_context(e, context, "max"))?;
    Ok(())
}

fn wrap_zone_map_context(
    err: grafeo_common::utils::error::Error,
    context: Option<(&str, &str)>,
    field: &str,
) -> grafeo_common::utils::error::Error {
    match (err, context) {
        (grafeo_common::utils::error::Error::Serialization(msg), Some((label, key))) => {
            grafeo_common::utils::error::Error::Serialization(format!(
                "{msg} (zone_map_{field} node_label={label}, property_key={key})"
            ))
        }
        (other, _) => other,
    }
}

fn write_optional_value(
    buf: &mut Vec<u8>,
    v: &Option<grafeo_common::types::Value>,
    version: u8,
) -> grafeo_common::utils::error::Result<()> {
    match v {
        None => {
            buf.push(0);
            Ok(())
        }
        Some(grafeo_common::types::Value::Int64(n)) => {
            buf.push(1);
            // Store as raw i64 bytes to avoid sign-loss lint.
            buf.extend_from_slice(&n.to_le_bytes());
            Ok(())
        }
        Some(grafeo_common::types::Value::Bool(b)) => {
            buf.push(2);
            buf.push(u8::from(*b));
            Ok(())
        }
        Some(grafeo_common::types::Value::String(s)) => {
            buf.push(3);
            write_string(buf, s.as_str(), version)
        }
        Some(_) => {
            // Unsupported type for zone map: write as absent.
            buf.push(0);
            Ok(())
        }
    }
}

// ── Read helpers ───────────────────────────────────────────────────

/// Rejects decoded counts that cannot fit in the remaining payload before any
/// `Vec`/`HashMap` pre-allocation.
///
/// `pos` must already be past the count field. `min_item_bytes` is a
/// conservative lower bound on the on-wire size of each counted item (at least
/// 1). Overflow and oversize counts fail closed so malformed supported
/// payloads cannot request pathological capacity.
fn checked_count_for_alloc(
    count: usize,
    pos: usize,
    data_len: usize,
    min_item_bytes: usize,
    what: &str,
) -> Result<(), String> {
    debug_assert!(min_item_bytes >= 1);
    let remaining = data_len.saturating_sub(pos);
    let fits = count
        .checked_mul(min_item_bytes)
        .is_some_and(|need| need <= remaining);
    if !fits {
        return Err(format!(
            "{what} count {count} exceeds remaining payload ({remaining} bytes)"
        ));
    }
    Ok(())
}

/// Minimum on-wire bytes for an empty node table header after its count slot:
/// label length prefix + `row_count` + `num_cols`.
fn min_node_table_wire_bytes(version: u8) -> usize {
    let label_len_bytes = match version {
        FORMAT_VERSION => 4,
        _ => 2,
    };
    label_len_bytes + 4 + 4
}

/// Minimum on-wire bytes for an empty relationship table header after its
/// count slot: edge-type length prefix + src/dst table ids + empty CSR
/// (`offsets_len`, `targets_len`, no edge_data) + bwd flag + `num_props`.
fn min_rel_table_wire_bytes(version: u8) -> usize {
    let edge_type_len_bytes = match version {
        FORMAT_VERSION => 4,
        _ => 2,
    };
    // CSR empty: u32 offsets_len + u32 targets_len + u8 has_edge_data(=0).
    let csr_min = 4 + 4 + 1;
    edge_type_len_bytes + 2 + 2 + csr_min + 1 + 4
}

/// Minimum on-wire bytes for a column/property entry after its count slot:
/// key length prefix + zone-map flag (node columns) or codec tag alone.
fn min_column_entry_wire_bytes(version: u8) -> usize {
    let key_len_bytes = match version {
        FORMAT_VERSION => 4,
        _ => 2,
    };
    // Node columns: key + has_zm flag. Edge props omit the flag but still need
    // a codec tag (≥1). Use the smaller bound so valid edge props pass.
    key_len_bytes + 1
}

/// Fixed on-wire size of one id-map entry: id u64 + table id u16 + offset u64.
const ID_MAP_ENTRY_WIRE_BYTES: usize = 8 + 2 + 8;

fn read_u16(data: &[u8], pos: &mut usize) -> Result<u16, String> {
    if *pos + 2 > data.len() {
        return Err("truncated u16".into());
    }
    let v = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
    *pos += 2;
    Ok(v)
}

fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32, String> {
    if *pos + 4 > data.len() {
        return Err("truncated u32".into());
    }
    let v = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
    *pos += 4;
    Ok(v)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    if *pos + 8 > data.len() {
        return Err("truncated u64".into());
    }
    let v = u64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

fn read_string(data: &[u8], pos: &mut usize, version: u8) -> Result<String, String> {
    let slen = match version {
        FORMAT_VERSION_V1 | FORMAT_VERSION_V2 | FORMAT_VERSION_V3 => {
            usize::from(read_u16(data, pos)?)
        }
        FORMAT_VERSION => read_u32(data, pos)? as usize,
        other => {
            return Err(format!(
                "unsupported CompactStore section version {other} for string read"
            ));
        }
    };
    let end = pos
        .checked_add(slen)
        .ok_or_else(|| "truncated string".to_string())?;
    if end > data.len() {
        return Err("truncated string".into());
    }
    let s = std::str::from_utf8(&data[*pos..end]).map_err(|_| "invalid UTF-8".to_string())?;
    *pos = end;
    Ok(s.to_string())
}

fn read_zone_map(data: &[u8], pos: &mut usize, version: u8) -> Result<ZoneMap, String> {
    let null_count = read_u32(data, pos)? as usize;
    let row_count = read_u32(data, pos)? as usize;
    let min = read_optional_value(data, pos, version)?;
    let max = read_optional_value(data, pos, version)?;
    Ok(ZoneMap {
        min,
        max,
        null_count,
        row_count,
    })
}

fn read_optional_value(
    data: &[u8],
    pos: &mut usize,
    version: u8,
) -> Result<Option<grafeo_common::types::Value>, String> {
    let tag = *data.get(*pos).ok_or("truncated value tag")?;
    *pos += 1;
    match tag {
        0 => Ok(None),
        1 => {
            // Read raw i64 bytes (written via i64::to_le_bytes).
            if *pos + 8 > data.len() {
                return Err("truncated i64 value".into());
            }
            let v = i64::from_le_bytes(data[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            Ok(Some(grafeo_common::types::Value::Int64(v)))
        }
        2 => {
            let b = *data.get(*pos).ok_or("truncated bool")?;
            *pos += 1;
            Ok(Some(grafeo_common::types::Value::Bool(b != 0)))
        }
        3 => {
            let s = read_string(data, pos, version)?;
            Ok(Some(grafeo_common::types::Value::String(
                arcstr::ArcStr::from(s.as_str()),
            )))
        }
        _ => Err(format!("unknown value tag {tag}")),
    }
}

fn infer_column_type_from_codec(codec: &ColumnCodec) -> ColumnType {
    match codec {
        ColumnCodec::BitPacked(bp) => ColumnType::UInt {
            bits: bp.bits_per_value(),
        },
        ColumnCodec::Dict(_) => ColumnType::DictString,
        ColumnCodec::Bitmap(_) => ColumnType::Bool,
        ColumnCodec::Int8Vector { dimensions, .. } => ColumnType::Int8Vector {
            dimensions: *dimensions,
        },
        ColumnCodec::Float64(_) => ColumnType::Float64,
        ColumnCodec::Float32Vector { dimensions, .. } => ColumnType::Float32Vector {
            dimensions: *dimensions,
        },
        ColumnCodec::RawI64(_) => ColumnType::Int64,
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::compact::from_graph_store_preserving_ids;
    use crate::graph::lpg::LpgStore;
    use crate::graph::traits::GraphStore;
    use grafeo_common::types::Value;

    #[test]
    fn test_round_trip_empty() {
        let store = LpgStore::new().unwrap();
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let section = CompactStoreSection::new(Arc::new(compact));

        let bytes = section.serialize().unwrap();
        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&bytes).unwrap();

        let restored = section2.store().unwrap();
        assert_eq!(restored.node_count(), 0);
        assert_eq!(restored.edge_count(), 0);
    }

    #[test]
    fn test_round_trip_nodes_and_edges() {
        let store = LpgStore::new().unwrap();
        let alix = store.create_node(&["Person"]);
        store.set_node_property(alix, "name", Value::from("Alix"));
        store.set_node_property(alix, "age", Value::Int64(30));

        let gus = store.create_node(&["Person"]);
        store.set_node_property(gus, "name", Value::from("Gus"));
        store.set_node_property(gus, "age", Value::Int64(25));

        let amsterdam = store.create_node(&["City"]);
        store.set_node_property(amsterdam, "name", Value::from("Amsterdam"));

        store.create_edge(alix, amsterdam, "LIVES_IN");
        store.create_edge(gus, amsterdam, "LIVES_IN");

        let compact = from_graph_store_preserving_ids(&store).unwrap();
        assert!(compact.preserves_ids());

        let section = CompactStoreSection::new(Arc::new(compact));
        let bytes = section.serialize().unwrap();

        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&bytes).unwrap();
        let restored = section2.store().unwrap();

        assert!(restored.preserves_ids());
        assert_eq!(restored.node_count(), 3);
        assert_eq!(restored.edge_count(), 2);

        // Verify original IDs survive.
        let alix_node = restored.get_node(alix).expect("Alix by original ID");
        assert_eq!(
            alix_node.properties.get(&PropertyKey::new("name")),
            Some(&Value::String(arcstr::ArcStr::from("Alix")))
        );
        assert_eq!(
            alix_node.properties.get(&PropertyKey::new("age")),
            Some(&Value::Int64(30))
        );

        // Verify edge traversal.
        let neighbors = restored.neighbors(alix, crate::graph::Direction::Outgoing);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0], amsterdam);
    }

    #[test]
    fn test_round_trip_without_id_preservation() {
        use crate::graph::compact::from_graph_store;

        let lpg = LpgStore::new().unwrap();
        let a = lpg.create_node(&["Node"]);
        lpg.set_node_property(a, "val", Value::Int64(42));
        let b = lpg.create_node(&["Node"]);
        lpg.set_node_property(b, "val", Value::Int64(99));
        lpg.create_edge(a, b, "LINK");

        let compact = from_graph_store(&lpg).unwrap();
        assert!(!compact.preserves_ids());

        let section = CompactStoreSection::new(Arc::new(compact));
        let bytes = section.serialize().unwrap();

        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&bytes).unwrap();
        let restored = section2.store().unwrap();

        assert!(!restored.preserves_ids());
        assert_eq!(restored.node_count(), 2);
        assert_eq!(restored.edge_count(), 1);
    }

    #[test]
    fn test_crc_integrity() {
        let store = LpgStore::new().unwrap();
        store.create_node(&["Test"]);
        let compact = from_graph_store_preserving_ids(&store).unwrap();

        let section = CompactStoreSection::new(Arc::new(compact));
        let mut bytes = section.serialize().unwrap();

        // Corrupt a byte in the middle.
        if bytes.len() > 10 {
            bytes[10] ^= 0xFF;
        }

        let mut section2 = CompactStoreSection::empty();
        assert!(section2.deserialize(&bytes).is_err());
    }

    #[test]
    fn test_section_type_and_version() {
        let section = CompactStoreSection::empty();
        assert_eq!(section.section_type(), SectionType::CompactStore);
        assert_eq!(section.version(), FORMAT_VERSION);
        assert!(!section.is_dirty());
        assert_eq!(section.memory_usage(), 0);
    }

    #[test]
    fn test_dirty_tracking() {
        let section = CompactStoreSection::empty();
        assert!(!section.is_dirty());
        section.mark_dirty();
        assert!(section.is_dirty());
        section.mark_clean();
        assert!(!section.is_dirty());
    }

    /// Phase 2b: confirm the v1 (flat-column) on-disk format still
    /// round-trips through the v2-aware deserializer, exercising the
    /// compat path users on 0.5.41 and earlier rely on for one release.
    #[test]
    fn nelson_v1_section_reads_through_v2_aware_deserializer() {
        let store = LpgStore::new().unwrap();
        let alix = store.create_node(&["Person"]);
        store.set_node_property(alix, "name", Value::from("Alix"));
        store.set_node_property(alix, "age", Value::Int64(30));

        let gus = store.create_node(&["Person"]);
        store.set_node_property(gus, "name", Value::from("Gus"));
        store.set_node_property(gus, "age", Value::Int64(25));

        store.create_edge(alix, gus, "KNOWS");

        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let section = CompactStoreSection::new(Arc::new(compact));

        // Force v1 layout (flat columns, version byte = 1).
        let v1_bytes = section.serialize_with_version(FORMAT_VERSION_V1).unwrap();
        // First byte after MAGIC must be the v1 marker.
        assert_eq!(
            v1_bytes[4], FORMAT_VERSION_V1,
            "expected v1 marker in version byte"
        );

        // The v2-aware deserializer must handle both versions.
        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&v1_bytes).unwrap();
        let restored = section2.store().unwrap();

        assert_eq!(restored.node_count(), 2);
        assert_eq!(restored.edge_count(), 1);
        assert_eq!(
            restored.get_node_property(alix, &PropertyKey::new("name")),
            Some(Value::String(arcstr::ArcStr::from("Alix")))
        );
        assert_eq!(
            restored.get_node_property(alix, &PropertyKey::new("age")),
            Some(Value::Int64(30))
        );
    }

    // ── Phase 2c: per-block zone maps ────────────────────────────────

    /// The builder must populate per-block zone maps for every column,
    /// one ZoneMap per block. `1024` rows per block (DEFAULT_BLOCK_ROWS).
    #[test]
    fn alix_builder_populates_per_block_zone_maps() {
        let store = LpgStore::new().unwrap();
        // 3000 nodes → 3 blocks (1024 + 1024 + 952).
        for i in 0i64..3000 {
            let n = store.create_node(&["Person"]);
            store.set_node_property(n, "age", Value::Int64(i));
        }
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let table = &compact.node_tables_by_id[0];
        let block_zms = table
            .block_zone_maps_for(&PropertyKey::new("age"))
            .expect("per-block stats present");
        assert_eq!(block_zms.len(), 3, "3000 rows should produce 3 blocks");
        assert_eq!(block_zms[0].row_count, 1024);
        assert_eq!(block_zms[1].row_count, 1024);
        assert_eq!(block_zms[2].row_count, 952);
        assert_eq!(block_zms[0].min, Some(Value::Int64(0)));
        assert_eq!(block_zms[0].max, Some(Value::Int64(1023)));
        assert_eq!(block_zms[1].min, Some(Value::Int64(1024)));
        assert_eq!(block_zms[1].max, Some(Value::Int64(2047)));
        assert_eq!(block_zms[2].min, Some(Value::Int64(2048)));
        assert_eq!(block_zms[2].max, Some(Value::Int64(2999)));
    }

    /// v3 round-trip preserves per-block zone maps verbatim.
    #[test]
    fn gus_v3_round_trip_preserves_block_zone_maps() {
        let store = LpgStore::new().unwrap();
        for i in 0i64..2500 {
            let n = store.create_node(&["Item"]);
            store.set_node_property(n, "score", Value::Int64(i));
        }
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let original = &compact.node_tables_by_id[0];
        let original_zms = original
            .block_zone_maps_for(&PropertyKey::new("score"))
            .expect("original block stats")
            .to_vec();

        let section = CompactStoreSection::new(Arc::new(compact));
        let bytes = section.serialize_with_version(FORMAT_VERSION_V3).unwrap();
        assert_eq!(bytes[4], FORMAT_VERSION_V3);
        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&bytes).unwrap();
        let restored = section2.store().unwrap();
        let restored_table = &restored.node_tables_by_id[0];
        let restored_zms = restored_table
            .block_zone_maps_for(&PropertyKey::new("score"))
            .expect("restored block stats");

        assert_eq!(restored_zms.len(), original_zms.len());
        for (i, (orig, rest)) in original_zms.iter().zip(restored_zms.iter()).enumerate() {
            assert_eq!(orig.row_count, rest.row_count, "row_count mismatch at {i}");
            assert_eq!(
                orig.null_count, rest.null_count,
                "null_count mismatch at {i}"
            );
            assert_eq!(orig.min, rest.min, "min mismatch at {i}");
            assert_eq!(orig.max, rest.max, "max mismatch at {i}");
        }
    }

    /// v2 sections (Phase 2b) carry no per-block zone maps; the v3 reader
    /// must accept them and leave `block_zone_maps_for` returning `None`.
    #[test]
    fn vincent_v2_section_round_trip_leaves_block_zone_maps_empty() {
        let store = LpgStore::new().unwrap();
        for i in 0i64..1500 {
            let n = store.create_node(&["Item"]);
            store.set_node_property(n, "score", Value::Int64(i));
        }
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let section = CompactStoreSection::new(Arc::new(compact));
        let v2_bytes = section.serialize_with_version(FORMAT_VERSION_V2).unwrap();
        assert_eq!(v2_bytes[4], FORMAT_VERSION_V2);

        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&v2_bytes).unwrap();
        let restored = section2.store().unwrap();
        let table = &restored.node_tables_by_id[0];
        assert!(
            table
                .block_zone_maps_for(&PropertyKey::new("score"))
                .is_none(),
            "v2 stream must not populate block_zone_maps"
        );
        // But the column data still survives.
        assert_eq!(table.len(), 1500);
    }

    /// v1 sections likewise carry no per-block stats.
    #[test]
    fn jules_v1_section_round_trip_leaves_block_zone_maps_empty() {
        let store = LpgStore::new().unwrap();
        for i in 0i64..1500 {
            let n = store.create_node(&["Item"]);
            store.set_node_property(n, "score", Value::Int64(i));
        }
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let section = CompactStoreSection::new(Arc::new(compact));
        let v1_bytes = section.serialize_with_version(FORMAT_VERSION_V1).unwrap();
        assert_eq!(v1_bytes[4], FORMAT_VERSION_V1);

        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&v1_bytes).unwrap();
        let restored = section2.store().unwrap();
        let table = &restored.node_tables_by_id[0];
        assert!(
            table
                .block_zone_maps_for(&PropertyKey::new("score"))
                .is_none(),
            "v1 stream must not populate block_zone_maps"
        );
        assert_eq!(table.len(), 1500);
    }

    /// String columns also get per-block min/max.
    #[test]
    fn mia_block_zone_maps_for_string_column() {
        let store = LpgStore::new().unwrap();
        // Use enough nodes to force >= 2 blocks.
        for i in 0u32..1100 {
            let n = store.create_node(&["Tag"]);
            store.set_node_property(n, "name", Value::from(format!("tag_{i:04}")));
        }
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let table = &compact.node_tables_by_id[0];
        let block_zms = table
            .block_zone_maps_for(&PropertyKey::new("name"))
            .expect("string column block stats");
        assert_eq!(block_zms.len(), 2);
        assert_eq!(
            block_zms[0].min,
            Some(Value::String(arcstr::ArcStr::from("tag_0000")))
        );
        assert_eq!(
            block_zms[0].max,
            Some(Value::String(arcstr::ArcStr::from("tag_1023")))
        );
        assert_eq!(
            block_zms[1].min,
            Some(Value::String(arcstr::ArcStr::from("tag_1024")))
        );
        assert_eq!(
            block_zms[1].max,
            Some(Value::String(arcstr::ArcStr::from("tag_1099")))
        );
    }

    /// Phase 2b: an unsupported version byte must produce a clean error,
    /// not panic or silently misread the section.
    #[test]
    fn rita_unknown_version_returns_clear_error() {
        let store = LpgStore::new().unwrap();
        let _ = store.create_node(&["Item"]);
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let section = CompactStoreSection::new(Arc::new(compact));
        let mut bytes = section.serialize().unwrap();
        // Strip CRC, flip version byte to a future v9, recompute CRC.
        let crc_pos = bytes.len() - 4;
        bytes[4] = 9;
        let crc = crc32fast::hash(&bytes[..crc_pos]);
        bytes[crc_pos..].copy_from_slice(&crc.to_le_bytes());

        let mut section2 = CompactStoreSection::empty();
        let err = section2
            .deserialize(&bytes)
            .expect_err("expected version error");
        let msg = err.to_string();
        assert!(
            msg.contains("unsupported CompactStore section version"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn test_round_trip_bool_column() {
        let store = LpgStore::new().unwrap();
        let a = store.create_node(&["Item"]);
        store.set_node_property(a, "active", Value::Bool(true));
        let b = store.create_node(&["Item"]);
        store.set_node_property(b, "active", Value::Bool(false));

        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let section = CompactStoreSection::new(Arc::new(compact));
        let bytes = section.serialize().unwrap();

        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&bytes).unwrap();
        let restored = section2.store().unwrap();

        assert_eq!(
            restored.get_node_property(a, &PropertyKey::new("active")),
            Some(Value::Bool(true))
        );
        assert_eq!(
            restored.get_node_property(b, &PropertyKey::new("active")),
            Some(Value::Bool(false))
        );
    }

    #[test]
    fn test_round_trip_edge_properties() {
        let store = LpgStore::new().unwrap();
        let a = store.create_node(&["Node"]);
        let b = store.create_node(&["Node"]);
        let e = store.create_edge(a, b, "LINK");
        store.set_edge_property(e, "weight", Value::Int64(5));

        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let section = CompactStoreSection::new(Arc::new(compact));
        let bytes = section.serialize().unwrap();

        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&bytes).unwrap();
        let restored = section2.store().unwrap();

        // Find the edge via traversal.
        let edges = restored.edges_from(a, crate::graph::Direction::Outgoing);
        assert_eq!(edges.len(), 1);
        let edge = restored.get_edge(edges[0].1).unwrap();
        assert_eq!(
            edge.properties.get(&PropertyKey::new("weight")),
            Some(&Value::Int64(5))
        );
    }

    // ── E-0.1: CompactStore payload v4 section-level strings ───────────

    /// Builds a compact store with a single `CodeSymbol`-like string property
    /// of the given UTF-8 byte length so table-level zone-map min/max exercise
    /// the section-level string codec (the production failure path).
    fn compact_with_documentation_json(len: usize) -> (Arc<CompactStore>, NodeId, String) {
        let body: String = "D".repeat(len);
        assert_eq!(body.len(), len);
        let store = LpgStore::new().unwrap();
        let node = store.create_node(&["CodeSymbol"]);
        store.set_node_property(node, "documentation_json", Value::from(body.as_str()));
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        // Prove the large value is present in the table zone map (section path).
        let table = &compact.node_tables_by_id[0];
        let zm = table
            .zone_map(&PropertyKey::new("documentation_json"))
            .expect("table zone map for documentation_json");
        assert_eq!(
            zm.min,
            Some(Value::String(arcstr::ArcStr::from(body.as_str())))
        );
        assert_eq!(
            zm.max,
            Some(Value::String(arcstr::ArcStr::from(body.as_str())))
        );
        (Arc::new(compact), node, body)
    }

    #[test]
    fn e0_v3_string_boundary_65535_round_trips() {
        let (compact, node, body) = compact_with_documentation_json(65_535);
        let section = CompactStoreSection::new(compact);
        let bytes = section
            .serialize_with_version(FORMAT_VERSION_V3)
            .expect("v3 must accept 65535-byte section strings");
        assert_eq!(bytes[4], FORMAT_VERSION_V3);

        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&bytes).unwrap();
        let restored = section2.store().unwrap();
        assert_eq!(
            restored.get_node_property(node, &PropertyKey::new("documentation_json")),
            Some(Value::String(arcstr::ArcStr::from(body.as_str())))
        );
    }

    #[test]
    fn e0_v3_string_boundary_65536_returns_serialization_error_without_panic() {
        let (compact, _node, _body) = compact_with_documentation_json(65_536);
        let section = CompactStoreSection::new(compact);
        let err = section
            .serialize_with_version(FORMAT_VERSION_V3)
            .expect_err("v3 must reject 65536-byte section strings");
        let display = err.to_string();
        assert!(
            display.contains("GRAFEO-X002") || display.contains("Serialization error"),
            "display form: {display}"
        );
        match err {
            grafeo_common::utils::error::Error::Serialization(msg) => {
                assert!(
                    msg.contains("65536") || msg.contains("u16::MAX") || msg.contains("65535"),
                    "unexpected serialization message: {msg}"
                );
                assert!(
                    msg.contains("documentation_json") || msg.contains("zone_map"),
                    "error should identify the zone-map property context: {msg}"
                );
            }
            other => panic!("expected Error::Serialization, got {other}"),
        }
    }

    #[test]
    fn e0_v4_string_boundary_65536_round_trips() {
        let (compact, node, body) = compact_with_documentation_json(65_536);
        let section = CompactStoreSection::new(compact);
        let bytes = section
            .serialize()
            .expect("v4 writer must accept 65536-byte strings");
        assert_eq!(bytes[4], FORMAT_VERSION);
        assert_eq!(section.version(), FORMAT_VERSION);
        assert_eq!(FORMAT_VERSION, 4);

        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&bytes).unwrap();
        let restored = section2.store().unwrap();
        let got = restored
            .get_node_property(node, &PropertyKey::new("documentation_json"))
            .expect("property present");
        match got {
            Value::String(s) => assert_eq!(s.as_str(), body.as_str()),
            other => panic!("expected string, got {other:?}"),
        }
    }

    #[test]
    fn e0_v4_production_like_96kib_round_trips() {
        let len = 96 * 1024;
        let (compact, node, body) = compact_with_documentation_json(len);
        let section = CompactStoreSection::new(compact);
        let bytes = section.serialize().unwrap();
        assert_eq!(bytes[4], 4);

        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&bytes).unwrap();
        let restored = section2.store().unwrap();
        assert_eq!(
            restored.get_node_property(node, &PropertyKey::new("documentation_json")),
            Some(Value::String(arcstr::ArcStr::from(body.as_str())))
        );
    }

    #[test]
    fn e0_v4_truncated_string_length_fails_closed() {
        // Claim 10 UTF-8 bytes but supply only 2 after a u32 length prefix.
        let mut data = Vec::new();
        data.extend_from_slice(&10u32.to_le_bytes());
        data.extend_from_slice(b"ab");
        let mut pos = 0;
        let err = read_string(&data, &mut pos, FORMAT_VERSION).expect_err("truncated");
        assert!(err.contains("truncated string"), "unexpected error: {err}");
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn e0_write_string_len_rejects_over_u32_max_without_allocating_string() {
        let mut buf = Vec::new();
        let over = (u32::MAX as usize).saturating_add(1);
        assert!(over > u32::MAX as usize);
        let err = write_string_len(&mut buf, over, FORMAT_VERSION).expect_err("over u32");
        match err {
            grafeo_common::utils::error::Error::Serialization(msg) => {
                assert!(
                    msg.contains("u32::MAX") || msg.contains(&over.to_string()),
                    "msg={msg}"
                );
            }
            other => panic!("expected Serialization, got {other}"),
        }
        assert!(buf.is_empty(), "must not write a partial length prefix");
    }

    #[test]
    fn e0_serialize_rejects_unknown_version() {
        let store = LpgStore::new().unwrap();
        let _ = store.create_node(&["Item"]);
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let section = CompactStoreSection::new(Arc::new(compact));
        let err = section
            .serialize_with_version(9)
            .expect_err("unknown version");
        match err {
            grafeo_common::utils::error::Error::Serialization(msg) => {
                assert!(
                    msg.contains("unsupported CompactStore section version 9"),
                    "msg={msg}"
                );
            }
            other => panic!("expected Serialization, got {other}"),
        }
    }

    #[test]
    fn e0_immutable_v1_fixture_remains_readable() {
        // SHA-256 (pin 9781320… writer): 287857884fbb9c5401d77ec17b12d12c22575b508510f1f19fd3e610edd110f2
        const FIXTURE: &[u8] = include_bytes!("fixtures/compact_store_v1_small.bin");
        assert_eq!(FIXTURE.len(), 280);
        assert_eq!(FIXTURE[4], FORMAT_VERSION_V1);
        let mut section = CompactStoreSection::empty();
        section
            .deserialize(FIXTURE)
            .expect("v1 fixture must decode");
        let restored = section.store().unwrap();
        assert_eq!(restored.node_count(), 2);
        assert_eq!(restored.edge_count(), 1);
    }

    #[test]
    fn e0_immutable_v2_fixture_remains_readable() {
        // SHA-256 (pin 9781320… writer): 12e4afb16e36172c46e59f41ee0a31c9d95234fafdca8e6a627021ff8ce917a0
        const FIXTURE: &[u8] = include_bytes!("fixtures/compact_store_v2_small.bin");
        assert_eq!(FIXTURE.len(), 304);
        assert_eq!(FIXTURE[4], FORMAT_VERSION_V2);
        let mut section = CompactStoreSection::empty();
        section
            .deserialize(FIXTURE)
            .expect("v2 fixture must decode");
        let restored = section.store().unwrap();
        assert_eq!(restored.node_count(), 2);
        assert_eq!(restored.edge_count(), 1);
    }

    #[test]
    fn e0_immutable_v3_fixture_remains_readable() {
        // SHA-256 (pin 9781320… writer): 62227ca683fafa1d4b4f61368ba1730e568fb29f473502f0ab345268836f1d8a
        const FIXTURE: &[u8] = include_bytes!("fixtures/compact_store_v3_small.bin");
        assert_eq!(FIXTURE.len(), 355);
        assert_eq!(FIXTURE[4], FORMAT_VERSION_V3);
        let mut section = CompactStoreSection::empty();
        section
            .deserialize(FIXTURE)
            .expect("v3 fixture must decode");
        let restored = section.store().unwrap();
        assert_eq!(restored.node_count(), 2);
        assert_eq!(restored.edge_count(), 1);
        // Fixture graph: Alix age=30, Gus age=25, KNOWS edge.
        // Original node ids start at 0 in LpgStore for this graph.
        let alix = NodeId::new(0);
        assert_eq!(
            restored.get_node_property(alix, &PropertyKey::new("name")),
            Some(Value::String(arcstr::ArcStr::from("Alix")))
        );
        assert_eq!(
            restored.get_node_property(alix, &PropertyKey::new("age")),
            Some(Value::Int64(30))
        );
    }

    #[test]
    fn e0_v4_payload_and_section_version_are_four() {
        let store = LpgStore::new().unwrap();
        let _ = store.create_node(&["Item"]);
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let section = CompactStoreSection::new(Arc::new(compact));
        assert_eq!(section.version(), 4);
        let bytes = section.serialize().unwrap();
        assert_eq!(bytes[4], 4);
    }

    /// Malformed supported v4: CRC-valid header-only payload whose trailing
    /// CRC bytes would otherwise be reinterpreted as `num_node_tables` and
    /// drive a pathological `Vec::with_capacity`. Must fail closed with a
    /// bounds error — never allocate gigabytes or abort.
    #[test]
    fn e0_malformed_v4_header_only_fails_closed_without_pathological_alloc() {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"GCST");
        payload.push(FORMAT_VERSION);
        payload.push(0);
        let crc = crc32fast::hash(&payload);
        payload.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(payload.len(), 10);

        let mut section = CompactStoreSection::empty();
        let err = section
            .deserialize(&payload)
            .expect_err("header-only v4 must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("exceeds remaining payload")
                || msg.contains("node_tables")
                || msg.contains("truncated"),
            "expected checked-bounds error, got: {msg}"
        );
    }

    /// Explicit huge `num_node_tables` on a tiny CRC-valid v4 payload.
    #[test]
    fn e0_malformed_v4_huge_node_table_count_fails_closed_without_pathological_alloc() {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"GCST");
        payload.push(FORMAT_VERSION);
        payload.push(0); // flags: no id maps
        payload.extend_from_slice(&u32::MAX.to_le_bytes()); // num_node_tables
        let crc = crc32fast::hash(&payload);
        payload.extend_from_slice(&crc.to_le_bytes());

        let mut section = CompactStoreSection::empty();
        let err = section
            .deserialize(&payload)
            .expect_err("huge node_tables count must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("exceeds remaining payload")
                || msg.contains("node_tables")
                || msg.contains("u16::MAX"),
            "expected checked-bounds error, got: {msg}"
        );
    }

    /// Recompute trailing CRC32 over `payload[..len-4]` after a surgical mutate.
    fn reseal_section_crc(payload: &mut [u8]) {
        assert!(payload.len() >= 4, "section payload too short for CRC");
        let body_end = payload.len() - 4;
        let crc = crc32fast::hash(&payload[..body_end]);
        payload[body_end..].copy_from_slice(&crc.to_le_bytes());
    }

    /// Malformed supported v4: `node_id_map` entry references a table id that
    /// does not exist. Must fail closed before accepting an inconsistent
    /// forward map or growing reverse vectors against an unknown table.
    #[test]
    fn e0_malformed_v4_invalid_node_table_id_in_id_map_fails_closed() {
        let store = LpgStore::new().unwrap();
        let _ = store.create_node(&["Person"]);
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        assert_eq!(compact.node_tables_by_id.len(), 1);
        assert!(compact.preserves_ids());
        let section = CompactStoreSection::new(Arc::new(compact));
        let mut bytes = section.serialize().unwrap();
        assert_eq!(bytes[4], FORMAT_VERSION);
        assert_ne!(bytes[5] & 0x01, 0, "preserves_ids flag must be set");

        // Trailing layout before CRC:
        //   node_map_len(4) | entry(nid u64, tid u16, off u64) | edge_map_len(4)=0
        // tid sits 14 bytes before CRC start: edge_map_len(4) + off(8) + tid(2).
        let tid_off = bytes.len() - 4 /*crc*/ - 4 /*edge_map_len*/ - 8 /*off*/ - 2 /*tid*/;
        let old_tid = u16::from_le_bytes([bytes[tid_off], bytes[tid_off + 1]]);
        assert_eq!(
            old_tid, 0,
            "expected sole node table id 0 before corruption"
        );
        // Out-of-range tid (only table 0 exists). Previously this inserted into
        // the forward map and skipped reverse validation — fail open.
        bytes[tid_off..tid_off + 2].copy_from_slice(&u16::MAX.to_le_bytes());
        reseal_section_crc(&mut bytes);

        let mut section2 = CompactStoreSection::empty();
        let err = section2
            .deserialize(&bytes)
            .expect_err("unknown node table id must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("unknown table id") && msg.contains("node_id_map"),
            "expected unknown node table id error, got: {msg}"
        );
        assert!(
            section2.store().is_none(),
            "must not accept a partially decoded / inconsistent store"
        );
    }

    /// Malformed supported v4: `edge_id_map` entry references a relationship
    /// table id that does not exist. Must fail closed without accepting an
    /// inconsistent edge map.
    #[test]
    fn e0_malformed_v4_invalid_rel_table_id_in_id_map_fails_closed() {
        let store = LpgStore::new().unwrap();
        let a = store.create_node(&["Person"]);
        let b = store.create_node(&["Person"]);
        store.create_edge(a, b, "KNOWS");
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        assert_eq!(compact.rel_tables_by_id.len(), 1);
        assert!(compact.preserves_ids());
        let section = CompactStoreSection::new(Arc::new(compact));
        let mut bytes = section.serialize().unwrap();
        assert_eq!(bytes[4], FORMAT_VERSION);
        assert_ne!(bytes[5] & 0x01, 0, "preserves_ids flag must be set");

        // Trailing layout before CRC ends with one edge_id_map entry:
        //   ... | edge_map_len(4)=1 | eid u64 | rtid u16 | csr_pos u64 | crc
        // rtid sits 14 bytes before end: crc(4) + csr_pos(8) + rtid(2).
        let rtid_off = bytes.len() - 4 /*crc*/ - 8 /*csr_pos*/ - 2 /*rtid*/;
        let old_rtid = u16::from_le_bytes([bytes[rtid_off], bytes[rtid_off + 1]]);
        assert_eq!(
            old_rtid, 0,
            "expected sole rel table id 0 before corruption"
        );
        bytes[rtid_off..rtid_off + 2].copy_from_slice(&1u16.to_le_bytes());
        reseal_section_crc(&mut bytes);

        let mut section2 = CompactStoreSection::empty();
        let err = section2
            .deserialize(&bytes)
            .expect_err("unknown rel table id must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("unknown rel table id") && msg.contains("edge_id_map"),
            "expected unknown rel table id error, got: {msg}"
        );
        assert!(
            section2.store().is_none(),
            "must not accept a partially decoded / inconsistent store"
        );
    }

    #[test]
    fn e0_v3_reader_compat_via_serialize_with_version() {
        let store = LpgStore::new().unwrap();
        let n = store.create_node(&["Person"]);
        store.set_node_property(n, "name", Value::from("Alix"));
        let compact = from_graph_store_preserving_ids(&store).unwrap();
        let section = CompactStoreSection::new(Arc::new(compact));
        let v3 = section.serialize_with_version(FORMAT_VERSION_V3).unwrap();
        assert_eq!(v3[4], FORMAT_VERSION_V3);
        let mut section2 = CompactStoreSection::empty();
        section2.deserialize(&v3).unwrap();
        assert_eq!(
            section2
                .store()
                .unwrap()
                .get_node_property(n, &PropertyKey::new("name")),
            Some(Value::String(arcstr::ArcStr::from("Alix")))
        );
    }
}
