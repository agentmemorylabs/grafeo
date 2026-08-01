//! Bounded emission: metadata + ID lookups + zone maps (G-EM0.5b Phase 2b).
//!
//! Builds the Metadata, NodeIdLookup, EdgeIdLookup, NodeOriginalIds,
//! EdgeOriginalIds, TableZoneMaps, and BlockZoneMaps segments from bounded
//! pass outputs. Layout is byte-exact with `emit_canonical_descriptors`.

use crate::graph::compact::generation::{
    CancelToken, GenerationBudget, GenerationError, GenerationMetrics, RunSetLease,
};
use crate::graph::compact::generation_builder::column_pass::ColumnGeometry;
use crate::graph::compact::generation_builder::emit_columns::codec_kind_of;
use crate::graph::compact::generation_builder::emit_meta::{w16, w32, w64};
use crate::graph::compact::mapped::id_index::MappedNodeIdIndex;
use grafeo_common::utils::hash::FxHashMap;

/// Builds the Metadata segment bytes from bounded pass outputs.
///
/// Layout: node_table_count u32, then per table: (tid u16, label_code u32,
/// row_count u32, col_count u32, per col: (key_code u32, disc u16, vtype u16)),
/// then rel_table_count u32, per rel: (rid u16, src u16, dst u16, type_code u32,
/// edge_count u32, prop_count u32, per prop: (key_code u32, disc u16, vtype u16)),
/// then total_nodes u64, total_edges u64.
#[allow(clippy::too_many_arguments)]
pub fn build_metadata(
    node_labels: &[String],
    node_row_counts: &[u64],
    node_col_keys: &[Vec<String>],   // per table, sorted prop keys
    rel_keys: &[(String, u16, u16)], // (edge_type, src_tid, dst_tid)
    rel_edge_counts: &[u64],
    rel_col_keys: &[Vec<String>], // per rel table, sorted prop keys
    string_index: &FxHashMap<String, u32>,
    geometries: &[ColumnGeometry],
    total_nodes: u64,
    total_edges: u64,
) -> Result<Vec<u8>, GenerationError> {
    let mut meta = Vec::new();
    let geo_by_key: FxHashMap<(u16, String), &ColumnGeometry> = geometries
        .iter()
        .map(|g| ((g.table_id, g.key.clone()), g))
        .collect();

    w32(&mut meta, node_labels.len() as u32);
    for (tid, label) in node_labels.iter().enumerate() {
        let label_code = *string_index
            .get(label)
            .ok_or_else(|| GenerationError::Codec(format!("label not interned: {label}")))?;
        w16(&mut meta, tid as u16);
        w32(&mut meta, label_code);
        w32(&mut meta, node_row_counts[tid] as u32);
        let keys = &node_col_keys[tid];
        w32(&mut meta, keys.len() as u32);
        for key in keys {
            let key_code = *string_index
                .get(key)
                .ok_or_else(|| GenerationError::Codec(format!("key not interned: {key}")))?;
            w32(&mut meta, key_code);
            let kind = geo_by_key
                .get(&(tid as u16, key.clone()))
                .map(|g| codec_kind_of(g))
                .unwrap_or(crate::graph::compact::generation_builder::emit_meta::CodecKind::Dict);
            w16(&mut meta, kind.disc());
            w16(&mut meta, kind.value_type());
        }
    }

    w32(&mut meta, rel_keys.len() as u32);
    for (rid, (edge_type, src_tid, dst_tid)) in rel_keys.iter().enumerate() {
        let type_code = *string_index.get(edge_type).ok_or_else(|| {
            GenerationError::Codec(format!("edge type not interned: {edge_type}"))
        })?;
        w16(&mut meta, rid as u16);
        w16(&mut meta, *src_tid);
        w16(&mut meta, *dst_tid);
        w32(&mut meta, type_code);
        w32(&mut meta, rel_edge_counts[rid] as u32);
        let keys = &rel_col_keys[rid];
        w32(&mut meta, keys.len() as u32);
        for key in keys {
            let key_code = *string_index
                .get(key)
                .ok_or_else(|| GenerationError::Codec(format!("key not interned: {key}")))?;
            w32(&mut meta, key_code);
            // Rel table geometry uses table_id = 0x8000 | rid (edge tables).
            let kind = geo_by_key
                .get(&(0x8000 | rid as u16, key.clone()))
                .map(|g| codec_kind_of(g))
                .unwrap_or(crate::graph::compact::generation_builder::emit_meta::CodecKind::Dict);
            w16(&mut meta, kind.disc());
            w16(&mut meta, kind.value_type());
        }
    }

    w64(&mut meta, total_nodes);
    w64(&mut meta, total_edges);
    Ok(meta)
}

/// Builds the NodeIdLookup segment from a mapped ID index.
///
/// Records are sorted by original_id (the index is already sorted).
/// Layout per record (24 bytes): original_id u64 LE, table_id u16 LE,
/// pad u16, pad u32, offset u64 LE.
pub fn build_node_id_lookup(id_index: &MappedNodeIdIndex) -> Vec<u8> {
    let mut out = Vec::with_capacity(id_index.len() * 24);
    for i in 0..id_index.len() {
        if let Some((tid, off)) = id_index.lookup_at(i) {
            let id = id_index.original_id_at(i);
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&tid.to_le_bytes());
            out.extend_from_slice(&0u16.to_le_bytes()); // pad
            out.extend_from_slice(&0u32.to_le_bytes()); // pad
            out.extend_from_slice(&off.to_le_bytes());
        }
    }
    out
}

/// Builds the NodeOriginalIds segment (per-table, per-offset original IDs).
///
/// Layout: concatenated u64 LE original IDs, grouped by table in table order.
pub fn build_node_original_ids(id_index: &MappedNodeIdIndex, table_counts: &[u64]) -> Vec<u8> {
    // Collect (tid, off, id) and sort by (tid, off).
    let mut entries: Vec<(u16, u64, u64)> = Vec::with_capacity(id_index.len());
    for i in 0..id_index.len() {
        if let Some((tid, off)) = id_index.lookup_at(i) {
            entries.push((tid, off, id_index.original_id_at(i)));
        }
    }
    entries.sort_by_key(|&(tid, off, _)| (tid, off));
    let mut out = Vec::with_capacity(entries.len() * 8);
    for (_, _, id) in &entries {
        out.extend_from_slice(&id.to_le_bytes());
    }
    let _ = table_counts; // used for validation in full impl
    out
}

/// Builds the EdgeIdLookup segment from forward CSR records.
///
/// Records are sorted by original edge ID.
/// Layout per record (24 bytes): edge_id u64 LE, rel_table_id u16 LE,
/// pad u16, pad u32, csr_position u64 LE.
pub fn build_edge_id_lookup(
    fwd_lease: &RunSetLease,
    merger: &mut dyn crate::graph::compact::generation::ExternalRunMerger,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<Vec<u8>, GenerationError> {
    // Collect (edge_id, rel_table_id, csr_position) from forward CSR records.
    let mut entries: Vec<(u64, u16, u64)> = Vec::new();
    merger.merge_all(&fwd_lease.handles, budget, metrics, cancel, &mut |rec| {
        // Forward CSR key: rel_table_id u16 BE, src_off u64 BE, dst_off u64 BE, edge_id u64 BE.
        if rec.key.len() < 26 {
            return Err(GenerationError::Codec("forward CSR key too short".into()));
        }
        let rel_table_id = u16::from_be_bytes([rec.key[0], rec.key[1]]);
        let edge_id = u64::from_be_bytes(rec.key[18..26].try_into().unwrap());
        let csr_position = entries.len() as u64; // position in forward CSR order
        entries.push((edge_id, rel_table_id, csr_position));
        Ok(())
    })?;
    // Sort by edge_id.
    entries.sort_by_key(|&(id, _, _)| id);
    let mut out = Vec::with_capacity(entries.len() * 24);
    for (id, rel_table_id, csr_pos) in &entries {
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&rel_table_id.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // pad
        out.extend_from_slice(&0u32.to_le_bytes()); // pad
        out.extend_from_slice(&csr_pos.to_le_bytes());
    }
    Ok(out)
}

/// Builds the EdgeOriginalIds segment (per-rel-table, per-csr-position edge IDs).
///
/// Layout: concatenated u64 LE edge IDs, grouped by rel_table in rel_table order.
pub fn build_edge_original_ids(
    fwd_lease: &RunSetLease,
    merger: &mut dyn crate::graph::compact::generation::ExternalRunMerger,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<Vec<u8>, GenerationError> {
    // Collect (rel_table_id, csr_position, edge_id) from forward CSR records.
    let mut entries: Vec<(u16, u64, u64)> = Vec::new();
    merger.merge_all(&fwd_lease.handles, budget, metrics, cancel, &mut |rec| {
        if rec.key.len() < 26 {
            return Err(GenerationError::Codec("forward CSR key too short".into()));
        }
        let rel_table_id = u16::from_be_bytes([rec.key[0], rec.key[1]]);
        let edge_id = u64::from_be_bytes(rec.key[18..26].try_into().unwrap());
        let csr_position = entries.len() as u64;
        entries.push((rel_table_id, csr_position, edge_id));
        Ok(())
    })?;
    // Sort by (rel_table_id, csr_position).
    entries.sort_by_key(|&(rel, pos, _)| (rel, pos));
    let mut out = Vec::with_capacity(entries.len() * 8);
    for (_, _, id) in &entries {
        out.extend_from_slice(&id.to_le_bytes());
    }
    Ok(out)
}

/// Counts edges per rel_table from the forward CSR records.
///
/// Returns a vector indexed by rel_table_id with the edge count for each.
pub fn count_edges_per_rel_table(
    fwd_lease: &RunSetLease,
    merger: &mut dyn crate::graph::compact::generation::ExternalRunMerger,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
    n_rel_tables: usize,
) -> Result<Vec<u64>, GenerationError> {
    let mut counts = vec![0u64; n_rel_tables];
    merger.merge_all(&fwd_lease.handles, budget, metrics, cancel, &mut |rec| {
        if rec.key.len() < 26 {
            return Err(GenerationError::Codec("forward CSR key too short".into()));
        }
        let rel_table_id = u16::from_be_bytes([rec.key[0], rec.key[1]]) as usize;
        if rel_table_id < n_rel_tables {
            counts[rel_table_id] += 1;
        }
        Ok(())
    })?;
    Ok(counts)
}

/// Builds the TableZoneMaps segment from column geometries.
///
/// Layout per record (40 bytes, matching ZONE_MAP_RECORD_LEN):
/// table_id u16, reserved u16, col_key_code u32, block_index u32,
/// null_count u32, row_count u32, min_tag u8, max_tag u8, pad u16,
/// min_payload u64, max_payload u64.
pub fn build_table_zone_maps(
    geometries: &[ColumnGeometry],
    string_index: &FxHashMap<String, u32>,
) -> Result<Vec<u8>, GenerationError> {
    use crate::graph::compact::mapped::ZONE_MAP_RECORD_LEN;
    const TAG_ABSENT: u8 = 0;
    const TAG_INT64: u8 = 1;
    const TAG_BOOL: u8 = 2;
    const TAG_STRING_CODE: u8 = 3;
    const TAG_FLOAT64: u8 = 4;
    const TABLE_ZONE_BLOCK_SENTINEL: u32 = u32::MAX;

    let mut out = Vec::new();
    for g in geometries {
        let key_code = *string_index
            .get(&g.key)
            .ok_or_else(|| GenerationError::Codec(format!("zone key not interned: {}", g.key)))?;

        // Determine min/max tags and payloads from geometry.
        let (min_tag, min_payload) = if let Some(s) = &g.min_str {
            let code = *string_index
                .get(s)
                .ok_or_else(|| GenerationError::Codec(format!("zone min not interned: {s}")))?;
            (TAG_STRING_CODE, u64::from(code))
        } else if let Some(n) = g.min_int {
            (TAG_INT64, n as u64)
        } else if let Some(f) = g.min_float {
            (TAG_FLOAT64, f.to_bits())
        } else if g.saw_false || g.saw_true {
            (TAG_BOOL, u64::from(!g.saw_false)) // min = !has_false
        } else {
            (TAG_ABSENT, 0)
        };
        let (max_tag, max_payload) = if let Some(s) = &g.max_str {
            let code = *string_index
                .get(s)
                .ok_or_else(|| GenerationError::Codec(format!("zone max not interned: {s}")))?;
            (TAG_STRING_CODE, u64::from(code))
        } else if let Some(n) = g.max_int {
            (TAG_INT64, n as u64)
        } else if let Some(f) = g.max_float {
            (TAG_FLOAT64, f.to_bits())
        } else if g.saw_false || g.saw_true {
            (TAG_BOOL, u64::from(g.saw_true)) // max = has_true
        } else {
            (TAG_ABSENT, 0)
        };

        let mut rec = [0u8; ZONE_MAP_RECORD_LEN];
        rec[0..2].copy_from_slice(&g.table_id.to_le_bytes());
        rec[2..4].copy_from_slice(&0u16.to_le_bytes()); // reserved
        rec[4..8].copy_from_slice(&key_code.to_le_bytes());
        rec[8..12].copy_from_slice(&TABLE_ZONE_BLOCK_SENTINEL.to_le_bytes());
        rec[12..16].copy_from_slice(&(g.null_count as u32).to_le_bytes());
        rec[16..20].copy_from_slice(&(g.row_count as u32).to_le_bytes());
        rec[20] = min_tag;
        rec[21] = max_tag;
        rec[22..24].copy_from_slice(&0u16.to_le_bytes()); // pad
        rec[24..32].copy_from_slice(&min_payload.to_le_bytes());
        rec[32..40].copy_from_slice(&max_payload.to_le_bytes());
        out.extend_from_slice(&rec);
    }
    Ok(out)
}

/// Builds the BlockZoneMaps segment from per-column per-block zone maps.
///
/// Each `EmittedColumn` carries `block_zone_maps` computed from its codec.
/// Layout per record (40 bytes): same as table zone maps but with a real
/// `block_index` (0, 1, 2, …) instead of the sentinel.
pub fn build_block_zone_maps(
    columns: &[crate::graph::compact::generation_builder::emit_columns::EmittedColumn],
    geometries: &[ColumnGeometry],
    string_index: &FxHashMap<String, u32>,
) -> Result<Vec<u8>, GenerationError> {
    use crate::graph::compact::mapped::ZONE_MAP_RECORD_LEN;
    const TAG_ABSENT: u8 = 0;
    const TAG_INT64: u8 = 1;
    const TAG_BOOL: u8 = 2;
    const TAG_STRING_CODE: u8 = 3;
    const TAG_FLOAT64: u8 = 4;

    let mut out = Vec::new();
    for (i, col) in columns.iter().enumerate() {
        let g = &geometries[i];
        let key_code = *string_index
            .get(&g.key)
            .ok_or_else(|| GenerationError::Codec(format!("block zone key not interned: {}", g.key)))?;
        for (block_idx, zm) in col.block_zone_maps.iter().enumerate() {
            let bi = u32::try_from(block_idx).map_err(|_| {
                GenerationError::WireWidthOverflow {
                    what: "block_index",
                    count: block_idx as u64,
                    max: u64::from(u32::MAX),
                }
            })?;
            let (min_tag, min_payload) = encode_zone_value(&zm.min, string_index)?;
            let (max_tag, max_payload) = encode_zone_value(&zm.max, string_index)?;
            let mut rec = [0u8; ZONE_MAP_RECORD_LEN];
            rec[0..2].copy_from_slice(&g.table_id.to_le_bytes());
            rec[2..4].copy_from_slice(&0u16.to_le_bytes()); // reserved
            rec[4..8].copy_from_slice(&key_code.to_le_bytes());
            rec[8..12].copy_from_slice(&bi.to_le_bytes());
            rec[12..16].copy_from_slice(&(zm.null_count as u32).to_le_bytes());
            rec[16..20].copy_from_slice(&(zm.row_count as u32).to_le_bytes());
            rec[20] = min_tag;
            rec[21] = max_tag;
            rec[22..24].copy_from_slice(&0u16.to_le_bytes()); // pad
            rec[24..32].copy_from_slice(&min_payload.to_le_bytes());
            rec[32..40].copy_from_slice(&max_payload.to_le_bytes());
            out.extend_from_slice(&rec);
        }
    }
    Ok(out)
}

/// Encodes an optional zone-map value to (tag, payload).
fn encode_zone_value(
    v: &Option<grafeo_common::types::Value>,
    string_index: &FxHashMap<String, u32>,
) -> Result<(u8, u64), GenerationError> {
    const TAG_ABSENT: u8 = 0;
    const TAG_INT64: u8 = 1;
    const TAG_BOOL: u8 = 2;
    const TAG_STRING_CODE: u8 = 3;
    const TAG_FLOAT64: u8 = 4;
    match v {
        None => Ok((TAG_ABSENT, 0)),
        Some(grafeo_common::types::Value::Int64(n)) => Ok((TAG_INT64, *n as u64)),
        Some(grafeo_common::types::Value::Bool(b)) => Ok((TAG_BOOL, u64::from(*b))),
        Some(grafeo_common::types::Value::String(s)) => {
            let code = *string_index
                .get(s.as_str())
                .ok_or_else(|| GenerationError::Codec(format!("zone string not interned: {s}")))?;
            Ok((TAG_STRING_CODE, u64::from(code)))
        }
        Some(grafeo_common::types::Value::Float64(f)) => Ok((TAG_FLOAT64, f.to_bits())),
        Some(_) => Ok((TAG_ABSENT, 0)),
    }
}
