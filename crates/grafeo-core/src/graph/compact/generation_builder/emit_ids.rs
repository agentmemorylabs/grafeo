//! Bounded emission: metadata + ID lookups + zone maps (G-EM0.5b Phase 2b).
//!
//! Builds the Metadata, NodeIdLookup, EdgeIdLookup, NodeOriginalIds,
//! EdgeOriginalIds, TableZoneMaps, and BlockZoneMaps segments from bounded
//! pass outputs. Layout is byte-exact with `emit_canonical_descriptors`.
//!
//! Boundedness: the four ID segments are **streamed** into spool sinks.
//! NodeIdLookup follows the (already sorted) mapped index; the other three
//! re-sort fixed-width records through the external-run machinery and never
//! retain a graph-proportional vector. Zone-map string bounds resolve
//! through the per-column DictValue chunk file, one column's map at a time.

use crate::graph::compact::generation::emit::sink::SegmentSink;
use crate::graph::compact::generation::{
    CancelToken, ExternalRunMerger, GenerationBudget, GenerationError, GenerationMetrics,
    RunSetLease, RunStore, SortRecord,
};
use crate::graph::compact::generation_builder::column_pass::ColumnGeometry;
use crate::graph::compact::generation::emit::dict_column_lookup::DictChunkCatalog;
use crate::graph::compact::generation_builder::emit_columns::codec_kind_of;
use crate::graph::compact::generation_builder::emit_meta::{w16, w32, w64};
use crate::graph::compact::mapped::id_index::MappedNodeIdIndex;
use crate::graph::compact::generation::emit::dict_column_lookup::DictCodeLookup;
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

/// Streams the NodeIdLookup segment into `sink`.
///
/// Records are sorted by original_id (the mapped index is already sorted).
/// Layout per record (24 bytes): original_id u64 LE, table_id u16 LE,
/// pad u16, pad u32, offset u64 LE. No graph-proportional resident buffer.
///
/// # Errors
///
/// Sink write failure.
pub fn build_node_id_lookup(
    id_index: &MappedNodeIdIndex,
    sink: &mut dyn SegmentSink,
) -> Result<(), GenerationError> {
    let mut rec = [0u8; 24];
    for i in 0..id_index.len() {
        if let Some((tid, off)) = id_index.lookup_at(i) {
            let id = id_index.original_id_at(i);
            rec[0..8].copy_from_slice(&id.to_le_bytes());
            rec[8..10].copy_from_slice(&tid.to_le_bytes());
            rec[10..12].copy_from_slice(&0u16.to_le_bytes()); // pad
            rec[12..16].copy_from_slice(&0u32.to_le_bytes()); // pad
            rec[16..24].copy_from_slice(&off.to_le_bytes());
            sink.write(&rec)?;
        }
    }
    Ok(())
}

/// Streams the NodeOriginalIds segment (per-table, per-offset original IDs)
/// into `sink`.
///
/// Layout: concatenated u64 LE original IDs, grouped by table in table
/// order. The index is not in `(table_id, dense_offset)` order, so records
/// are re-sorted externally (key = `table_id u16 BE || dense_offset u64 BE`,
/// payload = original_id u64 LE); the merged stream is written straight to
/// the spool sink.
///
/// # Errors
///
/// Codec, budget, or I/O failure.
pub fn build_node_original_ids(
    id_index: &MappedNodeIdIndex,
    run_store: &mut dyn RunStore,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
    sink: &mut dyn SegmentSink,
) -> Result<(), GenerationError> {
    let mut run_sink = run_store.sink("node-orig", budget)?;
    for i in 0..id_index.len() {
        if let Some((tid, off)) = id_index.lookup_at(i) {
            let mut key = Vec::with_capacity(10);
            key.extend_from_slice(&tid.to_be_bytes());
            key.extend_from_slice(&off.to_be_bytes());
            run_sink.push(SortRecord::new(
                key,
                id_index.original_id_at(i).to_le_bytes().to_vec(),
            ))?;
        }
    }
    let lease = run_sink.finish()?;
    let mut merger = run_store.merger("node-orig")?;
    merger.merge_all(&lease.handles, budget, metrics, cancel, &mut |rec| {
        if rec.payload.len() != 8 {
            return Err(GenerationError::Codec(
                "node original id payload width".into(),
            ));
        }
        sink.write(&rec.payload)
    })?;
    Ok(())
}

/// Extracts the original edge id from a forward CSR record key.
///
/// Key layout: `rel_table_id u16 BE, src_off u64 BE, dst_off u64 BE,
/// edge_id u64 BE`.
///
/// # Errors
///
/// [`GenerationError::Codec`] when the key is too short.
fn forward_csr_edge_id(rec: &SortRecord) -> Result<u64, GenerationError> {
    if rec.key.len() < 26 {
        return Err(GenerationError::Codec("forward CSR key too short".into()));
    }
    Ok(u64::from_be_bytes(
        rec.key[18..26]
            .try_into()
            .map_err(|_| GenerationError::Codec("forward CSR key width".into()))?,
    ))
}

/// Streams the EdgeIdLookup segment from forward CSR records into `sink`.
///
/// Records are sorted by original edge ID. Layout per record (24 bytes):
/// edge_id u64 LE, rel_table_id u16 LE, pad u16, pad u32, csr_position u64
/// LE. The CSR stream yields records in forward-CSR order, so each record's
/// `csr_position` is its running index; the fixed-width records are then
/// re-sorted externally by `edge_id` (key = `edge_id u64 BE || rel_table_id
/// u16 BE || position u64 BE`) and merged straight into the spool sink.
///
/// # Errors
///
/// Codec, budget, or I/O failure.
pub fn build_edge_id_lookup(
    fwd_lease: &RunSetLease,
    merger: &mut dyn ExternalRunMerger,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
    run_store: &mut dyn RunStore,
    sink: &mut dyn SegmentSink,
) -> Result<(), GenerationError> {
    let mut run_sink = run_store.sink("edge-lookup", budget)?;
    let mut pos: u64 = 0;
    merger.merge_all(&fwd_lease.handles, budget, metrics, cancel, &mut |rec| {
        // Forward CSR key: rel_table_id u16 BE, src_off u64 BE, dst_off u64 BE, edge_id u64 BE.
        let edge_id = forward_csr_edge_id(rec)?;
        let rel_table_id = u16::from_be_bytes([rec.key[0], rec.key[1]]);
        let mut key = Vec::with_capacity(26);
        key.extend_from_slice(&edge_id.to_be_bytes());
        key.extend_from_slice(&rel_table_id.to_be_bytes());
        key.extend_from_slice(&pos.to_be_bytes());
        let mut payload = Vec::with_capacity(18);
        payload.extend_from_slice(&rel_table_id.to_le_bytes());
        payload.extend_from_slice(&pos.to_le_bytes());
        run_sink.push(SortRecord::new(key, payload))?;
        pos += 1;
        Ok(())
    })?;
    let lease = run_sink.finish()?;
    let mut merger2 = run_store.merger("edge-lookup")?;
    merger2.merge_all(&lease.handles, budget, metrics, cancel, &mut |rec| {
        if rec.payload.len() != 10 {
            return Err(GenerationError::Codec("edge lookup payload width".into()));
        }
        let edge_id = u64::from_be_bytes(
            rec.key
                .get(0..8)
                .ok_or_else(|| GenerationError::Codec("edge lookup key short".into()))?
                .try_into()
                .map_err(|_| GenerationError::Codec("edge lookup key width".into()))?,
        );
        let mut out = [0u8; 24];
        out[0..8].copy_from_slice(&edge_id.to_le_bytes());
        out[8..10].copy_from_slice(&rec.payload[0..2]); // rel_table_id LE
        out[10..12].copy_from_slice(&0u16.to_le_bytes()); // pad
        out[12..16].copy_from_slice(&0u32.to_le_bytes()); // pad
        out[16..24].copy_from_slice(&rec.payload[2..10]); // csr_position LE
        sink.write(&out)
    })?;
    Ok(())
}

/// Streams the EdgeOriginalIds segment (per-rel-table, per-csr-position edge
/// IDs) into `sink`.
///
/// Layout: concatenated u64 LE edge IDs, grouped by rel_table in rel_table
/// order. Records are re-sorted externally (key = `rel_table_id u16 BE ||
/// csr_position u64 BE`, payload = edge_id u64 LE) and the merged stream is
/// written straight to the spool sink.
///
/// # Errors
///
/// Codec, budget, or I/O failure.
pub fn build_edge_original_ids(
    fwd_lease: &RunSetLease,
    merger: &mut dyn ExternalRunMerger,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
    run_store: &mut dyn RunStore,
    sink: &mut dyn SegmentSink,
) -> Result<(), GenerationError> {
    let mut run_sink = run_store.sink("edge-orig", budget)?;
    let mut pos: u64 = 0;
    merger.merge_all(&fwd_lease.handles, budget, metrics, cancel, &mut |rec| {
        let rel_table_id = u16::from_be_bytes([rec.key[0], rec.key[1]]);
        let edge_id = forward_csr_edge_id(rec)?;
        let mut key = Vec::with_capacity(10);
        key.extend_from_slice(&rel_table_id.to_be_bytes());
        key.extend_from_slice(&pos.to_be_bytes());
        run_sink.push(SortRecord::new(key, edge_id.to_le_bytes().to_vec()))?;
        pos += 1;
        Ok(())
    })?;
    let lease = run_sink.finish()?;
    let mut merger2 = run_store.merger("edge-orig")?;
    merger2.merge_all(&lease.handles, budget, metrics, cancel, &mut |rec| {
        if rec.payload.len() != 8 {
            return Err(GenerationError::Codec(
                "edge original id payload width".into(),
            ));
        }
        sink.write(&rec.payload)
    })?;
    Ok(())
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
pub(crate) fn build_table_zone_maps(
    geometries: &[ColumnGeometry],
    string_index: &FxHashMap<String, u32>,
    chunks: &mut DictChunkCatalog,
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

        // String bounds resolve through this column's DictValue chunk map
        // (loaded only when the column has string bounds, discarded after).
        let mut str_lookup: Option<Box<dyn DictCodeLookup>> = None;
        if g.min_str.is_some() || g.max_str.is_some() {
            str_lookup = chunks
                .lookup_for(g.table_id, &g.key)?
                .map(|lk| Box::new(lk) as Box<dyn DictCodeLookup>);
        }

        // Determine min/max tags and payloads from geometry.
        let (min_tag, min_payload) = if let Some(s) = &g.min_str {
            let code = lookup_zone_str(&mut str_lookup, s)?;
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
            let code = lookup_zone_str(&mut str_lookup, s)?;
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
pub(crate) fn build_block_zone_maps(
    columns: &[crate::graph::compact::generation_builder::emit_columns::EmittedColumn],
    geometries: &[ColumnGeometry],
    string_index: &FxHashMap<String, u32>,
    chunks: &mut DictChunkCatalog,
) -> Result<Vec<u8>, GenerationError> {
    use crate::graph::compact::mapped::ZONE_MAP_RECORD_LEN;

    let mut out = Vec::new();
    for (i, col) in columns.iter().enumerate() {
        let g = &geometries[i];
        let key_code = *string_index.get(&g.key).ok_or_else(|| {
            GenerationError::Codec(format!("block zone key not interned: {}", g.key))
        })?;
        // Load this column's DictValue chunk map only when some block bound
        // is a string; discarded after the column.
        let mut str_lookup: Option<Box<dyn DictCodeLookup>> = None;
        if col.block_zone_maps.iter().any(|zm| {
            matches!(zm.min, Some(grafeo_common::types::Value::String(_)))
                || matches!(zm.max, Some(grafeo_common::types::Value::String(_)))
        }) {
            str_lookup = chunks
                .lookup_for(g.table_id, &g.key)?
                .map(|lk| Box::new(lk) as Box<dyn DictCodeLookup>);
        }
        for (block_idx, zm) in col.block_zone_maps.iter().enumerate() {
            let bi = u32::try_from(block_idx).map_err(|_| GenerationError::WireWidthOverflow {
                what: "block_index",
                count: block_idx as u64,
                max: u64::from(u32::MAX),
            })?;
            let (min_tag, min_payload) = encode_zone_value(&zm.min, &mut str_lookup)?;
            let (max_tag, max_payload) = encode_zone_value(&zm.max, &mut str_lookup)?;
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

/// Resolves a zone-map string bound through the column's chunk map.
///
/// # Errors
///
/// [`GenerationError::Codec`] when the string is not interned or the map is
/// missing (fail closed: the bound must be a value of this column).
fn lookup_zone_str(
    str_lookup: &mut Option<Box<dyn DictCodeLookup>>,
    s: &str,
) -> Result<u32, GenerationError> {
    let lookup = str_lookup
        .as_mut()
        .ok_or_else(|| GenerationError::Codec("zone string lookup not loaded".into()))?;
    lookup.code_of(s.as_bytes()).ok_or_else(|| {
        GenerationError::Codec(format!("zone string not interned: {s}"))
    })
}

/// Encodes an optional zone-map value to (tag, payload).
fn encode_zone_value(
    v: &Option<grafeo_common::types::Value>,
    str_lookup: &mut Option<Box<dyn DictCodeLookup>>,
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
            let code = lookup_zone_str(str_lookup, s.as_str())?;
            Ok((TAG_STRING_CODE, u64::from(code)))
        }
        Some(grafeo_common::types::Value::Float64(f)) => Ok((TAG_FLOAT64, f.to_bits())),
        Some(_) => Ok((TAG_ABSENT, 0)),
    }
}
