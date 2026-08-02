//! Canonical v5 segment emitter (G-EM0.5b Phase 1).
//!
//! One implementation of CompactStore → ordered `SegmentDescriptor`s. Both
//! `serialize_v5_with_string_order` (feature ON) and `emit_v5_segments`
//! (feature ON) delegate here; the eager paths remain for feature OFF.
//!
//! Phase 1 uses `MemorySegmentSink` (compatibility). Phase 2's streaming
//! builder will drive the same logic through `SpoolSegmentSink`.

#![allow(clippy::cast_possible_truncation)]

use super::descriptor::SegmentDescriptor;
use super::sink::{MemorySegmentSink, SegmentSink};
use crate::graph::compact::CompactStore;
use crate::graph::compact::generation::error::GenerationError;
use crate::graph::compact::mapped::{
    SegmentKind, ZONE_MAP_RECORD_LEN, build_dictionary_code_index, build_string_segments,
    build_zone_map_segments, write_edge_id_record, write_node_id_record,
};
use crate::graph::compact::node_table::NodeTable;
use crate::graph::compact::rel_table::RelTable;
use crate::graph::compact::section_v5::{
    append_u32_array, codec_disc, value_type_code, write_column_body, write_u16, write_u32,
    write_u64,
};
use grafeo_common::utils::hash::FxHashMap;

/// Emits all v5 segments from a `CompactStore` and pre-built string index as
/// ordered `SegmentDescriptor`s (ascending `SegmentKind`).
///
/// This is the single canonical segment-building implementation. Callers:
/// - `serialize_v5_with_string_order` (feature ON) → descriptors → assembler
/// - `emit_v5_segments` (feature ON) → descriptors → `V5Segment` conversion
///
/// # Errors
///
/// Returns `GenerationError` on wire-width overflow, codec failure, or
/// missing string index entries.
///
/// # Panics
///
/// Panics if a column key present in a table's column map is missing from
/// that map on re-lookup (internal invariant violation — keys are iterated
/// from the same map).
pub fn emit_canonical_descriptors(
    store: &CompactStore,
    string_index: &FxHashMap<String, u32>,
    str_refs: &[&str],
) -> Result<Vec<SegmentDescriptor>, GenerationError> {
    let mut descriptors: Vec<SegmentDescriptor> = Vec::new();

    // Helper: build a MemorySegmentSink, write bytes, finish → descriptor.
    let emit = |kind: SegmentKind,
                encoding_version: u16,
                flags: u16,
                alignment: u16,
                element_width: u32,
                bytes: &[u8]|
     -> Result<SegmentDescriptor, GenerationError> {
        let mut sink = Box::new(MemorySegmentSink::new(
            kind,
            encoding_version,
            flags,
            alignment,
            element_width,
        ));
        sink.write(bytes)?;
        sink.finish()
    };

    // ── Metadata ────────────────────────────────────────────────────────
    let mut meta = Vec::new();
    let node_table_count = u32::try_from(store.node_tables_by_id.len()).map_err(|_| {
        GenerationError::WireWidthOverflow {
            what: "node_table_count",
            count: store.node_tables_by_id.len() as u64,
            max: u64::from(u32::MAX),
        }
    })?;
    write_u32(&mut meta, node_table_count);

    for (tid_usize, nt) in store.node_tables_by_id.iter().enumerate() {
        let tid = u16::try_from(tid_usize).map_err(|_| GenerationError::WireWidthOverflow {
            what: "node_table_id",
            count: tid_usize as u64,
            max: u64::from(u16::MAX),
        })?;
        write_u16(&mut meta, tid);
        let label_code = *string_index.get(nt.label()).ok_or_else(|| {
            GenerationError::Codec(format!("label string not interned: {}", nt.label()))
        })?;
        write_u32(&mut meta, label_code);
        let row_count =
            u32::try_from(nt.len()).map_err(|_| GenerationError::WireWidthOverflow {
                what: "node_table_row_count",
                count: nt.len() as u64,
                max: u64::from(u32::MAX),
            })?;
        write_u32(&mut meta, row_count);
        let cols = nt.columns();
        let col_count =
            u32::try_from(cols.len()).map_err(|_| GenerationError::WireWidthOverflow {
                what: "node_table_col_count",
                count: cols.len() as u64,
                max: u64::from(u32::MAX),
            })?;
        write_u32(&mut meta, col_count);
        let mut keys: Vec<_> = cols.keys().cloned().collect();
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for key in &keys {
            let key_code = *string_index
                .get(key.as_str())
                .ok_or_else(|| GenerationError::Codec(format!("prop key not interned: {key}")))?;
            write_u32(&mut meta, key_code);
            let codec = cols.get(key).unwrap();
            write_u16(&mut meta, codec_disc(codec));
            write_u16(&mut meta, value_type_code(codec));
        }
    }

    let rel_table_count = u32::try_from(store.rel_tables_by_id.len()).map_err(|_| {
        GenerationError::WireWidthOverflow {
            what: "rel_table_count",
            count: store.rel_tables_by_id.len() as u64,
            max: u64::from(u32::MAX),
        }
    })?;
    write_u32(&mut meta, rel_table_count);

    for (rid_usize, rt) in store.rel_tables_by_id.iter().enumerate() {
        let rid = u16::try_from(rid_usize).map_err(|_| GenerationError::WireWidthOverflow {
            what: "rel_table_id",
            count: rid_usize as u64,
            max: u64::from(u16::MAX),
        })?;
        write_u16(&mut meta, rid);
        write_u16(&mut meta, rt.src_table_id());
        write_u16(&mut meta, rt.dst_table_id());
        let type_code = *string_index.get(rt.edge_type().as_str()).ok_or_else(|| {
            GenerationError::Codec(format!("edge type not interned: {}", rt.edge_type()))
        })?;
        write_u32(&mut meta, type_code);
        let edge_count =
            u32::try_from(rt.num_edges()).map_err(|_| GenerationError::WireWidthOverflow {
                what: "rel_table_edge_count",
                count: rt.num_edges() as u64,
                max: u64::from(u32::MAX),
            })?;
        write_u32(&mut meta, edge_count);
        let props = rt.properties();
        let prop_count =
            u32::try_from(props.len()).map_err(|_| GenerationError::WireWidthOverflow {
                what: "rel_table_prop_count",
                count: props.len() as u64,
                max: u64::from(u32::MAX),
            })?;
        write_u32(&mut meta, prop_count);
        let mut keys: Vec<_> = props.keys().cloned().collect();
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for key in &keys {
            let key_code = *string_index
                .get(key.as_str())
                .ok_or_else(|| GenerationError::Codec(format!("prop key not interned: {key}")))?;
            write_u32(&mut meta, key_code);
            let codec = props.get(key).unwrap();
            write_u16(&mut meta, codec_disc(codec));
            write_u16(&mut meta, value_type_code(codec));
        }
    }

    let total_nodes = store
        .node_tables_by_id
        .iter()
        .map(NodeTable::len)
        .sum::<usize>() as u64;
    let total_edges = store
        .rel_tables_by_id
        .iter()
        .map(RelTable::num_edges)
        .sum::<usize>() as u64;
    write_u64(&mut meta, total_nodes);
    write_u64(&mut meta, total_edges);

    descriptors.push(emit(SegmentKind::Metadata, 1, 0x0001, 1, 0, &meta)?);

    // ── StringOffsets & StringBytes ─────────────────────────────────────
    let (off_bytes, str_bytes) = build_string_segments(str_refs);
    descriptors.push(emit(
        SegmentKind::StringOffsets,
        1,
        0x0001,
        8,
        8,
        &off_bytes,
    )?);
    descriptors.push(emit(SegmentKind::StringBytes, 1, 0x0001, 1, 1, &str_bytes)?);

    // ── Directories, Columns, CSR ───────────────────────────────────────
    let mut node_dir = Vec::new();
    let mut rel_dir = Vec::new();
    let mut col_dir = Vec::new();
    let mut col_block_index = Vec::new();
    let mut col_bodies = Vec::new();
    let mut fwd_offsets = Vec::new();
    let mut fwd_targets = Vec::new();
    let mut rev_offsets = Vec::new();
    let mut rev_targets = Vec::new();
    let mut fwd_positions = Vec::new();
    let mut has_reverse = false;
    let mut column_index: u32 = 0;

    for (tid_usize, nt) in store.node_tables_by_id.iter().enumerate() {
        let tid = u16::try_from(tid_usize).map_err(|_| GenerationError::WireWidthOverflow {
            what: "node_table_id",
            count: tid_usize as u64,
            max: u64::from(u16::MAX),
        })?;
        let col_start = column_index;
        let mut keys: Vec<_> = nt.columns().keys().cloned().collect();
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for key in &keys {
            let codec = nt.columns().get(key).unwrap();
            let body_start = u32::try_from(col_bodies.len()).map_err(|_| {
                GenerationError::WireWidthOverflow {
                    what: "col_body_offset",
                    count: col_bodies.len() as u64,
                    max: u64::from(u32::MAX),
                }
            })?;
            write_column_body(&mut col_bodies, codec, string_index)
                .map_err(GenerationError::Codec)?;
            let body_len = (u32::try_from(col_bodies.len()).map_err(|_| {
                GenerationError::WireWidthOverflow {
                    what: "col_body_offset",
                    count: col_bodies.len() as u64,
                    max: u64::from(u32::MAX),
                }
            })?)
            .saturating_sub(body_start);
            write_u16(&mut col_dir, codec_disc(codec));
            write_u16(&mut col_dir, value_type_code(codec));
            write_u32(&mut col_dir, column_index);
            write_u32(&mut col_dir, 1);
            write_u64(&mut col_dir, codec.len() as u64);
            write_u32(&mut col_dir, 0);
            write_u32(&mut col_block_index, body_start);
            write_u32(&mut col_block_index, body_len);
            let codec_len =
                u32::try_from(codec.len()).map_err(|_| GenerationError::WireWidthOverflow {
                    what: "codec_len",
                    count: codec.len() as u64,
                    max: u64::from(u32::MAX),
                })?;
            write_u32(&mut col_block_index, codec_len);
            column_index += 1;
        }
        write_u16(&mut node_dir, tid);
        write_u16(&mut node_dir, 0);
        write_u32(&mut node_dir, col_start);
        let key_count =
            u32::try_from(keys.len()).map_err(|_| GenerationError::WireWidthOverflow {
                what: "node_table_key_count",
                count: keys.len() as u64,
                max: u64::from(u32::MAX),
            })?;
        write_u32(&mut node_dir, key_count);
        write_u64(&mut node_dir, nt.len() as u64);
        write_u32(&mut node_dir, 0);
    }

    for (rid_usize, rt) in store.rel_tables_by_id.iter().enumerate() {
        let rid = u16::try_from(rid_usize).map_err(|_| GenerationError::WireWidthOverflow {
            what: "rel_table_id",
            count: rid_usize as u64,
            max: u64::from(u16::MAX),
        })?;
        let col_start = column_index;
        let mut keys: Vec<_> = rt.properties().keys().cloned().collect();
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for key in &keys {
            let codec = rt.properties().get(key).unwrap();
            let body_start = u32::try_from(col_bodies.len()).map_err(|_| {
                GenerationError::WireWidthOverflow {
                    what: "col_body_offset",
                    count: col_bodies.len() as u64,
                    max: u64::from(u32::MAX),
                }
            })?;
            write_column_body(&mut col_bodies, codec, string_index)
                .map_err(GenerationError::Codec)?;
            let body_len = (u32::try_from(col_bodies.len()).map_err(|_| {
                GenerationError::WireWidthOverflow {
                    what: "col_body_offset",
                    count: col_bodies.len() as u64,
                    max: u64::from(u32::MAX),
                }
            })?)
            .saturating_sub(body_start);
            write_u16(&mut col_dir, codec_disc(codec));
            write_u16(&mut col_dir, value_type_code(codec));
            write_u32(&mut col_dir, column_index);
            write_u32(&mut col_dir, 1);
            write_u64(&mut col_dir, codec.len() as u64);
            write_u32(&mut col_dir, 0);
            write_u32(&mut col_block_index, body_start);
            write_u32(&mut col_block_index, body_len);
            let codec_len =
                u32::try_from(codec.len()).map_err(|_| GenerationError::WireWidthOverflow {
                    what: "codec_len",
                    count: codec.len() as u64,
                    max: u64::from(u32::MAX),
                })?;
            write_u32(&mut col_block_index, codec_len);
            column_index += 1;
        }

        write_u16(&mut rel_dir, rid);
        write_u16(&mut rel_dir, rt.src_table_id());
        write_u16(&mut rel_dir, rt.dst_table_id());
        write_u16(&mut rel_dir, 0);
        write_u32(&mut rel_dir, col_start);
        let key_count =
            u32::try_from(keys.len()).map_err(|_| GenerationError::WireWidthOverflow {
                what: "rel_table_key_count",
                count: keys.len() as u64,
                max: u64::from(u32::MAX),
            })?;
        write_u32(&mut rel_dir, key_count);
        write_u64(&mut rel_dir, rt.num_edges() as u64);

        let fwd = rt.fwd();
        append_u32_array(&mut fwd_offsets, &fwd.offsets());
        append_u32_array(&mut fwd_targets, &fwd.targets());
        if let Some(bwd) = rt.bwd() {
            has_reverse = true;
            append_u32_array(&mut rev_offsets, &bwd.offsets());
            append_u32_array(&mut rev_targets, &bwd.targets());
            if let Some(ed) = bwd.edge_data() {
                append_u32_array(&mut fwd_positions, &ed);
            } else {
                for _ in 0..bwd.num_edges() {
                    write_u32(&mut fwd_positions, 0);
                }
            }
        }
    }

    descriptors.push(emit(
        SegmentKind::NodeTableDirectory,
        1,
        0x0001,
        8,
        24,
        &node_dir,
    )?);
    descriptors.push(emit(
        SegmentKind::RelTableDirectory,
        1,
        0x0001,
        8,
        24,
        &rel_dir,
    )?);
    descriptors.push(emit(
        SegmentKind::ColumnDirectory,
        1,
        0x0001,
        8,
        24,
        &col_dir,
    )?);
    descriptors.push(emit(
        SegmentKind::ColumnBlockIndex,
        1,
        0x0001,
        4,
        12,
        &col_block_index,
    )?);
    descriptors.push(emit(
        SegmentKind::ColumnBodies,
        1,
        0x0001,
        1,
        0,
        &col_bodies,
    )?);
    descriptors.push(emit(
        SegmentKind::ForwardCsrOffsets,
        1,
        0x0001,
        4,
        4,
        &fwd_offsets,
    )?);
    descriptors.push(emit(
        SegmentKind::ForwardCsrTargets,
        1,
        0x0001,
        4,
        4,
        &fwd_targets,
    )?);

    if has_reverse {
        descriptors.push(emit(
            SegmentKind::ReverseCsrOffsets,
            1,
            0x0001,
            4,
            4,
            &rev_offsets,
        )?);
        descriptors.push(emit(
            SegmentKind::ReverseCsrTargets,
            1,
            0x0001,
            4,
            4,
            &rev_targets,
        )?);
        descriptors.push(emit(
            SegmentKind::ForwardPositions,
            1,
            0x0001,
            4,
            4,
            &fwd_positions,
        )?);
    }

    // ── ID lookups ──────────────────────────────────────────────────────
    if store.preserves_ids() {
        let mut node_lookup = Vec::new();
        let mut edge_lookup = Vec::new();
        let mut node_orig = Vec::new();
        let mut edge_orig = Vec::new();

        if let Some(ref map) = store.node_id_map {
            let mut entries: Vec<_> = map.iter().map(|(&id, &v)| (id.as_u64(), v)).collect();
            entries.sort_by_key(|(id, _)| *id);
            for (id, (tid, off)) in entries {
                write_node_id_record(&mut node_lookup, id, tid, off);
            }
        }
        if let Some(ref rev) = store.node_offset_to_id {
            for table in rev {
                for id in table {
                    write_u64(&mut node_orig, id.as_u64());
                }
            }
        }
        if let Some(ref map) = store.edge_id_map {
            let mut entries: Vec<_> = map.iter().map(|(&id, &v)| (id.as_u64(), v)).collect();
            entries.sort_by_key(|(id, _)| *id);
            for (id, (rid, pos)) in entries {
                write_edge_id_record(&mut edge_lookup, id, rid, pos);
            }
        }
        if let Some(ref rev) = store.edge_offset_to_id {
            for table in rev {
                for id in table {
                    write_u64(&mut edge_orig, id.as_u64());
                }
            }
        }
        descriptors.push(emit(
            SegmentKind::NodeIdLookup,
            1,
            0x0001,
            8,
            24,
            &node_lookup,
        )?);
        descriptors.push(emit(
            SegmentKind::EdgeIdLookup,
            1,
            0x0001,
            8,
            24,
            &edge_lookup,
        )?);
        descriptors.push(emit(
            SegmentKind::NodeOriginalIds,
            1,
            0x0001,
            8,
            8,
            &node_orig,
        )?);
        descriptors.push(emit(
            SegmentKind::EdgeOriginalIds,
            1,
            0x0001,
            8,
            8,
            &edge_orig,
        )?);
    }

    // ── Zone maps & dictionary code index ───────────────────────────────
    let (table_zm, block_zm) =
        build_zone_map_segments(store, string_index).map_err(GenerationError::Codec)?;
    if !table_zm.is_empty() {
        let rec_len =
            u32::try_from(ZONE_MAP_RECORD_LEN).map_err(|_| GenerationError::WireWidthOverflow {
                what: "zone_map_rec_len",
                count: ZONE_MAP_RECORD_LEN as u64,
                max: u64::from(u32::MAX),
            })?;
        descriptors.push(emit(
            SegmentKind::TableZoneMaps,
            1,
            0x0001,
            8,
            rec_len,
            &table_zm,
        )?);
    }
    if !block_zm.is_empty() {
        let rec_len =
            u32::try_from(ZONE_MAP_RECORD_LEN).map_err(|_| GenerationError::WireWidthOverflow {
                what: "zone_map_rec_len",
                count: ZONE_MAP_RECORD_LEN as u64,
                max: u64::from(u32::MAX),
            })?;
        descriptors.push(emit(
            SegmentKind::BlockZoneMaps,
            1,
            0x0001,
            8,
            rec_len,
            &block_zm,
        )?);
    }

    let code_index_body = build_dictionary_code_index(str_refs);
    if !code_index_body.is_empty() {
        descriptors.push(emit(
            SegmentKind::DictionaryCodeIndex,
            1,
            0x0001,
            8,
            16,
            &code_index_body,
        )?);
    }

    // Sort strictly by segment kind ascending.
    descriptors.sort_by_key(|d| d.kind.as_u16());
    Ok(descriptors)
}
