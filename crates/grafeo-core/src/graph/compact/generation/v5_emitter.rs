//! Production v5 segment emission for core V5SegmentSource (G-EM0.W0-A3).

use super::error::GenerationError;
use super::segment_source::V5Segment;
use super::strings::GlobalStringDictionary;
use crate::graph::compact::CompactStore;
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

/// Emits all individual `V5Segment` objects for a `CompactStore` and `GlobalStringDictionary`
/// in strictly ascending `SegmentKind` order.
///
/// # Errors
///
/// Returns `GenerationError` if string lookup or wire width validation fails.
pub fn emit_v5_segments(
    store: &CompactStore,
    global_strings: &GlobalStringDictionary,
) -> Result<Vec<V5Segment>, GenerationError> {
    let str_slice = global_strings.as_slice();
    let mut string_index: FxHashMap<String, u32> = FxHashMap::default();
    for (idx, s) in str_slice.iter().enumerate() {
        let code = u32::try_from(idx).map_err(|_| GenerationError::WireWidthOverflow {
            what: "global_string_index",
            count: idx as u64,
            max: u64::from(u32::MAX),
        })?;
        string_index.insert(s.clone(), code);
    }

    let mut segments: Vec<V5Segment> = Vec::new();

    // ── 0. Metadata ────────────────────────────────────────────────────────
    let mut meta = Vec::new();
    write_u32(&mut meta, store.node_tables_by_id.len() as u32);
    for (tid, nt) in store.node_tables_by_id.iter().enumerate() {
        write_u16(&mut meta, tid as u16);
        let label_code = *string_index.get(nt.label()).ok_or_else(|| {
            GenerationError::Codec(format!("label string not interned: {}", nt.label()))
        })?;
        write_u32(&mut meta, label_code);
        write_u32(&mut meta, nt.len() as u32);
        let cols = nt.columns();
        write_u32(&mut meta, cols.len() as u32);
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

    write_u32(&mut meta, store.rel_tables_by_id.len() as u32);
    for (rid, rt) in store.rel_tables_by_id.iter().enumerate() {
        write_u16(&mut meta, rid as u16);
        write_u16(&mut meta, rt.src_table_id());
        write_u16(&mut meta, rt.dst_table_id());
        let type_code = *string_index.get(rt.edge_type().as_str()).ok_or_else(|| {
            GenerationError::Codec(format!("edge type not interned: {}", rt.edge_type()))
        })?;
        write_u32(&mut meta, type_code);
        write_u32(&mut meta, rt.num_edges() as u32);
        let props = rt.properties();
        write_u32(&mut meta, props.len() as u32);
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

    segments.push(V5Segment {
        kind: SegmentKind::Metadata,
        encoding_version: 1,
        flags: 0x0001,
        alignment: 1,
        element_width: 0,
        bytes: meta,
    });

    // ── 1 & 2. StringOffsets & StringBytes ─────────────────────────────────
    let str_refs: Vec<&str> = str_slice.iter().map(String::as_str).collect();
    let (off_bytes, str_bytes) = build_string_segments(&str_refs);
    segments.push(V5Segment {
        kind: SegmentKind::StringOffsets,
        encoding_version: 1,
        flags: 0x0001,
        alignment: 8,
        element_width: 8,
        bytes: off_bytes,
    });
    segments.push(V5Segment {
        kind: SegmentKind::StringBytes,
        encoding_version: 1,
        flags: 0x0001,
        alignment: 1,
        element_width: 1,
        bytes: str_bytes,
    });

    // ── 3..8. Directories, Columns, CSR ───────────────────────────────────
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

    for (tid, nt) in store.node_tables_by_id.iter().enumerate() {
        let col_start = column_index;
        let mut keys: Vec<_> = nt.columns().keys().cloned().collect();
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for key in &keys {
            let codec = nt.columns().get(key).unwrap();
            let body_start = col_bodies.len() as u32;
            write_column_body(&mut col_bodies, codec, &string_index)
                .map_err(GenerationError::Codec)?;
            let body_len = (col_bodies.len() as u32).saturating_sub(body_start);
            write_u16(&mut col_dir, codec_disc(codec));
            write_u16(&mut col_dir, value_type_code(codec));
            write_u32(&mut col_dir, column_index);
            write_u32(&mut col_dir, 1);
            write_u64(&mut col_dir, codec.len() as u64);
            write_u32(&mut col_dir, 0);

            write_u32(&mut col_block_index, body_start);
            write_u32(&mut col_block_index, body_len);
            write_u32(&mut col_block_index, codec.len() as u32);
            column_index += 1;
        }
        write_u16(&mut node_dir, tid as u16);
        write_u16(&mut node_dir, 0);
        write_u32(&mut node_dir, col_start);
        write_u32(&mut node_dir, keys.len() as u32);
        write_u64(&mut node_dir, nt.len() as u64);
        write_u32(&mut node_dir, 0);
    }

    for (rid, rt) in store.rel_tables_by_id.iter().enumerate() {
        let col_start = column_index;
        let mut keys: Vec<_> = rt.properties().keys().cloned().collect();
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for key in &keys {
            let codec = rt.properties().get(key).unwrap();
            let body_start = col_bodies.len() as u32;
            write_column_body(&mut col_bodies, codec, &string_index)
                .map_err(GenerationError::Codec)?;
            let body_len = (col_bodies.len() as u32).saturating_sub(body_start);
            write_u16(&mut col_dir, codec_disc(codec));
            write_u16(&mut col_dir, value_type_code(codec));
            write_u32(&mut col_dir, column_index);
            write_u32(&mut col_dir, 1);
            write_u64(&mut col_dir, codec.len() as u64);
            write_u32(&mut col_dir, 0);
            write_u32(&mut col_block_index, body_start);
            write_u32(&mut col_block_index, body_len);
            write_u32(&mut col_block_index, codec.len() as u32);
            column_index += 1;
        }

        write_u16(&mut rel_dir, rid as u16);
        write_u16(&mut rel_dir, rt.src_table_id());
        write_u16(&mut rel_dir, rt.dst_table_id());
        write_u16(&mut rel_dir, 0);
        write_u32(&mut rel_dir, col_start);
        write_u32(&mut rel_dir, keys.len() as u32);
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

    segments.push(V5Segment {
        kind: SegmentKind::NodeTableDirectory,
        encoding_version: 1,
        flags: 0x0001,
        alignment: 8,
        element_width: 24,
        bytes: node_dir,
    });
    segments.push(V5Segment {
        kind: SegmentKind::RelTableDirectory,
        encoding_version: 1,
        flags: 0x0001,
        alignment: 8,
        element_width: 24,
        bytes: rel_dir,
    });
    segments.push(V5Segment {
        kind: SegmentKind::ColumnDirectory,
        encoding_version: 1,
        flags: 0x0001,
        alignment: 8,
        element_width: 24,
        bytes: col_dir,
    });
    segments.push(V5Segment {
        kind: SegmentKind::ColumnBlockIndex,
        encoding_version: 1,
        flags: 0x0001,
        alignment: 4,
        element_width: 12,
        bytes: col_block_index,
    });
    segments.push(V5Segment {
        kind: SegmentKind::ColumnBodies,
        encoding_version: 1,
        flags: 0x0001,
        alignment: 1,
        element_width: 0,
        bytes: col_bodies,
    });
    segments.push(V5Segment {
        kind: SegmentKind::ForwardCsrOffsets,
        encoding_version: 1,
        flags: 0x0001,
        alignment: 4,
        element_width: 4,
        bytes: fwd_offsets,
    });
    segments.push(V5Segment {
        kind: SegmentKind::ForwardCsrTargets,
        encoding_version: 1,
        flags: 0x0001,
        alignment: 4,
        element_width: 4,
        bytes: fwd_targets,
    });

    if has_reverse {
        segments.push(V5Segment {
            kind: SegmentKind::ReverseCsrOffsets,
            encoding_version: 1,
            flags: 0x0001,
            alignment: 4,
            element_width: 4,
            bytes: rev_offsets,
        });
        segments.push(V5Segment {
            kind: SegmentKind::ReverseCsrTargets,
            encoding_version: 1,
            flags: 0x0001,
            alignment: 4,
            element_width: 4,
            bytes: rev_targets,
        });
        segments.push(V5Segment {
            kind: SegmentKind::ForwardPositions,
            encoding_version: 1,
            flags: 0x0001,
            alignment: 4,
            element_width: 4,
            bytes: fwd_positions,
        });
    }

    // ── ID lookups ─────────────────────────────────────────────────────────
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
        segments.push(V5Segment {
            kind: SegmentKind::NodeIdLookup,
            encoding_version: 1,
            flags: 0x0001,
            alignment: 8,
            element_width: 24,
            bytes: node_lookup,
        });
        segments.push(V5Segment {
            kind: SegmentKind::EdgeIdLookup,
            encoding_version: 1,
            flags: 0x0001,
            alignment: 8,
            element_width: 24,
            bytes: edge_lookup,
        });
        segments.push(V5Segment {
            kind: SegmentKind::NodeOriginalIds,
            encoding_version: 1,
            flags: 0x0001,
            alignment: 8,
            element_width: 8,
            bytes: node_orig,
        });
        segments.push(V5Segment {
            kind: SegmentKind::EdgeOriginalIds,
            encoding_version: 1,
            flags: 0x0001,
            alignment: 8,
            element_width: 8,
            bytes: edge_orig,
        });
    }

    // ── Zone maps & dictionary code index ─────────────────────────────────
    let (table_zm, block_zm) =
        build_zone_map_segments(store, &string_index).map_err(GenerationError::Codec)?;
    if !table_zm.is_empty() {
        segments.push(V5Segment {
            kind: SegmentKind::TableZoneMaps,
            encoding_version: 1,
            flags: 0x0001,
            alignment: 8,
            element_width: ZONE_MAP_RECORD_LEN as u32,
            bytes: table_zm,
        });
    }
    if !block_zm.is_empty() {
        segments.push(V5Segment {
            kind: SegmentKind::BlockZoneMaps,
            encoding_version: 1,
            flags: 0x0001,
            alignment: 8,
            element_width: ZONE_MAP_RECORD_LEN as u32,
            bytes: block_zm,
        });
    }

    let code_index_body = build_dictionary_code_index(&str_refs);
    if !code_index_body.is_empty() {
        segments.push(V5Segment {
            kind: SegmentKind::DictionaryCodeIndex,
            encoding_version: 1,
            flags: 0x0001,
            alignment: 8,
            element_width: 16,
            bytes: code_index_body,
        });
    }

    // Sort strictly by segment kind ascending
    segments.sort_by_key(|s| s.kind.as_u16());
    Ok(segments)
}
