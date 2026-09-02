//! CompactStore payload version 5: mapped segment directory codec (G-EM0.2).
//!
//! All `usize as u16/u32` casts in this codec write graph-structure counts and
//! offsets (table IDs, column counts, segment lengths) into fixed-width wire
//! fields. These are bounded by the v5 format by construction — a section
//! cannot exceed `u32::MAX` columns or `u16::MAX` tables — so truncation is
//! impossible for any valid store.
#![allow(clippy::cast_possible_truncation)]

use arcstr::ArcStr;
use bytes::Bytes;
use grafeo_common::types::PropertyKey;
use grafeo_common::utils::hash::FxHashMap;

use super::CompactStore;
use super::column::ColumnCodec;
use super::csr::CsrAdjacency;
use super::mapped::{
    CompactMemoryAccounting, DIRECTORY_ENTRY_LEN, DictionaryCodeIndex, FORMAT_VERSION_V5,
    HEADER_LEN, MappedEdgeIdLookup, MappedNodeIdLookup, MappedStringDictionary,
    SCHEMA_OWNER_BUDGET_BYTES, SegmentKind, U32View, ZONE_MAP_RECORD_LEN,
    build_dictionary_code_index, build_string_segments, build_zone_map_segments, layout_flags,
    parse_block_zone_maps, parse_segment_directory, parse_table_zone_maps, slice_segment_checked,
    write_edge_id_record, write_node_id_record,
};
use super::node_table::NodeTable;
use super::rel_table::RelTable;
use super::schema::{ColumnDef, ColumnType, EdgeSchema, TableSchema};
use super::zone_map::ZoneMap;
use crate::codec::DictionaryEncoding;
use crate::statistics::{EdgeTypeStatistics, LabelStatistics, Statistics};

const MAGIC: [u8; 4] = *b"GCST";

/// Global string code assignment policy for v5 serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StringCodeOrder {
    /// First-seen insertion order (legacy heap serializer path).
    #[default]
    Insertion,
    /// UTF-8 byte-lexicographic order (G-EM0.W0 generation contract).
    Lexicographic,
}

/// Serializes a heap-built [`CompactStore`] into a v5 payload with trailing CRC.
///
/// Uses insertion-order string codes for backward-compatible writers.
///
/// # Errors
///
/// Returns an error when a collection length exceeds the wire encoding.
pub fn serialize_v5(store: &CompactStore) -> Result<Vec<u8>, String> {
    serialize_v5_with_string_order(store, StringCodeOrder::Insertion)
}

/// Serializes a heap-built [`CompactStore`] into a v5 payload with the given
/// global string-code assignment policy.
///
/// # Errors
///
/// Returns an error when a collection length exceeds the wire encoding.
#[cfg_attr(
    feature = "generation-streaming",
    allow(unreachable_code, unused_variables, unused_mut)
)]
pub fn serialize_v5_with_string_order(
    store: &CompactStore,
    string_order: StringCodeOrder,
) -> Result<Vec<u8>, String> {
    let mut segments: Vec<(SegmentKind, u16, u16, u16, u32, Vec<u8>)> = Vec::new();
    // (kind, encoding_version, flags, alignment, element_width, bytes)

    // ── Global string dictionary (labels, keys, types, dict entries, zone strings)
    let mut strings: Vec<String> = Vec::new();
    let mut string_index: FxHashMap<String, u32> = FxHashMap::default();
    let mut intern = |s: &str| -> u32 {
        if let Some(&id) = string_index.get(s) {
            return id;
        }
        let id = u32::try_from(strings.len()).expect("string count fits u32");
        strings.push(s.to_string());
        string_index.insert(s.to_string(), id);
        id
    };

    // Pre-intern schema strings and dict entries so Metadata can reference codes.
    for nt in &store.node_tables_by_id {
        let _ = intern(nt.label());
        for key in nt.columns().keys() {
            let _ = intern(key.as_str());
        }
        for codec in nt.columns().values() {
            if let ColumnCodec::Dict(d) = codec {
                for i in 0..d.dictionary_size() {
                    if let Some(s) = d.get(i) {
                        let _ = intern(s);
                    }
                }
            }
        }
        for zm in nt.zone_maps().values() {
            intern_zone_strings(zm, &mut intern);
        }
        for zms in nt.block_zone_maps().values() {
            for zm in zms {
                intern_zone_strings(zm, &mut intern);
            }
        }
    }
    for rt in &store.rel_tables_by_id {
        let _ = intern(rt.edge_type().as_str());
        for key in rt.properties().keys() {
            let _ = intern(key.as_str());
        }
        for codec in rt.properties().values() {
            if let ColumnCodec::Dict(d) = codec {
                for i in 0..d.dictionary_size() {
                    if let Some(s) = d.get(i) {
                        let _ = intern(s);
                    }
                }
            }
        }
        // R3-B2: intern rel zone map strings.
        for zm in rt.zone_maps().values() {
            intern_zone_strings(zm, &mut intern);
        }
        for zms in rt.block_zone_maps().values() {
            for zm in zms {
                intern_zone_strings(zm, &mut intern);
            }
        }
    }

    if string_order == StringCodeOrder::Lexicographic {
        // Reassign codes in UTF-8 byte-lexicographic order; count must fit u32.
        strings.sort();
        strings.dedup();
        if strings.len() > u32::MAX as usize {
            return Err(format!(
                "global string dictionary length {} exceeds u32::MAX",
                strings.len()
            ));
        }
        string_index.clear();
        for (i, s) in strings.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            string_index.insert(s.clone(), i as u32);
        }
    }

    let str_refs: Vec<&str> = strings.iter().map(String::as_str).collect();

    // ── Canonical bounded emission (G-EM0.5b Phase 1) ──────────────────
    // Feature ON: delegate to the canonical emitter + assembler. The eager
    // path below is held harmless for feature OFF (Milestone R unchanged).
    #[cfg(feature = "generation-streaming")]
    {
        use super::generation::emit::{V5PayloadAssembler, emit_canonical_descriptors};
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
        let descriptors = emit_canonical_descriptors(store, &string_index, &str_refs)
            .map_err(|e| e.to_string())?;
        let assembler = V5PayloadAssembler::new(total_nodes, total_edges, store.preserves_ids())
            .with_layout_flags(V5PayloadAssembler::layout_flags_from_descriptors(
                &descriptors,
            ));
        return assembler.assemble(&descriptors).map_err(|e| e.to_string());
    }

    // ── Eager path (feature OFF; Milestone R held harmless) ─────────────
    // With generation-streaming ON this code is unreachable (canonical path
    // returns above). It is intentionally retained for feature OFF.
    let (off_bytes, str_bytes) = build_string_segments(&str_refs);
    segments.push((SegmentKind::StringOffsets, 1, 0x0001, 8, 8, off_bytes));
    segments.push((SegmentKind::StringBytes, 1, 0x0001, 1, 1, str_bytes));

    // ── Metadata
    let mut meta = Vec::new();
    write_u32(&mut meta, store.node_tables_by_id.len() as u32);
    for (tid, nt) in store.node_tables_by_id.iter().enumerate() {
        write_u16(&mut meta, tid as u16);
        write_u32(&mut meta, *string_index.get(nt.label()).unwrap());
        write_u32(&mut meta, nt.len() as u32);
        let cols = nt.columns();
        write_u32(&mut meta, cols.len() as u32);
        // Deterministic column order: sort by key.
        let mut keys: Vec<_> = cols.keys().cloned().collect();
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for key in &keys {
            write_u32(&mut meta, *string_index.get(key.as_str()).unwrap());
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
        write_u32(
            &mut meta,
            *string_index.get(rt.edge_type().as_str()).unwrap(),
        );
        write_u32(&mut meta, rt.num_edges() as u32);
        let props = rt.properties();
        write_u32(&mut meta, props.len() as u32);
        let mut keys: Vec<_> = props.keys().cloned().collect();
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for key in &keys {
            write_u32(&mut meta, *string_index.get(key.as_str()).unwrap());
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
    segments.insert(0, (SegmentKind::Metadata, 1, 0x0001, 1, 0, meta));

    // ── Node / rel table directories + columns + CSR
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
            write_column_body(&mut col_bodies, codec, &string_index)?;
            let body_len = (col_bodies.len() as u32).saturating_sub(body_start);
            // ColumnDirectory record (24 bytes)
            write_u16(&mut col_dir, codec_disc(codec));
            write_u16(&mut col_dir, value_type_code(codec));
            write_u32(&mut col_dir, column_index); // block_start index into block index
            write_u32(&mut col_dir, 1); // one logical block for v5 body
            write_u64(&mut col_dir, codec.len() as u64);
            write_u32(&mut col_dir, 0); // reserved
            // ColumnBlockIndex record (12 bytes)
            write_u32(&mut col_block_index, body_start);
            write_u32(&mut col_block_index, body_len);
            write_u32(&mut col_block_index, codec.len() as u32);
            column_index += 1;
        }
        // NodeTableDirectory record (24 bytes)
        write_u16(&mut node_dir, tid as u16);
        write_u16(&mut node_dir, 0); // reserved_a
        write_u32(&mut node_dir, col_start);
        write_u32(&mut node_dir, keys.len() as u32);
        write_u64(&mut node_dir, nt.len() as u64);
        write_u32(&mut node_dir, 0); // reserved_b
    }

    for (rid, rt) in store.rel_tables_by_id.iter().enumerate() {
        let col_start = column_index;
        let mut keys: Vec<_> = rt.properties().keys().cloned().collect();
        keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for key in &keys {
            let codec = rt.properties().get(key).unwrap();
            let body_start = col_bodies.len() as u32;
            write_column_body(&mut col_bodies, codec, &string_index)?;
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
        // RelTableDirectory (24 bytes)
        write_u16(&mut rel_dir, rid as u16);
        write_u16(&mut rel_dir, rt.src_table_id());
        write_u16(&mut rel_dir, rt.dst_table_id());
        write_u16(&mut rel_dir, 0);
        write_u32(&mut rel_dir, col_start);
        write_u32(&mut rel_dir, keys.len() as u32);
        write_u64(&mut rel_dir, rt.num_edges() as u64);

        // CSR arrays
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
                // empty edge_data: pad with zeros for alignment with targets
                for _ in 0..bwd.num_edges() {
                    write_u32(&mut fwd_positions, 0);
                }
            }
        }
    }

    segments.push((SegmentKind::NodeTableDirectory, 1, 0x0001, 8, 24, node_dir));
    segments.push((SegmentKind::RelTableDirectory, 1, 0x0001, 8, 24, rel_dir));
    segments.push((SegmentKind::ColumnDirectory, 1, 0x0001, 8, 24, col_dir));
    segments.push((
        SegmentKind::ColumnBlockIndex,
        1,
        0x0001,
        4,
        12,
        col_block_index,
    ));
    segments.push((SegmentKind::ColumnBodies, 1, 0x0001, 1, 0, col_bodies));
    segments.push((SegmentKind::ForwardCsrOffsets, 1, 0x0001, 4, 4, fwd_offsets));
    segments.push((SegmentKind::ForwardCsrTargets, 1, 0x0001, 4, 4, fwd_targets));
    if has_reverse {
        segments.push((SegmentKind::ReverseCsrOffsets, 1, 0x0001, 4, 4, rev_offsets));
        segments.push((SegmentKind::ReverseCsrTargets, 1, 0x0001, 4, 4, rev_targets));
        segments.push((
            SegmentKind::ForwardPositions,
            1,
            0x0001,
            4,
            4,
            fwd_positions,
        ));
    }

    // ── ID lookups
    let flags: u8 = u8::from(store.preserves_ids());
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
        segments.push((SegmentKind::NodeIdLookup, 1, 0x0001, 8, 24, node_lookup));
        segments.push((SegmentKind::EdgeIdLookup, 1, 0x0001, 8, 24, edge_lookup));
        segments.push((SegmentKind::NodeOriginalIds, 1, 0x0001, 8, 8, node_orig));
        segments.push((SegmentKind::EdgeOriginalIds, 1, 0x0001, 8, 8, edge_orig));
    }

    // ── Zone maps (kinds 18–19) + dictionary code index (kind 20)
    let (table_zm, block_zm) = build_zone_map_segments(store, &string_index)?;
    if !table_zm.is_empty() {
        segments.push((
            SegmentKind::TableZoneMaps,
            1,
            0x0001,
            8,
            ZONE_MAP_RECORD_LEN as u32,
            table_zm,
        ));
    }
    if !block_zm.is_empty() {
        segments.push((
            SegmentKind::BlockZoneMaps,
            1,
            0x0001,
            8,
            ZONE_MAP_RECORD_LEN as u32,
            block_zm,
        ));
    }
    let code_index_body = build_dictionary_code_index(&str_refs);
    if !code_index_body.is_empty() {
        segments.push((
            SegmentKind::DictionaryCodeIndex,
            1,
            0x0001,
            8,
            16,
            code_index_body,
        ));
    }

    // Sort segments by kind ascending (already mostly ordered; Metadata was inserted at 0).
    segments.sort_by_key(|(k, ..)| k.as_u16());

    // ── Assemble payload
    let segment_count = u16::try_from(segments.len()).map_err(|_| "too many segments")?;
    let directory_length = u64::from(segment_count) * DIRECTORY_ENTRY_LEN as u64;
    let data_offset = align_up(HEADER_LEN as u64 + directory_length, 8);

    // Build directory entries with provisional offsets.
    let mut dir_bytes = Vec::with_capacity(directory_length as usize);
    let mut data_bytes = Vec::new();
    let mut cursor = data_offset;
    let mut entries_meta = Vec::new();

    for (kind, _enc_ver, _flags_u16, alignment, element_width, body) in &segments {
        let align = u64::from(*alignment);
        let padded_off = align_up(cursor, align);
        // padding between segments
        let pad = (padded_off - cursor) as usize;
        data_bytes.resize(data_bytes.len() + pad, 0);
        let offset = padded_off;
        let length = body.len() as u64;
        let crc = crc32fast::hash(body);
        let element_count = if *element_width > 0 {
            (length / u64::from(*element_width)) as u32
        } else {
            0
        };
        entries_meta.push((*kind, offset, length, crc, element_count));
        data_bytes.extend_from_slice(body);
        cursor = offset + length;
    }

    for ((_kind, enc_ver, flags_u16, alignment, element_width, _), meta) in
        segments.iter().zip(entries_meta.iter())
    {
        let (kind, offset, length, crc, element_count) = *meta;
        write_u16(&mut dir_bytes, kind.as_u16());
        write_u16(&mut dir_bytes, *enc_ver);
        write_u16(&mut dir_bytes, *flags_u16);
        write_u16(&mut dir_bytes, *alignment);
        write_u64(&mut dir_bytes, offset);
        write_u64(&mut dir_bytes, length);
        write_u32(&mut dir_bytes, *element_width);
        write_u32(&mut dir_bytes, element_count);
        write_u32(&mut dir_bytes, crc);
        write_u32(&mut dir_bytes, 0); // reserved_a
        write_u64(&mut dir_bytes, 0); // reserved_b
        let _ = (enc_ver, flags_u16, alignment, element_width);
    }
    let directory_crc = crc32fast::hash(&dir_bytes);

    let layout_flags = layout_flags::from_companion_segments(
        segments
            .iter()
            .any(|(k, ..)| *k == SegmentKind::NodeLabelMembership),
        segments
            .iter()
            .any(|(k, ..)| *k == SegmentKind::ColumnRowPresence),
        segments
            .iter()
            .any(|(k, ..)| *k == SegmentKind::ColumnRowNull),
    );

    let mut out = Vec::with_capacity((data_offset as usize) + data_bytes.len() + 4);
    out.extend_from_slice(&MAGIC);
    out.push(FORMAT_VERSION_V5);
    out.push(flags);
    write_u16(&mut out, HEADER_LEN as u16);
    write_u16(&mut out, segment_count);
    write_u16(&mut out, DIRECTORY_ENTRY_LEN as u16);
    write_u32(&mut out, layout_flags);
    write_u64(&mut out, HEADER_LEN as u64); // directory_offset
    write_u64(&mut out, directory_length);
    write_u64(&mut out, data_offset);
    write_u64(&mut out, total_nodes);
    write_u64(&mut out, total_edges);
    write_u32(&mut out, directory_crc);
    write_u32(&mut out, 0); // reserved
    debug_assert_eq!(out.len(), HEADER_LEN);
    out.extend_from_slice(&dir_bytes);
    // pad to data_offset
    while out.len() < data_offset as usize {
        out.push(0);
    }
    out.extend_from_slice(&data_bytes);
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Deserializes a v5 payload into a mapped-backed [`CompactStore`].
///
/// # Errors
///
/// Returns an error on CRC failure, directory corruption, or structural
/// inconsistency. Failures do not expose unchecked slices.
pub fn deserialize_v5(data_bytes: &Bytes) -> Result<CompactStore, String> {
    let data: &[u8] = data_bytes.as_ref();
    if data.len() < HEADER_LEN + 4 {
        return Err("data too short for CompactStore v5".into());
    }
    // Outer CRC
    let payload = &data[..data.len() - 4];
    let stored_crc = u32::from_le_bytes([
        data[data.len() - 4],
        data[data.len() - 3],
        data[data.len() - 2],
        data[data.len() - 1],
    ]);
    let computed = crc32fast::hash(payload);
    if stored_crc != computed {
        return Err(format!(
            "CRC32 mismatch: stored {stored_crc:#010X}, computed {computed:#010X}"
        ));
    }

    let directory = parse_segment_directory(data_bytes, payload.len())?;
    let string_offsets =
        slice_segment_checked(data_bytes, directory.require(SegmentKind::StringOffsets)?)?;
    let string_bytes =
        slice_segment_checked(data_bytes, directory.require(SegmentKind::StringBytes)?)?;
    let global_dict = MappedStringDictionary::new(string_offsets, string_bytes)?;
    let code_index_bytes = directory
        .get(SegmentKind::DictionaryCodeIndex)
        .map(|e| slice_segment_checked(data_bytes, e))
        .transpose()?;
    let code_index = match code_index_bytes {
        Some(bytes) => Some(DictionaryCodeIndex::new(bytes, &global_dict)?),
        None => None,
    };
    let code_index_raw = code_index.as_ref().map(DictionaryCodeIndex::bytes);

    let meta_bytes = slice_segment_checked(data_bytes, directory.require(SegmentKind::Metadata)?)?;
    let meta = parse_metadata(meta_bytes.as_ref(), &global_dict)?;

    let node_dir_bytes = slice_segment_checked(
        data_bytes,
        directory.require(SegmentKind::NodeTableDirectory)?,
    )?;
    let rel_dir_bytes = slice_segment_checked(
        data_bytes,
        directory.require(SegmentKind::RelTableDirectory)?,
    )?;
    let col_dir_bytes =
        slice_segment_checked(data_bytes, directory.require(SegmentKind::ColumnDirectory)?)?;
    let col_block_bytes = slice_segment_checked(
        data_bytes,
        directory.require(SegmentKind::ColumnBlockIndex)?,
    )?;
    let col_bodies =
        slice_segment_checked(data_bytes, directory.require(SegmentKind::ColumnBodies)?)?;

    let fwd_off = slice_segment_checked(
        data_bytes,
        directory.require(SegmentKind::ForwardCsrOffsets)?,
    )?;
    let fwd_tgt = slice_segment_checked(
        data_bytes,
        directory.require(SegmentKind::ForwardCsrTargets)?,
    )?;
    let rev_off = directory
        .get(SegmentKind::ReverseCsrOffsets)
        .map(|e| slice_segment_checked(data_bytes, e))
        .transpose()?;
    let rev_tgt = directory
        .get(SegmentKind::ReverseCsrTargets)
        .map(|e| slice_segment_checked(data_bytes, e))
        .transpose()?;
    let fwd_pos = directory
        .get(SegmentKind::ForwardPositions)
        .map(|e| slice_segment_checked(data_bytes, e))
        .transpose()?;

    // Zone maps (kinds 18–19); absent segments yield empty maps (legacy fallback).
    let table_count = meta.node_tables.len();
    let rel_count = meta.rel_tables.len();
    let (table_zone_maps, rel_table_zone_maps) = match directory.get(SegmentKind::TableZoneMaps) {
        Some(entry) => {
            let bytes = slice_segment_checked(data_bytes, entry)?;
            parse_table_zone_maps(bytes.as_ref(), &global_dict, table_count, rel_count)?
        }
        None => (
            (0..table_count).map(|_| FxHashMap::default()).collect(),
            (0..rel_count).map(|_| FxHashMap::default()).collect(),
        ),
    };
    let (block_zone_maps, rel_block_zone_maps) = match directory.get(SegmentKind::BlockZoneMaps) {
        Some(entry) => {
            let bytes = slice_segment_checked(data_bytes, entry)?;
            parse_block_zone_maps(bytes.as_ref(), &global_dict, table_count, rel_count)?
        }
        None => (
            (0..table_count).map(|_| FxHashMap::default()).collect(),
            (0..rel_count).map(|_| FxHashMap::default()).collect(),
        ),
    };

    // Build node tables
    let mut node_tables = Vec::with_capacity(meta.node_tables.len());
    let mut label_to_table_id = FxHashMap::default();
    let mut table_id_to_label = Vec::with_capacity(meta.node_tables.len());
    let mut fwd_off_cursor = 0usize;
    let mut fwd_tgt_cursor = 0usize;
    let mut rev_off_cursor = 0usize;
    let mut rev_tgt_cursor = 0usize;
    let mut fwd_pos_cursor = 0usize;

    for (tid, nt_meta) in meta.node_tables.iter().enumerate() {
        let rec = read_node_table_record(&node_dir_bytes, tid)?;
        if rec.id as usize != tid {
            return Err(format!("NodeTableDirectory id {} != index {tid}", rec.id));
        }
        if rec.row_count as usize != nt_meta.row_count {
            return Err("NodeTableDirectory row_count mismatch with Metadata".into());
        }
        let mut columns = FxHashMap::default();
        let mut col_defs = Vec::new();
        for c in 0..rec.column_count as usize {
            let col_idx = rec.column_start as usize + c;
            let col_meta = nt_meta.columns.get(c).ok_or("metadata column missing")?;
            let body = column_body_slice(&col_block_bytes, &col_bodies, col_idx)?;
            let codec = read_column_body_with_index(
                &body,
                col_meta.disc,
                &global_dict,
                code_index_raw.clone(),
            )?;
            let key = PropertyKey::new(&col_meta.key);
            col_defs.push(ColumnDef::new(
                &col_meta.key,
                column_type_from_disc(col_meta.disc),
            ));
            columns.insert(key, codec);
        }
        let schema = TableSchema::new(&nt_meta.label, tid as u16, col_defs);
        let zone_maps = table_zone_maps.get(tid).cloned().unwrap_or_default();
        let block_zms = block_zone_maps.get(tid).cloned().unwrap_or_default();
        let table = NodeTable::from_columns_with_block_stats(
            schema,
            columns,
            zone_maps,
            block_zms,
            nt_meta.row_count,
        );
        label_to_table_id.insert(ArcStr::from(nt_meta.label.as_str()), tid as u16);
        table_id_to_label.push(ArcStr::from(nt_meta.label.as_str()));
        node_tables.push(table);
    }

    // Rel tables
    let mut rel_tables = Vec::with_capacity(meta.rel_tables.len());
    let mut edge_type_to_rel_id: FxHashMap<ArcStr, Vec<u16>> = FxHashMap::default();
    let mut rel_table_id_to_type = Vec::with_capacity(meta.rel_tables.len());

    for (rid, rt_meta) in meta.rel_tables.iter().enumerate() {
        let rec = read_rel_table_record(&rel_dir_bytes, rid)?;
        if rec.id as usize != rid {
            return Err(format!("RelTableDirectory id {} != index {rid}", rec.id));
        }
        let src_rows = meta
            .node_tables
            .get(rec.src_tid as usize)
            .map(|n| n.row_count)
            .ok_or("rel src table missing")?;
        let dst_rows = meta
            .node_tables
            .get(rec.dst_tid as usize)
            .map(|n| n.row_count)
            .ok_or("rel dst table missing")?;
        let off_len = src_rows + 1;
        let tgt_len = rec.edge_count as usize;

        let off_bytes = slice_u32_range(&fwd_off, fwd_off_cursor, off_len)?;
        let tgt_bytes = slice_u32_range(&fwd_tgt, fwd_tgt_cursor, tgt_len)?;
        fwd_off_cursor += off_len;
        fwd_tgt_cursor += tgt_len;
        let fwd = CsrAdjacency::from_mapped_parts(
            U32View::new(off_bytes).map_err(str::to_string)?,
            U32View::new(tgt_bytes).map_err(str::to_string)?,
            None,
        )
        .map_err(str::to_string)?;

        let bwd = if let (Some(rev_off_b), Some(rev_tgt_b), Some(pos_b)) =
            (rev_off.as_ref(), rev_tgt.as_ref(), fwd_pos.as_ref())
        {
            let boff_len = dst_rows + 1;
            let btgt_len = rec.edge_count as usize;
            let boff = slice_u32_range(rev_off_b, rev_off_cursor, boff_len)?;
            let btgt = slice_u32_range(rev_tgt_b, rev_tgt_cursor, btgt_len)?;
            let bpos = slice_u32_range(pos_b, fwd_pos_cursor, btgt_len)?;
            rev_off_cursor += boff_len;
            rev_tgt_cursor += btgt_len;
            fwd_pos_cursor += btgt_len;
            Some(
                CsrAdjacency::from_mapped_parts(
                    U32View::new(boff).map_err(str::to_string)?,
                    U32View::new(btgt).map_err(str::to_string)?,
                    Some(U32View::new(bpos).map_err(str::to_string)?),
                )
                .map_err(str::to_string)?,
            )
        } else {
            None
        };

        let mut properties = FxHashMap::default();
        let mut prop_defs = Vec::new();
        for c in 0..rec.column_count as usize {
            let col_idx = rec.column_start as usize + c;
            let col_meta = rt_meta
                .columns
                .get(c)
                .ok_or("rel metadata column missing")?;
            let body = column_body_slice(&col_block_bytes, &col_bodies, col_idx)?;
            let codec = read_column_body_with_index(
                &body,
                col_meta.disc,
                &global_dict,
                code_index_raw.clone(),
            )?;
            let key = PropertyKey::new(&col_meta.key);
            prop_defs.push(ColumnDef::new(
                &col_meta.key,
                column_type_from_disc(col_meta.disc),
            ));
            properties.insert(key, codec);
        }

        let src_label = table_id_to_label
            .get(rec.src_tid as usize)
            .cloned()
            .unwrap_or_default();
        let dst_label = table_id_to_label
            .get(rec.dst_tid as usize)
            .cloned()
            .unwrap_or_default();
        let schema = EdgeSchema::new(
            &rt_meta.edge_type,
            rid as u16,
            src_label.as_str(),
            dst_label.as_str(),
            prop_defs,
        );
        let rel_zm = rel_table_zone_maps.get(rid).cloned().unwrap_or_default();
        let rel_bzm = rel_block_zone_maps.get(rid).cloned().unwrap_or_default();
        let table = RelTable::with_zone_maps(
            schema,
            fwd,
            bwd,
            properties,
            rec.src_tid,
            rec.dst_tid,
            rel_zm,
            rel_bzm,
        );
        let et = ArcStr::from(rt_meta.edge_type.as_str());
        edge_type_to_rel_id
            .entry(et.clone())
            .or_default()
            .push(rid as u16);
        rel_table_id_to_type.push(et);
        rel_tables.push(table);
    }

    let mut stats = Statistics::new();
    stats.total_nodes = meta.total_nodes;
    stats.total_edges = meta.total_edges;
    for (idx, nt) in node_tables.iter().enumerate() {
        stats.update_label(
            table_id_to_label[idx].as_str(),
            LabelStatistics::new(nt.len() as u64),
        );
    }
    let mut edge_counts: FxHashMap<&str, u64> = FxHashMap::default();
    for (idx, rt) in rel_tables.iter().enumerate() {
        *edge_counts
            .entry(rel_table_id_to_type[idx].as_str())
            .or_default() += rt.num_edges() as u64;
    }
    for (et, count) in edge_counts {
        stats.update_edge_type(et, EdgeTypeStatistics::new(count, 0.0, 0.0));
    }

    let mut store = CompactStore::new(
        node_tables,
        label_to_table_id,
        rel_tables,
        edge_type_to_rel_id,
        table_id_to_label,
        rel_table_id_to_type,
        stats,
    );

    if directory.header.preserves_ids() {
        let node_lookup = MappedNodeIdLookup::new(slice_segment_checked(
            data_bytes,
            directory.require(SegmentKind::NodeIdLookup)?,
        )?)?;
        let edge_lookup = MappedEdgeIdLookup::new(slice_segment_checked(
            data_bytes,
            directory.require(SegmentKind::EdgeIdLookup)?,
        )?)?;
        let node_orig =
            slice_segment_checked(data_bytes, directory.require(SegmentKind::NodeOriginalIds)?)?;
        let edge_orig =
            slice_segment_checked(data_bytes, directory.require(SegmentKind::EdgeOriginalIds)?)?;
        let node_counts: Vec<usize> = meta.node_tables.iter().map(|n| n.row_count).collect();
        let edge_counts: Vec<usize> = meta
            .rel_tables
            .iter()
            .enumerate()
            .map(|(i, _)| store.rel_tables_by_id[i].num_edges())
            .collect();
        store.set_mapped_id_indexes(
            node_lookup,
            edge_lookup,
            node_orig,
            edge_orig,
            &node_counts,
            &edge_counts,
        );
    }

    // Verify residual proportional heap before accepting the open.
    // CSR must be mapped; dictionaries must be mapped; heap ID maps absent.
    for rt in &store.rel_tables_by_id {
        if !rt.fwd().is_mapped() {
            return Err("v5 forward CSR is not mapped".into());
        }
        if let Some(bwd) = rt.bwd()
            && !bwd.is_mapped()
        {
            return Err("v5 reverse CSR is not mapped".into());
        }
        for codec in rt.properties().values() {
            if let ColumnCodec::Dict(d) = codec
                && !d.is_mapped_dictionary()
            {
                return Err("v5 edge dict is not mapped".into());
            }
        }
    }
    for nt in &store.node_tables_by_id {
        for codec in nt.columns().values() {
            if let ColumnCodec::Dict(d) = codec
                && !d.is_mapped_dictionary()
            {
                return Err("v5 node dict is not mapped".into());
            }
        }
    }
    if store.node_id_map.is_some() || store.edge_id_map.is_some() {
        return Err("v5 open must not retain heap ID maps".into());
    }

    // Accounting: mapped payload is the whole section; schema is bounded.
    let mut accounting = CompactMemoryAccounting::with_defaults();
    accounting.mapped_payload_index_bytes = data_bytes.len();
    accounting.anonymous_owner_schema_bytes = estimate_schema_bytes(&store);
    accounting.anonymous_proportional_structure_bytes = 0;
    if accounting.schema_budget_exceeded() {
        return Err(format!(
            "schema/owner bytes {} exceed budget {}",
            accounting.anonymous_owner_schema_bytes, SCHEMA_OWNER_BUDGET_BYTES
        ));
    }
    store.set_memory_accounting(accounting);
    let _ = col_dir_bytes; // validated by existence; column geometry uses block index

    // ── G-EM0.5b D0.8.0 source-true companions (fail-closed contract) ──
    // `layout_flags` marks which companion segments are required; absence of a
    // required segment fails the open. When no companions are present and
    // layout_flags is zero, old-v5 defaults apply (one physical label per node,
    // every encoded row present and non-null).
    let header_layout_flags = directory.header.layout_flags;
    let membership_bytes = directory
        .get(SegmentKind::NodeLabelMembership)
        .map(|e| slice_segment_checked(data_bytes, e))
        .transpose()?;
    let presence_bytes = directory
        .get(SegmentKind::ColumnRowPresence)
        .map(|e| slice_segment_checked(data_bytes, e))
        .transpose()?;
    let null_bytes = directory
        .get(SegmentKind::ColumnRowNull)
        .map(|e| slice_segment_checked(data_bytes, e))
        .transpose()?;
    layout_flags::require_companion(
        header_layout_flags,
        layout_flags::REQUIRES_LABEL_MEMBERSHIP,
        SegmentKind::NodeLabelMembership,
        membership_bytes.is_some(),
    )?;
    layout_flags::require_companion(
        header_layout_flags,
        layout_flags::REQUIRES_COLUMN_PRESENCE,
        SegmentKind::ColumnRowPresence,
        presence_bytes.is_some(),
    )?;
    layout_flags::require_companion(
        header_layout_flags,
        layout_flags::REQUIRES_COLUMN_NULL,
        SegmentKind::ColumnRowNull,
        null_bytes.is_some(),
    )?;
    if membership_bytes.is_some() || presence_bytes.is_some() || null_bytes.is_some() {
        let membership = membership_bytes
            .as_ref()
            .map(crate::graph::compact::mapped::LabelMembershipView::parse)
            .transpose()?;
        let presence = presence_bytes
            .as_ref()
            .map(|b| {
                crate::graph::compact::mapped::RowBitmapView::parse(
                    b,
                    SegmentKind::ColumnRowPresence,
                )
            })
            .transpose()?;
        let null = null_bytes
            .as_ref()
            .map(|b| {
                crate::graph::compact::mapped::RowBitmapView::parse(b, SegmentKind::ColumnRowNull)
            })
            .transpose()?;
        store.set_source_true_companions(
            membership,
            presence,
            null,
            data_bytes.clone(),
            presence_bytes,
            null_bytes,
            Some(global_dict.clone()),
        );
    }

    // Build the (table_id, key) → flat column index map used to look up the
    // presence/null companions. Columns are emitted in order: node tables
    // first (each with its columns in sorted-key order), then rel tables
    // (tagged with table_id 0x8000 | rel_index, matching column_pass).
    {
        use grafeo_common::utils::hash::FxHashMap;
        let mut map: FxHashMap<(u16, grafeo_common::types::PropertyKey), u32> =
            FxHashMap::default();
        let mut col_idx: u32 = 0;
        for (tid, nt) in meta.node_tables.iter().enumerate() {
            let mut keys: Vec<&str> = nt.columns.iter().map(|c| c.key.as_str()).collect();
            keys.sort();
            for key in keys {
                map.insert((tid as u16, key.into()), col_idx);
                col_idx += 1;
            }
        }
        for (rid, rt) in meta.rel_tables.iter().enumerate() {
            let mut keys: Vec<&str> = rt.columns.iter().map(|c| c.key.as_str()).collect();
            keys.sort();
            for key in keys {
                map.insert((0x8000 | rid as u16, key.into()), col_idx);
                col_idx += 1;
            }
        }
        store.set_column_index_map(map);
    }

    Ok(store)
}

// ── helpers ─────────────────────────────────────────────────────────

struct MetaColumn {
    key: String,
    disc: u16,
}

struct MetaNodeTable {
    label: String,
    row_count: usize,
    columns: Vec<MetaColumn>,
}

struct MetaRelTable {
    edge_type: String,
    columns: Vec<MetaColumn>,
}

struct Metadata {
    node_tables: Vec<MetaNodeTable>,
    rel_tables: Vec<MetaRelTable>,
    total_nodes: u64,
    total_edges: u64,
}

fn parse_metadata(data: &[u8], dict: &MappedStringDictionary) -> Result<Metadata, String> {
    let mut pos = 0usize;
    let n_tables = read_u32(data, &mut pos)? as usize;
    let mut node_tables = Vec::with_capacity(n_tables);
    for _ in 0..n_tables {
        let _tid = read_u16(data, &mut pos)?;
        let label_code = read_u32(data, &mut pos)?;
        let label = dict
            .get(label_code)
            .ok_or("metadata label code OOB")?
            .to_string();
        let row_count = read_u32(data, &mut pos)? as usize;
        let n_cols = read_u32(data, &mut pos)? as usize;
        let mut columns = Vec::with_capacity(n_cols);
        for _ in 0..n_cols {
            let key_code = read_u32(data, &mut pos)?;
            let key = dict
                .get(key_code)
                .ok_or("metadata key code OOB")?
                .to_string();
            let disc = read_u16(data, &mut pos)?;
            let _vtype = read_u16(data, &mut pos)?;
            columns.push(MetaColumn { key, disc });
        }
        node_tables.push(MetaNodeTable {
            label,
            row_count,
            columns,
        });
    }
    let n_rels = read_u32(data, &mut pos)? as usize;
    let mut rel_tables = Vec::with_capacity(n_rels);
    for _ in 0..n_rels {
        let _rid = read_u16(data, &mut pos)?;
        let _src = read_u16(data, &mut pos)?;
        let _dst = read_u16(data, &mut pos)?;
        let type_code = read_u32(data, &mut pos)?;
        let edge_type = dict
            .get(type_code)
            .ok_or("metadata edge type code OOB")?
            .to_string();
        let _edge_count = read_u32(data, &mut pos)?;
        let n_props = read_u32(data, &mut pos)? as usize;
        let mut columns = Vec::with_capacity(n_props);
        for _ in 0..n_props {
            let key_code = read_u32(data, &mut pos)?;
            let key = dict
                .get(key_code)
                .ok_or("metadata prop key code OOB")?
                .to_string();
            let disc = read_u16(data, &mut pos)?;
            let _vtype = read_u16(data, &mut pos)?;
            columns.push(MetaColumn { key, disc });
        }
        rel_tables.push(MetaRelTable { edge_type, columns });
    }
    let total_nodes = read_u64(data, &mut pos)?;
    let total_edges = read_u64(data, &mut pos)?;
    Ok(Metadata {
        node_tables,
        rel_tables,
        total_nodes,
        total_edges,
    })
}

struct NodeTableRec {
    id: u16,
    column_start: u32,
    column_count: u32,
    row_count: u64,
}

fn read_node_table_record(bytes: &Bytes, index: usize) -> Result<NodeTableRec, String> {
    let base = index * 24;
    if base + 24 > bytes.len() {
        return Err("NodeTableDirectory truncated".into());
    }
    let b = &bytes[base..base + 24];
    let id = u16::from_le_bytes([b[0], b[1]]);
    let reserved_a = u16::from_le_bytes([b[2], b[3]]);
    if reserved_a != 0 {
        return Err("NodeTableDirectory reserved_a non-zero".into());
    }
    let column_start = u32::from_le_bytes([b[4], b[5], b[6], b[7]]);
    let column_count = u32::from_le_bytes([b[8], b[9], b[10], b[11]]);
    let row_count = u64::from_le_bytes([b[12], b[13], b[14], b[15], b[16], b[17], b[18], b[19]]);
    let reserved_b = u32::from_le_bytes([b[20], b[21], b[22], b[23]]);
    if reserved_b != 0 {
        return Err("NodeTableDirectory reserved_b non-zero".into());
    }
    Ok(NodeTableRec {
        id,
        column_start,
        column_count,
        row_count,
    })
}

struct RelTableRec {
    id: u16,
    src_tid: u16,
    dst_tid: u16,
    column_start: u32,
    column_count: u32,
    edge_count: u64,
}

fn read_rel_table_record(bytes: &Bytes, index: usize) -> Result<RelTableRec, String> {
    let base = index * 24;
    if base + 24 > bytes.len() {
        return Err("RelTableDirectory truncated".into());
    }
    let b = &bytes[base..base + 24];
    let id = u16::from_le_bytes([b[0], b[1]]);
    let src_tid = u16::from_le_bytes([b[2], b[3]]);
    let dst_tid = u16::from_le_bytes([b[4], b[5]]);
    let reserved = u16::from_le_bytes([b[6], b[7]]);
    if reserved != 0 {
        return Err("RelTableDirectory reserved non-zero".into());
    }
    let column_start = u32::from_le_bytes([b[8], b[9], b[10], b[11]]);
    let column_count = u32::from_le_bytes([b[12], b[13], b[14], b[15]]);
    let edge_count = u64::from_le_bytes([b[16], b[17], b[18], b[19], b[20], b[21], b[22], b[23]]);
    Ok(RelTableRec {
        id,
        src_tid,
        dst_tid,
        column_start,
        column_count,
        edge_count,
    })
}

fn column_body_slice(block_index: &Bytes, bodies: &Bytes, col_idx: usize) -> Result<Bytes, String> {
    let base = col_idx * 12;
    if base + 12 > block_index.len() {
        return Err("ColumnBlockIndex truncated".into());
    }
    let b = &block_index[base..base + 12];
    let byte_offset = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize;
    let byte_len = u32::from_le_bytes([b[4], b[5], b[6], b[7]]) as usize;
    let end = byte_offset
        .checked_add(byte_len)
        .ok_or("column body range overflow")?;
    if end > bodies.len() {
        return Err("column body out of range".into());
    }
    Ok(bodies.slice(byte_offset..end))
}

pub(crate) fn write_column_body(
    buf: &mut Vec<u8>,
    codec: &ColumnCodec,
    string_index: &FxHashMap<String, u32>,
) -> Result<(), String> {
    match codec {
        ColumnCodec::Dict(d) => {
            // v5 Dict body: [disc=1][codes_len u32][global codes as u32 LE]
            // Codes index the payload-global StringOffsets/StringBytes table.
            buf.push(1);
            write_u32(buf, d.code_count() as u32);
            let entries = d.dictionary_entries();
            for i in 0..d.code_count() {
                let local_code = d.code_at(i).unwrap_or(0) as usize;
                let s = entries
                    .get(local_code)
                    .map(|a| a.as_ref())
                    .ok_or("dict local code OOB")?;
                let gcode = *string_index
                    .get(s)
                    .ok_or_else(|| format!("dict string not interned: {s}"))?;
                write_u32(buf, gcode);
            }
        }
        other => {
            // Reuse v1 flat layout for non-dict codecs (already Bytes-friendly on read).
            let mut tmp = Vec::new();
            other.write_to(&mut tmp);
            buf.extend_from_slice(&tmp);
        }
    }
    Ok(())
}

#[allow(dead_code)] // reserved for the writable open path (Milestone W)
fn read_column_body(
    body: &Bytes,
    expected_disc: u16,
    global_dict: &MappedStringDictionary,
) -> Result<ColumnCodec, String> {
    read_column_body_with_index(body, expected_disc, global_dict, None)
}

fn read_column_body_with_index(
    body: &Bytes,
    expected_disc: u16,
    global_dict: &MappedStringDictionary,
    code_index: Option<Bytes>,
) -> Result<ColumnCodec, String> {
    let bytes = body.as_ref();
    if bytes.is_empty() {
        return Err("empty column body".into());
    }
    let disc = bytes[0];
    if u16::from(disc) != expected_disc && expected_disc != 0 {
        // expected_disc from metadata; still trust body disc for decoding.
    }
    if disc == 1 {
        // v5 mapped dict: [disc=1][codes_len u32][global codes...]
        let mut pos = 1usize;
        let codes_len = read_u32(bytes, &mut pos)? as usize;
        let need = codes_len.checked_mul(4).ok_or("dict codes overflow")?;
        if pos + need > bytes.len() {
            return Err("truncated dict codes".into());
        }
        let codes_bytes = body.slice(pos..pos + need);
        return Ok(ColumnCodec::Dict(
            DictionaryEncoding::from_mapped_strings_with_index(
                global_dict.offsets_bytes(),
                global_dict.string_bytes(),
                codes_bytes,
                codes_len,
                code_index,
            )?,
        ));
    }
    // Non-dict: use existing v1 reader (zero-copy for bitpacked etc.).
    let mut pos = 0usize;
    ColumnCodec::read_from(body, &mut pos).map_err(|e| e.to_string())
}

fn slice_u32_range(bytes: &Bytes, start_elem: usize, count: usize) -> Result<Bytes, String> {
    let start = start_elem
        .checked_mul(4)
        .ok_or("u32 range start overflow")?;
    let end = start_elem
        .checked_add(count)
        .and_then(|e| e.checked_mul(4))
        .ok_or("u32 range end overflow")?;
    if end > bytes.len() {
        return Err(format!(
            "u32 range [{start_elem}, +{count}) exceeds segment length {}",
            bytes.len() / 4
        ));
    }
    Ok(bytes.slice(start..end))
}

pub(crate) fn intern_zone_strings(zm: &ZoneMap, intern: &mut impl FnMut(&str) -> u32) {
    if let Some(grafeo_common::types::Value::String(s)) = &zm.min {
        let _ = intern(s.as_str());
    }
    if let Some(grafeo_common::types::Value::String(s)) = &zm.max {
        let _ = intern(s.as_str());
    }
}

pub(crate) fn codec_disc(codec: &ColumnCodec) -> u16 {
    match codec {
        ColumnCodec::BitPacked(_) => 0,
        ColumnCodec::Dict(_) => 1,
        ColumnCodec::Bitmap(_) => 2,
        ColumnCodec::Int8Vector { .. } => 3,
        ColumnCodec::Float64(_) => 4,
        ColumnCodec::Float32Vector { .. } => 5,
        ColumnCodec::RawI64(_) => 6,
    }
}

pub(crate) fn value_type_code(codec: &ColumnCodec) -> u16 {
    codec_disc(codec)
}

fn column_type_from_disc(disc: u16) -> ColumnType {
    match disc {
        0 | 6 => ColumnType::Int64,
        1 => ColumnType::DictString,
        2 => ColumnType::Bool,
        4 => ColumnType::Float64,
        _ => ColumnType::DictString,
    }
}

pub(crate) fn append_u32_array(buf: &mut Vec<u8>, values: &[u32]) {
    for &v in values {
        write_u32(buf, v);
    }
}

fn estimate_schema_bytes(store: &CompactStore) -> usize {
    // Lower-bound estimate of bounded schema/owner heap.
    let labels: usize = store.table_id_to_label.iter().map(|s| s.len() + 32).sum();
    let types: usize = store
        .rel_table_id_to_type
        .iter()
        .map(|s| s.len() + 32)
        .sum();
    let tables = store.node_tables_by_id.len() * 256 + store.rel_tables_by_id.len() * 256;
    labels + types + tables + 4096
}

pub(crate) fn align_up(value: u64, align: u64) -> u64 {
    debug_assert!(align.is_power_of_two());
    (value + (align - 1)) & !(align - 1)
}

pub(crate) fn write_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub(crate) fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub(crate) fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
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
    let v = u64::from_le_bytes([
        data[*pos],
        data[*pos + 1],
        data[*pos + 2],
        data[*pos + 3],
        data[*pos + 4],
        data[*pos + 5],
        data[*pos + 6],
        data[*pos + 7],
    ]);
    *pos += 8;
    Ok(v)
}
