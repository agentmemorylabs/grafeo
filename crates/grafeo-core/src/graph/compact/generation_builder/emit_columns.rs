//! Bounded column body emission (G-EM0.5b D0.8.6 body pass).
//!
//! Replays the property-occurrence run (sorted by `(table_id, prop_key,
//! row_offset)`) and emits each column's body bytes into the shared
//! `ColumnBodies` spool sink, plus the per-column directory records
//! (`ColumnDirectory`, `ColumnBlockIndex`). Byte-exact with the eager
//! `write_column_body` path: the column's values are fed through the
//! incremental [`ColumnEncoder`] in row order, producing the identical
//! `ColumnCodec`, then serialized via the production `write_column_body`.
//!
//! Boundedness: only the **current** column's values are retained (one
//! column at a time, contiguous in the sorted occurrence run). Absent rows
//! (sparse) and present-null rows are emitted as placeholders in the typed
//! body; the three-way distinction is carried by the `ColumnRowPresence` /
//! `ColumnRowNull` companion segments (D0.8.0), written here from the same
//! stream.

use crate::graph::compact::column::ColumnCodec;
use crate::graph::compact::generation::emit::column::ColumnEncoder;
use crate::graph::compact::generation::emit::sink::SegmentSink;
use crate::graph::compact::generation::{
    CancelToken, ExternalRunMerger, GenerationBudget, GenerationError, GenerationMetrics,
    RunSetLease,
};
use crate::graph::compact::generation_builder::column_pass::ColumnGeometry;
use crate::graph::compact::generation_builder::emit_meta::{CodecKind, w16, w32, w64};
use crate::graph::compact::mapped::SegmentKind;
use crate::graph::compact::zone_map::ZoneMap;
use grafeo_common::types::Value;

/// Decodes one occurrence payload (mirrors column_pass::decode_occ_value).
fn decode_occ(payload: &[u8]) -> Result<Value, GenerationError> {
    if payload.is_empty() {
        return Err(GenerationError::Codec("empty occurrence".into()));
    }
    Ok(match payload[0] {
        0 => Value::Null,
        1 => Value::Int64(i64::from_le_bytes(
            payload
                .get(1..9)
                .ok_or_else(|| GenerationError::Codec("int occ".into()))?
                .try_into()
                .unwrap(),
        )),
        2 => Value::Bool(payload.get(1).copied().unwrap_or(0) != 0),
        4 => Value::Float64(f64::from_le_bytes(
            payload
                .get(1..9)
                .ok_or_else(|| GenerationError::Codec("float occ".into()))?
                .try_into()
                .unwrap(),
        )),
        3 => {
            let b = payload
                .get(1..)
                .ok_or_else(|| GenerationError::Codec("str occ".into()))?;
            if b.len() < 4 {
                return Err(GenerationError::Codec("str occ len".into()));
            }
            let len = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
            let s = std::str::from_utf8(
                b.get(4..4 + len)
                    .ok_or_else(|| GenerationError::Codec("str range".into()))?,
            )
            .map_err(|_| GenerationError::Codec("str utf8".into()))?;
            Value::String(s.into())
        }
        5 => {
            let b = payload
                .get(1..)
                .ok_or_else(|| GenerationError::Codec("vec occ".into()))?;
            if b.len() < 2 {
                return Err(GenerationError::Codec("vec occ dims".into()));
            }
            let dims = u16::from_le_bytes([b[0], b[1]]) as usize;
            let mut v = Vec::with_capacity(dims);
            let mut pos = 2;
            for _ in 0..dims {
                let chunk = b
                    .get(pos..pos + 4)
                    .ok_or_else(|| GenerationError::Codec("vec trunc".into()))?;
                v.push(f32::from_le_bytes(chunk.try_into().unwrap()));
                pos += 4;
            }
            Value::Vector(std::sync::Arc::from(v))
        }
        other => return Err(GenerationError::Codec(format!("bad occ tag {other}"))),
    })
}

fn split_occ_key(key: &[u8]) -> Result<(u16, &str, u64), GenerationError> {
    if key.len() < 10 {
        return Err(GenerationError::Codec("occ key short".into()));
    }
    let tid = u16::from_be_bytes([key[0], key[1]]);
    let off_start = key.len() - 8;
    let prop = std::str::from_utf8(&key[2..off_start])
        .map_err(|_| GenerationError::Codec("occ key utf8".into()))?;
    let off = u64::from_be_bytes(key[off_start..].try_into().unwrap());
    Ok((tid, prop, off))
}

/// Maps a geometry family to its `CodecKind`.
pub fn codec_kind_of(g: &ColumnGeometry) -> CodecKind {
    if g.has_string {
        CodecKind::Dict
    } else if g.vector_dims.is_some() {
        CodecKind::Float32Vector
    } else if g.saw_signed_int {
        CodecKind::RawI64
    } else if g.min_float.is_some() || g.max_float.is_some() {
        CodecKind::Float64
    } else if g.min_int.is_some() || g.max_int.is_some() {
        CodecKind::BitPacked
    } else if g.saw_true || g.saw_false {
        CodecKind::Bitmap
    } else {
        CodecKind::Dict // empty column
    }
}

/// One emitted column's directory + zone-map data.
#[derive(Debug)]
pub struct EmittedColumn {
    /// Flat column index (directory order).
    pub column_index: u32,
    /// Codec kind.
    pub kind: CodecKind,
    /// Byte length of the serialized body.
    pub body_len: u32,
    /// Body start offset within ColumnBodies.
    pub body_start: u32,
    /// Codec logical row count.
    pub codec_len: u32,
    /// Per-block zone maps computed from the emitted codec.
    /// Empty when the column has no zone maps (e.g. Vector columns).
    pub block_zone_maps: Vec<ZoneMap>,
}

/// Result of the column body pass.
#[derive(Debug, Default)]
pub struct ColumnEmissionResult {
    /// Emitted columns in directory order.
    pub columns: Vec<EmittedColumn>,
    /// Presence companion records `(column_index, row_count, bits)`.
    pub presence: Vec<(u32, u32, Vec<bool>)>,
    /// Null companion records `(column_index, row_count, bits)`.
    pub null: Vec<(u32, u32, Vec<bool>)>,
}

/// Replays the occurrence run, emitting column bodies into `bodies_sink` and
/// returning per-column directory data + presence/null records.
///
/// `geometries` must be in the same order the occurrence run yields columns
/// (`(table_id, prop_key)` ascending). `string_index` resolves Dict global
/// codes (from the global dictionary pass).
///
/// # Errors
///
/// Codec, budget, or I/O failure.
#[allow(clippy::too_many_arguments)]
pub fn emit_column_bodies(
    occ_lease: &RunSetLease,
    merger: &mut dyn ExternalRunMerger,
    geometries: &[ColumnGeometry],
    string_index: &grafeo_common::utils::hash::FxHashMap<String, u32>,
    bodies_sink: &mut dyn SegmentSink,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<ColumnEmissionResult, GenerationError> {
    let mut result = ColumnEmissionResult::default();
    let mut geo_iter = geometries.iter();
    let mut current_geo: Option<&ColumnGeometry> = None;
    let mut encoder: Option<ColumnEncoder> = None;
    let mut col_values: Vec<Option<Value>> = Vec::new();
    let mut col_row_count = 0u64;
    let mut presence_bits: Vec<bool> = Vec::new();
    let mut null_bits: Vec<bool> = Vec::new();
    let mut body_cursor = 0u64;
    let mut next_row: u64 = 0;

    let mut flush = |result: &mut ColumnEmissionResult,
                     geo: Option<&ColumnGeometry>,
                     encoder: Option<ColumnEncoder>,
                     values: &mut Vec<Option<Value>>,
                     row_count: u64,
                     presence: &mut Vec<bool>,
                     null: &mut Vec<bool>,
                     body_cursor: &mut u64|
     -> Result<(), GenerationError> {
        let (Some(g), Some(enc)) = (geo, encoder) else {
            return Ok(());
        };
        let kind = codec_kind_of(g);
        let column_index = result.columns.len() as u32;
        let mut string_occ = Vec::new();
        let (codec, _col_type, _zm) = enc.finish(&mut string_occ)?;
        // Compute per-block zone maps from the emitted codec (byte-exact with eager).
        let block_zms = crate::graph::compact::zone_map::compute_block_zone_maps(&codec);
        // Serialize the body via production write_column_body.
        let mut body = Vec::new();
        crate::graph::compact::section_v5::write_column_body(&mut body, &codec, string_index)
            .map_err(GenerationError::Codec)?;
        let body_start = *body_cursor;
        bodies_sink.write(&body)?;
        *body_cursor += body.len() as u64;
        result.columns.push(EmittedColumn {
            column_index,
            kind,
            body_len: body.len() as u32,
            body_start: u32::try_from(body_start).map_err(|_| {
                GenerationError::WireWidthOverflow {
                    what: "col_body_offset",
                    count: body_start,
                    max: u64::from(u32::MAX),
                }
            })?,
            codec_len: codec.len() as u32,
            block_zone_maps: block_zms,
        });
        if g.needs_presence() {
            result
                .presence
                .push((column_index, row_count as u32, presence.clone()));
        }
        if g.needs_null() {
            result
                .null
                .push((column_index, row_count as u32, null.clone()));
        }
        values.clear();
        presence.clear();
        null.clear();
        Ok(())
    };

    merger.merge_all(&occ_lease.handles, budget, metrics, cancel, &mut |rec| {
        let (tid, prop, row_off) = split_occ_key(&rec.key)?;
        let value = decode_occ(&rec.payload)?;

        // Column transition.
        let matches = current_geo.is_some_and(|g| g.table_id == tid && g.key == prop);
        if !matches {
            // Fill trailing absent rows for the previous column.
            if let (Some(prev_g), Some(prev_enc)) = (current_geo, encoder.as_mut()) {
                let prev_family = family_of(prev_g);
                while next_row < prev_g.row_count {
                    prev_enc.push_placeholder(prev_family)?;
                    presence_bits.push(false);
                    null_bits.push(false);
                    next_row += 1;
                }
            }
            flush(
                &mut result,
                current_geo,
                encoder.take(),
                &mut col_values,
                col_row_count,
                &mut presence_bits,
                &mut null_bits,
                &mut body_cursor,
            )?;
            current_geo = geo_iter.next();
            let g = current_geo.ok_or_else(|| {
                GenerationError::Codec(format!("occurrence for unknown column {tid}:{prop}"))
            })?;
            if g.table_id != tid || g.key != prop {
                return Err(GenerationError::Codec(format!(
                    "geometry/occurrence order mismatch: geometry {}:{} vs occurrence {tid}:{prop}",
                    g.table_id, g.key
                )));
            }
            encoder = Some(ColumnEncoder::new(format!("table {tid} column {prop}")));
            col_row_count = g.row_count;
            next_row = 0;
        }
        let enc = encoder.as_mut().expect("just set");
        let g = current_geo.expect("geometry set");
        let family = family_of(g);

        // Fill absent rows (sparse) with placeholders up to this row offset.
        while next_row < row_off {
            enc.push_placeholder(family)?;
            presence_bits.push(false);
            null_bits.push(false);
            next_row += 1;
        }
        // This present row.
        let is_null = matches!(value, Value::Null);
        presence_bits.push(true);
        null_bits.push(is_null);
        if is_null {
            enc.push_placeholder(family)?;
        } else {
            enc.push(Some(&value))?;
        }
        next_row += 1;
        Ok(())
    })?;

    // Trailing absent rows in the final column.
    if let (Some(g), Some(enc)) = (current_geo, encoder.as_mut()) {
        let family = family_of(g);
        while next_row < g.row_count {
            enc.push_placeholder(family)?;
            presence_bits.push(false);
            null_bits.push(false);
            next_row += 1;
        }
    }

    flush(
        &mut result,
        current_geo,
        encoder.take(),
        &mut col_values,
        col_row_count,
        &mut presence_bits,
        &mut null_bits,
        &mut body_cursor,
    )?;
    Ok(result)
}

/// Maps a geometry to its column family name (for placeholder encoding).
fn family_of(g: &ColumnGeometry) -> &'static str {
    if g.has_string {
        "String"
    } else if g.vector_dims.is_some() {
        "Vector"
    } else if g.saw_signed_int || g.min_int.is_some() {
        "Int64"
    } else if g.min_float.is_some() || g.max_float.is_some() {
        "Float64"
    } else if g.saw_true || g.saw_false {
        "Bool"
    } else {
        "String" // empty column placeholder family
    }
}

/// Writes the ColumnDirectory + ColumnBlockIndex + per-table directory rows.
///
/// Mirrors `emit_canonical_descriptors` directory layout byte-exactly.
pub fn write_directory_segments(
    columns: &[EmittedColumn],
    node_tables: &[(u16, u64, Vec<u32>)], // (tid, row_count, col_indices)
    rel_tables: &[(u16, u16, u16, u64, Vec<u32>)], // (rid, src, dst, edge_count, col_indices)
    node_dir: &mut Vec<u8>,
    rel_dir: &mut Vec<u8>,
    col_dir: &mut Vec<u8>,
    col_block_index: &mut Vec<u8>,
) -> Result<(), GenerationError> {
    // ColumnDirectory + ColumnBlockIndex (flat, all columns in order).
    for col in columns {
        w16(col_dir, col.kind.disc());
        w16(col_dir, col.kind.value_type());
        w32(col_dir, col.column_index);
        w32(col_dir, 1); // block_count (single block, matching eager)
        w64(col_dir, u64::from(col.codec_len));
        w32(col_dir, 0);
        w32(col_block_index, col.body_start);
        w32(col_block_index, col.body_len);
        w32(col_block_index, col.codec_len);
    }
    // NodeTableDirectory.
    let mut running_col = 0u32;
    for (tid, row_count, col_indices) in node_tables {
        w16(node_dir, *tid);
        w16(node_dir, 0);
        let col_start = col_indices.first().copied().unwrap_or(running_col);
        w32(node_dir, col_start);
        w32(node_dir, col_indices.len() as u32);
        w64(node_dir, *row_count);
        w32(node_dir, 0);
        running_col = col_start + col_indices.len() as u32;
    }
    // RelTableDirectory.
    let mut next_col = running_col;
    for (rid, src, dst, edge_count, col_indices) in rel_tables {
        w16(rel_dir, *rid);
        w16(rel_dir, *src);
        w16(rel_dir, *dst);
        w16(rel_dir, 0);
        // col_start = first column index for this rel table, or the
        // running next column index when the rel table has no columns
        // (matching the eager path's `column_index` snapshot before
        // iterating keys, even when keys is empty).
        let col_start = col_indices.first().copied().unwrap_or(next_col);
        w32(rel_dir, col_start);
        w32(rel_dir, col_indices.len() as u32);
        w64(rel_dir, *edge_count);
        next_col = col_start + col_indices.len() as u32;
    }
    Ok(())
}

/// Kind codes for the column segments.
#[must_use]
pub fn column_segment_kinds() -> (SegmentKind, SegmentKind, SegmentKind) {
    (
        SegmentKind::ColumnDirectory,
        SegmentKind::ColumnBlockIndex,
        SegmentKind::ColumnBodies,
    )
}
