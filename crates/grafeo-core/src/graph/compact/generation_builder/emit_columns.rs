//! Bounded column body emission (G-EM0.5b D0.8.6 body pass).
//!
//! Replays the property-occurrence run (sorted by `(table_id, prop_key,
//! row_offset)`) and emits each column's body bytes into the shared
//! `ColumnBodies` spool sink, plus the per-column directory records
//! (`ColumnDirectory`, `ColumnBlockIndex`). Bodies stream directly through
//! [`StreamingBodyWriter`]; presence/null companions stream through
//! [`BitByteEmitter`] into optional spool sinks — no whole-column
//! `ColumnCodec`, body `Vec`, or `Vec<bool>` retention.

use crate::graph::compact::generation::emit::sink::SegmentSink;
use crate::graph::compact::generation::emit::streaming_column::{
    BitByteEmitter, StreamingBodyWriter,
};
use crate::graph::compact::generation::{
    CancelToken, ExternalRunMerger, GenerationBudget, GenerationError, GenerationMetrics,
    RunSetLease,
};
use crate::graph::compact::generation_builder::column_pass::ColumnGeometry;
use crate::graph::compact::generation_builder::emit_meta::{CodecKind, w16, w32, w64};
use crate::graph::compact::mapped::SegmentKind;
use crate::graph::compact::zone_map::ZoneMap;
use grafeo_common::types::Value;
use grafeo_common::utils::hash::FxHashMap;

/// Sequential reader over the per-column DictValue chunk file.
///
/// The orchestrator's remap pre-pass writes one chunk per Dict column (in
/// `(table_id, prop_key)` ascending order), each chunk holding that column's
/// distinct `string → global_code` pairs. Chunk layout (LE):
/// `[tid u16][prop_len u16][prop][count u32]` then
/// `count × [str_len u32][string][code u32]`; clean EOF (0 bytes) ends the
/// file.
///
/// Only the **current** column's map is ever resident; `map_for` advances
/// the underlying reader in lockstep with the column-ordered consumer and
/// fails closed on any order mismatch.
pub(crate) struct DictChunkReader<'a> {
    reader: &'a mut dyn std::io::Read,
    /// Next unconsumed chunk (column + map).
    pending: Option<(u16, Vec<u8>, FxHashMap<String, u32>)>,
}

impl<'a> DictChunkReader<'a> {
    /// Wraps a byte reader positioned at the start of a chunk file.
    #[must_use]
    pub(crate) fn new(reader: &'a mut dyn std::io::Read) -> Self {
        Self {
            reader,
            pending: None,
        }
    }

    /// Loads the string→code map for `(tid, prop)`, or an empty map when
    /// that column has no DictValue chunk. Fails closed on chunk/consumer
    /// order mismatches.
    ///
    /// # Errors
    ///
    /// Codec or I/O failure.
    pub(crate) fn map_for(
        &mut self,
        tid: u16,
        prop: &str,
    ) -> Result<FxHashMap<String, u32>, GenerationError> {
        let target: (u16, &[u8]) = (tid, prop.as_bytes());
        if self.pending.is_none() {
            match self.read_chunk()? {
                None => return Ok(FxHashMap::default()),
                Some(chunk) => self.pending = Some(chunk),
            }
        }
        let (ptid, pprop, _) = self.pending.as_ref().expect("just set");
        match (*ptid, pprop.as_slice()).cmp(&target) {
            std::cmp::Ordering::Equal => {
                let (_, _, map) = self.pending.take().expect("checked");
                Ok(map)
            }
            std::cmp::Ordering::Greater => {
                // The consumer's column has no chunk; keep pending for a
                // later column.
                Ok(FxHashMap::default())
            }
            std::cmp::Ordering::Less => Err(GenerationError::Codec(format!(
                "dict chunk order mismatch: chunk for table {ptid} column {} before table {tid} column {prop}",
                String::from_utf8_lossy(pprop)
            ))),
        }
    }

    /// Fails closed unless every chunk has been consumed (used at the end of
    /// the column-body pass, which visits every column).
    ///
    /// # Errors
    ///
    /// Codec or I/O failure.
    pub(crate) fn verify_drained(&mut self) -> Result<(), GenerationError> {
        if self.pending.is_some() {
            return Err(GenerationError::Codec(
                "dict chunk left unconsumed after column pass".into(),
            ));
        }
        match self.read_chunk()? {
            None => Ok(()),
            Some(_) => Err(GenerationError::Codec(
                "dict chunk left unconsumed after column pass".into(),
            )),
        }
    }

    fn read_chunk(
        &mut self,
    ) -> Result<Option<(u16, Vec<u8>, FxHashMap<String, u32>)>, GenerationError> {
        let mut hdr = [0u8; 4];
        if !read_full(self.reader, &mut hdr)? {
            return Ok(None);
        }
        let tid = u16::from_le_bytes([hdr[0], hdr[1]]);
        let prop_len = u16::from_le_bytes([hdr[2], hdr[3]]) as usize;
        let mut prop = vec![0u8; prop_len];
        if !read_full(self.reader, &mut prop)? {
            return Err(GenerationError::Codec("dict chunk prop truncated".into()));
        }
        let mut cnt = [0u8; 4];
        if !read_full(self.reader, &mut cnt)? {
            return Err(GenerationError::Codec("dict chunk count truncated".into()));
        }
        let count = u32::from_le_bytes(cnt) as usize;
        let mut map = FxHashMap::default();
        map.reserve(count.min(1 << 16));
        for _ in 0..count {
            let mut slen = [0u8; 4];
            if !read_full(self.reader, &mut slen)? {
                return Err(GenerationError::Codec("dict chunk entry truncated".into()));
            }
            let sl = u32::from_le_bytes(slen) as usize;
            let mut s = vec![0u8; sl];
            if !read_full(self.reader, &mut s)? {
                return Err(GenerationError::Codec("dict chunk string truncated".into()));
            }
            let mut code = [0u8; 4];
            if !read_full(self.reader, &mut code)? {
                return Err(GenerationError::Codec("dict chunk code truncated".into()));
            }
            let text = std::str::from_utf8(&s)
                .map_err(|_| GenerationError::Codec("dict chunk string not UTF-8".into()))?;
            map.insert(text.to_string(), u32::from_le_bytes(code));
        }
        Ok(Some((tid, prop, map)))
    }
}

/// Reads `buf.len()` bytes, returning `Ok(false)` on a clean EOF at the
/// start of the buffer and failing closed on a torn read.
fn read_full(r: &mut dyn std::io::Read, buf: &mut [u8]) -> Result<bool, GenerationError> {
    let mut filled = 0usize;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => {
                if filled == 0 {
                    return Ok(false);
                }
                return Err(GenerationError::Codec("dict chunk truncated".into()));
            }
            Ok(n) => filled += n,
            Err(e) => return Err(GenerationError::Io(e.to_string())),
        }
    }
    Ok(true)
}

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
    /// Owning table id (node table or `0x8000 | rel_id`).
    pub table_id: u16,
    /// Property key (table-scoped identity).
    pub key: String,
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
    /// True when at least one column wrote a presence companion record.
    pub emitted_presence: bool,
    /// True when at least one column wrote a null companion record.
    pub emitted_null: bool,
}

/// Replays the occurrence run, emitting column bodies into `bodies_sink` and
/// streaming presence/null companions into optional sinks.
///
/// `geometries` must be in the same order the occurrence run yields columns
/// (`(table_id, prop_key)` ascending). `dict_chunks` resolves Dict global
/// codes per column from the remap-run chunk file (one column's map at a
/// time, discarded after the column flush).
///
/// # Errors
///
/// Codec, budget, or I/O failure.
#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_column_bodies(
    occ_lease: &RunSetLease,
    merger: &mut dyn ExternalRunMerger,
    geometries: &[ColumnGeometry],
    dict_chunks: &mut DictChunkReader,
    bodies_sink: &mut dyn SegmentSink,
    presence_sink: &mut dyn SegmentSink,
    null_sink: &mut dyn SegmentSink,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<ColumnEmissionResult, GenerationError> {
    let mut result = ColumnEmissionResult::default();
    let mut geo_iter = geometries.iter();
    let mut current_geo: Option<&ColumnGeometry> = None;
    let mut writer: Option<StreamingBodyWriter> = None;
    let mut presence_emitter: Option<BitByteEmitter> = None;
    let mut null_emitter: Option<BitByteEmitter> = None;
    let mut body_cursor = 0u64;
    let mut next_row: u64 = 0;

    let flush = |result: &mut ColumnEmissionResult,
                     geo: Option<&ColumnGeometry>,
                     writer: Option<StreamingBodyWriter>,
                     presence_emitter: &mut Option<BitByteEmitter>,
                     null_emitter: &mut Option<BitByteEmitter>,
                     body_cursor: &mut u64,
                     bodies_sink: &mut dyn SegmentSink,
                     presence_sink: &mut dyn SegmentSink,
                     null_sink: &mut dyn SegmentSink|
     -> Result<(), GenerationError> {
        let (Some(_g), Some(w)) = (geo, writer) else {
            return Ok(());
        };
        if let Some(em) = presence_emitter.as_mut() {
            em.finish_record(presence_sink)?;
        }
        if let Some(em) = null_emitter.as_mut() {
            em.finish_record(null_sink)?;
        }
        let kind = codec_kind_of(_g);
        let column_index = result.columns.len() as u32;
        let body_start = *body_cursor;
        let (body_len, codec_len, block_zms) = w.finish(bodies_sink)?;
        *body_cursor += body_len;
        result.columns.push(EmittedColumn {
            column_index,
            table_id: _g.table_id,
            key: _g.key.clone(),
            kind,
            body_len: u32::try_from(body_len).map_err(|_| GenerationError::WireWidthOverflow {
                what: "col_body_len",
                count: body_len,
                max: u64::from(u32::MAX),
            })?,
            body_start: u32::try_from(body_start).map_err(|_| GenerationError::WireWidthOverflow {
                what: "col_body_offset",
                count: body_start,
                max: u64::from(u32::MAX),
            })?,
            codec_len,
            block_zone_maps: block_zms,
        });
        *presence_emitter = None;
        *null_emitter = None;
        Ok(())
    };

    merger.merge_all(&occ_lease.handles, budget, metrics, cancel, &mut |rec| {
        let (tid, prop, row_off) = split_occ_key(&rec.key)?;
        let value = decode_occ(&rec.payload)?;

        // Column transition.
        let matches = current_geo.is_some_and(|g| g.table_id == tid && g.key == prop);
        if !matches {
            // Fill trailing absent rows for the previous column.
            if let Some(w) = writer.as_mut() {
                let prev_g = current_geo.expect("geometry set");
                while next_row < prev_g.row_count {
                    w.push_placeholder(bodies_sink)?;
                    if let Some(em) = presence_emitter.as_mut() {
                        em.push_bit(presence_sink, false)?;
                    }
                    if let Some(em) = null_emitter.as_mut() {
                        em.push_bit(null_sink, false)?;
                    }
                    next_row += 1;
                }
            }
            flush(
                &mut result,
                current_geo,
                writer.take(),
                &mut presence_emitter,
                &mut null_emitter,
                &mut body_cursor,
                bodies_sink,
                presence_sink,
                null_sink,
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
            let (w, p_em, n_em) = begin_column_streams(
                g,
                dict_chunks,
                &mut result,
                bodies_sink,
                presence_sink,
                null_sink,
                tid,
                prop,
            )?;
            writer = Some(w);
            presence_emitter = p_em;
            null_emitter = n_em;
            next_row = 0;
        }
        let w = writer.as_mut().expect("just set");

        // Fill absent rows (sparse) with placeholders up to this row offset.
        while next_row < row_off {
            w.push_placeholder(bodies_sink)?;
            if let Some(em) = presence_emitter.as_mut() {
                em.push_bit(presence_sink, false)?;
            }
            if let Some(em) = null_emitter.as_mut() {
                em.push_bit(null_sink, false)?;
            }
            next_row += 1;
        }
        // This present row.
        let is_null = matches!(value, Value::Null);
        if let Some(em) = presence_emitter.as_mut() {
            em.push_bit(presence_sink, true)?;
        }
        if let Some(em) = null_emitter.as_mut() {
            em.push_bit(null_sink, is_null)?;
        }
        if is_null {
            w.push_placeholder(bodies_sink)?;
        } else {
            w.push(bodies_sink, &value)?;
        }
        next_row += 1;
        Ok(())
    })?;

    // Trailing absent rows in the final column.
    if let Some(w) = writer.as_mut() {
        let g = current_geo.expect("geometry set");
        while next_row < g.row_count {
            w.push_placeholder(bodies_sink)?;
            if let Some(em) = presence_emitter.as_mut() {
                em.push_bit(presence_sink, false)?;
            }
            if let Some(em) = null_emitter.as_mut() {
                em.push_bit(null_sink, false)?;
            }
            next_row += 1;
        }
    }

    flush(
        &mut result,
        current_geo,
        writer.take(),
        &mut presence_emitter,
        &mut null_emitter,
        &mut body_cursor,
        bodies_sink,
        presence_sink,
        null_sink,
    )?;
    // Every Dict column's chunk must have been consumed by the column opens/flushes.
    dict_chunks.verify_drained()?;
    Ok(result)
}

fn begin_column_streams(
    g: &ColumnGeometry,
    dict_chunks: &mut DictChunkReader,
    result: &mut ColumnEmissionResult,
    bodies_sink: &mut dyn SegmentSink,
    presence_sink: &mut dyn SegmentSink,
    null_sink: &mut dyn SegmentSink,
    tid: u16,
    prop: &str,
) -> Result<(StreamingBodyWriter, Option<BitByteEmitter>, Option<BitByteEmitter>), GenerationError> {
    let dict_map = dict_chunks.map_for(g.table_id, &g.key)?;
    let w = StreamingBodyWriter::new(
        bodies_sink,
        g,
        dict_map,
        format!("table {tid} column {prop}"),
    )?;
    let column_index = result.columns.len() as u32;
    let row_count = u32::try_from(g.row_count).map_err(|_| GenerationError::WireWidthOverflow {
        what: "col_row_count",
        count: g.row_count,
        max: u64::from(u32::MAX),
    })?;
    let presence_emitter = if g.needs_presence() {
        result.emitted_presence = true;
        let mut em = BitByteEmitter::new();
        em.begin_record(presence_sink, column_index, row_count)?;
        Some(em)
    } else {
        None
    };
    let null_emitter = if g.needs_null() {
        result.emitted_null = true;
        let mut em = BitByteEmitter::new();
        em.begin_record(null_sink, column_index, row_count)?;
        Some(em)
    } else {
        None
    };
    Ok((w, presence_emitter, null_emitter))
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
