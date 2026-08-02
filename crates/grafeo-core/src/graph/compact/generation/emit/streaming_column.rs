//! Sink-backed streaming column body writer (G-EM0.5b D0.8.6).
//!
//! Unlike the prior accumulator path — which retained a full-column primitive
//! buffer before building a [`ColumnCodec`] — this writer streams v5 body
//! bytes directly into a [`SegmentSink`]. Anonymous retention is bounded to
//! at most one open bit/word, one in-flight scalar value, and charged
//! schema/zone-map metadata.
//!
//! Wire layouts match production [`write_column_body`](super::super::section_v5::write_column_body)
//! / [`ColumnCodec::write_to`](super::super::column::ColumnCodec::write_to).

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

use crate::codec::DEFAULT_BLOCK_ROWS;
use crate::graph::compact::generation::emit::sink::SegmentSink;
use crate::graph::compact::generation::error::GenerationError;
use crate::graph::compact::generation_builder::column_pass::ColumnGeometry;
use crate::graph::compact::zone_map::{ZoneMap, fold_value_into_block_zone_map};
use grafeo_common::types::Value;
use grafeo_common::utils::hash::FxHashMap;

/// LSB-first bit packer with at most one open byte (presence/null companions).
pub(crate) struct BitByteEmitter {
    open_byte: u8,
    bit_in_byte: u8,
}

impl BitByteEmitter {
    /// Creates an empty emitter (no open byte retained across columns).
    #[must_use]
    pub fn new() -> Self {
        Self {
            open_byte: 0,
            bit_in_byte: 0,
        }
    }

    /// Writes one column record header `[column_index u32][row_count u32]`.
    ///
    /// # Errors
    ///
    /// [`GenerationError::Io`] on sink write failure.
    pub fn begin_record(
        &mut self,
        sink: &mut dyn SegmentSink,
        column_index: u32,
        row_count: u32,
    ) -> Result<(), GenerationError> {
        self.open_byte = 0;
        self.bit_in_byte = 0;
        let mut hdr = [0u8; 8];
        hdr[0..4].copy_from_slice(&column_index.to_le_bytes());
        hdr[4..8].copy_from_slice(&row_count.to_le_bytes());
        sink.write(&hdr)
    }

    /// Appends one bit to the current record.
    ///
    /// # Errors
    ///
    /// [`GenerationError::Io`] on sink write failure.
    pub fn push_bit(&mut self, sink: &mut dyn SegmentSink, bit: bool) -> Result<(), GenerationError> {
        if bit {
            self.open_byte |= 1 << self.bit_in_byte;
        }
        self.bit_in_byte += 1;
        if self.bit_in_byte == 8 {
            sink.write(&[self.open_byte])?;
            self.open_byte = 0;
            self.bit_in_byte = 0;
        }
        Ok(())
    }

    /// Flushes a partial final byte for the current record.
    ///
    /// # Errors
    ///
    /// [`GenerationError::Io`] on sink write failure.
    pub fn finish_record(&mut self, sink: &mut dyn SegmentSink) -> Result<(), GenerationError> {
        if self.bit_in_byte > 0 {
            sink.write(&[self.open_byte])?;
            self.open_byte = 0;
            self.bit_in_byte = 0;
        }
        Ok(())
    }
}

impl Default for BitByteEmitter {
    fn default() -> Self {
        Self::new()
    }
}

enum BodyFamily {
    Dict {
        /// `true` for the empty-column family (codes_len = 0).
        empty: bool,
    },
    BitPacked {
        bits: u8,
        open_word: u64,
        index_in_word: usize,
    },
    Bitmap {
        open_word: u64,
        bit_in_word: u8,
    },
    Float64,
    Float32Vector {
        dims: u16,
    },
    RawI64,
    Empty,
}

/// Streams one column's v5 body into a sink.
pub struct StreamingBodyWriter {
    context: String,
    family: BodyFamily,
    dict_map: Option<FxHashMap<String, u32>>,
    rows_written: usize,
    body_len: u64,
    block_maps: Vec<ZoneMap>,
    current_block: ZoneMap,
    block_row: usize,
}

impl StreamingBodyWriter {
    /// Opens a column body on `sink`, writing the v5 header from `geo`.
    ///
    /// `dict_map` supplies string→global_code for Dict columns (one column's
    /// distinct strings; discarded after the column finishes).
    ///
    /// # Errors
    ///
    /// Codec or I/O failure.
    pub fn new(
        sink: &mut dyn SegmentSink,
        geo: &ColumnGeometry,
        dict_map: FxHashMap<String, u32>,
        context: impl Into<String>,
    ) -> Result<Self, GenerationError> {
        let context = context.into();
        let row_count = geo.row_count;

        let family = if geo.has_string {
            BodyFamily::Dict { empty: false }
        } else if geo.vector_dims.is_some() {
            BodyFamily::Float32Vector {
                dims: geo.vector_dims.expect("checked"),
            }
        } else if geo.saw_signed_int || geo.min_int.is_some() {
            if geo.saw_signed_int {
                BodyFamily::RawI64
            } else {
                BodyFamily::BitPacked {
                    bits: geo.max_int_bits.max(1),
                    open_word: 0,
                    index_in_word: 0,
                }
            }
        } else if geo.min_float.is_some() || geo.max_float.is_some() {
            BodyFamily::Float64
        } else if geo.saw_true || geo.saw_false {
            BodyFamily::Bitmap {
                open_word: 0,
                bit_in_word: 0,
            }
        } else {
            BodyFamily::Empty
        };

        let body_len = write_body_header(sink, &family, row_count)?;

        let dict_map = match &family {
            BodyFamily::Dict { empty: false } => Some(dict_map),
            _ => None,
        };

        Ok(Self {
            context,
            family,
            dict_map,
            rows_written: 0,
            body_len,
            block_maps: Vec::new(),
            current_block: ZoneMap::default(),
            block_row: 0,
        })
    }

    /// Feeds one present, non-null row value.
    ///
    /// # Errors
    ///
    /// [`GenerationError::UnsupportedValue`] when the value family does not
    /// match the geometry-locked writer.
    pub fn push(
        &mut self,
        sink: &mut dyn SegmentSink,
        value: &Value,
    ) -> Result<(), GenerationError> {
        self.rows_written += 1;
        self.write_value(sink, value)?;
        self.fold_zone(value);
        Ok(())
    }

    /// Feeds a placeholder for an absent or present-null row.
    ///
    /// # Errors
    ///
    /// [`GenerationError::UnsupportedValue`] on a vector column whose
    /// dimensions are unknown.
    pub fn push_placeholder(&mut self, sink: &mut dyn SegmentSink) -> Result<(), GenerationError> {
        self.rows_written += 1;
        match &self.family {
            // Absent/present-null dict rows use code 0 in the body; presence/null
            // bitmaps carry the three-way distinction (D0.8.0).
            BodyFamily::Dict { .. } => {
                write_bytes(sink, &mut self.body_len, &0u32.to_le_bytes())?;
            }
            BodyFamily::Empty => return Ok(()),
            other => {
                let placeholder = match other {
                    BodyFamily::BitPacked { .. } => Value::Int64(0),
                    BodyFamily::Bitmap { .. } => Value::Bool(false),
                    BodyFamily::Float64 => Value::Float64(0.0),
                    BodyFamily::Float32Vector { dims } => {
                        Value::Vector(std::sync::Arc::from(vec![0.0f32; usize::from(*dims)]))
                    }
                    BodyFamily::RawI64 => Value::Int64(0),
                    BodyFamily::Dict { .. } | BodyFamily::Empty => unreachable!(),
                };
                self.write_value(sink, &placeholder)?;
                self.fold_zone(&placeholder);
            }
        }
        Ok(())
    }

    /// Finishes the column body.
    ///
    /// Returns `(body_len, codec_len, per-block zone maps)`.
    ///
    /// # Errors
    ///
    /// I/O failure flushing trailing packed words.
    pub fn finish(
        mut self,
        sink: &mut dyn SegmentSink,
    ) -> Result<(u64, u32, Vec<ZoneMap>), GenerationError> {
        if let BodyFamily::BitPacked {
            open_word,
            index_in_word,
            ..
        } = self.family
        {
            if index_in_word > 0 {
                write_bytes(sink, &mut self.body_len, &open_word.to_le_bytes())?;
            }
        }
        if let BodyFamily::Bitmap {
            open_word,
            bit_in_word,
        } = self.family
        {
            if bit_in_word > 0 {
                write_bytes(sink, &mut self.body_len, &open_word.to_le_bytes())?;
            }
        }

        let body_len = self.body_len;
        let codec_len = match &self.family {
            BodyFamily::Empty => 0,
            _ => self.rows_written as u32,
        };
        let block_maps = self.finalize_zone_maps(codec_len);
        Ok((body_len, codec_len, block_maps))
    }

    fn write_value(&mut self, sink: &mut dyn SegmentSink, value: &Value) -> Result<(), GenerationError> {
        match value {
            Value::String(s) if matches!(self.family, BodyFamily::Dict { empty: false }) => {
                let code = self.dict_code(s.as_str())?;
                write_bytes(sink, &mut self.body_len, &code.to_le_bytes())?;
            }
            Value::Int64(n) if matches!(self.family, BodyFamily::BitPacked { .. }) => {
                let BodyFamily::BitPacked {
                    bits,
                    ref mut open_word,
                    ref mut index_in_word,
                } = self.family
                else {
                    unreachable!()
                };
                flush_packed_value(
                    sink,
                    &mut self.body_len,
                    bits,
                    open_word,
                    index_in_word,
                    (*n).cast_unsigned(),
                )?;
            }
            Value::Int64(n) if matches!(self.family, BodyFamily::RawI64) => {
                write_bytes(sink, &mut self.body_len, &n.to_le_bytes())?;
            }
            Value::Float64(f) if matches!(self.family, BodyFamily::Float64) => {
                write_bytes(sink, &mut self.body_len, &f.to_le_bytes())?;
            }
            Value::Vector(vec) if matches!(self.family, BodyFamily::Float32Vector { .. }) => {
                let BodyFamily::Float32Vector { dims } = self.family else {
                    unreachable!()
                };
                if vec.len() != usize::from(dims) {
                    return Err(GenerationError::MixedColumnTypes {
                        context: self.context.clone(),
                        kinds: vec!["Vector(dims mismatch)"],
                    });
                }
                for &f in vec.iter() {
                    write_bytes(sink, &mut self.body_len, &f.to_le_bytes())?;
                }
            }
            Value::Bool(v) if matches!(self.family, BodyFamily::Bitmap { .. }) => {
                let BodyFamily::Bitmap {
                    ref mut open_word,
                    ref mut bit_in_word,
                } = self.family
                else {
                    unreachable!()
                };
                if *v {
                    *open_word |= 1u64 << *bit_in_word;
                }
                *bit_in_word += 1;
                if *bit_in_word == 64 {
                    let word_bytes = open_word.to_le_bytes();
                    *open_word = 0;
                    *bit_in_word = 0;
                    write_bytes(sink, &mut self.body_len, &word_bytes)?;
                }
            }
            other => {
                return Err(GenerationError::UnsupportedValue {
                    kind: value_kind_name(other),
                    context: format!("{} (family {:?})", self.context, family_tag(&self.family)),
                });
            }
        }
        Ok(())
    }

    fn dict_code(&self, s: &str) -> Result<u32, GenerationError> {
        let map = self
            .dict_map
            .as_ref()
            .ok_or_else(|| GenerationError::Codec("dict map missing".into()))?;
        map.get(s).copied().ok_or_else(|| {
            GenerationError::Codec(format!("dict string not interned: {s}"))
        })
    }

    fn fold_zone(&mut self, value: &Value) {
        if matches!(self.family, BodyFamily::Empty) {
            return;
        }
        self.current_block.row_count += 1;
        fold_value_into_block_zone_map(&mut self.current_block, value);
        self.block_row += 1;
        let block_rows = DEFAULT_BLOCK_ROWS as usize;
        if self.block_row == block_rows {
            self.block_maps.push(std::mem::take(&mut self.current_block));
            self.block_row = 0;
        }
    }

    fn finalize_zone_maps(&mut self, codec_len: u32) -> Vec<ZoneMap> {
        if codec_len == 0 {
            return vec![ZoneMap {
                row_count: 0,
                ..ZoneMap::default()
            }];
        }
        if self.block_row > 0 || self.block_maps.is_empty() {
            self.block_maps.push(std::mem::take(&mut self.current_block));
        }
        std::mem::take(&mut self.block_maps)
    }
}

fn write_bytes(
    sink: &mut dyn SegmentSink,
    body_len: &mut u64,
    bytes: &[u8],
) -> Result<(), GenerationError> {
    *body_len += bytes.len() as u64;
    sink.write(bytes)
}

fn write_body_header(
    sink: &mut dyn SegmentSink,
    family: &BodyFamily,
    row_count: u64,
) -> Result<u64, GenerationError> {
    let mut len = 0u64;
    let mut write = |sink: &mut dyn SegmentSink, bytes: &[u8]| -> Result<(), GenerationError> {
        len += bytes.len() as u64;
        sink.write(bytes)
    };
    match family {
        BodyFamily::Dict { empty } => {
            let codes_len = if *empty { 0 } else { row_count as u32 };
            write(sink, &[1])?;
            write(sink, &codes_len.to_le_bytes())?;
        }
        BodyFamily::BitPacked { bits, .. } => {
            let b = *bits as usize;
            let values_per_word = if b == 0 { 1 } else { 64 / b };
            let word_count = (row_count as usize).div_ceil(values_per_word) as u32;
            write(sink, &[0, *bits])?;
            write(sink, &(row_count as u32).to_le_bytes())?;
            write(sink, &word_count.to_le_bytes())?;
        }
        BodyFamily::Bitmap { .. } => {
            let word_count = (row_count as usize).div_ceil(64) as u32;
            write(sink, &[2])?;
            write(sink, &(row_count as u32).to_le_bytes())?;
            write(sink, &word_count.to_le_bytes())?;
        }
        BodyFamily::Float64 => {
            write(sink, &[4])?;
            write(sink, &(row_count as u32).to_le_bytes())?;
        }
        BodyFamily::Float32Vector { dims } => {
            let component_count = row_count
                .checked_mul(u64::from(*dims))
                .ok_or_else(|| GenerationError::WireWidthOverflow {
                    what: "vector_component_count",
                    count: row_count,
                    max: u64::MAX,
                })?;
            write(sink, &[5])?;
            write(sink, &dims.to_le_bytes())?;
            write(sink, &(component_count as u32).to_le_bytes())?;
        }
        BodyFamily::RawI64 => {
            write(sink, &[6])?;
            write(sink, &(row_count as u32).to_le_bytes())?;
        }
        BodyFamily::Empty => {
            write(sink, &[1])?;
            write(sink, &0u32.to_le_bytes())?;
        }
    }
    Ok(len)
}

fn flush_packed_value(
    sink: &mut dyn SegmentSink,
    body_len: &mut u64,
    bits: u8,
    open_word: &mut u64,
    index_in_word: &mut usize,
    value: u64,
) -> Result<(), GenerationError> {
    if bits == 0 {
        return Ok(());
    }
    let b = bits as usize;
    let values_per_word = 64 / b;
    let mask = if b >= 64 { u64::MAX } else { (1u64 << b) - 1 };
    let bit_offset = (*index_in_word % values_per_word) * b;
    *open_word |= (value & mask) << bit_offset;
    *index_in_word += 1;
    if *index_in_word % values_per_word == 0 {
        write_bytes(sink, body_len, &open_word.to_le_bytes())?;
        *open_word = 0;
    }
    Ok(())
}

fn family_tag(family: &BodyFamily) -> &'static str {
    match family {
        BodyFamily::Dict { .. } => "Dict",
        BodyFamily::BitPacked { .. } => "BitPacked",
        BodyFamily::Bitmap { .. } => "Bitmap",
        BodyFamily::Float64 => "Float64",
        BodyFamily::Float32Vector { .. } => "Float32Vector",
        BodyFamily::RawI64 => "RawI64",
        BodyFamily::Empty => "Empty",
    }
}

fn value_kind_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Int64(_) => "Int64",
        Value::Float64(_) => "Float64",
        Value::String(_) => "String",
        Value::Vector(_) => "Vector",
        _ => "Unsupported",
    }
}
