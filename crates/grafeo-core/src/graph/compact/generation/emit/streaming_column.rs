//! Streaming column encoder for bounded v5 body emission (G-EM0.5b D0.8.6).
//!
//! Unlike the Phase-0 [`ColumnEncoder`](super::column::ColumnEncoder) — which
//! buffers every row as `Option<Value>` (~40 B/row) before encoding — this
//! encoder appends each value directly to a **family-specific primitive
//! accumulator** chosen from the pre-computed [`ColumnGeometry`]. Anonymous
//! retention drops to the packed output width (1 bit/bool, `bits`/uint,
//! 8 B/i64, 8 B/f64, 4 B/dict-code, 4 B/vector-component); the raw `Value`
//! enum is never held.
//!
//! The produced [`ColumnCodec`] is **byte-identical** to the eager path, so
//! `write_column_body` and `compute_block_zone_maps` are reused unchanged and
//! the golden byte-parity fixture still matches.
//!
//! The family is locked by geometry (the geometry pass already fails closed on
//! mixed families), so `push` trusts the accumulator and never re-infers.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

use crate::codec::{BitVectorBuilder, DictionaryBuilder};
use crate::graph::compact::column::ColumnCodec;
use crate::graph::compact::generation::error::GenerationError;
use crate::graph::compact::generation_builder::column_pass::ColumnGeometry;
use grafeo_common::types::Value;

/// Family-specific primitive accumulator. Exactly one variant is live per
/// column, chosen from geometry at construction.
enum Acc {
    /// Bool column: 1 bit per row.
    Bool(BitVectorBuilder),
    /// Unsigned int column: bit-packed at a fixed width known from geometry.
    UInt {
        words: Vec<u64>,
        bits: u8,
        count: usize,
    },
    /// Signed int column: raw little-endian i64 (8 B/row).
    SInt(Vec<i64>),
    /// Float column: raw f64 (8 B/row).
    Float(Vec<f64>),
    /// String column: dictionary codes (4 B/row) + distinct-string table.
    Str(DictionaryBuilder),
    /// Vector column: flat f32 components (4 B/component).
    Vec { flat: Vec<f32>, dims: u16 },
    /// Empty/all-null column: emitted as an empty Dict (matches eager `None`).
    Empty,
}

/// Streaming column encoder. Construct from geometry, feed values one at a
/// time, then [`finish`](StreamingColumnEncoder::finish) to build the codec.
pub struct StreamingColumnEncoder {
    context: String,
    acc: Acc,
    row_count: usize,
}

impl StreamingColumnEncoder {
    /// Creates an encoder whose accumulator family is locked by `geo`.
    ///
    /// The family selection mirrors the eager encoder's `finish()` decision
    /// exactly (via the same priority as `family_of`) so the produced
    /// [`ColumnCodec`] is byte-identical — including all-null/sparse columns,
    /// where the eager path still emits a family-typed placeholder codec.
    #[must_use]
    pub fn from_geometry(context: impl Into<String>, geo: &ColumnGeometry) -> Self {
        let context = context.into();
        let acc = if geo.has_string {
            Acc::Str(DictionaryBuilder::new())
        } else if let Some(dims) = geo.vector_dims {
            Acc::Vec {
                flat: Vec::new(),
                dims,
            }
        } else if geo.saw_signed_int || geo.min_int.is_some() {
            // Int family. Signed (any negative seen) → raw i64; otherwise
            // bit-packed at the geometry-computed width (≥1 for present
            // non-negative values; default 1 for an all-null int column,
            // matching the eager `bits_needed` floor).
            if geo.saw_signed_int {
                Acc::SInt(Vec::new())
            } else {
                Acc::UInt {
                    words: Vec::new(),
                    bits: geo.max_int_bits.max(1),
                    count: 0,
                }
            }
        } else if geo.min_float.is_some() || geo.max_float.is_some() {
            Acc::Float(Vec::new())
        } else if geo.saw_true || geo.saw_false {
            Acc::Bool(BitVectorBuilder::new())
        } else {
            // No typed values and no family signal (truly empty column):
            // the eager path emits an empty Dict.
            Acc::Empty
        };
        Self {
            context,
            acc,
            row_count: 0,
        }
    }

    /// Feeds one present, non-null row value.
    ///
    /// # Errors
    ///
    /// [`GenerationError::UnsupportedValue`] when the value family does not
    /// match the geometry-locked accumulator.
    pub fn push(&mut self, value: &Value) -> Result<(), GenerationError> {
        self.row_count += 1;
        match (&mut self.acc, value) {
            (Acc::Bool(b), Value::Bool(v)) => b.push(*v),
            (Acc::UInt { words, bits, count }, Value::Int64(n)) => {
                push_packed(words, *bits, *count, (*n).cast_unsigned());
                *count += 1;
            }
            (Acc::SInt(v), Value::Int64(n)) => v.push(*n),
            (Acc::Float(v), Value::Float64(f)) => v.push(*f),
            (Acc::Str(db), Value::String(s)) => {
                db.add(s.as_str());
            }
            (Acc::Vec { flat, dims }, Value::Vector(vec)) => {
                if vec.len() != usize::from(*dims) {
                    return Err(GenerationError::MixedColumnTypes {
                        context: self.context.clone(),
                        kinds: vec!["Vector(dims mismatch)"],
                    });
                }
                flat.extend_from_slice(vec);
            }
            (acc, other) => {
                return Err(GenerationError::UnsupportedValue {
                    kind: value_kind_name(other),
                    context: format!("{} (accumulator {:?})", self.context, acc.tag()),
                });
            }
        }
        Ok(())
    }

    /// Feeds a placeholder for an absent or present-null row. Appends the
    /// family's neutral placeholder (false / 0 / 0.0 / "" / zero-vector),
    /// matching the eager `push_placeholder` byte output.
    ///
    /// # Errors
    ///
    /// [`GenerationError::UnsupportedValue`] on a vector column whose
    /// dimensions are unknown.
    pub fn push_placeholder(&mut self) -> Result<(), GenerationError> {
        self.row_count += 1;
        match &mut self.acc {
            Acc::Bool(b) => b.push(false),
            Acc::UInt { words, bits, count } => {
                push_packed(words, *bits, *count, 0);
                *count += 1;
            }
            Acc::SInt(v) => v.push(0),
            Acc::Float(v) => v.push(0.0),
            Acc::Str(db) => {
                db.add("");
            }
            Acc::Vec { flat, dims } => {
                flat.extend(std::iter::repeat_n(0.0f32, usize::from(*dims)));
            }
            Acc::Empty => {}
        }
        Ok(())
    }

    /// Builds the [`ColumnCodec`], consuming the encoder.
    ///
    /// # Errors
    ///
    /// [`GenerationError::MixedColumnTypes`] is never returned here (geometry
    /// already enforced a single family); the signature keeps `Result` for
    /// parity with the eager encoder.
    pub fn finish(self) -> Result<ColumnCodec, GenerationError> {
        Ok(match self.acc {
            Acc::Bool(b) => ColumnCodec::Bitmap(b.freeze()),
            Acc::UInt { words, bits, count } => {
                ColumnCodec::BitPacked(crate::codec::BitPackedInts::from_raw_parts(
                    words, bits, count,
                ))
            }
            Acc::SInt(v) => ColumnCodec::raw_i64(v),
            Acc::Float(v) => ColumnCodec::float64(v),
            Acc::Str(db) => ColumnCodec::Dict(db.build()),
            Acc::Vec { flat, dims } => ColumnCodec::float32_vector(flat, dims),
            Acc::Empty => ColumnCodec::Dict(DictionaryBuilder::new().build()),
        })
    }
}

/// Appends one `bits`-wide value to a packed word buffer, mirroring
/// `BitPackedInts::pack_with_bits` bit-for-bit.
fn push_packed(words: &mut Vec<u64>, bits: u8, index: usize, value: u64) {
    if bits == 0 {
        return;
    }
    let b = bits as usize;
    let values_per_word = 64 / b;
    let word_idx = index / values_per_word;
    let bit_offset = (index % values_per_word) * b;
    if word_idx >= words.len() {
        words.push(0);
    }
    let mask = if b >= 64 { u64::MAX } else { (1u64 << b) - 1 };
    words[word_idx] |= (value & mask) << bit_offset;
}

impl Acc {
    fn tag(&self) -> &'static str {
        match self {
            Acc::Bool(_) => "Bool",
            Acc::UInt { .. } => "UInt",
            Acc::SInt(_) => "SInt",
            Acc::Float(_) => "Float",
            Acc::Str(_) => "Str",
            Acc::Vec { .. } => "Vec",
            Acc::Empty => "Empty",
        }
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
