//! Incremental column encoder for bounded v5 emission (G-EM0.5b Phase 0).
//!
//! Accepts values one at a time, infers the column kind from the first
//! non-null value, builds the codec incrementally, and tracks zone maps
//! row-by-row. Produces the same `(ColumnCodec, ColumnType, Option<ZoneMap>)`
//! as the eager `encode_column` in `generation/columns.rs`.
//!
//! Phase 0 collects values internally (the API is incremental; the bounded
//! streaming comes in Phase 2 when the builder drives it). The output is
//! byte-identical to the eager path.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

use crate::codec::{BitPackedInts, BitVector, DictionaryBuilder};
use crate::graph::compact::column::ColumnCodec;
use crate::graph::compact::generation::error::GenerationError;
use crate::graph::compact::schema::ColumnType;
use crate::graph::compact::zone_map::ZoneMap;
use grafeo_common::types::Value;

/// Column value family (mirrors `columns.rs::Kind`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Family {
    Int,
    Float,
    Bool,
    Str,
    Vector,
}

fn family_name(f: Family) -> &'static str {
    match f {
        Family::Int => "Int64",
        Family::Float => "Float64",
        Family::Bool => "Bool",
        Family::Str => "String",
        Family::Vector => "Vector",
    }
}

/// Incremental column encoder. Feed values with [`push`](ColumnEncoder::push),
/// then call [`finish`](ColumnEncoder::finish) to produce the codec.
pub struct ColumnEncoder {
    context: String,
    values: Vec<Option<Value>>,
    families: Vec<Family>,
    saw_signed_int: bool,
    vector_dims: Option<u16>,
}

impl ColumnEncoder {
    /// Creates an encoder for the given column context (table + key).
    #[must_use]
    pub fn new(context: impl Into<String>) -> Self {
        Self {
            context: context.into(),
            values: Vec::new(),
            families: Vec::new(),
            saw_signed_int: false,
            vector_dims: None,
        }
    }

    /// Feeds one row value.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::NullValue`] on null/missing,
    /// [`GenerationError::UnsupportedValue`] on unsupported kinds,
    /// [`GenerationError::MixedColumnTypes`] on mixed families.
    pub fn push(&mut self, value: Option<&Value>) -> Result<(), GenerationError> {
        let row = self.values.len();
        let family = match value {
            None | Some(Value::Null) => {
                return Err(GenerationError::NullValue {
                    context: self.context.clone(),
                    row,
                });
            }
            Some(Value::Int64(n)) => {
                if *n < 0 {
                    self.saw_signed_int = true;
                }
                Family::Int
            }
            Some(Value::Float64(_)) => Family::Float,
            Some(Value::Bool(_)) => Family::Bool,
            Some(Value::String(_)) => Family::Str,
            Some(Value::Vector(vec)) => {
                let dims =
                    u16::try_from(vec.len()).map_err(|_| GenerationError::UnsupportedValue {
                        kind: "Vector(dimensions overflow u16)",
                        context: self.context.clone(),
                    })?;
                if dims == 0 {
                    return Err(GenerationError::UnsupportedValue {
                        kind: "Vector(zero dimensions)",
                        context: self.context.clone(),
                    });
                }
                match self.vector_dims {
                    Some(prev) if prev != dims => {
                        return Err(GenerationError::MixedColumnTypes {
                            context: self.context.clone(),
                            kinds: vec!["Vector(dims mismatch)"],
                        });
                    }
                    _ => self.vector_dims = Some(dims),
                }
                Family::Vector
            }
            Some(other) => {
                return Err(GenerationError::UnsupportedValue {
                    kind: value_kind_name(other),
                    context: self.context.clone(),
                });
            }
        };
        if !self.families.contains(&family) {
            self.families.push(family);
        }
        self.values.push(value.cloned());
        Ok(())
    }

    /// Feeds a placeholder row for an absent or present-null row (G-EM0.5b
    /// D0.8.0 sparse/null support).
    ///
    /// The canonical column body carries deterministic placeholder bytes for
    /// rows that do not carry a typed value; the `ColumnRowPresence` /
    /// `ColumnRowNull` companions carry the real three-way distinction. This
    /// pushes a neutral placeholder (`Some(Value::Null)`-marked) without
    /// failing, recording the column's locked `family` so the body encoder
    /// emits the family's zero/empty/false placeholder.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::MixedColumnTypes`] if `family` conflicts
    /// with an already-seen family.
    pub fn push_placeholder(&mut self, family_of_column: &str) -> Result<(), GenerationError> {
        let family = match family_of_column {
            "Int64" => Family::Int,
            "Float64" => Family::Float,
            "Bool" => Family::Bool,
            "String" => Family::Str,
            "Vector" => Family::Vector,
            other => {
                return Err(GenerationError::UnsupportedValue {
                    kind: "unknown placeholder family",
                    context: format!("{} ({other})", self.context),
                });
            }
        };
        if !self.families.contains(&family) {
            self.families.push(family);
        }
        self.values.push(None); // placeholder; presence/null bitmaps disambiguate
        Ok(())
    }

    /// Finishes encoding and produces the codec, type, and zone map.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::MixedColumnTypes`] when multiple families
    /// were observed.
    pub fn finish(
        self,
        string_occ: &mut Vec<String>,
    ) -> Result<(ColumnCodec, ColumnType, Option<ZoneMap>), GenerationError> {
        if self.families.len() > 1 {
            let kinds = self.families.iter().copied().map(family_name).collect();
            return Err(GenerationError::MixedColumnTypes {
                context: self.context,
                kinds,
            });
        }

        match self.families.first().copied() {
            Some(Family::Bool) => {
                let bools: Vec<bool> = self
                    .values
                    .iter()
                    .map(|v| matches!(v, Some(Value::Bool(true))))
                    .collect();
                let bv = BitVector::from_bools(&bools);
                Ok((
                    ColumnCodec::Bitmap(bv),
                    ColumnType::Bool,
                    Some(zone_from_bools(&bools)),
                ))
            }
            Some(Family::Int) if !self.saw_signed_int => {
                let ints: Vec<u64> = self
                    .values
                    .iter()
                    .map(|v| match v {
                        Some(Value::Int64(n)) => (*n).cast_unsigned(),
                        _ => 0,
                    })
                    .collect();
                let bits = bits_needed(&ints);
                let bp = BitPackedInts::pack_with_bits(&ints, bits);
                Ok((
                    ColumnCodec::BitPacked(bp),
                    ColumnType::UInt { bits },
                    Some(zone_from_u64(&ints)),
                ))
            }
            Some(Family::Int) => {
                let i64s: Vec<i64> = self
                    .values
                    .iter()
                    .map(|v| match v {
                        Some(Value::Int64(n)) => *n,
                        _ => 0,
                    })
                    .collect();
                let zm = zone_from_i64(&i64s);
                Ok((ColumnCodec::raw_i64(i64s), ColumnType::Int64, Some(zm)))
            }
            Some(Family::Float) => {
                let f64s: Vec<f64> = self
                    .values
                    .iter()
                    .map(|v| match v {
                        Some(Value::Float64(f)) => *f,
                        _ => 0.0,
                    })
                    .collect();
                let zm = zone_from_f64(&f64s);
                Ok((ColumnCodec::float64(f64s), ColumnType::Float64, Some(zm)))
            }
            Some(Family::Str) => {
                let strs: Vec<String> = self
                    .values
                    .iter()
                    .map(|v| match v {
                        Some(Value::String(s)) => s.to_string(),
                        _ => String::new(),
                    })
                    .collect();
                let mut db = DictionaryBuilder::new();
                for s in &strs {
                    db.add(s.as_str());
                    string_occ.push(s.clone());
                }
                let dict = db.build();
                let refs: Vec<&str> = strs.iter().map(String::as_str).collect();
                Ok((
                    ColumnCodec::Dict(dict),
                    ColumnType::DictString,
                    Some(zone_from_strings(&refs)),
                ))
            }
            Some(Family::Vector) => {
                let dims = self.vector_dims.unwrap_or(0);
                let mut flat: Vec<f32> = Vec::with_capacity(self.values.len() * usize::from(dims));
                for v in &self.values {
                    if let Some(Value::Vector(vec)) = v {
                        flat.extend_from_slice(vec);
                    }
                }
                Ok((
                    ColumnCodec::float32_vector(flat, dims),
                    ColumnType::Float32Vector { dimensions: dims },
                    None,
                ))
            }
            None => {
                // Empty column (no rows): emit an empty Dict column.
                let dict = DictionaryBuilder::new().build();
                Ok((ColumnCodec::Dict(dict), ColumnType::DictString, None))
            }
        }
    }
}

// ── helpers (mirror generation/columns.rs) ─────────────────────────

fn value_kind_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Int64(_) => "Int64",
        Value::Float64(_) => "Float64",
        Value::String(_) => "String",
        Value::Bytes(_) => "Bytes",
        Value::Timestamp(_) => "Timestamp",
        Value::Date(_) => "Date",
        Value::Time(_) => "Time",
        Value::Duration(_) => "Duration",
        Value::ZonedDatetime(_) => "ZonedDatetime",
        Value::List(_) => "List",
        Value::Map(_) => "Map",
        Value::Vector(_) => "Vector",
        Value::Path { .. } => "Path",
        Value::GCounter(_) => "GCounter",
        Value::OnCounter { .. } => "OnCounter",
        _ => "Unknown",
    }
}

fn bits_needed(values: &[u64]) -> u8 {
    let max = values.iter().copied().max().unwrap_or(0);
    if max == 0 {
        1
    } else {
        (64 - max.leading_zeros()) as u8
    }
}

fn zone_from_u64(values: &[u64]) -> ZoneMap {
    let Some(&min) = values.iter().min() else {
        return ZoneMap::new();
    };
    let max = *values.iter().max().unwrap_or(&min);
    if max > i64::MAX as u64 {
        return ZoneMap {
            row_count: values.len(),
            ..ZoneMap::default()
        };
    }
    ZoneMap {
        min: Some(Value::Int64(min as i64)),
        max: Some(Value::Int64(max as i64)),
        null_count: 0,
        row_count: values.len(),
    }
}

fn zone_from_i64(values: &[i64]) -> ZoneMap {
    let Some(&min) = values.iter().min() else {
        return ZoneMap::new();
    };
    let max = *values.iter().max().unwrap_or(&min);
    ZoneMap {
        min: Some(Value::Int64(min)),
        max: Some(Value::Int64(max)),
        null_count: 0,
        row_count: values.len(),
    }
}

fn zone_from_f64(values: &[f64]) -> ZoneMap {
    ZoneMap {
        min: None,
        max: None,
        null_count: 0,
        row_count: values.len(),
    }
}

fn zone_from_bools(values: &[bool]) -> ZoneMap {
    if values.is_empty() {
        return ZoneMap::new();
    }
    let has_false = values.iter().any(|&v| !v);
    let has_true = values.iter().any(|&v| v);
    ZoneMap {
        min: Some(Value::Bool(!has_false)),
        max: Some(Value::Bool(has_true)),
        null_count: 0,
        row_count: values.len(),
    }
}

fn zone_from_strings(values: &[&str]) -> ZoneMap {
    let Some(&min) = values.iter().min() else {
        return ZoneMap::new();
    };
    let max = *values.iter().max().unwrap_or(&min);
    ZoneMap {
        min: Some(Value::from(min)),
        max: Some(Value::from(max)),
        null_count: 0,
        row_count: values.len(),
    }
}
