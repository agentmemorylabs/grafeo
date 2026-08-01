//! Production column encoding + zone-map derivation for generation.
//!
//! W0 value semantics (packet §8): never map null/missing to an empty
//! string / zero default, never Debug/Display-stringify unsupported or mixed
//! values. Fail closed with a typed error instead. Supported homogeneous kinds
//! map to the production codecs exactly as `builder.rs` infers them:
//! non-negative `Int64` → `BitPacked`, signed `Int64` → `RawI64`,
//! `Float64` → `Float64`, `Bool` → `Bitmap`, `String` → `Dict`,
//! consistent-dimension `Vector` → `Float32Vector`.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

use super::error::GenerationError;
use crate::codec::{BitPackedInts, BitVector, DictionaryBuilder};
use crate::graph::compact::column::ColumnCodec;
use crate::graph::compact::schema::ColumnType;
use crate::graph::compact::zone_map::ZoneMap;
use grafeo_common::types::Value;

/// Encode one column through production v5 column codecs only, failing closed
/// on null/missing, unsupported, or mixed value kinds.
///
/// # Errors
///
/// [`GenerationError::NullValue`], [`GenerationError::UnsupportedValue`], or
/// [`GenerationError::MixedColumnTypes`].
pub(crate) fn encode_column(
    values: &[Option<&Value>],
    context: &str,
    string_occ: &mut Vec<String>,
) -> Result<(ColumnCodec, ColumnType, Option<ZoneMap>), GenerationError> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Kind {
        Int,
        Float,
        Bool,
        Str,
        Vector,
    }
    fn family_name(k: Kind) -> &'static str {
        match k {
            Kind::Int => "Int64",
            Kind::Float => "Float64",
            Kind::Bool => "Bool",
            Kind::Str => "String",
            Kind::Vector => "Vector",
        }
    }

    let mut families: Vec<Kind> = Vec::new();
    let mut saw_signed_int = false;
    let mut vector_dims: Option<u16> = None;
    for (row, v) in values.iter().enumerate() {
        let kind = match v {
            None | Some(Value::Null) => {
                return Err(GenerationError::NullValue {
                    context: context.to_string(),
                    row,
                });
            }
            Some(Value::Int64(n)) => {
                if *n < 0 {
                    saw_signed_int = true;
                }
                Kind::Int
            }
            Some(Value::Float64(_)) => Kind::Float,
            Some(Value::Bool(_)) => Kind::Bool,
            Some(Value::String(_)) => Kind::Str,
            Some(Value::Vector(vec)) => {
                let dims =
                    u16::try_from(vec.len()).map_err(|_| GenerationError::UnsupportedValue {
                        kind: "Vector(dimensions overflow u16)",
                        context: context.to_string(),
                    })?;
                if dims == 0 {
                    return Err(GenerationError::UnsupportedValue {
                        kind: "Vector(zero dimensions)",
                        context: context.to_string(),
                    });
                }
                match vector_dims {
                    Some(prev) if prev != dims => {
                        return Err(GenerationError::MixedColumnTypes {
                            context: context.to_string(),
                            kinds: vec!["Vector(dims mismatch)"],
                        });
                    }
                    _ => vector_dims = Some(dims),
                }
                Kind::Vector
            }
            Some(other) => {
                return Err(GenerationError::UnsupportedValue {
                    kind: value_kind_name(other),
                    context: context.to_string(),
                });
            }
        };
        if !families.contains(&kind) {
            families.push(kind);
        }
    }

    if families.len() > 1 {
        let kinds = families.iter().copied().map(family_name).collect();
        return Err(GenerationError::MixedColumnTypes {
            context: context.to_string(),
            kinds,
        });
    }

    match families.first().copied() {
        Some(Kind::Bool) => {
            let bools: Vec<bool> = values
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
        Some(Kind::Int) if !saw_signed_int => {
            let ints: Vec<u64> = values
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
        Some(Kind::Int) => {
            let i64s: Vec<i64> = values
                .iter()
                .map(|v| match v {
                    Some(Value::Int64(n)) => *n,
                    _ => 0,
                })
                .collect();
            let zm = zone_from_i64(&i64s);
            Ok((ColumnCodec::raw_i64(i64s), ColumnType::Int64, Some(zm)))
        }
        Some(Kind::Float) => {
            let f64s: Vec<f64> = values
                .iter()
                .map(|v| match v {
                    Some(Value::Float64(f)) => *f,
                    _ => 0.0,
                })
                .collect();
            let zm = zone_from_f64(&f64s);
            Ok((ColumnCodec::float64(f64s), ColumnType::Float64, Some(zm)))
        }
        Some(Kind::Str) => {
            let strs: Vec<String> = values
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
        Some(Kind::Vector) => {
            let dims = vector_dims.unwrap_or(0);
            let mut flat: Vec<f32> = Vec::with_capacity(values.len() * usize::from(dims));
            for v in values {
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

/// Stable variant name for fail-closed diagnostics.
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
        // bits_needed for values that fit bitpack path is at most 64.
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
    // Zone-map inline persistence only supports Int64/Bool/String min/max;
    // Float64 bounds are tracked in memory (row_count only) to match the
    // production `write_optional_value` behavior for float zone maps.
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

/// Push string-valued zone-map bounds into the global string occurrence stream.
pub(super) fn push_zone_strings(zm: &ZoneMap, occ: &mut Vec<String>) {
    if let Some(Value::String(s)) = &zm.min {
        occ.push(s.to_string());
    }
    if let Some(Value::String(s)) = &zm.max {
        occ.push(s.to_string());
    }
}
