//! Bounded column pass (G-EM0.5b D0.8.6).
//!
//! Replays the property-occurrence run (sorted by `(table_id, prop_key,
//! row_offset)`) and builds each column incrementally from its ordered value
//! stream. Geometry (family, signedness, max width, vector dims, row
//! presence/nullness, zone-map state) is computed in a first bounded merge;
//! the body is written in a second replay — never `Vec<Option<Value>>` and
//! never first-value inference.
//!
//! Presence/null (D0.8.0 item 4): a row with no occurrence is **absent**; a
//! row whose occurrence payload is the null marker is **present-null**;
//! otherwise the typed value is authoritative. The column pass emits the
//! `ColumnRowPresence` / `ColumnRowNull` companion bitmap records only when
//! needed (some row absent / some present row null).

use crate::graph::compact::generation::{
    CancelToken, ExternalRunMerger, GenerationBudget, GenerationError, GenerationMetrics,
    RunSetLease,
};
use grafeo_common::types::Value;

/// One column's locked geometry after the first bounded merge.
#[derive(Debug, Clone)]
pub struct ColumnGeometry {
    /// Physical node table id.
    pub table_id: u16,
    /// Property key.
    pub key: String,
    /// Row count of the table (for presence/absence reconstruction).
    pub row_count: u64,
    /// Number of present (non-absent) rows seen.
    pub present_count: u64,
    /// Number of present-null rows seen.
    pub null_count: u64,
    /// Whether any value is a string (Dict family).
    pub has_string: bool,
    /// Whether any value is a signed int.
    pub saw_signed_int: bool,
    /// Observed vector dimensions (consistent or fail).
    pub vector_dims: Option<u16>,
    /// Family tag of first-seen typed value (for mixed-family detection).
    family: Option<&'static str>,
    /// Maximum u64 bit width across unsigned int values (BitPacked sizing).
    pub max_int_bits: u8,
    /// Minimum int value (zone map, present non-null only).
    pub min_int: Option<i64>,
    /// Maximum int value (zone map).
    pub max_int: Option<i64>,
    /// Minimum float value (zone map, NaN-excluded).
    pub min_float: Option<f64>,
    /// Maximum float value (zone map).
    pub max_float: Option<f64>,
    /// Saw a false bool (zone min = !has_false).
    pub saw_false: bool,
    /// Saw a true bool (zone max = has_true).
    pub saw_true: bool,
    /// Minimum string (zone map, lexicographic).
    pub min_str: Option<String>,
    /// Maximum string (zone map).
    pub max_str: Option<String>,
}

impl ColumnGeometry {
    fn new(table_id: u16, key: String, row_count: u64) -> Self {
        Self {
            table_id,
            key,
            row_count,
            present_count: 0,
            null_count: 0,
            has_string: false,
            saw_signed_int: false,
            vector_dims: None,
            family: None,
            max_int_bits: 0,
            min_int: None,
            max_int: None,
            min_float: None,
            max_float: None,
            saw_false: false,
            saw_true: false,
            min_str: None,
            max_str: None,
        }
    }

    /// True when some row is absent → emit a presence bitmap.
    #[must_use]
    pub fn needs_presence(&self) -> bool {
        self.present_count < self.row_count
    }

    /// True when some present row is null → emit a null bitmap.
    #[must_use]
    pub fn needs_null(&self) -> bool {
        self.null_count > 0
    }
}

/// Decodes one occurrence payload into a `Value`.
fn decode_occ_value(payload: &[u8]) -> Result<Value, GenerationError> {
    if payload.is_empty() {
        return Err(GenerationError::Codec("empty occurrence value".into()));
    }
    let tag = payload[0];
    Ok(match tag {
        0 => Value::Null,
        1 => {
            if payload.len() < 9 {
                return Err(GenerationError::Codec("truncated int occurrence".into()));
            }
            Value::Int64(i64::from_le_bytes(payload[1..9].try_into().unwrap()))
        }
        2 => Value::Bool(payload.get(1).copied().unwrap_or(0) != 0),
        4 => {
            if payload.len() < 9 {
                return Err(GenerationError::Codec("truncated float occurrence".into()));
            }
            Value::Float64(f64::from_le_bytes(payload[1..9].try_into().unwrap()))
        }
        3 => {
            if payload.len() < 5 {
                return Err(GenerationError::Codec("truncated string occurrence".into()));
            }
            let len = u32::from_le_bytes(payload[1..5].try_into().unwrap()) as usize;
            let s =
                std::str::from_utf8(payload.get(5..5 + len).ok_or_else(|| {
                    GenerationError::Codec("string occurrence out of range".into())
                })?)
                .map_err(|_| GenerationError::Codec("invalid UTF-8 occurrence".into()))?;
            Value::String(s.into())
        }
        5 => {
            if payload.len() < 3 {
                return Err(GenerationError::Codec("truncated vector occurrence".into()));
            }
            let dims = u16::from_le_bytes([payload[1], payload[2]]) as usize;
            let mut v = Vec::with_capacity(dims);
            let mut pos = 3;
            for _ in 0..dims {
                if pos + 4 > payload.len() {
                    return Err(GenerationError::Codec("vector occurrence truncated".into()));
                }
                v.push(f32::from_le_bytes(
                    payload[pos..pos + 4].try_into().unwrap(),
                ));
                pos += 4;
            }
            Value::Vector(std::sync::Arc::from(v))
        }
        other => {
            return Err(GenerationError::Codec(format!(
                "bad occurrence tag {other}"
            )));
        }
    })
}

/// Splits an occurrence key into `(table_id, prop_key, row_offset)`.
fn split_occ_key(key: &[u8]) -> Result<(u16, &str, u64), GenerationError> {
    if key.len() < 2 + 8 {
        return Err(GenerationError::Codec("occurrence key too short".into()));
    }
    let tid = u16::from_be_bytes([key[0], key[1]]);
    let off_start = key.len() - 8;
    let key_bytes = &key[2..off_start];
    let prop = std::str::from_utf8(key_bytes)
        .map_err(|_| GenerationError::Codec("invalid UTF-8 occurrence key".into()))?;
    let off = u64::from_be_bytes(key[off_start..].try_into().unwrap());
    Ok((tid, prop, off))
}

/// First bounded merge: compute per-column geometry from the occurrence run.
///
/// Occurrences arrive sorted by `(table_id, prop_key, row_offset)`, so each
/// column's values are contiguous. Geometry is accumulated one column at a
/// time; only the current column's running state is retained.
///
/// `table_row_count(table_id)` supplies each table's row count.
///
/// # Errors
///
/// Codec, mixed-family, or dimension-mismatch failure.
pub fn compute_column_geometries(
    occ_lease: &RunSetLease,
    merger: &mut dyn ExternalRunMerger,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
    table_row_count: &dyn Fn(u16) -> u64,
) -> Result<Vec<ColumnGeometry>, GenerationError> {
    let mut geometries: Vec<ColumnGeometry> = Vec::new();
    let mut current: Option<ColumnGeometry> = None;
    let mut current_key: Option<(u16, String)> = None;

    merger.merge_all(&occ_lease.handles, budget, metrics, cancel, &mut |rec| {
        let (tid, prop, _off) = split_occ_key(&rec.key)?;
        let value = decode_occ_value(&rec.payload)?;

        // Flush the previous column on key change.
        let this_key = (tid, prop.to_string());
        if current_key.as_ref() != Some(&this_key) {
            if let Some(g) = current.take() {
                geometries.push(g);
            }
            current = Some(ColumnGeometry::new(
                tid,
                prop.to_string(),
                table_row_count(tid),
            ));
            current_key = Some(this_key);
        }
        let g = current.as_mut().expect("just set");

        match &value {
            Value::Null => {
                g.null_count += 1;
                g.present_count += 1;
            }
            Value::Int64(n) => {
                g.present_count += 1;
                if *n < 0 {
                    g.saw_signed_int = true;
                } else {
                    let bits = 64 - (*n as u64).leading_zeros() as u8;
                    g.max_int_bits = g.max_int_bits.max(bits.max(1));
                }
                g.min_int = Some(g.min_int.map_or(*n, |m: i64| m.min(*n)));
                g.max_int = Some(g.max_int.map_or(*n, |m: i64| m.max(*n)));
                check_family(g, "Int64")?;
            }
            Value::Float64(f) => {
                g.present_count += 1;
                if !f.is_nan() {
                    g.min_float = Some(g.min_float.map_or(*f, |m: f64| m.min(*f)));
                    g.max_float = Some(g.max_float.map_or(*f, |m: f64| m.max(*f)));
                }
                check_family(g, "Float64")?;
            }
            Value::Bool(bv) => {
                g.present_count += 1;
                if *bv {
                    g.saw_true = true;
                } else {
                    g.saw_false = true;
                }
                check_family(g, "Bool")?;
            }
            Value::String(s) => {
                g.present_count += 1;
                g.has_string = true;
                let st = s.as_str();
                g.min_str = Some(match g.min_str.take() {
                    Some(m) if m.as_str() <= st => m,
                    _ => st.to_string(),
                });
                g.max_str = Some(match g.max_str.take() {
                    Some(m) if m.as_str() >= st => m,
                    _ => st.to_string(),
                });
                check_family(g, "String")?;
            }
            Value::Vector(vec) => {
                g.present_count += 1;
                let dims =
                    u16::try_from(vec.len()).map_err(|_| GenerationError::UnsupportedValue {
                        kind: "Vector(dims overflow)",
                        context: g.key.clone(),
                    })?;
                match g.vector_dims {
                    Some(d) if d != dims => {
                        return Err(GenerationError::MixedColumnTypes {
                            context: g.key.clone(),
                            kinds: vec!["Vector(dims mismatch)".into()],
                        });
                    }
                    _ => g.vector_dims = Some(dims),
                }
                check_family(g, "Vector")?;
            }
            other => {
                return Err(GenerationError::UnsupportedValue {
                    kind: "unsupported",
                    context: format!("{other:?}"),
                });
            }
        }
        Ok(())
    })?;

    if let Some(g) = current.take() {
        geometries.push(g);
    }
    Ok(geometries)
}

fn check_family(g: &mut ColumnGeometry, fam: &'static str) -> Result<(), GenerationError> {
    match g.family {
        None => {
            g.family = Some(fam);
            Ok(())
        }
        Some(f) if f == fam => Ok(()),
        Some(f) => Err(GenerationError::MixedColumnTypes {
            context: g.key.clone(),
            kinds: vec![f.into(), fam.into()],
        }),
    }
}
