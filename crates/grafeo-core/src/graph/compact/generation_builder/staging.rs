//! Sort-domain record encodings for the streaming generation builder
//! (G-EM0.5b Phase 2).
//!
//! Every external-sort domain the builder drives is encoded here as a
//! [`SortRecord`] `(key, payload)` pair with a byte-lexicographic key. Keeping
//! all encodings in one module makes the sort order of each domain explicit and
//! auditable, and lets the merge/dedup/join logic operate on opaque bytes.
//!
//! Key layouts (all integers little-endian unless noted):
//!
//! - **node-row**: `label_len u16 || label || original_id u64 BE` →
//!   payload = serialized properties. Sorted by `(label, original_id)`.
//! - **edge-row**: `edge_type_len u16 || edge_type || original_id u64 BE` →
//!   payload = `src u64 BE || dst u64 BE || serialized properties`.
//! - **string-occ**: `string || use_kind u8 || owner_key` → payload empty.
//!   Sorted by `(string, use_kind, owner_key)`; dedup on `string`.
//! - **node-idmap**: `original_id u64 BE` → payload = `table_id u16 || offset u64`.
//! - **edge-idmap**: `original_id u64 BE` → payload = `rel_table_id u16 || csr_pos u64`.
//! - **fwd-csr**: `rel_table_id u16 || src_off u32 BE || dst_off u32 BE || original_edge_id u64 BE`
//!   → payload = serialized edge properties. Sorted into forward CSR order.
//! - **rev-csr**: `rel_table_id u16 || dst_off u32 BE || src_off u32 BE || forward_pos u32 BE`
//!   → payload empty. Sorted into reverse CSR order.
//!
//! Big-endian integer keys give numeric ordering under byte-lexicographic sort.

#![allow(clippy::cast_possible_truncation)]

use crate::graph::compact::generation::GenerationError;
use grafeo_common::types::{PropertyKey, Value};
use grafeo_common::utils::hash::FxHashMap;

/// Serialize a property map deterministically (sorted keys) to bytes.
///
/// Layout: `count u32 || (key_len u16 || key || value)*`. Values use a tagged
/// encoding mirroring the supported column families.
///
/// # Errors
///
/// Returns [`GenerationError::Codec`] on an unsupported value kind.
pub fn encode_properties(
    props: &FxHashMap<PropertyKey, Value>,
) -> Result<Vec<u8>, GenerationError> {
    let mut keys: Vec<&PropertyKey> = props.keys().collect();
    keys.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let mut out = Vec::new();
    out.extend_from_slice(&(keys.len() as u32).to_le_bytes());
    for key in keys {
        let kb = key.as_str().as_bytes();
        out.extend_from_slice(&(kb.len() as u16).to_le_bytes());
        out.extend_from_slice(kb);
        encode_value(&mut out, &props[key])?;
    }
    Ok(out)
}

/// Decode a property map produced by [`encode_properties`].
///
/// # Errors
///
/// Returns [`GenerationError::Codec`] on malformed bytes.
pub fn decode_properties(bytes: &[u8]) -> Result<FxHashMap<PropertyKey, Value>, GenerationError> {
    let mut pos = 0usize;
    let count = read_u32(bytes, &mut pos)? as usize;
    let mut map = FxHashMap::default();
    for _ in 0..count {
        let klen = read_u16(bytes, &mut pos)? as usize;
        let key = std::str::from_utf8(read_slice(bytes, &mut pos, klen)?)
            .map_err(|_| GenerationError::Codec("invalid UTF-8 property key".into()))?;
        let value = decode_value(bytes, &mut pos)?;
        map.insert(PropertyKey::new(key), value);
    }
    Ok(map)
}

/// Serialize a label vector deterministically to bytes.
///
/// Layout: `count u16 || (label_len u16 || label)*`. Labels are already
/// sorted and deduplicated by the input contract.
///
/// # Errors
///
/// Returns [`GenerationError::Codec`] on label count overflow.
pub fn encode_labels(labels: &[String]) -> Result<Vec<u8>, GenerationError> {
    let count = u16::try_from(labels.len()).map_err(|_| GenerationError::Codec(
        format!("label count {} exceeds u16::MAX", labels.len())
    ))?;
    let mut out = Vec::new();
    out.extend_from_slice(&count.to_le_bytes());
    for label in labels {
        let lb = label.as_bytes();
        out.extend_from_slice(&(lb.len() as u16).to_le_bytes());
        out.extend_from_slice(lb);
    }
    Ok(out)
}

/// Decode a label vector produced by [`encode_labels`].
///
/// # Errors
///
/// Returns [`GenerationError::Codec`] on malformed bytes.
pub fn decode_labels(bytes: &[u8]) -> Result<Vec<String>, GenerationError> {
    let mut pos = 0usize;
    let count = read_u16(bytes, &mut pos)? as usize;
    let mut labels = Vec::with_capacity(count);
    for _ in 0..count {
        let llen = read_u16(bytes, &mut pos)? as usize;
        let label = std::str::from_utf8(read_slice(bytes, &mut pos, llen)?)
            .map_err(|_| GenerationError::Codec("invalid UTF-8 label".into()))?;
        labels.push(label.to_string());
    }
    Ok(labels)
}

fn encode_value(out: &mut Vec<u8>, v: &Value) -> Result<(), GenerationError> {
    match v {
        Value::Bool(b) => {
            out.push(2);
            out.push(u8::from(*b));
        }
        Value::Int64(n) => {
            out.push(1);
            out.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float64(f) => {
            out.push(4);
            out.extend_from_slice(&f.to_le_bytes());
        }
        Value::String(s) => {
            out.push(3);
            let b = s.as_bytes();
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
        Value::Vector(vec) => {
            out.push(5);
            out.extend_from_slice(&(vec.len() as u16).to_le_bytes());
            for f in vec.iter() {
                out.extend_from_slice(&f.to_le_bytes());
            }
        }
        Value::Null => {
            out.push(0); // present-null marker (D0.8.0 three-way)
        }
        other => {
            return Err(GenerationError::UnsupportedValue {
                kind: value_kind_name(other),
                context: "streaming builder property".into(),
            });
        }
    }
    Ok(())
}

fn decode_value(bytes: &[u8], pos: &mut usize) -> Result<Value, GenerationError> {
    let tag = *read_slice(bytes, pos, 1)?
        .first()
        .ok_or_else(|| GenerationError::Codec("truncated value tag".into()))?;
    Ok(match tag {
        0 => Value::Null,
        1 => Value::Int64(i64::from_le_bytes(read_arr(bytes, pos)?)),
        2 => Value::Bool(read_slice(bytes, pos, 1)?[0] != 0),
        3 => {
            let len = read_u32(bytes, pos)? as usize;
            let s = std::str::from_utf8(read_slice(bytes, pos, len)?)
                .map_err(|_| GenerationError::Codec("invalid UTF-8 string value".into()))?;
            Value::String(s.into())
        }
        4 => Value::Float64(f64::from_le_bytes(read_arr(bytes, pos)?)),
        5 => {
            let dims = read_u16(bytes, pos)? as usize;
            let mut vec = Vec::with_capacity(dims);
            for _ in 0..dims {
                vec.push(f32::from_le_bytes(read_arr(bytes, pos)?));
            }
            Value::Vector(std::sync::Arc::from(vec))
        }
        other => {
            return Err(GenerationError::Codec(format!("unknown value tag {other}")));
        }
    })
}

fn value_kind_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "Null",
        Value::Bytes(_) => "Bytes",
        Value::Timestamp(_) => "Timestamp",
        Value::Date(_) => "Date",
        Value::Time(_) => "Time",
        Value::Duration(_) => "Duration",
        Value::ZonedDatetime(_) => "ZonedDatetime",
        Value::List(_) => "List",
        Value::Map(_) => "Map",
        Value::Path { .. } => "Path",
        Value::GCounter(_) => "GCounter",
        Value::OnCounter { .. } => "OnCounter",
        _ => "Unknown",
    }
}

// ── key builders ───────────────────────────────────────────────────

/// Build a node-row sort key: `label_len u16 || label || original_id u64 BE`.
#[must_use]
pub fn node_row_key(label: &str, original_id: u64) -> Vec<u8> {
    let lb = label.as_bytes();
    let mut key = Vec::with_capacity(2 + lb.len() + 8);
    key.extend_from_slice(&(lb.len() as u16).to_le_bytes());
    key.extend_from_slice(lb);
    key.extend_from_slice(&original_id.to_be_bytes());
    key
}

/// Split a node-row key into `(label, original_id)`.
///
/// # Errors
///
/// Returns [`GenerationError::Codec`] on malformed bytes.
pub fn split_node_row_key(key: &[u8]) -> Result<(&str, u64), GenerationError> {
    let mut pos = 0usize;
    let llen = read_u16(key, &mut pos)? as usize;
    let label = std::str::from_utf8(read_slice(key, &mut pos, llen)?)
        .map_err(|_| GenerationError::Codec("invalid UTF-8 label".into()))?;
    let id = read_u64_be(key, &mut pos)?;
    Ok((label, id))
}

/// Build an edge-row sort key: `type_len u16 || type || original_id u64 BE`.
#[must_use]
pub fn edge_row_key(edge_type: &str, original_id: u64) -> Vec<u8> {
    let tb = edge_type.as_bytes();
    let mut key = Vec::with_capacity(2 + tb.len() + 8);
    key.extend_from_slice(&(tb.len() as u16).to_le_bytes());
    key.extend_from_slice(tb);
    key.extend_from_slice(&original_id.to_be_bytes());
    key
}

/// Split an edge-row key into `(edge_type, original_id)`.
///
/// # Errors
///
/// Returns [`GenerationError::Codec`] on malformed bytes.
pub fn split_edge_row_key(key: &[u8]) -> Result<(&str, u64), GenerationError> {
    let mut pos = 0usize;
    let tlen = read_u16(key, &mut pos)? as usize;
    let edge_type = std::str::from_utf8(read_slice(key, &mut pos, tlen)?)
        .map_err(|_| GenerationError::Codec("invalid UTF-8 edge type".into()))?;
    let id = read_u64_be(key, &mut pos)?;
    Ok((edge_type, id))
}

/// Build an edge-row payload: `src u64 BE || dst u64 BE || properties`.
#[must_use]
pub fn edge_row_payload(src: u64, dst: u64, props_bytes: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(16 + props_bytes.len());
    p.extend_from_slice(&src.to_be_bytes());
    p.extend_from_slice(&dst.to_be_bytes());
    p.extend_from_slice(props_bytes);
    p
}

/// Split an edge-row payload into `(src, dst, properties_bytes)`.
///
/// # Errors
///
/// Returns [`GenerationError::Codec`] on malformed bytes.
pub fn split_edge_row_payload(payload: &[u8]) -> Result<(u64, u64, &[u8]), GenerationError> {
    let mut pos = 0usize;
    let src = read_u64_be(payload, &mut pos)?;
    let dst = read_u64_be(payload, &mut pos)?;
    Ok((src, dst, &payload[pos..]))
}

/// Build a forward-CSR sort key:
/// `rel_table_id u16 || src_off u32 BE || dst_off u32 BE || original_edge_id u64 BE`.
#[must_use]
pub fn fwd_csr_key(
    rel_table_id: u16,
    src_off: u32,
    dst_off: u32,
    original_edge_id: u64,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 4 + 4 + 8);
    key.extend_from_slice(&rel_table_id.to_be_bytes());
    key.extend_from_slice(&src_off.to_be_bytes());
    key.extend_from_slice(&dst_off.to_be_bytes());
    key.extend_from_slice(&original_edge_id.to_be_bytes());
    key
}

/// Split a forward-CSR key.
///
/// # Errors
///
/// Returns [`GenerationError::Codec`] on malformed bytes.
pub fn split_fwd_csr_key(key: &[u8]) -> Result<(u16, u32, u32, u64), GenerationError> {
    let mut pos = 0usize;
    let rid = read_u16_be(key, &mut pos)?;
    let src = read_u32_be(key, &mut pos)?;
    let dst = read_u32_be(key, &mut pos)?;
    let eid = read_u64_be(key, &mut pos)?;
    Ok((rid, src, dst, eid))
}

/// Build a reverse-CSR sort key:
/// `rel_table_id u16 || dst_off u32 BE || src_off u32 BE || forward_pos u32 BE`.
#[must_use]
pub fn rev_csr_key(rel_table_id: u16, dst_off: u32, src_off: u32, forward_pos: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 4 + 4 + 4);
    key.extend_from_slice(&rel_table_id.to_be_bytes());
    key.extend_from_slice(&dst_off.to_be_bytes());
    key.extend_from_slice(&src_off.to_be_bytes());
    key.extend_from_slice(&forward_pos.to_be_bytes());
    key
}

/// Split a reverse-CSR key.
///
/// # Errors
///
/// Returns [`GenerationError::Codec`] on malformed bytes.
pub fn split_rev_csr_key(key: &[u8]) -> Result<(u16, u32, u32, u32), GenerationError> {
    let mut pos = 0usize;
    let rid = read_u16_be(key, &mut pos)?;
    let dst = read_u32_be(key, &mut pos)?;
    let src = read_u32_be(key, &mut pos)?;
    let fp = read_u32_be(key, &mut pos)?;
    Ok((rid, dst, src, fp))
}

/// Build an ID-map key: `original_id u64 BE`.
#[must_use]
pub fn idmap_key(original_id: u64) -> Vec<u8> {
    original_id.to_be_bytes().to_vec()
}

/// Build an ID-map payload: `table_id u16 || position u64`.
#[must_use]
pub fn idmap_payload(table_id: u16, position: u64) -> Vec<u8> {
    let mut p = Vec::with_capacity(10);
    p.extend_from_slice(&table_id.to_le_bytes());
    p.extend_from_slice(&position.to_le_bytes());
    p
}

/// Split an ID-map payload into `(table_id, position)`.
///
/// # Errors
///
/// Returns [`GenerationError::Codec`] on malformed bytes.
pub fn split_idmap_payload(payload: &[u8]) -> Result<(u16, u64), GenerationError> {
    let mut pos = 0usize;
    let tid = read_u16(payload, &mut pos)?;
    let off = read_u64(payload, &mut pos)?;
    Ok((tid, off))
}

// ── byte readers ───────────────────────────────────────────────────

fn read_slice<'a>(b: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8], GenerationError> {
    let end = pos
        .checked_add(n)
        .ok_or_else(|| GenerationError::Codec("offset overflow".into()))?;
    if end > b.len() {
        return Err(GenerationError::Codec("truncated record".into()));
    }
    let s = &b[*pos..end];
    *pos = end;
    Ok(s)
}

fn read_u16(b: &[u8], pos: &mut usize) -> Result<u16, GenerationError> {
    let s = read_slice(b, pos, 2)?;
    Ok(u16::from_le_bytes([s[0], s[1]]))
}

fn read_u32(b: &[u8], pos: &mut usize) -> Result<u32, GenerationError> {
    let s = read_slice(b, pos, 4)?;
    Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn read_u64(b: &[u8], pos: &mut usize) -> Result<u64, GenerationError> {
    let s = read_slice(b, pos, 8)?;
    Ok(u64::from_le_bytes(s.try_into().unwrap()))
}

fn read_u16_be(b: &[u8], pos: &mut usize) -> Result<u16, GenerationError> {
    let s = read_slice(b, pos, 2)?;
    Ok(u16::from_be_bytes([s[0], s[1]]))
}

fn read_u32_be(b: &[u8], pos: &mut usize) -> Result<u32, GenerationError> {
    let s = read_slice(b, pos, 4)?;
    Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

fn read_u64_be(b: &[u8], pos: &mut usize) -> Result<u64, GenerationError> {
    let s = read_slice(b, pos, 8)?;
    Ok(u64::from_be_bytes(s.try_into().unwrap()))
}

fn read_arr<const N: usize>(b: &[u8], pos: &mut usize) -> Result<[u8; N], GenerationError> {
    let s = read_slice(b, pos, N)?;
    Ok(s.try_into().unwrap())
}

/// A decoded node row ready for column encoding.
#[derive(Debug, Clone)]
pub struct StagedNode {
    /// Table id (assigned from sorted label order).
    pub table_id: u16,
    /// Dense row offset within the table.
    pub offset: u64,
    /// Original node id.
    pub original_id: u64,
    /// Decoded properties.
    pub properties: FxHashMap<PropertyKey, Value>,
}

/// A decoded edge row with resolved dense endpoints.
#[derive(Debug, Clone)]
pub struct StagedEdge {
    /// Rel table id.
    pub rel_table_id: u16,
    /// Original edge id.
    pub original_id: u64,
    /// Dense source offset.
    pub src_off: u32,
    /// Dense destination offset.
    pub dst_off: u32,
    /// Decoded properties.
    pub properties: FxHashMap<PropertyKey, Value>,
}

/// A no-op marker so this module always has a public item even if all
/// encodings are used only through their free functions.
pub const STAGING_VERSION: u32 = 1;
