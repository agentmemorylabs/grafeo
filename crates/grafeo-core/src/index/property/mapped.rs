//! Mapped PropertyIndex section wire format and lookup (G-E1.RO).
//!
//! ```text
//! [magic "GPIX"][version u8=1][flags u8][pad u16]
//! [index_count u32]
//! directory: index_count × {
//!   name_off u32, name_len u32, entry_off u32, entry_count u32
//! }
//! names: UTF-8 property names
//! entries (per index, sorted by value_bytes then node_id):
//!   value_len u32 | value_bytes | node_id u64
//! ```
//!
//! Values are bincode-encoded [`Value`]s so equality matches the live
//! [`HashableValue`] path for exact-match lookups.

use std::sync::Arc;

use bytes::Bytes;

use grafeo_common::types::{NodeId, Value};
use grafeo_common::utils::error::{Error, Result};

/// Magic for the mapped PropertyIndex payload.
pub const PROPERTY_INDEX_MAGIC: &[u8; 4] = b"GPIX";
/// Current mapped PropertyIndex payload version.
pub const PROPERTY_INDEX_VERSION: u8 = 1;
const HEADER_LEN: usize = 12; // magic4 + version1 + flags1 + pad2 + count4
const DIR_ENTRY_LEN: usize = 16;

/// Accounting for a restored PropertyIndex set under RO mapped open.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PropertyIndexMemoryAccounting {
    /// Retained mapped section payload bytes (file-backed).
    pub mapped_payload_bytes: u64,
    /// Anonymous heap for this structure (must be 0 for proportional postings).
    pub anonymous_proportional_bytes: u64,
    /// Bounded owner handles (directory table, Arc shells).
    pub anonymous_owner_bytes: u64,
    /// Number of property indexes in the section.
    pub index_count: u32,
    /// Total value→node postings across all indexes.
    pub entry_count: u64,
}

/// One property's mapped postings.
#[derive(Debug, Clone)]
pub struct MappedPropertyIndex {
    /// Property name.
    pub name: String,
    /// Shared section mapping.
    data: Bytes,
    /// Byte offset of the first entry for this property.
    entry_off: usize,
    /// Number of (value, node_id) postings.
    entry_count: u32,
}

impl MappedPropertyIndex {
    /// Look up all node ids with the given property value (exact match).
    #[must_use]
    pub fn lookup(&self, value: &Value) -> Vec<NodeId> {
        let Ok(target) = encode_value(value) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let mut lo = 0u32;
        let mut hi = self.entry_count;
        // Lower bound on value_bytes
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (vb, _) = self.entry_at(mid);
            if vb < target.as_slice() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let mut i = lo;
        while i < self.entry_count {
            let (vb, node_id) = self.entry_at(i);
            if vb != target.as_slice() {
                break;
            }
            out.push(node_id);
            i += 1;
        }
        out
    }

    /// Number of postings in this property index.
    #[must_use]
    pub fn len(&self) -> u32 {
        self.entry_count
    }

    /// Whether this property index has no postings.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    fn entry_at(&self, index: u32) -> (&[u8], NodeId) {
        // Fixed-width offset table before variable entries:
        // entry_count × u32 offsets relative to the body start
        // (entry_off + entry_count*4).
        let table = &self.data[self.entry_off..self.entry_off + self.entry_count as usize * 4];
        let off_rel = u32::from_le_bytes(
            table[index as usize * 4..index as usize * 4 + 4]
                .try_into()
                .expect("4 bytes"),
        ) as usize;
        let base = self.entry_off + self.entry_count as usize * 4 + off_rel;
        let value_len =
            u32::from_le_bytes(self.data[base..base + 4].try_into().expect("4 bytes")) as usize;
        let value_bytes = &self.data[base + 4..base + 4 + value_len];
        let id_off = base + 4 + value_len;
        let node_id = NodeId::new(u64::from_le_bytes(
            self.data[id_off..id_off + 8].try_into().expect("8 bytes"),
        ));
        (value_bytes, node_id)
    }

    /// Iterate all (Value, NodeId) postings by decoding value blobs.
    pub fn iter_entries(&self) -> Result<Vec<(Value, NodeId)>> {
        let mut out = Vec::with_capacity(self.entry_count as usize);
        for i in 0..self.entry_count {
            let (vb, node_id) = self.entry_at(i);
            let value = decode_value(vb)?;
            out.push((value, node_id));
        }
        Ok(out)
    }
}

/// Full mapped PropertyIndex section (one or more properties).
#[derive(Debug, Clone)]
pub struct MappedPropertyIndexSet {
    indexes: Vec<MappedPropertyIndex>,
    accounting: PropertyIndexMemoryAccounting,
    /// Retain mapping so views stay valid.
    #[allow(dead_code)]
    data: Bytes,
}

impl MappedPropertyIndexSet {
    /// Indexes in this set.
    #[must_use]
    pub fn indexes(&self) -> &[MappedPropertyIndex] {
        &self.indexes
    }

    /// Memory accounting for the mapped open.
    #[must_use]
    pub fn accounting(&self) -> PropertyIndexMemoryAccounting {
        self.accounting
    }

    /// Find a mapped index by property name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&MappedPropertyIndex> {
        self.indexes.iter().find(|i| i.name == name)
    }
}

/// Snapshot of one heap property index for encoding.
#[derive(Debug, Clone)]
pub struct PropertyIndexSnapshot {
    /// Property name.
    pub name: String,
    /// Sorted unique (value, node_id) postings (encoder re-sorts).
    pub entries: Vec<(Value, NodeId)>,
}

/// Encode a PropertyIndex section from live heap snapshots.
pub fn encode_property_index_section(indexes: &[PropertyIndexSnapshot]) -> Result<Vec<u8>> {
    let mut names = Vec::new();
    let mut name_spans: Vec<(u32, u32)> = Vec::with_capacity(indexes.len());
    let mut entry_blobs: Vec<Vec<u8>> = Vec::with_capacity(indexes.len());
    let mut entry_counts: Vec<u32> = Vec::with_capacity(indexes.len());

    for idx in indexes {
        let name_off = names.len() as u32;
        names.extend_from_slice(idx.name.as_bytes());
        name_spans.push((name_off, idx.name.len() as u32));

        let mut entries = idx.entries.clone();
        // Sort by encoded value then node id for binary search.
        let mut encoded: Vec<(Vec<u8>, NodeId)> = entries
            .drain(..)
            .map(|(v, id)| Ok((encode_value(&v)?, id)))
            .collect::<Result<Vec<_>>>()?;
        encoded.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.as_u64().cmp(&b.1.as_u64())));

        // Fixed-width offset table + variable entries.
        let mut offsets: Vec<u32> = Vec::with_capacity(encoded.len());
        let mut body = Vec::new();
        for (vb, id) in &encoded {
            offsets.push(body.len() as u32);
            body.extend_from_slice(&(vb.len() as u32).to_le_bytes());
            body.extend_from_slice(vb);
            body.extend_from_slice(&id.as_u64().to_le_bytes());
        }
        let mut blob = Vec::with_capacity(offsets.len() * 4 + body.len());
        for off in offsets {
            blob.extend_from_slice(&off.to_le_bytes());
        }
        blob.extend_from_slice(&body);
        entry_counts.push(encoded.len() as u32);
        entry_blobs.push(blob);
    }

    let dir_len = indexes.len() * DIR_ENTRY_LEN;
    let names_off = HEADER_LEN + dir_len;
    let mut entries_region = Vec::new();
    let mut dir = Vec::with_capacity(dir_len);
    for i in 0..indexes.len() {
        let (name_off, name_len) = name_spans[i];
        let entry_off = (names_off + names.len() + entries_region.len()) as u32;
        dir.extend_from_slice(&name_off.to_le_bytes());
        dir.extend_from_slice(&name_len.to_le_bytes());
        dir.extend_from_slice(&entry_off.to_le_bytes());
        dir.extend_from_slice(&entry_counts[i].to_le_bytes());
        entries_region.extend_from_slice(&entry_blobs[i]);
    }

    let mut out = Vec::with_capacity(names_off + names.len() + entries_region.len());
    out.extend_from_slice(PROPERTY_INDEX_MAGIC);
    out.push(PROPERTY_INDEX_VERSION);
    out.push(0); // flags
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(indexes.len() as u32).to_le_bytes());
    debug_assert_eq!(out.len(), HEADER_LEN);
    out.extend_from_slice(&dir);
    // Fix name offsets to be absolute from section start
    // name_off was relative to names region; rewrite directory name_off.
    let names_abs = names_off as u32;
    for i in 0..indexes.len() {
        let dir_base = HEADER_LEN + i * DIR_ENTRY_LEN;
        let rel = u32::from_le_bytes(out[dir_base..dir_base + 4].try_into().unwrap());
        let abs = names_abs + rel;
        out[dir_base..dir_base + 4].copy_from_slice(&abs.to_le_bytes());
    }
    out.extend_from_slice(&names);
    out.extend_from_slice(&entries_region);
    Ok(out)
}

/// Parse a mapped PropertyIndex section from retained section bytes.
pub fn parse_property_index_section(data: Bytes) -> Result<MappedPropertyIndexSet> {
    if data.len() < HEADER_LEN {
        return Err(Error::Serialization(
            "PropertyIndex section truncated header".into(),
        ));
    }
    if &data[0..4] != PROPERTY_INDEX_MAGIC {
        return Err(Error::Serialization(format!(
            "PropertyIndex bad magic: {:?}",
            &data[0..4]
        )));
    }
    let version = data[4];
    if version != PROPERTY_INDEX_VERSION {
        return Err(Error::Serialization(format!(
            "Unsupported PropertyIndex version {version}; expected {PROPERTY_INDEX_VERSION}"
        )));
    }
    let index_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
    let dir_end =
        HEADER_LEN
            .checked_add(index_count.checked_mul(DIR_ENTRY_LEN).ok_or_else(|| {
                Error::Serialization("PropertyIndex directory size overflow".into())
            })?)
            .ok_or_else(|| Error::Serialization("PropertyIndex directory end overflow".into()))?;
    if data.len() < dir_end {
        return Err(Error::Serialization(
            "PropertyIndex section truncated directory".into(),
        ));
    }

    let mut indexes = Vec::with_capacity(index_count);
    let mut total_entries = 0u64;
    for i in 0..index_count {
        let base = HEADER_LEN + i * DIR_ENTRY_LEN;
        let name_off = u32::from_le_bytes(data[base..base + 4].try_into().unwrap()) as usize;
        let name_len = u32::from_le_bytes(data[base + 4..base + 8].try_into().unwrap()) as usize;
        let entry_off = u32::from_le_bytes(data[base + 8..base + 12].try_into().unwrap()) as usize;
        let entry_count = u32::from_le_bytes(data[base + 12..base + 16].try_into().unwrap());
        let name_end = name_off
            .checked_add(name_len)
            .ok_or_else(|| Error::Serialization("PropertyIndex name range overflow".into()))?;
        if name_end > data.len() {
            return Err(Error::Serialization(
                "PropertyIndex name range out of bounds".into(),
            ));
        }
        let name = std::str::from_utf8(&data[name_off..name_end])
            .map_err(|e| Error::Serialization(format!("PropertyIndex name UTF-8: {e}")))?
            .to_string();
        // Validate entry region minimally.
        let table_bytes = entry_count as usize * 4;
        let entry_region_end = entry_off
            .checked_add(table_bytes)
            .ok_or_else(|| Error::Serialization("PropertyIndex entry table overflow".into()))?;
        if entry_region_end > data.len() {
            return Err(Error::Serialization(
                "PropertyIndex entry table out of bounds".into(),
            ));
        }
        total_entries += u64::from(entry_count);
        indexes.push(MappedPropertyIndex {
            name,
            data: data.clone(),
            entry_off,
            entry_count,
        });
    }

    let owner = std::mem::size_of::<MappedPropertyIndexSet>()
        + indexes.len() * std::mem::size_of::<MappedPropertyIndex>()
        + indexes.iter().map(|i| i.name.len()).sum::<usize>();

    let accounting = PropertyIndexMemoryAccounting {
        mapped_payload_bytes: data.len() as u64,
        anonymous_proportional_bytes: 0,
        anonymous_owner_bytes: owner as u64,
        index_count: index_count as u32,
        entry_count: total_entries,
    };

    Ok(MappedPropertyIndexSet {
        indexes,
        accounting,
        data,
    })
}

fn encode_value(value: &Value) -> Result<Vec<u8>> {
    let config = bincode::config::standard();
    bincode::serde::encode_to_vec(value, config)
        .map_err(|e| Error::Internal(format!("PropertyIndex value encode failed: {e}")))
}

fn decode_value(bytes: &[u8]) -> Result<Value> {
    let config = bincode::config::standard();
    let (value, _): (Value, _) = bincode::serde::decode_from_slice(bytes, config)
        .map_err(|e| Error::Serialization(format!("PropertyIndex value decode failed: {e}")))?;
    Ok(value)
}

/// Shared handle used by LpgStore for RO mapped property indexes.
pub type SharedMappedPropertyIndex = Arc<MappedPropertyIndex>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_index_round_trip_lookup() {
        let snap = PropertyIndexSnapshot {
            name: "rank".into(),
            entries: vec![
                (Value::Int64(1), NodeId::new(10)),
                (Value::Int64(2), NodeId::new(20)),
                (Value::Int64(1), NodeId::new(11)),
                (Value::Int64(3), NodeId::new(30)),
            ],
        };
        let bytes = encode_property_index_section(&[snap]).expect("encode");
        let set = parse_property_index_section(Bytes::from(bytes)).expect("parse");
        assert_eq!(set.accounting().anonymous_proportional_bytes, 0);
        assert!(set.accounting().mapped_payload_bytes > 0);
        let idx = set.get("rank").expect("rank");
        let mut hits = idx.lookup(&Value::Int64(1));
        hits.sort_by_key(|id| id.as_u64());
        assert_eq!(hits, vec![NodeId::new(10), NodeId::new(11)]);
        assert!(idx.lookup(&Value::Int64(99)).is_empty());
    }

    #[test]
    fn property_index_corrupt_magic_fails_closed() {
        let err = parse_property_index_section(Bytes::from(vec![0u8; 16])).unwrap_err();
        assert!(err.to_string().contains("magic") || err.to_string().contains("PropertyIndex"));
    }

    #[test]
    fn property_index_unknown_version_fails_closed() {
        let mut bytes = encode_property_index_section(&[]).unwrap();
        bytes[4] = 9;
        let err = parse_property_index_section(Bytes::from(bytes)).unwrap_err();
        assert!(err.to_string().contains("version"));
    }
}
