//! PropertyIndex container section serializer (G-E1.RO).
//!
//! Serializes property hash-index postings so checkpoint → close → reopen
//! restores exact-match lookups without rebuilding from a full node scan.

// The `as usize`/`as u32`/`as u16` casts in this module convert between wire
// field widths and in-memory indices for data already bounds-checked against
// the resident section length; they cannot truncate on the 64-bit targets
// this engine supports.
#![allow(clippy::cast_possible_truncation)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;

use grafeo_common::storage::section::{Section, SectionType};
use grafeo_common::types::{HashableValue, NodeId, PropertyKey};
use grafeo_common::utils::error::Result;
use grafeo_common::utils::hash::FxHashSet;

use super::mapped::{
    MappedPropertyIndexSet, PropertyIndexSnapshot, encode_property_index_section,
    parse_property_index_section,
};

/// Property index section format version (mapped GPIX payload).
const PROPERTY_SECTION_VERSION: u8 = 1;

/// Live heap property indexes: property_key → (value → node set).
pub type HeapPropertyIndexes = Arc<
    RwLock<
        grafeo_common::utils::hash::FxHashMap<
            PropertyKey,
            DashMap<HashableValue, FxHashSet<NodeId>>,
        >,
    >,
>;

/// Property Index section for the `.grafeo` container.
pub struct PropertyIndexSection {
    /// Heap indexes used on the writable / LPG path.
    heap: Option<HeapPropertyIndexes>,
    /// Optional pre-built snapshots (when encoding from layered graph scan).
    snapshots: Vec<PropertyIndexSnapshot>,
    /// Restored mapped set after deserialize (RO path).
    restored_mapped: Option<MappedPropertyIndexSet>,
    dirty: AtomicBool,
}

impl PropertyIndexSection {
    /// Create a section that serializes the given live heap indexes.
    pub fn from_heap(heap: HeapPropertyIndexes) -> Self {
        Self {
            heap: Some(heap),
            snapshots: Vec::new(),
            restored_mapped: None,
            dirty: AtomicBool::new(false),
        }
    }

    /// Create a section from explicit snapshots (layered base+overlay scan).
    pub fn from_snapshots(snapshots: Vec<PropertyIndexSnapshot>) -> Self {
        Self {
            heap: None,
            snapshots,
            restored_mapped: None,
            dirty: AtomicBool::new(false),
        }
    }

    /// Empty section for deserialize targets.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            heap: None,
            snapshots: Vec::new(),
            restored_mapped: None,
            dirty: AtomicBool::new(false),
        }
    }

    /// Take the restored mapped set after deserialize.
    pub fn take_mapped(&mut self) -> Option<MappedPropertyIndexSet> {
        self.restored_mapped.take()
    }

    /// Mark dirty.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    fn collect_snapshots(&self) -> Result<Vec<PropertyIndexSnapshot>> {
        if !self.snapshots.is_empty() {
            return Ok(self.snapshots.clone());
        }
        let Some(ref heap) = self.heap else {
            return Ok(Vec::new());
        };
        let guard = heap.read();
        let mut out = Vec::with_capacity(guard.len());
        for (key, map) in &*guard {
            let mut entries = Vec::new();
            for item in map {
                let value = item.key().0.clone();
                for node_id in item.value() {
                    entries.push((value.clone(), *node_id));
                }
            }
            out.push(PropertyIndexSnapshot {
                name: key.to_string(),
                entries,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }
}

impl Section for PropertyIndexSection {
    fn section_type(&self) -> SectionType {
        SectionType::PropertyIndex
    }

    fn version(&self) -> u8 {
        PROPERTY_SECTION_VERSION
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        let snaps = self.collect_snapshots()?;
        encode_property_index_section(&snaps)
    }

    fn deserialize(&mut self, data: &[u8]) -> Result<()> {
        let mapped = parse_property_index_section(Bytes::copy_from_slice(data))?;
        self.restored_mapped = Some(mapped);
        Ok(())
    }

    fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    fn memory_usage(&self) -> usize {
        self.restored_mapped
            .as_ref()
            .map_or(0, |m| m.accounting().mapped_payload_bytes as usize)
    }
}

/// Populate a heap DashMap index from a mapped property index (writable open).
///
/// RO reopen keeps the mapped set and does **not** call this — proportional
/// postings stay file-backed.
#[allow(dead_code)] // reserved for the writable open path (Milestone W)
pub fn materialize_heap_from_mapped(
    mapped: &MappedPropertyIndexSet,
    heap: &HeapPropertyIndexes,
) -> Result<()> {
    let mut guard = heap.write();
    for idx in mapped.indexes() {
        let key = PropertyKey::new(&idx.name);
        let dash: DashMap<HashableValue, FxHashSet<NodeId>> = DashMap::new();
        for (value, node_id) in idx.iter_entries()? {
            let hv = HashableValue::new(value);
            dash.entry(hv).or_default().insert(node_id);
        }
        guard.insert(key, dash);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use grafeo_common::types::{NodeId, PropertyKey, Value};
    use grafeo_common::utils::hash::FxHashMap;

    #[test]
    fn property_section_round_trip() {
        let heap: HeapPropertyIndexes = Arc::new(RwLock::new(FxHashMap::default()));
        {
            let mut g = heap.write();
            let dash: DashMap<HashableValue, FxHashSet<NodeId>> = DashMap::new();
            dash.entry(HashableValue::new(Value::from("nyc")))
                .or_default()
                .insert(NodeId::new(1));
            dash.entry(HashableValue::new(Value::from("nyc")))
                .or_default()
                .insert(NodeId::new(2));
            dash.entry(HashableValue::new(Value::from("sf")))
                .or_default()
                .insert(NodeId::new(3));
            g.insert(PropertyKey::new("city"), dash);
        }
        let section = PropertyIndexSection::from_heap(Arc::clone(&heap));
        let bytes = section.serialize().expect("serialize");
        let mut section2 = PropertyIndexSection::empty();
        section2.deserialize(&bytes).expect("deserialize");
        let mapped = section2.take_mapped().expect("mapped");
        let city = mapped.get("city").expect("city");
        let mut hits = city.lookup(&Value::from("nyc"));
        hits.sort_by_key(|id| id.as_u64());
        assert_eq!(hits, vec![NodeId::new(1), NodeId::new(2)]);
    }
}
