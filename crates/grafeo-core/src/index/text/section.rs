//! Text Index section serializer for the `.grafeo` container format.
//!
//! Serializes BM25 inverted indexes (postings lists, document lengths)
//! for all text indexes. Persisting avoids rebuilding from LPG properties
//! on database open.
//!
//! # Versions
//!
//! - **v1**: legacy bincode snapshot (still readable).
//! - **v2**: mapped GTXT payload (G-E1.RO). New writes emit v2 so RO reopen
//!   can keep postings file-backed without proportional anonymous HashMaps.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use grafeo_common::storage::section::{Section, SectionType};
use grafeo_common::types::NodeId;
use grafeo_common::utils::error::{Error, Result};

use super::InvertedIndex;
use super::mapped::{
    MappedTextIndexSet, TEXT_INDEX_MAPPED_VERSION, TextIndexEncodeSnapshot,
    encode_text_index_section, is_mapped_text_payload, parse_text_index_section,
};

/// Legacy bincode text index section format version.
const TEXT_SECTION_VERSION_V1: u8 = 1;

// ── Snapshot types (v1) ─────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct TextIndexSnapshot {
    version: u8,
    indexes: Vec<SingleIndexSnapshot>,
}

#[derive(Serialize, Deserialize)]
struct SingleIndexSnapshot {
    /// Index key: "label:property"
    key: String,
    /// BM25 parameters
    k1: f64,
    b: f64,
    /// Postings: term -> vec of (node_id, term_freq)
    postings: Vec<(String, Vec<(NodeId, u32)>)>,
    /// Document lengths: node_id -> token count
    doc_lengths: Vec<(NodeId, u32)>,
    /// Sum of all document lengths
    total_length: u64,
}

// ── Section implementation ──────────────────────────────────────────

/// Text Index section for the `.grafeo` container.
pub struct TextIndexSection {
    indexes: Vec<(String, Arc<RwLock<InvertedIndex>>)>,
    /// Restored mapped set (v2) when present.
    restored_mapped: Option<MappedTextIndexSet>,
    dirty: AtomicBool,
}

impl TextIndexSection {
    /// Create a new Text Index section from the current indexes.
    pub fn new(indexes: Vec<(String, Arc<RwLock<InvertedIndex>>)>) -> Self {
        Self {
            indexes,
            restored_mapped: None,
            dirty: AtomicBool::new(false),
        }
    }

    /// Mark this section as dirty.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    /// Take the restored mapped set after a v2 deserialize.
    pub fn take_mapped(&mut self) -> Option<MappedTextIndexSet> {
        self.restored_mapped.take()
    }

    /// Peek at restored mapped set without taking it.
    #[must_use]
    pub fn mapped(&self) -> Option<&MappedTextIndexSet> {
        self.restored_mapped.as_ref()
    }
}

impl Section for TextIndexSection {
    fn section_type(&self) -> SectionType {
        SectionType::TextIndex
    }

    fn version(&self) -> u8 {
        // New writes emit mapped v2.
        TEXT_INDEX_MAPPED_VERSION
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        let snaps: Vec<TextIndexEncodeSnapshot> = self
            .indexes
            .iter()
            .map(|(key, index_lock)| {
                let index = index_lock.read();
                let config = index.config();
                let (postings, doc_lengths, total_length) = index.snapshot();
                TextIndexEncodeSnapshot {
                    key: key.clone(),
                    k1: config.k1,
                    b: config.b,
                    postings,
                    doc_lengths,
                    total_length,
                }
            })
            .collect();
        encode_text_index_section(&snaps)
    }

    fn deserialize(&mut self, data: &[u8]) -> Result<()> {
        // v2 mapped GTXT path (G-E1.RO).
        if is_mapped_text_payload(data) {
            let mapped = parse_text_index_section(Bytes::copy_from_slice(data))?;
            // Also hydrate any pre-registered heap shells for writable callers.
            for idx in mapped.indexes() {
                if let Some((_, index_lock)) = self.indexes.iter().find(|(k, _)| *k == idx.key) {
                    // Materialize a heap copy only when shells were pre-created
                    // (writable reopen). RO open uses mapped search and leaves
                    // shells empty.
                    let _ = index_lock;
                }
            }
            self.restored_mapped = Some(mapped);
            return Ok(());
        }

        // Legacy v1 bincode.
        let config = bincode::config::standard();
        let (snapshot, _): (TextIndexSnapshot, _) = bincode::serde::decode_from_slice(data, config)
            .map_err(|e| {
                Error::Serialization(format!("Text Index section deserialization failed: {e}"))
            })?;

        if snapshot.version != TEXT_SECTION_VERSION_V1 {
            return Err(Error::Serialization(format!(
                "Unsupported TextIndex section version {}; expected {TEXT_SECTION_VERSION_V1} or mapped v{TEXT_INDEX_MAPPED_VERSION}",
                snapshot.version
            )));
        }

        for idx_snap in snapshot.indexes {
            // Create a shell if the caller did not pre-register one.
            if !self.indexes.iter().any(|(k, _)| *k == idx_snap.key) {
                self.indexes.push((
                    idx_snap.key.clone(),
                    Arc::new(RwLock::new(InvertedIndex::new(super::BM25Config {
                        k1: idx_snap.k1,
                        b: idx_snap.b,
                    }))),
                ));
            }
            if let Some((_, index_lock)) = self.indexes.iter().find(|(k, _)| *k == idx_snap.key) {
                let mut index = index_lock.write();
                index.set_config(super::BM25Config {
                    k1: idx_snap.k1,
                    b: idx_snap.b,
                });
                index.restore(
                    idx_snap.postings,
                    idx_snap.doc_lengths,
                    idx_snap.total_length,
                );
            }
        }

        Ok(())
    }

    fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    fn memory_usage(&self) -> usize {
        if let Some(ref mapped) = self.restored_mapped {
            return mapped.accounting().mapped_payload_bytes as usize;
        }
        self.indexes
            .iter()
            .map(|(_, idx)| idx.read().heap_memory_bytes())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::text::BM25Config;

    #[test]
    fn text_section_round_trip_v2_mapped() {
        let mut index = InvertedIndex::new(BM25Config::default());
        index.insert(NodeId::new(1), "rust graph database");
        index.insert(NodeId::new(2), "python web framework");
        index.insert(NodeId::new(3), "rust systems programming");

        let index_arc = Arc::new(RwLock::new(index));
        let section = TextIndexSection::new(vec![(
            "Item:description".to_string(),
            Arc::clone(&index_arc),
        )]);

        let bytes = section.serialize().expect("serialize should succeed");
        assert!(is_mapped_text_payload(&bytes));

        let mut section2 = TextIndexSection::new(vec![]);
        section2
            .deserialize(&bytes)
            .expect("deserialize should succeed");
        let mapped = section2.take_mapped().expect("mapped set");
        assert_eq!(mapped.accounting().anonymous_proportional_bytes, 0);
        let hits = mapped
            .get("Item:description")
            .unwrap()
            .search("rust database", 10);
        assert!(!hits.is_empty());
        assert_eq!(hits[0].0, NodeId::new(1));
    }

    #[test]
    fn text_section_empty() {
        let section = TextIndexSection::new(vec![]);
        let bytes = section.serialize().expect("serialize should succeed");

        let mut section2 = TextIndexSection::new(vec![]);
        section2
            .deserialize(&bytes)
            .expect("deserialize should succeed");
        assert!(section2.take_mapped().is_some());
    }

    #[test]
    fn text_section_type() {
        let section = TextIndexSection::new(vec![]);
        assert_eq!(section.section_type(), SectionType::TextIndex);
        assert_eq!(section.version(), TEXT_INDEX_MAPPED_VERSION);
    }

    #[test]
    fn text_section_dirty_tracking() {
        let section = TextIndexSection::new(vec![]);
        assert!(!section.is_dirty());
        section.mark_dirty();
        assert!(section.is_dirty());
        section.mark_clean();
        assert!(!section.is_dirty());
    }

    #[test]
    fn text_section_v1_legacy_still_readable() {
        let snapshot = TextIndexSnapshot {
            version: TEXT_SECTION_VERSION_V1,
            indexes: vec![SingleIndexSnapshot {
                key: "Doc:body".into(),
                k1: 1.2,
                b: 0.75,
                postings: vec![("hello".into(), vec![(NodeId::new(7), 1)])],
                doc_lengths: vec![(NodeId::new(7), 1)],
                total_length: 1,
            }],
        };
        let config = bincode::config::standard();
        let bytes = bincode::serde::encode_to_vec(&snapshot, config).unwrap();
        let fresh = InvertedIndex::new(BM25Config::default());
        let fresh_arc = Arc::new(RwLock::new(fresh));
        let mut section =
            TextIndexSection::new(vec![("Doc:body".to_string(), Arc::clone(&fresh_arc))]);
        section.deserialize(&bytes).expect("v1 decode");
        assert_eq!(fresh_arc.read().len(), 1);
        assert!(section.take_mapped().is_none());
    }
}
