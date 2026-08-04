//! Vector Store section serializer for the `.grafeo` container format.
//!
//! Serializes HNSW topology (neighbor graphs) for all vector indexes.
//! Embeddings are not stored here: they live in LPG node properties and
//! are accessed via `VectorAccessor` during search.
//!
//! Persisting the topology eliminates the O(N log N) HNSW rebuild on
//! database open. For 1M vectors this saves 30-60 seconds of startup time.
//!
//! ## Format versioning (Phase 7b)
//!
//! The section transparently handles two on-disk formats:
//!
//! - **v2 paged (current):** packed envelope (`GVST` magic + index
//!   directory + per-index meta + per-index `GTOP` paged topology). Reads
//!   parse the directory and feed each topology blob into
//!   [`super::paged_topology::deserialize_topology`]. Writes always use
//!   this format.
//! - **v1 bincode (legacy):** preserved as a one-release fallback so
//!   existing `.grafeo` files keep loading after upgrade. Detected by
//!   the absence of the `GVST` magic at offset 0.
//!
//! On the next checkpoint after a v1→v2 read, the section serializes
//! the in-memory topologies as v2, completing the migration.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use grafeo_common::storage::section::{Section, SectionType};
use grafeo_common::types::NodeId;
use grafeo_common::utils::error::{Error, Result};

use super::hnsw::TopologyView;
use super::paged_topology::{MmapTopology, deserialize_topology, serialize_topology};
use super::{DistanceMetric, VectorIndexKind};

/// Current vector store section format version.
///
/// Phase 7b: bumped from 1 (bincode) to 2 (paged envelope).
const VECTOR_SECTION_VERSION: u8 = 2;

/// First 4 bytes of the v2 envelope; absent in v1 bincode output.
const V2_MAGIC: &[u8; 4] = b"GVST";

/// v2 envelope header size (magic + version + reserved + num_indexes).
const V2_HEADER_SIZE: usize = 16;

/// v2 directory entry size (meta_offset + meta_len + topology_offset + topology_len).
const V2_DIR_ENTRY_SIZE: usize = 32;

// ── v1 (legacy) snapshot types ─────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct VectorStoreSnapshotV1 {
    version: u8,
    indexes: Vec<IndexSnapshotV1>,
}

#[derive(Serialize, Deserialize)]
struct IndexSnapshotV1 {
    /// Index key: "label:property"
    key: String,
    /// HNSW configuration
    dimensions: usize,
    metric: DistanceMetric,
    m: usize,
    ef_construction: usize,
    /// Topology
    entry_point: Option<NodeId>,
    max_level: usize,
    /// Node neighbors: Vec<(NodeId, Vec<Vec<NodeId>>)>
    nodes: Vec<(NodeId, Vec<Vec<NodeId>>)>,
}

// ── v2 (current) per-index metadata ────────────────────────────────

#[derive(Serialize, Deserialize)]
struct IndexMetaV2 {
    /// Index key: "label:property"
    key: String,
    dimensions: usize,
    metric: DistanceMetric,
    m: usize,
    ef_construction: usize,
}

// ── Section implementation ──────────────────────────────────────────

/// Vector Store section for the `.grafeo` container.
///
/// Wraps a collection of `(key, Arc<VectorIndexKind>)` pairs and serializes
/// their HNSW topologies for persistence.
pub struct VectorStoreSection {
    /// Vector indexes: (key, index) pairs from LpgStore::vector_index_entries()
    indexes: Vec<(String, Arc<VectorIndexKind>)>,
    dirty: AtomicBool,
}

impl VectorStoreSection {
    /// Create a new Vector Store section from the current indexes.
    pub fn new(indexes: Vec<(String, Arc<VectorIndexKind>)>) -> Self {
        Self {
            indexes,
            dirty: AtomicBool::new(false),
        }
    }

    /// Mark this section as dirty.
    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    /// Restore topologies from owner-backed section bytes without a full heap rebuild.
    ///
    /// Prefer this on read-only container open after
    /// `GrafeoFileManager::mmap_section` + `into_bytes`. Each index adopts an
    /// [`MmapTopology`] over a zero-copy `Bytes` slice of the section mapping.
    ///
    /// # Errors
    ///
    /// Returns a serialization error on truncated/corrupt envelopes, bad magic,
    /// unsupported versions, or when the section body does not restore any
    /// catalog index keys. A successfully decoded zero-node topology is valid:
    /// catalog-registered vector indexes may be checkpointed before their first
    /// vector is inserted. A topology that declares an entry point but contains
    /// zero nodes is structurally inconsistent and is rejected (fail-closed).
    pub fn restore_from_mapped_bytes(&mut self, data: Bytes) -> Result<()> {
        if data.is_empty() {
            return Err(Error::Serialization(
                "Vector Store section is empty (fail-closed)".to_string(),
            ));
        }
        if data.len() >= 4 && &data[0..4] == V2_MAGIC {
            deserialize_v2(data, &mut self.indexes, true)
        } else {
            // Legacy v1 is always heap-restored; mmap topology is v2-only.
            deserialize_v1(data.as_ref(), &mut self.indexes)
        }
    }

    /// Exact total section byte length (H-ADOPT.6 item 0B, pass 1 only).
    ///
    /// Arithmetic pass over the same v2 layout [`Self::serialize`]
    /// produces: meta blobs are bincode-encoded (small, per index) and
    /// topology lengths are computed without materializing any topology
    /// bytes. Cheap enough to run ahead of container emission so the
    /// generation writer knows the section length before streaming.
    ///
    /// # Errors
    ///
    /// Returns an internal error if meta encoding fails.
    pub fn stream_len(&self) -> Result<u64> {
        let views: Vec<TopologyView<'_>> = self
            .indexes
            .iter()
            .map(|(_, index)| index.topology_view())
            .collect();
        let (meta_blobs, topology_lens) = self.plan_stream(&views)?;

        let mut total = (V2_HEADER_SIZE + self.indexes.len() * V2_DIR_ENTRY_SIZE) as u64;
        total += meta_blobs.iter().map(Vec::len).sum::<usize>() as u64;
        total += topology_lens.iter().sum::<u64>();
        Ok(total)
    }

    /// Stream the entire v2 section envelope to `sink` and return the
    /// exact total number of bytes written (H-ADOPT.6 item 0B).
    ///
    /// Byte-identical to [`Self::serialize`] (the `serialize_v2`
    /// layout: header → directory → ALL meta blobs → ALL topology
    /// blobs, in index order) but bounded: no full section buffer, no
    /// per-index GTOP blob materialization, no neighbor cloning.
    ///
    /// GLOBAL two-pass over the section:
    ///
    /// 1. **Length pass** — bincode-encode each meta blob (kept;
    ///    small) and compute each topology length arithmetically over a
    ///    no-clone backend walk. Directory offsets are computed exactly
    ///    as `serialize_v2` does (metas first, then topologies).
    /// 2. **Emission pass** — stream header + directory + meta blobs,
    ///    then each topology in the same index order.
    ///
    /// # Lock hold
    ///
    /// One [`TopologyView`] per index holds its topology read lock
    /// across both passes; publication runs in a quiesced window.
    ///
    /// # Errors
    ///
    /// Returns an internal error on meta-encoding failure; propagates
    /// sink I/O errors.
    pub fn stream_to<W: Write + ?Sized>(&self, sink: &mut W) -> Result<u64> {
        let views: Vec<TopologyView<'_>> = self
            .indexes
            .iter()
            .map(|(_, index)| index.topology_view())
            .collect();
        let (meta_blobs, topology_lens) = self.plan_stream(&views)?;

        let n = self.indexes.len();
        let body_start = V2_HEADER_SIZE + n * V2_DIR_ENTRY_SIZE;

        // Directory offsets — exactly the serialize_v2 computation
        // (all meta blobs first, then all topology blobs).
        let mut meta_offsets: Vec<u64> = Vec::with_capacity(n);
        let mut topology_offsets: Vec<u64> = Vec::with_capacity(n);
        let mut cursor = body_start as u64;
        for blob in &meta_blobs {
            meta_offsets.push(cursor);
            cursor += blob.len() as u64;
        }
        for len in &topology_lens {
            topology_offsets.push(cursor);
            cursor += len;
        }
        let total = cursor;

        // Header.
        let mut header = [0u8; V2_HEADER_SIZE];
        header[0..4].copy_from_slice(V2_MAGIC);
        header[4] = VECTOR_SECTION_VERSION;
        header[8..16].copy_from_slice(&(n as u64).to_le_bytes());
        sink.write_all(&header)?;

        // Directory.
        for i in 0..n {
            let mut entry = [0u8; V2_DIR_ENTRY_SIZE];
            entry[0..8].copy_from_slice(&meta_offsets[i].to_le_bytes());
            entry[8..16].copy_from_slice(&(meta_blobs[i].len() as u64).to_le_bytes());
            entry[16..24].copy_from_slice(&topology_offsets[i].to_le_bytes());
            entry[24..32].copy_from_slice(&topology_lens[i].to_le_bytes());
            sink.write_all(&entry)?;
        }

        // All meta blobs, in index order.
        for blob in &meta_blobs {
            sink.write_all(blob)?;
        }

        // All topology blobs, in index order — bounded walk per index.
        for view in &views {
            view.write_to(sink)?;
        }

        Ok(total)
    }

    /// Pass-1 artifacts shared by [`Self::stream_len`] and
    /// [`Self::stream_to`]: per-index bincode meta blobs and exact
    /// topology lengths (arithmetic walk, no topology bytes).
    fn plan_stream(&self, views: &[TopologyView<'_>]) -> Result<(Vec<Vec<u8>>, Vec<u64>)> {
        let bincode_config = bincode::config::standard();
        let mut meta_blobs: Vec<Vec<u8>> = Vec::with_capacity(self.indexes.len());
        let mut topology_lens: Vec<u64> = Vec::with_capacity(self.indexes.len());
        for ((key, index), view) in self.indexes.iter().zip(views) {
            let config = index.config();
            let meta = IndexMetaV2 {
                key: key.clone(),
                dimensions: config.dimensions,
                metric: config.metric,
                m: config.m,
                ef_construction: config.ef_construction,
            };
            let meta_bytes = bincode::serde::encode_to_vec(&meta, bincode_config).map_err(|e| {
                Error::Internal(format!("Vector Store v2 meta serialization failed: {e}"))
            })?;
            meta_blobs.push(meta_bytes);
            topology_lens.push(view.exact_len());
        }
        Ok((meta_blobs, topology_lens))
    }
}

/// Serializes all in-memory indexes to the v2 paged envelope.
///
/// Layout: 16-byte header (`GVST` magic, version, num_indexes) + index
/// directory (32 bytes/entry: meta_offset, meta_len, topology_offset,
/// topology_len) + bincode'd metadata blobs + per-index `GTOP` paged
/// topology blobs.
fn serialize_v2(indexes: &[(String, Arc<VectorIndexKind>)]) -> Result<Vec<u8>> {
    let bincode_config = bincode::config::standard();

    // Build per-index meta blobs and topology blobs upfront so we can
    // compute absolute offsets.
    let mut meta_blobs: Vec<Vec<u8>> = Vec::with_capacity(indexes.len());
    let mut topology_blobs: Vec<Vec<u8>> = Vec::with_capacity(indexes.len());

    for (key, index) in indexes {
        let config = index.config();
        let meta = IndexMetaV2 {
            key: key.clone(),
            dimensions: config.dimensions,
            metric: config.metric,
            m: config.m,
            ef_construction: config.ef_construction,
        };
        let meta_bytes = bincode::serde::encode_to_vec(&meta, bincode_config).map_err(|e| {
            Error::Internal(format!("Vector Store v2 meta serialization failed: {e}"))
        })?;
        meta_blobs.push(meta_bytes);

        let (entry_point, max_level, nodes) = index.snapshot_topology();
        let topology_bytes = serialize_topology(entry_point, max_level, &nodes);
        topology_blobs.push(topology_bytes);
    }

    let n = indexes.len();
    let header_size = V2_HEADER_SIZE;
    let dir_size = n * V2_DIR_ENTRY_SIZE;
    let body_start = header_size + dir_size;

    // Compute absolute offsets for each meta + topology blob.
    let mut meta_offsets: Vec<u64> = Vec::with_capacity(n);
    let mut topology_offsets: Vec<u64> = Vec::with_capacity(n);
    let mut cursor = body_start;
    for blob in &meta_blobs {
        meta_offsets.push(cursor as u64);
        cursor += blob.len();
    }
    for blob in &topology_blobs {
        topology_offsets.push(cursor as u64);
        cursor += blob.len();
    }

    let mut buf = Vec::with_capacity(cursor);

    // Header
    buf.extend_from_slice(V2_MAGIC);
    buf.push(VECTOR_SECTION_VERSION);
    buf.extend_from_slice(&[0u8; 3]);
    buf.extend_from_slice(&(n as u64).to_le_bytes());
    debug_assert_eq!(buf.len(), V2_HEADER_SIZE);

    // Directory
    for i in 0..n {
        buf.extend_from_slice(&meta_offsets[i].to_le_bytes());
        buf.extend_from_slice(&(meta_blobs[i].len() as u64).to_le_bytes());
        buf.extend_from_slice(&topology_offsets[i].to_le_bytes());
        buf.extend_from_slice(&(topology_blobs[i].len() as u64).to_le_bytes());
    }
    debug_assert_eq!(buf.len(), header_size + dir_size);

    // Body: meta blobs first, then topology blobs (matches the offsets
    // computed above).
    for blob in &meta_blobs {
        buf.extend_from_slice(blob);
    }
    for blob in &topology_blobs {
        buf.extend_from_slice(blob);
    }

    Ok(buf)
}

/// Rejects a structurally inconsistent topology: one that declares an
/// entry point but contains zero nodes. Legitimate writers never emit
/// this state — the entry point is set on first insert and cleared on
/// last removal, and restore derives both fields from the same blob —
/// so it is treated as corruption and fails closed. A valid empty
/// topology is `entry_point = None` with zero nodes.
fn reject_inconsistent_empty_topology(
    entry_point: Option<NodeId>,
    n_nodes: usize,
    key: &str,
) -> Result<()> {
    if entry_point.is_some() && n_nodes == 0 {
        return Err(Error::Serialization(format!(
            "Vector Store v2 topology for key '{key}' declares an entry point but contains no nodes (fail-closed)"
        )));
    }
    Ok(())
}

/// Restores indexes from a v2 paged envelope.
///
/// When `prefer_mmap` is true, each index adopts a zero-copy
/// [`MmapTopology`] over a `Bytes` slice (file-backed when `data` is an
/// mmap owner). When false, topology is fully decoded into a heap HashMap
/// via [`deserialize_topology`] + [`VectorIndexKind::restore_topology`]
/// (writable open / mutation-friendly path).
fn deserialize_v2(
    data: Bytes,
    indexes: &mut [(String, Arc<VectorIndexKind>)],
    prefer_mmap: bool,
) -> Result<()> {
    let bincode_config = bincode::config::standard();
    let slice = data.as_ref();

    if slice.len() < V2_HEADER_SIZE {
        return Err(Error::Serialization(
            "Vector Store v2 header truncated".to_string(),
        ));
    }
    if &slice[0..4] != V2_MAGIC {
        return Err(Error::Serialization(
            "Vector Store v2 bad magic".to_string(),
        ));
    }
    let version = slice[4];
    if version != VECTOR_SECTION_VERSION {
        return Err(Error::Serialization(format!(
            "Vector Store v2 unsupported version: {version}"
        )));
    }
    let n_u64 = u64::from_le_bytes(
        slice[8..16]
            .try_into()
            .expect("slice length 8 fits u64 array"),
    );
    let n =
        usize::try_from(n_u64).map_err(|_| Error::Serialization("v2 n_indexes overflow".into()))?;

    let dir_size = n
        .checked_mul(V2_DIR_ENTRY_SIZE)
        .ok_or_else(|| Error::Serialization("v2 directory size overflow".into()))?;
    let body_start = V2_HEADER_SIZE
        .checked_add(dir_size)
        .ok_or_else(|| Error::Serialization("v2 directory size overflow".into()))?;
    if slice.len() < body_start {
        return Err(Error::Serialization(format!(
            "Vector Store v2 directory truncated: expected {body_start} bytes, got {}",
            slice.len()
        )));
    }

    let mut restored = 0usize;
    for i in 0..n {
        let dir_off = V2_HEADER_SIZE + i * V2_DIR_ENTRY_SIZE;
        let meta_off = u64::from_le_bytes(
            slice[dir_off..dir_off + 8]
                .try_into()
                .expect("slice length 8 fits u64 array"),
        );
        let meta_len = u64::from_le_bytes(
            slice[dir_off + 8..dir_off + 16]
                .try_into()
                .expect("slice length 8 fits u64 array"),
        );
        let topology_off = u64::from_le_bytes(
            slice[dir_off + 16..dir_off + 24]
                .try_into()
                .expect("slice length 8 fits u64 array"),
        );
        let topology_len = u64::from_le_bytes(
            slice[dir_off + 24..dir_off + 32]
                .try_into()
                .expect("slice length 8 fits u64 array"),
        );

        let meta_off_usize = usize::try_from(meta_off)
            .map_err(|_| Error::Serialization("v2 meta_off overflow".into()))?;
        let meta_len_usize = usize::try_from(meta_len)
            .map_err(|_| Error::Serialization("v2 meta_len overflow".into()))?;
        let topology_off_usize = usize::try_from(topology_off)
            .map_err(|_| Error::Serialization("v2 topology_off overflow".into()))?;
        let topology_len_usize = usize::try_from(topology_len)
            .map_err(|_| Error::Serialization("v2 topology_len overflow".into()))?;

        let meta_end = meta_off_usize
            .checked_add(meta_len_usize)
            .ok_or_else(|| Error::Serialization("v2 meta range overflow".into()))?;
        let topology_end = topology_off_usize
            .checked_add(topology_len_usize)
            .ok_or_else(|| Error::Serialization("v2 topology range overflow".into()))?;
        if meta_end > slice.len() || topology_end > slice.len() {
            return Err(Error::Serialization(format!(
                "Vector Store v2 directory entry {i} out of range"
            )));
        }

        let meta_bytes = &slice[meta_off_usize..meta_end];
        let (meta, _): (IndexMetaV2, _) =
            bincode::serde::decode_from_slice(meta_bytes, bincode_config).map_err(|e| {
                Error::Serialization(format!("Vector Store v2 meta deserialization failed: {e}"))
            })?;

        // Find the matching index by key. v2 doesn't require ordering;
        // the section receives indexes in any order, so we look up by key.
        if let Some((_, index)) = indexes.iter().find(|(k, _)| *k == meta.key) {
            // Zero-copy slice of the section Bytes (mmap-owned when RO open
            // used GrafeoFileManager::mmap_section + into_bytes).
            let topology_bytes = data.slice(topology_off_usize..topology_end);
            if prefer_mmap {
                let topo = MmapTopology::from_bytes(topology_bytes).map_err(|e| {
                    Error::Serialization(format!(
                        "Vector Store v2 topology decode failed for key '{}': {e}",
                        meta.key
                    ))
                })?;
                reject_inconsistent_empty_topology(topo.entry_point(), topo.len(), &meta.key)?;
                index.adopt_mmap_topology(topo);
            } else {
                let (entry_point, max_level, nodes) = deserialize_topology(topology_bytes)
                    .map_err(|e| {
                        Error::Serialization(format!(
                            "Vector Store v2 topology decode failed for key '{}': {e}",
                            meta.key
                        ))
                    })?;
                reject_inconsistent_empty_topology(entry_point, nodes.len(), &meta.key)?;
                index.restore_topology(entry_point, max_level, nodes);
            }
            restored += 1;
        }
    }

    if !indexes.is_empty() && restored == 0 {
        return Err(Error::Serialization(
            "Vector Store v2 section present but no topology matched catalog index keys"
                .to_string(),
        ));
    }

    Ok(())
}

/// Restores indexes from a v1 bincode envelope (legacy fallback).
fn deserialize_v1(data: &[u8], indexes: &mut [(String, Arc<VectorIndexKind>)]) -> Result<()> {
    let config = bincode::config::standard();
    let (snapshot, _): (VectorStoreSnapshotV1, _) = bincode::serde::decode_from_slice(data, config)
        .map_err(|e| {
            Error::Serialization(format!("Vector Store v1 deserialization failed: {e}"))
        })?;

    for idx_snap in snapshot.indexes {
        if let Some((_, index)) = indexes.iter().find(|(k, _)| *k == idx_snap.key) {
            index.restore_topology(idx_snap.entry_point, idx_snap.max_level, idx_snap.nodes);
        }
    }
    Ok(())
}

impl Section for VectorStoreSection {
    fn section_type(&self) -> SectionType {
        SectionType::VectorStore
    }

    fn version(&self) -> u8 {
        VECTOR_SECTION_VERSION
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        serialize_v2(&self.indexes)
    }

    fn deserialize(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        // Phase 7b: detect v2 packed vs v1 bincode by magic bytes.
        // Writable / legacy path uses heap restore so subsequent inserts
        // work without an explicit mmap→heap reload.
        if data.len() >= 4 && &data[0..4] == V2_MAGIC {
            deserialize_v2(Bytes::copy_from_slice(data), &mut self.indexes, false)
        } else {
            // v1 fallback: bincode-encoded VectorStoreSnapshotV1.
            // Existing files keep loading; the next checkpoint flushes
            // them out as v2.
            deserialize_v1(data, &mut self.indexes)
        }
    }

    fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    fn mark_clean(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    fn memory_usage(&self) -> usize {
        self.indexes
            .iter()
            .map(|(_, idx)| idx.heap_memory_bytes())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::vector::{HnswConfig, HnswIndex};

    fn make_test_index() -> (String, Arc<VectorIndexKind>) {
        let config = HnswConfig::new(4, DistanceMetric::Cosine);
        let index = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config)));

        // Manually set up a small topology via snapshot/restore
        let nodes = vec![
            (NodeId::new(1), vec![vec![NodeId::new(2), NodeId::new(3)]]),
            (NodeId::new(2), vec![vec![NodeId::new(1), NodeId::new(3)]]),
            (NodeId::new(3), vec![vec![NodeId::new(1), NodeId::new(2)]]),
        ];
        index.restore_topology(Some(NodeId::new(1)), 0, nodes);

        ("Item:embedding".to_string(), index)
    }

    fn make_v1_snapshot_bytes(key: &str) -> Vec<u8> {
        // Encode a v1 bincode snapshot directly so we can prove the
        // legacy fallback path on real bytes.
        let snapshot = VectorStoreSnapshotV1 {
            version: 1,
            indexes: vec![IndexSnapshotV1 {
                key: key.to_string(),
                dimensions: 4,
                metric: DistanceMetric::Cosine,
                m: 16,
                ef_construction: 200,
                entry_point: Some(NodeId::new(1)),
                max_level: 0,
                nodes: vec![
                    (NodeId::new(1), vec![vec![NodeId::new(2), NodeId::new(3)]]),
                    (NodeId::new(2), vec![vec![NodeId::new(1), NodeId::new(3)]]),
                    (NodeId::new(3), vec![vec![NodeId::new(1), NodeId::new(2)]]),
                ],
            }],
        };
        bincode::serde::encode_to_vec(&snapshot, bincode::config::standard())
            .expect("v1 bincode encode")
    }

    #[test]
    fn vector_section_round_trip() {
        let (key, index) = make_test_index();
        let section = VectorStoreSection::new(vec![(key.clone(), Arc::clone(&index))]);

        let bytes = section.serialize().expect("serialize should succeed");
        assert!(!bytes.is_empty());

        // Create a fresh index with same config to restore into
        let config = index.config().clone();
        let fresh_index = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config)));
        let mut section2 = VectorStoreSection::new(vec![(key, fresh_index.clone())]);
        section2
            .deserialize(&bytes)
            .expect("deserialize should succeed");

        assert_eq!(fresh_index.len(), 3);
        let (ep, ml, nodes) = fresh_index.snapshot_topology();
        assert_eq!(ep, Some(NodeId::new(1)));
        assert_eq!(ml, 0);
        assert_eq!(nodes.len(), 3);
    }

    #[test]
    fn vector_section_empty() {
        let section = VectorStoreSection::new(vec![]);
        let bytes = section.serialize().expect("serialize should succeed");

        let mut section2 = VectorStoreSection::new(vec![]);
        section2
            .deserialize(&bytes)
            .expect("deserialize should succeed");
    }

    #[test]
    fn vector_section_type() {
        let section = VectorStoreSection::new(vec![]);
        assert_eq!(section.section_type(), SectionType::VectorStore);
        // Phase 7b: bumped from 1 (bincode) to 2 (paged envelope).
        assert_eq!(section.version(), 2);
    }

    #[test]
    fn vector_section_dirty_tracking() {
        let section = VectorStoreSection::new(vec![]);
        assert!(!section.is_dirty());
        section.mark_dirty();
        assert!(section.is_dirty());
        section.mark_clean();
        assert!(!section.is_dirty());
    }

    // ── Phase 7b: format detection + v1 → v2 migration ───────────────

    /// New writes produce a v2 buffer (starts with `GVST` magic).
    #[test]
    fn alix_section_serialize_writes_v2_magic() {
        let (key, index) = make_test_index();
        let section = VectorStoreSection::new(vec![(key, Arc::clone(&index))]);
        let bytes = section.serialize().expect("serialize should succeed");
        assert!(bytes.len() > 4);
        assert_eq!(&bytes[0..4], V2_MAGIC, "new writes must use v2 magic");
    }

    /// v1 bincode-encoded buffers still deserialize correctly. The
    /// check uses a directly-constructed v1 snapshot, guaranteeing the
    /// migration path works for files written by older Grafeo versions.
    #[test]
    fn gus_section_v1_bincode_buffer_still_loads() {
        let v1_bytes = make_v1_snapshot_bytes("Item:embedding");
        // Sanity: v1 bytes do NOT start with GVST.
        assert_ne!(
            &v1_bytes[0..4],
            V2_MAGIC,
            "v1 bincode must not have GVST magic"
        );

        let config = HnswConfig::new(4, DistanceMetric::Cosine);
        let fresh = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config)));
        let mut section =
            VectorStoreSection::new(vec![("Item:embedding".to_string(), Arc::clone(&fresh))]);
        section
            .deserialize(&v1_bytes)
            .expect("v1 fallback path must load");

        assert_eq!(fresh.len(), 3);
        let (ep, ml, nodes) = fresh.snapshot_topology();
        assert_eq!(ep, Some(NodeId::new(1)));
        assert_eq!(ml, 0);
        assert_eq!(nodes.len(), 3);
    }

    /// After a v1 read + a re-serialize, the new buffer is v2.
    /// Demonstrates the on-checkpoint migration.
    #[test]
    fn vincent_section_v1_then_reserialize_yields_v2() {
        let v1_bytes = make_v1_snapshot_bytes("Item:embedding");
        let config = HnswConfig::new(4, DistanceMetric::Cosine);
        let fresh = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config)));
        let mut section =
            VectorStoreSection::new(vec![("Item:embedding".to_string(), Arc::clone(&fresh))]);
        section.deserialize(&v1_bytes).expect("v1 load");

        // Re-serialize: now in v2.
        let v2_bytes = section.serialize().expect("v2 serialize");
        assert_eq!(&v2_bytes[0..4], V2_MAGIC, "post-migration write is v2");

        // And v2 round-trips cleanly.
        let config2 = HnswConfig::new(4, DistanceMetric::Cosine);
        let restored = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config2)));
        let mut section2 =
            VectorStoreSection::new(vec![("Item:embedding".to_string(), Arc::clone(&restored))]);
        section2.deserialize(&v2_bytes).expect("v2 load");
        assert_eq!(restored.len(), 3);
    }

    /// v2 with multiple indexes round-trips by key, including indexes
    /// with different shapes.
    #[test]
    fn jules_section_v2_multiple_indexes_round_trip() {
        let cfg_a = HnswConfig::new(4, DistanceMetric::Cosine);
        let idx_a = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(cfg_a)));
        idx_a.restore_topology(
            Some(NodeId::new(10)),
            1,
            vec![
                (NodeId::new(10), vec![vec![NodeId::new(20)], vec![]]),
                (NodeId::new(20), vec![vec![NodeId::new(10)]]),
            ],
        );

        let cfg_b = HnswConfig::new(8, DistanceMetric::Euclidean);
        let idx_b = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(cfg_b)));
        idx_b.restore_topology(
            Some(NodeId::new(100)),
            0,
            vec![(NodeId::new(100), vec![vec![]])],
        );

        let section = VectorStoreSection::new(vec![
            ("Doc:embedding".to_string(), Arc::clone(&idx_a)),
            ("User:embedding".to_string(), Arc::clone(&idx_b)),
        ]);
        let bytes = section.serialize().expect("v2 serialize");

        // Restore into fresh indexes and verify topology counts and
        // entry points match.
        let restored_a = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(HnswConfig::new(
            4,
            DistanceMetric::Cosine,
        ))));
        let restored_b = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(HnswConfig::new(
            8,
            DistanceMetric::Euclidean,
        ))));
        let mut section2 = VectorStoreSection::new(vec![
            ("Doc:embedding".to_string(), Arc::clone(&restored_a)),
            ("User:embedding".to_string(), Arc::clone(&restored_b)),
        ]);
        section2.deserialize(&bytes).expect("v2 load");

        assert_eq!(restored_a.len(), 2);
        assert_eq!(restored_b.len(), 1);
        let (ep_a, _, _) = restored_a.snapshot_topology();
        let (ep_b, _, _) = restored_b.snapshot_topology();
        assert_eq!(ep_a, Some(NodeId::new(10)));
        assert_eq!(ep_b, Some(NodeId::new(100)));
    }

    #[test]
    fn empty_catalog_index_restores_on_heap_and_mmap_paths() {
        let key = "SessionSummary:embedding".to_string();
        let config = HnswConfig::new(16, DistanceMetric::Cosine);
        let empty = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config.clone())));
        let section = VectorStoreSection::new(vec![(key.clone(), empty)]);
        let bytes = section.serialize().expect("serialize empty topology");

        let heap_index = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config.clone())));
        let mut heap_section =
            VectorStoreSection::new(vec![(key.clone(), Arc::clone(&heap_index))]);
        heap_section
            .deserialize(&bytes)
            .expect("heap restore accepts valid empty topology");
        assert_eq!(heap_index.len(), 0);

        let mmap_index = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config)));
        let mut mmap_section = VectorStoreSection::new(vec![(key, Arc::clone(&mmap_index))]);
        mmap_section
            .restore_from_mapped_bytes(Bytes::from(bytes))
            .expect("mmap restore accepts valid empty topology");
        assert_eq!(mmap_index.len(), 0);
    }

    /// Truncated v2 envelope is rejected without panicking.
    #[test]
    fn shosanna_section_truncated_v2_rejected() {
        let (key, index) = make_test_index();
        let section = VectorStoreSection::new(vec![(key.clone(), Arc::clone(&index))]);
        let bytes = section.serialize().expect("v2 serialize");

        // Truncate to less than the v2 header.
        let truncated = &bytes[..8];
        let fresh = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(
            index.config().clone(),
        )));
        let mut section2 = VectorStoreSection::new(vec![(key, fresh)]);
        let err = section2
            .deserialize(truncated)
            .expect_err("must reject truncated v2");
        match err {
            Error::Serialization(_) => {}
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    // ── H-ADOPT.6 item 0B RED tests ────────────────────────────────

    use crate::index::vector::{QuantizationType, QuantizedHnswIndex};

    /// Mixed-shape section: multi-level Hnsw, single-node Hnsw, empty
    /// topology, and a Quantized index (covers both `VectorIndexKind`
    /// arms and the empty-topology directory entry).
    fn make_mixed_section() -> VectorStoreSection {
        let cfg_a = HnswConfig::new(4, DistanceMetric::Cosine);
        let idx_a = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(cfg_a)));
        idx_a.restore_topology(
            Some(NodeId::new(10)),
            2,
            vec![
                (
                    NodeId::new(10),
                    vec![
                        vec![NodeId::new(20), NodeId::new(30)],
                        vec![NodeId::new(30)],
                        vec![],
                    ],
                ),
                (NodeId::new(20), vec![vec![NodeId::new(10)]]),
                (
                    NodeId::new(30),
                    vec![
                        vec![NodeId::new(10), NodeId::new(20)],
                        vec![NodeId::new(10)],
                    ],
                ),
            ],
        );

        let cfg_b = HnswConfig::new(8, DistanceMetric::Euclidean);
        let idx_b = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(cfg_b)));
        idx_b.restore_topology(
            Some(NodeId::new(100)),
            0,
            vec![(NodeId::new(100), vec![vec![]])],
        );

        // Empty topology: catalog-registered before first insert.
        let cfg_c = HnswConfig::new(16, DistanceMetric::Cosine);
        let idx_c = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(cfg_c)));

        // Quantized arm delegates topology to an inner HnswIndex.
        let cfg_d = HnswConfig::new(4, DistanceMetric::DotProduct);
        let idx_d = Arc::new(VectorIndexKind::Quantized(QuantizedHnswIndex::new(
            cfg_d,
            QuantizationType::Scalar,
        )));
        idx_d.restore_topology(
            Some(NodeId::new(7)),
            1,
            vec![
                (NodeId::new(7), vec![vec![NodeId::new(8)], vec![]]),
                (NodeId::new(8), vec![vec![NodeId::new(7)]]),
            ],
        );

        VectorStoreSection::new(vec![
            ("Doc:embedding".to_string(), Arc::clone(&idx_a)),
            ("User:embedding".to_string(), Arc::clone(&idx_b)),
            ("SessionSummary:embedding".to_string(), Arc::clone(&idx_c)),
            ("CodeChunk:embedding".to_string(), Arc::clone(&idx_d)),
        ])
    }

    /// (b) Byte-parity: `stream_to` output MUST equal legacy
    /// `serialize()` byte-for-byte on a mixed multi-index section.
    #[test]
    fn h_adopt6_stream_to_byte_parity_with_serialize() {
        let section = make_mixed_section();
        let legacy = section.serialize().expect("legacy serialize");

        let mut streamed: Vec<u8> = Vec::new();
        let reported_len = section.stream_to(&mut streamed).expect("stream_to");

        assert_eq!(
            streamed, legacy,
            "stream_to output must be byte-identical to serialize_v2"
        );
        assert_eq!(
            reported_len as usize,
            legacy.len(),
            "stream_to must report the exact total section length"
        );
    }

    /// (d) ExactSectionSource contract: the reported length MUST equal
    /// the bytes actually written to the sink. The generation writer
    /// fails closed on mismatch, so this is the trap that catches
    /// length-pass bugs. Uses a counting sink that is NOT a Vec to prove
    /// the encoder works against a generic `std::io::Write`.
    #[test]
    fn h_adopt6_stream_to_exact_len_matches_bytes_written() {
        struct CountingSink {
            count: usize,
        }
        impl std::io::Write for CountingSink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.count += buf.len();
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let section = make_mixed_section();
        let mut sink = CountingSink { count: 0 };
        let reported_len = section.stream_to(&mut sink).expect("stream_to");
        assert_eq!(
            reported_len as usize, sink.count,
            "reported exact_len must equal bytes actually written (writer fails closed on mismatch)"
        );
        assert_eq!(sink.count, section.serialize().expect("serialize").len());
    }

    /// Empty section (zero indexes) streams the bare v2 header and
    /// reports its exact length.
    #[test]
    fn h_adopt6_stream_to_empty_section() {
        let section = VectorStoreSection::new(Vec::new());
        let legacy = section.serialize().expect("legacy serialize");
        let mut streamed: Vec<u8> = Vec::new();
        let reported_len = section.stream_to(&mut streamed).expect("stream_to");
        assert_eq!(streamed, legacy);
        assert_eq!(reported_len as usize, legacy.len());
    }

    /// RssAnon from /proc/self/status in KiB (Linux). Anonymous memory
    /// is the encoder-heap figure: it excludes file-backed section bytes
    /// and is unaffected by page-cache churn.
    fn rss_anon_kb() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("RssAnon:") {
                return rest.trim().trim_end_matches(" kB").trim().parse().ok();
            }
        }
        None
    }

    /// Build a synthetic 1M-node topology (dim-independent: topology
    /// only, ~16 neighbors at level 0). Matches the Phase-0 probe shape
    /// (145 MiB serialized output at 1M).
    fn build_1m_topology_index() -> (String, Arc<VectorIndexKind>) {
        const N: u64 = 1_000_000;
        let config = HnswConfig::new(384, DistanceMetric::Cosine);
        let index = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config)));
        let mut state: u64 = 0x5EED_0B;
        let mut rng = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state >> 33
        };
        let nodes: Vec<(NodeId, Vec<Vec<NodeId>>)> = (1..=N)
            .map(|i| {
                let count = 12 + (rng() % 9) as usize; // 12..20 neighbors
                let layer0: Vec<NodeId> = (0..count).map(|_| NodeId::new(1 + rng() % N)).collect();
                (NodeId::new(i), vec![layer0])
            })
            .collect();
        index.restore_topology(Some(NodeId::new(1)), 0, nodes);
        ("Large:embedding".to_string(), index)
    }

    /// Byte-parity for the zero-copy path: a section restored via
    /// `restore_from_mapped_bytes` (mmap backend) must stream
    /// byte-identical to the original `serialize()` output.
    #[test]
    fn h_adopt6_stream_to_byte_parity_mmap_backend() {
        let section = make_mixed_section();
        let original = section.serialize().expect("legacy serialize");

        // Fresh section with identical catalog keys + configs adopts the
        // mapped topologies zero-copy.
        let mut restored = VectorStoreSection::new(vec![
            (
                "Doc:embedding".to_string(),
                Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(HnswConfig::new(
                    4,
                    DistanceMetric::Cosine,
                )))),
            ),
            (
                "User:embedding".to_string(),
                Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(HnswConfig::new(
                    8,
                    DistanceMetric::Euclidean,
                )))),
            ),
            (
                "SessionSummary:embedding".to_string(),
                Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(HnswConfig::new(
                    16,
                    DistanceMetric::Cosine,
                )))),
            ),
            (
                "CodeChunk:embedding".to_string(),
                Arc::new(VectorIndexKind::Quantized(QuantizedHnswIndex::new(
                    HnswConfig::new(4, DistanceMetric::DotProduct),
                    QuantizationType::Scalar,
                ))),
            ),
        ]);
        restored
            .restore_from_mapped_bytes(Bytes::from(original.clone()))
            .expect("mmap restore");

        let mut streamed: Vec<u8> = Vec::new();
        let reported = restored.stream_to(&mut streamed).expect("stream_to");
        assert_eq!(
            streamed, original,
            "mmap-backed stream_to must be byte-identical to the original section"
        );
        assert_eq!(reported as usize, original.len());
    }

    /// Pass-1 length only: `stream_len` must equal the serialized
    /// length without streaming any bytes.
    #[test]
    fn h_adopt6_stream_len_matches_serialize() {
        let section = make_mixed_section();
        let len = section.stream_len().expect("stream_len");
        assert_eq!(len as usize, section.serialize().expect("serialize").len());
    }

    /// (c) Heap gate — LEGACY comparison figure. `serialize()` at 1M
    /// vectors (dim 384): records the RssAnon delta the streaming
    /// encoder must beat. Run with `--ignored` (builds 1M nodes).
    #[test]
    #[ignore = "heap gate: heavy 1M-node fixture; run explicitly with --ignored"]
    fn h_adopt6_heap_gate_legacy_serialize_1m() {
        let (key, index) = build_1m_topology_index();
        let section = VectorStoreSection::new(vec![(key, index)]);

        let before = rss_anon_kb().expect("RssAnon readable on Linux");
        let bytes = section.serialize().expect("legacy serialize");
        let after = rss_anon_kb().expect("RssAnon readable on Linux");

        let delta_mib = (after.saturating_sub(before)) as f64 / 1024.0;
        eprintln!(
            "HEAP-GATE legacy serialize(): output={} bytes ({:.1} MiB), RssAnon delta={delta_mib:.1} MiB",
            bytes.len(),
            bytes.len() as f64 / (1024.0 * 1024.0),
        );
        // No assertion: informational baseline for the stream_to gate.
    }

    /// (c) Heap gate — STREAMING figure. `stream_to` peak anon-RAM
    /// delta MUST stay ≤ 256 MiB at 1M synthetic vectors (dim 384).
    /// Target ≤ output bytes + ~10%. Run with `--ignored`.
    #[test]
    #[ignore = "heap gate: heavy 1M-node fixture; run explicitly with --ignored"]
    fn h_adopt6_heap_gate_stream_to_1m() {
        let (key, index) = build_1m_topology_index();
        let section = VectorStoreSection::new(vec![(key, index)]);

        // Discarding sink: we measure encoder heap, not I/O buffering.
        struct DiscardSink(usize);
        impl std::io::Write for DiscardSink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0 += buf.len();
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let before = rss_anon_kb().expect("RssAnon readable on Linux");
        let mut sink = DiscardSink(0);
        let reported = section.stream_to(&mut sink).expect("stream_to");
        let after = rss_anon_kb().expect("RssAnon readable on Linux");

        let delta_mib = (after.saturating_sub(before)) as f64 / 1024.0;
        let out_mib = sink.0 as f64 / (1024.0 * 1024.0);
        eprintln!(
            "HEAP-GATE stream_to(): output={} bytes ({out_mib:.1} MiB), reported_len={reported}, RssAnon delta={delta_mib:.1} MiB",
            sink.0,
        );
        assert_eq!(reported as usize, sink.0, "exact_len contract");
        assert!(
            delta_mib <= 256.0,
            "stream_to peak anon-RAM delta {delta_mib:.1} MiB exceeds the 256 MiB gate at 1M vectors"
        );
    }
}
