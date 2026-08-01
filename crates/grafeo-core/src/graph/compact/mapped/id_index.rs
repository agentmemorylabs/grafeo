//! Disk-sorted mapped node-ID lookup view (G-EM0.5b D0.8.4).
//!
//! The builder resolves edge endpoints through a fixed-width
//! `(original_id, table_id, dense_offset)` index. The index is **not** held
//! in a graph-sized `FxHashMap<u64, (u16, u64)>`: it is external-sorted by
//! `original_id` into a single completed file, then exposed as a read-only
//! checked view over refcounted [`Bytes`]. The storage layer maps that file
//! with `memmap2` and wraps the owner in `Bytes::from_owner` (D0.8.4 lock);
//! this module is the checked core view and never copies the index into a
//! resident `Vec`.
//!
//! ## Record layout (fixed width, 18 bytes)
//!
//! ```text
//! original_id  u64 LE   (sorted ascending; duplicates rejected)
//! table_id     u16 LE
//! dense_offset u64 LE   (row offset within the physical table)
//! ```
//!
//! Binary search over the fixed-width records resolves `original_id →
//! (table_id, dense_offset)` in O(log n) with only fixed binary-search
//! scratch charged anonymous.

use bytes::Bytes;

use crate::graph::compact::generation::GenerationError;

/// Fixed width of one ID-index record.
pub const ID_INDEX_RECORD_LEN: usize = 18;

/// Writes one `(original_id, table_id, dense_offset)` record.
pub fn write_id_index_record(
    out: &mut Vec<u8>,
    original_id: u64,
    table_id: u16,
    dense_offset: u64,
) {
    out.extend_from_slice(&original_id.to_le_bytes());
    out.extend_from_slice(&table_id.to_le_bytes());
    out.extend_from_slice(&dense_offset.to_le_bytes());
}

/// Encodes one record to a fixed array (for streaming into a run/file).
#[must_use]
pub fn id_index_record_bytes(
    original_id: u64,
    table_id: u16,
    dense_offset: u64,
) -> [u8; ID_INDEX_RECORD_LEN] {
    let mut out = [0u8; ID_INDEX_RECORD_LEN];
    out[0..8].copy_from_slice(&original_id.to_le_bytes());
    out[8..10].copy_from_slice(&table_id.to_le_bytes());
    out[10..18].copy_from_slice(&dense_offset.to_le_bytes());
    out
}

/// A read-only checked view over a completed, sorted ID-index mapping.
///
/// The `Bytes` may be heap-owned (small tests) or `memmap2`-backed via
/// `Bytes::from_owner` (production, D0.8.4). Either way the view performs
/// binary search without copying the index.
#[derive(Debug, Clone)]
pub struct MappedNodeIdIndex {
    data: Bytes,
}

impl MappedNodeIdIndex {
    /// Validates and wraps a completed ID-index byte buffer.
    ///
    /// # Errors
    ///
    /// Returns an error when the length is not a multiple of the record
    /// width or the records are not strictly ascending by `original_id`
    /// (adjacent duplicates are rejected, per D0.8.2).
    pub fn new(data: Bytes) -> Result<Self, GenerationError> {
        if !data.len().is_multiple_of(ID_INDEX_RECORD_LEN) {
            return Err(GenerationError::Codec(format!(
                "ID index length {} not multiple of {ID_INDEX_RECORD_LEN}",
                data.len()
            )));
        }
        let count = data.len() / ID_INDEX_RECORD_LEN;
        let mut prev: Option<u64> = None;
        for i in 0..count {
            let (id, _, _) = record_at(&data, i);
            if let Some(p) = prev {
                if id == p {
                    return Err(GenerationError::DuplicateNodeId(id));
                }
                if id < p {
                    return Err(GenerationError::Codec(format!(
                        "ID index not sorted at record {i} ({id} < {p})"
                    )));
                }
            }
            prev = Some(id);
        }
        Ok(Self { data })
    }

    /// Number of indexed nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len() / ID_INDEX_RECORD_LEN
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Byte length of the index.
    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.data.len()
    }

    /// Resolves `original_id` to `(table_id, dense_offset)` via binary search.
    #[must_use]
    pub fn lookup(&self, original_id: u64) -> Option<(u16, u64)> {
        let count = self.len();
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (id, tid, off) = record_at(&self.data, mid);
            match id.cmp(&original_id) {
                std::cmp::Ordering::Equal => return Some((tid, off)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }
}

/// Reads one record at index `i`. Caller guarantees `i < len`.
fn record_at(data: &[u8], i: usize) -> (u64, u16, u64) {
    let base = i * ID_INDEX_RECORD_LEN;
    let id = u64::from_le_bytes(data[base..base + 8].try_into().unwrap());
    let tid = u16::from_le_bytes(data[base + 8..base + 10].try_into().unwrap());
    let off = u64::from_le_bytes(data[base + 10..base + 18].try_into().unwrap());
    (id, tid, off)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(entries: &[(u64, u16, u64)]) -> Bytes {
        let mut v = Vec::new();
        for &(id, tid, off) in entries {
            write_id_index_record(&mut v, id, tid, off);
        }
        Bytes::from(v)
    }

    #[test]
    fn binary_search_resolves() {
        let data = build(&[(1, 0, 0), (5, 0, 1), (1 << 40, 1, 0), (u64::MAX, 1, 1)]);
        let idx = MappedNodeIdIndex::new(data).unwrap();
        assert_eq!(idx.lookup(1), Some((0, 0)));
        assert_eq!(idx.lookup(5), Some((0, 1)));
        assert_eq!(idx.lookup(1 << 40), Some((1, 0)));
        assert_eq!(idx.lookup(u64::MAX), Some((1, 1)));
        assert_eq!(idx.lookup(2), None);
    }

    #[test]
    fn rejects_adjacent_duplicate() {
        let data = build(&[(1, 0, 0), (1, 0, 1)]);
        assert!(matches!(
            MappedNodeIdIndex::new(data),
            Err(GenerationError::DuplicateNodeId(1))
        ));
    }

    #[test]
    fn rejects_unsorted_and_truncated() {
        let unsorted = build(&[(5, 0, 0), (1, 0, 1)]);
        assert!(MappedNodeIdIndex::new(unsorted).is_err());
        assert!(MappedNodeIdIndex::new(Bytes::from_static(&[0u8; 7])).is_err());
    }
}
