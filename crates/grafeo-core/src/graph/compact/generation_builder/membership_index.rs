//! Bounded mapped label-membership index (R1).
//!
//! Replaces the O(N) `FxHashMap<u64, Vec<String>>` multi-label lookup with a
//! compact byte-buffer + fixed-width sparse index. Binary search over the
//! sparse index resolves `original_id → labels` in O(log n) with only
//! fixed-size scratch.
//!
//! ## Data buffer layout (variable-width records)
//!
//! ```text
//! original_id  u64 LE
//! label_count  u16 LE
//! (label_len u16 LE || label_bytes)* — label_count times
//! ```
//!
//! ## Sparse index layout (fixed-width, 16 bytes per entry)
//!
//! ```text
//! original_id  u64 LE   (sorted ascending)
//! byte_offset  u64 LE   (offset into data buffer)
//! ```

use bytes::Bytes;

use crate::graph::compact::generation::GenerationError;

/// Fixed width of one sparse-index entry.
const SPARSE_ENTRY_LEN: usize = 16;

/// A read-only, compact label-membership index over sorted records.
///
/// Built once from the membership merge stream, then queried via binary
/// search. Retains only the two `Bytes` buffers — no heap-proportional
/// `HashMap` or `Vec<String>` per node.
#[derive(Debug, Clone)]
pub struct MappedLabelMembershipIndex {
    /// Variable-width data records (see module docs).
    data: Bytes,
    /// Fixed-width sparse index: `(original_id, byte_offset)` per record.
    sparse: Bytes,
}

impl MappedLabelMembershipIndex {
    /// Builds from sorted `(original_id, labels)` pairs.
    ///
    /// The caller must sort by `original_id` ascending before calling.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::Codec`] on duplicate ids or label overflow.
    pub fn from_sorted(entries: &[(u64, Vec<String>)]) -> Result<Self, GenerationError> {
        let mut data = Vec::new();
        let mut sparse = Vec::with_capacity(entries.len() * SPARSE_ENTRY_LEN);
        let mut prev: Option<u64> = None;

        for &(original_id, ref labels) in entries {
            if let Some(p) = prev {
                if original_id == p {
                    return Err(GenerationError::DuplicateNodeId(original_id));
                }
                if original_id < p {
                    return Err(GenerationError::Codec(format!(
                        "membership index not sorted at {original_id} (prev {p})"
                    )));
                }
            }
            prev = Some(original_id);

            let byte_offset = data.len() as u64;
            // Sparse entry.
            sparse.extend_from_slice(&original_id.to_le_bytes());
            sparse.extend_from_slice(&byte_offset.to_le_bytes());

            // Data record.
            data.extend_from_slice(&original_id.to_le_bytes());
            let count = u16::try_from(labels.len()).map_err(|_| {
                GenerationError::Codec(format!(
                    "node {original_id} label count {} exceeds u16",
                    labels.len()
                ))
            })?;
            data.extend_from_slice(&count.to_le_bytes());
            for label in labels {
                let lb = label.as_bytes();
                let llen = u16::try_from(lb.len()).map_err(|_| {
                    GenerationError::Codec(format!(
                        "node {original_id} label length {} exceeds u16",
                        lb.len()
                    ))
                })?;
                data.extend_from_slice(&llen.to_le_bytes());
                data.extend_from_slice(lb);
            }
        }

        Ok(Self {
            data: Bytes::from(data),
            sparse: Bytes::from(sparse),
        })
    }

    /// Number of indexed nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sparse.len() / SPARSE_ENTRY_LEN
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sparse.is_empty()
    }

    /// True when `original_id` is present in the index.
    #[must_use]
    pub fn contains_node(&self, original_id: u64) -> bool {
        self.find_offset(original_id).is_some()
    }

    /// True when `label` is in the node's membership label set.
    ///
    /// Returns `false` when `original_id` is not in the index (the node has
    /// only its physical label).
    #[must_use]
    pub fn has_label(&self, original_id: u64, label: &str) -> bool {
        let Some(byte_offset) = self.find_offset(original_id) else {
            return false;
        };
        self.labels_at(byte_offset).any(|l| l == label)
    }

    /// Binary search the sparse index for `original_id`, returning the data
    /// byte offset.
    fn find_offset(&self, original_id: u64) -> Option<u64> {
        let count = self.len();
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let base = mid * SPARSE_ENTRY_LEN;
            let id = u64::from_le_bytes(self.sparse[base..base + 8].try_into().unwrap());
            match id.cmp(&original_id) {
                std::cmp::Ordering::Equal => {
                    let off =
                        u64::from_le_bytes(self.sparse[base + 8..base + 16].try_into().unwrap());
                    return Some(off);
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// Iterates labels at `byte_offset` in the data buffer.
    fn labels_at(&self, byte_offset: u64) -> impl Iterator<Item = &str> {
        let off = byte_offset as usize;
        // Skip original_id (8 bytes), read label_count (2 bytes).
        let count = u16::from_le_bytes(self.data[off + 8..off + 10].try_into().unwrap()) as usize;
        let mut pos = off + 10;
        (0..count).map(move |_| {
            let llen = u16::from_le_bytes(self.data[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let s = std::str::from_utf8(&self.data[pos..pos + llen]).unwrap_or("");
            pos += llen;
            s
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_lookup() {
        let entries = vec![
            (1u64, vec!["Person".into(), "Worker".into()]),
            (5, vec!["Company".into()]),
            (10, vec!["City".into(), "Capital".into(), "Metro".into()]),
        ];
        let idx = MappedLabelMembershipIndex::from_sorted(&entries).unwrap();
        assert_eq!(idx.len(), 3);
        assert!(idx.has_label(1, "Person"));
        assert!(idx.has_label(1, "Worker"));
        assert!(!idx.has_label(1, "City"));
        assert!(idx.has_label(5, "Company"));
        assert!(!idx.has_label(5, "Person"));
        assert!(idx.has_label(10, "Capital"));
        assert!(idx.has_label(10, "Metro"));
        assert!(!idx.has_label(99, "Anything"));
    }

    #[test]
    fn rejects_unsorted() {
        let entries = vec![(5u64, vec!["A".into()]), (1, vec!["B".into()])];
        assert!(MappedLabelMembershipIndex::from_sorted(&entries).is_err());
    }

    #[test]
    fn rejects_duplicate() {
        let entries = vec![(1u64, vec!["A".into()]), (1, vec!["B".into()])];
        assert!(matches!(
            MappedLabelMembershipIndex::from_sorted(&entries),
            Err(GenerationError::DuplicateNodeId(1))
        ));
    }

    #[test]
    fn empty_index() {
        let idx = MappedLabelMembershipIndex::from_sorted(&[]).unwrap();
        assert!(idx.is_empty());
        assert!(!idx.has_label(1, "X"));
    }
}
