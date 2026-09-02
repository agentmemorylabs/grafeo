//! Node logical-label membership companion segment (G-EM0.5b D0.8.0).
//!
//! CompactStore physically stores each node once, in the table of its
//! **first canonical label** (lexicographically smallest of the sorted,
//! deduplicated label vector). All *logical* label memberships are emitted
//! into this companion segment so readers can answer `get_node` (full label
//! set), `nodes_by_label`, and `all_labels` without duplicating the node.
//!
//! The segment is emitted **only when at least one node carries more than
//! one logical label**. A payload with exactly one physical label per node
//! omits it entirely; the reader then applies the old-v5 default (each node
//! belongs to exactly its physical table's label).
//!
//! ## Wire layout
//!
//! A header followed by one fixed-width record per logical membership, in
//! ascending `(node_compact_offset_key, label_code)` order:
//!
//! ```text
//! header:
//!   record_count u32 LE
//!   reserved     u32 LE (0)
//! record (16 bytes):
//!   node_table_id   u16 LE  (physical table the node is stored in)
//!   node_offset     u32 LE  (row offset within the physical table)
//!   label_code      u32 LE  (global dictionary code of the logical label)
//!   reserved        u32 LE (0)
//!   reserved2       u16 LE (0)
//! ```
//!
//! Records are sorted by `(node_table_id, node_offset, label_code)` so the
//! reader can binary-search a node's full label set, and a separate inverted
//! pass (label_code -> node list) can be derived by external sort without a
//! heap index proportional to memberships.
//!
//! Old readers fail closed on this unknown segment kind; new readers treat
//! its absence as the one-physical-label default.

use super::SegmentKind;
use bytes::Bytes;

/// Byte length of the membership header.
pub const MEMBERSHIP_HEADER_LEN: usize = 8;
/// Byte length of one membership record.
pub const MEMBERSHIP_RECORD_LEN: usize = 16;

/// One logical label membership for a physical node row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LabelMembership {
    /// Physical node table the row is stored in.
    pub node_table_id: u16,
    /// Row offset within the physical table.
    pub node_offset: u32,
    /// Global dictionary code of the logical label.
    pub label_code: u32,
}

impl LabelMembership {
    /// Encodes one record to bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; MEMBERSHIP_RECORD_LEN] {
        let mut out = [0u8; MEMBERSHIP_RECORD_LEN];
        out[0..2].copy_from_slice(&self.node_table_id.to_le_bytes());
        out[2..6].copy_from_slice(&self.node_offset.to_le_bytes());
        out[6..10].copy_from_slice(&self.label_code.to_le_bytes());
        // bytes 10..16 reserved zero
        out
    }

    /// Decodes one record, or `None` on truncation.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < MEMBERSHIP_RECORD_LEN {
            return None;
        }
        Some(Self {
            node_table_id: u16::from_le_bytes([bytes[0], bytes[1]]),
            node_offset: u32::from_le_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]),
            label_code: u32::from_le_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]),
        })
    }
}

/// Writes the membership segment body.
///
/// `memberships` must already be sorted ascending by
/// `(node_table_id, node_offset, label_code)`; this function validates the
/// invariant and fails closed on a violation.
///
/// # Errors
///
/// Returns [`GenerationError`] on a sort-order violation, count overflow, or
/// sink write failure.
pub fn write_membership_segment(
    sink: &mut dyn FnMut(&[u8]) -> Result<(), String>,
    memberships: &[LabelMembership],
) -> Result<(), String> {
    let count = u32::try_from(memberships.len()).map_err(|_| {
        format!(
            "label_membership_count {} exceeds u32::MAX",
            memberships.len()
        )
    })?;
    for w in memberships.windows(2) {
        if w[1] <= w[0] {
            return Err(String::from(
                "label membership records not strictly ascending",
            ));
        }
    }
    let mut header = [0u8; MEMBERSHIP_HEADER_LEN];
    header[0..4].copy_from_slice(&count.to_le_bytes());
    sink(&header)?;
    for m in memberships {
        sink(&m.to_bytes())?;
    }
    Ok(())
}

/// A checked read view over the membership segment body.
///
/// Retains the mapped [`Bytes`] and yields per-node label codes via binary
/// search over the fixed-width records — no `Vec<LabelMembership>` copy.
#[derive(Debug, Clone)]
pub struct LabelMembershipView {
    bytes: Bytes,
    count: usize,
}

impl LabelMembershipView {
    /// Parses and validates the segment body.
    ///
    /// # Errors
    ///
    /// Returns an error on truncation, a count/body mismatch, or a
    /// non-ascending record order.
    pub fn parse(bytes: &Bytes) -> Result<Self, String> {
        if bytes.len() < MEMBERSHIP_HEADER_LEN {
            return Err(format!(
                "{:?} truncated header ({} bytes)",
                SegmentKind::NodeLabelMembership,
                bytes.len()
            ));
        }
        let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let body_len = bytes.len() - MEMBERSHIP_HEADER_LEN;
        if body_len != count * MEMBERSHIP_RECORD_LEN {
            return Err(format!(
                "{:?} body length {body_len} != record_count {count} x {MEMBERSHIP_RECORD_LEN}",
                SegmentKind::NodeLabelMembership,
            ));
        }
        // Validate ascending order without copying.
        let mut prev: Option<LabelMembership> = None;
        for i in 0..count {
            let start = MEMBERSHIP_HEADER_LEN + i * MEMBERSHIP_RECORD_LEN;
            let rec = LabelMembership::from_bytes(&bytes[start..start + MEMBERSHIP_RECORD_LEN])
                .ok_or_else(|| {
                    format!(
                        "{:?} truncated record {i}",
                        SegmentKind::NodeLabelMembership
                    )
                })?;
            if let Some(p) = prev {
                if rec <= p {
                    return Err(format!(
                        "{:?} records not strictly ascending at {i}",
                        SegmentKind::NodeLabelMembership
                    ));
                }
            }
            prev = Some(rec);
        }
        Ok(Self {
            bytes: bytes.clone(),
            count,
        })
    }

    /// Returns `true` when there are no extra memberships.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns all logical label codes for one physical node row, ascending.
    #[must_use]
    pub fn labels_of(&self, node_table_id: u16, node_offset: u32) -> Vec<u32> {
        // Binary search for the first record matching (node_table_id, node_offset).
        // Records are sorted by (node_table_id, node_offset, label_code).
        let mut out = Vec::new();
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let rec = self.record_at(mid);
            let cmp = (rec.node_table_id, rec.node_offset).cmp(&(node_table_id, node_offset));
            match cmp {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    // Found a match; scan left to find the first, then collect right.
                    let mut start = mid;
                    while start > 0 {
                        let prev = self.record_at(start - 1);
                        if (prev.node_table_id, prev.node_offset) == (node_table_id, node_offset) {
                            start -= 1;
                        } else {
                            break;
                        }
                    }
                    let mut i = start;
                    while i < self.count {
                        let rec = self.record_at(i);
                        if (rec.node_table_id, rec.node_offset) != (node_table_id, node_offset) {
                            break;
                        }
                        out.push(rec.label_code);
                        i += 1;
                    }
                    return out;
                }
            }
        }
        out
    }

    /// Returns all physical `(node_table_id, node_offset)` rows carrying the
    /// given label code, ascending. Scans the fixed-width records (no heap index).
    #[must_use]
    pub fn nodes_with(&self, label_code: u32) -> Vec<(u16, u32)> {
        let mut out = Vec::new();
        for i in 0..self.count {
            let rec = self.record_at(i);
            if rec.label_code == label_code {
                out.push((rec.node_table_id, rec.node_offset));
            }
        }
        out
    }

    /// Number of membership records.
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.count
    }

    /// Returns the number of membership records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Reads the record at index `i` from the retained bytes.
    #[must_use]
    pub fn record_at(&self, i: usize) -> LabelMembership {
        let start = MEMBERSHIP_HEADER_LEN + i * MEMBERSHIP_RECORD_LEN;
        LabelMembership::from_bytes(&self.bytes[start..start + MEMBERSHIP_RECORD_LEN])
            .expect("validated in parse")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted() -> Vec<LabelMembership> {
        vec![
            LabelMembership {
                node_table_id: 0,
                node_offset: 0,
                label_code: 1,
            },
            LabelMembership {
                node_table_id: 0,
                node_offset: 0,
                label_code: 2,
            },
            LabelMembership {
                node_table_id: 0,
                node_offset: 1,
                label_code: 1,
            },
            LabelMembership {
                node_table_id: 1,
                node_offset: 0,
                label_code: 3,
            },
        ]
    }

    #[test]
    fn write_parse_round_trip() {
        let ms = sorted();
        let mut body = Vec::new();
        write_membership_segment(&mut |b| Ok::<_, String>(body.extend_from_slice(b)), &ms).unwrap();
        let bytes = Bytes::from(body);
        let view = LabelMembershipView::parse(&bytes).unwrap();
        assert_eq!(view.labels_of(0, 0), vec![1, 2]);
        assert_eq!(view.labels_of(0, 1), vec![1]);
        assert_eq!(view.nodes_with(1), vec![(0, 0), (0, 1)]);
        assert_eq!(view.nodes_with(3), vec![(1, 0)]);
    }

    #[test]
    fn rejects_unsorted() {
        let mut ms = sorted();
        ms.swap(0, 1);
        let mut body = Vec::new();
        assert!(
            write_membership_segment(&mut |b| Ok::<_, String>(body.extend_from_slice(b)), &ms)
                .is_err()
        );
    }
}
