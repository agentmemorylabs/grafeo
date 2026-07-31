//! Mapped sorted ID lookup indexes (O(log N) / O(log E)).

use super::views::U64View;
use bytes::Bytes;
use grafeo_common::types::{EdgeId, NodeId};

const NODE_RECORD_LEN: usize = 24;
const EDGE_RECORD_LEN: usize = 24;

/// Sorted `NodeIdLookup` records: `(id:u64, table:u16, pad:u16, pad:u32, offset:u64)`.
#[derive(Debug, Clone)]
pub struct MappedNodeIdLookup {
    bytes: Bytes,
    len: usize,
}

impl MappedNodeIdLookup {
    /// Builds a checked view over fixed 24-byte records.
    ///
    /// # Errors
    ///
    /// Returns an error when the length is not a multiple of 24 or reserved
    /// fields are non-zero.
    pub fn new(bytes: Bytes) -> Result<Self, String> {
        if !bytes.len().is_multiple_of(NODE_RECORD_LEN) {
            return Err(format!(
                "NodeIdLookup length {} is not a multiple of {NODE_RECORD_LEN}",
                bytes.len()
            ));
        }
        let len = bytes.len() / NODE_RECORD_LEN;
        // Fail closed on non-zero reserved fields before exposing the view.
        for i in 0..len {
            let base = i * NODE_RECORD_LEN;
            let reserved_a = u16::from_le_bytes([bytes[base + 10], bytes[base + 11]]);
            let reserved_b = u32::from_le_bytes([
                bytes[base + 12],
                bytes[base + 13],
                bytes[base + 14],
                bytes[base + 15],
            ]);
            if reserved_a != 0 || reserved_b != 0 {
                return Err(format!(
                    "NodeIdLookup record {i} has non-zero reserved fields"
                ));
            }
        }
        // Verify ascending sort.
        for i in 1..len {
            let prev = read_id(&bytes, (i - 1) * NODE_RECORD_LEN);
            let cur = read_id(&bytes, i * NODE_RECORD_LEN);
            if cur < prev {
                return Err("NodeIdLookup records are not sorted ascending by id".into());
            }
        }
        Ok(Self { bytes, len })
    }

    /// Number of lookup records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Mapped byte length.
    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    /// Binary-searches for `id`. Complexity O(log N).
    #[must_use]
    pub fn lookup(&self, id: NodeId) -> Option<(u16, u64)> {
        let target = id.as_u64();
        let mut lo = 0usize;
        let mut hi = self.len;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_id = read_id(&self.bytes, mid * NODE_RECORD_LEN);
            match mid_id.cmp(&target) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let base = mid * NODE_RECORD_LEN;
                    let table = u16::from_le_bytes([self.bytes[base + 8], self.bytes[base + 9]]);
                    let offset = u64::from_le_bytes([
                        self.bytes[base + 16],
                        self.bytes[base + 17],
                        self.bytes[base + 18],
                        self.bytes[base + 19],
                        self.bytes[base + 20],
                        self.bytes[base + 21],
                        self.bytes[base + 22],
                        self.bytes[base + 23],
                    ]);
                    return Some((table, offset));
                }
            }
        }
        None
    }
}

/// Sorted `EdgeIdLookup` records: `(id:u64, rel_table:u16, pad:u16, pad:u32, csr_pos:u64)`.
#[derive(Debug, Clone)]
pub struct MappedEdgeIdLookup {
    bytes: Bytes,
    len: usize,
}

impl MappedEdgeIdLookup {
    /// Builds a checked view over fixed 24-byte records.
    ///
    /// # Errors
    ///
    /// Returns an error when the length is not a multiple of 24, reserved
    /// fields are non-zero, or records are unsorted.
    pub fn new(bytes: Bytes) -> Result<Self, String> {
        if !bytes.len().is_multiple_of(EDGE_RECORD_LEN) {
            return Err(format!(
                "EdgeIdLookup length {} is not a multiple of {EDGE_RECORD_LEN}",
                bytes.len()
            ));
        }
        let len = bytes.len() / EDGE_RECORD_LEN;
        for i in 0..len {
            let base = i * EDGE_RECORD_LEN;
            let reserved_a = u16::from_le_bytes([bytes[base + 10], bytes[base + 11]]);
            let reserved_b = u32::from_le_bytes([
                bytes[base + 12],
                bytes[base + 13],
                bytes[base + 14],
                bytes[base + 15],
            ]);
            if reserved_a != 0 || reserved_b != 0 {
                return Err(format!(
                    "EdgeIdLookup record {i} has non-zero reserved fields"
                ));
            }
        }
        for i in 1..len {
            let prev = read_id(&bytes, (i - 1) * EDGE_RECORD_LEN);
            let cur = read_id(&bytes, i * EDGE_RECORD_LEN);
            if cur < prev {
                return Err("EdgeIdLookup records are not sorted ascending by id".into());
            }
        }
        Ok(Self { bytes, len })
    }

    /// Number of lookup records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Mapped byte length.
    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    /// Binary-searches for `id`. Complexity O(log E).
    #[must_use]
    pub fn lookup(&self, id: EdgeId) -> Option<(u16, u64)> {
        let target = id.as_u64();
        let mut lo = 0usize;
        let mut hi = self.len;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_id = read_id(&self.bytes, mid * EDGE_RECORD_LEN);
            match mid_id.cmp(&target) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let base = mid * EDGE_RECORD_LEN;
                    let rel_table =
                        u16::from_le_bytes([self.bytes[base + 8], self.bytes[base + 9]]);
                    let csr_pos = u64::from_le_bytes([
                        self.bytes[base + 16],
                        self.bytes[base + 17],
                        self.bytes[base + 18],
                        self.bytes[base + 19],
                        self.bytes[base + 20],
                        self.bytes[base + 21],
                        self.bytes[base + 22],
                        self.bytes[base + 23],
                    ]);
                    return Some((rel_table, csr_pos));
                }
            }
        }
        None
    }
}

/// Reverse original-ID array view (O(1) internal→original).
///
/// Reserved for the Milestone W writable read path (G-EM0.4a ID preservation);
/// the read-only reopen does not yet consume it.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MappedOriginalIds {
    view: U64View,
}

#[allow(dead_code)] // reserved for Milestone W writable read path
impl MappedOriginalIds {
    /// Wraps a u64 array of original IDs.
    ///
    /// # Errors
    ///
    /// Returns an error when the length is not 8-byte aligned.
    pub fn new(bytes: Bytes) -> Result<Self, &'static str> {
        Ok(Self {
            view: U64View::new(bytes)?,
        })
    }

    /// Empty reverse array.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            view: U64View::empty(),
        }
    }

    /// Number of IDs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.view.len()
    }

    /// Mapped byte length.
    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.view.byte_len()
    }

    /// O(1) lookup of original id bits at an absolute index.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<u64> {
        self.view.get(index)
    }
}

#[inline]
fn read_id(bytes: &Bytes, base: usize) -> u64 {
    u64::from_le_bytes([
        bytes[base],
        bytes[base + 1],
        bytes[base + 2],
        bytes[base + 3],
        bytes[base + 4],
        bytes[base + 5],
        bytes[base + 6],
        bytes[base + 7],
    ])
}

/// Writes one NodeIdLookup record.
pub fn write_node_id_record(buf: &mut Vec<u8>, id: u64, table: u16, internal_offset: u64) {
    buf.extend_from_slice(&id.to_le_bytes());
    buf.extend_from_slice(&table.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // reserved_a
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved_b
    buf.extend_from_slice(&internal_offset.to_le_bytes());
}

/// Writes one EdgeIdLookup record.
pub fn write_edge_id_record(buf: &mut Vec<u8>, id: u64, rel_table: u16, csr_position: u64) {
    buf.extend_from_slice(&id.to_le_bytes());
    buf.extend_from_slice(&rel_table.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // reserved_a
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved_b
    buf.extend_from_slice(&csr_position.to_le_bytes());
}
