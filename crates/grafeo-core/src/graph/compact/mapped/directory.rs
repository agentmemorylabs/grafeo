//! CompactStore v5 header and checked segment directory.
//!
//! The `u64 as usize` casts below convert wire offsets/lengths into in-memory
//! indices for a payload already fully resident in `Bytes`; they are bounded
//! by the section's own length on the 64-bit targets this engine supports.
#![allow(clippy::cast_possible_truncation)]

use super::views::{read_u16_le, read_u32_le, read_u64_le};
use bytes::Bytes;

/// CompactStore payload version for the mapped layout (G-EM0.R0).
pub const FORMAT_VERSION_V5: u8 = 5;

/// Payload header length in bytes.
pub const HEADER_LEN: usize = 64;

/// Alias used by call sites that prefer the `V5_` prefix.
pub const V5_HEADER_LEN: usize = HEADER_LEN;

/// Directory entry length in bytes.
pub const DIRECTORY_ENTRY_LEN: usize = 48;

/// Segment kind identifiers (ascending emission order).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum SegmentKind {
    /// Store metadata (table/column descriptors).
    Metadata = 0,
    /// String dictionary offset table.
    StringOffsets = 1,
    /// String dictionary byte pool.
    StringBytes = 2,
    /// Per-node-table directory entries.
    NodeTableDirectory = 3,
    /// Per-rel-table directory entries.
    RelTableDirectory = 4,
    /// Node-to-relationship adjacency directory.
    NodeRelationshipDirectory = 5,
    /// Per-column directory entries.
    ColumnDirectory = 6,
    /// Per-column block index.
    ColumnBlockIndex = 7,
    /// Column body payloads.
    ColumnBodies = 8,
    /// Forward CSR row offsets.
    ForwardCsrOffsets = 9,
    /// Forward CSR edge targets.
    ForwardCsrTargets = 10,
    /// Reverse CSR row offsets.
    ReverseCsrOffsets = 11,
    /// Reverse CSR edge targets.
    ReverseCsrTargets = 12,
    /// Forward edge position array.
    ForwardPositions = 13,
    /// Node ID lookup index.
    NodeIdLookup = 14,
    /// Edge ID lookup index.
    EdgeIdLookup = 15,
    /// Node original-ID mapping.
    NodeOriginalIds = 16,
    /// Edge original-ID mapping.
    EdgeOriginalIds = 17,
    /// Table-level zone maps.
    TableZoneMaps = 18,
    /// Per-block zone maps.
    BlockZoneMaps = 19,
    /// Dictionary code index.
    DictionaryCodeIndex = 20,
}

impl SegmentKind {
    /// Parses a kind code, failing closed on unknown values.
    ///
    /// # Errors
    ///
    /// Returns an error for unknown kind codes.
    pub fn from_u16(v: u16) -> Result<Self, String> {
        Ok(match v {
            0 => Self::Metadata,
            1 => Self::StringOffsets,
            2 => Self::StringBytes,
            3 => Self::NodeTableDirectory,
            4 => Self::RelTableDirectory,
            5 => Self::NodeRelationshipDirectory,
            6 => Self::ColumnDirectory,
            7 => Self::ColumnBlockIndex,
            8 => Self::ColumnBodies,
            9 => Self::ForwardCsrOffsets,
            10 => Self::ForwardCsrTargets,
            11 => Self::ReverseCsrOffsets,
            12 => Self::ReverseCsrTargets,
            13 => Self::ForwardPositions,
            14 => Self::NodeIdLookup,
            15 => Self::EdgeIdLookup,
            16 => Self::NodeOriginalIds,
            17 => Self::EdgeOriginalIds,
            18 => Self::TableZoneMaps,
            19 => Self::BlockZoneMaps,
            20 => Self::DictionaryCodeIndex,
            other => return Err(format!("unknown CompactStore v5 segment kind {other}")),
        })
    }

    /// Wire numeric value.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

/// One checked directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentEntry {
    /// Segment kind code.
    pub kind: SegmentKind,
    /// Per-kind encoding version.
    pub encoding_version: u16,
    /// Flags (bit 0 = required).
    pub flags: u16,
    /// Required alignment power-of-two.
    pub alignment: u16,
    /// Payload-relative byte offset.
    pub offset: u64,
    /// Byte length of segment data.
    pub length: u64,
    /// Fixed element width, or 0 for variable.
    pub element_width: u32,
    /// Element count for fixed-width segments.
    pub element_count: u32,
    /// IEEE CRC-32 over the segment bytes.
    pub crc32: u32,
}

impl SegmentEntry {
    /// True when the required-segment flag (bit 0) is set.
    #[must_use]
    pub fn is_required(&self) -> bool {
        self.flags & 0x0001 != 0
    }

    /// Byte end exclusive of this segment.
    #[must_use]
    pub fn end(&self) -> Option<u64> {
        self.offset.checked_add(self.length)
    }
}

/// Parsed v5 header fields (excluding the directory bytes themselves).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V5Header {
    /// Header flags (bit 0 = preserves original IDs).
    pub flags: u8,
    /// Number of directory entries.
    pub segment_count: u16,
    /// Directory byte offset (always 64).
    pub directory_offset: u64,
    /// Directory byte length (`segment_count × 48`).
    pub directory_length: u64,
    /// First segment data offset (8-byte aligned).
    pub data_offset: u64,
    /// Logical node count.
    pub logical_node_count: u64,
    /// Logical edge count.
    pub logical_edge_count: u64,
    /// CRC-32 over directory bytes.
    pub directory_crc32: u32,
}

impl V5Header {
    /// True when original IDs are preserved (flags bit 0).
    #[must_use]
    pub fn preserves_ids(&self) -> bool {
        self.flags & 0x01 != 0
    }
}

/// Full checked directory for a v5 payload.
#[derive(Debug, Clone)]
pub struct SegmentDirectory {
    /// Parsed header fields.
    pub header: V5Header,
    /// Checked directory entries in ascending kind order.
    pub entries: Vec<SegmentEntry>,
    /// Retained slice of the raw directory bytes for CRC rechecks.
    pub directory_bytes: Bytes,
}

impl SegmentDirectory {
    /// Looks up the first entry of `kind`.
    #[must_use]
    pub fn get(&self, kind: SegmentKind) -> Option<&SegmentEntry> {
        self.entries.iter().find(|e| e.kind == kind)
    }

    /// Requires `kind` and returns it, or an error.
    ///
    /// # Errors
    ///
    /// Returns an error when the segment is absent.
    pub fn require(&self, kind: SegmentKind) -> Result<&SegmentEntry, String> {
        self.get(kind)
            .ok_or_else(|| format!("required v5 segment {:?} missing", kind))
    }
}

/// Parses and validates the 64-byte v5 header.
///
/// # Errors
///
/// Returns an error on truncation, bad magic, wrong version, non-zero
/// reserved fields, or inconsistent directory geometry.
pub fn parse_v5_header(data: &[u8]) -> Result<V5Header, String> {
    if data.len() < HEADER_LEN {
        return Err("truncated CompactStore v5 header".into());
    }
    if data[0..4] != *b"GCST" {
        return Err("bad CompactStore magic".into());
    }
    if data[4] != FORMAT_VERSION_V5 {
        return Err(format!(
            "unsupported CompactStore section version {} (expected {FORMAT_VERSION_V5})",
            data[4]
        ));
    }
    let flags = data[5];
    let mut pos = 6;
    let header_length = read_u16_le(data, &mut pos).map_err(str::to_string)?;
    if header_length as usize != HEADER_LEN {
        return Err(format!(
            "v5 header_length {header_length} must be {HEADER_LEN}"
        ));
    }
    let segment_count = read_u16_le(data, &mut pos).map_err(str::to_string)?;
    let directory_entry_length = read_u16_le(data, &mut pos).map_err(str::to_string)?;
    if directory_entry_length as usize != DIRECTORY_ENTRY_LEN {
        return Err(format!(
            "v5 directory_entry_length {directory_entry_length} must be {DIRECTORY_ENTRY_LEN}"
        ));
    }
    let layout_flags = read_u32_le(data, &mut pos).map_err(str::to_string)?;
    if layout_flags != 0 {
        return Err(format!("v5 layout_flags must be zero, got {layout_flags}"));
    }
    let directory_offset = read_u64_le(data, &mut pos).map_err(str::to_string)?;
    if directory_offset != HEADER_LEN as u64 {
        return Err(format!(
            "v5 directory_offset {directory_offset} must be {HEADER_LEN}"
        ));
    }
    let directory_length = read_u64_le(data, &mut pos).map_err(str::to_string)?;
    let data_offset = read_u64_le(data, &mut pos).map_err(str::to_string)?;
    let logical_node_count = read_u64_le(data, &mut pos).map_err(str::to_string)?;
    let logical_edge_count = read_u64_le(data, &mut pos).map_err(str::to_string)?;
    let directory_crc32 = read_u32_le(data, &mut pos).map_err(str::to_string)?;
    let reserved = read_u32_le(data, &mut pos).map_err(str::to_string)?;
    if reserved != 0 {
        return Err(format!("v5 header reserved must be zero, got {reserved}"));
    }

    let expected_dir_len = u64::from(segment_count)
        .checked_mul(DIRECTORY_ENTRY_LEN as u64)
        .ok_or("v5 directory_length overflow")?;
    if directory_length != expected_dir_len {
        return Err(format!(
            "v5 directory_length {directory_length} != segment_count×{DIRECTORY_ENTRY_LEN} ({expected_dir_len})"
        ));
    }
    if data_offset < directory_offset.saturating_add(directory_length) {
        return Err("v5 data_offset overlaps directory".into());
    }
    if data_offset % 8 != 0 {
        return Err(format!(
            "v5 data_offset {data_offset} is not 8-byte aligned"
        ));
    }

    Ok(V5Header {
        flags,
        segment_count,
        directory_offset,
        directory_length,
        data_offset,
        logical_node_count,
        logical_edge_count,
        directory_crc32,
    })
}

/// Validates a segment range against the payload (excluding trailing CRC).
///
/// # Errors
///
/// Returns an error when the range is out of bounds, misaligned, or has
/// an element geometry overflow.
pub fn validate_segment_range(
    entry: &SegmentEntry,
    payload_len_without_crc: u64,
    data_offset: u64,
) -> Result<(), String> {
    if entry.offset < data_offset {
        return Err(format!(
            "segment {:?} offset {} before data_offset {data_offset}",
            entry.kind, entry.offset
        ));
    }
    let end = entry
        .end()
        .ok_or_else(|| format!("segment {:?} offset+length overflow", entry.kind))?;
    if end > payload_len_without_crc {
        return Err(format!(
            "segment {:?} range [{}, {}) exceeds payload {}",
            entry.kind, entry.offset, end, payload_len_without_crc
        ));
    }
    let alignment = u64::from(entry.alignment.max(1));
    if !alignment.is_power_of_two() || alignment > 16 {
        return Err(format!(
            "segment {:?} alignment {alignment} must be a power of two in {{1,2,4,8,16}}",
            entry.kind
        ));
    }
    if !entry.offset.is_multiple_of(alignment) {
        return Err(format!(
            "segment {:?} offset {} not aligned to {alignment}",
            entry.kind, entry.offset
        ));
    }
    if entry.element_width > 0 {
        let need = u64::from(entry.element_count)
            .checked_mul(u64::from(entry.element_width))
            .ok_or_else(|| {
                format!(
                    "segment {:?} element_count×element_width overflow",
                    entry.kind
                )
            })?;
        if need > entry.length {
            return Err(format!(
                "segment {:?} element_count×width {need} > length {}",
                entry.kind, entry.length
            ));
        }
    }
    Ok(())
}

/// Parses directory entries and validates ranges / CRC / sort order.
///
/// # Errors
///
/// Returns an error on truncation, CRC mismatch, overlap, unknown kinds,
/// non-zero reserved fields, or alignment violations.
pub fn parse_segment_directory(
    data: &Bytes,
    payload_len_without_crc: usize,
) -> Result<SegmentDirectory, String> {
    let header = parse_v5_header(data.as_ref())?;
    let dir_start = header.directory_offset as usize;
    let dir_end = dir_start
        .checked_add(header.directory_length as usize)
        .ok_or("v5 directory end overflow")?;
    if dir_end > data.len() {
        return Err("truncated v5 directory".into());
    }
    let directory_bytes = data.slice(dir_start..dir_end);
    let computed = crc32fast::hash(directory_bytes.as_ref());
    if computed != header.directory_crc32 {
        return Err(format!(
            "v5 directory CRC mismatch: stored {:#010X}, computed {:#010X}",
            header.directory_crc32, computed
        ));
    }

    let mut entries = Vec::with_capacity(header.segment_count as usize);
    let mut pos = 0usize;
    let dir = directory_bytes.as_ref();
    let mut prev_kind: Option<u16> = None;
    let mut prev_end: u64 = header.data_offset;

    for _ in 0..header.segment_count {
        let kind_raw = read_u16_le(dir, &mut pos).map_err(str::to_string)?;
        let kind = SegmentKind::from_u16(kind_raw)?;
        if let Some(prev) = prev_kind
            && kind_raw < prev
        {
            return Err(format!(
                "v5 directory not in ascending kind order: {kind_raw} after {prev}"
            ));
        }
        if let Some(prev) = prev_kind
            && kind_raw == prev
        {
            return Err(format!("duplicate v5 segment kind {kind_raw}"));
        }
        prev_kind = Some(kind_raw);

        let encoding_version = read_u16_le(dir, &mut pos).map_err(str::to_string)?;
        let flags = read_u16_le(dir, &mut pos).map_err(str::to_string)?;
        let alignment = read_u16_le(dir, &mut pos).map_err(str::to_string)?;
        let offset = read_u64_le(dir, &mut pos).map_err(str::to_string)?;
        let length = read_u64_le(dir, &mut pos).map_err(str::to_string)?;
        let element_width = read_u32_le(dir, &mut pos).map_err(str::to_string)?;
        let element_count = read_u32_le(dir, &mut pos).map_err(str::to_string)?;
        let crc32 = read_u32_le(dir, &mut pos).map_err(str::to_string)?;
        let reserved_a = read_u32_le(dir, &mut pos).map_err(str::to_string)?;
        let reserved_b = read_u64_le(dir, &mut pos).map_err(str::to_string)?;
        if reserved_a != 0 || reserved_b != 0 {
            return Err(format!(
                "v5 segment {:?} reserved fields must be zero",
                kind
            ));
        }

        let entry = SegmentEntry {
            kind,
            encoding_version,
            flags,
            alignment,
            offset,
            length,
            element_width,
            element_count,
            crc32,
        };
        validate_segment_range(&entry, payload_len_without_crc as u64, header.data_offset)?;
        if entry.offset < prev_end {
            return Err(format!(
                "v5 segment {:?} overlaps previous range (offset {} < prev_end {prev_end})",
                kind, entry.offset
            ));
        }
        prev_end = entry.end().unwrap_or(prev_end);
        entries.push(entry);
    }

    Ok(SegmentDirectory {
        header,
        entries,
        directory_bytes,
    })
}

/// Slices a segment's bytes after validating its per-segment CRC.
///
/// # Errors
///
/// Returns an error on range issues or CRC mismatch.
pub fn slice_segment_checked(data: &Bytes, entry: &SegmentEntry) -> Result<Bytes, String> {
    let start = usize::try_from(entry.offset).map_err(|_| "segment offset exceeds usize")?;
    let len = usize::try_from(entry.length).map_err(|_| "segment length exceeds usize")?;
    let end = start.checked_add(len).ok_or("segment end overflow")?;
    if end > data.len() {
        return Err(format!("segment {:?} out of bounds", entry.kind));
    }
    let slice = data.slice(start..end);
    let computed = crc32fast::hash(slice.as_ref());
    if computed != entry.crc32 {
        return Err(format!(
            "segment {:?} CRC mismatch: stored {:#010X}, computed {:#010X}",
            entry.kind, entry.crc32, computed
        ));
    }
    Ok(slice)
}
