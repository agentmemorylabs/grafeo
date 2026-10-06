//! CompactStore payload versions 5 and 6 (G4: 64-bit column-body geometry).
//!
//! v6 is the v5 mapped layout with every field that can pass 4 GiB widened.
//! Everything else is byte-identical to v5:
//!
//! - **ColumnBlockIndex records** are 24 bytes,
//!   `[body_offset u64][body_len u64][row_count u32][reserved u32]`
//!   (v5: 12 bytes, `[body_offset u32][body_len u32][row_count u32]`).
//! - **Directory entries** keep 48 bytes but carry a `u64` `element_count`:
//!   `[kind u16][enc u16][flags u16][align u16][offset u64][length u64]
//!   [element_width u32][crc32 u32][element_count u64][reserved u64]`
//!   (v5: `[element_width u32][element_count u32][crc32 u32][reserved u32]
//!   [reserved u64]`).
//! - **Vector column bodies** may use wide headers, legal only in v6:
//!   disc 7 = `Float32Vector` `[7][dims u16][component_count u64]`,
//!   disc 8 = `Int8Vector` `[8][dims u16][byte_len u64]`
//!   (the v5 discs 5 and 3 carry those counts as `u32`).
//! - The header version byte is 6, so a reader that knows only v5 refuses the
//!   payload ("unsupported CompactStore section version 6") instead of
//!   misreading the wider records.
//!
//! Writers pick the version per payload through [`PayloadVersionPolicy`]:
//! `Auto` writes v5 whenever every field fits (byte-identical to the v5
//! writer) and v6 only when one does not; `V6` always writes v6 with wide
//! vector headers, so the v6 paths run on small test data too.

use super::views::{read_u16_le, read_u32_le, read_u64_le};

/// CompactStore payload version with 64-bit column-body geometry (G4).
pub const FORMAT_VERSION_V6: u8 = 6;

/// v5 ColumnBlockIndex record length in bytes.
pub const BLOCK_INDEX_RECORD_LEN_V5: usize = 12;

/// v6 ColumnBlockIndex record length in bytes.
pub const BLOCK_INDEX_RECORD_LEN_V6: usize = 24;

/// Column-body discriminant: `Float32Vector` with a `u64` component count (v6 only).
pub const DISC_F32_VECTOR_WIDE: u8 = 7;

/// Column-body discriminant: `Int8Vector` with a `u64` byte length (v6 only).
pub const DISC_I8_VECTOR_WIDE: u8 = 8;

/// The mapped CompactStore payload layouts this build reads and writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PayloadVersion {
    /// v5: `u32` column-body offsets, lengths and directory element counts.
    V5,
    /// v6: `u64` column-body offsets, lengths and directory element counts.
    V6,
}

impl PayloadVersion {
    /// Parses a header version byte, failing closed on anything but 5 or 6.
    ///
    /// # Errors
    ///
    /// Returns an error naming the version for any other byte.
    pub fn from_byte(byte: u8) -> Result<Self, String> {
        match byte {
            super::FORMAT_VERSION_V5 => Ok(Self::V5),
            FORMAT_VERSION_V6 => Ok(Self::V6),
            other => Err(format!(
                "unsupported CompactStore section version {other} (expected {} or {FORMAT_VERSION_V6})",
                super::FORMAT_VERSION_V5
            )),
        }
    }

    /// The header version byte.
    #[must_use]
    pub const fn byte(self) -> u8 {
        match self {
            Self::V5 => super::FORMAT_VERSION_V5,
            Self::V6 => FORMAT_VERSION_V6,
        }
    }

    /// ColumnBlockIndex record length for this version.
    #[must_use]
    pub const fn block_index_record_len(self) -> usize {
        match self {
            Self::V5 => BLOCK_INDEX_RECORD_LEN_V5,
            Self::V6 => BLOCK_INDEX_RECORD_LEN_V6,
        }
    }

    /// ColumnBlockIndex segment alignment for this version.
    #[must_use]
    pub const fn block_index_alignment(self) -> u16 {
        match self {
            Self::V5 => 4,
            Self::V6 => 8,
        }
    }
}

/// Which payload version a writer emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadVersionPolicy {
    /// v5 when every field fits its v5 width, v6 only when one does not.
    #[default]
    Auto,
    /// Always v6, with wide vector headers on every vector column.
    V6,
}

impl PayloadVersionPolicy {
    /// Whether a vector column body with `count` components (or bytes) uses
    /// the wide v6 header.
    #[must_use]
    pub fn wide_vector_header(self, count: u64) -> bool {
        self == Self::V6 || count > u64::from(u32::MAX)
    }

    /// Resolves the payload version once every wide-field need is known.
    #[must_use]
    pub fn resolve(self, needs_v6: bool) -> PayloadVersion {
        if self == Self::V6 || needs_v6 {
            PayloadVersion::V6
        } else {
            PayloadVersion::V5
        }
    }
}

/// A value did not fit the wire width of the payload version being written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireOverflow {
    /// Which field overflowed.
    pub what: &'static str,
    /// The value that did not fit.
    pub count: u64,
    /// The field's maximum.
    pub max: u64,
}

impl std::fmt::Display for WireOverflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} count {} exceeds wire max {}",
            self.what, self.count, self.max
        )
    }
}

fn narrow(what: &'static str, value: u64) -> Result<u32, WireOverflow> {
    u32::try_from(value).map_err(|_| WireOverflow {
        what,
        count: value,
        max: u64::from(u32::MAX),
    })
}

/// One column's body geometry inside `ColumnBodies`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockIndexRecord {
    /// Byte offset of the body within the `ColumnBodies` segment.
    pub body_offset: u64,
    /// Byte length of the body.
    pub body_len: u64,
    /// Logical row count of the column.
    pub row_count: u32,
}

impl BlockIndexRecord {
    /// True when the record needs v6: its offset or its length, each on its
    /// own, does not fit the v5 `u32` field. The end (`offset + len`) is not
    /// a v5 wire field and may pass `u32::MAX` in a valid v5 payload (the v5
    /// reader adds them in `usize`); range validation is the reader's job.
    #[must_use]
    pub fn needs_v6(&self) -> bool {
        self.body_offset > u64::from(u32::MAX) || self.body_len > u64::from(u32::MAX)
    }
}

/// Appends one ColumnBlockIndex record in `version`'s layout.
///
/// # Errors
///
/// Returns [`WireOverflow`] when writing v5 and the offset or length does
/// not fit `u32`.
pub fn write_block_index_record(
    buf: &mut Vec<u8>,
    version: PayloadVersion,
    rec: BlockIndexRecord,
) -> Result<(), WireOverflow> {
    match version {
        PayloadVersion::V5 => {
            buf.extend_from_slice(&narrow("col_body_offset", rec.body_offset)?.to_le_bytes());
            buf.extend_from_slice(&narrow("col_body_len", rec.body_len)?.to_le_bytes());
        }
        PayloadVersion::V6 => {
            buf.extend_from_slice(&rec.body_offset.to_le_bytes());
            buf.extend_from_slice(&rec.body_len.to_le_bytes());
        }
    }
    buf.extend_from_slice(&rec.row_count.to_le_bytes());
    if version == PayloadVersion::V6 {
        buf.extend_from_slice(&0u32.to_le_bytes());
    }
    Ok(())
}

/// Reads ColumnBlockIndex record `index` in `version`'s layout.
///
/// # Errors
///
/// Returns an error on truncation or a non-zero v6 reserved field.
pub fn read_block_index_record(
    bytes: &[u8],
    version: PayloadVersion,
    index: usize,
) -> Result<BlockIndexRecord, String> {
    let len = version.block_index_record_len();
    let base = index
        .checked_mul(len)
        .ok_or("ColumnBlockIndex index overflow")?;
    let end = base
        .checked_add(len)
        .ok_or("ColumnBlockIndex end overflow")?;
    let rec = bytes.get(base..end).ok_or("ColumnBlockIndex truncated")?;
    let mut pos = 0usize;
    let (body_offset, body_len) = match version {
        PayloadVersion::V5 => (
            u64::from(read_u32_le(rec, &mut pos)?),
            u64::from(read_u32_le(rec, &mut pos)?),
        ),
        PayloadVersion::V6 => (read_u64_le(rec, &mut pos)?, read_u64_le(rec, &mut pos)?),
    };
    let row_count = read_u32_le(rec, &mut pos)?;
    if version == PayloadVersion::V6 && read_u32_le(rec, &mut pos)? != 0 {
        return Err("ColumnBlockIndex v6 reserved field must be zero".into());
    }
    Ok(BlockIndexRecord {
        body_offset,
        body_len,
        row_count,
    })
}

/// The fields of one 48-byte segment directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectoryEntryFields {
    /// Segment kind code.
    pub kind: u16,
    /// Per-kind encoding version.
    pub encoding_version: u16,
    /// Flags (bit 0 = required).
    pub flags: u16,
    /// Required alignment.
    pub alignment: u16,
    /// Payload-relative byte offset.
    pub offset: u64,
    /// Byte length.
    pub length: u64,
    /// Fixed element width, or 0 for variable.
    pub element_width: u32,
    /// Element count for fixed-width segments.
    pub element_count: u64,
    /// IEEE CRC-32 over the segment bytes.
    pub crc32: u32,
}

/// Appends one 48-byte directory entry in `version`'s layout.
///
/// # Errors
///
/// Returns [`WireOverflow`] when writing v5 and `element_count` does not
/// fit `u32`.
pub fn write_directory_entry(
    buf: &mut Vec<u8>,
    version: PayloadVersion,
    e: &DirectoryEntryFields,
) -> Result<(), WireOverflow> {
    buf.extend_from_slice(&e.kind.to_le_bytes());
    buf.extend_from_slice(&e.encoding_version.to_le_bytes());
    buf.extend_from_slice(&e.flags.to_le_bytes());
    buf.extend_from_slice(&e.alignment.to_le_bytes());
    buf.extend_from_slice(&e.offset.to_le_bytes());
    buf.extend_from_slice(&e.length.to_le_bytes());
    buf.extend_from_slice(&e.element_width.to_le_bytes());
    match version {
        PayloadVersion::V5 => {
            let count = narrow("segment_element_count", e.element_count)?;
            buf.extend_from_slice(&count.to_le_bytes());
            buf.extend_from_slice(&e.crc32.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes()); // reserved_a
        }
        PayloadVersion::V6 => {
            buf.extend_from_slice(&e.crc32.to_le_bytes());
            buf.extend_from_slice(&e.element_count.to_le_bytes());
        }
    }
    buf.extend_from_slice(&0u64.to_le_bytes()); // reserved
    Ok(())
}

/// Reads one 48-byte directory entry at `pos` in `version`'s layout,
/// advancing `pos`.
///
/// # Errors
///
/// Returns an error on truncation or non-zero reserved fields.
pub fn read_directory_entry(
    dir: &[u8],
    pos: &mut usize,
    version: PayloadVersion,
) -> Result<DirectoryEntryFields, String> {
    let kind = read_u16_le(dir, pos)?;
    let encoding_version = read_u16_le(dir, pos)?;
    let flags = read_u16_le(dir, pos)?;
    let alignment = read_u16_le(dir, pos)?;
    let offset = read_u64_le(dir, pos)?;
    let length = read_u64_le(dir, pos)?;
    let element_width = read_u32_le(dir, pos)?;
    let (element_count, crc32, reserved_a) = match version {
        PayloadVersion::V5 => {
            let count = u64::from(read_u32_le(dir, pos)?);
            let crc = read_u32_le(dir, pos)?;
            (count, crc, read_u32_le(dir, pos)?)
        }
        PayloadVersion::V6 => {
            let crc = read_u32_le(dir, pos)?;
            (read_u64_le(dir, pos)?, crc, 0)
        }
    };
    let reserved_b = read_u64_le(dir, pos)?;
    if reserved_a != 0 || reserved_b != 0 {
        return Err(format!(
            "segment kind {kind} directory reserved fields must be zero"
        ));
    }
    Ok(DirectoryEntryFields {
        kind,
        encoding_version,
        flags,
        alignment,
        offset,
        length,
        element_width,
        element_count,
        crc32,
    })
}

/// Encodes a vector column-body header: `[disc][dims u16][count]`.
///
/// `wide` selects the v6 form (disc 7 / 8 with a `u64` count); otherwise
/// the v5 form (disc 5 / 3 with a `u32` count). `int8` selects
/// `Int8Vector` (count = byte length) over `Float32Vector` (count =
/// component count).
///
/// # Errors
///
/// Returns [`WireOverflow`] when the narrow form is requested and `count`
/// does not fit `u32`.
pub fn vector_body_header(
    int8: bool,
    dims: u16,
    count: u64,
    wide: bool,
) -> Result<Vec<u8>, WireOverflow> {
    let mut out = Vec::with_capacity(11);
    let disc = match (int8, wide) {
        (false, false) => 5,
        (false, true) => DISC_F32_VECTOR_WIDE,
        (true, false) => 3,
        (true, true) => DISC_I8_VECTOR_WIDE,
    };
    out.push(disc);
    out.extend_from_slice(&dims.to_le_bytes());
    if wide {
        out.extend_from_slice(&count.to_le_bytes());
    } else {
        let what = if int8 {
            "int8_vector_byte_len"
        } else {
            "vector_component_count"
        };
        out.extend_from_slice(&narrow(what, count)?.to_le_bytes());
    }
    Ok(out)
}

#[cfg(test)]
#[path = "payload_version_tests.rs"]
mod tests;
