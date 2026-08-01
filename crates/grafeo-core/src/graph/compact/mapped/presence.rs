//! Per-column row-presence and row-null companion bitmaps (G-EM0.5b D0.8.0).
//!
//! The canonical column body stores fixed-width values for every row offset
//! `0..row_count` in a table, using deterministic placeholder bytes where a
//! row does not carry a typed value. Two optional companion segments recover
//! the full three-way logical state per `(column, row)`:
//!
//! - **presence** ([`SegmentKind::ColumnRowPresence`]): bit `i` = 1 when row
//!   `i` carries the property key at all. Omitted when every row has the
//!   property (all-present is the default).
//! - **nullness** ([`SegmentKind::ColumnRowNull`]): bit `i` = 1 when row
//!   `i` carries a present `Value::Null`. Omitted when no present row stores
//!   null (all-non-null is the default).
//!
//! The three-way distinction the reader reconstructs:
//!
//! | presence bit | null bit | logical value                          |
//! |--------------|----------|----------------------------------------|
//! | 0            | (any)    | absent — property key not present      |
//! | 1            | 1        | present null — `Some(Value::Null)`     |
//! | 1            | 0        | present value — typed body is authoritative |
//!
//! ## Wire layout
//!
//! Both segments are a sequence of fixed-width records, one per column that
//! needs the companion, in column-directory order:
//!
//! ```text
//! ColumnRowPresence / ColumnRowNull record:
//!   column_index u32 LE   (index into the flat ColumnDirectory)
//!   row_count    u32 LE   (number of bitmap rows; must match the table's row count)
//!   bitmap       ceil(row_count / 8) bytes, bit i = row i (LSB-first within each byte)
//! ```
//!
//! The record header is 8 bytes; the bitmap is byte-packed (not u64-word
//! packed) so the segment length is exact and independently checksummed.
//!
//! Old readers fail closed on these unknown segment kinds (via
//! [`SegmentKind::from_u16`]). A new reader treats their absence as the old-v5
//! default: every encoded row present and non-null.

use super::SegmentKind;
use bytes::Bytes;

/// Byte length of one presence/null record header (`column_index` + `row_count`).
pub const PRESENCE_RECORD_HEADER_LEN: usize = 8;

/// Computes the byte length of a packed bitmap for `row_count` rows.
#[must_use]
pub const fn bitmap_bytes(row_count: u32) -> usize {
    (row_count as usize).div_ceil(8)
}

/// Streams the presence bitmap records for all columns into `emit`.
///
/// `columns` yields `(column_index, row_count, presence_iter)` where
/// `presence_iter(row)` is `true` when the row carries the property.
///
/// # Errors
///
/// Returns [`GenerationError`] on write failure.
pub fn write_presence_segment(
    sink: &mut dyn FnMut(&[u8]) -> Result<(), String>,
    records: &[(u32, u32, Vec<bool>)],
) -> Result<(), String> {
    write_bitmap_segment(sink, records)
}

/// Streams the null bitmap records for all columns into `emit`.
///
/// # Errors
///
/// Returns [`GenerationError`] on write failure.
pub fn write_null_segment(
    sink: &mut dyn FnMut(&[u8]) -> Result<(), String>,
    records: &[(u32, u32, Vec<bool>)],
) -> Result<(), String> {
    write_bitmap_segment(sink, records)
}

fn write_bitmap_segment(
    sink: &mut dyn FnMut(&[u8]) -> Result<(), String>,
    records: &[(u32, u32, Vec<bool>)],
) -> Result<(), String> {
    for (column_index, row_count, bits) in records {
        if bits.len() != *row_count as usize {
            return Err(format!(
                "presence/null bitmap row_count {} != bits {}",
                row_count,
                bits.len()
            ));
        }
        let mut header = [0u8; PRESENCE_RECORD_HEADER_LEN];
        header[0..4].copy_from_slice(&column_index.to_le_bytes());
        header[4..8].copy_from_slice(&row_count.to_le_bytes());
        sink(&header)?;
        let packed = pack_bits(bits);
        sink(&packed)?;
    }
    Ok(())
}

/// Packs booleans into LSB-first bytes.
#[must_use]
pub fn pack_bits(bits: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; bits.len().div_ceil(8)];
    for (i, &b) in bits.iter().enumerate() {
        if b {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
}

/// Unpacks LSB-first bytes into booleans.
#[must_use]
pub fn unpack_bits(bytes: &[u8], row_count: usize) -> Vec<bool> {
    let mut out = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let byte = bytes.get(i / 8).copied().unwrap_or(0);
        out.push(byte & (1 << (i % 8)) != 0);
    }
    out
}

/// A parsed view over one presence or null companion segment.
///
/// Retains the mapped segment bytes and yields per-column row bits without a
/// heap index proportional to memberships.
#[derive(Debug, Clone)]
pub struct RowBitmapView {
    /// Records in column-directory order.
    records: Vec<RowBitmapRecord>,
}

#[derive(Debug, Clone)]
struct RowBitmapRecord {
    column_index: u32,
    row_count: u32,
    /// Byte offset of this record's bitmap within the segment body.
    bitmap_offset: usize,
}

impl RowBitmapView {
    /// Parses and validates a presence or null segment body.
    ///
    /// # Errors
    ///
    /// Returns an error on a truncated record, a row-count/bitmap mismatch,
    /// or a non-monotonic column index.
    pub fn parse(bytes: &Bytes, kind: SegmentKind) -> Result<Self, String> {
        let mut records = Vec::new();
        let mut cursor = 0usize;
        let mut prev_col: Option<u32> = None;
        while cursor < bytes.len() {
            if cursor + PRESENCE_RECORD_HEADER_LEN > bytes.len() {
                return Err(format!(
                    "{kind:?} truncated record header at offset {cursor}"
                ));
            }
            let column_index = u32::from_le_bytes([
                bytes[cursor],
                bytes[cursor + 1],
                bytes[cursor + 2],
                bytes[cursor + 3],
            ]);
            let row_count = u32::from_le_bytes([
                bytes[cursor + 4],
                bytes[cursor + 5],
                bytes[cursor + 6],
                bytes[cursor + 7],
            ]);
            if let Some(p) = prev_col {
                if column_index <= p {
                    return Err(format!(
                        "{kind:?} column indices not strictly ascending ({column_index} <= {p})"
                    ));
                }
            }
            prev_col = Some(column_index);
            let bitmap_offset = cursor + PRESENCE_RECORD_HEADER_LEN;
            let nbytes = bitmap_bytes(row_count);
            if bitmap_offset + nbytes > bytes.len() {
                return Err(format!(
                    "{kind:?} truncated bitmap for column {column_index}"
                ));
            }
            records.push(RowBitmapRecord {
                column_index,
                row_count,
                bitmap_offset,
            });
            cursor = bitmap_offset + nbytes;
        }
        Ok(Self { records })
    }

    /// Returns `true` when no columns need this companion.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Looks up the bit for `(column_index, row)`.
    ///
    /// Returns `None` when the column has no companion record (meaning the
    /// old-v5 default applies) or the row is out of range.
    #[must_use]
    pub fn get(&self, bytes: &Bytes, column_index: u32, row: u32) -> Option<bool> {
        let rec = self
            .records
            .binary_search_by_key(&column_index, |r| r.column_index)
            .ok()
            .map(|i| &self.records[i])?;
        if row >= rec.row_count {
            return None;
        }
        let bit_index = row as usize;
        let byte = bytes.get(rec.bitmap_offset + bit_index / 8).copied()?;
        Some(byte & (1 << (bit_index % 8)) != 0)
    }

    /// Number of companion records.
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_unpack_round_trip() {
        let bits = vec![true, false, true, true, false, false, false, true, true];
        let packed = pack_bits(&bits);
        assert_eq!(packed.len(), 2);
        let unpacked = unpack_bits(&packed, bits.len());
        assert_eq!(unpacked, bits);
    }

    #[test]
    fn view_parse_and_lookup() {
        let records = vec![
            (0u32, 5u32, vec![true, true, false, true, true]),
            (3u32, 4u32, vec![false, true, false, true]),
        ];
        let mut body = Vec::new();
        write_bitmap_segment(
            &mut |b| Ok::<_, String>(body.extend_from_slice(b)),
            &records,
        )
        .unwrap();
        let bytes = Bytes::from(body);
        let view = RowBitmapView::parse(&bytes, SegmentKind::ColumnRowPresence).unwrap();
        assert_eq!(view.record_count(), 2);
        assert_eq!(view.get(&bytes, 0, 0), Some(true));
        assert_eq!(view.get(&bytes, 0, 2), Some(false));
        assert_eq!(view.get(&bytes, 3, 1), Some(true));
        assert_eq!(view.get(&bytes, 1, 0), None); // no companion record
    }

    #[test]
    fn view_rejects_truncated() {
        let bytes = Bytes::from_static(&[1, 2, 3]);
        assert!(RowBitmapView::parse(&bytes, SegmentKind::ColumnRowPresence).is_err());
    }
}
