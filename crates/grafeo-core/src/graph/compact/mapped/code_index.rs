//! Sorted dictionary code index (v5 kind 20) for O(log D) string→code.

// The `as usize`/`as u32`/`as u16` casts in this module convert between wire
// field widths and in-memory indices for data already bounds-checked against
// the resident section length; they cannot truncate on the 64-bit targets
// this engine supports.
#![allow(clippy::cast_possible_truncation)]
use super::payload_version::WireOverflow;
use super::string_dict::MappedStringDictionary;
use bytes::Bytes;

fn read_u32_at(data: &[u8], off: usize) -> Result<u32, String> {
    let end = off.checked_add(4).ok_or("u32 offset overflow")?;
    if end > data.len() {
        return Err("truncated u32".into());
    }
    Ok(u32::from_le_bytes([
        data[off],
        data[off + 1],
        data[off + 2],
        data[off + 3],
    ]))
}
fn read_u64_at(data: &[u8], off: usize) -> Result<u64, String> {
    let end = off.checked_add(8).ok_or("u64 offset overflow")?;
    if end > data.len() {
        return Err("truncated u64".into());
    }
    Ok(u64::from_le_bytes([
        data[off],
        data[off + 1],
        data[off + 2],
        data[off + 3],
        data[off + 4],
        data[off + 5],
        data[off + 6],
        data[off + 7],
    ]))
}

/// Wire size of one code-index record.
pub const CODE_INDEX_RECORD_LEN: usize = 16;

/// Builds a sorted `(string_offset, string_len, code)` index for `strings`.
///
/// `strings[i]` is dictionary code `i`. Records are sorted lexicographically
/// by UTF-8 content.
///
/// # Errors
///
/// [`WireOverflow`] when a code or a single string's length does not fit
/// the record's `u32` field.
pub fn build_dictionary_code_index(strings: &[&str]) -> Result<Vec<u8>, WireOverflow> {
    let mut records: Vec<(u64, u32, u32, &str)> = Vec::with_capacity(strings.len());
    let mut offset = 0u64;
    for (i, s) in strings.iter().enumerate() {
        let (code, len) = code_index_fields(i, s.len())?;
        records.push((offset, len, code, *s));
        offset += s.len() as u64;
    }
    records.sort_by(|a, b| a.3.cmp(b.3));
    let mut out = Vec::with_capacity(records.len() * CODE_INDEX_RECORD_LEN);
    for (off, len, code, _) in records {
        out.extend_from_slice(&off.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&code.to_le_bytes());
    }
    Ok(out)
}

/// Checked `(code, string_len)` for one code-index record.
///
/// # Errors
///
/// [`WireOverflow`] when either does not fit `u32`.
pub(crate) fn code_index_fields(code: usize, len: usize) -> Result<(u32, u32), WireOverflow> {
    let narrow = |what, v: usize| {
        u32::try_from(v).map_err(|_| WireOverflow {
            what,
            count: v as u64,
            max: u64::from(u32::MAX),
        })
    };
    Ok((narrow("dict_code", code)?, narrow("dict_string_len", len)?))
}

/// Binary-searchable mapped code index.
#[derive(Debug, Clone)]
pub struct DictionaryCodeIndex {
    data: Bytes,
}

impl DictionaryCodeIndex {
    /// Parses a kind-20 segment body.
    ///
    /// # Errors
    ///
    /// Returns an error when length is not a multiple of 16 or records are
    /// not sorted by dictionary string content.
    pub fn new(data: Bytes, dict: &MappedStringDictionary) -> Result<Self, String> {
        if !data.len().is_multiple_of(CODE_INDEX_RECORD_LEN) {
            return Err(format!(
                "DictionaryCodeIndex length {} not multiple of {CODE_INDEX_RECORD_LEN}",
                data.len()
            ));
        }
        let count = data.len() / CODE_INDEX_RECORD_LEN;
        if count != dict.len() {
            return Err(format!(
                "DictionaryCodeIndex count {count} != dictionary len {}",
                dict.len()
            ));
        }
        // Verify sorted order.
        let mut prev: Option<&str> = None;
        for i in 0..count {
            let (off, len, code) = record_at(data.as_ref(), i)?;
            let end = off
                .checked_add(u64::from(len))
                .ok_or("code index range overflow")?;
            if end as usize > dict.string_bytes().len() {
                return Err("code index string range exceeds StringBytes".into());
            }
            let s = dict
                .get(code)
                .ok_or_else(|| format!("code index code {code} missing from dictionary"))?;
            // Cross-check offset/len against dictionary.
            if s.len() != len as usize {
                return Err(format!(
                    "code index len {len} != dictionary string len {} for code {code}",
                    s.len()
                ));
            }
            if let Some(p) = prev
                && p > s
            {
                return Err("DictionaryCodeIndex not sorted by UTF-8 content".into());
            }
            prev = Some(s);
            let _ = off; // offset validated against end ≤ blob
        }
        Ok(Self { data })
    }

    /// O(log D) string→code lookup.
    #[must_use]
    pub fn lookup(&self, dict: &MappedStringDictionary, value: &str) -> Option<u32> {
        let count = self.data.len() / CODE_INDEX_RECORD_LEN;
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (_, _, code) = record_at(self.data.as_ref(), mid).ok()?;
            let s = dict.get(code)?;
            match s.cmp(value) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(code),
            }
        }
        None
    }

    /// Mapped byte length.
    #[must_use]
    pub fn mapped_bytes(&self) -> usize {
        self.data.len()
    }

    /// Underlying bytes (refcount clone).
    #[must_use]
    pub fn bytes(&self) -> Bytes {
        self.data.clone()
    }
}

fn record_at(data: &[u8], index: usize) -> Result<(u64, u32, u32), String> {
    let base = index
        .checked_mul(CODE_INDEX_RECORD_LEN)
        .ok_or("code index index overflow")?;
    if base + CODE_INDEX_RECORD_LEN > data.len() {
        return Err("code index truncated".into());
    }
    let off = read_u64_at(data, base)?;
    let len = read_u32_at(data, base + 8)?;
    let code = read_u32_at(data, base + 12)?;
    Ok((off, len, code))
}
