//! Mapped string dictionary: `StringOffsets` + `StringBytes`.

use super::views::U64View;
use bytes::Bytes;

/// Zero-copy dictionary backed by offset + UTF-8 byte segments.
///
/// Code→string is O(1) via offset table. String→code uses linear scan over
/// dictionary size for small schemas, or the optional sorted
/// `DictionaryCodeIndex` (see directory) for O(log D).
#[derive(Debug, Clone)]
pub struct MappedStringDictionary {
    offsets: U64View,
    bytes: Bytes,
}

impl MappedStringDictionary {
    /// Builds a dictionary from a u64 offset array (with trailing sentinel)
    /// and raw UTF-8 bytes.
    ///
    /// # Errors
    ///
    /// Returns an error on misalignment, empty offsets, out-of-order offsets,
    /// out-of-range offsets, or invalid UTF-8 in any entry.
    pub fn new(offsets_bytes: Bytes, string_bytes: Bytes) -> Result<Self, String> {
        let offsets = U64View::new(offsets_bytes).map_err(str::to_string)?;
        if offsets.len() < 1 {
            return Err("StringOffsets must contain at least a sentinel".into());
        }
        let dict_len = offsets.len() - 1;
        let mut prev = 0u64;
        for i in 0..=dict_len {
            let off = offsets
                .get(i)
                .ok_or_else(|| format!("StringOffsets missing entry {i}"))?;
            if off < prev {
                return Err(format!(
                    "StringOffsets not monotonic at {i}: {off} < {prev}"
                ));
            }
            if off as usize > string_bytes.len() {
                return Err(format!(
                    "StringOffsets[{i}]={off} exceeds StringBytes length {}",
                    string_bytes.len()
                ));
            }
            prev = off;
        }
        // Validate UTF-8 for every entry before exposing the view.
        for i in 0..dict_len {
            let _ = Self::string_at_raw(&offsets, &string_bytes, i)?;
        }
        Ok(Self {
            offsets,
            bytes: string_bytes,
        })
    }

    /// Empty dictionary (one sentinel at 0).
    #[must_use]
    pub fn empty() -> Self {
        let mut offsets = Vec::new();
        offsets.extend_from_slice(&0u64.to_le_bytes());
        Self {
            offsets: U64View::new(Bytes::from(offsets)).expect("empty offsets aligned"),
            bytes: Bytes::new(),
        }
    }

    /// Number of dictionary strings (excluding the sentinel).
    #[must_use]
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Returns `true` when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Mapped bytes for offsets + string blob.
    #[must_use]
    pub fn mapped_bytes(&self) -> usize {
        self.offsets.byte_len() + self.bytes.len()
    }

    /// O(1) code→string lookup. Returns a borrow into the mapped UTF-8.
    #[must_use]
    pub fn get(&self, code: u32) -> Option<&str> {
        let idx = code as usize;
        if idx >= self.len() {
            return None;
        }
        Self::string_at_raw(&self.offsets, &self.bytes, idx).ok()
    }

    /// Linear string→code scan. Prefer a code index for large dictionaries.
    #[must_use]
    pub fn encode(&self, value: &str) -> Option<u32> {
        for i in 0..self.len() {
            if self.get(i as u32) == Some(value) {
                return u32::try_from(i).ok();
            }
        }
        None
    }

    /// Raw offset array bytes (refcount clone; file-backed when mapped).
    #[must_use]
    pub fn offsets_bytes(&self) -> Bytes {
        self.offsets.bytes().clone()
    }

    /// Raw UTF-8 blob bytes (refcount clone; file-backed when mapped).
    #[must_use]
    pub fn string_bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    fn string_at_raw<'a>(
        offsets: &U64View,
        bytes: &'a Bytes,
        idx: usize,
    ) -> Result<&'a str, String> {
        let start = offsets
            .get(idx)
            .ok_or_else(|| format!("missing StringOffsets[{idx}]"))? as usize;
        let end = offsets
            .get(idx + 1)
            .ok_or_else(|| format!("missing StringOffsets sentinel for {idx}"))?
            as usize;
        if end < start || end > bytes.len() {
            return Err(format!(
                "invalid StringOffsets range [{start}, {end}) for code {idx}"
            ));
        }
        std::str::from_utf8(&bytes[start..end])
            .map_err(|_| format!("invalid UTF-8 in StringBytes for code {idx}"))
    }
}

/// Builds offset + bytes segments from a list of unique strings.
#[must_use]
pub fn build_string_segments(strings: &[&str]) -> (Vec<u8>, Vec<u8>) {
    let mut offsets = Vec::with_capacity((strings.len() + 1) * 8);
    let mut bytes = Vec::new();
    for s in strings {
        offsets.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        bytes.extend_from_slice(s.as_bytes());
    }
    offsets.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    (offsets, bytes)
}
