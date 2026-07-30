//! Checked little-endian array views over mapped [`Bytes`].

use bytes::Bytes;

/// Fixed-width little-endian `u32` view over a mapped byte range.
#[derive(Debug, Clone)]
pub struct U32View {
    bytes: Bytes,
    len: usize,
}

impl U32View {
    /// Builds a view over `bytes`, requiring `bytes.len()` to be a multiple of 4.
    ///
    /// # Errors
    ///
    /// Returns an error when the length is not 4-byte aligned.
    pub fn new(bytes: Bytes) -> Result<Self, &'static str> {
        if !bytes.len().is_multiple_of(4) {
            return Err("U32View length is not a multiple of 4");
        }
        let len = bytes.len() / 4;
        Ok(Self { bytes, len })
    }

    /// Empty view (zero elements).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            bytes: Bytes::new(),
            len: 0,
        }
    }

    /// Number of `u32` elements.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` when empty.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Mapped byte length.
    #[must_use]
    #[inline]
    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns the element at `idx`, or `None` if out of range.
    #[must_use]
    #[inline]
    pub fn get(&self, idx: usize) -> Option<u32> {
        let start = idx.checked_mul(4)?;
        let end = start.checked_add(4)?;
        let chunk: [u8; 4] = self.bytes.get(start..end)?.try_into().ok()?;
        Some(u32::from_le_bytes(chunk))
    }

    /// Slice of LE bytes for a half-open element range `[start, end)`.
    ///
    /// Used to present CSR neighbor lists without allocating a `Vec`.
    ///
    /// # Errors
    ///
    /// Returns an error when the range is out of bounds.
    pub fn element_bytes(&self, start: usize, end: usize) -> Result<&[u8], &'static str> {
        if start > end || end > self.len {
            return Err("U32View element range out of bounds");
        }
        let byte_start = start
            .checked_mul(4)
            .ok_or("U32View element range overflow")?;
        let byte_end = end.checked_mul(4).ok_or("U32View element range overflow")?;
        self.bytes
            .get(byte_start..byte_end)
            .ok_or("U32View element range out of bounds")
    }

    /// Decodes neighbors as a temporary owned slice into `scratch`.
    ///
    /// Prefer [`Self::get`] for single elements. Scratch is query-local and
    /// must not be retained beyond the current query.
    pub fn copy_range_into(&self, start: usize, end: usize, scratch: &mut Vec<u32>) -> bool {
        if start > end || end > self.len {
            return false;
        }
        scratch.clear();
        scratch.reserve(end - start);
        for i in start..end {
            scratch.push(self.get(i).unwrap_or(0));
        }
        true
    }

    /// Underlying mapped bytes (refcount clone).
    #[must_use]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

/// Fixed-width little-endian `u64` view over a mapped byte range.
#[derive(Debug, Clone)]
pub struct U64View {
    bytes: Bytes,
    len: usize,
}

impl U64View {
    /// Builds a view over `bytes`, requiring `bytes.len()` to be a multiple of 8.
    ///
    /// # Errors
    ///
    /// Returns an error when the length is not 8-byte aligned.
    pub fn new(bytes: Bytes) -> Result<Self, &'static str> {
        if !bytes.len().is_multiple_of(8) {
            return Err("U64View length is not a multiple of 8");
        }
        let len = bytes.len() / 8;
        Ok(Self { bytes, len })
    }

    /// Empty view.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            bytes: Bytes::new(),
            len: 0,
        }
    }

    /// Number of `u64` elements.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` when empty.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Mapped byte length.
    #[must_use]
    #[inline]
    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns the element at `idx`, or `None` if out of range.
    #[must_use]
    #[inline]
    pub fn get(&self, idx: usize) -> Option<u64> {
        let start = idx.checked_mul(8)?;
        let end = start.checked_add(8)?;
        let chunk: [u8; 8] = self.bytes.get(start..end)?.try_into().ok()?;
        Some(u64::from_le_bytes(chunk))
    }

    /// Underlying mapped bytes.
    #[must_use]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

/// Reads a little-endian `u16` at `pos` and advances.
///
/// # Errors
///
/// Returns an error on truncation.
#[inline]
pub fn read_u16_le(data: &[u8], pos: &mut usize) -> Result<u16, &'static str> {
    if *pos + 2 > data.len() {
        return Err("truncated u16");
    }
    let v = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
    *pos += 2;
    Ok(v)
}

/// Reads a little-endian `u32` at `pos` and advances.
///
/// # Errors
///
/// Returns an error on truncation.
#[inline]
pub fn read_u32_le(data: &[u8], pos: &mut usize) -> Result<u32, &'static str> {
    if *pos + 4 > data.len() {
        return Err("truncated u32");
    }
    let v = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
    *pos += 4;
    Ok(v)
}

/// Reads a little-endian `u64` at `pos` and advances.
///
/// # Errors
///
/// Returns an error on truncation.
#[inline]
pub fn read_u64_le(data: &[u8], pos: &mut usize) -> Result<u64, &'static str> {
    if *pos + 8 > data.len() {
        return Err("truncated u64");
    }
    let v = u64::from_le_bytes([
        data[*pos],
        data[*pos + 1],
        data[*pos + 2],
        data[*pos + 3],
        data[*pos + 4],
        data[*pos + 5],
        data[*pos + 6],
        data[*pos + 7],
    ]);
    *pos += 8;
    Ok(v)
}
