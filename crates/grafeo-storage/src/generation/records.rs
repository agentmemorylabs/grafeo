//! Bounded external-sort record framing (G-EM0.W0-A1).
//!
//! On-disk framing: `[key_len: u32 LE][payload_len: u32 LE][key bytes][payload bytes]`.
//! Clean EOF (0 bytes read) → `None`; partial header (1–7 bytes) → `UnexpectedEof`.

use std::io::{self, Read, Write};

/// Hard cap on any single record body half before allocation. Prevents a
/// malicious or malformed length prefix from triggering an oversized `Vec`.
pub const MAX_RECORD_BODY_BYTES: u32 = 16 * 1024 * 1024; // 16 MiB

/// Errors from framed record I/O.
#[derive(Debug)]
pub enum FramedRecordError {
    /// A length prefix exceeded the hard cap before allocation.
    Oversized {
        /// Which field was oversized.
        field: &'static str,
        /// Declared length.
        declared: u32,
        /// Hard cap.
        cap: u32,
    },
}

impl std::fmt::Display for FramedRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Oversized {
                field,
                declared,
                cap,
            } => write!(
                f,
                "framed record {field} length {declared} exceeds hard cap {cap}"
            ),
        }
    }
}

impl std::error::Error for FramedRecordError {}

/// One framed sortable record.
///
/// The `key` determines sort order; `payload` is opaque data the caller
/// associates with that key. Both are plain byte vectors — no graph types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FramedRecord {
    /// Sort key (compared lexicographically by raw bytes).
    pub key: Vec<u8>,
    /// Opaque payload carried alongside the key.
    pub payload: Vec<u8>,
}

impl FramedRecord {
    /// Construct from key + payload.
    #[must_use]
    pub fn new(key: impl Into<Vec<u8>>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            payload: payload.into(),
        }
    }

    /// Encoded on-disk length: 8-byte header + key + payload.
    #[must_use]
    pub fn encoded_len(&self) -> u64 {
        8 + self.key.len() as u64 + self.payload.len() as u64
    }

    /// Write this record to `w` using the standard framing.
    ///
    /// # Errors
    /// Returns `io::Error` on write failure or if key/payload exceed `u32::MAX`.
    ///
    /// # Panics
    /// Panics if internal array slicing fails (impossible for a valid `[u8; 4]`).
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let key_len = u32::try_from(self.key.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "key longer than u32::MAX"))?;
        let payload_len = u32::try_from(self.payload.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "payload longer than u32::MAX")
        })?;
        w.write_all(&key_len.to_le_bytes())?;
        w.write_all(&payload_len.to_le_bytes())?;
        w.write_all(&self.key)?;
        w.write_all(&self.payload)?;
        Ok(())
    }

    /// Read the next framed record from `r`.
    ///
    /// - Clean EOF (0 bytes available) → `Ok(None)`.
    /// - Partial header (1–7 bytes then EOF) → `Err(UnexpectedEof)`.
    /// - Oversized length prefix → `Err(InvalidData)`.
    /// - Body truncated after a full header → `Err(UnexpectedEof)` (from `read_exact`).
    ///
    /// # Errors
    /// Returns `io::Error` on read failure, torn header, oversized length,
    /// or truncated body. Clean EOF returns `Ok(None)`.
    ///
    /// # Panics
    /// Panics if `try_into()` on a 4-byte slice fails (impossible for a valid `[u8; 4]`).
    pub fn read_next<R: Read>(r: &mut R) -> io::Result<Option<Self>> {
        let mut hdr = [0u8; 8];
        let mut got = 0usize;
        while got < 8 {
            match r.read(&mut hdr[got..])? {
                0 if got == 0 => return Ok(None), // clean EOF
                0 => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("torn framed header: got {got} of 8 bytes"),
                    ));
                }
                n => got += n,
            }
        }
        let key_len = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        let payload_len = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
        if key_len > MAX_RECORD_BODY_BYTES || payload_len > MAX_RECORD_BODY_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "framed record exceeds hard cap: key_len={key_len} payload_len={payload_len}"
                ),
            ));
        }
        let mut key = vec![0u8; key_len as usize];
        let mut payload = vec![0u8; payload_len as usize];
        r.read_exact(&mut key)?;
        r.read_exact(&mut payload)?;
        Ok(Some(Self { key, payload }))
    }
}

impl PartialOrd for FramedRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FramedRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.payload.cmp(&other.payload))
    }
}
