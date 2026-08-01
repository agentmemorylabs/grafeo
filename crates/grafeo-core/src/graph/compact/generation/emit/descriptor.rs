//! Segment descriptor and body storage for bounded v5 emission (G-EM0.5b Phase 0).
//!
//! A [`SegmentDescriptor`] carries cheap metadata (kind, lengths, CRC) plus a
//! [`SegmentBody`] that is either resident in memory or spilled to a temp file.
//! The assembler reads bodies through [`SegmentBody::stream`] without requiring
//! the whole segment resident.

use crate::graph::compact::generation::error::GenerationError;
use crate::graph::compact::mapped::SegmentKind;
use bytes::Bytes;
use std::io::Read;
use std::path::PathBuf;

/// Where a finished segment's bytes live.
#[derive(Debug, Clone)]
pub enum SegmentBody {
    /// Small segment held in anonymous memory.
    Resident(Bytes),
    /// Large segment spilled to a correlation-scoped temp file.
    Spilled(PathBuf),
}

impl SegmentBody {
    /// Total byte length of the body.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::Io`] when a spilled file cannot be stat'd.
    pub fn len(&self) -> Result<u64, GenerationError> {
        match self {
            Self::Resident(b) => Ok(b.len() as u64),
            Self::Spilled(p) => {
                let meta = std::fs::metadata(p)
                    .map_err(|e| GenerationError::Io(format!("stat {}: {e}", p.display())))?;
                Ok(meta.len())
            }
        }
    }

    /// Returns `true` when the body carries zero bytes.
    ///
    /// # Errors
    ///
    /// Propagates [`Self::len`] errors.
    pub fn is_empty(&self) -> Result<bool, GenerationError> {
        Ok(self.len()? == 0)
    }

    /// Stream body bytes through `f` in bounded chunks.
    ///
    /// Resident bodies are passed in one call. Spilled bodies are read
    /// through a 64 KiB buffer so anonymous peak stays O(buffer), not
    /// O(segment bytes).
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::Io`] on file read failure, or propagates
    /// errors from `f`.
    pub fn stream(
        &self,
        f: &mut dyn FnMut(&[u8]) -> Result<(), GenerationError>,
    ) -> Result<(), GenerationError> {
        match self {
            Self::Resident(b) => f(b),
            Self::Spilled(p) => {
                let file = std::fs::File::open(p)
                    .map_err(|e| GenerationError::Io(format!("open {}: {e}", p.display())))?;
                let mut reader = std::io::BufReader::with_capacity(64 * 1024, file);
                let mut buf = vec![0u8; 64 * 1024];
                loop {
                    let n = reader
                        .read(&mut buf)
                        .map_err(|e| GenerationError::Io(format!("read {}: {e}", p.display())))?;
                    if n == 0 {
                        break;
                    }
                    f(&buf[..n])?;
                }
                Ok(())
            }
        }
    }
}

impl Drop for SegmentBody {
    fn drop(&mut self) {
        if let Self::Spilled(path) = self {
            // Best-effort cleanup: delete the spool file. Ignore errors
            // (file may already be gone, or path may be on a tmpfs that
            // was cleaned). This is the RAII cleanup for spilled segment
            // bodies — the spool file is deleted when the descriptor is
            // dropped, whether on success (after assembly) or failure
            // (unwind, error return, or panic).
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Cheap metadata plus a readable body for one finished v5 segment.
///
/// Produced by [`SegmentSink::finish`]. The assembler consumes ordered
/// descriptors to build the final payload.
#[derive(Debug, Clone)]
pub struct SegmentDescriptor {
    /// Segment kind code (ascending emission order).
    pub kind: SegmentKind,
    /// Per-kind encoding version (usually 1).
    pub encoding_version: u16,
    /// Flags (bit 0 = required).
    pub flags: u16,
    /// Required alignment in bytes (1, 4, 8).
    pub alignment: u16,
    /// Fixed element width, or 0 for variable length.
    pub element_width: u32,
    /// Total body byte length.
    pub length: u64,
    /// CRC-32 of the body bytes.
    pub crc: u32,
    /// Element count for the directory entry.
    pub element_count: u32,
    /// Where the body bytes live.
    pub body: SegmentBody,
}
