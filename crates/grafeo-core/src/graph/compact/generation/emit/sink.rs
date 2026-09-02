//! Bounded segment sinks for v5 emission (G-EM0.5b Phase 0).
//!
//! One canonical [`SegmentSink`] trait; two implementations:
//! - [`SpoolSegmentSink`] — disk-backed; the only sink the writable generation
//!   path may use. Buffers up to a small in-memory cap, then spills to a
//!   correlation-scoped temp file under the caller-supplied `temp_dir`.
//! - [`MemorySegmentSink`] — bounded-chunk in-memory compatibility sink for
//!   narrow legacy/test callers only. `serialize_v5` will delegate through it
//!   in Phase 1 so its public signature and byte output are preserved.
//!
//! Core does plain `std::fs` I/O into the caller-supplied temp dir; core never
//! imports storage (dependency direction preserved, packet D0.3).

use super::descriptor::{SegmentBody, SegmentDescriptor};
use crate::graph::compact::generation::error::GenerationError;
use crate::graph::compact::mapped::SegmentKind;
use bytes::Bytes;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Bounded byte writer for one v5 segment.
///
/// Receives a segment's bytes incrementally; computes `length`, incremental
/// CRC-32, and `element_count` on the fly; never requires the whole segment
/// resident. [`finish`](SegmentSink::finish) yields a [`SegmentDescriptor`].
pub trait SegmentSink {
    /// Append bytes to the segment body.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::Io`] on spool write failure.
    fn write(&mut self, bytes: &[u8]) -> Result<(), GenerationError>;

    /// Finish writing and produce the descriptor with body.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::Io`] on spool flush failure.
    fn finish(self: Box<Self>) -> Result<SegmentDescriptor, GenerationError>;
}

// ── SpoolSegmentSink ─────────────────────────────────────────────

/// Disk-backed segment sink. Buffers up to `buf_cap` bytes in memory, then
/// spills to a temp file under `temp_dir`. This is the only sink the writable
/// generation path may use; it makes the build's anonymous peak O(budget),
/// not O(total bytes).
pub struct SpoolSegmentSink {
    kind: SegmentKind,
    encoding_version: u16,
    flags: u16,
    alignment: u16,
    element_width: u32,
    temp_dir: PathBuf,
    file_id: String,
    buf: Vec<u8>,
    buf_cap: usize,
    file: Option<std::io::BufWriter<std::fs::File>>,
    path: Option<PathBuf>,
    length: u64,
    crc: crc32fast::Hasher,
}

impl SpoolSegmentSink {
    /// Creates a spool sink that spills to `temp_dir` once the in-memory
    /// buffer exceeds `buf_cap` bytes.
    ///
    /// `file_id` is a correlation-scoped identifier used to name the temp
    /// file (e.g. `"seg-08-0001"`).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kind: SegmentKind,
        encoding_version: u16,
        flags: u16,
        alignment: u16,
        element_width: u32,
        temp_dir: &Path,
        file_id: impl Into<String>,
        buf_cap: usize,
    ) -> Self {
        Self {
            kind,
            encoding_version,
            flags,
            alignment,
            element_width,
            temp_dir: temp_dir.to_path_buf(),
            file_id: file_id.into(),
            buf: Vec::with_capacity(buf_cap.min(64 * 1024)),
            buf_cap: buf_cap.max(1),
            file: None,
            path: None,
            length: 0,
            crc: crc32fast::Hasher::new(),
        }
    }

    fn ensure_file(&mut self) -> Result<(), GenerationError> {
        if self.file.is_some() {
            return Ok(());
        }
        let path = self.temp_dir.join(format!("{}.spool", self.file_id));
        let file = std::fs::File::create(&path)
            .map_err(|e| GenerationError::Io(format!("create {}: {e}", path.display())))?;
        // Flush any buffered bytes to the new file.
        let mut writer = std::io::BufWriter::with_capacity(64 * 1024, file);
        if !self.buf.is_empty() {
            writer
                .write_all(&self.buf)
                .map_err(|e| GenerationError::Io(format!("write {}: {e}", path.display())))?;
            self.buf.clear();
        }
        self.file = Some(writer);
        self.path = Some(path);
        Ok(())
    }
}

impl SegmentSink for SpoolSegmentSink {
    fn write(&mut self, bytes: &[u8]) -> Result<(), GenerationError> {
        self.crc.update(bytes);
        self.length += bytes.len() as u64;
        if let Some(writer) = self.file.as_mut() {
            writer
                .write_all(bytes)
                .map_err(|e| GenerationError::Io(e.to_string()))?;
        } else if self.buf.len() + bytes.len() > self.buf_cap {
            self.ensure_file()?;
            let writer = self.file.as_mut().expect("file just created");
            writer
                .write_all(bytes)
                .map_err(|e| GenerationError::Io(e.to_string()))?;
        } else {
            self.buf.extend_from_slice(bytes);
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<SegmentDescriptor, GenerationError> {
        let body = if let Some(mut writer) = self.file.take() {
            writer
                .flush()
                .map_err(|e| GenerationError::Io(format!("flush spool: {e}")))?;
            let path = self.path.take().expect("path set with file");
            SegmentBody::Spilled(path)
        } else {
            SegmentBody::Resident(Bytes::from(std::mem::take(&mut self.buf)))
        };
        // Take the hasher out via replace since Drop now exists on this type.
        let crc = std::mem::replace(&mut self.crc, crc32fast::Hasher::new()).finalize();
        let length = self.length;
        let element_count = if self.element_width > 0 {
            #[allow(clippy::cast_possible_truncation)]
            let count = (length / u64::from(self.element_width)) as u32;
            count
        } else {
            0
        };
        Ok(SegmentDescriptor {
            kind: self.kind,
            encoding_version: self.encoding_version,
            flags: self.flags,
            alignment: self.alignment,
            element_width: self.element_width,
            length,
            crc,
            element_count,
            body,
        })
    }
}

impl Drop for SpoolSegmentSink {
    fn drop(&mut self) {
        // RAII cleanup: if the sink was spilled but never finished (error,
        // cancel, panic), delete the temp file. If finish() was called,
        // the file was transferred to SegmentBody::Spilled and the sink's
        // file/path are None, so this is a no-op.
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(&path);
        }
    }
}

// ── MemorySegmentSink ────────────────────────────────────────────

/// In-memory compatibility sink for narrow legacy/test callers.
///
/// `serialize_v5` delegates through this in Phase 1 so its public signature
/// and byte output are preserved. The writable generation path must NOT
/// construct this sink (tripwire in Phase 1, packet §9 / D0.5).
pub struct MemorySegmentSink {
    kind: SegmentKind,
    encoding_version: u16,
    flags: u16,
    alignment: u16,
    element_width: u32,
    bytes: Vec<u8>,
    crc: crc32fast::Hasher,
}

impl MemorySegmentSink {
    /// Creates an in-memory sink for one segment.
    #[must_use]
    pub fn new(
        kind: SegmentKind,
        encoding_version: u16,
        flags: u16,
        alignment: u16,
        element_width: u32,
    ) -> Self {
        Self {
            kind,
            encoding_version,
            flags,
            alignment,
            element_width,
            bytes: Vec::new(),
            crc: crc32fast::Hasher::new(),
        }
    }
}

impl SegmentSink for MemorySegmentSink {
    fn write(&mut self, bytes: &[u8]) -> Result<(), GenerationError> {
        self.crc.update(bytes);
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<SegmentDescriptor, GenerationError> {
        let length = self.bytes.len() as u64;
        let crc = self.crc.finalize();
        let element_count = if self.element_width > 0 {
            #[allow(clippy::cast_possible_truncation)]
            let count = (length / u64::from(self.element_width)) as u32;
            count
        } else {
            0
        };
        Ok(SegmentDescriptor {
            kind: self.kind,
            encoding_version: self.encoding_version,
            flags: self.flags,
            alignment: self.alignment,
            element_width: self.element_width,
            length,
            crc,
            element_count,
            body: SegmentBody::Resident(Bytes::from(self.bytes)),
        })
    }
}
