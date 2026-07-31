//! Streaming generation container writer (G-EM0.W0-A4).
//!
//! Writes a `.grafeo` container from streaming section sources without
//! requiring the caller to materialize each section as a `Vec<u8>`.
//! The [`ExactSectionSource`] trait streams section bytes through
//! [`copy_to`](ExactSectionSource::copy_to), and the writer computes
//! CRC-32 and byte counts on the fly.
//!
//! ## Feature gates
//!
//! - Base types (`ExactSectionSource`, `GenerationFileOps`,
//!   `create_versioned_sections_streaming`): available with `grafeo-file`.
//! - Core adapter (`CompactStoreSectionSource`): requires `generation`
//!   feature (pulls in `grafeo-core`).

use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use grafeo_common::storage::section::{SectionDirectoryEntry, SectionType};
use grafeo_common::utils::error::{Error, Result};

use crate::container::SectionDirectory;
use crate::container::directory::{DIRECTORY_OFFSET, SECTION_DATA_OFFSET};
use crate::file::format::{DbHeader, FileHeader};
use crate::file::header;

// ── Container header ────────────────────────────────────────────────

/// Metadata for a generation container write.
#[derive(Debug, Clone)]
pub struct GenerationContainerHeader {
    /// MVCC epoch.
    pub epoch: u64,
    /// Last committed transaction ID.
    pub transaction_id: u64,
    /// Total node count.
    pub node_count: u64,
    /// Total edge count.
    pub edge_count: u64,
}

// ── Section source trait ────────────────────────────────────────────

/// A section with a known exact byte length, streamed to a writer.
///
/// Implementations must write exactly [`exact_len`](Self::exact_len) bytes
/// in [`copy_to`](Self::copy_to). The streaming writer verifies this and
/// fails closed on mismatch.
pub trait ExactSectionSource {
    /// The container section type.
    fn section_type(&self) -> SectionType;
    /// The directory version byte (section's declared format version).
    fn directory_version(&self) -> u8;
    /// Exact byte length of the section payload.
    fn exact_len(&self) -> u64;
    /// Stream the section payload to `sink`.
    ///
    /// # Errors
    ///
    /// Returns an error if streaming fails.
    fn copy_to(&mut self, sink: &mut dyn Write) -> Result<()>;
}

// ── File operations trait ───────────────────────────────────────────

/// File system operations for generation writes, abstracted for testability.
///
/// W0-B will add a deterministic test implementation for fault injection.
pub trait GenerationFileOps {
    /// Create a new file, failing if it already exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created.
    fn create_new(&self, path: &Path) -> Result<File>;
    /// Fsync a file to durable storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the sync fails.
    fn sync_all(&self, file: &File) -> Result<()>;
    /// Fsync a directory entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the sync fails.
    fn sync_dir(&self, path: &Path) -> Result<()>;
    /// Atomically rename a file.
    ///
    /// # Errors
    ///
    /// Returns an error if the rename fails.
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;
    /// Remove a file.
    ///
    /// # Errors
    ///
    /// Returns an error if the removal fails.
    fn remove(&self, path: &Path) -> Result<()>;
    /// Check if a path exists.
    fn path_exists(&self, path: &Path) -> bool;
}

/// OS-backed file operations.
#[derive(Debug, Clone, Copy, Default)]
pub struct OsGenerationFileOps;

impl GenerationFileOps for OsGenerationFileOps {
    fn create_new(&self, path: &Path) -> Result<File> {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(Error::Io)
    }

    fn sync_all(&self, file: &File) -> Result<()> {
        file.sync_all().map_err(Error::Io)
    }

    fn sync_dir(&self, path: &Path) -> Result<()> {
        let dir = File::open(path).map_err(Error::Io)?;
        dir.sync_all().map_err(Error::Io)
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        fs::rename(from, to).map_err(Error::Io)
    }

    fn remove(&self, path: &Path) -> Result<()> {
        fs::remove_file(path).map_err(Error::Io)
    }

    fn path_exists(&self, path: &Path) -> bool {
        path.exists()
    }
}

// ── CRC + counting writer ───────────────────────────────────────────

/// Writer wrapper that computes CRC-32 and counts bytes on the fly.
struct CrcCountWriter<W: Write> {
    inner: W,
    hasher: crc32fast::Hasher,
    bytes_written: u64,
}

impl<W: Write> CrcCountWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: crc32fast::Hasher::new(),
            bytes_written: 0,
        }
    }

    fn crc(&self) -> u32 {
        self.hasher.clone().finalize()
    }
}

impl<W: Write> Write for CrcCountWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        // reason: write returns usize ≤ buf.len(), always fits u64
        #[allow(clippy::cast_possible_truncation)]
        {
            self.bytes_written += n as u64;
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

// ── Streaming container writer ──────────────────────────────────────

/// Page alignment for section data.
const PAGE_SIZE: u64 = 4096;

/// Writes a `.grafeo` container file from streaming section sources.
///
/// The file is written in production v2 container format:
/// - `FileHeader` at offset 0
/// - Two `DbHeader` slots at 4 KiB and 8 KiB
/// - Section directory at 12 KiB ([`DIRECTORY_OFFSET`])
/// - Section data starting at 16 KiB ([`SECTION_DATA_OFFSET`]), page-aligned
///
/// # Errors
///
/// Fails closed if:
/// - The target path already exists
/// - A section writes fewer or more bytes than declared
/// - Any I/O operation fails
pub fn create_versioned_sections_streaming(
    path: &Path,
    header: &GenerationContainerHeader,
    sections: &mut [Box<dyn ExactSectionSource>],
    file_ops: &dyn GenerationFileOps,
) -> Result<()> {
    // Fail closed: never overwrite an existing file.
    if file_ops.path_exists(path) {
        return Err(Error::Internal(format!(
            "generation target already exists: {}",
            path.display()
        )));
    }

    let mut file = file_ops.create_new(path)?;

    // Write file header at offset 0.
    let file_header = FileHeader::new();
    header::write_file_header(&mut file, &file_header)?;

    // Write empty DbHeaders to both slots.
    header::write_db_header(&mut file, 0, &DbHeader::EMPTY)?;
    header::write_db_header(&mut file, 1, &DbHeader::EMPTY)?;

    // Stream sections at page-aligned offsets.
    let mut current_offset = SECTION_DATA_OFFSET;
    let mut dir = SectionDirectory::new();

    for section in sections.iter_mut() {
        let section_type = section.section_type();
        let version = section.directory_version();
        let exact_len = section.exact_len();

        file.seek(SeekFrom::Start(current_offset))?;

        let crc;
        let written;
        {
            let mut crc_writer = CrcCountWriter::new(&mut file);
            section.copy_to(&mut crc_writer)?;
            crc = crc_writer.crc();
            written = crc_writer.bytes_written;
        }

        if written != exact_len {
            return Err(Error::Internal(format!(
                "section {section_type:?} declared exact_len {exact_len} but wrote {written} bytes"
            )));
        }

        let flags = section_type.default_flags();
        dir.upsert(SectionDirectoryEntry {
            section_type,
            version,
            flags,
            offset: current_offset,
            length: exact_len,
            checksum: crc,
        })?;

        // Advance to next page boundary.
        let section_end = current_offset + exact_len;
        current_offset = (section_end + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE;
    }

    // Truncate to final size.
    file.set_len(current_offset)?;

    // Write section directory.
    let dir_bytes = dir.to_bytes();
    file.seek(SeekFrom::Start(DIRECTORY_OFFSET))?;
    file.write_all(&dir_bytes)?;

    // Build and write DbHeader to slot 0 (first checkpoint).
    // reason: millis since UNIX epoch fits in u64 for ~585 million years
    #[allow(clippy::cast_possible_truncation)]
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let db_header = DbHeader {
        iteration: 1,
        checksum: dir.checksum(),
        snapshot_length: 0, // v2: directory CRC is in checksum field
        epoch: header.epoch,
        transaction_id: header.transaction_id,
        node_count: header.node_count,
        edge_count: header.edge_count,
        timestamp_ms,
    };
    header::write_db_header(&mut file, 0, &db_header)?;

    // Fsync file, then parent directory.
    file_ops.sync_all(&file)?;
    if let Some(parent) = path.parent() {
        file_ops.sync_dir(parent)?;
    }

    Ok(())
}

// ── Core adapter (generation feature) ───────────────────────────────

#[cfg(feature = "generation")]
mod adapter {
    use super::ExactSectionSource;
    use grafeo_common::storage::section::SectionType;
    use grafeo_common::utils::error::{Error, Result};
    use grafeo_core::graph::compact::CompactStore;
    use grafeo_core::graph::compact::generation::{
        CompactV5SegmentSource, GlobalStringDictionary, assemble_v5_payload_from_source,
    };
    use std::io::Write;

    /// Adapter that presents a core `V5SegmentSource` as a container
    /// [`ExactSectionSource`] with `SectionType::CompactStore`.
    ///
    /// Assembles the v5 payload via core's production codecs (no codec
    /// duplication in storage) and streams it through `copy_to`.
    pub struct CompactStoreSectionSource {
        payload: Vec<u8>,
    }

    impl CompactStoreSectionSource {
        /// Assembles the v5 payload from a heap-built `CompactStore`.
        ///
        /// # Errors
        ///
        /// Returns an error if segment emission or payload assembly fails.
        pub fn new(store: &CompactStore, global_strings: &GlobalStringDictionary) -> Result<Self> {
            let mut source = CompactV5SegmentSource::new(store, global_strings)
                .map_err(|e| Error::Internal(format!("V5SegmentSource: {e}")))?;
            let payload = assemble_v5_payload_from_source(
                &mut source,
                store.total_nodes(),
                store.total_edges(),
                store.preserves_ids(),
            )
            .map_err(|e| Error::Internal(format!("v5 assembly: {e}")))?;
            Ok(Self { payload })
        }
    }

    impl ExactSectionSource for CompactStoreSectionSource {
        fn section_type(&self) -> SectionType {
            SectionType::CompactStore
        }

        fn directory_version(&self) -> u8 {
            5 // CompactStore v5 format
        }

        fn exact_len(&self) -> u64 {
            // reason: payload length fits u64 on all targets
            #[allow(clippy::cast_possible_truncation)]
            let len = self.payload.len() as u64;
            len
        }

        fn copy_to(&mut self, sink: &mut dyn Write) -> Result<()> {
            sink.write_all(&self.payload).map_err(Error::Io)
        }
    }
}

#[cfg(feature = "generation")]
pub use adapter::CompactStoreSectionSource;
