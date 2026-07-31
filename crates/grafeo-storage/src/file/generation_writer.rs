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
use std::io::{self, Read, Seek, SeekFrom, Write};
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

    // ── W0-B lifecycle additions (additive only) ─────────────────────
    // These operations are needed by the writable generation lifecycle
    // (publication, recovery, snapshot, fault injection). Existing method
    // signatures are unchanged.

    /// Open an existing file for reading and writing.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened.
    fn open_existing(&self, path: &Path) -> Result<File>;

    /// Read the full contents of a file into memory.
    ///
    /// Used only for small fixed-size control files (manifest slots).
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read.
    fn read_to_end(&self, path: &Path) -> Result<Vec<u8>>;

    /// Create directories recursively.
    ///
    /// # Errors
    ///
    /// Returns an error if directory creation fails.
    fn create_dir_all(&self, path: &Path) -> Result<()>;

    /// List directory entry names (not full paths).
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be read.
    fn read_dir(&self, path: &Path) -> Result<Vec<String>>;

    /// Compute SHA-256 of a file (streaming, bounded memory).
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read.
    fn sha256(&self, path: &Path) -> Result<[u8; 32]>;

    /// Return the byte length of a file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be stat'ed.
    fn file_len(&self, path: &Path) -> Result<u64>;

    /// Path-based fsync (for manifest and directory sync where the caller
    /// does not hold a `File` handle).
    ///
    /// # Errors
    ///
    /// Returns an error if the sync fails.
    fn sync_path(&self, path: &Path) -> Result<()>;

    /// Bounded streaming copy from `src` to `dst`. Returns bytes copied.
    /// Uses a fixed buffer — never loads the whole file into memory.
    /// Fails if `dst` already exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the copy fails.
    fn copy_bounded(&self, src: &Path, dst: &Path, buf_size: usize) -> Result<u64>;
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

    fn open_existing(&self, path: &Path) -> Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(Error::Io)
    }

    fn read_to_end(&self, path: &Path) -> Result<Vec<u8>> {
        fs::read(path).map_err(Error::Io)
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path).map_err(Error::Io)
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(path).map_err(Error::Io)? {
            let entry = entry.map_err(Error::Io)?;
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        Ok(names)
    }

    fn sha256(&self, path: &Path) -> Result<[u8; 32]> {
        use sha2::Digest;
        let mut file = File::open(path).map_err(Error::Io)?;
        let mut hasher = sha2::Sha256::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = file.read(&mut buf).map_err(Error::Io)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        Ok(out)
    }

    fn file_len(&self, path: &Path) -> Result<u64> {
        fs::metadata(path).map_err(Error::Io).map(|m| m.len())
    }

    fn sync_path(&self, path: &Path) -> Result<()> {
        let f = File::open(path).map_err(Error::Io)?;
        f.sync_all().map_err(Error::Io)
    }

    fn copy_bounded(&self, src: &Path, dst: &Path, buf_size: usize) -> Result<u64> {
        let mut reader = File::open(src).map_err(Error::Io)?;
        let mut writer = self.create_new(dst)?;
        let mut buf = vec![0u8; buf_size.max(1)];
        let mut total: u64 = 0;
        loop {
            let n = reader.read(&mut buf).map_err(Error::Io)?;
            if n == 0 {
                break;
            }
            writer.write_all(&buf[..n]).map_err(Error::Io)?;
            // reason: total byte count is bounded by file size, fits in u64
            #[allow(clippy::cast_possible_truncation)]
            {
                total += n as u64;
            }
        }
        Ok(total)
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
        CompactV5SegmentSource, GlobalStringDictionary, V5SegmentSource,
    };
    use std::io::Write;

    /// Adapter that presents a core `V5SegmentSource` as a container
    /// [`ExactSectionSource`] with `SectionType::CompactStore`.
    ///
    /// Streams the v5 payload directly from `CompactV5SegmentSource`
    /// through `copy_to` without materializing the whole payload.
    pub struct CompactStoreSectionSource {
        store: CompactStore,
        global_strings: GlobalStringDictionary,
        total_nodes: u64,
        total_edges: u64,
        preserves_ids: bool,
    }

    impl CompactStoreSectionSource {
        /// Constructs a streaming section source from a heap-built `CompactStore`.
        ///
        /// # Errors
        ///
        /// Returns an error if segment planning fails.
        pub fn new(store: CompactStore, global_strings: GlobalStringDictionary) -> Result<Self> {
            let total_nodes = store.total_nodes();
            let total_edges = store.total_edges();
            let preserves_ids = store.preserves_ids();
            Ok(Self {
                store,
                global_strings,
                total_nodes,
                total_edges,
                preserves_ids,
            })
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
            CompactV5SegmentSource::new(&self.store, &self.global_strings)
                .map_or(0, |s| s.payload_len())
        }

        fn copy_to(&mut self, sink: &mut dyn Write) -> Result<()> {
            let mut source = CompactV5SegmentSource::new(&self.store, &self.global_strings)
                .map_err(|e| Error::Internal(format!("V5SegmentSource: {e}")))?;
            let plan = source.segment_plan().to_vec();
            let segment_count = plan.len();
            let segment_count_u16 = u16::try_from(segment_count)
                .map_err(|_| Error::Internal(format!("too many segments: {segment_count}")))?;

            let directory_length = u64::from(segment_count_u16) * 48; // DIRECTORY_ENTRY_LEN
            let data_offset = align_up(64 + directory_length, 8); // HEADER_LEN = 64

            // reason: directory_length fits usize on all supported targets
            #[allow(clippy::cast_possible_truncation)]
            let dir_cap = directory_length as usize;
            let mut dir_bytes = Vec::with_capacity(dir_cap);
            let mut cursor = data_offset;
            let mut entries_meta = Vec::with_capacity(segment_count);

            for entry in &plan {
                let align = u64::from(entry.alignment);
                let offset = align_up(cursor, align);
                entries_meta.push((
                    entry.kind,
                    entry.encoding_version,
                    entry.flags,
                    entry.alignment,
                    offset,
                    entry.length,
                    entry.element_width,
                    entry.element_count,
                    entry.crc,
                ));
                cursor = offset + entry.length;
            }

            for meta in &entries_meta {
                let (kind, enc_ver, flags, alignment, offset, length, el_width, el_count, crc) =
                    *meta;
                write_u16(&mut dir_bytes, kind.as_u16());
                write_u16(&mut dir_bytes, enc_ver);
                write_u16(&mut dir_bytes, flags);
                write_u16(&mut dir_bytes, alignment);
                write_u64(&mut dir_bytes, offset);
                write_u64(&mut dir_bytes, length);
                write_u32(&mut dir_bytes, el_width);
                write_u32(&mut dir_bytes, el_count);
                write_u32(&mut dir_bytes, crc);
                write_u32(&mut dir_bytes, 0); // reserved_a
                write_u64(&mut dir_bytes, 0); // reserved_b
            }

            let directory_crc = crc32fast::hash(&dir_bytes);
            let flags: u8 = u8::from(self.preserves_ids);

            let mut header = Vec::with_capacity(64);
            header.extend_from_slice(b"GCST");
            header.push(5); // FORMAT_VERSION_V5
            header.push(flags);
            write_u16(&mut header, 64); // HEADER_LEN
            write_u16(&mut header, segment_count_u16);
            write_u16(&mut header, 48); // DIRECTORY_ENTRY_LEN
            write_u32(&mut header, 0); // layout_flags
            write_u64(&mut header, 64); // directory_offset
            write_u64(&mut header, directory_length);
            write_u64(&mut header, data_offset);
            write_u64(&mut header, self.total_nodes);
            write_u64(&mut header, self.total_edges);
            write_u32(&mut header, directory_crc);
            write_u32(&mut header, 0); // reserved
            debug_assert_eq!(header.len(), 64);

            let mut hasher = crc32fast::Hasher::new();

            sink.write_all(&header).map_err(Error::Io)?;
            hasher.update(&header);

            sink.write_all(&dir_bytes).map_err(Error::Io)?;
            hasher.update(&dir_bytes);

            let mut written_so_far = 64 + directory_length;
            while written_so_far < data_offset {
                // reason: padding delta fits usize
                #[allow(clippy::cast_possible_truncation)]
                let pad_len = (data_offset - written_so_far) as usize;
                let zeros = vec![0u8; pad_len];
                sink.write_all(&zeros).map_err(Error::Io)?;
                hasher.update(&zeros);
                written_so_far = data_offset;
            }

            let mut current_offset = data_offset;
            while let Some(seg) = source
                .next_segment()
                .map_err(|e| Error::Internal(format!("next_segment: {e}")))?
            {
                let target_offset = align_up(current_offset, u64::from(seg.alignment));
                if target_offset > current_offset {
                    // reason: alignment padding fits usize
                    #[allow(clippy::cast_possible_truncation)]
                    let pad = (target_offset - current_offset) as usize;
                    let zeros = vec![0u8; pad];
                    sink.write_all(&zeros).map_err(Error::Io)?;
                    hasher.update(&zeros);
                    current_offset = target_offset;
                }
                sink.write_all(&seg.bytes).map_err(Error::Io)?;
                hasher.update(&seg.bytes);
                current_offset += seg.bytes.len() as u64;
            }

            let crc = hasher.finalize();
            sink.write_all(&crc.to_le_bytes()).map_err(Error::Io)?;

            Ok(())
        }
    }

    fn align_up(val: u64, align: u64) -> u64 {
        if align == 0 {
            return val;
        }
        (val + align - 1) & !(align - 1)
    }

    fn write_u16(buf: &mut Vec<u8>, v: u16) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_u32(buf: &mut Vec<u8>, v: u32) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_u64(buf: &mut Vec<u8>, v: u64) {
        buf.extend_from_slice(&v.to_le_bytes());
    }
}

#[cfg(feature = "generation")]
pub use adapter::CompactStoreSectionSource;
