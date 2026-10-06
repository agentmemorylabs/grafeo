//! High-level manager for `.grafeo` database files.
//!
//! [`GrafeoFileManager`] owns the file handle and provides create, open,
//! snapshot write/read, and sidecar WAL lifecycle management.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use grafeo_common::utils::error::{Error, Result, StorageError};
use parking_lot::Mutex;

use super::format::{DATA_OFFSET, DbHeader, FileHeader};
use super::header;

/// Suffix of the staging file a checkpoint image is built in before it
/// atomically replaces the database file (`mydb.grafeo.checkpoint-tmp`).
pub const CHECKPOINT_TMP_SUFFIX: &str = ".checkpoint-tmp";

/// Manages a single `.grafeo` database file.
///
/// # Lifecycle
///
/// 1. [`create`](Self::create) or [`open`](Self::open)
/// 2. Mutations flow through a sidecar WAL (managed externally by the engine)
/// 3. [`write_snapshot`](Self::write_snapshot) checkpoints memory to the file
/// 4. After a successful checkpoint, call [`remove_sidecar_wal`](Self::remove_sidecar_wal)
/// 5. [`close`](Self::close) (or drop) releases the file handle
///
/// # Crash safety
///
/// A checkpoint never modifies the published file. The complete new image
/// (file header, both DB header slots, payload) is built in a staging file
/// next to it, fsynced, renamed over the database file, and the directory is
/// fsynced. A crash at any point leaves either the old image or the new one
/// at the database path. A leftover staging file is deleted on the next
/// writable [`open`](Self::open).
pub struct GrafeoFileManager {
    /// Path to the `.grafeo` file.
    path: PathBuf,
    /// Open file handle (read/write or read-only).
    file: Mutex<File>,
    /// File header (read once on open, immutable afterwards).
    file_header: FileHeader,
    /// Currently active database header.
    active_header: Mutex<DbHeader>,
    /// Slot index (0 or 1) of the active header.
    active_slot: Mutex<u8>,
    /// Whether this manager was opened in read-only mode.
    read_only: bool,
    /// Encryptor for section data (None = unencrypted).
    #[cfg(feature = "encryption")]
    section_encryptor: Option<grafeo_common::encryption::PageEncryptor>,
}

impl GrafeoFileManager {
    /// Creates a new `.grafeo` file at `path`.
    ///
    /// Writes the file header and two empty database headers. The file must
    /// not already exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the file already exists or cannot be created.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        if path.exists() {
            return Err(Error::Internal(format!(
                "file already exists: {}",
                path.display()
            )));
        }

        // Ensure parent directory exists
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists || e.raw_os_error() == Some(183) {
                    Error::Io(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!(
                            "database file already exists (may be open by another process): {}",
                            path.display()
                        ),
                    ))
                } else {
                    Error::Io(e)
                }
            })?;

        // Acquire an exclusive lock: prevents other processes from opening the same file
        file.try_lock_exclusive().map_err(|_| {
            Error::Internal(format!(
                "database file is locked by another process: {}",
                path.display()
            ))
        })?;

        let file_header = FileHeader::new();
        header::write_file_header(&mut file, &file_header)?;
        header::write_db_header(&mut file, 0, &DbHeader::EMPTY)?;
        header::write_db_header(&mut file, 1, &DbHeader::EMPTY)?;
        file.sync_all()?;

        Ok(Self {
            path,
            file: Mutex::new(file),
            file_header,
            active_header: Mutex::new(DbHeader::EMPTY),
            active_slot: Mutex::new(0),
            read_only: false,
            #[cfg(feature = "encryption")]
            section_encryptor: None,
        })
    }

    /// Opens an existing `.grafeo` file.
    ///
    /// Validates the magic bytes and format version, then selects the
    /// active database header.
    ///
    /// # Errors
    ///
    /// Returns an error if the file does not exist, is locked by another
    /// process, keeps being replaced by a concurrent checkpoint while it is
    /// being locked, has invalid magic, or an unsupported format version.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Exclusive lock: prevents other processes from opening the same
        // file. The lock is verified to be on the file currently at `path`.
        let mut file = open_locked(&path, LockKind::Exclusive)?;

        let file_header = header::read_file_header(&mut file)?;
        header::validate_file_header(&file_header)?;

        let (h0, h1) = header::read_db_headers(&mut file)?;
        let (active_slot, active_header) = header::active_db_header(&h0, &h1);

        // A staging file left by a checkpoint that died before its rename is
        // garbage: the published file is the old, consistent image. We hold
        // a verified exclusive lock on the published file, so no other
        // writer can be mid-checkpoint.
        remove_if_exists(&checkpoint_tmp_path(&publish_target(&path)))?;

        Ok(Self {
            path,
            file: Mutex::new(file),
            file_header,
            active_header: Mutex::new(active_header),
            active_slot: Mutex::new(active_slot),
            read_only: false,
            #[cfg(feature = "encryption")]
            section_encryptor: None,
        })
    }

    /// Opens an existing `.grafeo` file in read-only mode.
    ///
    /// Uses a **shared** file lock (`try_lock_shared`), allowing multiple
    /// readers to open the same file concurrently, even while a writer holds
    /// an exclusive lock (on platforms with advisory locking).
    ///
    /// The returned manager only supports [`read_snapshot`](Self::read_snapshot)
    /// and other read-only operations. Calling [`write_snapshot`](Self::write_snapshot)
    /// will return an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the file does not exist, cannot be locked for
    /// reading, keeps being replaced by a concurrent checkpoint while it is
    /// being locked, has invalid magic, or an unsupported format version.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Shared lock: coexists with other shared locks but blocks if an
        // exclusive lock cannot be shared (platform-dependent). The lock is
        // verified to be on the file currently at `path`.
        let mut file = open_locked(&path, LockKind::Shared)?;

        let file_header = header::read_file_header(&mut file)?;
        header::validate_file_header(&file_header)?;

        let (h0, h1) = header::read_db_headers(&mut file)?;
        let (active_slot, active_header) = header::active_db_header(&h0, &h1);

        Ok(Self {
            path,
            file: Mutex::new(file),
            file_header,
            active_header: Mutex::new(active_header),
            active_slot: Mutex::new(active_slot),
            read_only: true,
            #[cfg(feature = "encryption")]
            section_encryptor: None,
        })
    }

    /// Sets the encryptor for section-level encryption.
    ///
    /// When set, all section data is encrypted on write and decrypted on read.
    /// The GCM authentication tag provides integrity verification, replacing
    /// the CRC-32 checksum for encrypted sections.
    #[cfg(feature = "encryption")]
    pub fn set_section_encryptor(&mut self, encryptor: grafeo_common::encryption::PageEncryptor) {
        self.section_encryptor = Some(encryptor);
    }

    /// Returns `true` if this manager was opened in read-only mode.
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Writes a v1 snapshot blob as a new image of the database file.
    ///
    /// Steps:
    /// 1. Write `data` at [`DATA_OFFSET`] of a staging file
    /// 2. Compute CRC-32 checksum
    /// 3. Build a new [`DbHeader`] for the next iteration
    /// 4. Publish the staging file atomically (see [`Self::publish_image`])
    /// 5. Update internal active header/slot state
    ///
    /// # Errors
    ///
    /// Returns an error if any I/O operation fails. The published file is
    /// left untouched unless the rename already happened.
    pub fn write_snapshot(
        &self,
        data: &[u8],
        epoch: u64,
        transaction_id: u64,
        node_count: u64,
        edge_count: u64,
    ) -> Result<()> {
        if self.read_only {
            return Err(Error::Internal(
                "cannot write snapshot: database is open in read-only mode".to_string(),
            ));
        }

        use grafeo_common::testing::crash::maybe_crash;

        let checksum = crc32fast::hash(data);
        // reason: millis since UNIX epoch fits in u64 for ~585 million years
        #[allow(clippy::cast_possible_truncation)]
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut file = self.file.lock();
        let mut active_header = self.active_header.lock();
        let mut active_slot = self.active_slot.lock();

        let new_header = DbHeader {
            iteration: active_header.iteration + 1,
            checksum,
            snapshot_length: data.len() as u64,
            epoch,
            transaction_id,
            node_count,
            edge_count,
            timestamp_ms,
        };

        self.publish_image(&mut file, &mut active_header, &mut active_slot, |tmp| {
            maybe_crash("write_snapshot:before_data_write");

            tmp.seek(SeekFrom::Start(DATA_OFFSET))?;
            tmp.write_all(data)?;

            maybe_crash("write_snapshot:after_data_write");

            tmp.set_len(DATA_OFFSET + data.len() as u64)?;

            maybe_crash("write_snapshot:after_truncate");

            Ok(new_header)
        })?;

        maybe_crash("write_snapshot:after_fsync");

        Ok(())
    }

    /// Builds a complete new image of the database file in a staging file and
    /// atomically publishes it over the database file.
    ///
    /// `write_body` writes the payload (everything past the DB header slots)
    /// into the staging file and returns the [`DbHeader`] describing it. This
    /// function writes the file header and both header slots around it, then:
    ///
    /// 1. `fsync` the staging file (payload and headers durable)
    /// 2. `rename` it over the database file (atomic replace)
    /// 3. `fsync` the parent directory (rename durable; Unix only)
    ///
    /// The published file is never written in place, so a crash at any point
    /// leaves the old image or the new one at the database path. On success,
    /// and on a failed directory fsync after the rename, the manager switches
    /// to the new image and `active_header`/`active_slot` are updated. Any
    /// error is returned so callers keep the WAL that still covers the data.
    fn publish_image(
        &self,
        file: &mut File,
        active_header: &mut DbHeader,
        active_slot: &mut u8,
        write_body: impl FnOnce(&mut File) -> Result<DbHeader>,
    ) -> Result<()> {
        use grafeo_common::testing::crash::maybe_crash;

        let target = publish_target(&self.path);
        let tmp_path = checkpoint_tmp_path(&target);
        let target_slot = u8::from(*active_slot == 0);

        remove_if_exists(&tmp_path)?;
        let mut tmp = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&tmp_path)?;

        let staged = (|| -> Result<DbHeader> {
            // Lock before the image becomes visible under the database path,
            // so the new image is locked from the moment it is published.
            // The old inode's lock is released when its handle is dropped
            // after the rename; an opener that opened the old inode before
            // the rename and locks it afterwards is caught by the identity
            // check in `open_locked` and retries on the new image.
            tmp.try_lock_exclusive().map_err(|e| {
                Error::Internal(format!(
                    "cannot lock checkpoint staging file {}: {e}",
                    tmp_path.display()
                ))
            })?;
            tmp.set_permissions(file.metadata()?.permissions())?;

            header::write_file_header(&mut tmp, &self.file_header)?;
            let new_header = write_body(&mut tmp)?;
            header::write_db_header(&mut tmp, target_slot, &new_header)?;
            header::write_db_header(&mut tmp, 1 - target_slot, &DbHeader::EMPTY)?;
            tmp.sync_all()?;
            Ok(new_header)
        })();
        let new_header = match staged {
            Ok(h) => h,
            Err(e) => {
                drop(tmp);
                let _ = fs::remove_file(&tmp_path);
                return Err(e);
            }
        };

        maybe_crash("checkpoint:before_rename");

        // Windows cannot replace a file that still has an open handle, so
        // close the old one first. The staging file stays locked throughout.
        #[cfg(windows)]
        drop(std::mem::replace(file, tmp));

        if let Err(e) = fs::rename(&tmp_path, &target) {
            // The old image is still the published one.
            #[cfg(windows)]
            {
                let old = OpenOptions::new().read(true).write(true).open(&target)?;
                old.try_lock_exclusive().map_err(|_| {
                    Error::Internal(format!(
                        "database file is locked by another process: {}",
                        target.display()
                    ))
                })?;
                *file = old;
            }
            let _ = fs::remove_file(&tmp_path);
            return Err(e.into());
        }

        // On Unix keep the old handle (and its lock) until the new image is
        // published, then drop it; the old inode is freed once unmapped.
        #[cfg(not(windows))]
        drop(std::mem::replace(file, tmp));
        *active_header = new_header;
        *active_slot = target_slot;

        maybe_crash("checkpoint:after_rename");

        sync_parent_dir(&target)
    }

    /// Reads snapshot data from the file using the active database header.
    ///
    /// Returns an empty `Vec` if the database has never been checkpointed
    /// (both headers are empty).
    ///
    /// # Errors
    ///
    /// Returns an error if the read fails or the CRC checksum does not match.
    pub fn read_snapshot(&self) -> Result<Vec<u8>> {
        let active_header = self.active_header.lock();

        if active_header.is_empty() {
            return Ok(Vec::new());
        }

        // v2 files store sections rather than a v1 snapshot blob. They set
        // snapshot_length == 0 and put the directory CRC in the checksum field.
        // Reading 0 bytes here would CRC to 0 and mismatch the directory CRC.
        if active_header.snapshot_length == 0 {
            return Ok(Vec::new());
        }

        // reason: snapshot_length is the size of serialized in-memory data, fits in usize on 64-bit targets;
        // on 32-bit targets the database would OOM long before reaching 4 GiB
        // reason: value bounded by collection size, fits usize
        #[allow(clippy::cast_possible_truncation)]
        let length = active_header.snapshot_length as usize;
        let expected_checksum = active_header.checksum;
        drop(active_header);

        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(DATA_OFFSET))?;

        let mut data = vec![0u8; length];
        std::io::Read::read_exact(&mut *file, &mut data)?;

        // Verify CRC
        let actual_checksum = crc32fast::hash(&data);
        if actual_checksum != expected_checksum {
            return Err(Error::Internal(format!(
                "snapshot checksum mismatch: expected {expected_checksum:#010X}, got {actual_checksum:#010X}"
            )));
        }

        Ok(data)
    }

    /// Returns the path for the sidecar WAL directory.
    ///
    /// For a database at `mydb.grafeo`, the sidecar is `mydb.grafeo.wal/`.
    #[must_use]
    pub fn sidecar_wal_path(&self) -> PathBuf {
        let mut wal_path = self.path.as_os_str().to_owned();
        wal_path.push(".wal");
        PathBuf::from(wal_path)
    }

    /// Returns `true` if a sidecar WAL directory exists.
    #[must_use]
    pub fn has_sidecar_wal(&self) -> bool {
        self.sidecar_wal_path().exists()
    }

    /// Removes the sidecar WAL directory after a successful checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory exists but cannot be removed.
    pub fn remove_sidecar_wal(&self) -> Result<()> {
        let wal_path = self.sidecar_wal_path();
        if wal_path.exists() {
            fs::remove_dir_all(&wal_path)?;
        }
        Ok(())
    }

    /// Returns the file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns a clone of the currently active database header.
    #[must_use]
    pub fn active_header(&self) -> DbHeader {
        self.active_header.lock().clone()
    }

    /// Returns the file header (written at creation, immutable).
    #[must_use]
    pub fn file_header(&self) -> &FileHeader {
        &self.file_header
    }

    /// Returns the total file size on disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the file metadata cannot be read.
    pub fn file_size(&self) -> Result<u64> {
        let file = self.file.lock();
        let metadata = file.metadata()?;
        Ok(metadata.len())
    }

    /// Flushes and syncs the file.
    ///
    /// # Errors
    ///
    /// Returns an error if sync fails.
    pub fn sync(&self) -> Result<()> {
        if !self.read_only {
            let file = self.file.lock();
            file.sync_all()?;
        }
        Ok(())
    }

    // ── Section-based I/O (v2 container format) ─────────────────────

    /// Writes multiple sections with directory version fixed to `1` for each.
    ///
    /// Prefer [`Self::write_versioned_sections`] when the caller knows each
    /// section's declared format version (engine flush path). This legacy
    /// entry point remains for tests and callers that only have opaque bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if write or sync fails.
    pub fn write_sections(
        &self,
        sections: &[(grafeo_common::storage::SectionType, &[u8])],
        epoch: u64,
        transaction_id: u64,
        node_count: u64,
        edge_count: u64,
    ) -> Result<()> {
        let versioned: Vec<(grafeo_common::storage::SectionType, u8, &[u8])> = sections
            .iter()
            .map(|(section_type, data)| (*section_type, 1u8, *data))
            .collect();
        self.write_versioned_sections(&versioned, epoch, transaction_id, node_count, edge_count)
    }

    /// Writes multiple sections to the file using the v2 container format.
    ///
    /// Each tuple is `(section_type, directory_version, payload)`. The directory
    /// entry records the supplied `directory_version` (the section's declared
    /// format version) rather than a hard-coded `1`.
    ///
    /// Each section is written at a page-aligned offset and a section
    /// directory at `DIRECTORY_OFFSET` of a staging file, which then
    /// atomically replaces the database file (see [`Self::publish_image`]).
    /// The image holds exactly the sections passed in.
    ///
    /// # Errors
    ///
    /// Returns an error if write or sync fails. The published file is left
    /// untouched unless the rename already happened.
    pub fn write_versioned_sections(
        &self,
        sections: &[(grafeo_common::storage::SectionType, u8, &[u8])],
        epoch: u64,
        transaction_id: u64,
        node_count: u64,
        edge_count: u64,
    ) -> Result<()> {
        use crate::container::SectionDirectory;
        use crate::container::directory::{DIRECTORY_OFFSET, SECTION_DATA_OFFSET};
        use grafeo_common::storage::SectionDirectoryEntry;
        use grafeo_common::testing::crash::maybe_crash;

        if self.read_only {
            return Err(Error::Internal(
                "cannot write sections: database is open in read-only mode".to_string(),
            ));
        }

        let mut file = self.file.lock();
        let mut active_header = self.active_header.lock();
        let mut active_slot = self.active_slot.lock();

        let new_iteration = active_header.iteration + 1;
        // Next checkpoint iteration, used as the high part of the nonce so that
        // the same (section_type, offset) pair produces a different nonce across
        // checkpoints. Without this, identical section layouts would reuse nonces.
        #[cfg(feature = "encryption")]
        // reason: iteration wraps at u32::MAX which takes billions of checkpoints (~100+ years at 1/s)
        #[allow(clippy::cast_possible_truncation)]
        let nonce_iteration = new_iteration as u32;

        self.publish_image(&mut file, &mut active_header, &mut active_slot, |tmp| {
            let mut dir = SectionDirectory::new();

            maybe_crash("write_sections:before_data");

            // Write each section at page-aligned offsets
            let page_size = 4096u64;
            let mut current_offset = SECTION_DATA_OFFSET;

            for (section_type, version, data) in sections {
                // Encrypt section data if encryption is enabled.
                // Nonce high word: iteration in bits [31:8], section type in bits [7:0].
                // Bit-packing (not XOR) ensures unique high words: XOR is commutative
                // so `iter ^ type` can collide across different (iter, type) pairs,
                // but packing into disjoint bit lanes is injective for type < 256.
                // Nonce low word: page-aligned write offset (unique within a checkpoint).
                // AAD binds the ciphertext to the section type, preventing relocation.
                // Encrypt section data if an encryptor is configured, otherwise
                // write the plaintext bytes directly (no allocation).
                #[cfg(feature = "encryption")]
                let encrypted_buf: Option<Vec<u8>> = if let Some(ref enc) = self.section_encryptor {
                    let nonce_high = (nonce_iteration << 8) | (*section_type as u32 & 0xFF);
                    let nonce = grafeo_common::encryption::build_nonce(nonce_high, current_offset);
                    let aad = format!("grafeo-section:{}", *section_type as u32);
                    Some(
                        enc.encrypt(data, &nonce, aad.as_bytes()).map_err(|e| {
                            Error::Internal(format!("section encryption failed: {e}"))
                        })?,
                    )
                } else {
                    None
                };

                #[cfg(feature = "encryption")]
                let write_data: &[u8] = encrypted_buf.as_deref().unwrap_or(data);
                #[cfg(not(feature = "encryption"))]
                let write_data: &[u8] = data;

                let checksum = crc32fast::hash(write_data);
                let length = write_data.len() as u64;

                tmp.seek(SeekFrom::Start(current_offset))?;
                tmp.write_all(write_data)?;

                dir.upsert(SectionDirectoryEntry {
                    section_type: *section_type,
                    version: *version,
                    flags: section_type.default_flags(),
                    offset: current_offset,
                    length,
                    checksum,
                })?;

                // Align next section to page boundary
                let section_end = current_offset + length;
                current_offset = (section_end + page_size - 1) / page_size * page_size;
            }

            maybe_crash("write_sections:after_data");

            // Extend the image to the page-aligned end of the last section
            tmp.set_len(current_offset)?;

            // Write section directory
            let dir_bytes = dir.to_bytes();
            tmp.seek(SeekFrom::Start(DIRECTORY_OFFSET))?;
            tmp.write_all(&dir_bytes)?;

            maybe_crash("write_sections:after_directory");

            // reason: millis since UNIX epoch fits in u64 for ~585 million years
            #[allow(clippy::cast_possible_truncation)]
            let timestamp_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            Ok(DbHeader {
                iteration: new_iteration,
                checksum: dir.checksum(),
                snapshot_length: 0, // Not used in v2; directory CRC is in checksum field
                epoch,
                transaction_id,
                node_count,
                edge_count,
                timestamp_ms,
            })
        })?;

        maybe_crash("write_sections:after_fsync");

        Ok(())
    }

    /// Reads the section directory from the file.
    ///
    /// Detects v2 format by checking the `snapshot_length` field in the active
    /// DbHeader: v2 writes set `snapshot_length = 0`, while v1 always has a
    /// non-zero snapshot length when data exists.
    ///
    /// Returns `None` only when the file is unambiguously v1
    /// (`snapshot_length` non-zero) or uninitialized (header iteration is 0).
    /// Once the header asserts v2, any failure to locate or parse the
    /// directory is surfaced as an error: misreporting v2 corruption as a v1
    /// file would cause callers to fall back to v1 read paths and mask the
    /// underlying problem.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - I/O fails
    /// - The header asserts v2 but the file is too short to hold a directory page
    /// - The directory page bytes fail to parse as a `SectionDirectory`
    /// - The directory page CRC does not match the value recorded in the active header
    pub fn read_section_directory(&self) -> Result<Option<crate::container::SectionDirectory>> {
        use crate::container::SectionDirectory;
        use crate::container::directory::DIRECTORY_OFFSET;

        let active_header = self.active_header.lock();

        // v1 files have snapshot_length > 0; v2 files set it to 0 and put the
        // directory CRC in the checksum field. An uninitialized header (iteration
        // == 0) means no data has been written yet.
        if active_header.is_empty() || active_header.snapshot_length > 0 {
            return Ok(None);
        }
        let expected_checksum = active_header.checksum;
        drop(active_header);

        // Past this point the header asserts v2: any failure to read or parse
        // the directory is real corruption, not a v1/v2 misdetection. Surface
        // it instead of silently falling through to read_snapshot, where v1 CRC
        // logic would mask the underlying cause.
        let file_size = self.file.lock().metadata()?.len();
        if file_size < DIRECTORY_OFFSET + 4096 {
            return Err(Error::Internal(format!(
                "v2 header indicates section directory at offset {DIRECTORY_OFFSET:#X}, \
                 but file is only {file_size} bytes",
            )));
        }

        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(DIRECTORY_OFFSET))?;

        let mut buf = vec![0u8; 4096];
        std::io::Read::read_exact(&mut *file, &mut buf)?;

        let dir = SectionDirectory::from_bytes(&buf).map_err(|e| {
            Error::Internal(format!(
                "v2 section directory at offset {DIRECTORY_OFFSET:#X} failed to parse: {e}",
            ))
        })?;

        // Cross-check the directory bytes against the CRC the writer recorded
        // in the active header. A mismatch means the directory page is torn or
        // corrupted (e.g. a partial write from a crashed checkpoint), not a
        // format ambiguity.
        let actual_checksum = crc32fast::hash(&buf);
        if actual_checksum != expected_checksum {
            return Err(Error::Internal(format!(
                "v2 section directory checksum mismatch: \
                 header recorded {expected_checksum:#010X}, computed {actual_checksum:#010X}",
            )));
        }

        if dir.is_empty() {
            return Ok(None);
        }
        Ok(Some(dir))
    }

    /// Reads a single section's data from the file.
    ///
    /// Uses the section directory entry to locate and verify the data.
    ///
    /// # Errors
    ///
    /// Returns an error if read fails or CRC checksum doesn't match.
    pub fn read_section_data(
        &self,
        entry: &grafeo_common::storage::SectionDirectoryEntry,
    ) -> Result<Vec<u8>> {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(entry.offset))?;

        // reason: section length is bounded by file size, which fits in usize on 64-bit targets;
        // on 32-bit targets sections would OOM long before reaching 4 GiB
        // reason: value bounded by collection size, fits usize
        #[allow(clippy::cast_possible_truncation)]
        let mut data = vec![0u8; entry.length as usize];
        std::io::Read::read_exact(&mut *file, &mut data)?;

        // Verify CRC on the raw bytes (encrypted or plaintext)
        let actual_crc = crc32fast::hash(&data);
        if actual_crc != entry.checksum {
            return Err(Error::Internal(format!(
                "section {:?} CRC mismatch: expected {:#010X}, got {actual_crc:#010X}",
                entry.section_type, entry.checksum
            )));
        }

        // Decrypt if encryption is enabled
        #[cfg(feature = "encryption")]
        if let Some(ref enc) = self.section_encryptor {
            let aad = format!("grafeo-section:{}", entry.section_type as u32);
            return enc.decrypt(&data, aad.as_bytes()).map_err(|_| {
                Error::Internal(format!(
                    "section {:?} decryption failed: wrong key or corrupted data",
                    entry.section_type
                ))
            });
        }

        Ok(data)
    }

    /// Memory-maps a single section for zero-copy read access.
    ///
    /// The section's CRC-32 is verified against the mmap'd bytes before
    /// returning, which also warms the OS page cache. Only sections with
    /// `flags.mmap_able = true` can be mapped (index sections).
    ///
    /// The returned [`MmapSection`](crate::container::MmapSection) is
    /// independent of the file mutex: multiple mmaps can coexist.
    ///
    /// Checkpoints (`write_sections()` / `write_snapshot()`) never modify the
    /// mapped file in place: they rename a new image over the database path.
    /// On Linux/macOS an existing mapping therefore keeps a consistent view
    /// of the old image (it does not see the new checkpoint) and keeps the
    /// old file's disk space allocated until it is unmapped. On Windows an
    /// active mapping makes the rename that publishes the new image fail.
    /// Drop all `MmapSection` handles **before writing** on every platform:
    /// it is required on Windows and frees the old image's space elsewhere.
    /// See [`MmapSection`](crate::container::MmapSection) for the full
    /// lifecycle.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The section is not mmap-able (data section)
    /// - The section is encrypted (typed `DirectMmapUnavailable`)
    /// - The mmap system call fails
    /// - The CRC-32 checksum does not match (corrupt data)
    ///
    /// # Direct-mmap support matrix (G-EM0.1)
    ///
    /// | Layout | Direct mmap |
    /// | --- | --- |
    /// | Plaintext, `mmap_able`, non-zero length CompactStore/index | yes (CRC-verified) |
    /// | Encrypted section (AES-GCM) | no — `DirectMmapUnavailable` |
    /// | Non-`mmap_able` data section (LPG, Catalog, …) | no — `DirectMmapUnavailable` |
    /// | Zero-length section | no — `DirectMmapUnavailable` |
    /// | Compressed payload (none shipped today) | would need a separate design |
    ///
    /// CRC validation may fault every page into the OS file cache; it must not
    /// copy the section into an anonymous `Vec`. Callers must not fall back to
    /// `read_section_data` while still reporting a mapped backing diagnostic.
    #[allow(unsafe_code)]
    pub fn mmap_section(
        &self,
        entry: &grafeo_common::storage::SectionDirectoryEntry,
    ) -> Result<crate::container::MmapSection> {
        // Direct mapping is valid only for the plaintext container bytes.
        // AES-GCM sections require whole-section decryption today, so letting
        // callers mmap ciphertext would either expose invalid bytes or tempt a
        // silent eager fallback. A future page-decryption design can add a
        // distinct mapped backend without weakening this fail-closed contract.
        #[cfg(feature = "encryption")]
        if self.section_encryptor.is_some() {
            return Err(Error::Storage(StorageError::DirectMmapUnavailable(
                "encrypted sections require page decryption before direct mmap".to_string(),
            )));
        }

        if !entry.flags.mmap_able {
            return Err(Error::Storage(StorageError::DirectMmapUnavailable(
                format!("section {:?} is not mmap-able", entry.section_type),
            )));
        }

        if entry.length == 0 {
            return Err(Error::Storage(StorageError::DirectMmapUnavailable(
                format!("section {:?} has zero length", entry.section_type),
            )));
        }

        let file = self.file.lock();

        // SAFETY: We hold a lock on the `.grafeo` file, preventing
        // concurrent modification by other processes, and checkpoints never
        // write a published file in place (they rename a new image over the
        // path), so the mapped bytes do not change. The mapping is read-only.
        // The section region [offset .. offset+length] was written by
        // write_sections() and its CRC is verified below before the mmap
        // is exposed to callers.
        // reason: section length is bounded by file size, fits in usize on 64-bit targets
        #[allow(clippy::cast_possible_truncation)]
        let section_len = entry.length as usize;
        let mmap = unsafe {
            memmap2::MmapOptions::new()
                .offset(entry.offset)
                .len(section_len)
                .map(&*file)
        }
        .map_err(Error::Io)?;

        drop(file);

        // Verify CRC on the mmap'd bytes. This reads through the mapping,
        // which triggers page faults and warms the OS page cache: a free
        // prefetch disguised as an integrity check.
        let actual_crc = crc32fast::hash(&mmap);
        if actual_crc != entry.checksum {
            return Err(Error::Internal(format!(
                "section {:?} CRC mismatch: expected {:#010X}, got {actual_crc:#010X}",
                entry.section_type, entry.checksum
            )));
        }

        Ok(crate::container::MmapSection::new(
            mmap,
            entry.section_type,
            entry.checksum,
        ))
    }

    /// Copies the database file to `dest` using the already-locked file handle.
    ///
    /// `std::fs::copy()` opens the source with a new handle, which fails on
    /// Windows when an exclusive lock is held. This method reads through the
    /// existing handle, avoiding lock conflicts.
    ///
    /// # Errors
    ///
    /// Returns an error if the read or write fails.
    pub fn copy_to(&self, dest: &Path) -> Result<u64> {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(0))?;

        let mut dest_file = fs::File::create(dest)?;
        let bytes = std::io::copy(&mut *file, &mut dest_file).map_err(Error::Io)?;
        dest_file.sync_all()?;
        Ok(bytes)
    }

    /// Syncs and releases the file lock. The lock is released even when the
    /// sync fails.
    ///
    /// # Errors
    ///
    /// Returns an error if sync or unlock fails (the sync error first).
    pub fn close(&self) -> Result<()> {
        let file = self.file.lock();
        let synced = if self.read_only {
            Ok(())
        } else {
            grafeo_common::testing::crash::maybe_fail_io("file_close_sync")
                .and_then(|()| file.sync_all())
        };
        // Release the lock even when the sync failed, so a caller that gives
        // up on this handle can reopen the file; report the sync error first.
        let unlocked = file
            .unlock()
            .map_err(|e| Error::Internal(format!("failed to unlock database file: {e}")));
        synced?;
        unlocked
    }
}

/// How many times [`open_locked`] reopens the database file when a
/// concurrent checkpoint replaced it between the open and the lock.
const LOCK_IDENTITY_ATTEMPTS: usize = 8;

/// Kind of advisory lock [`open_locked`] takes on the database file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LockKind {
    /// Read/write handle with an exclusive lock (writable open).
    Exclusive,
    /// Read-only handle with a shared lock (read-only open).
    Shared,
}

/// Opens the database file at `path` and locks it.
///
/// Every checkpoint renames a new image over `path` and then drops the old
/// handle, which releases the lock on the old (now unlinked) inode. A second
/// opener that opened `path` just before that rename can then lock the old
/// inode and believe it owns the database. So after locking, the handle's
/// identity is compared with the file currently at `path`; on a mismatch the
/// handle is dropped and the open is retried on the new file (whose lock is
/// held by the checkpointing writer, so a writable retry normally fails with
/// "locked by another process").
///
/// On non-Unix platforms the identity check is skipped (current behaviour).
fn open_locked(path: &Path, kind: LockKind) -> Result<File> {
    open_locked_with(path, kind, || {})
}

/// [`open_locked`] with a hook that runs between each open and lock attempt,
/// so tests can replace the file inside that window deterministically.
fn open_locked_with(path: &Path, kind: LockKind, mut before_lock: impl FnMut()) -> Result<File> {
    for _ in 0..LOCK_IDENTITY_ATTEMPTS {
        let file = match kind {
            LockKind::Exclusive => OpenOptions::new().read(true).write(true).open(path)?,
            LockKind::Shared => OpenOptions::new().read(true).open(path)?,
        };

        before_lock();

        match kind {
            LockKind::Exclusive => file.try_lock_exclusive().map_err(|_| {
                Error::Internal(format!(
                    "database file is locked by another process: {}",
                    path.display()
                ))
            })?,
            LockKind::Shared => file.try_lock_shared().map_err(|_| {
                Error::Internal(format!(
                    "database file cannot be locked for reading: {}",
                    path.display()
                ))
            })?,
        }

        if handle_is_file_at_path(&file, path)? {
            return Ok(file);
        }
        // The lock is on a replaced inode; dropping the handle releases it.
        drop(file);
    }
    Err(Error::Internal(format!(
        "database file {} was replaced by a concurrent checkpoint on each of \
         {LOCK_IDENTITY_ATTEMPTS} open attempts; giving up",
        path.display()
    )))
}

/// Returns `true` if `file` is the file currently at `path` (same device and
/// inode). A missing path counts as a mismatch; the retry then reports it.
#[cfg(unix)]
fn handle_is_file_at_path(file: &File, path: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let handle = file.metadata()?;
    match fs::metadata(path) {
        Ok(current) => Ok(handle.dev() == current.dev() && handle.ino() == current.ino()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Non-Unix: no portable file identity in std, keep the previous behaviour.
#[cfg(not(unix))]
fn handle_is_file_at_path(_file: &File, _path: &Path) -> Result<bool> {
    Ok(true)
}

/// Path the checkpoint image is published to: the database path with
/// symlinks resolved, so a symlinked database is replaced at its real
/// location instead of the link being overwritten by a regular file.
fn publish_target(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Staging file for a checkpoint image: `<target>.checkpoint-tmp`, in the
/// same directory so the rename never crosses a filesystem.
fn checkpoint_tmp_path(target: &Path) -> PathBuf {
    let mut tmp = target.as_os_str().to_owned();
    tmp.push(CHECKPOINT_TMP_SUFFIX);
    PathBuf::from(tmp)
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

/// Makes a completed rename in `path`'s directory durable.
///
/// On Unix the new directory entry is only guaranteed to survive power loss
/// once the directory itself is fsynced. Windows has no portable way to
/// fsync a directory from std; NTFS journals the rename as metadata.
fn sync_parent_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let parent = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        File::open(parent)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

impl Drop for GrafeoFileManager {
    fn drop(&mut self) {
        let file = self.file.lock();
        let _ = file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_dir() -> TempDir {
        TempDir::new().expect("create temp dir")
    }

    #[test]
    fn create_and_open() {
        let dir = test_dir();
        let path = dir.path().join("test.grafeo");

        // Create
        let manager = GrafeoFileManager::create(&path).unwrap();
        assert!(path.exists());
        assert!(manager.active_header().is_empty());
        drop(manager);

        // Open
        let manager = GrafeoFileManager::open(&path).unwrap();
        assert!(manager.active_header().is_empty());
    }

    #[test]
    fn create_fails_if_exists() {
        let dir = test_dir();
        let path = dir.path().join("test.grafeo");

        GrafeoFileManager::create(&path).unwrap();
        let result = GrafeoFileManager::create(&path);
        assert!(result.is_err());
    }

    #[test]
    fn open_fails_if_not_exists() {
        let dir = test_dir();
        let path = dir.path().join("nonexistent.grafeo");

        let result = GrafeoFileManager::open(&path);
        assert!(result.is_err());
    }

    #[test]
    fn write_and_read_snapshot() {
        let dir = test_dir();
        let path = dir.path().join("test.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();

        let snapshot_data = b"hello grafeo snapshot data";
        manager.write_snapshot(snapshot_data, 1, 1, 10, 20).unwrap();

        let loaded = manager.read_snapshot().unwrap();
        assert_eq!(loaded, snapshot_data);

        // Verify header was updated
        let header = manager.active_header();
        assert_eq!(header.iteration, 1);
        assert_eq!(header.snapshot_length, snapshot_data.len() as u64);
        assert_eq!(header.epoch, 1);
        assert_eq!(header.node_count, 10);
        assert_eq!(header.edge_count, 20);
    }

    #[test]
    fn snapshot_persists_across_reopen() {
        let dir = test_dir();
        let path = dir.path().join("test.grafeo");

        let snapshot_data = b"persistent data across reopen";

        // Write
        {
            let manager = GrafeoFileManager::create(&path).unwrap();
            manager
                .write_snapshot(snapshot_data, 5, 3, 100, 200)
                .unwrap();
        }

        // Reopen and read
        {
            let manager = GrafeoFileManager::open(&path).unwrap();
            let loaded = manager.read_snapshot().unwrap();
            assert_eq!(loaded, snapshot_data);

            let header = manager.active_header();
            assert_eq!(header.iteration, 1);
            assert_eq!(header.epoch, 5);
            assert_eq!(header.node_count, 100);
        }
    }

    #[test]
    fn alternating_snapshots() {
        let dir = test_dir();
        let path = dir.path().join("test.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();

        // First checkpoint
        let data1 = b"snapshot version 1";
        manager.write_snapshot(data1, 1, 1, 10, 5).unwrap();
        assert_eq!(manager.active_header().iteration, 1);

        // Second checkpoint (alternates to other slot)
        let data2 = b"snapshot version 2 with more data";
        manager.write_snapshot(data2, 2, 2, 20, 10).unwrap();
        assert_eq!(manager.active_header().iteration, 2);

        let loaded = manager.read_snapshot().unwrap();
        assert_eq!(loaded, data2);
    }

    #[test]
    fn read_empty_snapshot() {
        let dir = test_dir();
        let path = dir.path().join("test.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        let data = manager.read_snapshot().unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn read_snapshot_returns_empty_on_v2_header() {
        // After write_sections, snapshot_length == 0 in the active header and the
        // checksum field holds the section-directory CRC. The pre-fix v1 reader
        // would read 0 bytes, CRC empty data to 0, and mismatch the directory CRC.
        // The fix early-returns Ok(Vec::new()) when snapshot_length == 0.
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("v2.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        manager
            .write_sections(&[(SectionType::LpgStore, b"section payload")], 1, 1, 0, 0)
            .unwrap();

        // Pre-fix: this returned Err("snapshot checksum mismatch").
        // Post-fix: returns Ok(Vec::new()), letting engine fall through to v2 dispatch.
        let data = manager.read_snapshot().unwrap();
        assert!(
            data.is_empty(),
            "v2 file should produce empty snapshot vec, not an error"
        );

        // Sanity: header confirms this is a v2 file (snapshot_length == 0 with non-zero checksum).
        let header = manager.active_header();
        assert_eq!(header.snapshot_length, 0);
        assert!(!header.is_empty());
    }

    #[test]
    fn read_section_directory_surfaces_parse_error_on_v2_header() {
        // A v2 header with a corrupted directory page must not silently
        // degrade to "this is a v1 file" — that masking is what made the
        // GRAFEO-X001 in #323 surface as a misleading snapshot CRC error
        // instead of pointing at the real directory corruption.
        use crate::container::directory::DIRECTORY_OFFSET;
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("corrupt_dir.grafeo");

        {
            let manager = GrafeoFileManager::create(&path).unwrap();
            manager
                .write_sections(&[(SectionType::LpgStore, b"section payload")], 1, 1, 0, 0)
                .unwrap();
        }

        // Overwrite the directory page count field with a value above MAX_SECTIONS
        // so SectionDirectory::from_bytes rejects it as malformed.
        {
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(DIRECTORY_OFFSET)).unwrap();
            file.write_all(&u32::MAX.to_le_bytes()).unwrap();
        }

        let manager = GrafeoFileManager::open(&path).unwrap();
        let err = manager
            .read_section_directory()
            .expect_err("corrupt v2 directory must surface as Err, not Ok(None)");
        let msg = err.to_string();
        assert!(
            msg.contains("v2 section directory") && msg.contains("failed to parse"),
            "error should name the v2 directory and the parse failure, got: {msg}"
        );
    }

    #[test]
    fn read_section_directory_surfaces_checksum_mismatch_on_v2_header() {
        // A torn write (e.g. a crashed checkpoint) can leave the directory page
        // bytes inconsistent with the CRC the writer recorded in the active
        // header. The pre-fix wildcard match swallowed this, falling through to
        // v1 read logic that reported a misleading snapshot checksum mismatch.
        use crate::container::directory::DIRECTORY_OFFSET;
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("torn_dir.grafeo");

        {
            let manager = GrafeoFileManager::create(&path).unwrap();
            manager
                .write_sections(&[(SectionType::LpgStore, b"section payload")], 1, 1, 0, 0)
                .unwrap();
        }

        // Flip a byte in the reserved area of the directory page (bytes 4-7).
        // The page still parses (count is intact, no entries change) but the
        // CRC over the page no longer matches the value in the active header.
        {
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(DIRECTORY_OFFSET + 4)).unwrap();
            file.write_all(&[0xAA]).unwrap();
        }

        let manager = GrafeoFileManager::open(&path).unwrap();
        let err = manager
            .read_section_directory()
            .expect_err("torn v2 directory must surface as Err, not Ok(None)");
        let msg = err.to_string();
        assert!(
            msg.contains("v2 section directory checksum mismatch"),
            "error should identify the directory CRC mismatch, got: {msg}"
        );
    }

    #[test]
    fn sidecar_wal_path_computation() {
        let dir = test_dir();
        let path = dir.path().join("mydb.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        let wal_path = manager.sidecar_wal_path();

        assert_eq!(
            wal_path.file_name().unwrap().to_str().unwrap(),
            "mydb.grafeo.wal"
        );
        assert!(!manager.has_sidecar_wal());
    }

    #[test]
    fn sidecar_wal_detect_and_remove() {
        let dir = test_dir();
        let path = dir.path().join("test.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        assert!(!manager.has_sidecar_wal());

        // Create sidecar directory manually (simulating engine behavior)
        fs::create_dir_all(manager.sidecar_wal_path()).unwrap();
        assert!(manager.has_sidecar_wal());

        // Remove it
        manager.remove_sidecar_wal().unwrap();
        assert!(!manager.has_sidecar_wal());
    }

    #[test]
    fn file_size_grows_with_data() {
        let dir = test_dir();
        let path = dir.path().join("test.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        let empty_size = manager.file_size().unwrap();

        // Empty file should be at least 12 KiB (3 headers)
        assert!(empty_size >= DATA_OFFSET, "empty size: {empty_size}");

        let big_data = vec![0xAB; 100_000];
        manager.write_snapshot(&big_data, 1, 1, 0, 0).unwrap();

        let full_size = manager.file_size().unwrap();
        assert!(full_size > empty_size);
        assert_eq!(full_size, DATA_OFFSET + big_data.len() as u64);
    }

    #[test]
    fn exclusive_lock_prevents_second_open() {
        let dir = test_dir();
        let path = dir.path().join("locked.grafeo");

        let _manager1 = GrafeoFileManager::create(&path).unwrap();

        // Second open should fail
        let result = GrafeoFileManager::open(&path);
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("locked"));
    }

    #[test]
    fn lock_released_after_close() {
        let dir = test_dir();
        let path = dir.path().join("lockclose.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        manager.write_snapshot(b"data", 1, 1, 0, 0).unwrap();
        manager.close().unwrap();

        // Should succeed after close
        let manager2 = GrafeoFileManager::open(&path).unwrap();
        let data = manager2.read_snapshot().unwrap();
        assert_eq!(data, b"data");
    }

    #[cfg(feature = "testing-crash-injection")]
    #[test]
    fn lock_released_when_close_sync_fails() {
        use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
        let dir = test_dir();
        let path = dir.path().join("lockclosefail.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        manager.write_snapshot(b"data", 1, 1, 0, 0).unwrap();
        enable_io_failure_at(1);
        let result = manager.close();
        disable_io_failure();
        assert!(result.is_err(), "the sync failure is reported");

        // The lock is released anyway, while the old handle is still alive.
        let manager2 = GrafeoFileManager::open(&path).unwrap();
        assert_eq!(manager2.read_snapshot().unwrap(), b"data");
        drop(manager);
    }

    #[test]
    fn lock_released_on_drop() {
        let dir = test_dir();
        let path = dir.path().join("lockdrop.grafeo");

        {
            let _manager = GrafeoFileManager::create(&path).unwrap();
            // Drop without explicit close
        }

        // Should succeed after drop
        let _manager2 = GrafeoFileManager::open(&path).unwrap();
    }

    #[test]
    fn checksum_mismatch_detected() {
        let dir = test_dir();
        let path = dir.path().join("test.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        manager.write_snapshot(b"valid data", 1, 1, 0, 0).unwrap();
        drop(manager);

        // Corrupt the snapshot data in the file
        {
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(DATA_OFFSET)).unwrap();
            file.write_all(b"CORRUPT!!!").unwrap();
        }

        let manager = GrafeoFileManager::open(&path).unwrap();
        let result = manager.read_snapshot();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("checksum"));
    }

    #[test]
    fn open_read_only_reads_snapshot() {
        let dir = test_dir();
        let path = dir.path().join("ro.grafeo");

        // Create and write snapshot, then close
        {
            let manager = GrafeoFileManager::create(&path).unwrap();
            manager
                .write_snapshot(b"read-only test data", 3, 2, 5, 10)
                .unwrap();
            manager.close().unwrap();
        }

        // Open read-only
        let ro = GrafeoFileManager::open_read_only(&path).unwrap();
        assert!(ro.is_read_only());
        let data = ro.read_snapshot().unwrap();
        assert_eq!(data, b"read-only test data");

        let header = ro.active_header();
        assert_eq!(header.epoch, 3);
        assert_eq!(header.node_count, 5);
        assert_eq!(header.edge_count, 10);
    }

    #[test]
    fn read_only_rejects_write_snapshot() {
        let dir = test_dir();
        let path = dir.path().join("ro_write.grafeo");

        {
            let manager = GrafeoFileManager::create(&path).unwrap();
            manager.close().unwrap();
        }

        let ro = GrafeoFileManager::open_read_only(&path).unwrap();
        let result = ro.write_snapshot(b"nope", 1, 1, 0, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("read-only"));
    }

    #[test]
    fn read_only_coexists_with_exclusive_after_close() {
        let dir = test_dir();
        let path = dir.path().join("coexist.grafeo");

        // Create, write, close
        {
            let manager = GrafeoFileManager::create(&path).unwrap();
            manager.write_snapshot(b"coexist data", 1, 1, 1, 1).unwrap();
            manager.close().unwrap();
        }

        // Two read-only opens should coexist
        let ro1 = GrafeoFileManager::open_read_only(&path).unwrap();
        let ro2 = GrafeoFileManager::open_read_only(&path).unwrap();

        assert_eq!(ro1.read_snapshot().unwrap(), b"coexist data");
        assert_eq!(ro2.read_snapshot().unwrap(), b"coexist data");
    }

    // ── Mmap section tests ─────────────────────────────────────────

    #[test]
    fn mmap_section_roundtrip() {
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("mmap.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();

        // Write two sections: one data (LPG), one index (VectorStore)
        let lpg_data = b"lpg node data here";
        let vector_data = vec![0x42u8; 8192]; // 8 KiB of vector embeddings

        manager
            .write_sections(
                &[
                    (SectionType::LpgStore, lpg_data.as_slice()),
                    (SectionType::VectorStore, &vector_data),
                ],
                1,
                1,
                10,
                5,
            )
            .unwrap();

        // Read the directory to get entries
        let section_dir = manager.read_section_directory().unwrap().unwrap();

        // Mmap the VectorStore section (mmap-able)
        let vector_entry = section_dir.find(SectionType::VectorStore).unwrap();
        let mmap = manager.mmap_section(vector_entry).unwrap();

        assert_eq!(mmap.section_type(), SectionType::VectorStore);
        assert_eq!(mmap.len(), vector_data.len());
        assert_eq!(mmap.as_bytes(), &vector_data);
        assert!(!mmap.is_empty());
    }

    #[test]
    fn mmap_rejects_data_sections() {
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("mmap_reject.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        manager
            .write_sections(&[(SectionType::LpgStore, b"data")], 1, 1, 1, 0)
            .unwrap();

        let section_dir = manager.read_section_directory().unwrap().unwrap();
        let lpg_entry = section_dir.find(SectionType::LpgStore).unwrap();

        let result = manager.mmap_section(lpg_entry);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("not mmap-able"),
            "unexpected error text: {err}"
        );
        // Typed fail-closed result — never a silent eager materialization.
        match err {
            Error::Storage(StorageError::DirectMmapUnavailable(_)) => {}
            other => panic!("expected DirectMmapUnavailable, got {other:?}"),
        }
    }

    #[test]
    fn mmap_detects_corruption() {
        use grafeo_common::storage::SectionType;
        use std::io::Write as IoWrite;

        let dir = test_dir();
        let path = dir.path().join("mmap_corrupt.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        let vector_data = vec![0xAB; 4096];
        manager
            .write_sections(&[(SectionType::VectorStore, &vector_data)], 1, 1, 0, 0)
            .unwrap();

        let section_dir = manager.read_section_directory().unwrap().unwrap();
        let entry = section_dir.find(SectionType::VectorStore).unwrap().clone();

        // Corrupt the section data by writing directly to the file
        drop(manager);
        {
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(entry.offset)).unwrap();
            file.write_all(b"CORRUPTED!").unwrap();
        }

        let manager = GrafeoFileManager::open(&path).unwrap();
        let result = manager.mmap_section(&entry);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("CRC mismatch"));
    }

    #[test]
    fn mmap_multiple_sections_coexist() {
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("mmap_multi.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();

        let vector_data = vec![0x11; 4096];
        let text_data = vec![0x22; 2048];

        manager
            .write_sections(
                &[
                    (SectionType::VectorStore, &vector_data),
                    (SectionType::TextIndex, &text_data),
                ],
                1,
                1,
                0,
                0,
            )
            .unwrap();

        let section_dir = manager.read_section_directory().unwrap().unwrap();

        // Mmap both index sections simultaneously
        let vec_entry = section_dir.find(SectionType::VectorStore).unwrap();
        let text_entry = section_dir.find(SectionType::TextIndex).unwrap();

        let vec_mmap = manager.mmap_section(vec_entry).unwrap();
        let text_mmap = manager.mmap_section(text_entry).unwrap();

        // Both are valid and independent
        assert_eq!(vec_mmap.as_bytes(), &vector_data);
        assert_eq!(text_mmap.as_bytes(), &text_data);
        assert_eq!(vec_mmap.section_type(), SectionType::VectorStore);
        assert_eq!(text_mmap.section_type(), SectionType::TextIndex);
    }

    #[test]
    fn mmap_drop_then_checkpoint_lifecycle() {
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("mmap_lifecycle.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();

        // Checkpoint 1: write vector section
        let vector_v1 = vec![0x11; 4096];
        manager
            .write_sections(&[(SectionType::VectorStore, &vector_v1)], 1, 1, 0, 0)
            .unwrap();

        // Mmap the section, read it
        let section_dir = manager.read_section_directory().unwrap().unwrap();
        let entry = section_dir.find(SectionType::VectorStore).unwrap();
        let mmap = manager.mmap_section(entry).unwrap();
        assert_eq!(mmap.as_bytes(), &vector_v1);

        // Drop the mmap before next checkpoint.
        // On Windows, writes fail if mmaps are still active (error 1224).
        // On all platforms, the intended lifecycle is: drop mmaps, checkpoint,
        // re-mmap. This keeps the flow simple and cross-platform.
        drop(mmap);

        // Checkpoint 2: write updated vector section
        let vector_v2 = vec![0x22; 8192];
        manager
            .write_sections(&[(SectionType::VectorStore, &vector_v2)], 2, 2, 0, 0)
            .unwrap();

        // Re-mmap the new section
        let section_dir = manager.read_section_directory().unwrap().unwrap();
        let entry = section_dir.find(SectionType::VectorStore).unwrap();
        let mmap = manager.mmap_section(entry).unwrap();
        assert_eq!(mmap.as_bytes(), &vector_v2);
        assert_eq!(mmap.len(), 8192);
    }

    #[test]
    fn mmap_section_debug_format() {
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("mmap_debug.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        manager
            .write_sections(&[(SectionType::VectorStore, &[1, 2, 3, 4])], 1, 1, 0, 0)
            .unwrap();

        let section_dir = manager.read_section_directory().unwrap().unwrap();
        let entry = section_dir.find(SectionType::VectorStore).unwrap();
        let mmap = manager.mmap_section(entry).unwrap();

        let debug = format!("{mmap:?}");
        assert!(debug.contains("MmapSection"));
        assert!(debug.contains("VectorStore"));
    }

    #[test]
    fn path_returns_database_file_path() {
        let dir = test_dir();
        let path = dir.path().join("alix.grafeo");
        let manager = GrafeoFileManager::create(&path).unwrap();
        assert_eq!(manager.path(), path);
    }

    #[test]
    fn file_header_returns_valid_header() {
        use crate::file::format;
        let dir = test_dir();
        let path = dir.path().join("gus.grafeo");
        let manager = GrafeoFileManager::create(&path).unwrap();
        let header = manager.file_header();
        assert_eq!(header.magic, format::MAGIC);
        assert_eq!(header.format_version, format::FORMAT_VERSION);
    }

    #[test]
    fn sync_succeeds_for_writable_manager() {
        let dir = test_dir();
        let path = dir.path().join("vincent.grafeo");
        let manager = GrafeoFileManager::create(&path).unwrap();
        manager.write_snapshot(b"sync test", 1, 1, 5, 3).unwrap();
        manager.sync().unwrap();
    }

    #[test]
    fn sync_skips_for_read_only_manager() {
        let dir = test_dir();
        let path = dir.path().join("jules.grafeo");
        {
            let manager = GrafeoFileManager::create(&path).unwrap();
            manager.write_snapshot(b"ro sync", 1, 1, 0, 0).unwrap();
            manager.close().unwrap();
        }
        let ro = GrafeoFileManager::open_read_only(&path).unwrap();
        ro.sync().unwrap();
    }

    #[test]
    fn close_succeeds_for_read_only_manager() {
        let dir = test_dir();
        let path = dir.path().join("mia.grafeo");
        {
            let manager = GrafeoFileManager::create(&path).unwrap();
            manager.close().unwrap();
        }
        let ro = GrafeoFileManager::open_read_only(&path).unwrap();
        ro.close().unwrap();
    }

    #[test]
    fn remove_sidecar_wal_no_op_when_absent() {
        let dir = test_dir();
        let path = dir.path().join("django.grafeo");
        let manager = GrafeoFileManager::create(&path).unwrap();
        assert!(!manager.has_sidecar_wal());
        manager.remove_sidecar_wal().unwrap();
        assert!(!manager.has_sidecar_wal());
    }

    #[test]
    fn multiple_snapshots_alternate_slots() {
        let dir = test_dir();
        let path = dir.path().join("shosanna.grafeo");
        let manager = GrafeoFileManager::create(&path).unwrap();

        manager.write_snapshot(b"epoch one", 1, 1, 1, 0).unwrap();
        assert_eq!(manager.active_header().iteration, 1);

        manager.write_snapshot(b"epoch two", 2, 2, 2, 1).unwrap();
        assert_eq!(manager.active_header().iteration, 2);

        manager
            .write_snapshot(b"epoch three, longer data", 3, 3, 3, 2)
            .unwrap();
        assert_eq!(manager.active_header().iteration, 3);

        let loaded = manager.read_snapshot().unwrap();
        assert_eq!(loaded, b"epoch three, longer data");

        let header = manager.active_header();
        assert_eq!(header.epoch, 3);
        assert_eq!(header.node_count, 3);
        assert!(header.timestamp_ms > 0);
    }

    #[test]
    fn snapshot_truncates_stale_trailing_data() {
        let dir = test_dir();
        let path = dir.path().join("hans.grafeo");
        let manager = GrafeoFileManager::create(&path).unwrap();

        let large_data = vec![0xAA; 50_000];
        manager.write_snapshot(&large_data, 1, 1, 0, 0).unwrap();
        let size_after_large = manager.file_size().unwrap();

        let small_data = b"tiny";
        manager.write_snapshot(small_data, 2, 2, 0, 0).unwrap();
        let size_after_small = manager.file_size().unwrap();

        assert!(
            size_after_small < size_after_large,
            "file should shrink: {size_after_small} >= {size_after_large}"
        );
        assert_eq!(manager.read_snapshot().unwrap(), small_data);
    }

    #[test]
    fn open_read_only_fails_for_nonexistent_file() {
        let dir = test_dir();
        let path = dir.path().join("beatrix_missing.grafeo");
        assert!(GrafeoFileManager::open_read_only(&path).is_err());
    }

    #[test]
    fn copy_to_produces_identical_file() {
        let dir = test_dir();
        let src = dir.path().join("copy_src.grafeo");
        let dest = dir.path().join("copy_dest.grafeo");

        let manager = GrafeoFileManager::create(&src).unwrap();
        manager
            .write_snapshot(b"copy test payload", 5, 3, 10, 20)
            .unwrap();

        // copy_to reads through the locked handle (no new open)
        let bytes = manager.copy_to(&dest).unwrap();
        assert!(bytes > 0);

        // The original is still usable
        let snap = manager.read_snapshot().unwrap();
        assert_eq!(snap, b"copy test payload");
        manager.close().unwrap();

        // The copy is a valid .grafeo file
        let copy = GrafeoFileManager::open(&dest).unwrap();
        let snap = copy.read_snapshot().unwrap();
        assert_eq!(snap, b"copy test payload");

        let header = copy.active_header();
        assert_eq!(header.epoch, 5);
        assert_eq!(header.node_count, 10);
        assert_eq!(header.edge_count, 20);
        copy.close().unwrap();
    }

    #[test]
    fn copy_to_from_read_only_manager() {
        let dir = test_dir();
        let src = dir.path().join("ro_copy_src.grafeo");
        let dest = dir.path().join("ro_copy_dest.grafeo");

        {
            let manager = GrafeoFileManager::create(&src).unwrap();
            manager
                .write_snapshot(b"read-only copy data", 7, 4, 3, 1)
                .unwrap();
            manager.close().unwrap();
        }

        let ro = GrafeoFileManager::open_read_only(&src).unwrap();
        let bytes = ro.copy_to(&dest).unwrap();
        assert!(bytes > 0);

        let copy = GrafeoFileManager::open(&dest).unwrap();
        assert_eq!(copy.read_snapshot().unwrap(), b"read-only copy data");
        copy.close().unwrap();
    }

    #[test]
    #[cfg(all(feature = "encryption", not(miri)))]
    fn encrypted_section_roundtrip() {
        use grafeo_common::encryption::KeyChain;
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("encrypted.grafeo");

        let kc = KeyChain::new([0xAB; 32]);

        let section_data = b"sensitive graph data that must be encrypted";

        // Write with encryption
        {
            let mut manager = GrafeoFileManager::create(&path).unwrap();
            manager.set_section_encryptor(kc.encryptor_for("section", b"test"));
            manager
                .write_sections(&[(SectionType::LpgStore, &section_data[..])], 1, 0, 0, 0)
                .unwrap();
            manager.close().unwrap();
        }

        // Read back with same key
        {
            let mut manager = GrafeoFileManager::open(&path).unwrap();
            manager.set_section_encryptor(kc.encryptor_for("section", b"test"));
            let dir_opt = manager.read_section_directory().unwrap();
            let section_dir = dir_opt.expect("directory should exist");
            let entry = section_dir
                .entries()
                .iter()
                .find(|e| e.section_type == SectionType::LpgStore)
                .expect("LpgStore section should exist");
            let decrypted = manager.read_section_data(entry).unwrap();
            assert_eq!(decrypted, section_data);
        }
    }

    #[test]
    #[cfg(all(feature = "encryption", not(miri)))]
    fn encrypted_section_wrong_key_fails() {
        use grafeo_common::encryption::KeyChain;
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("wrong_key.grafeo");

        let kc_a = KeyChain::new([0xAA; 32]);
        let kc_b = KeyChain::new([0xBB; 32]);

        // Write with key A
        {
            let mut manager = GrafeoFileManager::create(&path).unwrap();
            manager.set_section_encryptor(kc_a.encryptor_for("section", b"test"));
            manager
                .write_sections(&[(SectionType::LpgStore, b"secret data")], 1, 0, 0, 0)
                .unwrap();
            manager.close().unwrap();
        }

        // Read with key B: CRC passes (computed on encrypted bytes), but decryption fails
        {
            let mut manager = GrafeoFileManager::open(&path).unwrap();
            manager.set_section_encryptor(kc_b.encryptor_for("section", b"test"));
            let dir_opt = manager.read_section_directory().unwrap();
            let section_dir = dir_opt.expect("directory should exist");
            let entry = section_dir
                .entries()
                .iter()
                .find(|e| e.section_type == SectionType::LpgStore)
                .expect("section should exist");
            let result = manager.read_section_data(entry);
            assert!(result.is_err(), "decryption with wrong key should fail");
        }
    }

    // ── G-F0.1: truthful per-section directory versions ─────────────

    #[test]
    fn write_versioned_sections_preserves_supplied_versions() {
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("versioned.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        manager
            .write_versioned_sections(
                &[
                    (SectionType::Catalog, 2, b"catalog-v2".as_slice()),
                    (SectionType::LpgStore, 2, b"lpg-v2".as_slice()),
                    (SectionType::CompactStore, 3, b"compact-v3".as_slice()),
                    (SectionType::VectorStore, 2, b"vector-v2".as_slice()),
                    (SectionType::TextIndex, 1, b"text-v1".as_slice()),
                    (SectionType::PropertyIndex, 1, b"prop-v1".as_slice()),
                ],
                1,
                1,
                0,
                0,
            )
            .unwrap();

        let section_dir = manager
            .read_section_directory()
            .unwrap()
            .expect("directory should exist");

        let expected = [
            (SectionType::Catalog, 2u8, b"catalog-v2".as_slice()),
            (SectionType::LpgStore, 2, b"lpg-v2"),
            (SectionType::CompactStore, 3, b"compact-v3"),
            (SectionType::VectorStore, 2, b"vector-v2"),
            (SectionType::TextIndex, 1, b"text-v1"),
            (SectionType::PropertyIndex, 1, b"prop-v1"),
        ];
        for (section_type, version, payload) in expected {
            let entry = section_dir
                .find(section_type)
                .unwrap_or_else(|| panic!("missing {section_type:?}"));
            assert_eq!(entry.version, version, "{section_type:?} directory version");
            let data = manager.read_section_data(entry).unwrap();
            assert_eq!(data, payload, "{section_type:?} payload");
        }
        manager.close().unwrap();
    }

    #[test]
    fn write_sections_still_records_directory_version_one() {
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("legacy_write.grafeo");

        let manager = GrafeoFileManager::create(&path).unwrap();
        manager
            .write_sections(
                &[
                    (SectionType::LpgStore, b"legacy".as_slice()),
                    (SectionType::VectorStore, b"vec".as_slice()),
                ],
                1,
                1,
                0,
                0,
            )
            .unwrap();

        let section_dir = manager
            .read_section_directory()
            .unwrap()
            .expect("directory should exist");
        assert_eq!(section_dir.find(SectionType::LpgStore).unwrap().version, 1);
        assert_eq!(
            section_dir.find(SectionType::VectorStore).unwrap().version,
            1
        );
        manager.close().unwrap();
    }

    #[test]
    fn historical_outer_v1_with_higher_payload_bytes_remains_readable() {
        // Historical writers recorded directory version 1 even when the
        // payload body was a later format. Readers must not reject that
        // outer-vs-payload mismatch (no strict equality gate).
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("outer_v1_payload.grafeo");

        // Simulate historical outer-v1: use write_sections (always v1) with
        // bytes that a modern CompactStore/LPG payload reader would treat as
        // its own higher internal version. Storage only needs the bytes
        // round-trip; payload dispatch is covered by engine/core tests.
        let payload = b"pretend-v2-or-v3-payload-body";
        {
            let manager = GrafeoFileManager::create(&path).unwrap();
            manager
                .write_sections(
                    &[(SectionType::CompactStore, payload.as_slice())],
                    1,
                    1,
                    0,
                    0,
                )
                .unwrap();
            manager.close().unwrap();
        }

        let manager = GrafeoFileManager::open(&path).unwrap();
        let section_dir = manager
            .read_section_directory()
            .unwrap()
            .expect("directory should exist");
        let entry = section_dir.find(SectionType::CompactStore).unwrap();
        assert_eq!(
            entry.version, 1,
            "historical outer directory version stays 1"
        );
        assert_eq!(manager.read_section_data(entry).unwrap(), payload);
        manager.close().unwrap();
    }

    #[test]
    #[cfg(all(feature = "encryption", not(miri)))]
    fn encrypted_write_versioned_sections_preserves_versions() {
        use grafeo_common::encryption::KeyChain;
        use grafeo_common::storage::SectionType;

        let dir = test_dir();
        let path = dir.path().join("encrypted_versions.grafeo");
        let kc = KeyChain::new([0xCD; 32]);

        {
            let mut manager = GrafeoFileManager::create(&path).unwrap();
            manager.set_section_encryptor(kc.encryptor_for("section", b"test"));
            manager
                .write_versioned_sections(
                    &[
                        (SectionType::Catalog, 2, b"enc-catalog".as_slice()),
                        (SectionType::LpgStore, 2, b"enc-lpg".as_slice()),
                        (SectionType::VectorStore, 2, b"enc-vec".as_slice()),
                    ],
                    1,
                    0,
                    0,
                    0,
                )
                .unwrap();
            manager.close().unwrap();
        }

        let mut manager = GrafeoFileManager::open(&path).unwrap();
        manager.set_section_encryptor(kc.encryptor_for("section", b"test"));
        let section_dir = manager
            .read_section_directory()
            .unwrap()
            .expect("directory should exist");
        for (section_type, version, payload) in [
            (SectionType::Catalog, 2u8, b"enc-catalog".as_slice()),
            (SectionType::LpgStore, 2, b"enc-lpg"),
            (SectionType::VectorStore, 2, b"enc-vec"),
        ] {
            let entry = section_dir.find(section_type).unwrap();
            assert_eq!(entry.version, version, "{section_type:?}");
            assert_eq!(manager.read_section_data(entry).unwrap(), payload);
        }
        manager.close().unwrap();
    }

    // ── Crash-atomic checkpoint publication ──────────────────────────

    mod atomic_publish {
        use super::*;
        use grafeo_common::storage::SectionType;

        fn read_all_sections(manager: &GrafeoFileManager) -> Vec<(SectionType, Vec<u8>)> {
            let dir = manager
                .read_section_directory()
                .unwrap()
                .expect("directory should exist");
            dir.entries()
                .iter()
                .map(|e| (e.section_type, manager.read_section_data(e).unwrap()))
                .collect()
        }

        #[test]
        fn checkpoint_leaves_no_staging_file() {
            let dir = test_dir();
            let path = dir.path().join("staging.grafeo");
            let manager = GrafeoFileManager::create(&path).unwrap();
            manager
                .write_sections(&[(SectionType::LpgStore, b"one")], 1, 1, 0, 0)
                .unwrap();
            manager.write_snapshot(b"two", 2, 2, 0, 0).unwrap();
            let names: Vec<_> = fs::read_dir(dir.path())
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            assert_eq!(names, vec![std::ffi::OsString::from("staging.grafeo")]);
        }

        #[test]
        fn open_removes_stale_staging_file_and_keeps_old_image() {
            let dir = test_dir();
            let path = dir.path().join("stale.grafeo");
            {
                let manager = GrafeoFileManager::create(&path).unwrap();
                manager
                    .write_sections(&[(SectionType::LpgStore, b"published")], 1, 1, 0, 0)
                    .unwrap();
                manager.close().unwrap();
            }
            // What a checkpoint killed before its rename leaves behind.
            let tmp = checkpoint_tmp_path(&publish_target(&path));
            fs::write(&tmp, b"half-written image").unwrap();

            let manager = GrafeoFileManager::open(&path).unwrap();
            assert!(!tmp.exists(), "stale staging file must be removed on open");
            assert_eq!(
                read_all_sections(&manager),
                vec![(SectionType::LpgStore, b"published".to_vec())]
            );
        }

        #[test]
        fn read_only_open_leaves_staging_file_alone() {
            let dir = test_dir();
            let path = dir.path().join("ro_stale.grafeo");
            GrafeoFileManager::create(&path).unwrap().close().unwrap();
            let tmp = checkpoint_tmp_path(&publish_target(&path));
            fs::write(&tmp, b"x").unwrap();
            let _ro = GrafeoFileManager::open_read_only(&path).unwrap();
            assert!(tmp.exists());
        }

        #[test]
        fn checkpoint_after_reopen_alternates_slots_and_reads_back() {
            let dir = test_dir();
            let path = dir.path().join("slots.grafeo");
            let manager = GrafeoFileManager::create(&path).unwrap();
            for i in 1..=4u64 {
                let payload = vec![u8::try_from(i).unwrap(); 5000];
                manager
                    .write_sections(&[(SectionType::LpgStore, &payload)], i, i, 0, 0)
                    .unwrap();
                assert_eq!(manager.active_header().iteration, i);
                assert_eq!(
                    read_all_sections(&manager),
                    vec![(SectionType::LpgStore, payload.clone())]
                );
            }
            drop(manager);
            let manager = GrafeoFileManager::open(&path).unwrap();
            assert_eq!(manager.active_header().iteration, 4);
            assert_eq!(
                read_all_sections(&manager),
                vec![(SectionType::LpgStore, vec![4u8; 5000])]
            );
        }

        #[test]
        fn checkpoint_keeps_exclusive_lock_on_new_image() {
            let dir = test_dir();
            let path = dir.path().join("lock.grafeo");
            let manager = GrafeoFileManager::create(&path).unwrap();
            manager
                .write_sections(&[(SectionType::LpgStore, b"data")], 1, 1, 0, 0)
                .unwrap();
            assert!(
                GrafeoFileManager::open(&path).is_err(),
                "the published image must still be exclusively locked"
            );
        }

        #[cfg(unix)]
        #[test]
        fn checkpoint_preserves_permissions() {
            use std::os::unix::fs::PermissionsExt;
            let dir = test_dir();
            let path = dir.path().join("perms.grafeo");
            let manager = GrafeoFileManager::create(&path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            manager
                .write_sections(&[(SectionType::LpgStore, b"data")], 1, 1, 0, 0)
                .unwrap();
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }

        #[cfg(unix)]
        #[test]
        fn checkpoint_through_symlink_replaces_target_not_link() {
            let dir = test_dir();
            let real = dir.path().join("real.grafeo");
            let link = dir.path().join("link.grafeo");
            GrafeoFileManager::create(&real).unwrap().close().unwrap();
            std::os::unix::fs::symlink(&real, &link).unwrap();

            let manager = GrafeoFileManager::open(&link).unwrap();
            manager
                .write_sections(&[(SectionType::LpgStore, b"via link")], 1, 1, 0, 0)
                .unwrap();
            drop(manager);

            assert!(
                fs::symlink_metadata(&link)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            let manager = GrafeoFileManager::open(&real).unwrap();
            assert_eq!(
                read_all_sections(&manager),
                vec![(SectionType::LpgStore, b"via link".to_vec())]
            );
        }

        // ── Lock identity after a concurrent checkpoint's rename ─────

        #[cfg(unix)]
        fn inode(path: &Path) -> u64 {
            use std::os::unix::fs::MetadataExt;
            fs::metadata(path).unwrap().ino()
        }

        #[cfg(unix)]
        #[test]
        fn identity_check_detects_file_replaced_under_open_handle() {
            let dir = test_dir();
            let path = dir.path().join("ident.grafeo");
            let other = dir.path().join("other.grafeo");
            fs::write(&path, b"old").unwrap();
            fs::write(&other, b"new").unwrap();

            let handle = File::open(&path).unwrap();
            assert!(handle_is_file_at_path(&handle, &path).unwrap());
            fs::rename(&other, &path).unwrap();
            assert!(!handle_is_file_at_path(&handle, &path).unwrap());
            fs::remove_file(&path).unwrap();
            assert!(!handle_is_file_at_path(&handle, &path).unwrap());
        }

        /// Deterministic replay of the race: the file at `path` is replaced
        /// after the open but before the lock. The stale handle's lock must
        /// be rejected and the retry must lock the new inode.
        #[cfg(unix)]
        #[test]
        fn open_retries_and_locks_new_inode_when_replaced_before_lock() {
            use std::os::unix::fs::MetadataExt;

            let dir = test_dir();
            let path = dir.path().join("race.grafeo");
            let replacement = dir.path().join("replacement.grafeo");
            GrafeoFileManager::create(&path).unwrap().close().unwrap();
            {
                let manager = GrafeoFileManager::create(&replacement).unwrap();
                manager
                    .write_sections(&[(SectionType::LpgStore, b"new image")], 1, 1, 0, 0)
                    .unwrap();
            }
            let old_ino = inode(&path);
            let new_ino = inode(&replacement);
            assert_ne!(old_ino, new_ino);

            for kind in [LockKind::Exclusive, LockKind::Shared] {
                if kind == LockKind::Shared {
                    // Set the race up again for the read-only path.
                    fs::copy(&path, &replacement).unwrap();
                }
                let expected_ino = inode(&replacement);
                let mut attempts = 0;
                let file = open_locked_with(&path, kind, || {
                    attempts += 1;
                    if attempts == 1 {
                        // A concurrent checkpoint publishes its image now.
                        fs::rename(&replacement, &path).unwrap();
                    }
                })
                .unwrap();
                assert_eq!(attempts, 2, "{kind:?}: exactly one retry expected");
                assert_eq!(file.metadata().unwrap().ino(), expected_ino, "{kind:?}");
                drop(file);
            }

            // The retried open is a fully usable manager on the new image.
            let manager = GrafeoFileManager::open(&path).unwrap();
            assert_eq!(
                read_all_sections(&manager),
                vec![(SectionType::LpgStore, b"new image".to_vec())]
            );
        }

        /// The review scenario end to end: a second opener opens the file,
        /// the owning writer checkpoints (rename + drop of the old handle,
        /// which releases the old inode's lock), then the second opener
        /// locks. It must not end up owning the replaced inode.
        #[cfg(unix)]
        #[test]
        fn second_opener_cannot_lock_inode_replaced_by_checkpoint() {
            let dir = test_dir();
            let path = dir.path().join("owner.grafeo");
            let writer = GrafeoFileManager::create(&path).unwrap();
            writer
                .write_sections(&[(SectionType::LpgStore, b"v1")], 1, 1, 0, 0)
                .unwrap();

            let mut checkpointed = false;
            let result = open_locked_with(&path, LockKind::Exclusive, || {
                if !checkpointed {
                    checkpointed = true;
                    writer
                        .write_sections(&[(SectionType::LpgStore, b"v2")], 2, 2, 0, 0)
                        .unwrap();
                }
            });
            assert!(checkpointed);
            let err = result.expect_err("the replaced inode must not be handed out");
            assert!(
                err.to_string().contains("locked by another process"),
                "unexpected error: {err}"
            );
            // The writer's staging file and image are untouched.
            assert!(!checkpoint_tmp_path(&publish_target(&path)).exists());
            assert_eq!(
                read_all_sections(&writer),
                vec![(SectionType::LpgStore, b"v2".to_vec())]
            );
        }

        #[cfg(unix)]
        #[test]
        fn open_gives_up_when_file_is_replaced_on_every_attempt() {
            let dir = test_dir();
            let path = dir.path().join("churn.grafeo");
            let next = dir.path().join("next.grafeo");
            fs::write(&path, b"0").unwrap();
            let mut attempts = 0;
            let err = open_locked_with(&path, LockKind::Exclusive, || {
                attempts += 1;
                fs::write(&next, b"x").unwrap();
                fs::rename(&next, &path).unwrap();
            })
            .expect_err("must stop retrying");
            assert_eq!(attempts, LOCK_IDENTITY_ATTEMPTS);
            assert!(
                err.to_string()
                    .contains("replaced by a concurrent checkpoint"),
                "unexpected error: {err}"
            );
        }

        /// Crash at every injected point of a second checkpoint, then reopen.
        /// The file must hold exactly the first or the second image.
        #[cfg(feature = "testing-crash-injection")]
        #[test]
        fn crash_at_every_point_of_second_checkpoint_reopens_old_or_new() {
            use grafeo_common::testing::crash::{CrashResult, with_crash_at};

            let old = vec![
                (SectionType::Catalog, vec![1u8; 9000]),
                (SectionType::LpgStore, vec![2u8; 20000]),
            ];
            let new = vec![
                (SectionType::Catalog, vec![3u8; 7000]),
                (SectionType::LpgStore, vec![4u8; 30000]),
            ];
            let mut crashed = 0;
            for point in 1..=32 {
                let dir = test_dir();
                let path = dir.path().join("crash.grafeo");
                {
                    let manager = GrafeoFileManager::create(&path).unwrap();
                    let refs: Vec<_> = old.iter().map(|(t, d)| (*t, d.as_slice())).collect();
                    manager.write_sections(&refs, 1, 1, 0, 0).unwrap();
                }
                let manager = GrafeoFileManager::open(&path).unwrap();
                let refs: Vec<_> = new.iter().map(|(t, d)| (*t, d.as_slice())).collect();
                let result = with_crash_at(
                    point,
                    std::panic::AssertUnwindSafe(|| {
                        manager.write_sections(&refs, 2, 2, 0, 0).unwrap();
                    }),
                );
                let done = matches!(result, CrashResult::Completed(()));
                drop(manager);

                let reopened = GrafeoFileManager::open(&path)
                    .unwrap_or_else(|e| panic!("point {point}: reopen failed: {e}"));
                let got = read_all_sections(&reopened);
                assert!(
                    got == old || got == new,
                    "point {point}: file holds neither the old nor the new image"
                );
                if done {
                    assert_eq!(got, new);
                    break;
                }
                crashed += 1;
            }
            // before_data, after_data, after_directory, before_rename,
            // after_rename, after_fsync
            assert_eq!(crashed, 6);
        }
    }
}
