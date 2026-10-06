//! One WAL group (a transaction's records) buffered before its commit, with
//! bounded RAM.
//!
//! A [`GroupBuffer`] holds the group's frames already encoded. While they fit
//! under the spill threshold they stay in RAM; past it they move to a spill
//! file of their own in the WAL directory's [`SPILL_DIR`] and every later
//! frame is appended there. RAM per group stays around the threshold plus a
//! fixed I/O chunk, whatever the group's size. The byte cap
//! ([`GroupLimits::max_bytes`]) bounds the group's total encoded size, so
//! once a group spills it is a disk budget.
//!
//! At commit, [`TypedWal::log_group`](super::TypedWal::log_group) streams the
//! frames into the active WAL file under the WAL's append lock, through a
//! bounded read buffer, followed by the commit markers. The group is one
//! contiguous run of frames there, exactly as if it had been buffered in RAM.
//!
//! Spill files are scratch, never WAL:
//! - they live in a subdirectory, and every WAL reader (recovery, the
//!   generation cursor, live backup) only takes `wal_*.log` files from the WAL
//!   directory itself;
//! - a [`GroupBuffer`] deletes its file on commit, rollback and drop, and
//!   [`remove_leftover_spill_files`] deletes what a crash left behind; the
//!   engine calls it when it opens a database for writing (not every
//!   `WalManager` does: a live database opens private managers on its own
//!   WAL directory, for an epoch handoff for example);
//! - with WAL encryption on, each spill file is encrypted with a key of its
//!   own that is never stored ([`PageEncryptor::ephemeral`]), so a file left
//!   by a crash cannot be read by anyone.
//!
//! [`PageEncryptor::ephemeral`]: grafeo_common::encryption::PageEncryptor::ephemeral

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::WalEntry;
use grafeo_common::testing::crash::{maybe_crash, maybe_fail_io};
use grafeo_common::utils::error::{Error, Result};

/// Subdirectory of the WAL directory that holds spill files.
pub const SPILL_DIR: &str = "txn-spill";

/// Extension of a spill file.
const SPILL_EXTENSION: &str = "spill";

/// Default spill threshold: a group's encoded frames move to disk past 8 MiB.
pub const DEFAULT_SPILL_THRESHOLD: usize = 8 << 20;

/// Default byte cap on a group's encoded size (512 MiB).
pub const DEFAULT_MAX_GROUP_BYTES: u64 = 512 << 20;

/// Size of the spill file's write buffer and of the read buffer used to copy
/// it into the WAL.
const IO_CHUNK: usize = 64 << 10;

/// Bytes a RAM frame carries besides its payload: the `u32` length prefix.
const RAM_FRAME_HEADER: u64 = 4;

/// Distinguishes the spill files of one process.
static SPILL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Size limits of a [`GroupBuffer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupLimits {
    /// Encoded bytes a group may hold in RAM. Past it, the group moves to a
    /// spill file. `usize::MAX` never spills.
    pub spill_threshold: usize,
    /// Cap on a group's total encoded size, in RAM or on disk. A push past it
    /// fails with a retryable [`Error::AdmissionRetryable`]. `u64::MAX` is no
    /// cap.
    pub max_bytes: u64,
}

impl Default for GroupLimits {
    fn default() -> Self {
        Self {
            spill_threshold: DEFAULT_SPILL_THRESHOLD,
            max_bytes: DEFAULT_MAX_GROUP_BYTES,
        }
    }
}

/// A position in a group, for savepoints: the frames before it and their
/// encoded size.
#[derive(Debug, Clone, Copy, Default)]
pub struct GroupPosition {
    frames: u64,
    /// Encoded size of those frames, counting a 4-byte length prefix each.
    bytes: u64,
    /// Taken (by [`GroupBuffer::position`]) while a push was refused: a
    /// truncate to it must not clear that refusal, because the refused write
    /// happened before it. Not part of equality.
    after_failure: bool,
}

impl PartialEq for GroupPosition {
    fn eq(&self, other: &Self) -> bool {
        self.frames == other.frames && self.bytes == other.bytes
    }
}

impl Eq for GroupPosition {}

impl GroupPosition {
    /// Number of frames before this position.
    #[must_use]
    pub fn frames(&self) -> u64 {
        self.frames
    }

    /// Encoded size of the frames before this position.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// Why a group refuses pushes, and up to where its frames are intact.
#[derive(Debug, Clone)]
struct Failure {
    message: String,
    /// Retryable after a rollback (the cap, a failed spill), as opposed to a
    /// record that can never be encoded.
    retryable: bool,
    /// Every frame before this position is intact; a truncate to it or below
    /// clears the failure.
    intact: GroupPosition,
}

/// The encoded frames of one WAL group, in RAM or spilled to a file.
pub struct GroupBuffer {
    spill_dir: PathBuf,
    limits: GroupLimits,
    /// Encrypt the spill file (the WAL is encrypted).
    encrypt: bool,
    /// Frames as `[len: u32 LE][payload]` while the group is not spilled.
    ram: Vec<u8>,
    spill: Option<SpillFile>,
    /// End of the group.
    end: GroupPosition,
    /// Whether a frame requires an fsync at commit (see
    /// [`WalEntry::requires_sync`]).
    requires_sync: bool,
    failure: Option<Failure>,
    /// Reused encoding buffer.
    scratch: Vec<u8>,
    /// Highest [`ram_bytes`](Self::ram_bytes) seen.
    peak_ram: usize,
}

/// A group's spill file.
///
/// Frame format, plaintext: `[len: u32 LE][payload][crc32: u32 LE]`, as a WAL
/// frame. Encrypted: `[len: u32 LE][nonce(12) || ciphertext || tag(16)]`
/// under the file's own key. Either way a frame takes a fixed number of bytes
/// more than in RAM, so a [`GroupPosition`] maps to a file offset.
struct SpillFile {
    path: PathBuf,
    file: File,
    /// Frames not yet written to the file, starting at `written`.
    pending: Vec<u8>,
    /// Every frame before this position is in the file.
    written: GroupPosition,
    #[cfg(feature = "encryption")]
    cipher: Option<SpillCipher>,
}

#[cfg(feature = "encryption")]
struct SpillCipher {
    encryptor: grafeo_common::encryption::PageEncryptor,
    /// Next nonce counter. Never reused, even after a truncate rewrites a
    /// region of the file.
    next_nonce: u64,
}

#[cfg(feature = "encryption")]
const SPILL_AAD: &[u8] = b"grafeo-wal-spill";

impl Drop for SpillFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

impl SpillFile {
    /// Bytes a spilled frame carries besides its RAM form.
    fn extra_per_frame(&self) -> u64 {
        #[cfg(feature = "encryption")]
        if self.cipher.is_some() {
            return grafeo_common::encryption::ENCRYPTION_OVERHEAD as u64;
        }
        4 // CRC
    }

    /// File offset of `position`.
    fn offset(&self, position: GroupPosition) -> u64 {
        position.bytes + position.frames * self.extra_per_frame()
    }

    /// Appends one frame to the write buffer.
    fn push_frame(&mut self, payload: &[u8]) -> Result<()> {
        #[cfg(feature = "encryption")]
        if let Some(cipher) = self.cipher.as_mut() {
            let nonce = grafeo_common::encryption::build_nonce(0, cipher.next_nonce);
            cipher.next_nonce += 1;
            let sealed = cipher.encryptor.encrypt(payload, &nonce, SPILL_AAD)?;
            self.pending
                .extend_from_slice(&frame_len(sealed.len())?.to_le_bytes());
            self.pending.extend_from_slice(&sealed);
            return Ok(());
        }
        self.pending
            .extend_from_slice(&frame_len(payload.len())?.to_le_bytes());
        self.pending.extend_from_slice(payload);
        self.pending
            .extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        Ok(())
    }

    /// Writes the buffered frames to the file; afterwards everything before
    /// `end` is in the file.
    fn write_pending(&mut self, end: GroupPosition) -> Result<()> {
        if self.pending.is_empty() {
            self.written = end;
            return Ok(());
        }
        maybe_fail_io("wal_spill_write")?;
        let offset = self.offset(self.written);
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&self.pending)?;
        maybe_crash("wal_spill_after_write");
        self.pending.clear();
        // A huge frame must not pin its size in the write buffer.
        if self.pending.capacity() > 2 * IO_CHUNK {
            self.pending = Vec::with_capacity(IO_CHUNK + IO_CHUNK / 4);
        }
        self.written = end;
        Ok(())
    }

    /// Calls `sink` with each frame's payload, up to `end`. Returns the
    /// largest decrypted payload it held (0 without encryption).
    fn read_frames(
        &mut self,
        end: GroupPosition,
        payload: &mut Vec<u8>,
        sink: &mut dyn FnMut(&[u8]) -> Result<()>,
    ) -> Result<usize> {
        let mut reader = BufReader::with_capacity(IO_CHUNK, self.file.try_clone()?);
        reader.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; 4];
        // Encoded RAM-form bytes of the frames not read yet: no length prefix
        // may claim more, so a damaged one cannot trigger a huge allocation.
        let mut remaining = end.bytes;
        // A length prefix covers the payload, plus nonce and tag when sealed.
        #[cfg(feature = "encryption")]
        let extra = if self.cipher.is_some() {
            grafeo_common::encryption::ENCRYPTION_OVERHEAD as u64
        } else {
            0
        };
        #[cfg(not(feature = "encryption"))]
        let extra = 0u64;
        #[cfg_attr(not(feature = "encryption"), allow(unused_mut))]
        let mut largest_plain = 0usize;
        for _ in 0..end.frames {
            reader.read_exact(&mut header)?;
            let len = u64::from(u32::from_le_bytes(header));
            let plain_len = len
                .checked_sub(extra)
                .filter(|plain| plain + 4 <= remaining);
            let Some(plain_len) = plain_len else {
                return Err(Error::Internal(format!(
                    "WAL spill file {} is damaged (frame length {len} out of bounds)",
                    self.path.display()
                )));
            };
            remaining -= plain_len + 4;
            // reason: bounded by the group's size, which is in this process's address space
            #[allow(clippy::cast_possible_truncation)]
            payload.resize(len as usize, 0);
            reader.read_exact(payload)?;

            #[cfg(feature = "encryption")]
            if let Some(cipher) = self.cipher.as_ref() {
                let plain = cipher.encryptor.decrypt(payload, SPILL_AAD)?;
                largest_plain = largest_plain.max(plain.len());
                sink(&plain)?;
                continue;
            }
            reader.read_exact(&mut header)?;
            if u32::from_le_bytes(header) != crc32fast::hash(payload) {
                return Err(Error::Internal(format!(
                    "WAL spill file {} is damaged (checksum mismatch)",
                    self.path.display()
                )));
            }
            sink(payload)?;
        }
        Ok(largest_plain)
    }
}

/// A frame's length prefix.
fn frame_len(len: usize) -> Result<u32> {
    u32::try_from(len)
        .map_err(|_| Error::Serialization(format!("WAL record of {len} bytes exceeds 4 GiB")))
}

/// The error of a refused push: retryable when a rollback frees the space
/// (the cap, a failed spill), internal when the record itself is at fault.
fn refusal(message: String, retryable: bool) -> Error {
    if retryable {
        Error::AdmissionRetryable(message)
    } else {
        Error::Internal(message)
    }
}

impl GroupBuffer {
    /// Creates an empty group whose spill file, if it needs one, goes into
    /// `spill_dir` (created on first spill), encrypted when `encrypt` is set.
    #[must_use]
    pub fn new(spill_dir: PathBuf, limits: GroupLimits, encrypt: bool) -> Self {
        Self {
            spill_dir,
            limits,
            encrypt,
            ram: Vec::new(),
            spill: None,
            end: GroupPosition::default(),
            requires_sync: false,
            failure: None,
            scratch: Vec::new(),
            peak_ram: 0,
        }
    }

    /// The group's limits.
    #[must_use]
    pub fn limits(&self) -> GroupLimits {
        self.limits
    }

    /// Encodes `record` and appends it as the group's next frame.
    ///
    /// # Errors
    ///
    /// Returns a retryable [`Error::AdmissionRetryable`] when the frame would
    /// take the group past its byte cap, or the spill file cannot be created
    /// or written (disk full, I/O error), and an error when the record cannot
    /// be encoded. The group then refuses every push, with the same error,
    /// until it is cleared or truncated to a position where its frames are
    /// intact.
    pub fn push<R: WalEntry>(&mut self, record: &R) -> Result<()> {
        if let Some(failure) = &self.failure {
            return Err(refusal(failure.message.clone(), failure.retryable));
        }
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        let result = match bincode::serde::encode_into_std_write(
            record,
            &mut scratch,
            bincode::config::standard(),
        ) {
            Ok(_) => {
                // The encoded record (and, when spilling encrypted, its sealed
                // copy) sits in RAM beside the group while it is appended.
                let sealed = if self.encrypt && self.spill.is_some() {
                    scratch.len() + 28
                } else {
                    0
                };
                let result = self.push_encoded(&scratch);
                self.peak_ram = self
                    .peak_ram
                    .max(self.ram_bytes() + scratch.capacity() + sealed);
                result
            }
            Err(e) => Err(self.fail(
                format!(
                    "a WAL record of the transaction could not be encoded ({e}); \
                     roll the transaction back"
                ),
                self.end,
                false,
            )),
        };
        if result.is_ok() {
            self.requires_sync |= record.requires_sync();
        }
        // A single huge record must not pin its size in RAM for the rest of
        // the group.
        if scratch.capacity() > IO_CHUNK {
            scratch = Vec::new();
        }
        self.scratch = scratch;
        result
    }

    /// Appends one encoded frame.
    fn push_encoded(&mut self, payload: &[u8]) -> Result<()> {
        let Ok(len) = frame_len(payload.len()) else {
            return Err(self.fail(
                format!(
                    "a WAL record of {} bytes exceeds the 4 GiB frame limit; roll the \
                     transaction back",
                    payload.len()
                ),
                self.end,
                false,
            ));
        };
        let frame_bytes = RAM_FRAME_HEADER + payload.len() as u64;
        let new_end = GroupPosition {
            frames: self.end.frames + 1,
            bytes: self.end.bytes + frame_bytes,
            after_failure: false,
        };
        // Charge what the spill file would hold, checksum or encryption
        // overhead included, so the cap bounds the file.
        let charged = new_end.bytes + new_end.frames * self.spill_extra_per_frame();
        if charged > self.limits.max_bytes {
            return Err(self.fail(
                format!(
                    "the transaction's WAL records need at least {charged} bytes, over the \
                     {}-byte transaction WAL buffer cap (Config::wal_transaction_buffer_cap); \
                     nothing of it was written to the WAL. Roll the transaction back and \
                     retry the work in smaller transactions",
                    self.limits.max_bytes
                ),
                self.end,
                true,
            ));
        }
        self.note_ram();

        if self.spill.is_none() {
            // reason: the threshold check only compares sizes that are in RAM already
            #[allow(clippy::cast_possible_truncation)]
            let stays_in_ram = self.ram.len() + frame_bytes as usize <= self.limits.spill_threshold;
            if stays_in_ram {
                // Grow geometrically, but never past the threshold: a doubling
                // would otherwise hold up to twice the threshold.
                // reason: as above, sizes already in RAM
                #[allow(clippy::cast_possible_truncation)]
                let needed = self.ram.len() + frame_bytes as usize;
                if needed > self.ram.capacity() {
                    let target = (self.ram.capacity() * 2)
                        .max(needed)
                        .min(self.limits.spill_threshold);
                    self.ram.reserve_exact(target - self.ram.len());
                }
                self.ram.extend_from_slice(&len.to_le_bytes());
                self.ram.extend_from_slice(payload);
                self.end = new_end;
                self.note_ram();
                return Ok(());
            }
            if let Err(e) = self.start_spill() {
                return Err(self.fail(
                    format!(
                        "could not spill the transaction's WAL records to disk ({e}); \
                         nothing of it was written to the WAL. Roll the transaction back \
                         and retry once there is disk space"
                    ),
                    self.end,
                    true,
                ));
            }
        }

        let spill = self.spill.as_mut().expect("spilled above");
        let result = spill.push_frame(payload).and_then(|()| {
            if spill.pending.len() >= IO_CHUNK {
                spill.write_pending(new_end)
            } else {
                Ok(())
            }
        });
        self.note_ram();
        let end = self.end;
        let spill = self.spill.as_mut().expect("spilled above");
        if result.is_err() {
            // Drop the refused frame's bytes; the frames before it are still
            // in the write buffer (or the file), so the group is intact up to
            // its end and a later write may succeed.
            let keep = spill.offset(end) - spill.offset(spill.written);
            // reason: the pending buffer holds at most a few I/O chunks
            #[allow(clippy::cast_possible_truncation)]
            spill.pending.truncate(keep as usize);
        }
        match result {
            Ok(()) => {
                self.end = new_end;
                Ok(())
            }
            Err(e) => Err(self.fail(
                format!(
                    "could not write the transaction's WAL records to their spill file \
                     ({e}); nothing of it was written to the WAL. Roll the transaction \
                     back and retry once there is disk space"
                ),
                end,
                true,
            )),
        }
    }

    /// Moves the RAM frames to a new spill file.
    fn start_spill(&mut self) -> Result<()> {
        maybe_fail_io("wal_spill_create")?;
        fs::create_dir_all(&self.spill_dir)?;
        let name = format!(
            "txn_{}_{}.{SPILL_EXTENSION}",
            std::process::id(),
            SPILL_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let path = self.spill_dir.join(name);
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let mut spill = SpillFile {
            path,
            file,
            pending: Vec::with_capacity(IO_CHUNK + IO_CHUNK / 4),
            written: GroupPosition::default(),
            #[cfg(feature = "encryption")]
            cipher: self.encrypt.then(|| SpillCipher {
                encryptor: grafeo_common::encryption::PageEncryptor::ephemeral(),
                next_nonce: 0,
            }),
        };
        #[cfg(not(feature = "encryption"))]
        let _ = self.encrypt;

        // Copy the RAM frames over in chunks (the file is deleted on error).
        let mut position = GroupPosition::default();
        let mut offset = 0usize;
        while offset < self.ram.len() {
            let len = u32::from_le_bytes(
                self.ram[offset..offset + 4]
                    .try_into()
                    .expect("4-byte length prefix"),
            ) as usize;
            let payload = &self.ram[offset + 4..offset + 4 + len];
            spill.push_frame(payload)?;
            offset += 4 + len;
            position.frames += 1;
            position.bytes += RAM_FRAME_HEADER + len as u64;
            if spill.pending.len() >= IO_CHUNK {
                spill.write_pending(position)?;
            }
        }
        spill.write_pending(position)?;
        debug_assert_eq!(position, self.end);
        self.spill = Some(spill);
        self.note_ram();
        self.ram = Vec::new();
        Ok(())
    }

    /// Marks the group failed and returns the error for the caller.
    fn fail(&mut self, message: String, intact: GroupPosition, retryable: bool) -> Error {
        self.failure = Some(Failure {
            message: message.clone(),
            retryable,
            intact,
        });
        refusal(message, retryable)
    }

    /// Bytes a spilled frame takes beyond its RAM form: a CRC, or the
    /// encryption nonce and tag.
    fn spill_extra_per_frame(&self) -> u64 {
        #[cfg(feature = "encryption")]
        if self.encrypt {
            return grafeo_common::encryption::ENCRYPTION_OVERHEAD as u64;
        }
        4
    }

    /// The end of the group, as a savepoint position.
    #[must_use]
    pub fn position(&self) -> GroupPosition {
        GroupPosition {
            after_failure: self.failure.is_some(),
            ..self.end
        }
    }

    /// Whether the group holds no frame.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.end.frames == 0
    }

    /// Whether a frame requires an fsync at commit.
    #[must_use]
    pub fn requires_sync(&self) -> bool {
        self.requires_sync
    }

    /// The error a commit of this group must report instead of writing it,
    /// if a push failed.
    #[must_use]
    pub fn failure(&self) -> Option<Error> {
        self.failure
            .as_ref()
            .map(|failure| refusal(failure.message.clone(), failure.retryable))
    }

    /// Drops the frames after `position` (savepoint rollback). Clears a
    /// failure when every frame before `position` is intact.
    pub fn truncate(&mut self, position: GroupPosition) {
        if position.frames < self.end.frames {
            self.truncate_frames(position);
        }
        // A refused push leaves the end where it was, so a savepoint taken
        // right before it sits at the end and still clears it; one taken
        // after it never does (the refused write stays in the transaction).
        if !position.after_failure
            && self
                .failure
                .as_ref()
                .is_some_and(|failure| position.frames <= failure.intact.frames)
        {
            self.failure = None;
        }
    }

    /// Drops the frames after `position`, which is before the end.
    fn truncate_frames(&mut self, position: GroupPosition) {
        if let Some(spill) = self.spill.as_mut() {
            if position.frames >= spill.written.frames {
                let keep = spill.offset(position) - spill.offset(spill.written);
                // reason: the pending buffer holds at most a few I/O chunks
                #[allow(clippy::cast_possible_truncation)]
                spill.pending.truncate(keep as usize);
            } else {
                spill.pending.clear();
                spill.written = position;
                // Only disk usage: reads stop at the group's end, and writes
                // seek to their position.
                let _ = spill.file.set_len(spill.offset(position));
            }
        } else {
            // reason: RAM frames are in a Vec, so their size fits in usize
            #[allow(clippy::cast_possible_truncation)]
            self.ram.truncate(position.bytes as usize);
        }
        self.end = position;
    }

    /// Drops every frame and the spill file, and clears a failure.
    pub fn clear(&mut self) {
        self.spill = None;
        self.ram.clear();
        // A long-lived session keeps at most one I/O chunk between groups.
        if self.ram.capacity() > IO_CHUNK {
            self.ram = Vec::new();
        }
        self.end = GroupPosition::default();
        self.requires_sync = false;
        self.failure = None;
    }

    /// Whether the group has moved to a spill file.
    #[must_use]
    pub fn is_spilled(&self) -> bool {
        self.spill.is_some()
    }

    /// The spill file, if the group has one.
    #[must_use]
    pub fn spill_path(&self) -> Option<&Path> {
        self.spill.as_ref().map(|spill| spill.path.as_path())
    }

    /// Bytes this group holds in RAM right now: the RAM frames, the encoding
    /// buffer and the spill file's write buffer (allocated capacity).
    #[must_use]
    pub fn ram_bytes(&self) -> usize {
        self.ram.capacity()
            + self.scratch.capacity()
            + self
                .spill
                .as_ref()
                .map_or(0, |spill| spill.pending.capacity())
    }

    /// The highest [`ram_bytes`](Self::ram_bytes) since the group was created,
    /// including the copy buffer of a commit.
    #[must_use]
    pub fn peak_ram_bytes(&self) -> usize {
        self.peak_ram.max(self.ram_bytes())
    }

    fn note_ram(&mut self) {
        self.peak_ram = self.peak_ram.max(self.ram_bytes());
    }

    /// Calls `sink` with each frame's payload, in order. For a spilled group
    /// this writes the buffered frames out and reads the file back through a
    /// bounded buffer.
    ///
    /// # Errors
    ///
    /// Returns the first error of `sink`, or an I/O or integrity error of the
    /// spill file.
    pub(crate) fn for_each_frame(
        &mut self,
        sink: &mut dyn FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let end = self.end;
        let Some(spill) = self.spill.as_mut() else {
            let mut offset = 0usize;
            while offset < self.ram.len() {
                let len = u32::from_le_bytes(
                    self.ram[offset..offset + 4]
                        .try_into()
                        .expect("4-byte length prefix"),
                ) as usize;
                sink(&self.ram[offset + 4..offset + 4 + len])?;
                offset += 4 + len;
            }
            return Ok(());
        };
        spill.write_pending(end)?;
        let mut payload = Vec::new();
        let result = spill.read_frames(end, &mut payload, sink);
        // The read buffer, the largest payload and (encrypted) its decrypted
        // copy were in RAM meanwhile.
        let largest_plain = result.as_ref().map_or(0, |plain| *plain);
        let copy_ram = IO_CHUNK + payload.capacity() + largest_plain;
        self.peak_ram = self.peak_ram.max(self.ram_bytes() + copy_ram);
        result.map(|_| ())
    }

    /// Writes a spilled group's buffered frames to its spill file, so that
    /// the commit's copy under the WAL's append lock only reads. Call it
    /// before the commit is applied: a failure here is still a refused push
    /// (retryable, the transaction can roll back), where the same failure
    /// inside the copy would poison the WAL. No-op for a group in RAM.
    ///
    /// # Errors
    ///
    /// Returns the group's failure, or the write's error (which then becomes
    /// the group's failure).
    pub fn prepare_commit(&mut self) -> Result<()> {
        if let Some(error) = self.failure() {
            return Err(error);
        }
        let end = self.end;
        let Some(spill) = self.spill.as_mut() else {
            return Ok(());
        };
        if let Err(e) = spill.write_pending(end) {
            return Err(self.fail(
                format!(
                    "could not write the transaction's WAL records to their spill file \
                     ({e}); nothing of it was written to the WAL. Roll the transaction \
                     back and retry once there is disk space"
                ),
                end,
                true,
            ));
        }
        Ok(())
    }
}

/// Removes the spill files a crashed process left in `wal_dir`, and the spill
/// directory itself when it ends up empty. Returns how many were removed.
///
/// Call it only when opening the database for writing, before any session
/// exists: no live group of this database can exist then. A private
/// `WalManager` opened on a live database's WAL directory must not call it.
///
/// # Errors
///
/// Returns an error if the spill directory cannot be listed or a file cannot
/// be removed.
pub fn remove_leftover_spill_files(wal_dir: &Path) -> Result<usize> {
    let spill_dir = wal_dir.join(SPILL_DIR);
    let entries = match fs::read_dir(&spill_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    let mut removed = 0;
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == SPILL_EXTENSION) {
            fs::remove_file(&path)?;
            removed += 1;
        }
    }
    // Fails if something else is in there: leave it alone.
    let _ = fs::remove_dir(&spill_dir);
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalRecord;
    use grafeo_common::types::{NodeId, TransactionId};

    fn node(id: u64) -> WalRecord {
        WalRecord::CreateNode {
            id: NodeId::new(id),
            labels: vec!["SpillTest".to_string()],
        }
    }

    fn decode_all(group: &mut GroupBuffer) -> Vec<WalRecord> {
        let mut out = Vec::new();
        group
            .for_each_frame(&mut |payload| {
                let (record, _): (WalRecord, usize) =
                    bincode::serde::decode_from_slice(payload, bincode::config::standard())
                        .unwrap();
                out.push(record);
                Ok(())
            })
            .unwrap();
        out
    }

    fn ids(records: &[WalRecord]) -> Vec<u64> {
        records
            .iter()
            .map(|r| match r {
                WalRecord::CreateNode { id, .. } => id.as_u64(),
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }

    fn limits(spill_threshold: usize) -> GroupLimits {
        GroupLimits {
            spill_threshold,
            max_bytes: u64::MAX,
        }
    }

    fn group(dir: &Path, spill_threshold: usize, encrypt: bool) -> GroupBuffer {
        GroupBuffer::new(dir.join(SPILL_DIR), limits(spill_threshold), encrypt)
    }

    #[test]
    fn small_group_stays_in_ram() {
        let dir = tempfile::tempdir().unwrap();
        let mut g = group(dir.path(), 1 << 20, false);
        for i in 0..10 {
            g.push(&node(i)).unwrap();
        }
        assert!(!g.is_spilled());
        assert!(!dir.path().join(SPILL_DIR).exists());
        assert_eq!(ids(&decode_all(&mut g)), (0..10).collect::<Vec<_>>());
    }

    fn spills_and_reads_back(encrypt: bool) {
        let dir = tempfile::tempdir().unwrap();
        let mut g = group(dir.path(), 256, encrypt);
        for i in 0..5000 {
            g.push(&node(i)).unwrap();
        }
        assert!(g.is_spilled());
        let path = g.spill_path().unwrap().to_path_buf();
        assert!(path.starts_with(dir.path().join(SPILL_DIR)));
        assert_eq!(ids(&decode_all(&mut g)), (0..5000).collect::<Vec<_>>());
        // RAM stays bounded by the threshold plus the I/O chunks, not by the
        // group's size.
        assert!(g.position().bytes() > 50_000);
        assert!(
            g.peak_ram_bytes() <= 256 + 4 * IO_CHUNK,
            "peak {}",
            g.peak_ram_bytes()
        );
        g.clear();
        assert!(!path.exists(), "clear deletes the spill file");
    }

    #[test]
    fn spilled_group_reads_back_in_order() {
        spills_and_reads_back(false);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_spill_reads_back_and_holds_no_plaintext() {
        spills_and_reads_back(true);

        let dir = tempfile::tempdir().unwrap();
        let mut g = group(dir.path(), 0, true);
        g.push(&WalRecord::CreateNode {
            id: NodeId::new(1),
            labels: vec!["PlaintextMarkerLabel".to_string()],
        })
        .unwrap();
        let path = g.spill_path().unwrap().to_path_buf();
        // Write the buffered frame out, as a commit would.
        assert_eq!(decode_all(&mut g).len(), 1);
        let bytes = fs::read(&path).unwrap();
        assert!(!bytes.windows(20).any(|w| w == b"PlaintextMarkerLabel"));
    }

    #[test]
    fn truncate_across_the_spill_boundary() {
        truncate_across_the_spill_boundary_with(false);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn encrypted_truncate_across_the_spill_boundary() {
        truncate_across_the_spill_boundary_with(true);
    }

    fn truncate_across_the_spill_boundary_with(encrypt: bool) {
        let dir = tempfile::tempdir().unwrap();
        let mut g = group(dir.path(), 200, encrypt);
        for i in 0..5 {
            g.push(&node(i)).unwrap();
        }
        assert!(!g.is_spilled());
        let before_spill = g.position();
        for i in 5..3000 {
            g.push(&node(i)).unwrap();
        }
        assert!(g.is_spilled());
        let in_spill = g.position();
        for i in 3000..3010 {
            g.push(&node(i)).unwrap();
        }
        // Truncate within the pending write buffer.
        g.truncate(in_spill);
        g.push(&node(77_777)).unwrap();
        let mut expected: Vec<u64> = (0..3000).collect();
        expected.push(77_777);
        assert_eq!(ids(&decode_all(&mut g)), expected);
        // Truncate to a position from before the spill (inside the file).
        g.truncate(before_spill);
        g.push(&node(88_888)).unwrap();
        assert_eq!(ids(&decode_all(&mut g)), vec![0, 1, 2, 3, 4, 88_888]);
    }

    /// A group over `max_bytes` that refuses a large record, in RAM or
    /// spilled.
    fn refusing_group(dir: &Path, spill_threshold: usize) -> GroupBuffer {
        GroupBuffer::new(
            dir.join(SPILL_DIR),
            GroupLimits {
                spill_threshold,
                max_bytes: 2000,
            },
            false,
        )
    }

    fn big_node(id: u64) -> WalRecord {
        WalRecord::CreateNode {
            id: NodeId::new(id),
            labels: vec!["x".repeat(4000)],
        }
    }

    #[test]
    fn only_a_savepoint_before_a_refused_push_clears_it() {
        for spill_threshold in [usize::MAX, 0] {
            let dir = tempfile::tempdir().unwrap();
            let mut g = refusing_group(dir.path(), spill_threshold);
            g.push(&node(1)).unwrap();
            let before = g.position();
            assert!(g.push(&big_node(2)).is_err());
            // Taken after the refusal: rolling back to it keeps the refusal,
            // because the refused write happened before it.
            let after = g.position();
            g.truncate(after);
            assert!(g.failure().is_some(), "threshold {spill_threshold}");
            // Taken before the refusal: rolling back to it clears it.
            g.truncate(before);
            assert!(g.failure().is_none(), "threshold {spill_threshold}");
            g.push(&node(3)).unwrap();
            assert_eq!(ids(&decode_all(&mut g)), vec![1, 3]);
        }
    }

    #[test]
    fn damaged_length_prefix_is_an_error_not_a_huge_allocation() {
        let dir = tempfile::tempdir().unwrap();
        let mut g = group(dir.path(), 0, false);
        for i in 0..10 {
            g.push(&node(i)).unwrap();
        }
        g.prepare_commit().unwrap();
        let path = g.spill_path().unwrap().to_path_buf();
        let mut bytes = fs::read(&path).unwrap();
        bytes[..4].copy_from_slice(&[0xFF; 4]);
        fs::write(&path, &bytes).unwrap();
        let err = g.for_each_frame(&mut |_| Ok(())).unwrap_err();
        assert!(err.to_string().contains("out of bounds"), "{err}");
    }

    #[test]
    fn cap_charges_the_spill_overhead() {
        // 100 tiny frames: the RAM form fits a cap the spill file would not.
        let dir = tempfile::tempdir().unwrap();
        let frame = 4 + bincode::serde::encode_to_vec(&node(1), bincode::config::standard())
            .unwrap()
            .len() as u64;
        let mut g = GroupBuffer::new(
            dir.path().join(SPILL_DIR),
            GroupLimits {
                spill_threshold: 0,
                max_bytes: 100 * frame,
            },
            false,
        );
        let mut pushed = 0;
        while g.push(&node(1)).is_ok() {
            pushed += 1;
        }
        assert!(pushed < 100, "{pushed} frames");
        g.prepare_commit().unwrap_err(); // the refusal stays
        g.truncate(GroupPosition::default());
        g.push(&node(1)).unwrap();
        g.prepare_commit().unwrap();
        let len = fs::metadata(g.spill_path().unwrap()).unwrap().len();
        assert!(len <= 100 * frame);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn peak_counts_a_large_encrypted_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut g = group(dir.path(), 1024, true);
        g.push(&node(0)).unwrap();
        let big = WalRecord::CreateNode {
            id: NodeId::new(1),
            labels: vec!["y".repeat(1 << 20)],
        };
        g.push(&big).unwrap();
        assert!(g.is_spilled());
        assert_eq!(decode_all(&mut g).len(), 2);
        let peak = g.peak_ram_bytes();
        // The encoded record, its sealed copy, the write buffer holding it,
        // and at commit the payload read back and its decrypted copy.
        assert!(peak >= 2 << 20, "peak {peak} misses the record's copies");
        assert!(peak <= 1024 + 4 * IO_CHUNK + 5 * (1 << 20), "peak {peak}");
    }

    #[cfg(feature = "testing-crash-injection")]
    #[test]
    fn prepare_commit_write_failure_is_retryable_and_recoverable() {
        use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
        let dir = tempfile::tempdir().unwrap();
        let mut g = group(dir.path(), 0, false);
        g.push(&node(1)).unwrap();
        let savepoint = g.position();
        enable_io_failure_at(1);
        let err = g.prepare_commit().unwrap_err();
        disable_io_failure();
        assert!(err.error_code().is_retryable(), "{err}");
        // The frames are still buffered: a savepoint at the end recovers.
        g.truncate(savepoint);
        g.prepare_commit().unwrap();
        assert_eq!(ids(&decode_all(&mut g)), vec![1]);
    }

    #[test]
    fn cap_refuses_the_push_and_truncate_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let mut g = GroupBuffer::new(
            dir.path().join(SPILL_DIR),
            GroupLimits {
                spill_threshold: 64,
                max_bytes: 400,
            },
            false,
        );
        let mut pushed = 0;
        let err = loop {
            match g.push(&node(pushed)) {
                Ok(()) => pushed += 1,
                Err(e) => break e,
            }
        };
        assert!(err.error_code().is_retryable());
        assert!(err.to_string().contains("400-byte"), "{err}");
        assert!(g.failure().is_some());
        // Still refused, even a small record.
        assert!(g.push(&node(0)).is_err());
        // Back to a position where every frame is intact: usable again.
        g.truncate(GroupPosition::default());
        assert!(g.failure().is_none());
        g.push(&node(5)).unwrap();
        assert_eq!(ids(&decode_all(&mut g)), vec![5]);
    }

    #[test]
    fn truncate_to_the_end_clears_a_refused_push() {
        // A savepoint taken right before the refused push sits at the end of
        // the group, which the refused push did not move.
        let dir = tempfile::tempdir().unwrap();
        let mut g = GroupBuffer::new(
            dir.path().join(SPILL_DIR),
            GroupLimits {
                spill_threshold: usize::MAX,
                max_bytes: 100,
            },
            false,
        );
        g.push(&node(1)).unwrap();
        let savepoint = g.position();
        let big = WalRecord::CreateNode {
            id: NodeId::new(2),
            labels: vec!["x".repeat(200)],
        };
        assert!(g.push(&big).is_err());
        assert_eq!(g.position(), savepoint);
        g.truncate(savepoint);
        assert!(g.failure().is_none());
        g.push(&node(3)).unwrap();
        assert_eq!(ids(&decode_all(&mut g)), vec![1, 3]);
    }

    #[cfg(feature = "testing-crash-injection")]
    #[test]
    fn spill_io_failure_is_retryable_and_rollback_cleans_up() {
        use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_from};
        let dir = tempfile::tempdir().unwrap();
        let mut g = group(dir.path(), 64, false);
        enable_io_failure_from(1);
        let mut err = None;
        for i in 0..100 {
            if let Err(e) = g.push(&node(i)) {
                err = Some(e);
                break;
            }
        }
        disable_io_failure();
        let err = err.expect("the spill must fail");
        assert!(err.error_code().is_retryable(), "{err}");
        assert!(matches!(err, Error::AdmissionRetryable(_)));
        assert!(g.push(&node(1)).is_err(), "refused until rolled back");
        g.clear();
        g.push(&node(1)).unwrap();
        assert_eq!(ids(&decode_all(&mut g)), vec![1]);
        let leftovers = fs::read_dir(dir.path().join(SPILL_DIR))
            .map(|d| d.count())
            .unwrap_or(0);
        assert_eq!(leftovers, 0);
    }

    #[cfg(feature = "testing-crash-injection")]
    #[test]
    fn failed_spill_write_keeps_earlier_frames_and_a_savepoint_recovers() {
        use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
        let dir = tempfile::tempdir().unwrap();
        let mut g = group(dir.path(), 64, false);
        let mut next = 0;
        while !g.is_spilled() {
            g.push(&node(next)).unwrap();
            next += 1;
        }
        // The next write of the buffered frames to the file fails once.
        enable_io_failure_at(1);
        let savepoint = loop {
            let before = g.position();
            if g.push(&node(next)).is_err() {
                break before;
            }
            next += 1;
        };
        disable_io_failure();
        assert!(g.failure().is_some());
        assert_eq!(
            g.position(),
            savepoint,
            "the refused frame is not in the group"
        );
        // Rolling back to just before the refused push recovers the group;
        // the frames still in the write buffer are written on the next try.
        g.truncate(savepoint);
        assert!(g.failure().is_none());
        g.push(&node(99_999)).unwrap();
        let mut expected: Vec<u64> = (0..next).collect();
        expected.push(99_999);
        assert_eq!(ids(&decode_all(&mut g)), expected);
    }

    #[test]
    fn drop_deletes_the_spill_file_and_leftovers_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let mut g = group(dir.path(), 0, false);
        g.push(&node(1)).unwrap();
        let path = g.spill_path().unwrap().to_path_buf();
        assert!(path.exists());
        drop(g);
        assert!(!path.exists());

        // A crash leaves a file behind; opening for writing removes it.
        let spill_dir = dir.path().join(SPILL_DIR);
        fs::create_dir_all(&spill_dir).unwrap();
        fs::write(spill_dir.join("txn_1_1.spill"), b"junk").unwrap();
        assert_eq!(remove_leftover_spill_files(dir.path()).unwrap(), 1);
        assert!(!spill_dir.exists());
        assert_eq!(remove_leftover_spill_files(dir.path()).unwrap(), 0);
    }

    #[test]
    fn requires_sync_tracks_pushed_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut g = group(dir.path(), 1024, false);
        g.push(&node(1)).unwrap();
        assert!(!g.requires_sync());
        g.push(&WalRecord::TransactionCommit {
            transaction_id: TransactionId::new(1),
        })
        .unwrap();
        assert!(g.requires_sync());
        g.clear();
        assert!(!g.requires_sync());
    }
}
