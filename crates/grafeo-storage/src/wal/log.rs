//! WAL log file management.

use super::WalRecord;
use super::group::GroupBuffer;
use grafeo_common::types::{EpochId, TransactionId};
use grafeo_common::utils::error::{Error, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Checkpoint metadata stored in a separate file.
///
/// This file is written atomically (via rename) during checkpoint and read
/// during recovery to determine which WAL files can be skipped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// The epoch at which the checkpoint was taken.
    pub epoch: EpochId,
    /// The log sequence number at the time of checkpoint.
    pub log_sequence: u64,
    /// Timestamp of the checkpoint (milliseconds since UNIX epoch).
    pub timestamp_ms: u64,
    /// Transaction ID at checkpoint.
    pub transaction_id: TransactionId,
}

/// Name of the checkpoint metadata file.
const CHECKPOINT_METADATA_FILE: &str = "checkpoint.meta";

/// Durability mode for the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    /// Sync (fsync) after every commit for maximum durability.
    /// Slowest but safest.
    Sync,
    /// Batch sync - fsync periodically (e.g., every N ms or N records).
    /// Good balance of performance and durability.
    Batch {
        /// Maximum time between syncs in milliseconds.
        max_delay_ms: u64,
        /// Maximum records between syncs.
        max_records: u64,
    },
    /// Adaptive sync - background thread adjusts timing based on flush duration.
    ///
    /// Unlike `Batch` which checks thresholds inline, `Adaptive` spawns a
    /// dedicated flusher thread that maintains consistent flush cadence
    /// regardless of disk speed. Use [`AdaptiveFlusher`](super::AdaptiveFlusher)
    /// to manage the background thread.
    ///
    /// The WAL itself only buffers writes; the flusher thread handles syncing.
    Adaptive {
        /// Target interval between flushes in milliseconds.
        /// The flusher adjusts wait times to maintain this cadence.
        target_interval_ms: u64,
    },
    /// No sync - rely on OS buffer flushing.
    /// Fastest but may lose recent data on crash.
    NoSync,
}

impl Default for DurabilityMode {
    fn default() -> Self {
        Self::Batch {
            max_delay_ms: 100,
            max_records: 1000,
        }
    }
}

/// Configuration for the WAL manager.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Durability mode.
    pub durability: DurabilityMode,
    /// Maximum log file size before rotation (in bytes).
    pub max_log_size: u64,
    /// Whether to enable compression.
    pub compression: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            durability: DurabilityMode::default(),
            max_log_size: 64 * 1024 * 1024, // 64 MB
            compression: false,
        }
    }
}

/// The frames of one append, produced while the active-log lock is held.
trait FrameSource {
    /// Calls `sink` with each frame, in order.
    fn for_each_frame(&mut self, sink: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()>;
}

/// Frames already in RAM.
struct SliceFrames<'a>(&'a [&'a [u8]]);

impl FrameSource for SliceFrames<'_> {
    fn for_each_frame(&mut self, sink: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        self.0.iter().try_for_each(|frame| sink(frame))
    }
}

/// A buffered group followed by its trailer.
struct GroupFrames<'a> {
    group: &'a mut GroupBuffer,
    trailer: &'a [&'a [u8]],
}

impl FrameSource for GroupFrames<'_> {
    fn for_each_frame(&mut self, sink: &mut dyn FnMut(&[u8]) -> Result<()>) -> Result<()> {
        self.group.for_each_frame(sink)?;
        self.trailer.iter().try_for_each(|frame| sink(frame))
    }
}

/// State for a single log file.
struct LogFile {
    /// File handle.
    writer: BufWriter<File>,
    /// Current size in bytes.
    size: u64,
    /// File path.
    path: PathBuf,
}

/// Manages the Write-Ahead Log with rotation, checkpointing, and durability modes.
pub struct WalManager {
    /// Directory for WAL files.
    dir: PathBuf,
    /// Configuration.
    config: WalConfig,
    /// Active log file.
    active_log: Mutex<Option<LogFile>>,
    /// Total number of records written across all log files.
    total_record_count: AtomicU64,
    /// Records since last sync (for batch mode).
    records_since_sync: AtomicU64,
    /// Time of last sync (for batch mode).
    last_sync: Mutex<Instant>,
    /// Current log sequence number.
    current_sequence: AtomicU64,
    /// Latest checkpoint epoch.
    checkpoint_epoch: Mutex<Option<EpochId>>,
    /// Set by [`poison`](Self::poison): every later append is refused.
    poisoned: Mutex<Option<String>>,
    /// Encryptor for WAL records (None = unencrypted).
    #[cfg(feature = "encryption")]
    encryptor: Option<grafeo_common::encryption::PageEncryptor>,
    /// Test hook: called by [`rotate_if_full`](Self::rotate_if_full) before
    /// it takes the active-log lock, so a test can hold writers that have
    /// all seen the same full file and release them in a chosen order.
    #[cfg(test)]
    rotate_hook: Mutex<Option<RotateHook>>,
}

#[cfg(test)]
type RotateHook = std::sync::Arc<dyn Fn(u64) + Send + Sync>;

impl WalManager {
    /// Opens or creates a WAL in the given directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or accessed.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::with_config(dir, WalConfig::default())
    }

    /// Opens or creates a WAL with custom configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or accessed.
    pub fn with_config(dir: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Find the highest existing sequence number
        let mut max_sequence = 0u64;
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str()
                    && let Some(seq_str) = name
                        .strip_prefix("wal_")
                        .and_then(|s| s.strip_suffix(".log"))
                    && let Ok(seq) = seq_str.parse::<u64>()
                {
                    max_sequence = max_sequence.max(seq);
                }
            }
        }

        let manager = Self {
            dir,
            config,
            active_log: Mutex::new(None),
            total_record_count: AtomicU64::new(0),
            records_since_sync: AtomicU64::new(0),
            last_sync: Mutex::new(Instant::now()),
            current_sequence: AtomicU64::new(max_sequence),
            checkpoint_epoch: Mutex::new(None),
            poisoned: Mutex::new(None),
            #[cfg(feature = "encryption")]
            encryptor: None,
            #[cfg(test)]
            rotate_hook: Mutex::new(None),
        };

        // Open or create the active log
        manager.ensure_active_log()?;

        Ok(manager)
    }

    /// Sets the encryptor for WAL record encryption.
    ///
    /// When set, all written records are encrypted with AES-256-GCM and the
    /// GCM authentication tag replaces the CRC32 checksum. The nonce is derived
    /// from the WAL sequence number.
    #[cfg(feature = "encryption")]
    pub fn set_encryptor(&mut self, encryptor: grafeo_common::encryption::PageEncryptor) {
        self.encryptor = Some(encryptor);
    }

    /// Returns whether encryption is active.
    #[cfg(feature = "encryption")]
    #[must_use]
    pub fn is_encrypted(&self) -> bool {
        self.encryptor.is_some()
    }

    /// Logs a record to the WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if the record cannot be written.
    pub fn log(&self, record: &WalRecord) -> Result<()> {
        let data = bincode::serde::encode_to_vec(record, bincode::config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let force_sync = matches!(record, WalRecord::TransactionCommit { .. });
        self.write_frame(&data, force_sync)
    }

    /// Logs records as one contiguous group: no other writer's records can
    /// land between them.
    ///
    /// # Errors
    ///
    /// Returns an error if a record cannot be serialized or written.
    pub fn log_batch(&self, records: &[WalRecord]) -> Result<()> {
        let frames = records
            .iter()
            .map(|record| {
                bincode::serde::encode_to_vec(record, bincode::config::standard())
                    .map_err(|e| Error::Serialization(e.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let frame_refs: Vec<&[u8]> = frames.iter().map(Vec::as_slice).collect();
        let force_sync = records
            .iter()
            .any(|record| matches!(record, WalRecord::TransactionCommit { .. }));
        self.write_frames(&frame_refs, force_sync)
    }

    /// Writes a pre-serialized frame to the active WAL log.
    ///
    /// Frame format: `[length: u32 LE][data: bytes][crc32: u32 LE]`.
    /// Handles durability mode (sync/batch/adaptive/nosync) and log rotation.
    ///
    /// `force_sync` controls whether an fsync is performed in Sync durability
    /// mode. Callers typically set this to `true` for commit markers.
    pub(crate) fn write_frame(&self, data: &[u8], force_sync: bool) -> Result<()> {
        self.write_frames(&[data], force_sync)
    }

    /// Writes several pre-serialized frames back to back under one hold of
    /// the active-log lock, so no other writer's frame can land between them.
    ///
    /// Used for record pairs that readers require to be adjacent, such as a
    /// `TransactionCommit` and its `EpochAdvance` (generation-root replay
    /// rejects any record between the two). Durability and rotation are
    /// handled once for the whole group, as for a single frame.
    pub(crate) fn write_frames(&self, frames: &[&[u8]], force_sync: bool) -> Result<()> {
        self.write_frames_inner(&mut SliceFrames(frames), force_sync, false)
    }

    /// [`write_frames`](Self::write_frames) for a group whose failure leaves
    /// the log in an unknown state for its writer (a commit marker): any
    /// failure poisons the WAL before the active-log lock is released, so no
    /// other writer can append after the failed group. The fsync, when
    /// needed, runs under that lock too.
    pub(crate) fn write_frames_or_poison(&self, frames: &[&[u8]], force_sync: bool) -> Result<()> {
        self.write_frames_inner(&mut SliceFrames(frames), force_sync, true)
    }

    /// Writes a buffered group, then `trailer` (its commit markers), as one
    /// contiguous run of frames under one hold of the active-log lock, as
    /// [`write_frames`](Self::write_frames) does for frames in RAM.
    ///
    /// A spilled group is copied from its spill file while the lock is held,
    /// through a bounded buffer: other appends wait for the whole copy. With
    /// `poison_on_error`, any failure (including a damaged spill file) poisons
    /// the WAL before the lock is released, as in
    /// [`write_frames_or_poison`](Self::write_frames_or_poison).
    pub(crate) fn write_group(
        &self,
        group: &mut GroupBuffer,
        trailer: &[&[u8]],
        force_sync: bool,
        poison_on_error: bool,
    ) -> Result<()> {
        self.write_frames_inner(
            &mut GroupFrames { group, trailer },
            force_sync,
            poison_on_error,
        )
    }

    fn poisoned_error(reason: &str) -> Error {
        use grafeo_common::utils::write_outcome::UNTIL_REOPENED;
        Error::Internal(format!(
            "WAL refuses appends {UNTIL_REOPENED}: {reason}"
        ))
    }

    fn write_frames_inner(
        &self,
        frames: &mut dyn FrameSource,
        force_sync: bool,
        poison_on_error: bool,
    ) -> Result<()> {
        use grafeo_common::testing::crash::maybe_crash;

        if let Err(e) = self.ensure_active_log() {
            if poison_on_error {
                self.poison(format!("WAL could not be opened for a commit marker: {e}"));
            }
            return Err(e);
        }

        // Phase 1: write frame data and flush buffer while holding the lock.
        // Determine whether an fsync is needed, and if so clone the file handle
        // so we can release the lock before the (potentially slow) sync_all().
        // The poison check runs under the same lock, so no append can slip in
        // after a writer poisoned the log.
        let (needs_rotation, sync_file, synced_records, written_sequence) = {
            let mut guard = self.active_log.lock();
            if let Some(reason) = self.poisoned.lock().as_ref() {
                return Err(Self::poisoned_error(reason));
            }
            let phase1 = (|| -> Result<(bool, Option<File>, u64, u64)> {
                let log_file = guard
                    .as_mut()
                    .ok_or_else(|| Error::Internal("WAL writer not available".to_string()))?;

                // Test hook: fail the whole append before any byte is written.
                grafeo_common::testing::crash::maybe_fail_io("wal_write")?;

                frames.for_each_frame(&mut |data| self.write_one_frame(log_file, data))?;

                let needs_rotation = log_file.size >= self.config.max_log_size;

                // Decide whether we need to fsync based on durability mode.
                // Always flush the BufWriter so data reaches the OS page cache.
                let needs_sync = match &self.config.durability {
                    DurabilityMode::Sync => {
                        if force_sync {
                            maybe_crash("wal_before_flush");
                        }
                        force_sync
                    }
                    DurabilityMode::Batch {
                        max_delay_ms,
                        max_records,
                    } => {
                        let records = self.records_since_sync.load(Ordering::Relaxed);
                        let elapsed = self.last_sync.lock().elapsed();
                        records >= *max_records || elapsed >= Duration::from_millis(*max_delay_ms)
                    }
                    DurabilityMode::Adaptive { .. } | DurabilityMode::NoSync => false,
                };

                // Flush the BufWriter while holding the lock (pushes data to OS).
                log_file.writer.flush()?;

                // Snapshot the record count while holding the lock so we can
                // subtract exactly this amount after sync, preserving any
                // concurrent increments that arrive between lock release and sync.
                let synced_records = if needs_sync {
                    self.records_since_sync.load(Ordering::Relaxed)
                } else {
                    0
                };

                // Clone the file handle for out-of-lock sync if needed.
                let mut sync_file = if needs_sync {
                    Some(log_file.writer.get_ref().try_clone()?)
                } else {
                    None
                };

                // Poison mode: fsync under the lock, so a failure is known before
                // any other writer can append.
                if poison_on_error && let Some(file) = sync_file.take() {
                    file.sync_all()?;
                    self.records_since_sync
                        .fetch_sub(synced_records, Ordering::Relaxed);
                    *self.last_sync.lock() = Instant::now();
                }

                // The sequence only changes under this lock (see `rotate`), so
                // it names the file this group was written to.
                let written_sequence = self.current_sequence.load(Ordering::SeqCst);
                Ok((needs_rotation, sync_file, synced_records, written_sequence))
            })();
            match phase1 {
                Ok(done) => done,
                Err(e) => {
                    if poison_on_error {
                        self.set_poisoned(format!("a commit marker could not be written: {e}"));
                    }
                    return Err(e);
                }
            }
            // guard dropped here: active_log lock released
        };

        // Phase 2: fsync outside the lock so other threads can write concurrently.
        if let Some(file) = sync_file {
            file.sync_all()?;
            self.records_since_sync
                .fetch_sub(synced_records, Ordering::Relaxed);
            *self.last_sync.lock() = Instant::now();
        }

        // Rotate if needed. The check above ran under the lock that has since
        // been released, so another writer may already have rotated this
        // file: rotate_if_full re-checks under the lock.
        if needs_rotation && let Err(e) = self.rotate_if_full(written_sequence) {
            // The group itself is durable by now, but the writer reports this
            // error as an unconfirmed commit marker and says further writes
            // are refused: poison so that holds.
            if poison_on_error {
                self.poison(format!("WAL rotation failed after a commit marker: {e}"));
            }
            return Err(e);
        }

        Ok(())
    }

    /// Writes one frame to `log_file` (encrypted when configured) and counts
    /// it. The caller holds the active-log lock.
    fn write_one_frame(&self, log_file: &mut LogFile, data: &[u8]) -> Result<()> {
        use grafeo_common::testing::crash::maybe_crash;

        maybe_crash("wal_before_write");

        // Encrypt or write plaintext depending on encryption configuration.
        // Encrypted frame: [len:4][nonce(12) || ciphertext || tag(16)]
        // Plaintext frame:  [len:4][data][crc32:4]
        #[cfg(feature = "encryption")]
        let (frame_data, record_size) = if let Some(ref enc) = self.encryptor {
            let file_seq = self.current_sequence.load(Ordering::Relaxed);
            // Use the file byte offset as the nonce counter, not the ephemeral
            // record count. The byte offset survives restarts (file is append-only)
            // and is unique per record within a file. Combined with the file sequence,
            // this guarantees nonce uniqueness even after crash + restart.
            //
            // The nonce high word is 4 bytes, so the file sequence must fit in u32.
            // With one rotation per ~64 MB of WAL, this allows ~256 exabytes of
            // total WAL writes before exhaustion, which is effectively unlimited.
            let seq_u32 = u32::try_from(file_seq).map_err(|_| {
                Error::Internal(
                    "WAL file sequence exceeds u32::MAX: encryption nonce space exhausted"
                        .to_string(),
                )
            })?;
            let byte_offset = log_file.size;
            let nonce = grafeo_common::encryption::build_nonce(seq_u32, byte_offset);
            let aad = b"grafeo-wal";
            let encrypted = enc
                .encrypt(data, &nonce, aad)
                .map_err(|e| Error::Internal(format!("WAL encryption failed: {e}")))?;
            // reason: WAL wire format uses u32 length prefix; individual records are well under 4 GiB
            #[allow(clippy::cast_possible_truncation)]
            let len = encrypted.len() as u32;
            log_file.writer.write_all(&len.to_le_bytes())?;
            log_file.writer.write_all(&encrypted)?;
            let size = 4 + encrypted.len() as u64;
            (true, size)
        } else {
            (false, 0u64)
        };

        #[cfg(feature = "encryption")]
        if !frame_data {
            // reason: WAL wire format uses u32 length prefix; individual records are well under 4 GiB
            #[allow(clippy::cast_possible_truncation)]
            let len = data.len() as u32;
            log_file.writer.write_all(&len.to_le_bytes())?;
            log_file.writer.write_all(data)?;
            let checksum = crc32fast::hash(data);
            log_file.writer.write_all(&checksum.to_le_bytes())?;
        }

        #[cfg(not(feature = "encryption"))]
        {
            // Write length prefix
            // reason: WAL wire format uses u32 length prefix; individual records are well under 4 GiB
            #[allow(clippy::cast_possible_truncation)]
            let len = data.len() as u32;
            log_file.writer.write_all(&len.to_le_bytes())?;

            // Write data
            log_file.writer.write_all(data)?;

            // Write checksum
            let checksum = crc32fast::hash(data);
            log_file.writer.write_all(&checksum.to_le_bytes())?;
        }

        maybe_crash("wal_after_write");

        // Update size tracking
        #[cfg(feature = "encryption")]
        let record_size = if frame_data {
            record_size
        } else {
            4 + data.len() as u64 + 4
        };
        #[cfg(not(feature = "encryption"))]
        let record_size = 4 + data.len() as u64 + 4; // length + data + checksum
        log_file.size += record_size;

        self.total_record_count.fetch_add(1, Ordering::Relaxed);
        self.records_since_sync.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Refuses every later append until this WAL is reopened.
    ///
    /// For a writer that cannot tell what the log holds any more (a commit
    /// marker it could not write): appending more records after it could let
    /// a later commit or abort settle that transaction the wrong way on
    /// replay. The first reason is kept.
    pub fn poison(&self, reason: impl Into<String>) {
        // Taken in the same order as an append (active log, then poison), so
        // an append either completes before this or sees the poison.
        let _active = self.active_log.lock();
        self.set_poisoned(reason);
    }

    /// Sets the poison reason (first one wins). The caller holds the
    /// active-log lock.
    fn set_poisoned(&self, reason: impl Into<String>) {
        let mut poisoned = self.poisoned.lock();
        if poisoned.is_none() {
            *poisoned = Some(reason.into());
        }
    }

    /// Why appends are refused, if [`poison`](Self::poison) was called.
    #[must_use]
    pub fn poisoned_reason(&self) -> Option<String> {
        self.poisoned.lock().clone()
    }

    /// Writes a checkpoint marker and persists checkpoint metadata.
    ///
    /// The checkpoint metadata is written atomically to a separate file,
    /// allowing recovery to skip WAL files that precede the checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint cannot be written.
    pub fn checkpoint(&self, current_transaction: TransactionId, epoch: EpochId) -> Result<()> {
        self.log(&WalRecord::Checkpoint {
            transaction_id: current_transaction,
        })?;
        self.complete_checkpoint(current_transaction, epoch, None)
    }

    /// Completes a checkpoint after the checkpoint record has been written.
    ///
    /// Syncs the WAL, writes checkpoint metadata atomically, updates the
    /// in-memory epoch, and truncates old log files.
    ///
    /// `covered_sequence` caps the log sequence recorded in the metadata.
    /// Recovery skips log files below the recorded sequence, so a caller
    /// that captured the sequence before taking its snapshot passes it here:
    /// records that landed in files rotated out after the capture are then
    /// still replayed.
    pub(crate) fn complete_checkpoint(
        &self,
        transaction_id: TransactionId,
        epoch: EpochId,
        covered_sequence: Option<u64>,
    ) -> Result<()> {
        // Ordering guarantee: fsync all WAL data before writing checkpoint
        // metadata. This ensures that on recovery, any WAL entries referenced
        // by the checkpoint metadata are durable on disk. Without this barrier,
        // a crash between metadata write and WAL sync could cause recovery to
        // skip replaying un-synced WAL records.
        self.sync()?;

        // Get current log sequence
        let current_sequence = self.current_sequence.load(Ordering::SeqCst);
        let log_sequence = covered_sequence.map_or(current_sequence, |s| s.min(current_sequence));

        // Get current timestamp
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            // reason: millis since UNIX epoch fits in u64 for ~585 million years
            .map_or(0, |d| {
                // reason: value is bounded by format constraints
                #[allow(clippy::cast_possible_truncation)]
                let ms = d.as_millis() as u64;
                ms
            });

        // Create checkpoint metadata
        let metadata = CheckpointMetadata {
            epoch,
            log_sequence,
            timestamp_ms,
            transaction_id,
        };

        // Write checkpoint metadata atomically
        self.write_checkpoint_metadata(&metadata)?;

        // Crash window: recovery now skips files below `log_sequence`, but
        // old log files are not truncated yet.
        grafeo_common::testing::crash::maybe_crash("wal_checkpoint:after_metadata");

        // Update in-memory checkpoint epoch
        *self.checkpoint_epoch.lock() = Some(epoch);

        // Optionally truncate old logs
        self.truncate_old_logs(log_sequence)?;

        Ok(())
    }

    /// Writes checkpoint metadata to disk atomically.
    ///
    /// Uses a write-to-temp-then-rename pattern for atomicity.
    fn write_checkpoint_metadata(&self, metadata: &CheckpointMetadata) -> Result<()> {
        let metadata_path = self.dir.join(CHECKPOINT_METADATA_FILE);
        let temp_path = self.dir.join(format!("{}.tmp", CHECKPOINT_METADATA_FILE));

        // Serialize metadata
        let data = bincode::serde::encode_to_vec(metadata, bincode::config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Write to temp file
        let mut file = File::create(&temp_path)?;
        file.write_all(&data)?;
        file.sync_all()?;
        drop(file);

        // Atomic rename
        fs::rename(&temp_path, &metadata_path)?;

        Ok(())
    }

    /// Reads checkpoint metadata from disk.
    ///
    /// Returns `None` if no checkpoint metadata exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata file cannot be read or deserialized.
    pub fn read_checkpoint_metadata(&self) -> Result<Option<CheckpointMetadata>> {
        let metadata_path = self.dir.join(CHECKPOINT_METADATA_FILE);

        if !metadata_path.exists() {
            return Ok(None);
        }

        let file = File::open(&metadata_path)?;
        let mut reader = BufReader::new(file);
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;

        let (metadata, _): (CheckpointMetadata, _) =
            bincode::serde::decode_from_slice(&data, bincode::config::standard())
                .map_err(|e| Error::Serialization(e.to_string()))?;

        Ok(Some(metadata))
    }

    /// Rotates to a new log file.
    ///
    /// The new sequence is allocated, its file opened and the active log
    /// swapped while the active-log lock is held, so the active file only
    /// ever moves to a higher sequence and `current_sequence()` names it
    /// whenever that lock is free.
    ///
    /// # Errors
    ///
    /// Returns an error if rotation fails.
    pub fn rotate(&self) -> Result<()> {
        let mut guard = self.active_log.lock();
        self.rotate_locked(&mut guard)
    }

    /// Size rotation after an append to `written_sequence`: rotates only if
    /// that file is still the active one and still at or over
    /// `max_log_size`. Several writers can see the same file full; the first
    /// rotates it and the others find a fresh file and leave it.
    pub(crate) fn rotate_if_full(&self, written_sequence: u64) -> Result<()> {
        #[cfg(test)]
        {
            let hook = self.rotate_hook.lock().clone();
            if let Some(hook) = hook {
                hook(written_sequence);
            }
        }
        let mut guard = self.active_log.lock();
        let still_full = guard
            .as_ref()
            .is_some_and(|log| log.size >= self.config.max_log_size);
        if still_full && self.current_sequence.load(Ordering::SeqCst) == written_sequence {
            self.rotate_locked(&mut guard)?;
        }
        Ok(())
    }

    /// Rotation body; the caller holds the active-log lock.
    fn rotate_locked(&self, active: &mut Option<LogFile>) -> Result<()> {
        // Make the outgoing file final first: flushed and fsynced. On error
        // nothing has changed and the same file stays active.
        if let Some(old_log) = active.as_mut() {
            old_log.writer.flush()?;
            old_log.writer.get_ref().sync_all()?;
        }

        let new_sequence = self.current_sequence.load(Ordering::SeqCst) + 1;
        let new_path = self.log_path(new_sequence);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&new_path)?;

        self.current_sequence.store(new_sequence, Ordering::SeqCst);
        *active = Some(LogFile {
            writer: BufWriter::new(file),
            size: 0,
            path: new_path,
        });

        Ok(())
    }

    /// Flushes the WAL buffer to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush fails.
    pub fn flush(&self) -> Result<()> {
        let mut guard = self.active_log.lock();
        if let Some(log_file) = guard.as_mut() {
            log_file.writer.flush()?;
        }
        Ok(())
    }

    /// Flush and fsync the active log, returning where it ends: `(sequence,
    /// length in bytes)` of the file that is active **under the append lock**.
    ///
    /// This is the cut a live backup needs. The sequence comes from the
    /// active file itself (which, since [`rotate`](Self::rotate) swaps the
    /// file under the same lock, is also `current_sequence()`). Because the
    /// active log is held while the answer is read, every lower sequence is
    /// already final (rotated out and fsynced), and the length lies on an
    /// append-group boundary: appends write whole groups under the same lock. The fsync happens after the
    /// lock is released, as in [`sync`](Self::sync).
    ///
    /// # Errors
    ///
    /// Returns an error if there is no active log, or the flush/fsync fails.
    pub fn flush_for_cut(&self) -> Result<(u64, u64)> {
        let (sequence, length, sync_file) = {
            let mut guard = self.active_log.lock();
            let Some(log_file) = guard.as_mut() else {
                return Err(grafeo_common::utils::error::Error::Internal(
                    "WAL has no active log file to cut".to_string(),
                ));
            };
            // Same order as an append: a poison set before this point is seen
            // here, so a poisoned log is never cut.
            if let Some(reason) = self.poisoned_reason() {
                return Err(Self::poisoned_error(&reason));
            }
            log_file.writer.flush()?;
            let sequence = Self::sequence_from_path(&log_file.path).ok_or_else(|| {
                grafeo_common::utils::error::Error::Internal(format!(
                    "active WAL file {} has no sequence in its name",
                    log_file.path.display()
                ))
            })?;
            let file = log_file.writer.get_ref();
            (sequence, file.metadata()?.len(), file.try_clone()?)
        };
        sync_file.sync_all()?;
        self.records_since_sync.store(0, Ordering::Relaxed);
        *self.last_sync.lock() = Instant::now();
        Ok((sequence, length))
    }

    /// Syncs the WAL to disk (fsync).
    ///
    /// # Errors
    ///
    /// Returns an error if the sync fails.
    pub fn sync(&self) -> Result<()> {
        // Flush buffer and clone handle while holding the lock, then sync outside.
        let sync_file = {
            let mut guard = self.active_log.lock();
            if let Some(log_file) = guard.as_mut() {
                log_file.writer.flush()?;
                Some(log_file.writer.get_ref().try_clone()?)
            } else {
                None
            }
        };
        if let Some(file) = sync_file {
            file.sync_all()?;
        }
        self.records_since_sync.store(0, Ordering::Relaxed);
        *self.last_sync.lock() = Instant::now();
        Ok(())
    }

    /// Returns the total number of records written.
    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.total_record_count.load(Ordering::Relaxed)
    }

    /// Returns the WAL directory path.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Returns the current WAL log sequence number.
    ///
    /// Each log file has a sequence number embedded in its name
    /// (`wal_XXXXXXXX.log`). This returns the sequence of the active log file:
    /// [`rotate`](Self::rotate) changes it and swaps the file under the same
    /// append lock.
    #[must_use]
    pub fn current_sequence(&self) -> u64 {
        self.current_sequence.load(Ordering::Relaxed)
    }

    /// Returns the current durability mode.
    #[must_use]
    pub fn durability_mode(&self) -> DurabilityMode {
        self.config.durability
    }

    /// Returns all WAL log file paths in sequence order.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL directory cannot be read.
    pub fn log_files(&self) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        for entry in fs::read_dir(&self.dir)?.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "log") {
                files.push(path);
            }
        }

        // Sort by sequence number
        files.sort_by(|a, b| {
            let seq_a = Self::sequence_from_path(a).unwrap_or(0);
            let seq_b = Self::sequence_from_path(b).unwrap_or(0);
            seq_a.cmp(&seq_b)
        });

        Ok(files)
    }

    /// Returns the latest checkpoint epoch, if any.
    #[must_use]
    pub fn checkpoint_epoch(&self) -> Option<EpochId> {
        *self.checkpoint_epoch.lock()
    }

    /// Returns the total size of all WAL files in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        let mut total = 0usize;
        if let Ok(files) = self.log_files() {
            for file in files {
                if let Ok(metadata) = fs::metadata(&file) {
                    // reason: WAL files are capped at max_log_size (default 64 MiB), fits in usize on all targets
                    #[allow(clippy::cast_possible_truncation)]
                    let file_len = metadata.len() as usize;
                    total += file_len;
                }
            }
        }
        // Also include checkpoint metadata file
        let metadata_path = self.dir.join(CHECKPOINT_METADATA_FILE);
        if let Ok(metadata) = fs::metadata(&metadata_path) {
            // reason: checkpoint metadata file is a small fixed-size struct, fits in usize
            #[allow(clippy::cast_possible_truncation)]
            let meta_len = metadata.len() as usize;
            total += meta_len;
        }
        total
    }

    /// Returns the timestamp of the last checkpoint (Unix epoch seconds), if any.
    #[must_use]
    pub fn last_checkpoint_timestamp(&self) -> Option<u64> {
        if let Ok(Some(metadata)) = self.read_checkpoint_metadata() {
            // Convert milliseconds to seconds
            Some(metadata.timestamp_ms / 1000)
        } else {
            None
        }
    }

    /// Closes the active log file, releasing its file handle.
    ///
    /// This allows the WAL directory to be safely removed on Windows,
    /// where open file handles prevent directory deletion. A new log file
    /// will be created automatically on the next write.
    pub fn close_active_log(&self) {
        let mut guard = self.active_log.lock();
        // Dropping the LogFile closes the BufWriter and underlying File
        *guard = None;
    }

    // === Private methods ===

    fn ensure_active_log(&self) -> Result<()> {
        let mut guard = self.active_log.lock();
        if guard.is_none() {
            let sequence = self.current_sequence.load(Ordering::Relaxed);
            let path = self.log_path(sequence);

            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&path)?;

            let size = file.metadata()?.len();

            *guard = Some(LogFile {
                writer: BufWriter::new(file),
                size,
                path,
            });
        }
        Ok(())
    }

    fn log_path(&self, sequence: u64) -> PathBuf {
        self.dir.join(format!("wal_{:08}.log", sequence))
    }

    fn sequence_from_path(path: &Path) -> Option<u64> {
        path.file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.strip_prefix("wal_"))
            .and_then(|s| s.parse().ok())
    }

    fn truncate_old_logs(&self, checkpoint_sequence: u64) -> Result<()> {
        let Some(checkpoint) = *self.checkpoint_epoch.lock() else {
            return Ok(());
        };

        // Keep logs that might still be needed
        // For now, keep the two most recent logs after checkpoint
        let files = self.log_files()?;
        let current_seq = self.current_sequence.load(Ordering::Relaxed);

        for file in files {
            if let Some(seq) = Self::sequence_from_path(&file) {
                // Keep the last 2 log files before current, and every file
                // recovery would still read after this checkpoint
                if seq + 2 < current_seq && seq < checkpoint_sequence {
                    // Only delete if we have a checkpoint after this log
                    if checkpoint.as_u64() > seq {
                        let _ = fs::remove_file(&file);
                    }
                }
            }
        }

        Ok(())
    }
}

// Backward compatibility - single-file API
impl WalManager {
    /// Opens a single WAL file (legacy API).
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened.
    pub fn open_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let dir = path.parent().unwrap_or(Path::new("."));
        let manager = Self::open(dir)?;
        Ok(manager)
    }

    /// Returns the path to the active WAL file.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        let guard = self.active_log.lock();
        guard
            .as_ref()
            .map_or_else(|| self.log_path(0), |l| l.path.clone())
    }
}

/// Truncates the active (highest-sequence) WAL log file in `wal_dir` to
/// `stop_byte_offset` bytes, removing a torn tail after the last committed
/// frame. Verifies the max-sequence file's sequence equals `stop_seq` and
/// that the file is at least `stop_byte_offset` long; both fail closed
/// (never extends, never touches a mismatched file). No-op when the file
/// already ends exactly at `stop_byte_offset`. Syncs after truncation.
///
/// # Errors
///
/// Returns [`Error::Internal`] if the directory has no log files, the
/// max-sequence file is not `stop_seq`, or the file is shorter than
/// `stop_byte_offset`. I/O failures propagate as [`Error::Io`].
pub fn truncate_active_tail(wal_dir: &Path, stop_seq: u64, stop_byte_offset: u64) -> Result<()> {
    // Discover WAL log files the same way WalManager::with_config does:
    // parse `wal_%08u.log` names from the directory listing.
    let mut files: Vec<(u64, PathBuf)> = Vec::new();
    for entry in fs::read_dir(wal_dir)?.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "log") {
            let Some(seq) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.strip_prefix("wal_"))
                .and_then(|s| s.parse().ok())
            else {
                continue;
            };
            files.push((seq, path));
        }
    }

    // Fail closed: a TornTail implies the active file exists.
    let (max_seq, active_path) = files.iter().max_by_key(|(seq, _)| *seq).ok_or_else(|| {
        Error::Internal(format!(
            "truncate_active_tail: no WAL log files in {}",
            wal_dir.display()
        ))
    })?;

    // Fail closed: never truncate a file whose sequence does not match the
    // replay-reported torn tail.
    if *max_seq != stop_seq {
        return Err(Error::Internal(format!(
            "truncate_active_tail: active WAL sequence {max_seq} != torn-tail sequence {stop_seq} in {}",
            wal_dir.display()
        )));
    }

    let current_len = fs::metadata(active_path)?.len();
    if current_len == stop_byte_offset {
        // Clean cut: nothing torn after the last committed frame.
        return Ok(());
    }

    // Fail closed: never extend the file.
    if current_len < stop_byte_offset {
        return Err(Error::Internal(format!(
            "truncate_active_tail: WAL file {} is {current_len} bytes, shorter than stop offset {stop_byte_offset}",
            active_path.display()
        )));
    }

    let file = File::options().write(true).open(active_path)?;
    file.set_len(stop_byte_offset)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use grafeo_common::types::NodeId;
    use tempfile::tempdir;

    /// A commit-marker append that fails poisons the WAL by itself, inside
    /// the failing append, and every later append is refused.
    #[cfg(feature = "testing-crash-injection")]
    #[test]
    fn test_failing_commit_marker_append_poisons_the_wal() {
        use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
        let dir = tempdir().unwrap();
        let wal = WalManager::open(dir.path()).unwrap();

        enable_io_failure_at(1);
        let failed = wal.write_frames_or_poison(&[b"commit".as_slice()], true);
        disable_io_failure();
        assert!(failed.is_err());
        assert!(
            wal.poisoned_reason().is_some(),
            "poisoned by the failing append"
        );

        let record = WalRecord::CreateNode {
            id: NodeId::new(1),
            labels: vec!["Person".to_string()],
        };
        assert!(wal.log(&record).is_err(), "later appends are refused");
        assert_eq!(wal.record_count(), 0);
    }

    /// A plain append that fails does not poison the WAL.
    #[cfg(feature = "testing-crash-injection")]
    #[test]
    fn test_failing_plain_append_does_not_poison() {
        use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
        let dir = tempdir().unwrap();
        let wal = WalManager::open(dir.path()).unwrap();

        enable_io_failure_at(1);
        let failed = wal.write_frames(&[b"data".as_slice()], false);
        disable_io_failure();
        assert!(failed.is_err());
        assert!(wal.poisoned_reason().is_none());
        assert!(wal.write_frames(&[b"data".as_slice()], false).is_ok());
    }

    #[test]
    fn test_wal_write() {
        let dir = tempdir().unwrap();

        let wal = WalManager::open(dir.path()).unwrap();

        let record = WalRecord::CreateNode {
            id: NodeId::new(1),
            labels: vec!["Person".to_string()],
        };

        wal.log(&record).unwrap();
        wal.flush().unwrap();

        assert_eq!(wal.record_count(), 1);
    }

    #[test]
    fn test_wal_rotation() {
        let dir = tempdir().unwrap();

        // Small max size to force rotation
        let config = WalConfig {
            max_log_size: 100,
            ..Default::default()
        };

        let wal = WalManager::with_config(dir.path(), config).unwrap();

        // Write enough records to trigger rotation
        for i in 0..10 {
            let record = WalRecord::CreateNode {
                id: NodeId::new(i),
                labels: vec!["Person".to_string()],
            };
            wal.log(&record).unwrap();
        }

        wal.flush().unwrap();

        // Should have multiple log files
        let files = wal.log_files().unwrap();
        assert!(
            files.len() > 1,
            "Expected multiple log files after rotation"
        );
    }

    #[test]
    fn test_durability_modes() {
        let dir = tempdir().unwrap();

        // Test Sync mode
        let config = WalConfig {
            durability: DurabilityMode::Sync,
            ..Default::default()
        };
        let wal = WalManager::with_config(dir.path().join("sync"), config).unwrap();
        wal.log(&WalRecord::TransactionCommit {
            transaction_id: TransactionId::new(1),
        })
        .unwrap();

        // Test NoSync mode
        let config = WalConfig {
            durability: DurabilityMode::NoSync,
            ..Default::default()
        };
        let wal = WalManager::with_config(dir.path().join("nosync"), config).unwrap();
        wal.log(&WalRecord::CreateNode {
            id: NodeId::new(1),
            labels: vec![],
        })
        .unwrap();

        // Test Batch mode
        let config = WalConfig {
            durability: DurabilityMode::Batch {
                max_delay_ms: 10,
                max_records: 5,
            },
            ..Default::default()
        };
        let wal = WalManager::with_config(dir.path().join("batch"), config).unwrap();
        for i in 0..10 {
            wal.log(&WalRecord::CreateNode {
                id: NodeId::new(i),
                labels: vec![],
            })
            .unwrap();
        }

        // Test Adaptive mode (just buffer flush, no inline sync)
        let config = WalConfig {
            durability: DurabilityMode::Adaptive {
                target_interval_ms: 100,
            },
            ..Default::default()
        };
        let wal = WalManager::with_config(dir.path().join("adaptive"), config).unwrap();
        for i in 0..10 {
            wal.log(&WalRecord::CreateNode {
                id: NodeId::new(i),
                labels: vec![],
            })
            .unwrap();
        }
        // Manually sync since no flusher thread in this test
        wal.sync().unwrap();
    }

    #[test]
    fn test_checkpoint() {
        let dir = tempdir().unwrap();

        let wal = WalManager::open(dir.path()).unwrap();

        // Write some records
        wal.log(&WalRecord::CreateNode {
            id: NodeId::new(1),
            labels: vec!["Test".to_string()],
        })
        .unwrap();

        wal.log(&WalRecord::TransactionCommit {
            transaction_id: TransactionId::new(1),
        })
        .unwrap();

        // Create checkpoint
        wal.checkpoint(TransactionId::new(1), EpochId::new(10))
            .unwrap();

        assert_eq!(wal.checkpoint_epoch(), Some(EpochId::new(10)));
    }

    /// A checkpointer whose sequence capture runs ahead of the file a racing
    /// commit lands in (as `rotate()` allowed before it swapped files under
    /// the append lock) must still keep that file. Covering
    /// `current_sequence() - 1` keeps file S in recovery; covering the raw
    /// value skips it and loses the commit.
    #[test]
    fn checkpoint_covering_previous_sequence_keeps_commit_written_mid_rotation() {
        use crate::wal::WalRecovery;

        for (step_back, expect_recovered) in [(1, true), (0, false)] {
            let dir = tempdir().unwrap();
            let wal = WalManager::open(dir.path()).unwrap();

            // Simulate a sequence bump ahead of the file swap.
            wal.current_sequence.fetch_add(1, Ordering::SeqCst);
            let covered = wal.current_sequence().saturating_sub(step_back);

            // A commit that races with the snapshot still lands in the old file.
            wal.log(&WalRecord::CreateNode {
                id: NodeId::new(7),
                labels: vec![],
            })
            .unwrap();
            wal.log(&WalRecord::TransactionCommit {
                transaction_id: TransactionId::new(7),
            })
            .unwrap();

            wal.log(&WalRecord::Checkpoint {
                transaction_id: TransactionId::new(7),
            })
            .unwrap();
            wal.complete_checkpoint(TransactionId::new(7), EpochId::new(1), Some(covered))
                .unwrap();
            drop(wal);

            let records = WalRecovery::new(dir.path()).recover().unwrap();
            let recovered = records
                .iter()
                .any(|r| matches!(r, WalRecord::CreateNode { id, .. } if *id == NodeId::new(7)));
            assert_eq!(recovered, expect_recovered, "step_back={step_back}");
        }
    }

    // ── H-ADOPT.3 Phase C: truncate_active_tail ──────────────────────────

    /// Parses frames (`[len u32 LE][data][crc32 u32 LE]`) sequentially and
    /// returns the end offset of every fully valid frame.
    fn parse_frame_end_offsets(bytes: &[u8]) -> Vec<u64> {
        let mut ends = Vec::new();
        let mut pos = 0usize;
        while pos + 4 <= bytes.len() {
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            let end = pos + 4 + len + 4;
            if end > bytes.len() {
                break;
            }
            let data = &bytes[pos + 4..pos + 4 + len];
            let stored = u32::from_le_bytes(bytes[pos + 4 + len..end].try_into().unwrap());
            if crc32fast::hash(data) != stored {
                break;
            }
            ends.push(end as u64);
            pos = end;
        }
        ends
    }

    /// Writes committed frames, syncs, drops the manager, and returns the
    /// active log path, its sequence, and its byte length.
    fn committed_wal_fixture(dir: &std::path::Path, records: u64) -> (PathBuf, u64, u64) {
        let wal = WalManager::open(dir).unwrap();
        for i in 0..records {
            wal.log(&WalRecord::CreateNode {
                id: NodeId::new(i),
                labels: vec!["Person".to_string()],
            })
            .unwrap();
        }
        wal.log(&WalRecord::TransactionCommit {
            transaction_id: TransactionId::new(1),
        })
        .unwrap();
        wal.sync().unwrap();
        let path = wal.path();
        let seq = wal.current_sequence();
        let len = fs::metadata(&path).unwrap().len();
        drop(wal);
        (path, seq, len)
    }

    fn append_garbage(path: &Path, count: usize) {
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(&vec![0xABu8; count]).unwrap();
        file.sync_all().unwrap();
    }

    #[test]
    fn truncate_active_tail_removes_torn_bytes_after_stop() {
        let dir = tempdir().unwrap();
        let (active_path, seq, stop_offset) = committed_wal_fixture(dir.path(), 3);

        // Simulate a torn tail: garbage appended after the last committed frame.
        append_garbage(&active_path, 37);
        assert_eq!(fs::metadata(&active_path).unwrap().len(), stop_offset + 37);

        truncate_active_tail(dir.path(), seq, stop_offset).unwrap();

        assert_eq!(fs::metadata(&active_path).unwrap().len(), stop_offset);
        let bytes = fs::read(&active_path).unwrap();
        let ends = parse_frame_end_offsets(&bytes);
        assert_eq!(ends.len(), 4, "all committed frames survive truncation");
        assert_eq!(
            *ends.last().unwrap(),
            stop_offset,
            "file ends exactly at the last committed frame"
        );
    }

    #[test]
    fn truncate_active_tail_is_byte_identical_noop_at_eof() {
        let dir = tempdir().unwrap();
        let (active_path, seq, len) = committed_wal_fixture(dir.path(), 2);

        let before = fs::read(&active_path).unwrap();
        assert_eq!(before.len() as u64, len);

        truncate_active_tail(dir.path(), seq, len).unwrap();

        assert_eq!(fs::read(&active_path).unwrap(), before);
    }

    #[test]
    fn truncate_active_tail_rejects_sequence_mismatch() {
        let dir = tempdir().unwrap();
        let (active_path, seq, stop_offset) = committed_wal_fixture(dir.path(), 2);
        append_garbage(&active_path, 16);
        let before = fs::read(&active_path).unwrap();

        let err = truncate_active_tail(dir.path(), seq + 1, stop_offset).unwrap_err();
        assert!(matches!(err, Error::Internal(_)));
        assert_eq!(
            fs::read(&active_path).unwrap(),
            before,
            "mismatched sequence must leave the file untouched"
        );
    }

    #[test]
    fn truncate_active_tail_rejects_offset_beyond_eof() {
        let dir = tempdir().unwrap();
        let (active_path, seq, len) = committed_wal_fixture(dir.path(), 2);
        let before = fs::read(&active_path).unwrap();

        let err = truncate_active_tail(dir.path(), seq, len + 1).unwrap_err();
        assert!(matches!(err, Error::Internal(_)));
        assert_eq!(
            fs::read(&active_path).unwrap(),
            before,
            "offset beyond EOF must never extend the file"
        );
    }

    #[test]
    fn truncate_active_tail_errors_when_no_log_files() {
        let dir = tempdir().unwrap();
        // Empty directory: no log files exist (fail closed).
        assert!(truncate_active_tail(dir.path(), 0, 0).is_err());
        // Missing directory entirely (fail closed).
        assert!(truncate_active_tail(&dir.path().join("missing"), 0, 0).is_err());
    }

    #[test]
    fn flush_for_cut_reports_the_active_file_not_the_bumped_sequence() {
        let dir = tempdir().unwrap();
        let wal = WalManager::open(dir.path()).unwrap();
        let record = WalRecord::CreateNode {
            id: NodeId::new(1),
            labels: vec!["Person".to_string()],
        };
        wal.log(&record).unwrap();
        let (seq, len) = wal.flush_for_cut().unwrap();
        let path = dir.path().join(format!("wal_{seq:08}.log"));
        assert_eq!(len, fs::metadata(&path).unwrap().len());
        assert!(len > 0, "the logged record is inside the cut");

        // After a rotation the cut names the new active file, empty.
        wal.rotate().unwrap();
        let (seq2, len2) = wal.flush_for_cut().unwrap();
        assert_eq!(seq2, seq + 1);
        assert_eq!(len2, 0);
        assert_eq!(seq2, wal.current_sequence());
    }

    /// Decodes every frame of `wal_*.log` files in sequence order as
    /// `(sequence, record)`.
    fn read_all_frames(dir: &Path) -> Vec<(u64, WalRecord)> {
        let mut files: Vec<(u64, PathBuf)> = fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| {
                let path = e.ok()?.path();
                Some((WalManager::sequence_from_path(&path)?, path))
            })
            .collect();
        files.sort_by_key(|(seq, _)| *seq);
        let mut out = Vec::new();
        for (seq, path) in files {
            let bytes = fs::read(&path).unwrap();
            let mut at = 0usize;
            while at < bytes.len() {
                let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
                let body = &bytes[at + 4..at + 4 + len];
                let (record, _): (WalRecord, _) =
                    bincode::serde::decode_from_slice(body, bincode::config::standard()).unwrap();
                out.push((seq, record));
                at += 4 + len + 4;
            }
        }
        out
    }

    #[test]
    fn poisoned_append_is_classified_wal_poisoned() {
        let dir = tempdir().unwrap();
        let wal = WalManager::open(dir.path()).unwrap();
        wal.poison("test: injected");
        let err = wal
            .log(&WalRecord::CreateNode {
                id: NodeId::new(1),
                labels: vec![],
            })
            .expect_err("a poisoned WAL refuses appends");
        assert_eq!(
            err.write_outcome(),
            Some(grafeo_common::utils::WriteOutcome::WalPoisoned),
            "{err}"
        );
    }

    #[test]
    fn concurrent_size_rotations_keep_files_in_append_order() {
        // Many writers cross max_log_size at once, so several of them see the
        // same full file and ask for a rotation. The active file must only
        // ever move to a higher sequence: reading files in sequence order
        // must give every writer's records in the order it appended them,
        // and the newest file must be the one still being written.
        const WRITERS: u64 = 8;
        const RECORDS: u64 = 400;
        for _round in 0..5 {
            let dir = tempdir().unwrap();
            let wal = WalManager::with_config(
                dir.path(),
                WalConfig {
                    durability: DurabilityMode::NoSync,
                    max_log_size: 96,
                    ..WalConfig::default()
                },
            )
            .unwrap();
            let barrier = std::sync::Barrier::new(usize::try_from(WRITERS).unwrap());
            std::thread::scope(|scope| {
                for writer in 0..WRITERS {
                    let (wal, barrier) = (&wal, &barrier);
                    scope.spawn(move || {
                        barrier.wait();
                        for i in 0..RECORDS {
                            wal.log(&WalRecord::CreateNode {
                                id: NodeId::new(writer * 1_000_000 + i),
                                labels: vec![],
                            })
                            .unwrap();
                        }
                    });
                }
            });
            // A last record lands in the active file, which must be the newest.
            wal.log(&WalRecord::CreateNode {
                id: NodeId::new(u64::MAX - 1),
                labels: vec![],
            })
            .unwrap();
            wal.flush().unwrap();
            let max_file_seq = wal
                .log_files()
                .unwrap()
                .iter()
                .filter_map(|p| WalManager::sequence_from_path(p))
                .max()
                .unwrap();
            assert_eq!(
                wal.current_sequence(),
                max_file_seq,
                "current_sequence names the newest file"
            );

            let frames = read_all_frames(dir.path());
            assert_eq!(frames.len() as u64, WRITERS * RECORDS + 1);
            let (last_seq, last) = frames.last().unwrap();
            assert!(
                matches!(last, WalRecord::CreateNode { id, .. } if id.as_u64() == u64::MAX - 1),
                "the record written last is read last"
            );
            // Nothing was written to a file newer than the one that took the
            // last record (a file after it, if any, is the fresh one its
            // own size rotation opened).
            let newest_written = frames.iter().map(|(seq, _)| *seq).max().unwrap();
            assert_eq!(*last_seq, newest_written, "the active file is the newest");
            let mut next = vec![0u64; usize::try_from(WRITERS).unwrap()];
            for (seq, record) in &frames[..frames.len() - 1] {
                let WalRecord::CreateNode { id, .. } = record else {
                    panic!("unexpected record {record:?}");
                };
                let (writer, i) = (id.as_u64() / 1_000_000, id.as_u64() % 1_000_000);
                let slot = usize::try_from(writer).unwrap();
                assert_eq!(i, next[slot], "writer {writer} out of order in file {seq}");
                next[slot] += 1;
            }
        }
    }

    #[test]
    fn a_size_rotation_request_for_a_file_already_rotated_is_a_no_op() {
        // Two writers can both see file N full; only the first rotation may
        // happen, the second finds a fresh file and leaves it active.
        let dir = tempdir().unwrap();
        let wal = WalManager::with_config(
            dir.path(),
            WalConfig {
                max_log_size: 8,
                ..WalConfig::default()
            },
        )
        .unwrap();
        let seq0 = wal.current_sequence();
        wal.log(&WalRecord::CreateNode {
            id: NodeId::new(1),
            labels: vec!["Person".to_string()],
        })
        .unwrap();
        assert_eq!(wal.current_sequence(), seq0 + 1, "the full file rotated");
        // A stale request for the file that is already rotated out.
        wal.rotate_if_full(seq0).unwrap();
        assert_eq!(wal.current_sequence(), seq0 + 1, "no second rotation");
        let (cut_seq, _) = wal.flush_for_cut().unwrap();
        assert_eq!(cut_seq, seq0 + 1);
    }

    #[test]
    fn writers_that_saw_the_same_full_file_rotate_it_once_in_any_release_order() {
        // Forced schedule: writer A then writer B each append to file N and
        // see it full, and both are held before rotating. B (the later one)
        // is released first, a third append arrives, then A is released.
        // A's stale request must not rotate, the active file must be the
        // newest, and files read in sequence order must give append order.
        use std::collections::HashMap;
        use std::sync::mpsc;

        let dir = tempdir().unwrap();
        let wal = std::sync::Arc::new(
            WalManager::with_config(
                dir.path(),
                WalConfig {
                    max_log_size: 8,
                    ..WalConfig::default()
                },
            )
            .unwrap(),
        );
        let seq_n = wal.current_sequence();

        let (arrived_tx, arrived_rx) = mpsc::channel::<(String, u64)>();
        let gates: std::sync::Arc<parking_lot::Mutex<HashMap<String, mpsc::Receiver<()>>>> =
            std::sync::Arc::default();
        let mut release = HashMap::new();
        for name in ["A", "B"] {
            let (tx, rx) = mpsc::channel::<()>();
            gates.lock().insert(name.to_string(), rx);
            release.insert(name, tx);
        }
        {
            let gates = std::sync::Arc::clone(&gates);
            let arrived_tx = parking_lot::Mutex::new(arrived_tx);
            *wal.rotate_hook.lock() = Some(std::sync::Arc::new(move |seq| {
                let name = std::thread::current().name().unwrap_or("").to_string();
                let Some(gate) = gates.lock().remove(&name) else {
                    return;
                };
                arrived_tx.lock().send((name, seq)).unwrap();
                gate.recv().unwrap();
            }));
        }

        let spawn = |name: &'static str, id: u64| {
            let wal = std::sync::Arc::clone(&wal);
            std::thread::Builder::new()
                .name(name.to_string())
                .spawn(move || {
                    wal.log(&WalRecord::CreateNode {
                        id: NodeId::new(id),
                        labels: vec![],
                    })
                    .unwrap();
                })
                .unwrap()
        };
        let a = spawn("A", 1);
        assert_eq!(arrived_rx.recv().unwrap(), ("A".to_string(), seq_n));
        let b = spawn("B", 2);
        assert_eq!(
            arrived_rx.recv().unwrap(),
            ("B".to_string(), seq_n),
            "B appended to the same full file A is about to rotate"
        );

        release["B"].send(()).unwrap();
        b.join().unwrap();
        // Another append arrives between B's rotation and A's: it lands in
        // the file B opened (and, being full, rotates it in turn).
        wal.log(&WalRecord::CreateNode {
            id: NodeId::new(4),
            labels: vec![],
        })
        .unwrap();
        release["A"].send(()).unwrap();
        a.join().unwrap();

        // A's request was for file N, which B already rotated: no-op. Only
        // B's rotation and the third append's rotation happened.
        assert_eq!(wal.current_sequence(), seq_n + 2, "A did not rotate again");
        let newest_on_disk = wal
            .log_files()
            .unwrap()
            .iter()
            .filter_map(|p| WalManager::sequence_from_path(p))
            .max()
            .unwrap();
        let (cut_seq, _) = wal.flush_for_cut().unwrap();
        assert_eq!(cut_seq, newest_on_disk, "the active file is the newest");

        // A later append lands in the newest file, and reading files in
        // sequence order gives every record in append order.
        wal.log(&WalRecord::CreateNode {
            id: NodeId::new(3),
            labels: vec![],
        })
        .unwrap();
        wal.flush().unwrap();
        let placed: Vec<(u64, u64)> = read_all_frames(dir.path())
            .iter()
            .map(|(seq, r)| match r {
                WalRecord::CreateNode { id, .. } => (*seq, id.as_u64()),
                other => panic!("unexpected record {other:?}"),
            })
            .collect();
        assert_eq!(
            placed,
            vec![(seq_n, 1), (seq_n, 2), (seq_n + 1, 4), (seq_n + 2, 3)]
        );
    }
}
