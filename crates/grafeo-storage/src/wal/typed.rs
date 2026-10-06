//! Type-safe WAL wrapper.
//!
//! [`TypedWal`] wraps a [`WalManager`] and ensures that only records of type `R`
//! can be written. This prevents accidentally mixing record types (e.g., LPG
//! and RDF) in the same WAL instance.
//!
//! Use [`LpgWal`] for the standard labeled property graph WAL.

use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use grafeo_common::types::EpochId;
use grafeo_common::types::TransactionId;
use grafeo_common::utils::error::{Error, Result};

use super::WalRecord;
use super::group::{GroupBuffer, GroupLimits, SPILL_DIR};
use super::log::{CheckpointMetadata, DurabilityMode, WalConfig, WalManager};
use super::record::WalEntry;

/// A type-safe wrapper around [`WalManager`] that constrains record types
/// at compile time.
///
/// `TypedWal<R>` ensures that only records implementing [`WalEntry`] with
/// the specific type `R` can be logged. This prevents accidentally writing
/// the wrong record type to a WAL instance.
///
/// # Example
///
/// ```no_run
/// use grafeo_storage::wal::{LpgWal, WalRecord};
/// use grafeo_common::types::NodeId;
///
/// # fn main() -> grafeo_common::utils::error::Result<()> {
/// let wal = LpgWal::open("wal_dir")?;
/// wal.log(&WalRecord::CreateNode {
///     id: NodeId::new(1),
///     labels: vec!["Person".to_string()],
/// })?;
/// # Ok(())
/// # }
/// ```
pub struct TypedWal<R: WalEntry> {
    manager: WalManager,
    _record: PhantomData<R>,
}

impl<R: WalEntry> TypedWal<R> {
    /// Opens or creates a typed WAL in the given directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or accessed.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            manager: WalManager::open(dir)?,
            _record: PhantomData,
        })
    }

    /// Opens or creates a typed WAL with custom configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or accessed.
    pub fn with_config(dir: impl AsRef<Path>, config: WalConfig) -> Result<Self> {
        Ok(Self {
            manager: WalManager::with_config(dir, config)?,
            _record: PhantomData,
        })
    }

    /// Logs a typed record to the WAL.
    ///
    /// The record is serialized via bincode and written with a length prefix
    /// and CRC32 checksum. Durability handling (fsync) is determined by the
    /// record's [`WalEntry::requires_sync`] method.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or writing fails.
    pub fn log(&self, record: &R) -> Result<()> {
        let data = bincode::serde::encode_to_vec(record, bincode::config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;
        let force_sync = record.requires_sync();
        self.manager.write_frame(&data, force_sync)
    }

    /// Refuses every later append until this WAL is reopened. See
    /// [`WalManager::poison`].
    pub fn poison(&self, reason: impl Into<String>) {
        self.manager.poison(reason);
    }

    /// Why appends are refused, if the WAL was poisoned.
    #[must_use]
    pub fn poisoned_reason(&self) -> Option<String> {
        self.manager.poisoned_reason()
    }

    /// [`log_atomic`](Self::log_atomic) for a commit marker: any failure
    /// (including the fsync) poisons the WAL before another writer can
    /// append. See [`WalManager::write_frames_or_poison`].
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or writing fails, or the WAL is
    /// already poisoned.
    pub fn log_atomic_or_poison(&self, records: &[R]) -> Result<()> {
        let mut encoded = Vec::with_capacity(records.len());
        let mut force_sync = false;
        for record in records {
            encoded.push(
                bincode::serde::encode_to_vec(record, bincode::config::standard())
                    .map_err(|e| Error::Serialization(e.to_string()))?,
            );
            force_sync |= record.requires_sync();
        }
        let frames: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
        self.manager.write_frames_or_poison(&frames, force_sync)
    }

    /// Encodes `record` as the payload of one WAL frame, the same encoding
    /// [`log`](Self::log) writes. Lets a caller hold encoded frames instead
    /// of records and write them later with
    /// [`log_encoded_or_poison`](Self::log_encoded_or_poison).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn encode(record: &R) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(record, bincode::config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))
    }

    /// [`log_atomic_or_poison`](Self::log_atomic_or_poison) for payloads
    /// already encoded with [`encode`](Self::encode): written as adjacent
    /// frames under one hold of the append lock, and any write failure
    /// poisons the WAL. `force_sync` fsyncs in sync durability mode.
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails or the WAL is already poisoned.
    pub fn log_encoded_or_poison(&self, frames: &[&[u8]], force_sync: bool) -> Result<()> {
        self.manager.write_frames_or_poison(frames, force_sync)
    }

    /// Logs several records as adjacent frames: no other writer's record can
    /// land between them. Fsyncs (in sync durability mode) when any of them
    /// [requires it](WalEntry::requires_sync).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or writing fails; nothing is written
    /// when serialization fails.
    pub fn log_atomic(&self, records: &[R]) -> Result<()> {
        let mut encoded = Vec::with_capacity(records.len());
        let mut force_sync = false;
        for record in records {
            encoded.push(
                bincode::serde::encode_to_vec(record, bincode::config::standard())
                    .map_err(|e| Error::Serialization(e.to_string()))?,
            );
            force_sync |= record.requires_sync();
        }
        let frames: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
        self.manager.write_frames(&frames, force_sync)
    }

    /// Logs typed records as one contiguous group.
    ///
    /// No other writer's records can land between them, and the group is
    /// synced once if any record requires it (see [`WalEntry::requires_sync`]).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or writing fails.
    pub fn log_batch(&self, records: &[R]) -> Result<()> {
        let frames = records
            .iter()
            .map(|record| {
                bincode::serde::encode_to_vec(record, bincode::config::standard())
                    .map_err(|e| Error::Serialization(e.to_string()))
            })
            .collect::<Result<Vec<_>>>()?;
        let frame_refs: Vec<&[u8]> = frames.iter().map(Vec::as_slice).collect();
        let force_sync = records.iter().any(WalEntry::requires_sync);
        self.manager.write_frames(&frame_refs, force_sync)
    }

    /// Creates an empty group for this WAL: its spill file, if it needs one,
    /// goes into the WAL directory's spill subdirectory, encrypted when this
    /// WAL is.
    #[must_use]
    pub fn new_group(&self, limits: GroupLimits) -> GroupBuffer {
        self.new_group_with(limits, false)
    }

    /// [`new_group`](Self::new_group), also encrypting the spill file when
    /// `encrypt_spill` is set although this WAL is not encrypted (the caller
    /// was configured for encryption at rest). Encryption needs the
    /// `encryption` feature; without it the flag is ignored.
    #[must_use]
    pub fn new_group_with(&self, limits: GroupLimits, encrypt_spill: bool) -> GroupBuffer {
        #[cfg(feature = "encryption")]
        let encrypt = encrypt_spill || self.manager.is_encrypted();
        #[cfg(not(feature = "encryption"))]
        let encrypt = {
            let _ = encrypt_spill;
            false
        };
        GroupBuffer::new(self.manager.dir().join(SPILL_DIR), limits, encrypt)
    }

    /// Writes `group`'s records followed by `trailer` (its commit markers) as
    /// one contiguous run: no other writer's record can land between them,
    /// and the trailer comes last, so recovery commits the group only if all
    /// of it reached the log. A spilled group is copied from its spill file
    /// under the WAL's append lock (see [`GroupBuffer`]). The group is empty
    /// afterwards, its spill file deleted, whatever the outcome.
    ///
    /// With `poison_on_error`, any failure poisons the WAL before another
    /// writer can append (see [`log_atomic_or_poison`](Self::log_atomic_or_poison)).
    ///
    /// # Errors
    ///
    /// Returns the group's own error, writing nothing, if a push to it failed
    /// (see [`GroupBuffer::push`]); otherwise an error if serialization,
    /// reading the spill file or writing fails, or the WAL is poisoned.
    pub fn log_group(
        &self,
        group: &mut GroupBuffer,
        trailer: &[R],
        poison_on_error: bool,
    ) -> Result<()> {
        let result = (|| {
            if let Some(error) = group.failure() {
                return Err(error);
            }
            let mut encoded = Vec::with_capacity(trailer.len());
            let mut force_sync = group.requires_sync();
            for record in trailer {
                encoded.push(
                    bincode::serde::encode_to_vec(record, bincode::config::standard())
                        .map_err(|e| Error::Serialization(e.to_string()))?,
                );
                force_sync |= record.requires_sync();
            }
            if group.is_empty() && encoded.is_empty() {
                return Ok(());
            }
            let frames: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
            self.manager
                .write_group(group, &frames, force_sync, poison_on_error)
        })();
        group.clear();
        result
    }

    /// Writes a checkpoint marker and persists checkpoint metadata.
    ///
    /// Creates a checkpoint record via [`WalEntry::make_checkpoint`], logs it,
    /// then syncs and writes the checkpoint metadata file.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint cannot be written.
    pub fn checkpoint(&self, current_transaction: TransactionId, epoch: EpochId) -> Result<()> {
        let checkpoint_record = R::make_checkpoint(current_transaction);
        self.log(&checkpoint_record)?;
        self.manager
            .complete_checkpoint(current_transaction, epoch, None)
    }

    /// Like [`checkpoint`](Self::checkpoint), for a snapshot taken when the
    /// WAL was at log sequence `covered_sequence`, read before the snapshot.
    /// Recovery will still replay every log file from that sequence on, so
    /// records written while the snapshot was being taken are not skipped
    /// even if the WAL rotated meanwhile.
    ///
    /// Callers pass `current_sequence().saturating_sub(1)`: one file of
    /// margin before the sequence read. Rotation now swaps files under the
    /// append lock, so [`current_sequence`](Self::current_sequence) names
    /// the active file; the margin costs at most one extra replayed file.
    ///
    /// Call this only after the snapshot is durable.
    ///
    /// # Errors
    ///
    /// Returns an error if the checkpoint cannot be written.
    pub fn checkpoint_covering(
        &self,
        current_transaction: TransactionId,
        epoch: EpochId,
        covered_sequence: u64,
    ) -> Result<()> {
        let checkpoint_record = R::make_checkpoint(current_transaction);
        self.log(&checkpoint_record)?;
        self.manager
            .complete_checkpoint(current_transaction, epoch, Some(covered_sequence))
    }

    /// Syncs the WAL to disk (fsync).
    ///
    /// # Errors
    ///
    /// Returns an error if the sync fails.
    pub fn sync(&self) -> Result<()> {
        self.manager.sync()
    }

    /// Flushes the WAL buffer to disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush fails.
    pub fn flush(&self) -> Result<()> {
        self.manager.flush()
    }

    /// Rotates to a new log file.
    ///
    /// # Errors
    ///
    /// Returns an error if rotation fails.
    pub fn rotate(&self) -> Result<()> {
        self.manager.rotate()
    }

    /// Closes the active log file, releasing its file handle.
    ///
    /// This allows the WAL directory to be safely removed on Windows,
    /// where open file handles prevent directory deletion.
    pub fn close_active_log(&self) {
        self.manager.close_active_log();
    }

    /// Returns the underlying [`WalManager`].
    ///
    /// Useful for accessing administrative methods or for passing to
    /// [`AdaptiveFlusher`](super::AdaptiveFlusher).
    #[must_use]
    pub fn manager(&self) -> &WalManager {
        &self.manager
    }

    /// Returns the total number of records written.
    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.manager.record_count()
    }

    /// Returns the WAL directory path.
    #[must_use]
    pub fn dir(&self) -> &Path {
        self.manager.dir()
    }

    /// Returns the current durability mode.
    #[must_use]
    pub fn durability_mode(&self) -> DurabilityMode {
        self.manager.durability_mode()
    }

    /// Returns the total size of all WAL files in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.manager.size_bytes()
    }

    /// Returns the timestamp of the last checkpoint (Unix epoch seconds), if any.
    #[must_use]
    pub fn last_checkpoint_timestamp(&self) -> Option<u64> {
        self.manager.last_checkpoint_timestamp()
    }

    /// Returns the latest checkpoint epoch, if any.
    #[must_use]
    pub fn checkpoint_epoch(&self) -> Option<EpochId> {
        self.manager.checkpoint_epoch()
    }

    /// Returns all WAL log file paths in sequence order.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL directory cannot be read.
    pub fn log_files(&self) -> Result<Vec<PathBuf>> {
        self.manager.log_files()
    }

    /// Reads checkpoint metadata from disk.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata file cannot be read or deserialized.
    pub fn read_checkpoint_metadata(&self) -> Result<Option<CheckpointMetadata>> {
        self.manager.read_checkpoint_metadata()
    }

    /// Returns the path to the active WAL file.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.manager.path()
    }

    /// Returns the current WAL log sequence number.
    #[must_use]
    pub fn current_sequence(&self) -> u64 {
        self.manager.current_sequence()
    }
}

/// Type alias for the LPG (labeled property graph) WAL.
pub type LpgWal = TypedWal<WalRecord>;

impl TypedWal<WalRecord> {
    /// Seals a torn tail left by a crash, so a later commit marker cannot
    /// commit the torn records on replay.
    ///
    /// Starts a new log file (new records appended after a partially written
    /// frame would be unreadable) and writes an abort marker there, which makes
    /// recovery discard the records that no marker closed.
    ///
    /// Call this at open, before logging anything, when recovery reported a
    /// torn tail.
    ///
    /// # Errors
    ///
    /// Returns an error if the new log file or the marker cannot be written.
    pub fn seal_torn_tail(&self) -> Result<()> {
        self.manager.rotate()?;
        self.manager.log(&WalRecord::TransactionAbort {
            transaction_id: TransactionId::INVALID,
        })?;
        self.manager.sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grafeo_common::types::NodeId;
    use tempfile::tempdir;

    #[test]
    fn test_typed_wal_write() {
        let dir = tempdir().unwrap();
        let wal: LpgWal = TypedWal::open(dir.path()).unwrap();

        let record = WalRecord::CreateNode {
            id: NodeId::new(1),
            labels: vec!["Person".to_string()],
        };

        wal.log(&record).unwrap();
        wal.flush().unwrap();
        assert_eq!(wal.record_count(), 1);
    }

    #[test]
    fn test_typed_wal_checkpoint() {
        let dir = tempdir().unwrap();
        let wal: LpgWal = TypedWal::open(dir.path()).unwrap();

        wal.log(&WalRecord::CreateNode {
            id: NodeId::new(1),
            labels: vec!["Test".to_string()],
        })
        .unwrap();

        wal.log(&WalRecord::TransactionCommit {
            transaction_id: TransactionId::new(1),
        })
        .unwrap();

        wal.checkpoint(TransactionId::new(1), EpochId::new(10))
            .unwrap();
        assert_eq!(wal.checkpoint_epoch(), Some(EpochId::new(10)));
    }

    #[test]
    fn test_typed_wal_recovery_compatible() {
        // Verify TypedWal writes are recoverable by existing WalRecovery
        let dir = tempdir().unwrap();

        {
            let wal: LpgWal = TypedWal::open(dir.path()).unwrap();
            wal.log(&WalRecord::CreateNode {
                id: NodeId::new(1),
                labels: vec!["Person".to_string()],
            })
            .unwrap();
            wal.log(&WalRecord::TransactionCommit {
                transaction_id: TransactionId::new(1),
            })
            .unwrap();
            wal.sync().unwrap();
        }

        let recovery = super::super::WalRecovery::new(dir.path());
        let records = recovery.recover().unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_typed_wal_delegates_admin_methods() {
        let dir = tempdir().unwrap();
        let wal: LpgWal = TypedWal::open(dir.path()).unwrap();

        // Verify delegation works
        assert_eq!(wal.record_count(), 0);
        assert_eq!(wal.dir(), dir.path());
        assert!(wal.size_bytes() > 0 || wal.size_bytes() == 0);
        assert!(wal.checkpoint_epoch().is_none());
        assert!(wal.last_checkpoint_timestamp().is_none());

        let files = wal.log_files().unwrap();
        assert!(!files.is_empty());

        let _path = wal.path();
        let _mode = wal.durability_mode();
    }

    fn log_files_with_data(dir: &Path) -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
            .filter(|p| std::fs::metadata(p).unwrap().len() > 0)
            .count()
    }

    #[test]
    fn test_log_batch_groups_do_not_interleave() {
        use super::super::WalRecovery;
        use std::sync::Arc;

        /// Records per group, and the same count as an id offset.
        const GROUP: usize = 5;
        const GROUP_IDS: u64 = 5;
        let dir = tempdir().unwrap();
        {
            let wal: Arc<LpgWal> = Arc::new(TypedWal::open(dir.path()).unwrap());
            let writers: Vec<_> = (0..4u64)
                .map(|writer| {
                    let wal = Arc::clone(&wal);
                    std::thread::spawn(move || {
                        for group in 0..50u64 {
                            let base = (writer * 1000 + group) * 10;
                            let mut records: Vec<WalRecord> = (0..GROUP_IDS)
                                .map(|k| WalRecord::CreateNode {
                                    id: NodeId::new(base + k),
                                    labels: vec!["N".to_string()],
                                })
                                .collect();
                            records.push(WalRecord::TransactionCommit {
                                transaction_id: TransactionId::new(base),
                            });
                            wal.log_batch(&records).unwrap();
                        }
                    })
                })
                .collect();
            for writer in writers {
                writer.join().unwrap();
            }
            wal.sync().unwrap();
        }

        // Every commit marker must directly follow its own group's records.
        let records = WalRecovery::new(dir.path()).recover().unwrap();
        assert_eq!(records.len(), 4 * 50 * (GROUP + 1));
        for chunk in records.chunks(GROUP + 1) {
            let WalRecord::TransactionCommit { transaction_id } = chunk[GROUP] else {
                panic!("group not closed by its commit marker: {chunk:?}");
            };
            let ids: Vec<u64> = chunk[..GROUP]
                .iter()
                .map(|r| match r {
                    WalRecord::CreateNode { id, .. } => id.as_u64(),
                    other => panic!("unexpected record {other:?}"),
                })
                .collect();
            let base = transaction_id.as_u64();
            assert_eq!(ids, (base..base + GROUP_IDS).collect::<Vec<_>>());
        }
    }

    #[test]
    fn test_log_batch_rotates_only_between_groups() {
        let dir = tempdir().unwrap();
        let wal: LpgWal = TypedWal::with_config(
            dir.path(),
            WalConfig {
                max_log_size: 100,
                ..WalConfig::default()
            },
        )
        .unwrap();
        let group: Vec<WalRecord> = (0..10)
            .map(|i| WalRecord::CreateNode {
                id: NodeId::new(i),
                labels: vec!["Rotation".to_string()],
            })
            .chain(std::iter::once(WalRecord::TransactionCommit {
                transaction_id: TransactionId::new(1),
            }))
            .collect();

        // Each group is far over the size limit, yet stays in one file.
        wal.log_batch(&group).unwrap();
        assert_eq!(log_files_with_data(dir.path()), 1);
        wal.log_batch(&group).unwrap();
        assert_eq!(log_files_with_data(dir.path()), 2);
    }

    // --- Spilling groups (`log_group`) ---

    fn spill_limits() -> GroupLimits {
        GroupLimits {
            spill_threshold: 128,
            max_bytes: u64::MAX,
        }
    }

    fn spill_node(id: u64) -> WalRecord {
        WalRecord::CreateNode {
            id: NodeId::new(id),
            labels: vec!["Spill".to_string()],
        }
    }

    fn created(records: &[WalRecord]) -> Vec<u64> {
        records
            .iter()
            .filter_map(|r| match r {
                WalRecord::CreateNode { id, .. } => Some(id.as_u64()),
                _ => None,
            })
            .collect()
    }

    fn spill_files(dir: &Path) -> usize {
        std::fs::read_dir(dir.join(SPILL_DIR)).map_or(0, |d| d.count())
    }

    fn commit_marker(tx: u64) -> WalRecord {
        WalRecord::TransactionCommit {
            transaction_id: TransactionId::new(tx),
        }
    }

    #[test]
    fn test_spilled_group_commits_as_one_contiguous_run() {
        use super::super::WalRecovery;
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        {
            let wal: Arc<LpgWal> = Arc::new(TypedWal::open(dir.path()).unwrap());
            // Small writers keep appending while two large spilled groups commit.
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let small = {
                let wal = Arc::clone(&wal);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut n = 0u64;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let id = 1_000_000 + n;
                        wal.log_batch(&[spill_node(id), commit_marker(id)]).unwrap();
                        n += 1;
                    }
                    n
                })
            };
            let large: Vec<_> = (0..2u64)
                .map(|writer| {
                    let wal = Arc::clone(&wal);
                    std::thread::spawn(move || {
                        let mut group = wal.new_group(spill_limits());
                        for k in 0..3000 {
                            group.push(&spill_node(writer * 10_000 + k)).unwrap();
                        }
                        assert!(group.is_spilled());
                        let path = group.spill_path().unwrap().to_path_buf();
                        wal.log_group(&mut group, &[commit_marker(writer)], true)
                            .unwrap();
                        assert!(!path.exists(), "the commit deletes the spill file");
                    })
                })
                .collect();
            for writer in large {
                writer.join().unwrap();
            }
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            assert!(small.join().unwrap() > 0);
            wal.sync().unwrap();
        }
        assert_eq!(spill_files(dir.path()), 0);

        // Each large group is one unbroken run closed by its own commit.
        let records = WalRecovery::new(dir.path()).recover().unwrap();
        for writer in 0..2u64 {
            let base = writer * 10_000;
            let start = records
                .iter()
                .position(|r| created(std::slice::from_ref(r)) == [base])
                .unwrap();
            let run = &records[start..start + 3001];
            assert_eq!(
                created(&run[..3000]),
                (base..base + 3000).collect::<Vec<_>>()
            );
            assert!(matches!(
                run[3000],
                WalRecord::TransactionCommit { transaction_id } if transaction_id.as_u64() == writer
            ));
        }
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn test_spilled_group_in_encrypted_wal() {
        use super::super::WalRecovery;
        // A fresh random key per run, never a constant.
        let key: [u8; 32] = {
            use std::hash::{BuildHasher, Hasher};
            let mut key = [0u8; 32];
            for chunk in key.chunks_mut(8) {
                let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
                hasher.write_u64(0);
                chunk.copy_from_slice(&hasher.finish().to_le_bytes());
            }
            key
        };
        let dir = tempdir().unwrap();
        {
            let mut manager = WalManager::open(dir.path()).unwrap();
            manager.set_encryptor(grafeo_common::encryption::PageEncryptor::new(&key));
            let wal: LpgWal = TypedWal {
                manager,
                _record: PhantomData,
            };
            let mut group = wal.new_group(spill_limits());
            for k in 0..500 {
                group.push(&spill_node(k)).unwrap();
            }
            assert!(group.is_spilled());
            wal.log_group(&mut group, &[commit_marker(1)], true)
                .unwrap();
            wal.sync().unwrap();
        }
        let mut recovery = WalRecovery::new(dir.path());
        recovery.set_encryptor(grafeo_common::encryption::PageEncryptor::new(&key));
        let records = recovery.recover().unwrap();
        assert_eq!(created(&records), (0..500).collect::<Vec<_>>());
    }

    /// Encrypted spilled groups are copied into the WAL while other writers
    /// keep forcing rotations (a tiny `max_log_size` plus explicit
    /// `rotate()`). Each WAL frame's nonce comes from the active file's
    /// sequence and offset, so a rotation in the middle of a group copy would
    /// leave frames that do not decrypt, or split the group. Every group must
    /// decrypt on recovery and stay contiguous.
    #[cfg(feature = "encryption")]
    #[test]
    fn test_encrypted_spilled_groups_under_competing_rotations() {
        use super::super::WalRecovery;
        const GROUPS: u64 = 4;
        const PER_GROUP: u64 = 300;
        const SMALL: u64 = 400;
        let key: [u8; 32] = {
            use std::hash::{BuildHasher, Hasher};
            let mut key = [0u8; 32];
            for chunk in key.chunks_mut(8) {
                let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
                hasher.write_u64(0);
                chunk.copy_from_slice(&hasher.finish().to_le_bytes());
            }
            key
        };
        let dir = tempdir().unwrap();
        {
            let mut manager = WalManager::with_config(
                dir.path(),
                WalConfig {
                    durability: DurabilityMode::NoSync,
                    max_log_size: 256,
                    ..WalConfig::default()
                },
            )
            .unwrap();
            manager.set_encryptor(grafeo_common::encryption::PageEncryptor::new(&key));
            let wal: LpgWal = TypedWal {
                manager,
                _record: PhantomData,
            };
            let barrier = std::sync::Barrier::new(usize::try_from(GROUPS + 1).unwrap());
            std::thread::scope(|scope| {
                for g in 0..GROUPS {
                    let (wal, barrier) = (&wal, &barrier);
                    scope.spawn(move || {
                        let mut group = wal.new_group(spill_limits());
                        for k in 0..PER_GROUP {
                            group.push(&spill_node(g * 1_000_000 + k)).unwrap();
                        }
                        assert!(group.is_spilled());
                        barrier.wait();
                        wal.log_group(&mut group, &[commit_marker(g + 1)], true)
                            .unwrap();
                    });
                }
                let (wal, barrier) = (&wal, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for i in 0..SMALL {
                        let mut group = wal.new_group(spill_limits());
                        group.push(&spill_node(900_000_000 + i)).unwrap();
                        wal.log_group(&mut group, &[commit_marker(1_000 + i)], true)
                            .unwrap();
                        if i % 7 == 0 {
                            wal.rotate().unwrap();
                        }
                    }
                });
            });
            wal.sync().unwrap();
            assert!(wal.log_files().unwrap().len() > 10, "rotations happened");
        }
        let mut recovery = WalRecovery::new(dir.path());
        recovery.set_encryptor(grafeo_common::encryption::PageEncryptor::new(&key));
        let ids = created(&recovery.recover().unwrap());
        assert_eq!(ids.len() as u64, GROUPS * PER_GROUP + SMALL);
        for g in 0..GROUPS {
            let first = g * 1_000_000;
            let at = ids.iter().position(|&id| id == first).unwrap();
            assert_eq!(
                ids[at..at + PER_GROUP as usize],
                (first..first + PER_GROUP).collect::<Vec<_>>(),
                "group {g} is contiguous and complete"
            );
        }
    }

    #[test]
    fn test_failed_group_writes_nothing() {
        use super::super::WalRecovery;
        let dir = tempdir().unwrap();
        let wal: LpgWal = TypedWal::open(dir.path()).unwrap();
        let mut group = wal.new_group(GroupLimits {
            spill_threshold: 64,
            max_bytes: 200,
        });
        while group.push(&spill_node(1)).is_ok() {}
        let err = wal
            .log_group(&mut group, &[commit_marker(1)], true)
            .unwrap_err();
        assert!(err.error_code().is_retryable(), "{err}");
        assert!(group.is_empty());
        assert!(wal.poisoned_reason().is_none(), "nothing was appended");
        wal.sync().unwrap();
        assert!(WalRecovery::new(dir.path()).recover().unwrap().is_empty());
        assert_eq!(spill_files(dir.path()), 0);
    }

    #[test]
    fn test_damaged_spill_file_poisons_and_commits_nothing() {
        use super::super::WalRecovery;
        let dir = tempdir().unwrap();
        {
            let wal: LpgWal = TypedWal::open(dir.path()).unwrap();
            wal.log_batch(&[spill_node(1), commit_marker(1)]).unwrap();
            let mut group = wal.new_group(spill_limits());
            for k in 100..400 {
                group.push(&spill_node(k)).unwrap();
            }
            // Write the frames out, then flip a byte in the middle of the file.
            let path = group.spill_path().unwrap().to_path_buf();
            let mut sink_count = 0;
            group
                .for_each_frame(&mut |_| {
                    sink_count += 1;
                    Ok(())
                })
                .unwrap();
            assert_eq!(sink_count, 300);
            let mut bytes = std::fs::read(&path).unwrap();
            let middle = bytes.len() / 2;
            bytes[middle] ^= 0xFF;
            std::fs::write(&path, &bytes).unwrap();

            let err = wal
                .log_group(&mut group, &[commit_marker(2)], true)
                .unwrap_err();
            assert!(err.to_string().contains("damaged"), "{err}");
            assert!(wal.poisoned_reason().is_some());
            assert!(wal.log(&spill_node(5)).is_err(), "poisoned");
        }
        let recovered = WalRecovery::new(dir.path()).recover_with_tail().unwrap();
        assert_eq!(created(&recovered.records), vec![1]);
        assert!(recovered.torn_tail, "the partial group is an open tail");
    }

    #[cfg(feature = "testing-crash-injection")]
    #[test]
    fn test_crash_mid_copy_leaves_an_uncommitted_tail() {
        use super::super::WalRecovery;
        use grafeo_common::testing::crash::{CrashResult, with_crash_at};

        let dir = tempdir().unwrap();
        let leftover;
        {
            let wal: LpgWal = TypedWal::open(dir.path()).unwrap();
            wal.log_batch(&[spill_node(1), commit_marker(1)]).unwrap();
            let mut group = wal.new_group(spill_limits());
            for k in 100..1100 {
                group.push(&spill_node(k)).unwrap();
            }
            leftover = group.spill_path().unwrap().to_path_buf();
            // Each frame passes two crash points: crash halfway through the copy.
            let wal_ref = std::panic::AssertUnwindSafe(&wal);
            let mut group = std::panic::AssertUnwindSafe(group);
            let outcome = with_crash_at(1000, move || {
                let _ = wal_ref.log_group(&mut group, &[commit_marker(2)], true);
                // A real crash runs no destructor: keep the spill file.
                std::mem::forget(std::mem::replace(
                    &mut *group,
                    GroupBuffer::new(PathBuf::new(), GroupLimits::default(), false),
                ));
            });
            assert!(matches!(outcome, CrashResult::Crashed));
        }
        // The panic unwound through the group, which deleted its file; put a
        // stand-in back to check that reopening cleans up after a hard crash.
        std::fs::create_dir_all(leftover.parent().unwrap()).unwrap();
        std::fs::write(&leftover, b"left by a crash").unwrap();

        let recovered = WalRecovery::new(dir.path()).recover_with_tail().unwrap();
        assert_eq!(
            created(&recovered.records),
            vec![1],
            "the group is not committed"
        );
        assert!(recovered.torn_tail);

        // Reopen as the engine does: seal the tail, then commit more.
        {
            // As the engine does at a writable open.
            assert_eq!(
                super::super::remove_leftover_spill_files(dir.path()).unwrap(),
                1
            );
            let wal: LpgWal = TypedWal::open(dir.path()).unwrap();
            assert!(
                !leftover.exists(),
                "the cleanup removes leftover spill files"
            );
            wal.seal_torn_tail().unwrap();
            wal.log_batch(&[spill_node(9), commit_marker(9)]).unwrap();
            wal.sync().unwrap();
        }
        let records = WalRecovery::new(dir.path()).recover().unwrap();
        assert_eq!(created(&records), vec![1, 9]);
    }

    /// Measures how long a commit holds the WAL's append lock for a spilled
    /// group, against the same group in RAM. Run with
    /// `cargo test -p grafeo-storage --release --lib measure_spilled_commit -- --ignored --nocapture`.
    #[test]
    #[ignore = "measurement, not a check"]
    fn measure_spilled_commit_lock_hold() {
        for mib in [16u64, 64, 256] {
            let records = mib * 1024; // ~1 KiB each
            let record = |k: u64| WalRecord::SetNodeProperty {
                id: NodeId::new(k),
                key: "payload".to_string(),
                value: grafeo_common::types::Value::from("x".repeat(1000)),
            };
            for spill in [false, true] {
                let dir = tempdir().unwrap();
                let wal: LpgWal = TypedWal::open(dir.path()).unwrap();
                let mut group = wal.new_group(GroupLimits {
                    spill_threshold: if spill { 8 << 20 } else { usize::MAX },
                    max_bytes: u64::MAX,
                });
                for k in 0..records {
                    group.push(&record(k)).unwrap();
                }
                assert_eq!(group.is_spilled(), spill);
                let peak = group.peak_ram_bytes();
                let start = std::time::Instant::now();
                wal.log_group(&mut group, &[commit_marker(1)], true)
                    .unwrap();
                let held = start.elapsed();
                println!(
                    "{mib:>4} MiB group, {}: append (lock held) {:>7.1} ms, peak buffer RAM {:>6} KiB",
                    if spill { "spilled" } else { "in RAM " },
                    held.as_secs_f64() * 1000.0,
                    peak.max(group.peak_ram_bytes()) / 1024
                );
            }
        }
    }
}
