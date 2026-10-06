//! Per-session buffer of WAL records.
//!
//! Records carry no transaction id, and recovery keeps a single buffer of
//! pending records that the next commit or abort marker closes. If sessions
//! wrote their records as they happen, records of concurrent transactions
//! would interleave and one session's marker would commit or discard another
//! session's records (#411).
//!
//! A [`WalBuffer`] collects a session's records instead, and writes them as
//! one contiguous group at commit, followed by the commit marker. A rollback
//! clears the buffer and writes nothing; a rollback to a savepoint truncates
//! it. Writes outside a transaction are written as an implicit group with its
//! own commit marker.
//!
//! Each group carries its own named-graph context: it emits `SwitchGraph`
//! before records of another graph and switches back to the default graph
//! before its markers, so replay of every group starts and ends in the
//! default graph.
//!
//! Fork additions: every group ends with a `TransactionCommit` followed by an
//! `EpochAdvance`, implicit groups included, because generation-root replay
//! rejects a commit without its epoch advance. A group that fails to append
//! poisons the WAL (#13), on every kind of database. Sessions of one database
//! share a commit-order lock, so groups reach the WAL in commit order.
//!
//! Bounded RAM: records are encoded when they are pushed, into a
//! [`GroupBuffer`] that moves to a spill file next to the WAL once it holds
//! more than the configured threshold (`Config::wal_spill_threshold`). A
//! spilled group is copied into the WAL at commit under its append lock, so
//! it is still one contiguous group there. The configured cap
//! (`Config::wal_transaction_buffer_cap`) bounds the group's size, in RAM or
//! on disk; a record past it, or one that cannot be spilled, is refused and
//! the transaction can only be rolled back.

use std::sync::Arc;

use grafeo_common::grafeo_warn;
use grafeo_common::types::{EpochId, TransactionId};
use grafeo_common::utils::error::{Error, Result};
use grafeo_storage::wal::{GroupBuffer, GroupLimits, GroupPosition, LpgWal, WalRecord};
use parking_lot::Mutex;

/// Debug-only test seam: when set, the next committing transaction clears it
/// and parks right before writing its WAL group (after its commit is applied
/// in memory), with [`COMMIT_STALL_PARKED`] set until the test clears that.
/// Lets a test commit a second transaction that saw the first one's writes
/// while the first one's group is not written yet.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub static COMMIT_STALL_BEFORE_GROUP: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// See [`COMMIT_STALL_BEFORE_GROUP`]: `true` while a commit is parked; the
/// test clears it to release the commit.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub static COMMIT_STALL_PARKED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Debug-only test seam, like [`COMMIT_STALL_BEFORE_GROUP`], but the commit
/// parks after its WAL writability check and before conflict validation.
/// Lets a test poison the WAL inside that window.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub static COMMIT_STALL_BEFORE_VALIDATION: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Parks the calling commit if `seam` is set (clearing it), with
/// [`COMMIT_STALL_PARKED`] set until the test clears that.
#[cfg(debug_assertions)]
fn stall_if(seam: &std::sync::atomic::AtomicBool) {
    use std::sync::atomic::Ordering;
    if seam
        .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        COMMIT_STALL_PARKED.store(true, Ordering::Release);
        while COMMIT_STALL_PARKED.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

/// The [`COMMIT_STALL_BEFORE_GROUP`] seam.
#[cfg(debug_assertions)]
pub(crate) fn maybe_stall_before_group() {
    stall_if(&COMMIT_STALL_BEFORE_GROUP);
}

#[cfg(not(debug_assertions))]
pub(crate) fn maybe_stall_before_group() {}

/// The [`COMMIT_STALL_BEFORE_VALIDATION`] seam.
#[cfg(debug_assertions)]
pub(crate) fn maybe_stall_before_validation() {
    stall_if(&COMMIT_STALL_BEFORE_VALIDATION);
}

#[cfg(not(debug_assertions))]
pub(crate) fn maybe_stall_before_validation() {}

/// Buffers one session's WAL records until they are written as a group.
pub(crate) struct WalBuffer {
    wal: Arc<LpgWal>,
    state: Mutex<BufferState>,
    /// Shared by every session of the database (see
    /// [`commit_order`](Self::commit_order)).
    commit_order: Arc<Mutex<()>>,
    /// Spill threshold and byte cap of the buffer's groups.
    limits: GroupLimits,
    /// Encrypt spill files although the WAL is not (encryption at rest is
    /// configured).
    encrypt_spill: bool,
}

/// The buffered group and what it needs to resume at a savepoint.
struct BufferState {
    /// The group's frames, encoded at push time; spilled to disk past the
    /// configured threshold.
    group: GroupBuffer,
    /// The graph the group's last record applies to (`None` = default).
    context: Option<String>,
    /// Records pushed, not counting the `SwitchGraph` records the buffer adds.
    records: usize,
}

/// A position in a [`WalBuffer`], for savepoints.
#[derive(Debug, Clone)]
pub(crate) struct WalBufferMark {
    position: GroupPosition,
    context: Option<String>,
    records: usize,
}

impl WalBuffer {
    /// Creates an empty buffer writing to `wal`, with its own commit-order
    /// lock and the default limits.
    #[cfg(test)]
    pub(crate) fn new(wal: Arc<LpgWal>) -> Self {
        Self::for_database(wal, Arc::new(Mutex::new(())), GroupLimits::default(), false)
    }

    /// Creates an empty buffer writing to `wal` for a session of a database
    /// whose sessions share `commit_order`. `limits` set when the buffer
    /// spills to disk and how large its group may grow (the
    /// `wal_transaction_buffer_cap`, in RAM or on disk). Spill files are
    /// encrypted when the WAL is, or when `encrypt_spill` is set.
    pub(crate) fn for_database(
        wal: Arc<LpgWal>,
        commit_order: Arc<Mutex<()>>,
        limits: GroupLimits,
        encrypt_spill: bool,
    ) -> Self {
        let group = wal.new_group_with(limits, encrypt_spill);
        Self {
            wal,
            state: Mutex::new(BufferState {
                group,
                context: None,
                records: 0,
            }),
            commit_order,
            limits,
            encrypt_spill,
        }
    }

    /// Locks the database's commit order. A committing session holds it from
    /// before its commit validation until its group is written, so a
    /// transaction that saw another one's committed writes (an edge onto its
    /// new node, a later SET of its property) is always written after it, and
    /// replay applies them in that order. Records are only written at
    /// commit, so without it the second transaction's group could reach the
    /// WAL first.
    pub(crate) fn commit_order(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.commit_order.lock()
    }

    /// The WAL this buffer writes to.
    pub(crate) fn wal(&self) -> &Arc<LpgWal> {
        &self.wal
    }

    /// Adds a record for `graph` (`None` = default graph), encoded right away,
    /// preceded by a `SwitchGraph` when the graph changes.
    ///
    /// Store mutations cannot fail, so neither does this call: if the record
    /// cannot be buffered (over the cap, spilling to disk failed, or it
    /// cannot be encoded), the buffer keeps the error, refuses every later
    /// record and reports it from [`check`](Self::check). The statement and
    /// the commit check that, so the transaction can only be rolled back.
    pub(crate) fn push(&self, graph: Option<String>, record: WalRecord) {
        let mut state = self.state.lock();
        if graph != state.context {
            if let Err(e) = state.group.push(&WalRecord::SwitchGraph {
                name: graph.clone(),
            }) {
                grafeo_warn!("WAL buffer refused a record: {}", e);
                return;
            }
            state.context = graph;
        }
        match state.group.push(&record) {
            Ok(()) => state.records += 1,
            Err(e) => grafeo_warn!("WAL buffer refused a record: {}", e),
        }
    }

    /// The error of a refused push, if any: over the cap or a failed spill
    /// give a retryable [`Error::AdmissionRetryable`]. A transaction with a
    /// refused push must be rolled back (or rolled back to a savepoint
    /// before it) before it can commit.
    ///
    /// # Errors
    ///
    /// Returns the refused push's error.
    ///
    /// [`Error::AdmissionRetryable`]: grafeo_common::utils::error::Error::AdmissionRetryable
    pub(crate) fn check(&self) -> Result<()> {
        match self.state.lock().group.failure() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Like [`check`](Self::check), and also writes a spilled group's last
    /// buffered frames to its spill file, so that a disk error there is still
    /// a refused push the transaction can roll back, instead of a failure
    /// inside the commit's copy (which poisons the WAL after the commit is
    /// applied). Call it right before committing.
    ///
    /// # Errors
    ///
    /// Returns the refused push's error, or the spill write's.
    pub(crate) fn prepare_commit(&self) -> Result<()> {
        self.state.lock().group.prepare_commit()
    }

    /// Number of buffered records.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn len(&self) -> usize {
        self.state.lock().records
    }

    /// Whether nothing is buffered.
    pub(crate) fn is_empty(&self) -> bool {
        self.state.lock().group.is_empty()
    }

    /// The current position, to return to with [`truncate`](Self::truncate).
    pub(crate) fn savepoint(&self) -> WalBufferMark {
        let state = self.state.lock();
        WalBufferMark {
            position: state.group.position(),
            context: state.context.clone(),
            records: state.records,
        }
    }

    /// Drops the records added after `mark` (savepoint rollback).
    pub(crate) fn truncate(&self, mark: &WalBufferMark) {
        let mut state = self.state.lock();
        // Also at the end: that clears a push refused right after `mark`.
        state.group.truncate(mark.position);
        state.context = mark.context.clone();
        state.records = mark.records;
    }

    /// Drops every buffered record and the spill file (rollback).
    pub(crate) fn clear(&self) {
        let mut state = self.state.lock();
        state.group.clear();
        state.context = None;
        state.records = 0;
    }

    /// Whether the buffered records have moved to a spill file.
    #[doc(hidden)]
    pub(crate) fn is_spilled(&self) -> bool {
        self.state.lock().group.is_spilled()
    }

    /// The highest number of bytes this buffer has held in RAM.
    #[doc(hidden)]
    pub(crate) fn peak_ram_bytes(&self) -> usize {
        self.state.lock().group.peak_ram_bytes()
    }

    /// Writes the buffered records as one group, closed by a switch back to
    /// the default graph if needed and then `markers`.
    ///
    /// The markers are written even when no record is buffered. A spilled
    /// group is copied from its spill file under the WAL's append lock.
    ///
    /// # Errors
    ///
    /// Returns the refused push's error (see [`check`](Self::check)) without
    /// writing anything. Otherwise returns an error if the
    /// WAL write fails or the WAL is poisoned; a failed write has poisoned
    /// the WAL by then, on every kind of database: the group may be partly
    /// on disk, and a later group must not land after it. The buffered
    /// records are dropped either way.
    pub(crate) fn flush(&self, markers: &[WalRecord]) -> Result<()> {
        let mut state = self.state.lock();
        let mut trailer = Vec::with_capacity(markers.len() + 1);
        if state.context.is_some() {
            trailer.push(WalRecord::SwitchGraph { name: None });
        }
        trailer.extend(markers.iter().cloned());
        let result = self.wal.log_group(&mut state.group, &trailer, true);
        state.context = None;
        state.records = 0;
        result
    }

    /// Writes `records` (default graph) right away as their own implicit
    /// group, leaving the buffer alone: for schema changes, which take effect
    /// immediately and are not undone by a rollback, and for `GrafeoDB`-level
    /// writes. Not subject to the cap (the records are applied already), but
    /// spilled past the threshold like any group.
    ///
    /// # Errors
    ///
    /// Returns an error if a record cannot be encoded or spilled, or the WAL
    /// write fails (either poisons the WAL), or the WAL is poisoned. Never a
    /// retryable error: the records are applied already.
    pub(crate) fn write_implicit_group(&self, records: &[WalRecord], epoch: EpochId) -> Result<()> {
        let mut group = self.wal.new_group_with(
            GroupLimits {
                max_bytes: u64::MAX,
                ..self.limits
            },
            self.encrypt_spill,
        );
        for record in records {
            if let Err(e) = group.push(record) {
                // The records are applied already and will never reach the
                // WAL: like a failed append, poison it, and never report this
                // as retryable (a retry would apply them twice).
                let reason = format!("an implicit WAL group could not be buffered: {e}");
                self.wal.poison(reason.clone());
                return Err(Error::Internal(reason));
            }
        }
        self.wal
            .log_group(&mut group, &implicit_markers(epoch), true)
    }

    /// Writes buffered records from outside a transaction as an implicit
    /// group with its own commit marker. Does nothing when the buffer is
    /// empty.
    ///
    /// Outside a transaction a refused push cannot be rolled back: its write
    /// is already applied in memory and will never reach the WAL. That
    /// poisons the WAL, so every later write is refused until a reopen.
    ///
    /// # Errors
    ///
    /// Returns the refused push's error, or an error if the WAL write fails.
    pub(crate) fn flush_implicit(&self, epoch: EpochId) -> Result<()> {
        if let Err(error) = self.check() {
            self.clear();
            self.wal.poison(format!(
                "a write outside a transaction could not be logged: {error}"
            ));
            // Keep the error's kind (#27 keeps the cap retryable), but say
            // that nothing succeeds before a reopen.
            const REOPEN: &str = " The write is applied in memory but not logged; the WAL \
                                  refuses writes until the database is reopened.";
            return Err(match error {
                Error::AdmissionRetryable(message) => {
                    Error::AdmissionRetryable(format!("{message}.{REOPEN}"))
                }
                Error::Internal(message) => Error::Internal(format!("{message}.{REOPEN}")),
                other => other,
            });
        }
        if self.is_empty() {
            return Ok(());
        }
        self.flush(&implicit_markers(epoch))
    }
}

/// The error for a write outside a transaction whose implicit group failed:
/// a refused record (over the cap) keeps its retryable error; a failed
/// append becomes "durability unconfirmed", because the write is applied in
/// memory, cannot be undone, and the WAL is poisoned.
pub(crate) fn unconfirmed_write_error(error: Error) -> Error {
    match error {
        Error::AdmissionRetryable(_) => error,
        other => Error::Internal(format!(
            "write applied in memory; durability unconfirmed (it may have been written): \
             its WAL group failed ({other}); the WAL refuses further writes until the \
             database is reopened"
        )),
    }
}

/// Markers closing an implicit group (writes and schema changes outside a
/// transaction): a system commit, and the epoch advance generation-root
/// replay requires right after every commit. `epoch` is the current epoch;
/// replay keeps the highest epoch it reads, and plain WAL recovery treats the
/// advance as metadata.
pub(crate) fn implicit_markers(epoch: EpochId) -> [WalRecord; 2] {
    [
        WalRecord::TransactionCommit {
            transaction_id: TransactionId::SYSTEM,
        },
        WalRecord::EpochAdvance { epoch },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use grafeo_common::types::NodeId;
    use grafeo_storage::wal::WalRecovery;

    fn create(id: u64) -> WalRecord {
        WalRecord::CreateNode {
            id: NodeId::new(id),
            labels: vec!["N".to_string()],
        }
    }

    fn commit() -> WalRecord {
        WalRecord::TransactionCommit {
            transaction_id: TransactionId::new(7),
        }
    }

    /// Short form of a group for assertions.
    fn shape(group: &[WalRecord]) -> Vec<String> {
        group
            .iter()
            .map(|record| match record {
                WalRecord::CreateNode { id, .. } => format!("node {}", id.as_u64()),
                WalRecord::SwitchGraph { name } => format!("switch {name:?}"),
                WalRecord::TransactionCommit { .. } => "commit".to_string(),
                WalRecord::EpochAdvance { epoch } => format!("epoch {}", epoch.as_u64()),
                other => format!("{other:?}"),
            })
            .collect()
    }

    /// Pushes `pending`, flushes with `markers` and returns what the WAL holds.
    fn written(
        pending: Vec<(Option<String>, WalRecord)>,
        markers: &[WalRecord],
        limits: GroupLimits,
    ) -> Vec<String> {
        let dir = tempfile::tempdir().unwrap();
        {
            let wal = Arc::new(LpgWal::open(dir.path()).unwrap());
            let buffer = WalBuffer::for_database(Arc::clone(&wal), Arc::default(), limits, false);
            for (graph, record) in pending {
                buffer.push(graph, record);
            }
            buffer.flush(markers).unwrap();
            wal.sync().unwrap();
        }
        let records = WalRecovery::new(dir.path()).recover().unwrap();
        shape(&records)
    }

    #[test]
    fn default_graph_group_has_no_switches() {
        assert_eq!(
            written(
                vec![(None, create(1)), (None, create(2))],
                &[commit()],
                GroupLimits::default()
            ),
            ["node 1", "node 2", "commit"]
        );
    }

    fn switches_graphs_and_returns_to_default(limits: GroupLimits) {
        let pending = vec![
            (Some("g".to_string()), create(1)),
            (Some("g".to_string()), create(2)),
            (None, create(3)),
            (Some("h".to_string()), create(4)),
        ];
        let markers = [
            commit(),
            WalRecord::EpochAdvance {
                epoch: EpochId::new(5),
            },
        ];
        assert_eq!(
            written(pending, &markers, limits),
            [
                "switch Some(\"g\")",
                "node 1",
                "node 2",
                "switch None",
                "node 3",
                "switch Some(\"h\")",
                "node 4",
                "switch None",
                "commit",
                "epoch 5",
            ]
        );
    }

    #[test]
    fn group_switches_graphs_and_returns_to_default() {
        switches_graphs_and_returns_to_default(GroupLimits::default());
    }

    #[test]
    fn spilled_group_switches_graphs_and_returns_to_default() {
        switches_graphs_and_returns_to_default(GroupLimits {
            spill_threshold: 0,
            max_bytes: u64::MAX,
        });
    }

    #[test]
    fn empty_group_is_only_markers() {
        assert_eq!(
            written(Vec::new(), &[commit()], GroupLimits::default()),
            ["commit"]
        );
    }

    #[test]
    fn truncate_restores_the_graph_context() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(LpgWal::open(dir.path()).unwrap());
        let buffer = WalBuffer::new(Arc::clone(&wal));
        buffer.push(Some("g".to_string()), create(1));
        let mark = buffer.savepoint();
        buffer.push(None, create(2));
        buffer.truncate(&mark);
        // Still in graph g: the next record of g needs no switch, and the
        // group switches back to the default graph before its marker.
        buffer.push(Some("g".to_string()), create(3));
        assert_eq!(buffer.len(), 2);
        buffer.flush(&[commit()]).unwrap();
        wal.sync().unwrap();
        let records = WalRecovery::new(dir.path()).recover().unwrap();
        assert_eq!(
            shape(&records),
            [
                "switch Some(\"g\")",
                "node 1",
                "node 3",
                "switch None",
                "commit"
            ]
        );
    }

    #[test]
    fn truncate_and_clear() {
        let dir = tempfile::tempdir().unwrap();
        let buffer = WalBuffer::new(Arc::new(LpgWal::open(dir.path()).unwrap()));
        buffer.push(None, create(1));
        let savepoint = buffer.savepoint();
        buffer.push(None, create(2));
        buffer.push(None, create(3));
        buffer.truncate(&savepoint);
        assert_eq!(buffer.len(), 1);
        buffer.clear();
        assert!(buffer.is_empty());
        // Nothing buffered: an implicit flush writes nothing.
        buffer.flush_implicit(EpochId::new(1)).unwrap();
        assert_eq!(buffer.wal().record_count(), 0);
    }

    fn capped(wal: Arc<LpgWal>, cap: u64) -> WalBuffer {
        WalBuffer::for_database(
            wal,
            Arc::default(),
            GroupLimits {
                spill_threshold: usize::MAX,
                max_bytes: cap,
            },
            false,
        )
    }

    #[test]
    fn push_over_the_cap_faults_until_rolled_back() {
        let dir = tempfile::tempdir().unwrap();
        // RAM frame (length prefix) plus the spill frame's CRC, as the cap charges.
        let one = 8 + LpgWal::encode(&create(1)).unwrap().len() as u64;
        let buffer = capped(Arc::new(LpgWal::open(dir.path()).unwrap()), 2 * one);
        buffer.push(None, create(1));
        buffer.push(None, create(2));
        buffer.check().unwrap();
        buffer.push(None, create(3));
        let err = buffer.check().expect_err("over the cap");
        assert!(err.error_code().is_retryable(), "{err}");
        assert!(
            err.to_string().contains("transaction WAL buffer cap"),
            "{err}"
        );
        // Later pushes are dropped, and the buffer cannot be flushed.
        buffer.push(None, create(4));
        assert_eq!(buffer.len(), 2);
        assert!(buffer.flush(&[commit()]).is_err());
        assert_eq!(buffer.wal().record_count(), 0, "nothing was written");
        assert!(buffer.wal().poisoned_reason().is_none());

        // Rolling back to a savepoint before the refused push clears it.
        buffer.push(None, create(1));
        let savepoint_after = buffer.savepoint();
        buffer.push(None, create(2));
        buffer.push(None, create(3));
        assert!(buffer.check().is_err());
        buffer.truncate(&savepoint_after);
        buffer.check().unwrap();
        buffer.flush(&[commit()]).unwrap();
        let records = WalRecovery::new(dir.path()).recover().unwrap();
        assert_eq!(shape(&records), ["node 1", "commit"]);
    }

    #[test]
    fn refused_push_outside_a_transaction_poisons_the_wal() {
        let dir = tempfile::tempdir().unwrap();
        let buffer = capped(Arc::new(LpgWal::open(dir.path()).unwrap()), 1);
        buffer.push(None, create(1));
        assert!(buffer.flush_implicit(EpochId::new(1)).is_err());
        assert!(buffer.wal().poisoned_reason().is_some());
        assert!(buffer.is_empty());
    }

    #[test]
    fn implicit_group_is_not_capped_but_spills() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(LpgWal::open(dir.path()).unwrap());
        let buffer = WalBuffer::for_database(
            Arc::clone(&wal),
            Arc::default(),
            GroupLimits {
                spill_threshold: 64,
                max_bytes: 1,
            },
            false,
        );
        let records: Vec<WalRecord> = (0..200).map(create).collect();
        buffer
            .write_implicit_group(&records, EpochId::new(3))
            .unwrap();
        wal.sync().unwrap();
        let recovered = WalRecovery::new(dir.path()).recover().unwrap();
        assert_eq!(recovered.len(), 202);
        assert!(
            std::fs::read_dir(dir.path().join(grafeo_storage::wal::SPILL_DIR))
                .map_or(0, |d| d.count())
                == 0
        );
    }
}
