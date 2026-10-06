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
//! poisons the WAL (#13's rule, on every database: the group is the only
//! copy of its records). Sessions of one database share a commit-order lock,
//! so groups reach the WAL in commit order.

use std::sync::Arc;

use grafeo_common::types::{EpochId, TransactionId};
use grafeo_common::utils::error::Result;
use grafeo_storage::wal::{LpgWal, WalRecord};
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

/// The [`COMMIT_STALL_BEFORE_GROUP`] seam.
#[cfg(debug_assertions)]
pub(crate) fn maybe_stall_before_group() {
    use std::sync::atomic::Ordering;
    if COMMIT_STALL_BEFORE_GROUP
        .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        COMMIT_STALL_PARKED.store(true, Ordering::Release);
        while COMMIT_STALL_PARKED.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

#[cfg(not(debug_assertions))]
pub(crate) fn maybe_stall_before_group() {}

/// A record waiting for its group, with the named graph it applies to
/// (`None` = default graph).
type PendingRecord = (Option<String>, WalRecord);

/// Buffers one session's WAL records until they are written as a group.
pub(crate) struct WalBuffer {
    wal: Arc<LpgWal>,
    pending: Mutex<Vec<PendingRecord>>,
    /// Shared by every session of the database (see
    /// [`commit_order`](Self::commit_order)).
    commit_order: Arc<Mutex<()>>,
}

impl WalBuffer {
    /// Creates an empty buffer writing to `wal`, with its own commit-order
    /// lock.
    #[cfg(test)]
    pub(crate) fn new(wal: Arc<LpgWal>) -> Self {
        Self::for_database(wal, Arc::new(Mutex::new(())))
    }

    /// Creates an empty buffer writing to `wal` for a session of a database
    /// whose sessions share `commit_order`.
    pub(crate) fn for_database(wal: Arc<LpgWal>, commit_order: Arc<Mutex<()>>) -> Self {
        Self {
            wal,
            pending: Mutex::new(Vec::new()),
            commit_order,
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

    /// Adds a record for `graph` (`None` = default graph).
    pub(crate) fn push(&self, graph: Option<String>, record: WalRecord) {
        self.pending.lock().push((graph, record));
    }

    /// Number of buffered records, used as a savepoint position.
    pub(crate) fn len(&self) -> usize {
        self.pending.lock().len()
    }

    /// Whether no records are buffered.
    pub(crate) fn is_empty(&self) -> bool {
        self.pending.lock().is_empty()
    }

    /// Drops the records added after position `len` (savepoint rollback).
    pub(crate) fn truncate(&self, len: usize) {
        self.pending.lock().truncate(len);
    }

    /// Drops every buffered record (rollback).
    pub(crate) fn clear(&self) {
        self.pending.lock().clear();
    }

    /// Writes the buffered records as one group, closed by `markers`.
    ///
    /// The markers are written even when no record is buffered.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails or the WAL is poisoned. A
    /// write failure has poisoned the WAL by then (see
    /// `TypedWal::log_atomic_or_poison`): the records may be partly on disk,
    /// and nothing may be appended after them. The buffered records are
    /// dropped either way.
    pub(crate) fn flush(&self, markers: &[WalRecord]) -> Result<()> {
        let pending = std::mem::take(&mut *self.pending.lock());
        let group = build_group(pending, markers);
        self.append(&group)
    }

    /// Writes `records` (default graph) right away as their own implicit
    /// group, leaving the buffer alone: for schema changes, which take effect
    /// immediately and are not undone by a rollback.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails or the WAL is poisoned.
    pub(crate) fn write_implicit_group(&self, records: &[WalRecord], epoch: EpochId) -> Result<()> {
        let pending = records.iter().map(|r| (None, r.clone())).collect();
        self.append(&build_group(pending, &implicit_markers(epoch)))
    }

    /// Appends a built group in one write, poisoning the WAL on failure.
    fn append(&self, group: &[WalRecord]) -> Result<()> {
        if group.is_empty() {
            return Ok(());
        }
        self.wal.log_atomic_or_poison(group)
    }

    /// Writes buffered records from outside a transaction as an implicit
    /// group with its own commit marker. Does nothing when the buffer is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails.
    pub(crate) fn flush_implicit(&self, epoch: EpochId) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        self.flush(&implicit_markers(epoch))
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

/// Builds a group: the records with `SwitchGraph` wherever the graph changes,
/// a switch back to the default graph if needed, then the markers.
fn build_group(pending: Vec<PendingRecord>, markers: &[WalRecord]) -> Vec<WalRecord> {
    let mut group = Vec::with_capacity(pending.len() + markers.len() + 2);
    let mut context: Option<String> = None;
    for (graph, record) in pending {
        if graph != context {
            group.push(WalRecord::SwitchGraph {
                name: graph.clone(),
            });
            context = graph;
        }
        group.push(record);
    }
    if context.is_some() {
        group.push(WalRecord::SwitchGraph { name: None });
    }
    group.extend(markers.iter().cloned());
    group
}

#[cfg(test)]
mod tests {
    use super::*;
    use grafeo_common::types::NodeId;

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

    #[test]
    fn default_graph_group_has_no_switches() {
        let group = build_group(vec![(None, create(1)), (None, create(2))], &[commit()]);
        assert_eq!(shape(&group), ["node 1", "node 2", "commit"]);
    }

    #[test]
    fn group_switches_graphs_and_returns_to_default() {
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
            shape(&build_group(pending, &markers)),
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
    fn empty_group_is_only_markers() {
        assert_eq!(shape(&build_group(Vec::new(), &[commit()])), ["commit"]);
        assert!(build_group(Vec::new(), &[]).is_empty());
    }

    #[test]
    fn truncate_and_clear() {
        let dir = tempfile::tempdir().unwrap();
        let buffer = WalBuffer::new(Arc::new(LpgWal::open(dir.path()).unwrap()));
        buffer.push(None, create(1));
        let savepoint = buffer.len();
        buffer.push(None, create(2));
        buffer.push(None, create(3));
        buffer.truncate(savepoint);
        assert_eq!(buffer.len(), 1);
        buffer.clear();
        assert!(buffer.is_empty());
        // Nothing buffered: an implicit flush writes nothing.
        buffer.flush_implicit(EpochId::new(1)).unwrap();
        assert_eq!(buffer.wal().record_count(), 0);
    }
}
