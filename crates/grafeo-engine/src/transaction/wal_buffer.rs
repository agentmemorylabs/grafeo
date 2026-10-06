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
//!
//! Records are encoded when they are pushed, so a commit holds one encoded
//! copy of the transaction, not the records plus their encoding. The buffer
//! has a byte cap (`Config::wal_transaction_buffer_cap`, 512 MiB by default):
//! a push that would exceed it is refused and leaves the buffer faulted, so
//! the statement fails with a retryable error and the transaction cannot
//! commit until it is rolled back (or rolled back to a savepoint before the
//! refused push). The cap is a stopgap until transaction buffers are charged
//! to a process-wide memory ledger; it keeps one huge transaction from
//! taking the process down.

use std::sync::Arc;

use grafeo_common::types::{EpochId, TransactionId};
use grafeo_common::utils::error::{Error, Result};
use grafeo_storage::wal::{LpgWal, WalEntry, WalRecord};
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

/// One encoded record waiting for its group, with the named graph it
/// applies to (`None` = default graph).
struct PendingFrame {
    graph: Option<String>,
    frame: Vec<u8>,
}

/// Why the buffer refused a push. Sticky until the records from `at` on are
/// dropped (rollback, or rollback to a savepoint at or before `at`).
#[derive(Debug, Clone)]
enum Fault {
    /// The push would have taken the buffer to `size` bytes, over `cap`.
    Cap { cap: usize, size: usize, at: usize },
    /// The record could not be encoded.
    Encode { message: String, at: usize },
}

impl Fault {
    fn at(&self) -> usize {
        match self {
            Self::Cap { at, .. } | Self::Encode { at, .. } => *at,
        }
    }

    fn to_error(&self) -> Error {
        match self {
            Self::Cap { cap, size, .. } => Error::AdmissionRetryable(format!(
                "the transaction's WAL records need at least {size} bytes, over the \
                 {cap}-byte transaction WAL buffer cap (Config::wal_transaction_buffer_cap); \
                 nothing of it was written to the WAL. Roll the transaction back and retry \
                 the work in smaller transactions. The cap stands in for a memory ledger \
                 that does not exist yet."
            )),
            Self::Encode { message, .. } => Error::Internal(format!(
                "a WAL record of the transaction could not be encoded ({message}); nothing \
                 of it was written to the WAL; roll the transaction back"
            )),
        }
    }
}

#[derive(Default)]
struct Pending {
    frames: Vec<PendingFrame>,
    /// Encoded bytes held (frames plus graph names).
    bytes: usize,
    fault: Option<Fault>,
}

/// Buffers one session's WAL records, encoded, until they are written as a
/// group.
pub(crate) struct WalBuffer {
    wal: Arc<LpgWal>,
    pending: Mutex<Pending>,
    /// Shared by every session of the database (see
    /// [`commit_order`](Self::commit_order)).
    commit_order: Arc<Mutex<()>>,
    /// Byte cap on the buffered records (`None` = no cap).
    cap: Option<usize>,
}

impl WalBuffer {
    /// Creates an empty, uncapped buffer writing to `wal`, with its own
    /// commit-order lock.
    #[cfg(test)]
    pub(crate) fn new(wal: Arc<LpgWal>) -> Self {
        Self::for_database(wal, Arc::new(Mutex::new(())), None)
    }

    /// Creates an empty buffer writing to `wal` for a session of a database
    /// whose sessions share `commit_order`, holding at most `cap` bytes.
    pub(crate) fn for_database(
        wal: Arc<LpgWal>,
        commit_order: Arc<Mutex<()>>,
        cap: Option<usize>,
    ) -> Self {
        Self {
            wal,
            pending: Mutex::new(Pending::default()),
            commit_order,
            cap,
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

    /// Adds a record for `graph` (`None` = default graph), encoded.
    ///
    /// Refused, leaving the buffer faulted (see [`check`](Self::check)), when
    /// it would take the buffer over its cap or cannot be encoded. Once
    /// faulted, later pushes are dropped: the transaction cannot commit.
    pub(crate) fn push(&self, graph: Option<String>, record: WalRecord) {
        let mut pending = self.pending.lock();
        if pending.fault.is_some() {
            return;
        }
        let at = pending.frames.len();
        let frame = match LpgWal::encode(&record) {
            Ok(frame) => frame,
            Err(e) => {
                pending.fault = Some(Fault::Encode {
                    message: e.to_string(),
                    at,
                });
                return;
            }
        };
        let size = pending.bytes + frame.len() + graph.as_ref().map_or(0, String::len);
        if let Some(cap) = self.cap
            && size > cap
        {
            pending.fault = Some(Fault::Cap { cap, size, at });
            return;
        }
        pending.bytes = size;
        pending.frames.push(PendingFrame { graph, frame });
    }

    /// The error of a refused push, if the buffer is faulted. A faulted
    /// transaction must be rolled back (or rolled back to a savepoint
    /// before the refused push) before it can commit.
    ///
    /// # Errors
    ///
    /// Returns the refused push's error.
    pub(crate) fn check(&self) -> Result<()> {
        match &self.pending.lock().fault {
            Some(fault) => Err(fault.to_error()),
            None => Ok(()),
        }
    }

    /// Number of buffered records, used as a savepoint position.
    pub(crate) fn len(&self) -> usize {
        self.pending.lock().frames.len()
    }

    /// Bytes held by the buffered records.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.pending.lock().bytes
    }

    /// Drops the records added after position `len` (savepoint rollback),
    /// and a fault raised at or after it.
    pub(crate) fn truncate(&self, len: usize) {
        let mut pending = self.pending.lock();
        pending.frames.truncate(len);
        pending.bytes = pending
            .frames
            .iter()
            .map(|f| f.frame.len() + f.graph.as_ref().map_or(0, String::len))
            .sum();
        if pending.fault.as_ref().is_some_and(|f| f.at() >= len) {
            pending.fault = None;
        }
    }

    /// Drops every buffered record and any fault (rollback).
    pub(crate) fn clear(&self) {
        *self.pending.lock() = Pending::default();
    }

    /// Writes the buffered records as one group, closed by `markers`.
    ///
    /// The markers are written even when no record is buffered.
    ///
    /// # Errors
    ///
    /// Returns the fault of a refused push without writing anything.
    /// Otherwise returns an error if the WAL write fails or the WAL is
    /// poisoned; a write failure has poisoned the WAL by then (see
    /// `TypedWal::log_encoded_or_poison`): the records may be partly on
    /// disk, and nothing may be appended after them. The buffered records are
    /// dropped either way.
    pub(crate) fn flush(&self, markers: &[WalRecord]) -> Result<()> {
        let pending = std::mem::take(&mut *self.pending.lock());
        if let Some(fault) = pending.fault {
            return Err(fault.to_error());
        }
        self.write_group(&pending.frames, markers)
    }

    /// Writes `records` (default graph) right away as their own implicit
    /// group, leaving the buffer alone: for schema changes, which take effect
    /// immediately and are not undone by a rollback, and for `GrafeoDB`-level
    /// writes. Not subject to the cap.
    ///
    /// # Errors
    ///
    /// Returns an error if a record cannot be encoded, the WAL write fails or
    /// the WAL is poisoned.
    pub(crate) fn write_implicit_group(&self, records: &[WalRecord], epoch: EpochId) -> Result<()> {
        let frames = records
            .iter()
            .map(|record| {
                Ok(PendingFrame {
                    graph: None,
                    frame: LpgWal::encode(record)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.write_group(&frames, &implicit_markers(epoch))
    }

    /// Writes `frames` and `markers` as one group in one append, poisoning
    /// the WAL on a write failure.
    fn write_group(&self, frames: &[PendingFrame], markers: &[WalRecord]) -> Result<()> {
        let slots = layout(frames.iter().map(|f| f.graph.as_deref()), markers.len());
        if slots.is_empty() {
            return Ok(());
        }
        // Encode the switches and markers; the data frames are already
        // encoded and are written from the buffer without a copy.
        let extra = slots
            .iter()
            .map(|slot| match slot {
                Slot::Switch(name) => {
                    LpgWal::encode(&WalRecord::SwitchGraph { name: name.clone() })
                }
                Slot::Marker(i) => LpgWal::encode(&markers[*i]),
                Slot::Frame(_) => Ok(Vec::new()),
            })
            .collect::<Result<Vec<_>>>()?;
        let refs: Vec<&[u8]> = slots
            .iter()
            .zip(&extra)
            .map(|(slot, encoded)| match slot {
                Slot::Frame(i) => frames[*i].frame.as_slice(),
                Slot::Switch(_) | Slot::Marker(_) => encoded.as_slice(),
            })
            .collect();
        let force_sync = markers.iter().any(WalEntry::requires_sync);
        self.wal.log_encoded_or_poison(&refs, force_sync)
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
        {
            let mut pending = self.pending.lock();
            if let Some(fault) = pending.fault.take() {
                *pending = Pending::default();
                let error = fault.to_error();
                self.wal.poison(format!(
                    "a write outside a transaction could not be logged: {error}"
                ));
                return Err(error);
            }
            if pending.frames.is_empty() {
                return Ok(());
            }
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

/// One frame of a group: a buffered data frame, a `SwitchGraph`, or a
/// marker.
#[derive(Debug, PartialEq, Eq)]
enum Slot {
    Frame(usize),
    Switch(Option<String>),
    Marker(usize),
}

/// Lays out a group: the data frames (given by their graphs) with a
/// `SwitchGraph` wherever the graph changes, a switch back to the default
/// graph if needed, then the markers.
fn layout<'a>(graphs: impl Iterator<Item = Option<&'a str>>, markers: usize) -> Vec<Slot> {
    let mut slots = Vec::new();
    let mut context: Option<&str> = None;
    for (i, graph) in graphs.enumerate() {
        if graph != context {
            slots.push(Slot::Switch(graph.map(str::to_string)));
            context = graph;
        }
        slots.push(Slot::Frame(i));
    }
    if context.is_some() {
        slots.push(Slot::Switch(None));
    }
    slots.extend((0..markers).map(Slot::Marker));
    slots
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

    /// Short form of a layout for assertions.
    fn shape(slots: &[Slot]) -> Vec<String> {
        slots
            .iter()
            .map(|slot| match slot {
                Slot::Frame(i) => format!("frame {i}"),
                Slot::Switch(name) => format!("switch {name:?}"),
                Slot::Marker(i) => format!("marker {i}"),
            })
            .collect()
    }

    /// Every record in the WAL at `dir`, committed ones only.
    fn wal_records(dir: &std::path::Path) -> Vec<WalRecord> {
        grafeo_storage::wal::WalRecovery::new(dir)
            .recover()
            .unwrap()
    }

    #[test]
    fn default_graph_group_has_no_switches() {
        let slots = layout([None, None].into_iter(), 1);
        assert_eq!(shape(&slots), ["frame 0", "frame 1", "marker 0"]);
    }

    #[test]
    fn group_switches_graphs_and_returns_to_default() {
        let slots = layout([Some("g"), Some("g"), None, Some("h")].into_iter(), 2);
        assert_eq!(
            shape(&slots),
            [
                "switch Some(\"g\")",
                "frame 0",
                "frame 1",
                "switch None",
                "frame 2",
                "switch Some(\"h\")",
                "frame 3",
                "switch None",
                "marker 0",
                "marker 1",
            ]
        );
    }

    #[test]
    fn empty_group_is_only_markers() {
        assert_eq!(shape(&layout(std::iter::empty(), 1)), ["marker 0"]);
        assert!(layout(std::iter::empty(), 0).is_empty());
    }

    #[test]
    fn flushed_group_decodes_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let buffer = WalBuffer::new(Arc::new(LpgWal::open(dir.path()).unwrap()));
        buffer.push(Some("g".to_string()), create(1));
        buffer.push(None, create(2));
        buffer
            .flush(&[
                commit(),
                WalRecord::EpochAdvance {
                    epoch: EpochId::new(5),
                },
            ])
            .unwrap();
        let records = wal_records(dir.path());
        let names: Vec<String> = records
            .iter()
            .map(|record| match record {
                WalRecord::CreateNode { id, .. } => format!("node {}", id.as_u64()),
                WalRecord::SwitchGraph { name } => format!("switch {name:?}"),
                WalRecord::TransactionCommit { .. } => "commit".to_string(),
                WalRecord::EpochAdvance { epoch } => format!("epoch {}", epoch.as_u64()),
                other => format!("{other:?}"),
            })
            .collect();
        assert_eq!(
            names[..5],
            [
                "switch Some(\"g\")",
                "node 1",
                "switch None",
                "node 2",
                "commit",
            ]
        );
    }

    #[test]
    fn truncate_and_clear() {
        let dir = tempfile::tempdir().unwrap();
        let buffer = WalBuffer::new(Arc::new(LpgWal::open(dir.path()).unwrap()));
        buffer.push(None, create(1));
        let savepoint = buffer.len();
        let bytes_at_savepoint = buffer.bytes();
        buffer.push(None, create(2));
        buffer.push(None, create(3));
        buffer.truncate(savepoint);
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.bytes(), bytes_at_savepoint);
        buffer.clear();
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.bytes(), 0);
        // Nothing buffered: an implicit flush writes nothing.
        buffer.flush_implicit(EpochId::new(1)).unwrap();
        assert_eq!(buffer.wal().record_count(), 0);
    }

    #[test]
    fn push_over_the_cap_faults_until_rolled_back() {
        let dir = tempfile::tempdir().unwrap();
        let one = LpgWal::encode(&create(1)).unwrap().len();
        let buffer = WalBuffer::for_database(
            Arc::new(LpgWal::open(dir.path()).unwrap()),
            Arc::new(Mutex::new(())),
            Some(2 * one),
        );
        buffer.push(None, create(1));
        let savepoint = buffer.len();
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

        // Rolling back to a savepoint before the refused push clears it.
        buffer.push(None, create(1));
        buffer.push(None, create(2));
        buffer.push(None, create(3));
        assert!(buffer.check().is_err());
        buffer.truncate(savepoint);
        buffer.check().unwrap();
        buffer.flush(&[commit()]).unwrap();
        assert_eq!(wal_records(dir.path()).len(), 2, "node 1 and the commit");
    }

    #[test]
    fn refused_push_outside_a_transaction_poisons_the_wal() {
        let dir = tempfile::tempdir().unwrap();
        let buffer = WalBuffer::for_database(
            Arc::new(LpgWal::open(dir.path()).unwrap()),
            Arc::new(Mutex::new(())),
            Some(1),
        );
        buffer.push(None, create(1));
        assert!(buffer.flush_implicit(EpochId::new(1)).is_err());
        assert!(buffer.wal().poisoned_reason().is_some());
    }
}
