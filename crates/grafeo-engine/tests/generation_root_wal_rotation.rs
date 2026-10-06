//! A size-based WAL rotation inside a transaction must not stop a generation
//! root from reopening.
//!
//! Before transaction groups (#411), data records were appended one group
//! per mutation, so a `max_log_size` boundary could fall between two records
//! of one transaction. The rotated file is final (flushed and fsynced by
//! `rotate()`), so a rotated file that ends with the transaction open is not
//! damage: the transaction continues in the next file.
//!
//! The straddle tests write that old-style WAL by hand: a published base,
//! then raw `WalRecord`s appended through a `WalManager` with a tiny
//! `max_log_size` on `root/wal`, then a reopen. That keeps them straddling
//! however the engine groups its own writes, and it is the case the reader
//! rule exists for once writes are grouped: WAL written before that.
//!
//! The live-session test shrinks the root WAL's `max_log_size` through a
//! debug-only seam (a process-wide static; nextest runs each test in its
//! own process, and every test here sets the same value).

#![cfg(all(
    debug_assertions,
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal"
))]

use std::path::Path;
use std::sync::atomic::Ordering;

use grafeo_common::types::{EpochId, NodeId, TransactionId, Value};
use grafeo_engine::{GENERATION_ROOT_WAL_MAX_LOG_SIZE, GrafeoDB, generation_build_request};
use grafeo_storage::wal::{WalConfig, WalManager, WalRecord};
use tempfile::tempdir;

/// Small enough that a rotation lands after almost every append group.
const TINY_MAX_LOG_SIZE: u64 = 256;
/// Statements per transaction (each logs at least one WAL record).
const RECORDS_PER_TX: i64 = 30;

fn tiny_wal() {
    GENERATION_ROOT_WAL_MAX_LOG_SIZE.store(TINY_MAX_LOG_SIZE, Ordering::Release);
}

/// Publish a small base generation (two nodes) at `root`.
fn publish_base(root: &Path, generation_id: &str) {
    std::fs::create_dir_all(root).expect("create generation root");
    let source = GrafeoDB::new_in_memory();
    source
        .create_node_with_props(&["Person"], [("name", Value::from("Ada"))])
        .expect("create Ada");
    source
        .create_node_with_props(&["Person"], [("name", Value::from("Grace"))])
        .expect("create Grace");
    source
        .build_and_publish_generation(generation_build_request(root, generation_id))
        .expect("publish base generation");
}

fn count(db: &GrafeoDB, label: &str) -> i64 {
    let result = db
        .session()
        .execute(&format!("MATCH (n:{label}) RETURN count(n)"))
        .expect("count query");
    match &result.rows()[0][0] {
        Value::Int64(v) => *v,
        other => panic!("expected integer count, got {other:?}"),
    }
}

fn wal_file_count(root: &Path) -> usize {
    std::fs::read_dir(root.join("wal"))
        .expect("read wal dir")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "log"))
        .count()
}

/// One committed explicit transaction of `RECORDS_PER_TX` inserts.
fn explicit_tx(db: &GrafeoDB, label: &str, tag: i64) {
    let session = db.session();
    session.execute("START TRANSACTION").expect("begin");
    for i in 0..RECORDS_PER_TX {
        session
            .execute(&format!("INSERT (:{label} {{tag: {tag}, i: {i}}})"))
            .expect("insert inside explicit tx");
    }
    session.execute("COMMIT").expect("commit");
}

/// Old-style (ungrouped) WAL on a closed root: one append group per record,
/// rotating by size after almost every record.
struct RawWal {
    wal: WalManager,
}

impl RawWal {
    fn open(root: &Path) -> Self {
        let wal = WalManager::with_config(
            root.join("wal"),
            WalConfig {
                max_log_size: TINY_MAX_LOG_SIZE / 4,
                ..WalConfig::default()
            },
        )
        .expect("open root WAL");
        Self { wal }
    }

    /// `RECORDS_PER_TX` nodes with `label`, ids from `first_id`, each with a
    /// property set in its own append group.
    fn data(&self, label: &str, first_id: u64) {
        for i in 0..RECORDS_PER_TX {
            let id = NodeId::new(first_id + u64::try_from(i).unwrap());
            self.wal
                .log(&WalRecord::CreateNode {
                    id,
                    labels: vec![label.to_string()],
                })
                .unwrap();
            self.wal
                .log(&WalRecord::SetNodeProperty {
                    id,
                    key: "i".to_string(),
                    value: Value::Int64(i),
                })
                .unwrap();
        }
    }

    /// Commit pair, written as the engine writes it: one atomic group.
    fn commit(&self, tx: u64, epoch: u64) {
        self.wal
            .log(&WalRecord::TransactionCommit {
                transaction_id: TransactionId::new(tx),
            })
            .unwrap();
        self.wal
            .log(&WalRecord::EpochAdvance {
                epoch: EpochId::new(epoch),
            })
            .unwrap();
    }

    fn abort(&self, tx: u64) {
        self.wal
            .log(&WalRecord::TransactionAbort {
                transaction_id: TransactionId::new(tx),
            })
            .unwrap();
    }

    /// Flush and drop without writing any marker (the crash shape).
    fn close(self) {
        self.wal.sync().unwrap();
    }

    fn sequence(&self) -> u64 {
        self.wal.current_sequence()
    }
}

#[test]
fn transaction_straddling_size_rotation_survives_reopen() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("straddle.grafeo.d");
    publish_base(&root, "straddle-g1");

    let raw = RawWal::open(&root);
    let first = raw.sequence();
    raw.data("Batch", 1_000);
    assert!(
        raw.sequence() >= first + 2,
        "the transaction must cross at least two size rotations"
    );
    raw.commit(1_000, 1_000);
    raw.close();

    for read_only in [false, true, false] {
        let db = GrafeoDB::open_generation_root(&root, read_only)
            .unwrap_or_else(|e| panic!("reopen (read_only={read_only}) failed: {e}"));
        assert_eq!(count(&db, "Batch"), RECORDS_PER_TX);
        assert_eq!(count(&db, "Person"), 2);
    }
}

#[test]
fn straddling_transactions_commit_and_abort_across_rotations() {
    // Committed, aborted, committed: each straddles several files.
    let dir = tempdir().unwrap();
    let root = dir.path().join("mixed.grafeo.d");
    publish_base(&root, "mixed-g1");

    let raw = RawWal::open(&root);
    let first = raw.sequence();
    raw.data("First", 1_000);
    raw.commit(1_000, 1_000);
    raw.data("Aborted", 2_000);
    raw.abort(1_001);
    raw.data("Second", 3_000);
    raw.commit(1_002, 1_001);
    assert!(raw.sequence() >= first + 6, "every transaction straddles");
    raw.close();

    for read_only in [false, true] {
        let db = GrafeoDB::open_generation_root(&root, read_only)
            .unwrap_or_else(|e| panic!("reopen (read_only={read_only}) failed: {e}"));
        assert_eq!(count(&db, "First"), RECORDS_PER_TX);
        assert_eq!(count(&db, "Aborted"), 0);
        assert_eq!(count(&db, "Second"), RECORDS_PER_TX);
    }
}

#[test]
fn straddling_transaction_open_at_active_end_stays_uncommitted() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("open-tail.grafeo.d");
    publish_base(&root, "open-tail-g1");

    // A committed transaction, then one whose records span rotated files
    // and the active file with no marker: the state a crash leaves.
    let raw = RawWal::open(&root);
    raw.data("Committed", 1_000);
    raw.commit(1_000, 1_000);
    let open_start = raw.sequence();
    raw.data("Open", 2_000);
    assert!(
        raw.sequence() >= open_start + 2,
        "the open transaction must cross at least two size rotations"
    );
    raw.close();

    {
        let db = GrafeoDB::open_generation_root(&root, false).expect("open writable");
        assert_eq!(count(&db, "Committed"), RECORDS_PER_TX);
        assert_eq!(
            count(&db, "Open"),
            0,
            "the open transaction stays uncommitted"
        );
        // A later commit must not pick up the open transaction's records.
        let session = db.session();
        session
            .execute("INSERT (:After {i: 0})")
            .expect("commit after reopen");
    }
    for read_only in [false, true] {
        let db = GrafeoDB::open_generation_root(&root, read_only)
            .unwrap_or_else(|e| panic!("second reopen (read_only={read_only}) failed: {e}"));
        assert_eq!(count(&db, "Committed"), RECORDS_PER_TX);
        assert_eq!(count(&db, "Open"), 0);
        assert_eq!(count(&db, "After"), 1);
    }
}

#[test]
fn concurrent_writer_sessions_with_size_rotations_survive_reopen() {
    // Live sessions on a root whose WAL rotates every few appends. Whether a
    // transaction straddles depends on how the engine groups its records;
    // either way the reopen must keep every committed write.
    tiny_wal();
    let dir = tempdir().unwrap();
    let root = dir.path().join("concurrent.grafeo.d");
    publish_base(&root, "concurrent-g1");

    const TXS_PER_WRITER: i64 = 3;
    {
        let db = GrafeoDB::open_generation_root(&root, false).expect("open writable");
        let files_before = wal_file_count(&root);
        std::thread::scope(|scope| {
            for label in ["WriterA", "WriterB"] {
                let db = &db;
                scope.spawn(move || {
                    for tag in 0..TXS_PER_WRITER {
                        explicit_tx(db, label, tag);
                    }
                });
            }
        });
        assert!(wal_file_count(&root) > files_before, "the WAL rotated");
        assert_eq!(count(&db, "WriterA"), TXS_PER_WRITER * RECORDS_PER_TX);
        assert_eq!(count(&db, "WriterB"), TXS_PER_WRITER * RECORDS_PER_TX);
    }

    for read_only in [false, true] {
        let db = GrafeoDB::open_generation_root(&root, read_only)
            .unwrap_or_else(|e| panic!("reopen (read_only={read_only}) failed: {e}"));
        assert_eq!(count(&db, "WriterA"), TXS_PER_WRITER * RECORDS_PER_TX);
        assert_eq!(count(&db, "WriterB"), TXS_PER_WRITER * RECORDS_PER_TX);
    }
}
