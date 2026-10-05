//! A size-based WAL rotation inside a transaction must not stop a generation
//! root from reopening.
//!
//! Data records are appended one group per mutation, so a `max_log_size`
//! boundary can fall between two records of one transaction. The rotated
//! file is final (flushed and fsynced by `rotate()`), so a rotated file that
//! ends with the transaction open is not damage: the transaction continues
//! in the next file. These tests shrink the root WAL's `max_log_size`
//! through a debug-only seam so every transaction crosses several files.
//!
//! The seam is a process-wide static; nextest runs each test in its own
//! process, and every test here sets the same value.

#![cfg(all(
    debug_assertions,
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal"
))]

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use grafeo_common::types::Value;
use grafeo_engine::{GENERATION_ROOT_WAL_MAX_LOG_SIZE, GrafeoDB, generation_build_request};
use tempfile::tempdir;

/// Small enough that a rotation lands after almost every append group.
const TINY_MAX_LOG_SIZE: u64 = 256;
/// Statements per transaction (each logs at least one WAL record).
const RECORDS_PER_TX: usize = 30;

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

/// One explicit transaction of `RECORDS_PER_TX` inserts with `label`.
fn explicit_tx(db: &GrafeoDB, label: &str, tag: usize, commit: bool) {
    let session = db.session();
    session.execute("START TRANSACTION").expect("begin");
    for i in 0..RECORDS_PER_TX {
        session
            .execute(&format!("INSERT (:{label} {{tag: {tag}, i: {i}}})"))
            .expect("insert inside explicit tx");
    }
    if commit {
        session.execute("COMMIT").expect("commit");
    } else {
        // Leave the transaction open: dropping the session would log a
        // rollback marker, which a crash never writes.
        std::mem::forget(session);
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target: PathBuf = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

#[test]
fn transaction_straddling_size_rotation_survives_reopen() {
    tiny_wal();
    let dir = tempdir().unwrap();
    let root = dir.path().join("straddle.grafeo.d");
    publish_base(&root, "straddle-g1");

    {
        let db = GrafeoDB::open_generation_root(&root, false).expect("open writable");
        let files_before = wal_file_count(&root);
        explicit_tx(&db, "Batch", 1, true);
        assert!(
            wal_file_count(&root) >= files_before + 2,
            "the transaction must cross at least two size rotations"
        );
        assert_eq!(count(&db, "Batch"), RECORDS_PER_TX as i64);
    }

    for read_only in [false, true, false] {
        let db = GrafeoDB::open_generation_root(&root, read_only)
            .unwrap_or_else(|e| panic!("reopen (read_only={read_only}) failed: {e}"));
        assert_eq!(count(&db, "Batch"), RECORDS_PER_TX as i64);
        assert_eq!(count(&db, "Person"), 2);
    }
}

#[test]
fn concurrent_writer_sessions_straddling_rotations_survive_reopen() {
    tiny_wal();
    let dir = tempdir().unwrap();
    let root = dir.path().join("concurrent.grafeo.d");
    publish_base(&root, "concurrent-g1");

    const TXS_PER_WRITER: usize = 3;
    {
        let db = GrafeoDB::open_generation_root(&root, false).expect("open writable");
        std::thread::scope(|scope| {
            for label in ["WriterA", "WriterB"] {
                let db = &db;
                scope.spawn(move || {
                    for tag in 0..TXS_PER_WRITER {
                        explicit_tx(db, label, tag, true);
                    }
                });
            }
        });
        assert_eq!(
            count(&db, "WriterA"),
            (TXS_PER_WRITER * RECORDS_PER_TX) as i64
        );
        assert_eq!(
            count(&db, "WriterB"),
            (TXS_PER_WRITER * RECORDS_PER_TX) as i64
        );
    }

    for read_only in [false, true] {
        let db = GrafeoDB::open_generation_root(&root, read_only)
            .unwrap_or_else(|e| panic!("reopen (read_only={read_only}) failed: {e}"));
        assert_eq!(
            count(&db, "WriterA"),
            (TXS_PER_WRITER * RECORDS_PER_TX) as i64
        );
        assert_eq!(
            count(&db, "WriterB"),
            (TXS_PER_WRITER * RECORDS_PER_TX) as i64
        );
    }
}

#[test]
fn straddling_transaction_open_at_active_end_stays_uncommitted() {
    tiny_wal();
    let dir = tempdir().unwrap();
    let root = dir.path().join("open-tail.grafeo.d");
    let copy = dir.path().join("open-tail-copy.grafeo.d");
    publish_base(&root, "open-tail-g1");

    {
        let db = GrafeoDB::open_generation_root(&root, false).expect("open writable");
        explicit_tx(&db, "Committed", 1, true);
        let files_before = wal_file_count(&root);
        explicit_tx(&db, "Open", 2, false);
        assert!(
            wal_file_count(&root) >= files_before + 2,
            "the open transaction must cross at least two size rotations"
        );
        // The on-disk state a crash would leave: the open transaction's
        // records span rotated files and the active file, with no marker.
        copy_dir(&root, &copy);
    }

    {
        let db = GrafeoDB::open_generation_root(&copy, false).expect("open the copy writable");
        assert_eq!(count(&db, "Committed"), RECORDS_PER_TX as i64);
        assert_eq!(
            count(&db, "Open"),
            0,
            "the open transaction stays uncommitted"
        );
        // A later commit must not pick up the open transaction's records.
        explicit_tx(&db, "After", 3, true);
    }
    let db = GrafeoDB::open_generation_root(&copy, false).expect("second reopen");
    assert_eq!(count(&db, "Committed"), RECORDS_PER_TX as i64);
    assert_eq!(count(&db, "Open"), 0);
    assert_eq!(count(&db, "After"), RECORDS_PER_TX as i64);
}
