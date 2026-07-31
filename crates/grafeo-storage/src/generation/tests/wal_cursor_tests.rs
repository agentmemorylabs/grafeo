//! WAL cursor tests: frame-aligned cuts, replayability validation, gap and
//! missing-file failures, truncation floors, and dual-slot retention.

use std::path::PathBuf;

use grafeo_common::types::{NodeId, TransactionId};

use crate::wal::WalManager;
use crate::wal::WalRecord;

use super::{
    WalReplayCursor, cut_generation_boundary, earliest_retained_cursor, truncate_before,
    validate_replayable,
};
use tempfile::TempDir;

fn log_records(wal: &WalManager, count: u64) {
    for i in 0..count {
        wal.log(&WalRecord::CreateNode {
            id: NodeId::new(i + 1),
            labels: vec!["Person".to_string()],
        })
        .unwrap();
        wal.log(&WalRecord::TransactionCommit {
            transaction_id: TransactionId::new(i + 1),
        })
        .unwrap();
    }
}

fn fixture_wal() -> (TempDir, WalManager) {
    let dir = TempDir::new().unwrap();
    let wal = WalManager::open(dir.path()).unwrap();
    (dir, wal)
}

fn cursor_at(seq: u64, offset: u64) -> WalReplayCursor {
    WalReplayCursor {
        log_sequence: seq,
        byte_offset: offset,
        epoch: 0,
        transaction_id: 0,
    }
}

#[test]
fn wal_cut_returns_frame_aligned_cursor() {
    let (dir, wal) = fixture_wal();
    log_records(&wal, 5);

    let cut = cut_generation_boundary(&wal).unwrap();
    assert_eq!(cut.cursor.byte_offset, 0, "new file starts at offset 0");
    assert_eq!(cut.cursor.log_sequence, wal.current_sequence());
    assert!(
        cut.cursor.log_sequence > 0,
        "rotate must advance past the initial file"
    );

    // Every retained file is a wal_*.log file in the WAL dir.
    let files: Vec<PathBuf> = cut.retained_log_files;
    assert!(files.len() >= 2, "initial file + rotated file: {files:?}");
    assert!(
        files
            .iter()
            .all(|p| p.extension().is_some_and(|e| e == "log"))
    );

    // The new file exists and is empty (frame-aligned trivially).
    let new_path = dir
        .path()
        .join(format!("wal_{:08}.log", cut.cursor.log_sequence));
    assert!(new_path.exists());

    // Cursor epoch/txn come from checkpoint metadata (none here → 0).
    assert_eq!(cut.cursor.epoch, 0);
    assert_eq!(cut.cursor.transaction_id, 0);
}

#[test]
fn wal_validate_replayable_success() {
    let (_dir, wal) = fixture_wal();
    log_records(&wal, 3);
    let cut = cut_generation_boundary(&wal).unwrap();

    validate_replayable(wal.dir(), &cut.cursor).expect("cursor must be replayable");
}

#[test]
fn wal_validate_missing_file_fails() {
    let (dir, wal) = fixture_wal();
    log_records(&wal, 3);
    let cut = cut_generation_boundary(&wal).unwrap();

    let path = dir
        .path()
        .join(format!("wal_{:08}.log", cut.cursor.log_sequence));
    std::fs::remove_file(&path).unwrap();

    match validate_replayable(wal.dir(), &cut.cursor) {
        Err(super::WalCursorError::MissingFile(seq)) => assert_eq!(seq, cut.cursor.log_sequence),
        other => panic!("expected MissingFile, got {other:?}"),
    }
}

#[test]
fn wal_validate_gap_fails() {
    let (_dir, wal) = fixture_wal();
    log_records(&wal, 2);
    let cut1 = cut_generation_boundary(&wal).unwrap();
    cut_generation_boundary(&wal).unwrap();
    let cut3 = cut_generation_boundary(&wal).unwrap();
    assert!(cut3.cursor.log_sequence >= cut1.cursor.log_sequence + 2);

    // Delete the intermediate file between cut1's cursor and the newest.
    let middle_seq = cut1.cursor.log_sequence + 1;
    let path = wal.dir().join(format!("wal_{middle_seq:08}.log"));
    assert!(path.exists());
    std::fs::remove_file(&path).unwrap();

    match validate_replayable(wal.dir(), &cut1.cursor) {
        Err(super::WalCursorError::SequenceGap { expected, found }) => {
            assert_eq!(expected, middle_seq);
            assert!(found > expected);
        }
        other => panic!("expected SequenceGap, got {other:?}"),
    }
}

#[test]
fn wal_truncate_preserves_floor() {
    let (dir, wal) = fixture_wal();
    log_records(&wal, 2);
    let cut1 = cut_generation_boundary(&wal).unwrap();
    let cut2 = cut_generation_boundary(&wal).unwrap();

    let deleted = truncate_before(wal.dir(), &cut2.cursor).unwrap();

    // First cursor's file is older than the floor → deleted.
    let first_file = dir
        .path()
        .join(format!("wal_{:08}.log", cut1.cursor.log_sequence));
    assert!(
        deleted.contains(&first_file),
        "first cursor file must be deleted: {deleted:?}"
    );
    assert!(!first_file.exists());

    // Floor's own file is preserved.
    let floor_file = dir
        .path()
        .join(format!("wal_{:08}.log", cut2.cursor.log_sequence));
    assert!(floor_file.exists());
    assert!(!deleted.contains(&floor_file));

    // The floor cursor remains replayable after truncation.
    validate_replayable(wal.dir(), &cut2.cursor).expect("floor cursor must stay replayable");
}

#[test]
fn wal_dual_slot_retention() {
    let (dir, wal) = fixture_wal();
    log_records(&wal, 2);
    let cut1 = cut_generation_boundary(&wal).unwrap();
    let cut2 = cut_generation_boundary(&wal).unwrap();

    let floor = earliest_retained_cursor(&cut2.cursor, Some(&cut1.cursor));
    assert_eq!(floor, cut1.cursor, "older cursor must win as the floor");

    let deleted = truncate_before(wal.dir(), &floor).unwrap();

    // Both slots' cursor files survive the floor truncation.
    for seq in [cut1.cursor.log_sequence, cut2.cursor.log_sequence] {
        let path = dir.path().join(format!("wal_{seq:08}.log"));
        assert!(path.exists(), "cursor file {path:?} must be preserved");
    }

    // The pre-cut initial file was deleted.
    let initial = dir.path().join("wal_00000000.log");
    assert!(
        deleted.contains(&initial),
        "initial file must be eligible: {deleted:?}"
    );

    // Both cursors still validate.
    validate_replayable(wal.dir(), &cut1.cursor).unwrap();
    validate_replayable(wal.dir(), &cut2.cursor).unwrap();
}

#[test]
fn wal_earliest_retained_cursor_handles_none() {
    let c = cursor_at(9, 0);
    assert_eq!(earliest_retained_cursor(&c, None), c);
}
