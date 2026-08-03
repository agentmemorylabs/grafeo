//! H-ADOPT.3 Phase A tests: bounded streaming WAL-replay scanner.
//!
//! Fixtures are built with the real [`WalManager`] writing real frames;
//! corruption cases are hand-crafted by byte-editing files after writing.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use grafeo_common::types::{EpochId, NodeId, TransactionId};
use tempfile::TempDir;

use crate::generation::records::MAX_RECORD_BODY_BYTES;
use crate::wal::{WalManager, WalRecord};

use super::{
    ReplayFrame, WalCursorError, WalReplayCursor, WalReplayStream, replay_stream_from,
    validate_replayable,
};

fn cursor_at(seq: u64, offset: u64) -> WalReplayCursor {
    WalReplayCursor {
        log_sequence: seq,
        byte_offset: offset,
        epoch: 0,
        transaction_id: 0,
    }
}

fn fixture_wal() -> (TempDir, WalManager) {
    let dir = TempDir::new().unwrap();
    let wal = WalManager::open(dir.path()).unwrap();
    (dir, wal)
}

/// One committed transaction: `[CreateNode, TransactionCommit, EpochAdvance]`.
fn log_committed_tx(wal: &WalManager, tx: u64) {
    wal.log(&WalRecord::CreateNode {
        id: NodeId::new(tx),
        labels: vec!["Person".to_string()],
    })
    .unwrap();
    wal.log(&WalRecord::TransactionCommit {
        transaction_id: TransactionId::new(tx),
    })
    .unwrap();
    wal.log(&WalRecord::EpochAdvance {
        epoch: EpochId::new(tx),
    })
    .unwrap();
}

fn wal_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("wal_{seq:08}.log"))
}

fn file_len(dir: &Path, seq: u64) -> u64 {
    File::open(wal_path(dir, seq))
        .unwrap()
        .metadata()
        .unwrap()
        .len()
}

/// Frame offsets (offset of each frame's length prefix) in one WAL file.
fn frame_offsets(dir: &Path, seq: u64) -> Vec<u64> {
    let mut f = File::open(wal_path(dir, seq)).unwrap();
    let len = f.metadata().unwrap().len();
    let mut offsets = Vec::new();
    let mut offset = 0u64;
    loop {
        let mut len_buf = [0u8; 4];
        if f.read_exact(&mut len_buf).is_err() {
            break;
        }
        offsets.push(offset);
        let frame_len = u32::from_le_bytes(len_buf) as u64;
        let advance = 4 + frame_len + 4;
        offset += advance;
        if offset > len || f.seek(SeekFrom::Start(offset)).is_err() {
            break;
        }
    }
    offsets
}

/// Collect the stream to a Vec (test-only convenience; the stream itself is
/// O(one frame)).
fn collect_stream(stream: &mut WalReplayStream) -> Result<Vec<ReplayFrame>, WalCursorError> {
    let mut frames = Vec::new();
    for item in stream {
        frames.push(item?);
    }
    Ok(frames)
}

fn discriminant_name(r: &WalRecord) -> &'static str {
    match r {
        WalRecord::CreateNode { .. } => "CreateNode",
        WalRecord::DeleteNode { .. } => "DeleteNode",
        WalRecord::CreateEdge { .. } => "CreateEdge",
        WalRecord::DeleteEdge { .. } => "DeleteEdge",
        WalRecord::TransactionCommit { .. } => "TransactionCommit",
        WalRecord::TransactionAbort { .. } => "TransactionAbort",
        WalRecord::Checkpoint { .. } => "Checkpoint",
        WalRecord::EpochAdvance { .. } => "EpochAdvance",
        _ => "other",
    }
}

// ---------------------------------------------------------------------------
// 1. Committed frames across a rotation, cursor mid-file
// ---------------------------------------------------------------------------

#[test]
fn replay_stream_yields_committed_frames_across_rotation_from_mid_file() {
    let (_dir, wal) = fixture_wal();
    // Two committed txs in the initial file, then rotate, then one committed
    // tx in the second file.
    log_committed_tx(&wal, 1);
    log_committed_tx(&wal, 2);
    let cut = crate::generation::wal_cursor::cut_generation_boundary(&wal).unwrap();
    let seq0 = cut.cursor.log_sequence - 1;
    let seq1 = cut.cursor.log_sequence;
    log_committed_tx(&wal, 3);
    wal.flush().unwrap();

    let dir = wal.dir().to_path_buf();
    let offsets0 = frame_offsets(&dir, seq0);
    assert_eq!(offsets0.len(), 6, "two committed txs = 6 frames");
    let mid = offsets0[3]; // start of the second transaction

    let mut stream =
        replay_stream_from(&dir, &cursor_at(seq0, mid)).expect("stream from mid-file cursor");
    let frames = collect_stream(&mut stream).expect("clean stream");

    // Tx2 (frames 3..6 of file 0) + tx3 (all frames of file 1).
    assert_eq!(frames.len(), 6);

    let expect_variant = |f: &ReplayFrame, name: &str, seq: u64, off: u64| {
        assert_eq!(
            discriminant_name(&f.record),
            name,
            "record variant mismatch at ({seq},{off}): got {:?}",
            f.record
        );
        assert_eq!(f.log_sequence, seq, "wrong file for frame at offset {off}");
        assert_eq!(f.byte_offset, off, "wrong byte_offset");
    };

    expect_variant(&frames[0], "CreateNode", seq0, offsets0[3]);
    expect_variant(&frames[1], "TransactionCommit", seq0, offsets0[4]);
    expect_variant(&frames[2], "EpochAdvance", seq0, offsets0[5]);

    // Record identity: the second tx's CreateNode carries NodeId(2).
    match &frames[0].record {
        WalRecord::CreateNode { id, labels } => {
            assert_eq!(id.as_u64(), 2);
            assert_eq!(labels.len(), 1);
            assert_eq!(labels[0], "Person");
        }
        other => panic!("expected CreateNode, got {other:?}"),
    }

    let offsets1 = frame_offsets(&dir, seq1);
    assert_eq!(offsets1.len(), 3);
    expect_variant(&frames[3], "CreateNode", seq1, offsets1[0]);
    expect_variant(&frames[4], "TransactionCommit", seq1, offsets1[1]);
    expect_variant(&frames[5], "EpochAdvance", seq1, offsets1[2]);
    match &frames[3].record {
        WalRecord::CreateNode { id, .. } => assert_eq!(id.as_u64(), 3),
        other => panic!("expected CreateNode, got {other:?}"),
    }

    // Positions strictly increasing across the rotation.
    let mut prev: Option<(u64, u64)> = None;
    for f in &frames {
        if let Some((ps, po)) = prev {
            assert!(
                f.log_sequence > ps || (f.log_sequence == ps && f.byte_offset > po),
                "positions must be strictly increasing: ({ps},{po}) -> ({},{})",
                f.log_sequence,
                f.byte_offset
            );
        }
        prev = Some((f.log_sequence, f.byte_offset));
    }

    // Clean termination at the end of the active file: stopped_at = EOF.
    assert_eq!(stream.stopped_at(), Some((seq1, file_len(&dir, seq1))));
}

#[test]
fn replay_stream_offset_at_eof_continues_into_next_file() {
    let (_dir, wal) = fixture_wal();
    log_committed_tx(&wal, 1);
    let cut = crate::generation::wal_cursor::cut_generation_boundary(&wal).unwrap();
    let seq0 = cut.cursor.log_sequence - 1;
    let seq1 = cut.cursor.log_sequence;
    log_committed_tx(&wal, 2);
    wal.flush().unwrap();

    let dir = wal.dir().to_path_buf();
    let eof = file_len(&dir, seq0);

    let mut stream =
        replay_stream_from(&dir, &cursor_at(seq0, eof)).expect("cursor at EOF of cursor file");
    let frames = collect_stream(&mut stream).expect("clean stream");
    assert_eq!(frames.len(), 3, "frames come from the next file only");
    assert!(frames.iter().all(|f| f.log_sequence == seq1));

    let offsets1 = frame_offsets(&dir, seq1);
    for (f, off) in frames.iter().zip(offsets1.iter()) {
        assert_eq!(f.byte_offset, *off);
    }
}

#[test]
fn replay_stream_cursor_file_is_active_mid_file() {
    let (_dir, wal) = fixture_wal();
    log_committed_tx(&wal, 1);
    log_committed_tx(&wal, 2);
    wal.flush().unwrap();

    let dir = wal.dir().to_path_buf();
    let seq0 = wal.current_sequence();
    let offsets = frame_offsets(&dir, seq0);
    assert_eq!(offsets.len(), 6);

    let mut stream = replay_stream_from(&dir, &cursor_at(seq0, offsets[3])).unwrap();
    let frames = collect_stream(&mut stream).expect("clean stream");
    assert_eq!(frames.len(), 3, "tx2 only");
    assert!(frames.iter().all(|f| f.log_sequence == seq0));
    assert_eq!(stream.stopped_at(), Some((seq0, file_len(&dir, seq0))));
}

// ---------------------------------------------------------------------------
// 2. Torn tail in the active file: clean end + stopped_at
// ---------------------------------------------------------------------------

#[test]
fn replay_stream_torn_active_tail_ends_cleanly_with_stopped_at() {
    let (dir, wal) = fixture_wal();
    log_committed_tx(&wal, 1);
    // Begin tx2: one data record, no commit.
    wal.log(&WalRecord::CreateNode {
        id: NodeId::new(99),
        labels: vec!["Ghost".to_string()],
    })
    .unwrap();
    wal.flush().unwrap();
    let seq = wal.current_sequence();
    drop(wal);

    // Truncate the active file mid-frame: keep the committed tx1 frames and
    // the torn tx2 length prefix + a few payload bytes.
    let offsets = frame_offsets(dir.path(), seq);
    assert_eq!(offsets.len(), 4, "tx1 (3 frames) + tx2 (1 frame)");
    let torn_frame_start = offsets[3];
    let partial_len = torn_frame_start + 7; // partial payload, no CRC
    {
        let f = OpenOptions::new()
            .write(true)
            .open(wal_path(dir.path(), seq))
            .unwrap();
        f.set_len(partial_len).unwrap();
    }

    let mut stream = replay_stream_from(dir.path(), &cursor_at(seq, 0))
        .expect("stream opens over torn active tail");
    let frames = collect_stream(&mut stream).expect("torn tail is not an error");
    assert_eq!(frames.len(), 3, "only committed tx1 frames are yielded");

    // The stream ended cleanly at the first partial frame position.
    assert_eq!(stream.stopped_at(), Some((seq, torn_frame_start)));
}

#[test]
fn replay_stream_partial_length_prefix_at_active_tail_is_clean_end() {
    let (dir, wal) = fixture_wal();
    log_committed_tx(&wal, 1);
    wal.flush().unwrap();
    let seq = wal.current_sequence();
    drop(wal);

    // Append 2 stray bytes: a partial length prefix with no frame after it.
    let torn_start = file_len(dir.path(), seq);
    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(wal_path(dir.path(), seq))
            .unwrap();
        f.write_all(&[0x07, 0x00]).unwrap();
    }

    let mut stream = replay_stream_from(dir.path(), &cursor_at(seq, 0)).unwrap();
    let frames = collect_stream(&mut stream).expect("partial prefix is tolerated");
    assert_eq!(frames.len(), 3);
    assert_eq!(stream.stopped_at(), Some((seq, torn_start)));
}

// ---------------------------------------------------------------------------
// 3. Corrupted CRC in a rotated file → FrameChecksum (never torn)
// ---------------------------------------------------------------------------

#[test]
fn replay_stream_corrupted_crc_in_rotated_file_is_frame_checksum() {
    let (_dir, wal) = fixture_wal();
    log_committed_tx(&wal, 1);
    let cut = crate::generation::wal_cursor::cut_generation_boundary(&wal).unwrap();
    let seq0 = cut.cursor.log_sequence - 1;
    log_committed_tx(&wal, 2);
    wal.flush().unwrap();

    let dir = wal.dir().to_path_buf();
    // Flip one payload byte of the first frame in the rotated file (frame
    // shape intact; only the CRC stops matching).
    {
        let mut f = OpenOptions::new()
            .write(true)
            .open(wal_path(&dir, seq0))
            .unwrap();
        f.seek(SeekFrom::Start(4)).unwrap(); // first payload byte
        f.write_all(&[0xFF]).unwrap();
    }

    let mut stream = replay_stream_from(&dir, &cursor_at(seq0, 0)).expect("stream opens");
    match stream.next() {
        Some(Err(WalCursorError::FrameChecksum { seq, offset })) => {
            assert_eq!(seq, seq0);
            assert_eq!(offset, 0);
        }
        other => panic!("expected FrameChecksum from stream, got {other:?}"),
    }
    // Fused after error: no further items, no stopped_at.
    assert!(stream.next().is_none());
    assert_eq!(stream.stopped_at(), None);
}

// ---------------------------------------------------------------------------
// 4. Gap / missing cursor file / misaligned offset
// ---------------------------------------------------------------------------

#[test]
fn replay_stream_sequence_gap_is_sequence_gap() {
    let (_dir, wal) = fixture_wal();
    log_committed_tx(&wal, 1);
    let cut1 = crate::generation::wal_cursor::cut_generation_boundary(&wal).unwrap();
    crate::generation::wal_cursor::cut_generation_boundary(&wal).unwrap();
    let cut3 = crate::generation::wal_cursor::cut_generation_boundary(&wal).unwrap();
    assert!(cut3.cursor.log_sequence >= cut1.cursor.log_sequence + 2);

    let dir = wal.dir().to_path_buf();
    let middle_seq = cut1.cursor.log_sequence + 1;
    std::fs::remove_file(wal_path(&dir, middle_seq)).unwrap();

    match replay_stream_from(&dir, &cut1.cursor) {
        Err(WalCursorError::SequenceGap { expected, .. }) => assert_eq!(expected, middle_seq),
        other => panic!("expected SequenceGap, got {other:?}"),
    }
}

#[test]
fn replay_stream_missing_cursor_file_is_missing_file() {
    let (dir, wal) = fixture_wal();
    log_committed_tx(&wal, 1);
    let cut = crate::generation::wal_cursor::cut_generation_boundary(&wal).unwrap();
    std::fs::remove_file(wal_path(dir.path(), cut.cursor.log_sequence)).unwrap();

    match replay_stream_from(dir.path(), &cut.cursor) {
        Err(WalCursorError::MissingFile(seq)) => assert_eq!(seq, cut.cursor.log_sequence),
        other => panic!("expected MissingFile, got {other:?}"),
    }
}

#[test]
fn replay_stream_offset_past_eof_is_frame_misaligned() {
    let (_dir, wal) = fixture_wal();
    log_committed_tx(&wal, 1);
    wal.flush().unwrap();
    let seq = wal.current_sequence();
    let len = file_len(wal.dir(), seq);

    match replay_stream_from(wal.dir(), &cursor_at(seq, len + 1)) {
        Err(WalCursorError::FrameMisaligned(off)) => assert_eq!(off, len + 1),
        other => panic!("expected FrameMisaligned, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 5. §A-cap: oversized length prefix → FrameOversized before allocation,
//    in BOTH the stream and validate_replayable paths
// ---------------------------------------------------------------------------

fn append_oversized_frame(dir: &Path, seq: u64) -> u64 {
    let path = wal_path(dir, seq);
    let offset = file_len(dir, seq);
    // Declare far beyond MAX_RECORD_BODY_BYTES but write a short body: an
    // unchecked implementation would try to allocate declared bytes.
    let declared = MAX_RECORD_BODY_BYTES + 1;
    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(&declared.to_le_bytes()).unwrap();
    f.write_all(&[0xAB; 16]).unwrap();
    f.write_all(&0u32.to_le_bytes()).unwrap();
    drop(f);
    offset
}

#[test]
fn replay_stream_oversized_length_prefix_is_frame_oversized() {
    let (dir, wal) = fixture_wal();
    log_committed_tx(&wal, 1);
    wal.flush().unwrap();
    let seq = wal.current_sequence();
    drop(wal);
    let offset = append_oversized_frame(dir.path(), seq);

    let mut stream = replay_stream_from(dir.path(), &cursor_at(seq, 0)).expect("stream opens");
    let mut saw = None;
    for item in &mut stream {
        match item {
            Ok(_) => {}
            Err(e) => {
                saw = Some(e);
                break;
            }
        }
    }
    match saw {
        Some(WalCursorError::FrameOversized {
            seq: s,
            offset: o,
            declared,
        }) => {
            assert_eq!(s, seq);
            assert_eq!(o, offset);
            assert_eq!(declared, MAX_RECORD_BODY_BYTES + 1);
        }
        other => panic!("expected FrameOversized, got {other:?}"),
    }
}

#[test]
fn validate_replayable_oversized_length_prefix_is_frame_oversized() {
    let (dir, wal) = fixture_wal();
    log_committed_tx(&wal, 1);
    wal.flush().unwrap();
    let seq = wal.current_sequence();
    drop(wal);
    let offset = append_oversized_frame(dir.path(), seq);

    match validate_replayable(dir.path(), &cursor_at(seq, 0)) {
        Err(WalCursorError::FrameOversized {
            seq: s,
            offset: o,
            declared,
        }) => {
            assert_eq!(s, seq);
            assert_eq!(o, offset);
            assert_eq!(declared, MAX_RECORD_BODY_BYTES + 1);
        }
        other => panic!("expected FrameOversized from validate_replayable, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 6. Agreement: stream verdict == validate_replayable verdict
// ---------------------------------------------------------------------------

fn stream_verdict(dir: &Path, cursor: &WalReplayCursor) -> Result<(), String> {
    match replay_stream_from(dir, cursor) {
        Ok(mut s) => collect_stream(&mut s)
            .map(|_| ())
            .map_err(|e| format!("{e:?}")),
        Err(e) => Err(format!("{e:?}")),
    }
}

fn check_agreement(dir: &Path, cursor: &WalReplayCursor, label: &str) {
    let s = stream_verdict(dir, cursor);
    let v = validate_replayable(dir, cursor).map_err(|e| format!("{e:?}"));
    assert_eq!(s, v, "stream and validate_replayable disagree: {label}");
}

#[test]
fn replay_stream_agrees_with_validate_replayable_across_fixtures() {
    // (a) clean: committed tx + rotation + committed tx.
    {
        let (_dir, wal) = fixture_wal();
        log_committed_tx(&wal, 1);
        let cut = crate::generation::wal_cursor::cut_generation_boundary(&wal).unwrap();
        let seq0 = cut.cursor.log_sequence - 1;
        log_committed_tx(&wal, 2);
        wal.flush().unwrap();
        let dir = wal.dir().to_path_buf();
        check_agreement(&dir, &cursor_at(seq0, 0), "clean rotation");
    }

    // (b) torn tail in the active file: both accept (torn tail is tolerated).
    {
        let (_dir, wal) = fixture_wal();
        log_committed_tx(&wal, 1);
        wal.log(&WalRecord::CreateNode {
            id: NodeId::new(42),
            labels: vec![],
        })
        .unwrap();
        wal.flush().unwrap();
        let seq = wal.current_sequence();
        let dir = wal.dir().to_path_buf();
        drop(wal);
        let len = file_len(&dir, seq);
        let f = OpenOptions::new()
            .write(true)
            .open(wal_path(&dir, seq))
            .unwrap();
        f.set_len(len - 5).unwrap(); // truncate mid-frame
        drop(f);
        check_agreement(&dir, &cursor_at(seq, 0), "torn active tail");
    }

    // (c) corrupted CRC in a rotated file: both reject identically.
    {
        let (_dir, wal) = fixture_wal();
        log_committed_tx(&wal, 1);
        let cut = crate::generation::wal_cursor::cut_generation_boundary(&wal).unwrap();
        let seq0 = cut.cursor.log_sequence - 1;
        log_committed_tx(&wal, 2);
        wal.flush().unwrap();
        let dir = wal.dir().to_path_buf();
        {
            let mut f = OpenOptions::new()
                .write(true)
                .open(wal_path(&dir, seq0))
                .unwrap();
            f.seek(SeekFrom::Start(5)).unwrap();
            f.write_all(&[0x00]).unwrap();
        }
        check_agreement(&dir, &cursor_at(seq0, 0), "corrupted rotated file");
    }
}

// ---------------------------------------------------------------------------
// 7. Large stream: ~100k frames consumed with bounded buffering
// ---------------------------------------------------------------------------

#[test]
fn replay_stream_consumes_100k_frames_with_monotone_positions() {
    let (_dir, wal) = fixture_wal();
    const TXS: u64 = 50_000; // 50k committed txs = 150k frames (> 100k)
    for i in 1..=TXS {
        log_committed_tx(&wal, i);
    }
    wal.flush().unwrap();
    let seq = wal.current_sequence();
    let dir = wal.dir().to_path_buf();

    let mut stream = replay_stream_from(&dir, &cursor_at(seq, 0)).expect("stream opens");
    let mut count = 0u64;
    let mut prev: Option<(u64, u64)> = None;
    for item in &mut stream {
        let frame = item.expect("large stream must stay clean");
        if let Some((ps, po)) = prev {
            assert!(
                frame.log_sequence > ps || (frame.log_sequence == ps && frame.byte_offset > po),
                "positions must be strictly increasing at frame {count}"
            );
        }
        prev = Some((frame.log_sequence, frame.byte_offset));
        count += 1;
    }
    assert_eq!(
        count,
        TXS * 3,
        "every committed frame is yielded exactly once"
    );
    assert!(stream.stopped_at().is_some(), "clean termination recorded");
}

// ---------------------------------------------------------------------------
// 8. Tx-state tracking: commit / abort / torn layout per packet §3
// ---------------------------------------------------------------------------

#[test]
fn replay_stream_tx_state_layout_commit_abort_torn() {
    let (_dir, wal) = fixture_wal();
    // Committed transaction 1.
    log_committed_tx(&wal, 1);
    // Aborted transaction 2: data then abort.
    wal.log(&WalRecord::CreateNode {
        id: NodeId::new(2),
        labels: vec![],
    })
    .unwrap();
    wal.log(&WalRecord::TransactionAbort {
        transaction_id: TransactionId::new(2),
    })
    .unwrap();
    // Torn transaction 3: one data record, then truncate mid next frame.
    wal.log(&WalRecord::CreateNode {
        id: NodeId::new(3),
        labels: vec![],
    })
    .unwrap();
    wal.flush().unwrap();
    let seq = wal.current_sequence();
    let dir = wal.dir().to_path_buf();
    drop(wal);

    // validate_replayable accepts this layout (the abort closes tx2; the
    // torn tx3 tail is tolerated in the active file).
    validate_replayable(&dir, &cursor_at(seq, 0)).expect("commit/abort/torn layout is replayable");

    // The stream yields the same record sequence the parse path sees:
    // tx1 (3 records) + tx2 (2 records) + tx3 (1 record) = 6 frames.
    let mut stream = replay_stream_from(&dir, &cursor_at(seq, 0)).expect("stream opens");
    let frames = collect_stream(&mut stream).expect("clean stream");
    assert_eq!(frames.len(), 6);
    let variants: Vec<&str> = frames
        .iter()
        .map(|f| discriminant_name(&f.record))
        .collect();
    assert_eq!(
        variants,
        vec![
            "CreateNode",
            "TransactionCommit",
            "EpochAdvance",
            "CreateNode",
            "TransactionAbort",
            "CreateNode"
        ]
    );
    // stopped_at points at the torn tail position (EOF after truncation).
    let (sseq, soff) = stream.stopped_at().expect("clean end");
    assert_eq!(sseq, seq);
    assert_eq!(soff, file_len(&dir, seq));
}

// ---------------------------------------------------------------------------
// Rotated-file anomalies (fail-closed per packet §3)
// ---------------------------------------------------------------------------

#[test]
fn replay_stream_rotated_file_ending_tx_open_is_incomplete_transaction() {
    let (_dir, wal) = fixture_wal();
    // File 0 ends with an uncommitted data record, then rotate.
    wal.log(&WalRecord::CreateNode {
        id: NodeId::new(1),
        labels: vec![],
    })
    .unwrap();
    crate::generation::wal_cursor::cut_generation_boundary(&wal).unwrap();
    log_committed_tx(&wal, 2);
    wal.flush().unwrap();

    let dir = wal.dir().to_path_buf();
    let seq0 = wal.current_sequence() - 1;
    let mut stream = replay_stream_from(&dir, &cursor_at(seq0, 0)).expect("stream opens");
    // The rotated file's uncommitted data frame is yielded first; the error
    // arrives at the rotated file's EOF with a transaction still open.
    match stream.next() {
        Some(Ok(frame)) => {
            assert_eq!(frame.log_sequence, seq0);
            assert_eq!(frame.byte_offset, 0);
        }
        other => panic!("expected the rotated file's data frame first, got {other:?}"),
    }
    match stream.next() {
        Some(Err(WalCursorError::IncompleteTransaction(seq))) => assert_eq!(seq, seq0),
        other => panic!("expected IncompleteTransaction, got {other:?}"),
    }
    // Fused after error; no clean termination.
    assert!(stream.next().is_none());
    assert_eq!(stream.stopped_at(), None);
}

#[test]
fn validate_replayable_rotated_file_ending_tx_open_is_incomplete_transaction() {
    let (_dir, wal) = fixture_wal();
    wal.log(&WalRecord::CreateNode {
        id: NodeId::new(1),
        labels: vec![],
    })
    .unwrap();
    crate::generation::wal_cursor::cut_generation_boundary(&wal).unwrap();
    log_committed_tx(&wal, 2);
    wal.flush().unwrap();

    let dir = wal.dir().to_path_buf();
    let seq0 = wal.current_sequence() - 1;
    match validate_replayable(&dir, &cursor_at(seq0, 0)) {
        Err(WalCursorError::IncompleteTransaction(seq)) => assert_eq!(seq, seq0),
        other => panic!("expected IncompleteTransaction, got {other:?}"),
    }
}
