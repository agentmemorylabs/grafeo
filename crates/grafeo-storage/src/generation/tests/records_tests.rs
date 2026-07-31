//! Framed record I/O tests (W0-A1).
//!
//! Covers: frame round-trip, short read (torn header), oversized record,
//! checked length cap, clean EOF.

#![allow(clippy::cast_possible_truncation)]

use crate::generation::records::{FramedRecord, MAX_RECORD_BODY_BYTES};
use std::io::{BufReader, Cursor, Write};

#[test]
fn frame_round_trip() {
    let rec = FramedRecord::new(b"key123".to_vec(), b"payload data".to_vec());
    let mut buf = Vec::new();
    rec.write_to(&mut buf).unwrap();
    assert_eq!(buf.len() as u64, rec.encoded_len());

    let mut cursor = Cursor::new(&buf);
    let read = FramedRecord::read_next(&mut cursor).unwrap().unwrap();
    assert_eq!(read, rec);
}

#[test]
fn frame_round_trip_empty_key_and_payload() {
    let rec = FramedRecord::new(vec![], vec![]);
    let mut buf = Vec::new();
    rec.write_to(&mut buf).unwrap();
    let mut cursor = Cursor::new(&buf);
    let read = FramedRecord::read_next(&mut cursor).unwrap().unwrap();
    assert_eq!(read, rec);
}

#[test]
fn clean_eof_returns_none() {
    let data: Vec<u8> = vec![];
    let mut cursor = Cursor::new(&data);
    let result = FramedRecord::read_next(&mut cursor).unwrap();
    assert!(result.is_none());
}

#[test]
fn torn_header_fails_closed() {
    // Only 3 bytes — partial header (1..7 bytes read, then EOF)
    let data = vec![1u8, 2, 3];
    let mut cursor = Cursor::new(&data);
    let err = FramedRecord::read_next(&mut cursor).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
}

#[test]
fn short_body_after_full_header_fails() {
    // Full header declares key_len=4, payload_len=4 but body is truncated.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short_body.bin");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&4u32.to_le_bytes()).unwrap(); // key_len
        f.write_all(&4u32.to_le_bytes()).unwrap(); // payload_len
        f.write_all(b"ab").unwrap(); // only 2 of 8 body bytes
    }
    let mut reader = BufReader::new(std::fs::File::open(&path).unwrap());
    let err = FramedRecord::read_next(&mut reader).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    drop(dir);
}

#[test]
fn oversized_length_prefix_rejected_before_alloc() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("oversized.bin");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        // Declare key_len = MAX + 1
        let oversize = MAX_RECORD_BODY_BYTES + 1;
        f.write_all(&oversize.to_le_bytes()).unwrap();
        f.write_all(&0u32.to_le_bytes()).unwrap(); // payload_len=0
    }
    let mut reader = BufReader::new(std::fs::File::open(&path).unwrap());
    let err = FramedRecord::read_next(&mut reader).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn multiple_records_sequential() {
    let recs: Vec<_> = (0..10u32)
        .map(|i| FramedRecord::new(i.to_le_bytes().to_vec(), vec![i as u8; i as usize]))
        .collect();
    let mut buf = Vec::new();
    for r in &recs {
        r.write_to(&mut buf).unwrap();
    }
    let mut cursor = Cursor::new(&buf);
    for expected in &recs {
        let read = FramedRecord::read_next(&mut cursor).unwrap().unwrap();
        assert_eq!(&read, expected);
    }
    // Next read should be clean EOF
    assert!(FramedRecord::read_next(&mut cursor).unwrap().is_none());
}

#[test]
fn ordering_is_key_then_payload() {
    use std::cmp::Ordering;
    let a = FramedRecord::new(b"a", b"x");
    let b_rec = FramedRecord::new(b"a", b"y");
    let c = FramedRecord::new(b"b", b"z");
    assert_eq!(a.cmp(&b_rec), Ordering::Less);
    assert_eq!(b_rec.cmp(&c), Ordering::Less);
}
