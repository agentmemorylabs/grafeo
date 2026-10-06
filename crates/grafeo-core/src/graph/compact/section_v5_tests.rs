//! v6 reader geometry and corruption tests for `section_v5` (G4).

use super::*;

fn wide_body(disc: u8, dims: u16, count: u64, data: &[u8]) -> Bytes {
    let mut body = vec![disc];
    body.extend_from_slice(&dims.to_le_bytes());
    body.extend_from_slice(&count.to_le_bytes());
    body.extend_from_slice(data);
    Bytes::from(body)
}

fn f32s(values: &[f32]) -> Vec<u8> {
    values.iter().flat_map(|f| f.to_le_bytes()).collect()
}

fn read(body: &Bytes, version: PayloadVersion, rows: u32) -> Result<ColumnCodec, String> {
    let dict = MappedStringDictionary::empty();
    read_column_body_with_index(body, 5, &dict, None, version, rows)
}

#[test]
fn wide_f32_body_reads_in_v6_and_is_refused_in_v5() {
    let data = f32s(&[1.0, 2.0, 3.0, 4.0]);
    let body = wide_body(DISC_F32_VECTOR_WIDE, 2, 4, &data);
    match read(&body, PayloadVersion::V6, 2).expect("v6 reads the wide header") {
        ColumnCodec::Float32Vector { bytes, dimensions } => {
            assert_eq!(dimensions, 2);
            assert_eq!(bytes.as_ref(), data.as_slice());
        }
        other => panic!("expected Float32Vector, got {other:?}"),
    }
    let err = read(&body, PayloadVersion::V5, 2).expect_err("v5 must refuse a wide body");
    assert!(err.contains("only valid in a v6 payload"), "{err}");
}

#[test]
fn wide_i8_body_reads_and_rejects_malformed() {
    let body = wide_body(DISC_I8_VECTOR_WIDE, 3, 6, &[1, 2, 3, 4, 5, 6]);
    match read(&body, PayloadVersion::V6, 2).unwrap() {
        ColumnCodec::Int8Vector { bytes, dimensions } => {
            assert_eq!((dimensions, bytes.len()), (3, 6));
        }
        other => panic!("expected Int8Vector, got {other:?}"),
    }
    let short = wide_body(DISC_I8_VECTOR_WIDE, 3, 6, &[1, 2, 3]);
    assert!(
        read(&short, PayloadVersion::V6, 2)
            .unwrap_err()
            .contains("truncated")
    );
    assert!(read(&body, PayloadVersion::V5, 2).is_err());
}

#[test]
fn wide_body_truncation_and_trailing_bytes_are_refused() {
    let truncated = wide_body(DISC_F32_VECTOR_WIDE, 2, 4, &[0u8; 12]);
    let err = read(&truncated, PayloadVersion::V6, 2).unwrap_err();
    assert!(err.contains("truncated wide vector body"), "{err}");
    let trailing = wide_body(DISC_F32_VECTOR_WIDE, 2, 4, &[0u8; 20]);
    let err = read(&trailing, PayloadVersion::V6, 2).unwrap_err();
    assert!(err.contains("4 trailing bytes"), "{err}");
    let header_only = Bytes::from(vec![DISC_F32_VECTOR_WIDE, 2, 0, 4]);
    assert!(read(&header_only, PayloadVersion::V6, 2).is_err());
}

#[test]
fn wide_body_geometry_must_match_rows_and_dims() {
    let data = f32s(&[0.0; 6]);
    // 6 components is not 2 rows x 2 dims.
    let mismatched = wide_body(DISC_F32_VECTOR_WIDE, 2, 6, &data);
    let err = read(&mismatched, PayloadVersion::V6, 2).unwrap_err();
    assert!(err.contains("count 6 != 2 rows x 2 dims"), "{err}");
    // Not divisible by dims: no row count can match.
    let ragged = wide_body(DISC_F32_VECTOR_WIDE, 4, 6, &data);
    assert!(read(&ragged, PayloadVersion::V6, 1).is_err());
    let zero_dims = wide_body(DISC_F32_VECTOR_WIDE, 0, 0, &[]);
    let err = read(&zero_dims, PayloadVersion::V6, 0).unwrap_err();
    assert!(err.contains("zero dimensions"), "{err}");
}

#[test]
fn wide_body_u64_count_overflow_is_refused() {
    // rows x dims cannot reach u64::MAX / 2, so a count that large fails the
    // geometry check before any length arithmetic.
    let body = wide_body(DISC_F32_VECTOR_WIDE, 1, u64::MAX / 2, &[]);
    assert!(read(&body, PayloadVersion::V6, u32::MAX).is_err());
}

#[test]
fn block_index_ranges_are_checked() {
    let bodies = Bytes::from(vec![0u8; 100]);
    let rec = |offset: u64, len: u64| {
        let mut buf = Vec::new();
        write_block_index_record(
            &mut buf,
            PayloadVersion::V6,
            BlockIndexRecord {
                body_offset: offset,
                body_len: len,
                row_count: 1,
            },
        )
        .unwrap();
        Bytes::from(buf)
    };
    let (ok, rows) = column_body_slice(&rec(90, 10), &bodies, 0, PayloadVersion::V6).unwrap();
    assert_eq!((ok.len(), rows), (10, 1));
    for (offset, len) in [(90, 11), (101, 0), (u64::MAX, 1), (1, u64::MAX)] {
        assert!(
            column_body_slice(&rec(offset, len), &bodies, 0, PayloadVersion::V6).is_err(),
            "offset {offset} len {len} must be refused"
        );
    }
    // A record index past the block index is refused, not read past.
    assert!(column_body_slice(&rec(0, 1), &bodies, 1, PayloadVersion::V6).is_err());
}

#[test]
fn heap_writer_vector_header_matches_the_v1_codec_layout() {
    let codec = ColumnCodec::Float32Vector {
        bytes: Bytes::from(vec![0u8; 24]),
        dimensions: 3,
    };
    let mut ours = Vec::new();
    write_column_body(&mut ours, &codec, &FxHashMap::default()).unwrap();
    let mut v1 = Vec::new();
    codec.write_to(&mut v1);
    assert_eq!(ours, v1);
}
