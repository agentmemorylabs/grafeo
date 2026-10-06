//! Unit tests for `payload_version` (split out to keep the module short).

use super::*;

#[test]
fn block_index_record_v5_is_byte_identical_to_legacy_layout() {
    let mut buf = Vec::new();
    let rec = BlockIndexRecord {
        body_offset: 0x0102_0304,
        body_len: 0x0506_0708,
        row_count: 9,
    };
    write_block_index_record(&mut buf, PayloadVersion::V5, rec).unwrap();
    let mut legacy = Vec::new();
    legacy.extend_from_slice(&0x0102_0304u32.to_le_bytes());
    legacy.extend_from_slice(&0x0506_0708u32.to_le_bytes());
    legacy.extend_from_slice(&9u32.to_le_bytes());
    assert_eq!(buf, legacy);
    assert_eq!(
        read_block_index_record(&buf, PayloadVersion::V5, 0),
        Ok(rec)
    );
}

#[test]
fn block_index_record_v6_round_trips_past_u32() {
    let rec = BlockIndexRecord {
        body_offset: 5_000_000_000,
        body_len: 4_915_200_007,
        row_count: 600_000,
    };
    assert!(rec.needs_v6());
    let mut buf = Vec::new();
    assert_eq!(
        write_block_index_record(&mut buf, PayloadVersion::V5, rec),
        Err(WireOverflow {
            what: "col_body_offset",
            count: 5_000_000_000,
            max: u64::from(u32::MAX),
        })
    );
    buf.clear();
    write_block_index_record(&mut buf, PayloadVersion::V6, rec).unwrap();
    assert_eq!(buf.len(), BLOCK_INDEX_RECORD_LEN_V6);
    assert_eq!(
        read_block_index_record(&buf, PayloadVersion::V6, 0),
        Ok(rec)
    );
    buf[20] = 1;
    assert!(read_block_index_record(&buf, PayloadVersion::V6, 0).is_err());
}

/// v5 limits each field on its own: offset ≤ u32::MAX and length ≤
/// u32::MAX. Their sum may pass u32::MAX in a valid v5 payload.
#[test]
fn block_index_record_needs_v6_per_field_not_on_the_end() {
    let max = u64::from(u32::MAX);
    let rec = |body_offset, body_len| BlockIndexRecord {
        body_offset,
        body_len,
        row_count: 1,
    };
    // Both fields at the maximum: fits v5 even though the end is ~8 GiB.
    assert!(!rec(max, max).needs_v6());
    // The end passes u32 by one byte: still v5.
    assert!(!rec(max - 10, 11).needs_v6());
    // Each field one past its maximum: v6.
    assert!(rec(max + 1, 0).needs_v6());
    assert!(rec(0, max + 1).needs_v6());
    // A v5 writer accepts exactly what needs_v6 calls v5.
    let mut buf = Vec::new();
    write_block_index_record(&mut buf, PayloadVersion::V5, rec(max, max)).unwrap();
    assert!(write_block_index_record(&mut buf, PayloadVersion::V5, rec(max + 1, 0)).is_err());
    assert!(write_block_index_record(&mut buf, PayloadVersion::V5, rec(0, max + 1)).is_err());
}

#[test]
fn directory_entry_round_trips_in_both_layouts() {
    let e = DirectoryEntryFields {
        kind: 2,
        encoding_version: 1,
        flags: 1,
        alignment: 1,
        offset: 4096,
        length: 6_000_000_000,
        element_width: 1,
        element_count: 6_000_000_000,
        crc32: 0xDEAD_BEEF,
    };
    let mut buf = Vec::new();
    assert!(write_directory_entry(&mut buf, PayloadVersion::V5, &e).is_err());
    buf.clear();
    write_directory_entry(&mut buf, PayloadVersion::V6, &e).unwrap();
    assert_eq!(buf.len(), super::super::DIRECTORY_ENTRY_LEN);
    let mut pos = 0;
    assert_eq!(
        read_directory_entry(&buf, &mut pos, PayloadVersion::V6),
        Ok(e)
    );

    let small = DirectoryEntryFields {
        element_count: 7,
        length: 7,
        ..e
    };
    let mut v5 = Vec::new();
    write_directory_entry(&mut v5, PayloadVersion::V5, &small).unwrap();
    assert_eq!(v5.len(), super::super::DIRECTORY_ENTRY_LEN);
    let mut pos = 0;
    assert_eq!(
        read_directory_entry(&v5, &mut pos, PayloadVersion::V5),
        Ok(small)
    );
}

#[test]
fn vector_header_wide_and_narrow_forms() {
    assert_eq!(
        vector_body_header(false, 2048, 12, false).unwrap(),
        [5, 0x00, 0x08, 12, 0, 0, 0]
    );
    let wide = vector_body_header(false, 2048, 4_404_019_200, true).unwrap();
    assert_eq!(wide[0], DISC_F32_VECTOR_WIDE);
    assert_eq!(&wide[3..], &4_404_019_200u64.to_le_bytes());
    assert!(vector_body_header(false, 2048, 4_404_019_200, false).is_err());
    assert_eq!(
        vector_body_header(true, 8, 64, true).unwrap()[0],
        DISC_I8_VECTOR_WIDE
    );
}

#[test]
fn policy_resolution() {
    assert_eq!(
        PayloadVersionPolicy::Auto.resolve(false),
        PayloadVersion::V5
    );
    assert_eq!(PayloadVersionPolicy::Auto.resolve(true), PayloadVersion::V6);
    assert_eq!(PayloadVersionPolicy::V6.resolve(false), PayloadVersion::V6);
    assert!(!PayloadVersionPolicy::Auto.wide_vector_header(u64::from(u32::MAX)));
    assert!(PayloadVersionPolicy::Auto.wide_vector_header(u64::from(u32::MAX) + 1));
    assert!(PayloadVersionPolicy::V6.wide_vector_header(1));
}

#[test]
fn unknown_versions_are_refused() {
    assert_eq!(PayloadVersion::from_byte(5), Ok(PayloadVersion::V5));
    assert_eq!(PayloadVersion::from_byte(6), Ok(PayloadVersion::V6));
    let err = PayloadVersion::from_byte(7).unwrap_err();
    assert!(
        err.contains("unsupported CompactStore section version 7"),
        "{err}"
    );
}
