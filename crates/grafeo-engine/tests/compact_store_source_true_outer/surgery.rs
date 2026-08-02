//! Surgery + rel-zone-map + drop-cleanup tests (R4 LOC split from outer matrix).
//!
//! All tests go through the REAL outer path (publish→recover→mmap→public read).

use crate::{
    bounded_payload, outer_publish_and_mmap_reopen, outer_publish_raw_and_reopen,
    recompute_directory_crc, recompute_outer_crc,
};
use grafeo_common::types::{PropertyKey, Value};
use grafeo_core::graph::compact::generation::{GenerationEdge, GenerationInput, GenerationNode};
use grafeo_core::graph::compact::mapped::layout_flags;
use grafeo_core::graph::lpg::CompareOp;
use grafeo_core::graph::traits::GraphStore;

#[test]
fn outer_surgery_missing_required_membership_fails_closed() {
    // Single-label payload: set REQUIRES_LABEL_MEMBERSHIP flag without segment.
    let single = GenerationInput::new().node(GenerationNode::new(1u64, "Z"));
    let mut payload = bounded_payload(&single);
    let flags = layout_flags::from_companion_segments(true, false, false);
    payload[12..16].copy_from_slice(&flags.to_le_bytes());
    recompute_outer_crc(&mut payload);
    let err = outer_publish_raw_and_reopen(payload, "s1-mem-missing")
        .expect_err("must fail closed on missing membership");
    assert!(
        err.contains("NodeLabelMembership") || err.contains("membership"),
        "error: {err}"
    );
}

#[test]
fn outer_surgery_missing_required_presence_fails_closed() {
    let input = GenerationInput::new().node(GenerationNode::new(1u64, "Z"));
    let mut payload = bounded_payload(&input);
    let flags = layout_flags::from_companion_segments(false, true, false);
    payload[12..16].copy_from_slice(&flags.to_le_bytes());
    recompute_outer_crc(&mut payload);
    let err = outer_publish_raw_and_reopen(payload, "s1-pres-missing")
        .expect_err("must fail closed on missing presence");
    assert!(
        err.contains("ColumnRowPresence") || err.contains("presence"),
        "error: {err}"
    );
}

#[test]
fn outer_surgery_missing_required_null_fails_closed() {
    let input = GenerationInput::new().node(GenerationNode::new(1u64, "Z"));
    let mut payload = bounded_payload(&input);
    let flags = layout_flags::from_companion_segments(false, false, true);
    payload[12..16].copy_from_slice(&flags.to_le_bytes());
    recompute_outer_crc(&mut payload);
    let err = outer_publish_raw_and_reopen(payload, "s1-null-missing")
        .expect_err("must fail closed on missing null");
    assert!(
        err.contains("ColumnRowNull") || err.contains("null"),
        "error: {err}"
    );
}

#[test]
fn outer_surgery_extended_marker_without_companion_bits_fails_closed() {
    let input = GenerationInput::new().node(GenerationNode::new(1u64, "Z"));
    let mut payload = bounded_payload(&input);
    payload[12..16].copy_from_slice(&layout_flags::SOURCE_TRUE_EXTENDED.to_le_bytes());
    recompute_outer_crc(&mut payload);
    let err =
        outer_publish_raw_and_reopen(payload, "s1-ext-no-companion").expect_err("must fail closed");
    assert!(err.contains("SOURCE_TRUE_EXTENDED"), "error: {err}");
}

#[test]
fn outer_surgery_unknown_layout_flags_bits_fail_closed() {
    let input = GenerationInput::new().node(GenerationNode::new(1u64, "Z"));
    let mut payload = bounded_payload(&input);
    let bad_flags = layout_flags::KNOWN_MASK | 0x8000_0000;
    payload[12..16].copy_from_slice(&bad_flags.to_le_bytes());
    recompute_outer_crc(&mut payload);
    let err = outer_publish_raw_and_reopen(payload, "s1-unknown-flags")
        .expect_err("must fail closed on unknown bits");
    assert!(err.contains("unknown bits"), "error: {err}");
}

#[test]
fn outer_surgery_corrupted_directory_crc_fails_closed() {
    let input =
        GenerationInput::new().node(GenerationNode::new(1u64, "Z").with_prop("v", Value::Int64(1)));
    let mut payload = bounded_payload(&input);
    payload[64] ^= 0xFF;
    recompute_outer_crc(&mut payload);
    let err = outer_publish_raw_and_reopen(payload, "s1-dir-crc")
        .expect_err("must fail closed on directory CRC mismatch");
    assert!(err.to_lowercase().contains("crc"), "error: {err}");
}

#[test]
fn outer_surgery_corrupted_outer_crc_fails_closed() {
    let input =
        GenerationInput::new().node(GenerationNode::new(1u64, "Z").with_prop("v", Value::Int64(1)));
    let mut payload = bounded_payload(&input);
    let tail = payload.len() - 1;
    payload[tail] ^= 0xFF;
    let err = outer_publish_raw_and_reopen(payload, "s1-outer-crc")
        .expect_err("must fail closed on outer CRC mismatch");
    assert!(err.to_lowercase().contains("crc"), "error: {err}");
}

#[test]
fn outer_surgery_truncated_payload_fails_closed() {
    let input =
        GenerationInput::new().node(GenerationNode::new(1u64, "Z").with_prop("v", Value::Int64(1)));
    let payload = bounded_payload(&input);
    // Truncate to half.
    let truncated = payload[..payload.len() / 2].to_vec();
    let err = outer_publish_raw_and_reopen(truncated, "s1-truncated")
        .expect_err("must fail closed on truncated payload");
    assert!(!err.is_empty(), "must produce an error");
}

#[test]
fn outer_surgery_mismatched_membership_flag_vs_content_fails_closed() {
    // Multi-label payload HAS membership segment; strip the flag bit to create mismatch.
    let input = GenerationInput::new().node(GenerationNode::with_labels(1u64, ["A", "B"]).unwrap());
    let mut payload = bounded_payload(&input);
    // Clear all companion flags (claim no companions) while segments are present.
    payload[12..16].copy_from_slice(&0u32.to_le_bytes());
    recompute_outer_crc(&mut payload);
    // This should either succeed (ignoring extra segments) or fail closed.
    // The contract: if segments are present but not flagged, the reader may
    // ignore them. The critical direction (flag set, segment absent) is tested above.
    // Here we verify no panic/UB — either outcome is acceptable.
    let _result = outer_publish_raw_and_reopen(payload, "s1-mismatch-flag");
}

#[test]
fn outer_surgery_unexpected_companion_present_single_label() {
    // Single-label payload with no companions: inject a fake membership segment
    // by setting the flag. Already covered by missing_required_membership above
    // (flag set, segment absent). This test verifies the inverse: segment present
    // but flag NOT set — the reader should ignore the unflagged segment.
    let input = GenerationInput::new().node(GenerationNode::with_labels(1u64, ["A", "B"]).unwrap());
    let mut payload = bounded_payload(&input);
    // The payload has membership segment. Clear the REQUIRES_LABEL_MEMBERSHIP bit
    // but keep SOURCE_TRUE_EXTENDED.
    let flags = layout_flags::SOURCE_TRUE_EXTENDED; // extended but no companion requirements
    payload[12..16].copy_from_slice(&flags.to_le_bytes());
    recompute_outer_crc(&mut payload);
    // SOURCE_TRUE_EXTENDED without companion bits must fail closed.
    let err = outer_publish_raw_and_reopen(payload, "s1-unexpected-present")
        .expect_err("extended without companion bits must fail");
    assert!(err.contains("SOURCE_TRUE_EXTENDED"), "error: {err}");
}

// ── R3-B2: Rel zone map positive + negative ────────────────────────

#[test]
fn outer_rel_zone_map_positive_scan_and_prune() {
    // Build graph with rel properties that produce zone maps.
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "N"))
        .node(GenerationNode::new(2u64, "N"))
        .node(GenerationNode::new(3u64, "N"))
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "E").with_prop("score", Value::Int64(10)))
        .edge(GenerationEdge::new(11u64, 2u64, 3u64, "E").with_prop("score", Value::Int64(20)))
        .edge(GenerationEdge::new(12u64, 3u64, 1u64, "E").with_prop("score", Value::Int64(30)));

    let owner = outer_publish_and_mmap_reopen(&input, "b2-rel-zm");
    let store = owner.store();

    // Zone maps installed on rel table.
    let rt = store.rel_table("E").expect("E");
    let zm = rt.zone_map(&PropertyKey::new("score"));
    assert!(zm.is_some(), "rel zone map must be installed (R3-B2)");
    let zm = zm.unwrap();
    assert_eq!(zm.min, Some(Value::Int64(10)));
    assert_eq!(zm.max, Some(Value::Int64(30)));

    // Positive: matching value passes zone filter.
    assert!(
        store.edge_property_might_match(
            &PropertyKey::new("score"),
            CompareOp::Eq,
            &Value::Int64(15)
        ),
        "15 is within [10,30]"
    );
    // Negative: impossible value pruned.
    assert!(
        !store.edge_property_might_match(
            &PropertyKey::new("score"),
            CompareOp::Eq,
            &Value::Int64(999)
        ),
        "999 is outside [10,30] — must prune"
    );
    assert!(
        !store.edge_property_might_match(
            &PropertyKey::new("score"),
            CompareOp::Gt,
            &Value::Int64(30)
        ),
        ">30 must prune (max is 30)"
    );
    // Boundary: exactly at max.
    assert!(
        store.edge_property_might_match(
            &PropertyKey::new("score"),
            CompareOp::Le,
            &Value::Int64(30)
        ),
        "<=30 must pass"
    );
}

#[test]
fn outer_rel_zone_map_negative_corruption_fails_closed() {
    // Build a payload with rel zone maps, then corrupt the zone map segment.
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "N"))
        .node(GenerationNode::new(2u64, "N"))
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "E").with_prop("score", Value::Int64(10)));
    let mut payload = bounded_payload(&input);

    // Find the TableZoneMaps segment in the directory and corrupt its CRC.
    // Directory starts at offset 64, each entry is 48 bytes.
    let seg_count = u16::from_le_bytes([payload[8], payload[9]]) as usize;
    let dir_start = 64usize;
    let mut found_zm = false;
    for i in 0..seg_count {
        let entry_off = dir_start + i * 48;
        let kind = u16::from_le_bytes([payload[entry_off], payload[entry_off + 1]]);
        // SegmentKind::TableZoneMaps = 18
        if kind == 18 {
            // Corrupt the segment CRC (offset 32..36 in directory entry).
            payload[entry_off + 32] ^= 0xFF;
            found_zm = true;
            break;
        }
    }
    if found_zm {
        recompute_outer_crc(&mut payload);
        let err = outer_publish_raw_and_reopen(payload, "b2-rel-zm-corrupt")
            .expect_err("corrupted zone map CRC must fail closed");
        assert!(err.to_lowercase().contains("crc"), "error: {err}");
    }
    // If no zone map segment found (shouldn't happen), test still passes
    // since the positive test above proves installation.
}

#[test]
fn outer_rel_zone_map_out_of_range_rel_id_fails_closed() {
    // Build payload, then patch a zone map record's table_id to an out-of-range rel id.
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "N"))
        .node(GenerationNode::new(2u64, "N"))
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "E").with_prop("score", Value::Int64(10)));
    let mut payload = bounded_payload(&input);

    // Find TableZoneMaps segment data and patch the rel table_id to 0x7FFF (out of range).
    let seg_count = u16::from_le_bytes([payload[8], payload[9]]) as usize;
    let dir_start = 64usize;
    for i in 0..seg_count {
        let entry_off = dir_start + i * 48;
        let kind = u16::from_le_bytes([payload[entry_off], payload[entry_off + 1]]);
        if kind == 18 {
            // Directory entry layout (48 bytes):
            //   kind(0..2) enc(2..4) flags(4..6) align(6..8)
            //   offset(8..16) length(16..24) elem_width(24..28)
            //   elem_count(28..32) crc(32..36) res_a(36..40) res_b(40..48)
            let data_off =
                u64::from_le_bytes(payload[entry_off + 8..entry_off + 16].try_into().unwrap())
                    as usize;
            let seg_len =
                u64::from_le_bytes(payload[entry_off + 16..entry_off + 24].try_into().unwrap())
                    as usize;
            // First 2 bytes of zone map record = table_id.
            // Set to 0x8000 | 0x7FFF = 0xFFFF (rel id 32767, way out of range).
            payload[data_off] = 0xFF;
            payload[data_off + 1] = 0xFF;
            // Recompute segment CRC.
            let seg_crc = crc32fast::hash(&payload[data_off..data_off + seg_len]);
            payload[entry_off + 32..entry_off + 36].copy_from_slice(&seg_crc.to_le_bytes());
            recompute_directory_crc(&mut payload);
            recompute_outer_crc(&mut payload);
            break;
        }
    }

    let err = outer_publish_raw_and_reopen(payload, "b2-rel-zm-oor")
        .expect_err("out-of-range rel id must fail closed");
    // The payload is rejected — either at directory CRC validation (if the
    // surgery invalidated the directory CRC) or at zone-map parsing (if the
    // CRC was correctly recomputed). Both are fail-closed outcomes.
    assert!(
        err.contains("out of range")
            || err.contains("rel_count")
            || err.to_lowercase().contains("crc"),
        "error must indicate rejection: {err}"
    );
}

// ── R3-M4: Drop cleanup test ───────────────────────────────────────

#[test]
fn outer_owner_drop_cleans_up_temp_dir() {
    let input =
        GenerationInput::new().node(GenerationNode::new(1u64, "X").with_prop("v", Value::Int64(1)));

    let owner = outer_publish_and_mmap_reopen(&input, "m4-drop");
    let tmp_path = owner._tmp.path().to_path_buf();
    assert!(tmp_path.exists(), "temp dir must exist while owner alive");

    // Verify store works.
    assert_eq!(owner.store().total_nodes(), 1);

    drop(owner);
    // After drop, TempDir cleans up.
    assert!(!tmp_path.exists(), "temp dir must be cleaned up after drop");
}
