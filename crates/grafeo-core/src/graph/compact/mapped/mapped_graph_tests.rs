//! Unit tests for mapped v5 graph views (G-EM0.2).

use super::*;
use bytes::Bytes;

#[test]
fn u32_view_rejects_misaligned_length() {
    let err = U32View::new(Bytes::from(vec![0u8; 3])).expect_err("misaligned");
    assert!(err.contains("multiple of 4"), "{err}");
}

#[test]
fn u32_view_get_and_range() {
    let mut raw = Vec::new();
    for v in [1u32, 2, 3, 4] {
        raw.extend_from_slice(&v.to_le_bytes());
    }
    let view = U32View::new(Bytes::from(raw)).unwrap();
    assert_eq!(view.len(), 4);
    assert_eq!(view.get(0), Some(1));
    assert_eq!(view.get(3), Some(4));
    assert_eq!(view.get(4), None);
    let slice = view.element_bytes(1, 3).unwrap();
    assert_eq!(slice.len(), 8);
}

#[test]
fn string_dict_round_trip_and_utf8_fail_closed() {
    let (off, bytes) = build_string_segments(&["alpha", "beta", "gamma"]);
    let dict = MappedStringDictionary::new(Bytes::from(off), Bytes::from(bytes)).unwrap();
    assert_eq!(dict.len(), 3);
    assert_eq!(dict.get(0), Some("alpha"));
    assert_eq!(dict.get(2), Some("gamma"));
    assert_eq!(dict.encode("beta"), Some(1));
    assert_eq!(dict.encode("missing"), None);

    // Invalid UTF-8 in string bytes.
    let mut off = Vec::new();
    off.extend_from_slice(&0u64.to_le_bytes());
    off.extend_from_slice(&1u64.to_le_bytes());
    let err = MappedStringDictionary::new(Bytes::from(off), Bytes::from(vec![0xFFu8]))
        .expect_err("bad utf8");
    assert!(err.contains("UTF-8"), "{err}");
}

#[test]
fn node_id_lookup_binary_search_and_sort_check() {
    let mut buf = Vec::new();
    write_node_id_record(&mut buf, 10, 0, 0);
    write_node_id_record(&mut buf, 20, 0, 1);
    write_node_id_record(&mut buf, 30, 1, 0);
    let lookup = MappedNodeIdLookup::new(Bytes::from(buf)).unwrap();
    assert_eq!(
        lookup.lookup(grafeo_common::types::NodeId::new(20)),
        Some((0, 1))
    );
    assert_eq!(
        lookup.lookup(grafeo_common::types::NodeId::new(99)),
        None
    );

    // Unsorted fails closed.
    let mut bad = Vec::new();
    write_node_id_record(&mut bad, 30, 0, 0);
    write_node_id_record(&mut bad, 10, 0, 1);
    let err = MappedNodeIdLookup::new(Bytes::from(bad)).expect_err("unsorted");
    assert!(err.contains("sorted"), "{err}");
}

#[test]
fn edge_id_lookup_rejects_nonzero_reserved() {
    let mut buf = Vec::new();
    write_edge_id_record(&mut buf, 1, 0, 0);
    // Corrupt reserved_a.
    buf[10] = 1;
    let err = MappedEdgeIdLookup::new(Bytes::from(buf)).expect_err("reserved");
    assert!(err.contains("reserved"), "{err}");
}

#[test]
fn directory_header_rejects_bad_version_and_reserved() {
    let mut hdr = vec![0u8; HEADER_LEN];
    hdr[0..4].copy_from_slice(b"GCST");
    hdr[4] = 4; // not v5
    let err = parse_v5_header(&hdr).expect_err("v4");
    assert!(err.contains("unsupported"), "{err}");

    hdr[4] = FORMAT_VERSION_V5;
    hdr[5] = 0; // flags
    // header_length at offset 6
    hdr[6..8].copy_from_slice(&(HEADER_LEN as u16).to_le_bytes());
    // segment_count = 0
    hdr[8..10].copy_from_slice(&0u16.to_le_bytes());
    // directory_entry_length
    hdr[10..12].copy_from_slice(&(DIRECTORY_ENTRY_LEN as u16).to_le_bytes());
    // layout_flags zero already
    // directory_offset
    hdr[16..24].copy_from_slice(&(HEADER_LEN as u64).to_le_bytes());
    // directory_length 0
    // data_offset 64
    hdr[32..40].copy_from_slice(&(HEADER_LEN as u64).to_le_bytes());
    // reserved non-zero at 60
    hdr[60..64].copy_from_slice(&1u32.to_le_bytes());
    let err = parse_v5_header(&hdr).expect_err("reserved");
    assert!(err.contains("reserved"), "{err}");
}

#[test]
fn accounting_disk_native_requires_zero_proportional() {
    let mut acc = CompactMemoryAccounting::with_defaults();
    acc.mapped_payload_index_bytes = 1_000_000;
    acc.anonymous_owner_schema_bytes = 4_096;
    assert!(acc.is_disk_native_graph());
    acc.anonymous_proportional_structure_bytes = 1;
    assert!(!acc.is_disk_native_graph());
    acc.anonymous_proportional_structure_bytes = 0;
    acc.anonymous_owner_schema_bytes = SCHEMA_OWNER_BUDGET_BYTES + 1;
    assert!(acc.schema_budget_exceeded());
    assert!(!acc.is_disk_native_graph());
}

#[test]
fn segment_kind_unknown_fails_closed() {
    let err = SegmentKind::from_u16(99).expect_err("unknown");
    assert!(err.contains("unknown"), "{err}");
}

#[test]
fn v5_round_trip_preserves_properties_and_csr_without_heap_id_maps() {
    use crate::graph::compact::builder::from_graph_store_preserving_ids;
    use crate::graph::compact::section::CompactStoreSection;
    use crate::graph::lpg::LpgStore;
    use grafeo_common::storage::section::Section;
    use grafeo_common::types::{PropertyKey, Value};

    let store = LpgStore::new().unwrap();
    let a = store.create_node_with_props(
        &["Person"],
        [
            ("name", Value::from("Alix")),
            ("rank", Value::Int64(1)),
        ],
    );
    let b = store.create_node_with_props(
        &["Person"],
        [
            ("name", Value::from("Gus")),
            ("rank", Value::Int64(2)),
        ],
    );
    let _e = store.create_edge(a, b, "KNOWS");
    let compact = from_graph_store_preserving_ids(&store).unwrap();
    let section = CompactStoreSection::new(std::sync::Arc::new(compact));
    let bytes = section.serialize().expect("v5 serialize");
    assert_eq!(bytes[4], FORMAT_VERSION_V5);

    let mut section2 = CompactStoreSection::empty();
    section2
        .deserialize_from_mapped_bytes(Bytes::from(bytes))
        .expect("v5 mapped deserialize");
    let restored = section2.store().unwrap();
    assert!(restored.preserves_ids());
    assert!(restored.memory_accounting().is_some());
    let acc = restored.memory_accounting().unwrap();
    assert_eq!(acc.anonymous_proportional_structure_bytes, 0);
    assert!(acc.is_disk_native_graph());

    assert_eq!(
        restored.get_node_property(a, &PropertyKey::new("name")),
        Some(Value::String(arcstr::ArcStr::from("Alix")))
    );
    assert_eq!(
        restored.get_node_property(b, &PropertyKey::new("rank")),
        Some(Value::Int64(2))
    );
    use crate::graph::Direction;
    use crate::graph::traits::GraphStore;
    let out = restored.neighbors(a, Direction::Outgoing);
    assert!(out.contains(&b), "CSR forward must survive v5 mapped open");
}
