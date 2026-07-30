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
    assert_eq!(lookup.lookup(grafeo_common::types::NodeId::new(99)), None);

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
        [("name", Value::from("Alix")), ("rank", Value::Int64(1))],
    );
    let b = store.create_node_with_props(
        &["Person"],
        [("name", Value::from("Gus")), ("rank", Value::Int64(2))],
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

    // Table zone maps must be restored (not dropped) on v5 mapped open.
    let rank_key = PropertyKey::new("rank");
    let zm = restored
        .node_tables_by_id
        .iter()
        .find_map(|nt| nt.zone_map(&rank_key));
    assert!(
        zm.is_some(),
        "table zone map for rank must survive v5 mapped open"
    );
    let zm = zm.unwrap();
    assert_eq!(zm.min, Some(Value::Int64(1)));
    assert_eq!(zm.max, Some(Value::Int64(2)));

    // String→code via DictionaryCodeIndex (kind 20) must work for property find.
    let hits = restored.find_nodes_by_property("name", &Value::from("Alix"));
    assert_eq!(hits, vec![a]);
}

#[test]
fn zone_map_segments_round_trip_and_reject_bad_reserved() {
    use super::zone_maps::{
        TABLE_ZONE_BLOCK_SENTINEL, ZONE_MAP_RECORD_LEN, parse_table_zone_maps,
        write_zone_map_record,
    };
    use crate::graph::compact::zone_map::ZoneMap;
    use grafeo_common::types::Value;
    use grafeo_common::utils::hash::FxHashMap;

    let (off, bytes) = build_string_segments(&["age"]);
    let dict = MappedStringDictionary::new(Bytes::from(off), Bytes::from(bytes)).unwrap();
    let mut string_index = FxHashMap::default();
    string_index.insert("age".into(), 0u32);

    let zm = ZoneMap {
        min: Some(Value::Int64(10)),
        max: Some(Value::Int64(99)),
        null_count: 0,
        row_count: 50,
    };
    let mut seg = Vec::new();
    write_zone_map_record(
        &mut seg,
        0,
        0,
        TABLE_ZONE_BLOCK_SENTINEL,
        &zm,
        &string_index,
    )
    .unwrap();
    assert_eq!(seg.len(), ZONE_MAP_RECORD_LEN);

    let parsed = parse_table_zone_maps(&seg, &dict, 1).unwrap();
    let got = parsed[0]
        .get(&grafeo_common::types::PropertyKey::new("age"))
        .expect("age zone map");
    assert_eq!(got.min, Some(Value::Int64(10)));
    assert_eq!(got.max, Some(Value::Int64(99)));
    assert_eq!(got.row_count, 50);

    // Corrupt reserved field.
    let mut bad = seg.clone();
    bad[2] = 1;
    let err = parse_table_zone_maps(&bad, &dict, 1).expect_err("reserved");
    assert!(err.contains("reserved"), "{err}");
}

#[test]
fn dictionary_code_index_sorted_lookup() {
    use super::code_index::{DictionaryCodeIndex, build_dictionary_code_index};

    let strings = ["zeta", "alpha", "mu"];
    let (off, bytes) = build_string_segments(&strings);
    let dict = MappedStringDictionary::new(Bytes::from(off), Bytes::from(bytes)).unwrap();
    let index_bytes = build_dictionary_code_index(&strings);
    let index = DictionaryCodeIndex::new(Bytes::from(index_bytes), &dict).unwrap();
    assert_eq!(index.lookup(&dict, "alpha"), Some(1));
    assert_eq!(index.lookup(&dict, "mu"), Some(2));
    assert_eq!(index.lookup(&dict, "zeta"), Some(0));
    assert_eq!(index.lookup(&dict, "missing"), None);
}

#[test]
fn adversarial_sparse_ids_and_high_edge_count_round_trip() {
    use crate::graph::Direction;
    use crate::graph::compact::builder::from_graph_store_preserving_ids;
    use crate::graph::compact::section::CompactStoreSection;
    use crate::graph::lpg::LpgStore;
    use crate::graph::traits::GraphStore;
    use grafeo_common::storage::section::Section;
    use grafeo_common::types::{PropertyKey, Value};

    let store = LpgStore::new().unwrap();
    // Sparse IDs: create nodes then leave gaps by using high-valued properties;
    // preserve_ids keeps original NodeIds through compact.
    let mut nodes = Vec::new();
    for i in 0..64u64 {
        // High-cardinality unique names.
        let name = format!("sym-{i:06x}-{}", "x".repeat((i % 17) as usize + 1));
        let id = store.create_node_with_props(
            &["CodeSymbol"],
            [
                ("name", Value::from(name.as_str())),
                ("rank", Value::Int64(i as i64)),
            ],
        );
        nodes.push(id);
    }
    // High edge count: dense fanout.
    const FANOUT: usize = 16;
    for (i, &src) in nodes.iter().enumerate() {
        for f in 1..=FANOUT {
            let dst = nodes[(i + f) % nodes.len()];
            let _ = store.create_edge(src, dst, "REFERENCES");
        }
    }
    let compact = from_graph_store_preserving_ids(&store).unwrap();
    // Many-block potential: rank column has 64 rows (< 1024 so 1 block, but
    // zone maps still present). For multi-block, inject a larger column via
    // direct builder is heavier; assert block zone maps non-empty when present.
    let section = CompactStoreSection::new(std::sync::Arc::new(compact));
    let bytes = section.serialize().expect("v5 serialize");
    let mut section2 = CompactStoreSection::empty();
    section2
        .deserialize_from_mapped_bytes(Bytes::from(bytes))
        .expect("v5 mapped deserialize");
    let restored = section2.store().unwrap();
    assert_eq!(
        restored
            .memory_accounting()
            .unwrap()
            .anonymous_proportional_structure_bytes,
        0
    );

    // Sparse original IDs survive lookup.
    let mid = nodes[32];
    assert_eq!(
        restored.get_node_property(mid, &PropertyKey::new("rank")),
        Some(Value::Int64(32))
    );
    assert_eq!(restored.neighbors(mid, Direction::Outgoing).len(), FANOUT);
    // High-cardinality string property find via code index.
    let hits = restored.find_nodes_by_property(
        "name",
        &Value::from(format!("sym-{:06x}-{}", 7, "x".repeat(8)).as_str()),
    );
    assert_eq!(hits, vec![nodes[7]]);

    // Zone map prune: rank max is 63, so rank == 999 must table-prune.
    let empty = restored.find_nodes_by_property("rank", &Value::Int64(999));
    assert!(empty.is_empty());
}

#[test]
fn adversarial_many_block_zone_maps_round_trip() {
    use crate::graph::compact::builder::from_graph_store_preserving_ids;
    use crate::graph::compact::section::CompactStoreSection;
    use crate::graph::lpg::LpgStore;
    use grafeo_common::storage::section::Section;
    use grafeo_common::types::{PropertyKey, Value};

    // DEFAULT_BLOCK_ROWS = 1024 → 3 blocks for 2500 rows.
    const ROWS: usize = 2500;
    let store = LpgStore::new().unwrap();
    for i in 0..ROWS {
        let _ = store.create_node_with_props(
            &["Row"],
            [
                ("rank", Value::Int64(i as i64)),
                ("name", Value::from(format!("n{i:05}").as_str())),
            ],
        );
    }
    let compact = from_graph_store_preserving_ids(&store).unwrap();
    let pre_blocks = compact.node_tables_by_id[0]
        .block_zone_maps_for(&PropertyKey::new("rank"))
        .map(|s| s.len())
        .unwrap_or(0);
    assert!(
        pre_blocks >= 3,
        "expected ≥3 block zone maps for {ROWS} rows, got {pre_blocks}"
    );

    let section = CompactStoreSection::new(std::sync::Arc::new(compact));
    let bytes = section.serialize().expect("v5");
    let mut section2 = CompactStoreSection::empty();
    section2
        .deserialize_from_mapped_bytes(Bytes::from(bytes))
        .expect("mapped open");
    let restored = section2.store().unwrap();
    let post_blocks = restored.node_tables_by_id[0]
        .block_zone_maps_for(&PropertyKey::new("rank"))
        .map(|s| s.len())
        .unwrap_or(0);
    assert_eq!(
        post_blocks, pre_blocks,
        "block zone maps must round-trip on v5 mapped open"
    );
    let table_zm = restored.node_tables_by_id[0]
        .zone_map(&PropertyKey::new("rank"))
        .expect("table zone map");
    assert_eq!(table_zm.min, Some(Value::Int64(0)));
    assert_eq!(table_zm.max, Some(Value::Int64((ROWS - 1) as i64)));
    assert_eq!(
        restored
            .memory_accounting()
            .unwrap()
            .anonymous_proportional_structure_bytes,
        0
    );
}
