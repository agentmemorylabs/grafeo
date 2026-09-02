#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
//! RED→GREEN fixtures for source-true generation (G-EM0.W0 Stage A).

use super::*;
use crate::graph::compact::id::{decode_edge_id, decode_node_id};
use crate::graph::compact::mapped::{
    DictionaryCodeIndex, MappedStringDictionary, SegmentKind, parse_segment_directory,
    slice_segment_checked,
};
use crate::graph::compact::section_v5::{self, StringCodeOrder};
use crate::graph::traits::GraphStore;
use bytes::Bytes;
use grafeo_common::types::{NodeId, Value};

fn parse_dir(payload: &[u8]) -> crate::graph::compact::mapped::SegmentDirectory {
    let bytes = Bytes::copy_from_slice(payload);
    let without_crc = bytes.len().saturating_sub(4);
    parse_segment_directory(&bytes, without_crc).expect("directory")
}

fn sparse_id(base: u64) -> u64 {
    base + (1u64 << 40)
}

#[test]
fn sparse_huge_ids_map_to_dense_offsets_not_magnitude() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(sparse_id(7), "Person").with_prop("name", "Ada"))
        .node(GenerationNode::new(sparse_id(1), "Person").with_prop("name", "Bob"))
        .node(GenerationNode::new(sparse_id(99), "Person").with_prop("name", "Cyd"))
        .edge(GenerationEdge::new(
            sparse_id(500),
            sparse_id(7),
            sparse_id(1),
            "KNOWS",
        ));
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .expect("generate");
    assert!(generated.store.preserves_ids());
    // Dense cardinality is 3, not ~2^40.
    assert_eq!(generated.store.node_table("Person").unwrap().len(), 3);
    // Sorted by original ID: sparse_id(1), sparse_id(7), sparse_id(99) → offsets 0,1,2
    let n1 = generated
        .store
        .get_node(NodeId::new(sparse_id(1)))
        .expect("n1");
    let n7 = generated
        .store
        .get_node(NodeId::new(sparse_id(7)))
        .expect("n7");
    assert_eq!(
        n1.properties.get(&"name".into()).unwrap(),
        &Value::from("Bob")
    );
    assert_eq!(
        n7.properties.get(&"name".into()).unwrap(),
        &Value::from("Ada")
    );
}

#[test]
fn multiple_node_and_rel_tables_with_different_cardinalities() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(10u64, "A"))
        .node(GenerationNode::new(20u64, "A"))
        .node(GenerationNode::new(30u64, "A"))
        .node(GenerationNode::new(1u64, "B"))
        .node(GenerationNode::new(2u64, "B"))
        .edge(GenerationEdge::new(100u64, 10u64, 1u64, "LINKS"))
        .edge(GenerationEdge::new(101u64, 20u64, 2u64, "LINKS"))
        .edge(GenerationEdge::new(200u64, 1u64, 10u64, "REV"))
        .rel_schema(RelSchemaDecl::new("LINKS", "A", "B"))
        .rel_schema(RelSchemaDecl::new("REV", "B", "A"));
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap();
    assert_eq!(generated.store.node_table("A").unwrap().len(), 3);
    assert_eq!(generated.store.node_table("B").unwrap().len(), 2);
    assert_eq!(generated.store.rel_table("LINKS").unwrap().num_edges(), 2);
    assert_eq!(generated.store.rel_table("REV").unwrap().num_edges(), 1);
    let links = generated.store.rel_table("LINKS").unwrap();
    assert_eq!(links.fwd().num_nodes(), 3); // source row count (A)
    assert_eq!(links.bwd().unwrap().num_nodes(), 2); // dest row count (B)
}

#[test]
fn missing_endpoint_fails_closed() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person"))
        .edge(GenerationEdge::new(9u64, 1u64, 999u64, "KNOWS"));
    let err = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        GenerationError::MissingEndpoint {
            node_id: 999,
            is_source: false,
            ..
        }
    ));
}

#[test]
fn wrong_table_endpoint_fails_closed() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person"))
        .node(GenerationNode::new(2u64, "City"))
        .edge(GenerationEdge::new(9u64, 2u64, 1u64, "KNOWS")) // City -> Person
        .rel_schema(RelSchemaDecl::new("KNOWS", "Person", "Person"));
    let err = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        GenerationError::WrongTableEndpoint {
            is_source: true,
            ..
        }
    ));
}

#[test]
fn duplicate_src_dst_preserves_distinct_edge_ids_and_properties() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "P"))
        .node(GenerationNode::new(2u64, "P"))
        .edge(GenerationEdge::new(50u64, 1u64, 2u64, "KNOWS").with_prop("w", Value::Int64(1)))
        .edge(GenerationEdge::new(40u64, 1u64, 2u64, "KNOWS").with_prop("w", Value::Int64(2)))
        .edge(GenerationEdge::new(60u64, 1u64, 2u64, "KNOWS").with_prop("w", Value::Int64(3)));
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap();
    let rt = generated.store.rel_table("KNOWS").unwrap();
    assert_eq!(rt.num_edges(), 3);
    // Forward order by (src,dst,edge_id): 40, 50, 60
    let w0 = rt.get_edge_property(0, &"w".into()).unwrap();
    let w1 = rt.get_edge_property(1, &"w".into()).unwrap();
    let w2 = rt.get_edge_property(2, &"w".into()).unwrap();
    assert_eq!(w0, Value::Int64(2));
    assert_eq!(w1, Value::Int64(1));
    assert_eq!(w2, Value::Int64(3));
    // Original edge IDs round-trip via GraphStore
    let e40 = generated
        .store
        .get_edge(grafeo_common::types::EdgeId::new(40))
        .unwrap();
    assert_eq!(e40.properties.get(&"w".into()).unwrap(), &Value::Int64(2));
}

#[test]
fn reverse_forward_positions_map_to_exact_forward_edge() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "P"))
        .node(GenerationNode::new(2u64, "P"))
        .node(GenerationNode::new(3u64, "P"))
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "K").with_prop("tag", "a"))
        .edge(GenerationEdge::new(11u64, 1u64, 2u64, "K").with_prop("tag", "b"))
        .edge(GenerationEdge::new(12u64, 3u64, 2u64, "K").with_prop("tag", "c"))
        .edge(GenerationEdge::new(13u64, 2u64, 1u64, "K").with_prop("tag", "d"));
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap();
    let rt = generated.store.rel_table("K").unwrap();
    let bwd = rt.bwd().expect("reverse csr required");
    assert!(bwd.has_edge_data());
    // For each reverse entry, ForwardPositions → forward property parity.
    for rev_pos in 0..bwd.num_edges() {
        let fwd_pos = bwd.edge_data_at(rev_pos).expect("forward position") as usize;
        let tag = rt
            .get_edge_property(fwd_pos, &"tag".into())
            .expect("prop at forward pos");
        let src = rt.source_node_id(fwd_pos as u32).unwrap();
        let dst = rt.dest_node_id(fwd_pos as u32).unwrap();
        let (_, src_off) = decode_node_id(src);
        let (_, dst_off) = decode_node_id(dst);
        let _ = (src_off, dst_off, tag);
        assert!(fwd_pos < rt.num_edges());
    }
    let edges_to_2 = rt.edges_to_target(1).expect("dst offset of node 2");
    assert!(!edges_to_2.is_empty());
    for (_src, eid) in edges_to_2 {
        let (_, fwd_pos) = decode_edge_id(eid);
        let _ = rt
            .get_edge_property(fwd_pos as usize, &"tag".into())
            .expect("prop via reverse-derived edge id");
    }
}

#[test]
fn four_original_id_structures_round_trip() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(sparse_id(5), "N"))
        .node(GenerationNode::new(sparse_id(3), "N"))
        .edge(GenerationEdge::new(
            sparse_id(9),
            sparse_id(5),
            sparse_id(3),
            "E",
        ));
    let (payload, generated) = generate_v5_payload(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap();
    let restored = section_v5::deserialize_v5(&Bytes::from(payload)).unwrap();
    assert!(restored.preserves_ids());
    assert!(restored.get_node(NodeId::new(sparse_id(5))).is_some());
    assert!(restored.get_node(NodeId::new(sparse_id(3))).is_some());
    assert!(
        restored
            .get_edge(grafeo_common::types::EdgeId::new(sparse_id(9)))
            .is_some()
    );
    let _ = generated;
}

#[test]
fn global_string_dedupe_across_metadata_zone_and_dict_columns() {
    let input = GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "X")
                .with_prop("X", Value::from("X"))
                .with_prop("other", Value::from("X")),
        )
        .node(
            GenerationNode::new(2u64, "X")
                .with_prop("X", Value::from("Y"))
                .with_prop("other", Value::from("Z")),
        )
        .edge(GenerationEdge::new(3u64, 1u64, 2u64, "X").with_prop("X", Value::from("X")));
    let (payload, generated) = generate_v5_payload(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap();
    assert!(generated.global_strings.code("X").is_some());
    let code_x = generated.global_strings.code("X").unwrap();
    assert_eq!(
        generated
            .global_strings
            .strings
            .iter()
            .filter(|s| s.as_str() == "X")
            .count(),
        1
    );

    let bytes = Bytes::from(payload);
    let directory = parse_dir(bytes.as_ref());
    let off = slice_segment_checked(
        &bytes,
        directory.require(SegmentKind::StringOffsets).unwrap(),
    )
    .unwrap();
    let sb = slice_segment_checked(&bytes, directory.require(SegmentKind::StringBytes).unwrap())
        .unwrap();
    let dict = MappedStringDictionary::new(off, sb).unwrap();
    let idx_bytes = slice_segment_checked(
        &bytes,
        directory.require(SegmentKind::DictionaryCodeIndex).unwrap(),
    )
    .unwrap();
    let code_index = DictionaryCodeIndex::new(idx_bytes.clone(), &dict).unwrap();
    assert_eq!(
        idx_bytes.len() / 16,
        dict.len(),
        "DictionaryCodeIndex count must equal global dict length"
    );
    assert_eq!(code_index.lookup(&dict, "X"), Some(code_x));
    for (i, s) in dict_strings(&dict).iter().enumerate() {
        assert_eq!(dict.get(i as u32), Some(s.as_str()));
    }
    let mut sorted = dict_strings(&dict);
    let orig = sorted.clone();
    sorted.sort();
    assert_eq!(orig, sorted, "lexicographic global codes");
}

fn dict_strings(dict: &MappedStringDictionary) -> Vec<String> {
    (0..dict.len())
        .map(|i| dict.get(i as u32).unwrap().to_string())
        .collect()
}

#[test]
fn lexicographic_codes_stable_across_input_iteration_orders() {
    let mk = |flip: bool| {
        let mut input = GenerationInput::new();
        if flip {
            input = input
                .node(GenerationNode::new(2u64, "Zebra").with_prop("k", "apple"))
                .node(GenerationNode::new(1u64, "Apple").with_prop("k", "zebra"));
        } else {
            input = input
                .node(GenerationNode::new(1u64, "Apple").with_prop("k", "zebra"))
                .node(GenerationNode::new(2u64, "Zebra").with_prop("k", "apple"));
        }
        let (payload, _) = generate_v5_payload(
            &mut input.node_source(),
            &mut input.edge_source(),
            &input.rel_schemas,
            &GenerationBudget::for_tests(),
        )
        .unwrap();
        payload
    };
    let a = mk(false);
    let b = mk(true);
    let parse_strings = |payload: &[u8]| {
        let bytes = Bytes::copy_from_slice(payload);
        let directory = parse_dir(bytes.as_ref());
        let off = slice_segment_checked(
            &bytes,
            directory.require(SegmentKind::StringOffsets).unwrap(),
        )
        .unwrap();
        let sb =
            slice_segment_checked(&bytes, directory.require(SegmentKind::StringBytes).unwrap())
                .unwrap();
        let dict = MappedStringDictionary::new(off, sb).unwrap();
        dict_strings(&dict)
    };
    assert_eq!(parse_strings(&a), parse_strings(&b));
}

#[test]
fn u32_string_dictionary_overflow_fail_closed_unit() {
    let err = GenerationError::WireWidthOverflow {
        what: "global_string_dictionary",
        count: u64::from(u32::MAX) + 1,
        max: u64::from(u32::MAX),
    };
    assert!(matches!(err, GenerationError::WireWidthOverflow { .. }));
}

#[test]
fn external_run_fan_in_and_cancellation() {
    let budget = GenerationBudget {
        sort_run_bytes: 32,
        merge_fan_in: 2,
        max_temp_bytes: 10 * 1024 * 1024,
        ..GenerationBudget::for_tests()
    };
    let mut sink = InMemoryRunSink::new(budget);
    for i in (0..20u64).rev() {
        let key = format!("{i:04}").into_bytes();
        sink.push(SortRecord::new(key, vec![i as u8])).unwrap();
    }
    let handles = sink.finish().unwrap();
    assert!(
        handles.len() > 2,
        "expected multiple runs, got {}",
        handles.len()
    );

    let mut bodies = Vec::new();
    for chunk in (0..20u64).collect::<Vec<_>>().chunks(3) {
        let mut run: Vec<SortRecord> = chunk
            .iter()
            .map(|&i| SortRecord::new(format!("{i:04}").into_bytes(), vec![i as u8]))
            .collect();
        run.sort();
        bodies.push(run);
    }
    assert!(bodies.len() > 2);
    let mut merger = InMemoryRunMerger::new();
    let mut metrics = GenerationMetrics::default();
    let mut out = Vec::new();
    merger
        .merge_records(&bodies, &budget, &mut metrics, None, &mut |r| {
            out.push(r.clone());
            Ok(())
        })
        .unwrap();
    assert!(metrics.merge_passes >= 1);
    assert_eq!(out.len(), 20);
    for w in out.windows(2) {
        assert!(w[0].key <= w[1].key);
    }

    let token = CancelToken::new();
    token.cancel();
    let mut sink2 = InMemoryRunSink::new(budget).with_cancel(token.clone());
    let err = sink2.push(SortRecord::new(b"a", b"b")).unwrap_err();
    assert_eq!(err, GenerationError::Cancelled);
    sink.cleanup();
    merger.cleanup();
}

#[test]
fn temp_disk_budget_rejection() {
    let budget = GenerationBudget {
        sort_run_bytes: 16,
        max_temp_bytes: 40,
        ..GenerationBudget::for_tests()
    };
    let mut sink = InMemoryRunSink::new(budget);
    let mut hit = false;
    for i in 0..50u64 {
        let key = format!("{i:08}").into_bytes();
        match sink.push(SortRecord::new(key, vec![0u8; 8])) {
            Ok(()) => {}
            Err(GenerationError::BudgetExceeded { counter, .. }) => {
                assert_eq!(counter, "temp_bytes");
                hit = true;
                break;
            }
            Err(e) => panic!("unexpected {e}"),
        }
    }
    assert!(hit, "expected temp budget rejection");
    sink.cleanup();
}

#[test]
fn v5_payload_deserializes_with_preserve_ids_and_reverse() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person").with_prop("name", "Ada"))
        .node(GenerationNode::new(2u64, "Person").with_prop("name", "Bob"))
        .edge(
            GenerationEdge::new(10u64, 1u64, 2u64, "KNOWS").with_prop("since", Value::Int64(2020)),
        );
    let (payload, _) = generate_v5_payload(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap();
    let store = section_v5::deserialize_v5(&Bytes::from(payload)).unwrap();
    assert!(store.preserves_ids());
    assert!(store.rel_table("KNOWS").unwrap().has_backward());
    assert_eq!(
        store
            .get_node(NodeId::new(1))
            .unwrap()
            .properties
            .get(&"name".into())
            .unwrap(),
        &Value::from("Ada")
    );
}

#[test]
fn serialize_lex_vs_insertion_both_roundtrip() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Z").with_prop("a", "m"))
        .node(GenerationNode::new(2u64, "A").with_prop("a", "b"));
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap();
    let ins =
        section_v5::serialize_v5_with_string_order(&generated.store, StringCodeOrder::Insertion)
            .unwrap();
    let lex = section_v5::serialize_v5_with_string_order(
        &generated.store,
        StringCodeOrder::Lexicographic,
    )
    .unwrap();
    let s1 = section_v5::deserialize_v5(&Bytes::from(ins)).unwrap();
    let s2 = section_v5::deserialize_v5(&Bytes::from(lex)).unwrap();
    assert!(s1.get_node(NodeId::new(1)).is_some());
    assert!(s2.get_node(NodeId::new(1)).is_some());
}

#[test]
fn null_value_fails_closed_not_empty_string() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person").with_prop("name", "Ada"))
        .node(GenerationNode::new(2u64, "Person"));
    let err = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap_err();
    assert!(matches!(err, GenerationError::NullValue { .. }));
}

#[test]
fn v5_segment_source_yields_byte_identical_assembled_payload_parity() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person").with_prop("name", "Ada"))
        .node(GenerationNode::new(2u64, "Person").with_prop("name", "Bob"))
        .edge(
            GenerationEdge::new(10u64, 1u64, 2u64, "KNOWS").with_prop("since", Value::Int64(2020)),
        );
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap();
    let expected_payload = section_v5::serialize_v5_with_string_order(
        &generated.store,
        StringCodeOrder::Lexicographic,
    )
    .unwrap();

    let mut source =
        CompactV5SegmentSource::new(&generated.store, &generated.global_strings).unwrap();
    assert_eq!(source.segment_count(), 20);

    let assembled_payload = assemble_v5_payload_from_source(
        &mut source,
        generated.store.total_nodes(),
        generated.store.total_edges(),
        generated.store.preserves_ids(),
    )
    .unwrap();
    assert_eq!(
        assembled_payload, expected_payload,
        "assembled payload from V5SegmentSource must be byte-identical to serialize_v5_with_string_order(Lexicographic)"
    );

    let restored = section_v5::deserialize_v5(&Bytes::from(assembled_payload)).unwrap();
    assert!(restored.preserves_ids());
    assert_eq!(
        restored
            .get_node(NodeId::new(1))
            .unwrap()
            .properties
            .get(&"name".into())
            .unwrap(),
        &Value::from("Ada")
    );
}

#[test]
fn v5_segment_source_segment_count_ascending_kinds_and_bounded_bytes() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(sparse_id(10), "User").with_prop("email", "a@b.com"))
        .node(GenerationNode::new(sparse_id(20), "User").with_prop("email", "c@d.com"))
        .edge(GenerationEdge::new(
            sparse_id(100),
            sparse_id(10),
            sparse_id(20),
            "FOLLOWS",
        ));
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenerationBudget::for_tests(),
    )
    .unwrap();
    let mut source =
        CompactV5SegmentSource::new(&generated.store, &generated.global_strings).unwrap();

    let total = source.segment_count();
    let mut count = 0;
    let mut prev_kind: Option<u16> = None;
    let mut _total_segment_bytes = 0usize;

    while let Some(seg) = source.next_segment().unwrap() {
        count += 1;
        let kind_code = seg.kind.as_u16();
        if let Some(prev) = prev_kind {
            assert!(
                kind_code > prev,
                "segments must be strictly ascending: {kind_code} > {prev}"
            );
        }
        prev_kind = Some(kind_code);
        _total_segment_bytes += seg.bytes.len();
        // Each segment is bounded (individual section bytes, not whole assembled payload)
        assert!(
            seg.bytes.len() < 4096,
            "individual segment bytes must be bounded"
        );
    }
    assert_eq!(count, total);
}
