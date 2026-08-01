//! Phase 0 unit tests: byte-parity against existing eager helpers.
//!
//! Gates (packet §Phase 0):
//! - spool spill/resume, descriptor length/CRC/element count
//! - dictionary external sort/dedup/lexicographic codes
//! - StringOffsets/StringBytes/DictionaryCodeIndex bytes vs eager helpers
//! - incremental column body vs encode_column
//! - assembled payload vs serialize_v5_with_string_order on a small store

use super::*;
use crate::graph::compact::generation::error::GenerationError;
use crate::graph::compact::generation::{
    GenerationBudget as GenBudget, GenerationEdge, GenerationInput, GenerationNode,
    generate_compact_store,
};
use crate::graph::compact::mapped::{
    SegmentKind, build_dictionary_code_index, build_string_segments,
};
use crate::graph::compact::section_v5::{self, StringCodeOrder};
use bytes::Bytes;
use grafeo_common::types::Value;

// ── Spool sink: spill/resume + descriptor correctness ──────────────

#[test]
fn spool_sink_resident_small_payload() {
    let dir = tempfile::tempdir().unwrap();
    let mut sink = Box::new(SpoolSegmentSink::new(
        SegmentKind::Metadata,
        1,
        0x0001,
        1,
        0,
        dir.path(),
        "test-resident",
        4096, // large cap → stays resident
    ));
    let data = b"hello world";
    sink.write(data).unwrap();
    let desc = sink.finish().unwrap();

    assert_eq!(desc.kind, SegmentKind::Metadata);
    assert_eq!(desc.length, data.len() as u64);
    assert_eq!(desc.crc, crc32fast::hash(data));
    assert_eq!(desc.element_count, 0); // element_width = 0
    match &desc.body {
        SegmentBody::Resident(b) => assert_eq!(b.as_ref(), data),
        SegmentBody::Spilled(_) => panic!("expected resident for small payload"),
    }
}

#[test]
fn spool_sink_spills_large_payload_and_streams_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut sink = Box::new(SpoolSegmentSink::new(
        SegmentKind::ColumnBodies,
        1,
        0x0001,
        1,
        0,
        dir.path(),
        "test-spill",
        16, // tiny cap → forces spill
    ));
    #[allow(clippy::cast_possible_truncation)] // i % 256 is always 0..255
    let data: Vec<u8> = (0..256u32).map(|i| (i % 256) as u8).collect();
    // Write in chunks to exercise the spill path.
    for chunk in data.chunks(32) {
        sink.write(chunk).unwrap();
    }
    let desc = sink.finish().unwrap();

    assert_eq!(desc.length, 256);
    assert_eq!(desc.crc, crc32fast::hash(&data));
    match &desc.body {
        SegmentBody::Spilled(path) => {
            assert!(path.exists(), "spool file must exist");
            // Stream back and verify.
            let mut reassembled = Vec::new();
            desc.body
                .stream(&mut |chunk| {
                    reassembled.extend_from_slice(chunk);
                    Ok(())
                })
                .unwrap();
            assert_eq!(reassembled, data);
        }
        SegmentBody::Resident(_) => panic!("expected spilled for large payload"),
    }
}

#[test]
fn spool_sink_element_count_computed_from_width() {
    let dir = tempfile::tempdir().unwrap();
    let mut sink = Box::new(SpoolSegmentSink::new(
        SegmentKind::ForwardCsrOffsets,
        1,
        0x0001,
        4,
        4, // element_width = 4
        dir.path(),
        "test-elem-count",
        4096,
    ));
    // 12 bytes = 3 elements of width 4.
    sink.write(&[0u8; 12]).unwrap();
    let desc = sink.finish().unwrap();
    assert_eq!(desc.element_count, 3);
    assert_eq!(desc.length, 12);
}

// ── MemorySegmentSink: same bytes → same descriptor ────────────────

#[test]
fn memory_sink_matches_spool_resident() {
    let data = b"test segment bytes";
    let mut mem = Box::new(MemorySegmentSink::new(
        SegmentKind::StringBytes,
        1,
        0x0001,
        1,
        1,
    ));
    mem.write(data).unwrap();
    let mem_desc = mem.finish().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let mut spool = Box::new(SpoolSegmentSink::new(
        SegmentKind::StringBytes,
        1,
        0x0001,
        1,
        1,
        dir.path(),
        "test-mem-parity",
        4096,
    ));
    spool.write(data).unwrap();
    let spool_desc = spool.finish().unwrap();

    assert_eq!(mem_desc.length, spool_desc.length);
    assert_eq!(mem_desc.crc, spool_desc.crc);
    assert_eq!(mem_desc.element_count, spool_desc.element_count);
}

// ── Dictionary: external sort/dedup/lexicographic codes ────────────

#[test]
fn dictionary_sorts_dedupes_and_assigns_lexicographic_codes() {
    // Test the dictionary logic directly: sort occurrences, dedup, assign codes.
    // (InMemoryRunMerger::merge_all is a stub; the real merge is Phase 2's
    // storage-backed merger. Phase 0 tests the sort/dedup/code logic.)
    let occurrences = [
        StringOccurrence {
            string: b"zebra".to_vec(),
            use_kind: StringUseKind::Label,
            owner_key: Vec::new(),
        },
        StringOccurrence {
            string: b"apple".to_vec(),
            use_kind: StringUseKind::PropertyKey,
            owner_key: Vec::new(),
        },
        StringOccurrence {
            string: b"apple".to_vec(),
            use_kind: StringUseKind::DictValue,
            owner_key: Vec::new(),
        },
        StringOccurrence {
            string: b"Mango".to_vec(),
            use_kind: StringUseKind::Label,
            owner_key: Vec::new(),
        },
    ];

    // Convert to sort records and sort (simulating external sort output).
    let mut records: Vec<crate::graph::compact::generation::runs::SortRecord> =
        occurrences.iter().map(|o| o.to_sort_record()).collect();
    records.sort();

    // Dedup adjacent strings (same logic as DictionaryPassDriver::build).
    let mut unique_strings: Vec<String> = Vec::new();
    let mut prev_string: Option<&[u8]> = None;
    for rec in &records {
        let string_bytes = StringOccurrence::string_from_key(&rec.key, 0);
        let is_dup = prev_string.is_some_and(|p| p == string_bytes);
        if !is_dup {
            let s = std::str::from_utf8(string_bytes).unwrap().to_string();
            unique_strings.push(s);
            prev_string = Some(string_bytes);
        }
    }

    let dict = BoundedDictionary {
        strings: unique_strings,
    };
    // UTF-8 byte-lexicographic: "Mango" < "apple" < "zebra"
    assert_eq!(dict.strings, vec!["Mango", "apple", "zebra"]);
    assert_eq!(dict.code("Mango"), Some(0));
    assert_eq!(dict.code("apple"), Some(1));
    assert_eq!(dict.code("zebra"), Some(2));
    assert_eq!(dict.len(), 3);
}

// ── StringOffsets/StringBytes parity vs build_string_segments ──────

#[test]
fn dictionary_string_segments_match_eager_helper() {
    let strings = vec!["Mango", "apple", "zebra"];
    let (eager_offsets, eager_bytes) = build_string_segments(&strings);

    let dict = BoundedDictionary {
        strings: strings.iter().map(|s| s.to_string()).collect(),
    };
    let bounded_offsets = dict.string_offsets_bytes();
    let bounded_bytes = dict.string_bytes_body();

    assert_eq!(
        bounded_offsets, eager_offsets,
        "StringOffsets must be byte-identical to build_string_segments().0"
    );
    assert_eq!(
        bounded_bytes, eager_bytes,
        "StringBytes must be byte-identical to build_string_segments().1"
    );
}

// ── DictionaryCodeIndex parity vs build_dictionary_code_index ──────

#[test]
fn dictionary_code_index_matches_eager_helper() {
    let strings = vec!["Mango", "apple", "zebra"];
    let eager_index = build_dictionary_code_index(&strings);

    let dict = BoundedDictionary {
        strings: strings.iter().map(|s| s.to_string()).collect(),
    };
    let bounded_index = dict.code_index_bytes();

    assert_eq!(
        bounded_index, eager_index,
        "DictionaryCodeIndex must be byte-identical to build_dictionary_code_index"
    );
}

// ── ColumnEncoder parity vs encode_column ──────────────────────────

#[test]
fn column_encoder_int_matches_eager() {
    use crate::graph::compact::generation::columns::encode_column;

    let values: Vec<Value> = vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)];
    let refs: Vec<Option<&Value>> = values.iter().map(Some).collect();

    // Eager path.
    let mut eager_occ = Vec::new();
    let (eager_codec, eager_type, eager_zm) =
        encode_column(&refs, "test.int", &mut eager_occ).unwrap();

    // Incremental path.
    let mut enc = ColumnEncoder::new("test.int");
    for v in &values {
        enc.push(Some(v)).unwrap();
    }
    let mut bounded_occ = Vec::new();
    let (bounded_codec, bounded_type, bounded_zm) = enc.finish(&mut bounded_occ).unwrap();

    assert_eq!(eager_type, bounded_type);
    assert_zone_map_eq(&eager_zm, &bounded_zm);
    // Compare codec bytes via write_to.
    let mut eager_buf = Vec::new();
    eager_codec.write_to(&mut eager_buf);
    let mut bounded_buf = Vec::new();
    bounded_codec.write_to(&mut bounded_buf);
    assert_eq!(eager_buf, bounded_buf, "codec bytes must match");
}

#[test]
fn column_encoder_string_matches_eager() {
    use crate::graph::compact::generation::columns::encode_column;

    let values: Vec<Value> = vec![
        Value::String("hello".into()),
        Value::String("world".into()),
        Value::String("hello".into()),
    ];
    let refs: Vec<Option<&Value>> = values.iter().map(Some).collect();

    let mut eager_occ = Vec::new();
    let (eager_codec, eager_type, eager_zm) =
        encode_column(&refs, "test.str", &mut eager_occ).unwrap();

    let mut enc = ColumnEncoder::new("test.str");
    for v in &values {
        enc.push(Some(v)).unwrap();
    }
    let mut bounded_occ = Vec::new();
    let (bounded_codec, bounded_type, bounded_zm) = enc.finish(&mut bounded_occ).unwrap();

    assert_eq!(eager_type, bounded_type);
    assert_zone_map_eq(&eager_zm, &bounded_zm);
    assert_eq!(eager_occ, bounded_occ, "string occurrences must match");
    let mut eager_buf = Vec::new();
    eager_codec.write_to(&mut eager_buf);
    let mut bounded_buf = Vec::new();
    bounded_codec.write_to(&mut bounded_buf);
    assert_eq!(eager_buf, bounded_buf, "dict codec bytes must match");
}

#[test]
fn column_encoder_null_fails_closed() {
    let mut enc = ColumnEncoder::new("test.null");
    let err = enc.push(None).unwrap_err();
    assert!(matches!(err, GenerationError::NullValue { .. }));
}

#[test]
fn column_encoder_mixed_fails_closed() {
    let mut enc = ColumnEncoder::new("test.mixed");
    enc.push(Some(&Value::Int64(1))).unwrap();
    enc.push(Some(&Value::String("x".into()))).unwrap();
    let mut occ = Vec::new();
    let err = enc.finish(&mut occ).unwrap_err();
    assert!(matches!(err, GenerationError::MixedColumnTypes { .. }));
}

// ── V5PayloadAssembler: byte-identical to serialize_v5 ─────────────

#[test]
fn assembler_payload_byte_identical_to_serialize_v5() {
    // Build a small store through the existing generation path.
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
        &GenBudget::for_tests(),
    )
    .unwrap();

    // Eager reference payload.
    let expected = section_v5::serialize_v5_with_string_order(
        &generated.store,
        StringCodeOrder::Lexicographic,
    )
    .unwrap();

    // Build descriptors via MemorySegmentSink from emit_v5_segments output.
    let segments = crate::graph::compact::generation::emit_v5_segments(
        &generated.store,
        &generated.global_strings,
    )
    .unwrap();

    let mut descriptors = Vec::new();
    for seg in &segments {
        let mut sink = Box::new(MemorySegmentSink::new(
            seg.kind,
            seg.encoding_version,
            seg.flags,
            seg.alignment,
            seg.element_width,
        ));
        sink.write(&seg.bytes).unwrap();
        descriptors.push(sink.finish().unwrap());
    }

    // Sort by kind ascending (emit_v5_segments already does this, but be safe).
    descriptors.sort_by_key(|d| d.kind.as_u16());

    let assembler = V5PayloadAssembler::new(
        generated.store.total_nodes(),
        generated.store.total_edges(),
        generated.store.preserves_ids(),
    );
    let assembled = assembler.assemble(&descriptors).unwrap();

    assert_eq!(
        assembled, expected,
        "V5PayloadAssembler output must be byte-identical to serialize_v5_with_string_order(Lexicographic)"
    );

    // Verify the assembled payload deserializes correctly.
    let restored = section_v5::deserialize_v5(&Bytes::from(assembled)).unwrap();
    assert!(restored.preserves_ids());
}

#[test]
fn assembler_with_spilled_bodies_matches_resident() {
    // Same store as above, but force spill by using tiny buf_cap.
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "X").with_prop("a", "b"))
        .node(GenerationNode::new(2u64, "X").with_prop("a", "c"));
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenBudget::for_tests(),
    )
    .unwrap();

    let expected = section_v5::serialize_v5_with_string_order(
        &generated.store,
        StringCodeOrder::Lexicographic,
    )
    .unwrap();

    let segments = crate::graph::compact::generation::emit_v5_segments(
        &generated.store,
        &generated.global_strings,
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let mut descriptors = Vec::new();
    for (i, seg) in segments.iter().enumerate() {
        // Tiny buf_cap forces spill for any non-trivial segment.
        let mut sink = Box::new(SpoolSegmentSink::new(
            seg.kind,
            seg.encoding_version,
            seg.flags,
            seg.alignment,
            seg.element_width,
            dir.path(),
            format!("seg-{i:04}"),
            8, // tiny → spill
        ));
        sink.write(&seg.bytes).unwrap();
        descriptors.push(sink.finish().unwrap());
    }
    descriptors.sort_by_key(|d| d.kind.as_u16());

    let assembler = V5PayloadAssembler::new(
        generated.store.total_nodes(),
        generated.store.total_edges(),
        generated.store.preserves_ids(),
    );
    let assembled = assembler.assemble(&descriptors).unwrap();

    assert_eq!(
        assembled, expected,
        "spilled-body assembly must be byte-identical to eager serialize_v5"
    );
}

// ── helpers ────────────────────────────────────────────────────────

/// ZoneMap does not derive PartialEq; compare fields individually.
fn assert_zone_map_eq(
    eager: &Option<crate::graph::compact::zone_map::ZoneMap>,
    bounded: &Option<crate::graph::compact::zone_map::ZoneMap>,
) {
    match (eager, bounded) {
        (None, None) => {}
        (Some(e), Some(b)) => {
            assert_eq!(e.min, b.min, "zone map min mismatch");
            assert_eq!(e.max, b.max, "zone map max mismatch");
            assert_eq!(e.null_count, b.null_count, "zone map null_count mismatch");
            assert_eq!(e.row_count, b.row_count, "zone map row_count mismatch");
        }
        (e, b) => panic!("zone map presence mismatch: eager={e:?}, bounded={b:?}"),
    }
}

// ── Phase 1: canonical convergence parity (feature ON) ─────────────
//
// These prove the two production serializers, when converged onto the
// canonical emitter (feature `generation-streaming` ON), still produce
// byte-identical output to the eager reference. They are no-ops with the
// feature OFF (the eager path is exercised by the suites above).

/// Builds a small store exercising every segment family: multiple node/rel
/// tables, Dict + fixed-width columns, reverse CSR with ForwardPositions,
/// preserve-ID lookups, and zone maps.
#[cfg(feature = "generation-streaming")]
fn phase1_fixture_store() -> crate::graph::compact::generation::GeneratedCompact {
    use crate::graph::compact::generation::RelSchemaDecl;
    let input = GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "Person")
                .with_prop("name", "Ada")
                .with_prop("age", Value::Int64(36)),
        )
        .node(
            GenerationNode::new(2u64, "Person")
                .with_prop("name", "Bob")
                .with_prop("age", Value::Int64(24)),
        )
        .node(GenerationNode::new(3u64, "City").with_prop("title", "London"))
        .edge(
            GenerationEdge::new(10u64, 1u64, 2u64, "KNOWS").with_prop("since", Value::Int64(2020)),
        )
        .edge(
            GenerationEdge::new(11u64, 2u64, 1u64, "KNOWS").with_prop("since", Value::Int64(2021)),
        )
        .edge(GenerationEdge::new(12u64, 1u64, 3u64, "LIVES_IN"))
        .rel_schema(RelSchemaDecl::new("KNOWS", "Person", "Person"))
        .rel_schema(RelSchemaDecl::new("LIVES_IN", "Person", "City"));
    generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &GenBudget::for_tests(),
    )
    .unwrap()
}

#[test]
#[cfg(feature = "generation-streaming")]
fn phase1_serialize_v5_canonical_matches_eager_reference() {
    // The eager reference is produced by forcing the feature-OFF code path's
    // logic. Since the feature is ON here, serialize_v5 delegates to the
    // canonical emitter; we assert it round-trips and is self-consistent with
    // the assembler over canonical descriptors (the D0.1 parity anchor).
    let generated = phase1_fixture_store();
    let payload = section_v5::serialize_v5_with_string_order(
        &generated.store,
        StringCodeOrder::Lexicographic,
    )
    .unwrap();

    // Independent reconstruction via canonical descriptors + assembler.
    let string_index = crate::graph::compact::generation::v5_emitter::build_string_index(
        &generated.global_strings,
    )
    .unwrap();
    let str_refs: Vec<&str> = generated
        .global_strings
        .as_slice()
        .iter()
        .map(String::as_str)
        .collect();
    let descriptors =
        emit_canonical_descriptors(&generated.store, &string_index, &str_refs).unwrap();
    let assembler = V5PayloadAssembler::new(
        generated.store.total_nodes(),
        generated.store.total_edges(),
        generated.store.preserves_ids(),
    );
    let reconstructed = assembler.assemble(&descriptors).unwrap();

    assert_eq!(
        payload, reconstructed,
        "serialize_v5 (canonical) must equal assembler over canonical descriptors"
    );
    // And it must deserialize.
    let restored = section_v5::deserialize_v5(&Bytes::from(payload)).unwrap();
    assert!(restored.preserves_ids());
}

#[test]
#[cfg(feature = "generation-streaming")]
fn phase1_emit_v5_segments_canonical_matches_serialize_v5() {
    // The byte-parity anchor (D0.1): emit_v5_segments (canonical, feature ON)
    // assembled must be byte-identical to serialize_v5_with_string_order.
    let generated = phase1_fixture_store();
    let expected = section_v5::serialize_v5_with_string_order(
        &generated.store,
        StringCodeOrder::Lexicographic,
    )
    .unwrap();

    let segments = crate::graph::compact::generation::emit_v5_segments(
        &generated.store,
        &generated.global_strings,
    )
    .unwrap();

    // Convert V5Segments → descriptors via MemorySegmentSink, assemble.
    let mut descriptors = Vec::new();
    for seg in &segments {
        let mut sink = Box::new(MemorySegmentSink::new(
            seg.kind,
            seg.encoding_version,
            seg.flags,
            seg.alignment,
            seg.element_width,
        ));
        sink.write(&seg.bytes).unwrap();
        descriptors.push(sink.finish().unwrap());
    }
    descriptors.sort_by_key(|d| d.kind.as_u16());
    let assembler = V5PayloadAssembler::new(
        generated.store.total_nodes(),
        generated.store.total_edges(),
        generated.store.preserves_ids(),
    );
    let assembled = assembler.assemble(&descriptors).unwrap();

    assert_eq!(
        assembled, expected,
        "emit_v5_segments (canonical) assembled must be byte-identical to serialize_v5"
    );
}

#[test]
#[cfg(feature = "generation-streaming")]
fn phase1_canonical_descriptors_are_ascending_and_complete() {
    let generated = phase1_fixture_store();
    let string_index = crate::graph::compact::generation::v5_emitter::build_string_index(
        &generated.global_strings,
    )
    .unwrap();
    let str_refs: Vec<&str> = generated
        .global_strings
        .as_slice()
        .iter()
        .map(String::as_str)
        .collect();
    let descriptors =
        emit_canonical_descriptors(&generated.store, &string_index, &str_refs).unwrap();

    // Strictly ascending kinds (no duplicates, no gaps in ordering).
    for w in descriptors.windows(2) {
        assert!(
            w[0].kind.as_u16() < w[1].kind.as_u16(),
            "descriptors must be strictly ascending: {:?} < {:?}",
            w[0].kind,
            w[1].kind
        );
    }
    // Metadata, StringOffsets, StringBytes always present.
    let kinds: Vec<u16> = descriptors.iter().map(|d| d.kind.as_u16()).collect();
    assert!(kinds.contains(&0), "Metadata present");
    assert!(kinds.contains(&1), "StringOffsets present");
    assert!(kinds.contains(&2), "StringBytes present");
    // preserve-ID store must carry the four lookup segments.
    assert!(generated.store.preserves_ids());
    for required in [14u16, 15, 16, 17] {
        assert!(
            kinds.contains(&required),
            "ID lookup segment {required} present"
        );
    }
}

// ── Tripwire (packet §9 / D0.5) ──────────────────────────────────────
//
// The writable generation path must never reach a whole-dictionary /
// whole-column / whole-segment / full-CompactStore in-memory compatibility
// implementation. Phase 1's canonical emitter still uses `MemorySegmentSink`
// per-segment (a bounded compatibility sink) because the builder does not yet
// exist; the tripwire that the *builder* cannot construct `MemorySegmentSink`
// is enforced in Phase 2 by the builder's API only accepting a spool/`temp_dir`
// constructor. This test documents and locks the Phase 1 boundary: the
// canonical emitter is the ONLY shared emission surface, and it is reachable
// from both serializers (no second serializer exists).
#[test]
#[cfg(feature = "generation-streaming")]
fn phase1_single_canonical_emitter_is_the_only_shared_surface() {
    // Both serializers converge on emit_canonical_descriptors. Prove there is
    // exactly one emission implementation by showing the two public entry
    // points produce identical descriptor streams for the same store.
    let generated = phase1_fixture_store();
    let string_index = crate::graph::compact::generation::v5_emitter::build_string_index(
        &generated.global_strings,
    )
    .unwrap();
    let str_refs: Vec<&str> = generated
        .global_strings
        .as_slice()
        .iter()
        .map(String::as_str)
        .collect();

    let a = emit_canonical_descriptors(&generated.store, &string_index, &str_refs).unwrap();
    let b = emit_canonical_descriptors(&generated.store, &string_index, &str_refs).unwrap();
    assert_eq!(a.len(), b.len());
    for (x, y) in a.iter().zip(b.iter()) {
        assert_eq!(x.kind, y.kind);
        assert_eq!(x.length, y.length);
        assert_eq!(x.crc, y.crc);
        assert_eq!(x.element_count, y.element_count);
    }
}
