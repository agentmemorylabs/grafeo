//! CompactStore v6 payload tests (G4: 64-bit column-body geometry).
//!
//! The bounded builder writes v5 under `PayloadVersionPolicy::Auto` whenever
//! every field fits (the existing golden byte-parity tests keep proving that
//! output is unchanged), and v6 under `PayloadVersionPolicy::V6` or when a
//! column needs it. These tests run the v6 writer and reader on small data;
//! the past-4-GiB case is the `#[ignore]`d engine test
//! `compact_store_v6_payload::v6_vector_column_over_4gib_round_trips`.

#![cfg(feature = "generation-streaming")]

use std::collections::BTreeMap;

use bytes::Bytes;
use grafeo_common::types::{EdgeId, NodeId, Value};

use crate::graph::compact::CompactStore;
use crate::graph::compact::generation::emit::{SegmentDescriptor, V5PayloadAssembler};
use crate::graph::compact::generation::{
    GenerationBudget, GenerationEdge, GenerationInput, GenerationNode, InMemoryRunStore,
};
use crate::graph::compact::generation_builder::dict_pass::make_resident_desc;
use crate::graph::compact::generation_builder::orchestrator::{
    BoundedBuildConfig, BoundedGenerationBuilder,
};
use crate::graph::compact::generation_builder::tests as golden_inputs;
use crate::graph::compact::mapped::{
    BLOCK_INDEX_RECORD_LEN_V5, BLOCK_INDEX_RECORD_LEN_V6, DISC_F32_VECTOR_WIDE, PayloadVersion,
    PayloadVersionPolicy, SegmentKind, parse_segment_directory, read_block_index_record,
};
use crate::graph::compact::section::CompactStoreSection;
use crate::graph::compact::section_v5::deserialize_v5;
use crate::graph::traits::GraphStore;
use tempfile::TempDir;

fn build(input: &GenerationInput, policy: PayloadVersionPolicy) -> Vec<u8> {
    let tmp = TempDir::new().unwrap();
    let config = BoundedBuildConfig {
        budget: GenerationBudget::for_tests(),
        temp_dir: tmp.path().to_path_buf(),
        correlation_id: "v6".into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
    };
    let mut store = InMemoryRunStore::new();
    let mut builder = BoundedGenerationBuilder::new(config).with_payload_version_policy(policy);
    let mut lease = builder
        .build(
            &mut input.node_source(),
            &mut input.edge_source(),
            &mut store,
        )
        .expect("bounded build");
    let expected = match policy {
        PayloadVersionPolicy::Auto => PayloadVersion::V5,
        PayloadVersionPolicy::V6 => PayloadVersion::V6,
    };
    assert_eq!(lease.payload_version(), expected);
    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");
    assert_eq!(payload.len() as u64, lease.exact_len().unwrap());
    payload
}

/// Every value type the streaming writer has a body family for, sparse ids,
/// absent and null properties (presence/null companions), multi-label nodes
/// (label membership companion), and edge properties.
fn mixed_input() -> GenerationInput {
    let big = 1u64 << 42;
    let mut input = GenerationInput::new();
    for i in 0..40u64 {
        let mut node = GenerationNode::new(big + i * 3, "Doc")
            .with_prop("title", Value::String(format!("doc-{i:03}").into()))
            .with_prop("rank", Value::Int64(i as i64 * 7))
            .with_prop("delta", Value::Int64(20 - i as i64))
            .with_prop("score", Value::Float64(i as f64 / 3.0))
            .with_prop("live", Value::Bool(i % 2 == 0))
            .with_prop(
                "embedding",
                Value::Vector(vec![i as f32, -(i as f32), 0.5, 1e-3 * i as f32].into()),
            );
        if i % 5 == 0 {
            node = node.with_prop("note", Value::String("sparse".into()));
        }
        if i % 7 == 0 {
            node = node.with_prop("maybe", Value::Null);
        } else if i % 3 == 0 {
            node = node.with_prop("maybe", Value::Int64(1));
        }
        input = input.node(node);
    }
    input = input.node(
        GenerationNode::with_labels(7u64, ["Doc".to_string(), "Pinned".to_string()])
            .unwrap()
            .with_prop("title", Value::String("pinned".into())),
    );
    for i in 0..39u64 {
        input = input.edge(
            GenerationEdge::new(big + 1000 + i, big + i * 3, big + (i + 1) * 3, "NEXT")
                .with_prop("w", Value::Float64(i as f64)),
        );
    }
    input.edge(GenerationEdge::new(5u64, 7u64, big, "PINS"))
}

/// Canonical text form of every input node and edge as read back from a store.
fn snapshot(store: &CompactStore, input: &GenerationInput) -> Vec<String> {
    let mut out = Vec::new();
    for n in &input.nodes {
        let id = NodeId::new(n.id.as_u64());
        let node = store
            .get_node(id)
            .unwrap_or_else(|| panic!("node {id:?} missing"));
        let mut labels: Vec<String> = node.labels.iter().map(ToString::to_string).collect();
        labels.sort();
        out.push(format!(
            "{id:?} {labels:?} {:?}",
            node.properties_as_btree()
        ));
    }
    for e in &input.edges {
        let id = EdgeId::new(e.id.as_u64());
        let edge = store
            .get_edge(id)
            .unwrap_or_else(|| panic!("edge {id:?} missing"));
        out.push(format!(
            "{id:?} {:?}->{:?} {} {:?}",
            edge.src,
            edge.dst,
            edge.edge_type,
            edge.properties_as_btree()
        ));
    }
    out
}

/// The same canonical form computed from the input itself (the oracle).
fn expected(input: &GenerationInput) -> Vec<String> {
    let mut out = Vec::new();
    for n in &input.nodes {
        let props: BTreeMap<_, _> = n.properties.clone().into_iter().collect();
        out.push(format!(
            "{:?} {:?} {props:?}",
            NodeId::new(n.id.as_u64()),
            n.labels
        ));
    }
    for e in &input.edges {
        let props: BTreeMap<_, _> = e.properties.clone().into_iter().collect();
        out.push(format!(
            "{:?} {:?}->{:?} {} {props:?}",
            EdgeId::new(e.id.as_u64()),
            NodeId::new(e.src.as_u64()),
            NodeId::new(e.dst.as_u64()),
            e.edge_type
        ));
    }
    out
}

fn block_index_width(payload: &[u8]) -> u32 {
    let bytes = Bytes::copy_from_slice(payload);
    let dir = parse_segment_directory(&bytes, payload.len() - 4).unwrap();
    dir.require(SegmentKind::ColumnBlockIndex)
        .unwrap()
        .element_width
}

#[test]
fn auto_writes_v5_and_v6_policy_writes_v6() {
    let input = mixed_input();
    let v5 = build(&input, PayloadVersionPolicy::Auto);
    let v6 = build(&input, PayloadVersionPolicy::V6);
    assert_eq!(v5[4], 5);
    assert_eq!(v6[4], 6);
    assert_eq!(block_index_width(&v5) as usize, BLOCK_INDEX_RECORD_LEN_V5);
    assert_eq!(block_index_width(&v6) as usize, BLOCK_INDEX_RECORD_LEN_V6);
    let bytes = Bytes::from(v6);
    let dir = parse_segment_directory(&bytes, bytes.len() - 4).unwrap();
    assert_eq!(dir.header.version, PayloadVersion::V6);
}

#[test]
fn v6_round_trips_every_value_like_v5() {
    let input = mixed_input();
    let want = expected(&input);
    let v5 = deserialize_v5(&Bytes::from(build(&input, PayloadVersionPolicy::Auto))).unwrap();
    let v6 = deserialize_v5(&Bytes::from(build(&input, PayloadVersionPolicy::V6))).unwrap();
    assert_eq!(
        snapshot(&v5, &input),
        want,
        "v5 build must read back the input"
    );
    assert_eq!(
        snapshot(&v6, &input),
        want,
        "v6 build must read back the input"
    );
    assert_eq!(v6.total_nodes(), v5.total_nodes());
    assert_eq!(v6.total_edges(), v5.total_edges());
}

#[test]
fn v6_round_trips_through_the_section_reader() {
    let input = mixed_input();
    let mut section = CompactStoreSection::empty();
    section
        .deserialize_from_bytes(Bytes::from(build(&input, PayloadVersionPolicy::V6)))
        .expect("section reader accepts v6");
    let store = section.store().expect("store");
    assert_eq!(snapshot(&store, &input), expected(&input));
}

#[test]
fn v6_vector_columns_use_the_wide_header() {
    let input = mixed_input();
    let payload = Bytes::from(build(&input, PayloadVersionPolicy::V6));
    let dir = parse_segment_directory(&payload, payload.len() - 4).unwrap();
    let block_index = &payload
        [usize::try_from(dir.require(SegmentKind::ColumnBlockIndex).unwrap().offset).unwrap()..];
    let bodies_entry = dir.require(SegmentKind::ColumnBodies).unwrap();
    let bodies = &payload[usize::try_from(bodies_entry.offset).unwrap()..];
    let columns = dir
        .require(SegmentKind::ColumnBlockIndex)
        .unwrap()
        .element_count;
    let wide = (0..usize::try_from(columns).unwrap())
        .map(|i| read_block_index_record(block_index, PayloadVersion::V6, i).unwrap())
        .filter(|r| bodies[usize::try_from(r.body_offset).unwrap()] == DISC_F32_VECTOR_WIDE)
        .count();
    assert_eq!(wide, 1, "the one vector column carries the wide v6 header");
}

#[test]
fn v6_is_deterministic() {
    let input = mixed_input();
    assert_eq!(
        build(&input, PayloadVersionPolicy::V6),
        build(&input, PayloadVersionPolicy::V6)
    );
}

/// The committed v5 fixtures keep their exact bytes (pinned by length and
/// CRC-32 here; SHA-256s are in `fixtures/v5/README.md`) and decode through
/// the v5/v6 reader to the same graph a v6 build of the same input holds.
#[test]
fn v5_fixtures_still_decode_to_the_v6_graph() {
    let cases: &[(&str, fn() -> GenerationInput, usize)] = &[
        ("simple", golden_inputs::simple_input, 1772),
        ("complex", golden_inputs::complex_input, 2844),
        ("sparse_ids", golden_inputs::sparse_id_input, 1636),
        (
            "duplicate_endpoints",
            golden_inputs::duplicate_endpoint_input,
            1468,
        ),
        ("self_loops", golden_inputs::self_loop_input, 1692),
        (
            "high_cardinality_strings",
            golden_inputs::high_cardinality_string_input,
            7940,
        ),
        ("signed_ints", golden_inputs::signed_int_input, 1644),
        ("vectors", golden_inputs::vector_input, 1436),
    ];
    for (name, input_fn, len) in cases {
        let fixture = golden_inputs::golden_payload(name);
        assert_eq!(fixture.len(), *len, "{name}.v5.bin changed");
        assert_eq!(fixture[4], 5, "{name}.v5.bin is not v5");
        let input = input_fn();
        let from_v5 = deserialize_v5(&Bytes::from(fixture)).unwrap();
        let from_v6 =
            deserialize_v5(&Bytes::from(build(&input, PayloadVersionPolicy::V6))).unwrap();
        assert_eq!(
            snapshot(&from_v5, &input),
            snapshot(&from_v6, &input),
            "{name}: v5 fixture and v6 build disagree"
        );
        assert_eq!(snapshot(&from_v6, &input), expected(&input), "{name}");
    }
}

/// A reader refuses a payload version it does not know, by version number,
/// before touching the directory. Binaries that only know v5 take the same
/// branch for a v6 payload ("unsupported CompactStore section version 6").
#[test]
fn unknown_future_version_is_refused_by_number() {
    let mut payload = build(&mixed_input(), PayloadVersionPolicy::V6);
    payload[4] = 7;
    let body_len = payload.len() - 4;
    let crc = crc32fast::hash(&payload[..body_len]);
    payload[body_len..].copy_from_slice(&crc.to_le_bytes());
    let mut section = CompactStoreSection::empty();
    let err = section
        .deserialize_from_bytes(Bytes::from(payload))
        .expect_err("version 7 must be refused")
        .to_string();
    assert!(
        err.contains("unsupported CompactStore section version 7"),
        "{err}"
    );
}

/// A v6 directory relabelled as v5 is refused, not misread: the v5 parser
/// reads the v6 entry's CRC as `element_count` and its `element_count` as
/// the reserved fields.
#[test]
fn v6_directory_relabelled_v5_is_refused() {
    let mut payload = build(&mixed_input(), PayloadVersionPolicy::V6);
    payload[4] = 5;
    let body_len = payload.len() - 4;
    let crc = crc32fast::hash(&payload[..body_len]);
    payload[body_len..].copy_from_slice(&crc.to_le_bytes());
    assert!(deserialize_v5(&Bytes::from(payload)).is_err());
}

#[test]
fn assembler_refuses_a_block_index_in_the_wrong_layout() {
    let narrow: SegmentDescriptor = make_resident_desc(
        SegmentKind::ColumnBlockIndex,
        4,
        BLOCK_INDEX_RECORD_LEN_V5 as u32,
        &[0u8; 12],
    );
    let assembler = V5PayloadAssembler::new(0, 0, false).with_payload_version(PayloadVersion::V6);
    let err = assembler.assemble(&[narrow]).expect_err("width mismatch");
    assert!(
        err.to_string().contains("ColumnBlockIndex record width 12"),
        "{err}"
    );
}
