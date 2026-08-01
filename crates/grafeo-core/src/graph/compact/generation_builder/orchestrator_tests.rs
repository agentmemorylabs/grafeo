//! Orchestrator end-to-end test (G-EM0.5b Phase 2b).

#![cfg(feature = "generation-streaming")]

use crate::graph::compact::generation::{
    GenerationBudget, GenerationInput, GenerationNode, GenerationEdge, InMemoryRunStore,
};
use crate::graph::compact::generation_builder::orchestrator::{
    BoundedBuildConfig, BoundedGenerationBuilder,
};
use crate::graph::compact::section_v5::deserialize_v5;
use crate::graph::traits::GraphStore;
use grafeo_common::types::{NodeId, Value};
use tempfile::TempDir;

fn budget() -> GenerationBudget {
    GenerationBudget::for_tests()
}

fn config(temp: &std::path::Path) -> BoundedBuildConfig {
    BoundedBuildConfig {
        budget: budget(),
        temp_dir: temp.to_path_buf(),
        correlation_id: "test".into(),
        spool_buf_cap: 64 * 1024,
    }
}

#[test]
fn orchestrator_produces_deserializable_payload() {
    let tmp = TempDir::new().unwrap();
    let input = GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "Person")
                .with_prop("name", "Ada")
                .with_prop("age", Value::Int64(30)),
        )
        .node(
            GenerationNode::new(2u64, "Person")
                .with_prop("name", "Bob")
                .with_prop("age", Value::Int64(25)),
        )
        .node(
            GenerationNode::new(100u64, "Project")
                .with_prop("title", "Grafeo"),
        )
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "KNOWS"))
        .edge(GenerationEdge::new(11u64, 1u64, 100u64, "WORKS_ON"));

    let mut store = InMemoryRunStore::new();
    let mut builder = BoundedGenerationBuilder::new(config(tmp.path()));
    let mut lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect("build");

    // Stream the payload.
    let mut payload = Vec::new();
    lease
        .stream_to(&mut payload)
        .expect("stream_to");

    // Deserialize.
    let bytes = bytes::Bytes::from(payload);
    let compact = deserialize_v5(&bytes).expect("deserialize_v5");

    // Verify round-trip.
    assert_eq!(compact.total_nodes(), 3);
    assert_eq!(compact.total_edges(), 2);
    assert!(compact.get_node(NodeId::new(1)).is_some());
    assert!(compact.get_node(NodeId::new(2)).is_some());
    assert!(compact.get_node(NodeId::new(100)).is_some());
    assert!(compact.get_node(NodeId::new(999)).is_none());
}

#[test]
fn orchestrator_sparse_columns() {
    let tmp = TempDir::new().unwrap();
    let input = GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "Person")
                .with_prop("name", "Ada")
                .with_prop("age", Value::Int64(30)),
        )
        .node(GenerationNode::new(2u64, "Person").with_prop("name", "Bob")); // no age

    let mut store = InMemoryRunStore::new();
    let mut builder = BoundedGenerationBuilder::new(config(tmp.path()));
    let mut lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect("build");

    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");
    let bytes = bytes::Bytes::from(payload);
    let compact = deserialize_v5(&bytes).expect("deserialize_v5");

    assert_eq!(compact.total_nodes(), 2);
    let n1 = compact.get_node(NodeId::new(1)).unwrap();
    let n2 = compact.get_node(NodeId::new(2)).unwrap();
    assert_eq!(n1.get_property("age"), Some(&Value::Int64(30)));
    assert_eq!(n2.get_property("age"), None); // sparse
}

/// The eager lexicographic v5 reference payload for an input (D0.8.10 oracle).
fn eager_lexicographic_payload(input: &GenerationInput) -> Vec<u8> {
    use crate::graph::compact::generation::generate_compact_store;
    use crate::graph::compact::section_v5::{StringCodeOrder, serialize_v5_with_string_order};
    let budget = GenerationBudget::for_tests();
    let mut nodes = input.node_source();
    let mut edges = input.edge_source();
    let generated =
        generate_compact_store(&mut nodes, &mut edges, &input.rel_schemas, &budget).unwrap();
    serialize_v5_with_string_order(&generated.store, StringCodeOrder::Lexicographic).unwrap()
}

/// The bounded orchestrator payload for an input.
fn bounded_payload(input: &GenerationInput, temp: &std::path::Path) -> Vec<u8> {
    let mut store = InMemoryRunStore::new();
    let mut builder = BoundedGenerationBuilder::new(config(temp));
    let mut lease = builder
        .build(&mut input.node_source(), &mut input.edge_source(), &mut store)
        .expect("bounded build");
    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");
    payload
}

/// D0.8.10 core acceptance: the bounded orchestrator emits a v5 payload
/// **byte-identical** to the eager lexicographic reference for the dense
/// single-label domain (the reference's supported semantic domain).
///
/// The golden fixtures are frozen at SHA ca0d6069 (D0.8.10 #2: no
/// regenerating the oracle at test time). The test loads the committed
/// binary fixtures and compares against the bounded output.
#[test]
fn bounded_byte_parity_with_eager_lexicographic() {
    let tmp = TempDir::new().unwrap();
    let input = gem0_5b_parity_input();

    let bounded = bounded_payload(&input, tmp.path());
    let golden = golden_fixture("parity_dense");

    assert_eq!(
        golden.len(),
        bounded.len(),
        "payload length mismatch: golden {} vs bounded {}",
        golden.len(),
        bounded.len()
    );
    assert_eq!(
        golden, bounded,
        "bounded payload not byte-identical to frozen golden fixture (SHA ca0d6069)"
    );
}

/// The shared parity test input (3 Person nodes, 2 KNOWS edges, all with
/// properties). Matches the golden fixture generated at SHA ca0d6069.
#[allow(clippy::needless_pass_by_value)]
fn gem0_5b_parity_input() -> GenerationInput {
    GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "Person")
                .with_prop("name", "Alice")
                .with_prop("age", Value::Int64(30)),
        )
        .node(
            GenerationNode::new(2u64, "Person")
                .with_prop("name", "Bob")
                .with_prop("age", Value::Int64(25)),
        )
        .node(
            GenerationNode::new(3u64, "Person")
                .with_prop("name", "Carol")
                .with_prop("age", Value::Int64(40)),
        )
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "KNOWS"))
        .edge(GenerationEdge::new(11u64, 2u64, 3u64, "KNOWS"))
}

/// Loads a committed golden fixture from `tests/golden/gem0_5b/`.
fn golden_fixture(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/gem0_5b")
        .join(format!("{name}.v5"));
    std::fs::read(&path)
        .unwrap_or_else(|e| panic!("read golden fixture {}: {e}", path.display()))
}

/// One-time fixture generator: writes the golden payloads to
/// `tests/golden/gem0_5b/`. Run with `cargo test write_golden_fixtures
/// -- --ignored --nocapture` then commit the output. This test is
/// `#[ignore]` so it does NOT run in normal CI (D0.8.10 #2 forbids
/// regenerating the oracle at test time).
#[test]
#[ignore = "fixture generator: run manually, commit output (D0.8.10 #2)"]
fn write_golden_fixtures() {
    let input = gem0_5b_parity_input();
    let eager = eager_lexicographic_payload(&input);
    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/gem0_5b");
    std::fs::create_dir_all(&out_dir).expect("create golden dir");
    let path = out_dir.join("parity_dense.v5");
    std::fs::write(&path, &eager).expect("write golden fixture");
    eprintln!("wrote {} bytes to {}", eager.len(), path.display());
}

/// Diagnostic: dump the segment directory of a v5 payload (kind, length).
fn dump_plan(payload: &[u8]) -> Vec<(u16, u64)> {
    let seg_count = u16::from_le_bytes([payload[8], payload[9]]) as usize;
    let mut out = Vec::new();
    for i in 0..seg_count {
        let base = 64 + i * 48;
        let kind = u16::from_le_bytes([payload[base], payload[base + 1]]);
        let length = u64::from_le_bytes(payload[base + 16..base + 24].try_into().unwrap());
        out.push((kind, length));
    }
    out
}

#[test]
fn diag_diff_byte_by_byte() {
    let tmp = TempDir::new().unwrap();
    let input = GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "Person")
                .with_prop("name", "Alice")
                .with_prop("age", Value::Int64(30)),
        )
        .node(
            GenerationNode::new(2u64, "Person")
                .with_prop("name", "Bob")
                .with_prop("age", Value::Int64(25)),
        )
        .node(
            GenerationNode::new(3u64, "Person")
                .with_prop("name", "Carol")
                .with_prop("age", Value::Int64(40)),
        )
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "KNOWS"))
        .edge(GenerationEdge::new(11u64, 2u64, 3u64, "KNOWS"));

    let eager = eager_lexicographic_payload(&input);
    let bounded = bounded_payload(&input, tmp.path());

    // Parse segment directory from each.
    let seg_count = u16::from_le_bytes([eager[8], eager[9]]) as usize;
    let dir_start = 64;
    let entry_len = 48;
    let data_offset = u64::from_le_bytes(eager[32..40].try_into().unwrap()) as usize;

    eprintln!("seg_count={seg_count}, data_offset={data_offset}");
    let mut diffs = Vec::new();
    for i in 0..seg_count {
        let base = dir_start + i * entry_len;
        let kind = u16::from_le_bytes([eager[base], eager[base + 1]]);
        let e_off = u64::from_le_bytes(eager[base + 16..base + 24].try_into().unwrap());
        let e_len = u64::from_le_bytes(eager[base + 24..base + 32].try_into().unwrap());
        let b_off = u64::from_le_bytes(bounded[base + 16..base + 24].try_into().unwrap());
        let b_len = u64::from_le_bytes(bounded[base + 24..base + 32].try_into().unwrap());
        let e_crc = u32::from_le_bytes(eager[base + 32..base + 36].try_into().unwrap());
        let b_crc = u32::from_le_bytes(bounded[base + 32..base + 36].try_into().unwrap());
        let e_ecount = u32::from_le_bytes(eager[base + 36..base + 40].try_into().unwrap());
        let b_ecount = u32::from_le_bytes(bounded[base + 36..base + 40].try_into().unwrap());

        let mismatch = e_off != b_off || e_len != b_len || e_crc != b_crc || e_ecount != b_ecount;
        eprintln!(
            "  seg {i:2} kind={kind:3}: off {e_off}/{b_off} len {e_len}/{b_len} crc {e_crc:#010x}/{b_crc:#010x} ecount {e_ecount}/{b_ecount} {}",
            if mismatch { "*** DIFF ***" } else { "" }
        );
        if mismatch {
            diffs.push((i, kind, e_off, b_off, e_len, b_len, e_crc, b_crc, e_ecount, b_ecount));
        }
    }

    // Also find body byte diffs after the directory.
    let mut body_diffs = Vec::new();
    for i in data_offset..eager.len().min(bounded.len()) {
        if eager[i] != bounded[i] {
            body_diffs.push(i);
        }
    }

    eprintln!("\nDirectory diffs: {} entries", diffs.len());
    eprintln!("Body byte diffs: {} (first few: {:?})", body_diffs.len(), body_diffs.iter().take(20).copied().collect::<Vec<_>>());

    // For each directory diff, also dump the segment body region
    for &(i, kind, e_off, b_off, e_len, b_len, e_crc, b_crc, _e, _f) in &diffs {
        let eo = e_off as usize;
        let bo = b_off as usize;
        let el = e_len as usize;
        let bl = b_len as usize;
        eprintln!("\nSegment {i} kind={kind} body diff:");
        if el == bl {
            let mut seg_diffs = Vec::new();
            for j in 0..el {
                if eager[eo + j] != bounded[bo + j] {
                    seg_diffs.push(j);
                }
            }
            eprintln!("  body len {el}, {}/{} bytes differ: first 10 {:?}",
                seg_diffs.len(), el, seg_diffs.iter().take(10).copied().collect::<Vec<_>>());
        } else {
            eprintln!("  body len mismatch: eager={el} bounded={bl}");
        }
    }

    assert_eq!(eager, bounded, "not byte-identical");
}
