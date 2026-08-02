//! Streaming-vs-golden payload parity tests (G-EM0.5b Phase 2).
//!
//! The core acceptance criterion: the streaming bounded builder must produce
//! the **committed golden** v5 payload bytes for each adversarial input. The
//! golden bytes are the eager `generate_compact_store` +
//! `serialize_v5_with_string_order(Lexicographic)` output, frozen at commit
//! time. The parity tests never run the eager heap path — they compare the
//! streaming builder against frozen bytes, so the production build carries no
//! runtime eager oracle.
//!
//! To regenerate the goldens after an *intentional* format change:
//!   cargo test -p grafeo-core --features generation-streaming \
//!     generation_builder::tests::regenerate_v5_golden_fixtures -- --ignored
//! then commit the updated `fixtures/v5/*.bin` and refresh the README SHAs.

#![cfg(feature = "generation-streaming")]

use crate::graph::compact::generation::{
    GenerationBudget, GenerationEdge, GenerationInput, GenerationNode, InMemoryRunStore,
    generate_compact_store,
};
use crate::graph::compact::generation_builder::{StreamingBuildConfig, StreamingGenerationBuilder};
use crate::graph::compact::section_v5::{StringCodeOrder, serialize_v5_with_string_order};
use grafeo_common::types::Value;

/// Build the streaming payload for a given input (the path under test).
fn streaming_payload(input: &GenerationInput, temp_dir: &std::path::Path) -> Vec<u8> {
    let config = StreamingBuildConfig::for_tests(temp_dir);
    let run_store = Box::new(InMemoryRunStore::new());
    let mut builder = StreamingGenerationBuilder::new(config, run_store);
    let mut nodes = input.node_source();
    let mut edges = input.edge_source();
    let output = builder.build(&mut nodes, &mut edges).unwrap();
    output.payload
}

/// Load a committed golden v5 payload fixture.
fn golden_payload(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/graph/compact/fixtures/v5")
        .join(format!("{name}.v5.bin"));
    std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "missing golden fixture {} ({}); run regenerate_v5_golden_fixtures --ignored",
            path.display(),
            e
        )
    })
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("grafeo-5b-test-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Simple single-label, single-property graph.
fn simple_input() -> GenerationInput {
    GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person").with_prop("name", Value::String("Alice".into())))
        .node(GenerationNode::new(2u64, "Person").with_prop("name", Value::String("Bob".into())))
        .node(GenerationNode::new(3u64, "Person").with_prop("name", Value::String("Carol".into())))
        .edge(
            GenerationEdge::new(10u64, 1u64, 2u64, "KNOWS").with_prop("since", Value::Int64(2020)),
        )
        .edge(
            GenerationEdge::new(11u64, 2u64, 3u64, "KNOWS").with_prop("since", Value::Int64(2021)),
        )
}

/// Multi-label, multi-property, multi-edge-type graph.
fn complex_input() -> GenerationInput {
    GenerationInput::new()
        .node(
            GenerationNode::new(100u64, "Person")
                .with_prop("name", Value::String("Alice".into()))
                .with_prop("age", Value::Int64(30))
                .with_prop("active", Value::Bool(true)),
        )
        .node(
            GenerationNode::new(200u64, "Person")
                .with_prop("name", Value::String("Bob".into()))
                .with_prop("age", Value::Int64(25))
                .with_prop("active", Value::Bool(false)),
        )
        .node(
            GenerationNode::new(300u64, "Company")
                .with_prop("name", Value::String("Acme".into()))
                .with_prop("founded", Value::Int64(1990)),
        )
        .edge(
            GenerationEdge::new(1000u64, 100u64, 200u64, "KNOWS")
                .with_prop("since", Value::Int64(2020))
                .with_prop("weight", Value::Float64(0.8)),
        )
        .edge(
            GenerationEdge::new(1001u64, 100u64, 300u64, "WORKS_AT")
                .with_prop("role", Value::String("Engineer".into())),
        )
        .edge(
            GenerationEdge::new(1002u64, 200u64, 300u64, "WORKS_AT")
                .with_prop("role", Value::String("Designer".into())),
        )
}

/// Sparse IDs (≥ 2^40) to exercise the preserve-ID path.
fn sparse_id_input() -> GenerationInput {
    let base = 1u64 << 42;
    GenerationInput::new()
        .node(GenerationNode::new(base + 1, "Node").with_prop("val", Value::Int64(10)))
        .node(GenerationNode::new(base + 5, "Node").with_prop("val", Value::Int64(50)))
        .node(GenerationNode::new(base + 9, "Node").with_prop("val", Value::Int64(90)))
        .edge(GenerationEdge::new(base + 100, base + 1, base + 5, "LINK"))
        .edge(GenerationEdge::new(base + 101, base + 5, base + 9, "LINK"))
        .edge(GenerationEdge::new(base + 102, base + 9, base + 1, "LINK"))
}

/// Duplicate endpoints (same src/dst, distinct edge IDs).
fn duplicate_endpoint_input() -> GenerationInput {
    GenerationInput::new()
        .node(GenerationNode::new(1u64, "A"))
        .node(GenerationNode::new(2u64, "B"))
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "REL").with_prop("seq", Value::Int64(1)))
        .edge(GenerationEdge::new(11u64, 1u64, 2u64, "REL").with_prop("seq", Value::Int64(2)))
        .edge(GenerationEdge::new(12u64, 1u64, 2u64, "REL").with_prop("seq", Value::Int64(3)))
}

/// Self-loops.
fn self_loop_input() -> GenerationInput {
    GenerationInput::new()
        .node(GenerationNode::new(1u64, "Node").with_prop("x", Value::Int64(1)))
        .node(GenerationNode::new(2u64, "Node").with_prop("x", Value::Int64(2)))
        .edge(GenerationEdge::new(10u64, 1u64, 1u64, "SELF"))
        .edge(GenerationEdge::new(11u64, 2u64, 2u64, "SELF"))
        .edge(GenerationEdge::new(12u64, 1u64, 2u64, "CROSS"))
}

/// High-cardinality strings for dictionary stress.
fn high_cardinality_string_input() -> GenerationInput {
    let mut input = GenerationInput::new();
    for i in 0..50u64 {
        input = input.node(
            GenerationNode::new(i, "Item")
                .with_prop("name", Value::String(format!("item-{i:04}").into()))
                .with_prop("category", Value::String(format!("cat-{}", i % 7).into())),
        );
    }
    for i in 0..49u64 {
        input = input.edge(GenerationEdge::new(1000 + i, i, i + 1, "NEXT"));
    }
    input
}

/// Signed integers (RawI64 codec path).
fn signed_int_input() -> GenerationInput {
    GenerationInput::new()
        .node(GenerationNode::new(1u64, "Sensor").with_prop("reading", Value::Int64(-42)))
        .node(GenerationNode::new(2u64, "Sensor").with_prop("reading", Value::Int64(100)))
        .node(GenerationNode::new(3u64, "Sensor").with_prop("reading", Value::Int64(-1)))
        .edge(
            GenerationEdge::new(10u64, 1u64, 2u64, "FEEDS").with_prop("delta", Value::Int64(-142)),
        )
}

/// Vector column path.
fn vector_input() -> GenerationInput {
    GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "Embedding")
                .with_prop("vec", Value::Vector(vec![1.0, 2.0, 3.0].into())),
        )
        .node(
            GenerationNode::new(2u64, "Embedding")
                .with_prop("vec", Value::Vector(vec![4.0, 5.0, 6.0].into())),
        )
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "SIMILAR"))
}

macro_rules! parity_test {
    ($name:ident, $golden:literal, $input_fn:ident) => {
        #[test]
        fn $name() {
            let input = $input_fn();
            let golden = golden_payload($golden);
            let dir = temp_dir(stringify!($name));
            let streaming = streaming_payload(&input, &dir);
            cleanup(&dir);
            assert_eq!(
                golden, streaming,
                "streaming payload must be byte-identical to committed golden fixture '{}'",
                $golden
            );
        }
    };
}

parity_test!(parity_simple, "simple", simple_input);
parity_test!(parity_complex, "complex", complex_input);
parity_test!(parity_sparse_ids, "sparse_ids", sparse_id_input);
parity_test!(
    parity_duplicate_endpoints,
    "duplicate_endpoints",
    duplicate_endpoint_input
);
parity_test!(parity_self_loops, "self_loops", self_loop_input);
parity_test!(
    parity_high_cardinality_strings,
    "high_cardinality_strings",
    high_cardinality_string_input
);
parity_test!(parity_signed_ints, "signed_ints", signed_int_input);
parity_test!(parity_vectors, "vectors", vector_input);

/// Regenerator: writes the golden fixtures from the eager oracle. `#[ignore]`d
/// so it never runs in CI; invoke explicitly after an intentional format change.
#[test]
#[ignore = "regenerates golden fixtures; run only on intentional format change"]
fn regenerate_v5_golden_fixtures() {
    fn eager_payload(input: &GenerationInput) -> Vec<u8> {
        let budget = GenerationBudget::for_tests();
        let mut nodes = input.node_source();
        let mut edges = input.edge_source();
        let generated =
            generate_compact_store(&mut nodes, &mut edges, &input.rel_schemas, &budget).unwrap();
        serialize_v5_with_string_order(&generated.store, StringCodeOrder::Lexicographic).unwrap()
    }

    let out_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/graph/compact/fixtures/v5");
    std::fs::create_dir_all(&out_dir).unwrap();

    let cases: &[(&str, fn() -> GenerationInput)] = &[
        ("simple", simple_input),
        ("complex", complex_input),
        ("sparse_ids", sparse_id_input),
        ("duplicate_endpoints", duplicate_endpoint_input),
        ("self_loops", self_loop_input),
        ("high_cardinality_strings", high_cardinality_string_input),
        ("signed_ints", signed_int_input),
        ("vectors", vector_input),
    ];
    for (name, input_fn) in cases {
        let payload = eager_payload(&input_fn());
        std::fs::write(out_dir.join(format!("{name}.v5.bin")), &payload).unwrap();
        eprintln!("wrote {name}.v5.bin ({} bytes)", payload.len());
    }
    panic!("fixtures written; record SHA-256s with `sha256sum` in fixtures/v5/README.md");
}

/// Determinism: two runs of the streaming builder produce identical bytes.
#[test]
fn streaming_determinism() {
    let input = complex_input();
    let dir1 = temp_dir("determinism-1");
    let dir2 = temp_dir("determinism-2");
    let p1 = streaming_payload(&input, &dir1);
    let p2 = streaming_payload(&input, &dir2);
    cleanup(&dir1);
    cleanup(&dir2);
    assert_eq!(p1, p2, "two streaming builds must be byte-identical");
}

/// The streaming payload deserializes through the production reader.
#[test]
fn streaming_payload_deserializes() {
    use crate::graph::compact::section::CompactStoreSection;
    use bytes::Bytes;

    let input = complex_input();
    let dir = temp_dir("deserialize");
    let payload = streaming_payload(&input, &dir);
    cleanup(&dir);

    let mut sec = CompactStoreSection::empty();
    sec.deserialize_from_bytes(Bytes::from(payload)).unwrap();
    let store = sec.store().expect("store present");
    assert_eq!(store.total_nodes(), 3);
    assert_eq!(store.total_edges(), 3);
    assert!(store.preserves_ids());
}
