//! G-EM0.5b R3 — Outer publish→recover→mmap source-true matrix.
//!
//! Proves the full production path: DiskRunStore bounded build → W0
//! publication → recovery selection → fresh mmap reopen → public
//! `GraphStore` reads. Covers bullets A–E of the R3 contract.
#![cfg(all(
    feature = "generation-streaming",
    feature = "compact-store",
    feature = "lpg",
    feature = "mmap"
))]

use std::sync::Arc;

use bytes::Bytes;
use grafeo_common::storage::SectionType;
use grafeo_common::types::{NodeId, PropertyKey, Value};
use grafeo_core::graph::compact::generation::{
    GenerationBudget, GenerationEdge, GenerationInput, GenerationNode, InMemoryRunStore,
};
use grafeo_core::graph::compact::generation_builder::orchestrator::{
    BoundedBuildConfig, BoundedGenerationBuilder,
};
use grafeo_core::graph::compact::mapped::layout_flags;
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_core::graph::compact::CompactStore;
use grafeo_core::graph::traits::GraphStore;
use grafeo_core::graph::Direction;
use grafeo_storage::file::generation_writer::{
    GenerationContainerHeader, OsGenerationFileOps, StreamingPayloadSectionSource,
};
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::publication::{publish_generation, PublicationInput};
use grafeo_storage::generation::recovery::recover;
use grafeo_storage::generation::run_adapter::DiskRunStore;
use grafeo_storage::wal::WalManager;
use tempfile::TempDir;

// ── Helper: outer publish → recover → mmap reopen ──────────────────

/// Full production path: DiskRunStore bounded build → W0 publication →
/// recovery selection → fresh mmap reopen → `Arc<CompactStore>`.
fn outer_publish_and_mmap_reopen(input: &GenerationInput, gen_id: &str) -> Arc<CompactStore> {
    let tmp = TempDir::new().unwrap();
    // W0 layout: WAL must live under the generation root (same as n_vs_4n).
    let root = tmp.path().join("gen-root");
    let runs_dir = tmp.path().join("runs");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let budget = GenerationBudget::for_tests();
    let mut run_store = DiskRunStore::new(&runs_dir, budget, gen_id).expect("DiskRunStore");
    let config = BoundedBuildConfig {
        budget,
        temp_dir: tmp.path().join("build-tmp"),
        correlation_id: gen_id.into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
    };
    let mut builder = BoundedGenerationBuilder::new(config);
    let lease = builder
        .build(
            &mut input.node_source(),
            &mut input.edge_source(),
            &mut run_store,
        )
        .expect("bounded build");

    let node_count = lease.total_nodes();
    let edge_count = lease.total_edges();

    let wal = WalManager::open(&wal_dir).expect("wal");
    let lock = RootLock::try_acquire(&root).expect("root lock");
    let header = GenerationContainerHeader {
        epoch: 1,
        transaction_id: 1,
        node_count,
        edge_count,
    };
    let section_source = StreamingPayloadSectionSource::new(lease);
    let mut sections: Vec<Box<dyn grafeo_storage::file::generation_writer::ExactSectionSource>> =
        vec![Box::new(section_source)];
    let _published = publish_generation(
        &lock,
        PublicationInput {
            header,
            sections: &mut sections,
            generation_id: gen_id.to_string(),
            parent_generation_id: None,
            parent_publication_sequence: None,
        },
        &wal,
        &OsGenerationFileOps,
    )
    .expect("publish");
    drop(lock);

    let lock = RootLock::try_acquire(&root).expect("re-lock");
    let selected = recover(&lock).expect("recover");
    drop(lock);

    let manager =
        GrafeoFileManager::open_read_only(&selected.generation_abs_path).expect("open ro");
    let section_dir = manager.read_section_directory().unwrap().unwrap();
    let entry = section_dir
        .find(SectionType::CompactStore)
        .expect("CompactStore section");
    let mmap = Arc::new(manager.mmap_section(entry).expect("mmap section"));
    assert_eq!(mmap.section_type(), SectionType::CompactStore);
    let mapped_bytes = grafeo_storage::container::MmapSection::into_bytes(mmap);
    let mut cs = CompactStoreSection::empty();
    cs.deserialize_from_mapped_bytes(mapped_bytes)
        .expect("mapped reopen");
    let store = cs.store().expect("store");
    assert_eq!(store.total_nodes(), node_count);
    assert_eq!(store.total_edges(), edge_count);

    // Leak the TempDir so files survive for the store's mapped backing.
    std::mem::forget(tmp);
    store
}

/// In-memory bounded build → raw v5 payload bytes (for surgery tests).
fn bounded_payload(input: &GenerationInput) -> Vec<u8> {
    let tmp = TempDir::new().unwrap();
    let mut store = InMemoryRunStore::new();
    let config = BoundedBuildConfig {
        budget: GenerationBudget::for_tests(),
        temp_dir: tmp.path().to_path_buf(),
        correlation_id: "surgery".into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: Vec::new(),
        frozen_epoch: 0,
    };
    let mut builder = BoundedGenerationBuilder::new(config);
    let mut lease = builder
        .build(
            &mut input.node_source(),
            &mut input.edge_source(),
            &mut store,
        )
        .expect("bounded build");
    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");
    payload
}

/// Recompute the trailing outer CRC after payload surgery.
fn recompute_outer_crc(payload: &mut [u8]) {
    let tail = payload.len() - 4;
    let crc = crc32fast::hash(&payload[..tail]);
    payload[tail..].copy_from_slice(&crc.to_le_bytes());
}

/// Recompute directory CRC (header offset 52..56) after directory surgery.
#[allow(dead_code)]
fn recompute_directory_crc(payload: &mut [u8]) {
    let seg_count = u16::from_le_bytes([payload[8], payload[9]]) as usize;
    let dir_start = 64usize;
    let dir_len = seg_count * 48;
    let dir_crc = crc32fast::hash(&payload[dir_start..dir_start + dir_len]);
    payload[52..56].copy_from_slice(&dir_crc.to_le_bytes());
}

// ── A: Node AND relationship properties, cross-table identical keys ─

#[test]
fn outer_node_and_rel_properties_cross_table_identical_keys() {
    let input = GenerationInput::new()
        // Two node tables sharing the key "name"
        .node(
            GenerationNode::new(1u64, "Person")
                .with_prop("name", "Alice")
                .with_prop("score", Value::Int64(100)),
        )
        .node(
            GenerationNode::new(2u64, "Project")
                .with_prop("name", "Grafeo")
                .with_prop("score", Value::Int64(200)),
        )
        // Two rel tables sharing the key "weight"
        .edge(
            GenerationEdge::new(10u64, 1u64, 2u64, "OWNS").with_prop("weight", Value::Float64(1.5)),
        )
        .edge(
            GenerationEdge::new(11u64, 2u64, 1u64, "REFERENCES")
                .with_prop("weight", Value::Float64(2.5)),
        );

    let store = outer_publish_and_mmap_reopen(&input, "a-cross-keys");

    // Node point reads — identical key "name" resolves per-table.
    assert_eq!(
        store.get_node_property(NodeId::new(1), &PropertyKey::new("name")),
        Some(Value::from("Alice"))
    );
    assert_eq!(
        store.get_node_property(NodeId::new(2), &PropertyKey::new("name")),
        Some(Value::from("Grafeo"))
    );
    assert_eq!(
        store.get_node_property(NodeId::new(1), &PropertyKey::new("score")),
        Some(Value::Int64(100))
    );
    assert_eq!(
        store.get_node_property(NodeId::new(2), &PropertyKey::new("score")),
        Some(Value::Int64(200))
    );

    // Rel point reads — identical key "weight" resolves per-rel-table by
    // original edge id (preserve-IDs) and by direct table path.
    assert_eq!(
        store.get_edge_property(
            grafeo_common::types::EdgeId::new(10),
            &PropertyKey::new("weight")
        ),
        Some(Value::Float64(1.5)),
        "OWNS weight via original edge id 10"
    );
    assert_eq!(
        store.get_edge_property(
            grafeo_common::types::EdgeId::new(11),
            &PropertyKey::new("weight")
        ),
        Some(Value::Float64(2.5)),
        "REFERENCES weight via original edge id 11"
    );
    let owns = store.rel_table("OWNS").expect("OWNS");
    let refs = store.rel_table("REFERENCES").expect("REFERENCES");
    assert_eq!(
        owns.get_edge_property(0, &PropertyKey::new("weight")),
        Some(Value::Float64(1.5))
    );
    assert_eq!(
        refs.get_edge_property(0, &PropertyKey::new("weight")),
        Some(Value::Float64(2.5))
    );

    // Batch reads.
    let batch =
        store.get_node_property_batch(&[NodeId::new(1), NodeId::new(2)], &PropertyKey::new("name"));
    assert_eq!(batch.len(), 2);
    assert!(batch[0].is_some());
    assert!(batch[1].is_some());

    // get_all_properties via node_table.
    let person_table = store.node_table("Person").expect("Person table");
    let props = person_table.get_all_properties(0);
    assert_eq!(
        props.get(&PropertyKey::new("name")),
        Some(&Value::from("Alice"))
    );
    assert_eq!(
        props.get(&PropertyKey::new("score")),
        Some(&Value::Int64(100))
    );
}

// ── B: Absent vs present-null across types and read paths ──────────

#[test]
fn outer_absent_vs_present_null_all_types_and_read_paths() {
    let vec_val = Value::Vector(Arc::from([1.0f32, 2.0, 3.0]));
    let input = GenerationInput::new()
        // Row 1: all properties present with real values (float includes a
        // finite value so zone maps are non-empty; NaN is added on row 1b
        // style via a second finite+NaN pair if needed — NaN is also stored
        // here as the sole float to exercise NaN-only family classification).
        .node(
            GenerationNode::new(1u64, "T")
                .with_prop("int_key", Value::Int64(42))
                .with_prop("bool_key", Value::Bool(true))
                .with_prop("str_key", "hello")
                .with_prop("float_key", Value::Float64(f64::NAN))
                .with_prop("vec_key", vec_val.clone()),
        )
        // Row 2: all properties present-null.
        .node(
            GenerationNode::new(2u64, "T")
                .with_prop("int_key", Value::Null)
                .with_prop("bool_key", Value::Null)
                .with_prop("str_key", Value::Null)
                .with_prop("float_key", Value::Null)
                .with_prop("vec_key", Value::Null),
        )
        // Row 3: all properties absent.
        .node(GenerationNode::new(3u64, "T"));

    let store = outer_publish_and_mmap_reopen(&input, "b-null-absent");

    let keys = ["int_key", "bool_key", "str_key", "float_key", "vec_key"];

    // Point reads: row 1 present, row 2 present-null, row 3 absent.
    for key in &keys {
        let pk = PropertyKey::new(*key);
        let v1 = store.get_node_property(NodeId::new(1), &pk);
        let v2 = store.get_node_property(NodeId::new(2), &pk);
        let v3 = store.get_node_property(NodeId::new(3), &pk);
        assert!(v1.is_some(), "row 1 {key} must be present");
        assert_ne!(v1, Some(Value::Null), "row 1 {key} must not be null");
        assert_eq!(v2, Some(Value::Null), "row 2 {key} must be present-null");
        assert_eq!(v3, None, "row 3 {key} must be absent");
    }

    // NaN round-trips.
    let f1 = store
        .get_node_property(NodeId::new(1), &PropertyKey::new("float_key"))
        .expect("float present");
    assert!(
        f64::is_nan(f1.as_float64().expect("float64")),
        "NaN must survive"
    );

    // Vector round-trips.
    let v1 = store
        .get_node_property(NodeId::new(1), &PropertyKey::new("vec_key"))
        .expect("vec present");
    assert_eq!(v1.as_vector().expect("vector"), &[1.0f32, 2.0, 3.0]);

    // Batch reads preserve three-way distinction.
    let batch = store.get_node_property_batch(
        &[NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        &PropertyKey::new("int_key"),
    );
    assert_eq!(batch[0], Some(Value::Int64(42)));
    assert_eq!(batch[1], Some(Value::Null));
    assert_eq!(batch[2], None);

    // Eq scan: find_nodes_by_property for a present value.
    let hits = store.find_nodes_by_property("int_key", &Value::Int64(42));
    assert_eq!(hits, vec![NodeId::new(1)]);

    // Zone-map pruning: impossible value returns empty.
    let pruned = store.find_nodes_by_property("int_key", &Value::Int64(i64::MAX));
    assert!(pruned.is_empty(), "zone map must prune impossible value");

    // get_all_properties via the public GraphStore path (get_node applies
    // presence/null companions). NodeTable::get_all_properties returns raw
    // column bodies without companions and is not the product contract.
    let n2 = store.get_node(NodeId::new(2)).expect("node 2");
    for key in &keys {
        assert_eq!(
            n2.properties.get(&PropertyKey::new(*key)),
            Some(&Value::Null),
            "row 2 get_node properties {key} must be Null"
        );
    }
    let n3 = store.get_node(NodeId::new(3)).expect("node 3");
    for key in &keys {
        assert!(
            !n3.properties.contains_key(&PropertyKey::new(*key)),
            "row 3 get_node properties {key} must be absent"
        );
    }
}

// ── C: Multi-label membership through outer path ───────────────────

#[test]
fn outer_two_and_three_label_membership() {
    let input = GenerationInput::new()
        // Two-label node: canonical sort → ["Employee", "Person"], physical = "Employee".
        .node(
            GenerationNode::with_labels(1u64, ["Person", "Employee"])
                .unwrap()
                .with_prop("name", "Alice"),
        )
        // Three-label node: canonical sort → ["A", "B", "C"], physical = "A".
        .node(
            GenerationNode::with_labels(2u64, ["C", "A", "B"])
                .unwrap()
                .with_prop("name", "Bob"),
        )
        // Single-label node.
        .node(GenerationNode::new(3u64, "Person").with_prop("name", "Carol"));

    let store = outer_publish_and_mmap_reopen(&input, "c-labels");

    // get_node returns full logical label sets.
    let n1 = store.get_node(NodeId::new(1)).expect("node 1");
    let labels1: Vec<String> = n1.labels.iter().map(|l| l.to_string()).collect();
    assert!(
        labels1.contains(&"Person".to_string()),
        "node 1 must have Person"
    );
    assert!(
        labels1.contains(&"Employee".to_string()),
        "node 1 must have Employee"
    );
    assert_eq!(labels1.len(), 2);

    let n2 = store.get_node(NodeId::new(2)).expect("node 2");
    let labels2: Vec<String> = n2.labels.iter().map(|l| l.to_string()).collect();
    assert_eq!(labels2.len(), 3, "node 2 must have 3 labels");
    for l in ["A", "B", "C"] {
        assert!(labels2.contains(&l.to_string()), "node 2 must have {l}");
    }

    // nodes_by_label resolves every logical label (including lex-earlier).
    assert_eq!(
        store.nodes_by_label("Person").len(),
        2,
        "Person sees nodes 1,3"
    );
    assert_eq!(
        store.nodes_by_label("Employee").len(),
        1,
        "Employee sees node 1"
    );
    assert_eq!(store.nodes_by_label("A").len(), 1, "A sees node 2");
    assert_eq!(store.nodes_by_label("B").len(), 1, "B sees node 2");
    assert_eq!(store.nodes_by_label("C").len(), 1, "C sees node 2");

    // all_labels includes every logical label.
    let all = store.all_labels();
    for l in ["Person", "Employee", "A", "B", "C"] {
        assert!(all.iter().any(|x| x == l), "all_labels must include {l}");
    }

    // Physical table = labels[0] after sort.
    assert!(
        store.node_table("Employee").is_some(),
        "physical table Employee"
    );
    assert!(store.node_table("A").is_some(), "physical table A");
    assert!(
        store.node_table("Person").is_some(),
        "Person table for node 3"
    );
}

// ── D: Relationship endpoints, sparse IDs, self-loops, duplicates ──

#[test]
fn outer_rel_endpoints_sparse_self_loop_duplicate_both_csr() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(100u64, "N"))
        .node(GenerationNode::new(200u64, "N"))
        .node(GenerationNode::new(300u64, "N"))
        // Normal edge.
        .edge(GenerationEdge::new(10u64, 100u64, 200u64, "LINK"))
        // Self-loop.
        .edge(GenerationEdge::new(11u64, 100u64, 100u64, "LINK"))
        // Duplicate endpoint pair (same src→dst, different edge ID).
        .edge(GenerationEdge::new(12u64, 100u64, 200u64, "LINK"))
        // Reverse direction edge.
        .edge(GenerationEdge::new(13u64, 200u64, 100u64, "LINK"))
        // Edge involving node 300.
        .edge(GenerationEdge::new(14u64, 300u64, 100u64, "LINK"));

    let store = outer_publish_and_mmap_reopen(&input, "d-rel");

    assert_eq!(store.total_nodes(), 3);
    assert_eq!(store.total_edges(), 5);

    // Forward CSR: node 100 has 3 outgoing (→200, →100 self, →200 dup).
    let out_100 = store.neighbors(NodeId::new(100), Direction::Outgoing);
    assert_eq!(out_100.len(), 3, "node 100 out-degree");
    assert!(out_100.contains(&NodeId::new(200)));
    assert!(out_100.contains(&NodeId::new(100)), "self-loop target");

    // Reverse CSR: node 100 has 3 incoming (←200, ←100 self, ←300).
    let inc_100 = store.neighbors(NodeId::new(100), Direction::Incoming);
    assert_eq!(inc_100.len(), 3, "node 100 in-degree");
    assert!(inc_100.contains(&NodeId::new(200)));
    assert!(inc_100.contains(&NodeId::new(100)), "self-loop source");
    assert!(inc_100.contains(&NodeId::new(300)));

    // Node 200: 1 outgoing (→100), 2 incoming (←100 ×2).
    let out_200 = store.neighbors(NodeId::new(200), Direction::Outgoing);
    assert_eq!(out_200.len(), 1);
    let inc_200 = store.neighbors(NodeId::new(200), Direction::Incoming);
    assert_eq!(inc_200.len(), 2, "duplicate pair gives 2 incoming");

    // Both directions.
    let both_100 = store.neighbors(NodeId::new(100), Direction::Both);
    assert_eq!(both_100.len(), 6, "3 out + 3 in");

    // Edge point reads with sparse original IDs.
    let e10 = store
        .get_edge(grafeo_common::types::EdgeId::new(10))
        .expect("edge 10");
    assert_eq!(e10.src, NodeId::new(100));
    assert_eq!(e10.dst, NodeId::new(200));
    let e11 = store
        .get_edge(grafeo_common::types::EdgeId::new(11))
        .expect("edge 11 self-loop");
    assert_eq!(e11.src, NodeId::new(100));
    assert_eq!(e11.dst, NodeId::new(100));

    // Forward positions: edges_from returns (target, edge_id) pairs.
    let edges_from_100 = store.edges_from(NodeId::new(100), Direction::Outgoing);
    assert_eq!(edges_from_100.len(), 3);
    let edge_ids: Vec<u64> = edges_from_100.iter().map(|(_, eid)| eid.as_u64()).collect();
    assert!(
        edge_ids.contains(&10),
        "forward position must carry real edge id 10"
    );
    assert!(
        edge_ids.contains(&11),
        "forward position must carry real edge id 11"
    );
    assert!(
        edge_ids.contains(&12),
        "forward position must carry real edge id 12"
    );

    // Rel table direct access.
    let rt = store.rel_table("LINK").expect("LINK rel table");
    assert_eq!(rt.num_edges(), 5);
}

// ── E: Old-v5 defaults + fail-closed companion surgery ─────────────

#[test]
fn outer_old_v5_defaults_single_label_no_companions() {
    // Single-label, all-present, no-null payload: no companion segments.
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "X").with_prop("v", Value::Int64(1)))
        .node(GenerationNode::new(2u64, "X").with_prop("v", Value::Int64(2)));
    let store = outer_publish_and_mmap_reopen(&input, "e-old-v5");
    assert_eq!(store.total_nodes(), 2);
    let n1 = store.get_node(NodeId::new(1)).expect("n1");
    assert_eq!(n1.labels.len(), 1, "single physical label, no membership");
    assert_eq!(
        store.get_node_property(NodeId::new(1), &PropertyKey::new("v")),
        Some(Value::Int64(1))
    );
}

#[test]
fn surgery_missing_required_membership_fails_closed() {
    // Multi-label payload has membership segment; claim it required then strip flag.
    let input = GenerationInput::new().node(GenerationNode::with_labels(1u64, ["A", "B"]).unwrap());
    let payload = bounded_payload(&input);
    // Tamper: set REQUIRES_LABEL_MEMBERSHIP without the segment being absent
    // is already tested; here we strip the segment requirement to prove the
    // inverse: claim membership required on a single-label payload.
    let single = GenerationInput::new().node(GenerationNode::new(1u64, "Z"));
    let mut p2 = bounded_payload(&single);
    let flags = layout_flags::from_companion_segments(true, false, false);
    p2[12..16].copy_from_slice(&flags.to_le_bytes());
    recompute_outer_crc(&mut p2);
    let mut sec = CompactStoreSection::empty();
    let err = sec
        .deserialize_from_bytes(Bytes::from(p2))
        .expect_err("must fail closed on missing membership");
    assert!(
        err.to_string().contains("NodeLabelMembership"),
        "error: {err}"
    );
    let _ = payload; // suppress unused
}

#[test]
fn surgery_extended_marker_without_companion_bits_fails_closed() {
    let input = GenerationInput::new().node(GenerationNode::new(1u64, "Z"));
    let mut payload = bounded_payload(&input);
    // SOURCE_TRUE_EXTENDED alone (no companion requirement bits).
    payload[12..16].copy_from_slice(&layout_flags::SOURCE_TRUE_EXTENDED.to_le_bytes());
    recompute_outer_crc(&mut payload);
    let mut sec = CompactStoreSection::empty();
    let err = sec
        .deserialize_from_bytes(Bytes::from(payload))
        .expect_err("must fail closed");
    assert!(
        err.to_string().contains("SOURCE_TRUE_EXTENDED"),
        "error: {err}"
    );
}

#[test]
fn surgery_unknown_layout_flags_bits_fail_closed() {
    let input = GenerationInput::new().node(GenerationNode::new(1u64, "Z"));
    let mut payload = bounded_payload(&input);
    let bad_flags = layout_flags::KNOWN_MASK | 0x8000_0000;
    payload[12..16].copy_from_slice(&bad_flags.to_le_bytes());
    recompute_outer_crc(&mut payload);
    let mut sec = CompactStoreSection::empty();
    let err = sec
        .deserialize_from_bytes(Bytes::from(payload))
        .expect_err("must fail closed on unknown bits");
    assert!(err.to_string().contains("unknown bits"), "error: {err}");
}

#[test]
fn surgery_missing_required_presence_fails_closed() {
    let input = GenerationInput::new().node(GenerationNode::new(1u64, "Z"));
    let mut payload = bounded_payload(&input);
    let flags = layout_flags::from_companion_segments(false, true, false);
    payload[12..16].copy_from_slice(&flags.to_le_bytes());
    recompute_outer_crc(&mut payload);
    let mut sec = CompactStoreSection::empty();
    let err = sec
        .deserialize_from_bytes(Bytes::from(payload))
        .expect_err("must fail closed on missing presence");
    assert!(
        err.to_string().contains("ColumnRowPresence"),
        "error: {err}"
    );
}

#[test]
fn surgery_missing_required_null_fails_closed() {
    let input = GenerationInput::new().node(GenerationNode::new(1u64, "Z"));
    let mut payload = bounded_payload(&input);
    let flags = layout_flags::from_companion_segments(false, false, true);
    payload[12..16].copy_from_slice(&flags.to_le_bytes());
    recompute_outer_crc(&mut payload);
    let mut sec = CompactStoreSection::empty();
    let err = sec
        .deserialize_from_bytes(Bytes::from(payload))
        .expect_err("must fail closed on missing null");
    assert!(err.to_string().contains("ColumnRowNull"), "error: {err}");
}

#[test]
fn surgery_corrupted_directory_crc_fails_closed() {
    let input =
        GenerationInput::new().node(GenerationNode::new(1u64, "Z").with_prop("v", Value::Int64(1)));
    let mut payload = bounded_payload(&input);
    // Flip a byte inside the directory region.
    payload[64] ^= 0xFF;
    // Do NOT recompute directory CRC — reader must detect mismatch.
    recompute_outer_crc(&mut payload);
    let mut sec = CompactStoreSection::empty();
    let err = sec
        .deserialize_from_bytes(Bytes::from(payload))
        .expect_err("must fail closed on directory CRC mismatch");
    assert!(err.to_string().contains("CRC"), "error: {err}");
}

#[test]
fn surgery_corrupted_outer_crc_fails_closed() {
    let input =
        GenerationInput::new().node(GenerationNode::new(1u64, "Z").with_prop("v", Value::Int64(1)));
    let mut payload = bounded_payload(&input);
    // Corrupt the trailing CRC.
    let tail = payload.len() - 1;
    payload[tail] ^= 0xFF;
    let mut sec = CompactStoreSection::empty();
    let err = sec
        .deserialize_from_bytes(Bytes::from(payload))
        .expect_err("must fail closed on outer CRC mismatch");
    assert!(
        err.to_string().to_lowercase().contains("crc"),
        "error: {err}"
    );
}
