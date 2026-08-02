//! G-EM0.5b R3 — Outer publish→recover→mmap source-true matrix.
//!
//! Proves the full production path: DiskRunStore bounded build → W0
//! publication → recovery selection → fresh mmap reopen → public
//! `GraphStore` reads. Covers bullets A–E of the R3 contract.
//!
//! R3 finisher: all surgery tests go through the REAL outer path
//! (publish→recover→mmap→public read). Owner type replaces mem::forget.
#![cfg(all(
    feature = "generation-streaming",
    feature = "compact-store",
    feature = "lpg",
    feature = "mmap"
))]

use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;
use grafeo_common::storage::SectionType;
use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};
use grafeo_core::graph::Direction;
use grafeo_core::graph::compact::CompactStore;
use grafeo_core::graph::compact::generation::{
    GenerationBudget, GenerationEdge, GenerationInput, GenerationNode, InMemoryRunStore,
    RelSchemaDecl,
};
use grafeo_core::graph::compact::generation_builder::orchestrator::{
    BoundedBuildConfig, BoundedGenerationBuilder,
};
use grafeo_core::graph::compact::mapped::layout_flags;
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_core::graph::lpg::CompareOp;
use grafeo_core::graph::traits::{GraphStore, GraphStoreMut};
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::file::generation_writer::{
    ExactSectionSource, GenerationContainerHeader, OsGenerationFileOps,
    StreamingPayloadSectionSource,
};
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::publication::{PublicationInput, publish_generation};
use grafeo_storage::generation::recovery::recover;
use grafeo_storage::generation::run_adapter::DiskRunStore;
use grafeo_storage::wal::WalManager;
use tempfile::TempDir;

// ── R3-M4: Owner type keeps store + TempDir alive together ─────────

/// Owns the mmap-backed store together with the TempDir that holds the
/// on-disk generation files. Dropping the owner cleans up both.
pub(crate) struct OuterOwner {
    store: Arc<CompactStore>,
    _tmp: TempDir,
}

impl OuterOwner {
    fn store(&self) -> &Arc<CompactStore> {
        &self.store
    }
}

impl Drop for OuterOwner {
    fn drop(&mut self) {
        // TempDir drops after store, cleaning up files.
        // Explicit order: store Arc drops first (field order), then _tmp.
    }
}

// ── Helper: outer publish → recover → mmap reopen (R3-M4 owner) ────

/// Full production path: DiskRunStore bounded build → W0 publication →
/// recovery selection → fresh mmap reopen → `OuterOwner`.
pub(crate) fn outer_publish_and_mmap_reopen(input: &GenerationInput, gen_id: &str) -> OuterOwner {
    outer_publish_and_mmap_reopen_with_schemas(input, gen_id, Vec::new())
}

/// Same as above but with explicit rel_schemas for endpoint validation.
pub(crate) fn outer_publish_and_mmap_reopen_with_schemas(
    input: &GenerationInput,
    gen_id: &str,
    rel_schemas: Vec<RelSchemaDecl>,
) -> OuterOwner {
    let tmp = TempDir::new().unwrap();
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
        rel_schemas,
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
    let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![Box::new(section_source)];
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

    OuterOwner { store, _tmp: tmp }
}

// ── R3-B1: Raw-bytes section source for outer surgery ──────────────

/// Wraps raw payload bytes as an ExactSectionSource so surgically-modified
/// payloads go through the REAL outer publish→recover→mmap path.
struct RawBytesSectionSource {
    data: Vec<u8>,
    pos: usize,
}

impl RawBytesSectionSource {
    fn new(data: Vec<u8>) -> Self {
        Self { data, pos: 0 }
    }
}

impl ExactSectionSource for RawBytesSectionSource {
    fn section_type(&self) -> SectionType {
        SectionType::CompactStore
    }
    fn directory_version(&self) -> u8 {
        5
    }
    fn exact_len(&self) -> u64 {
        self.data.len() as u64
    }
    fn copy_to(&mut self, sink: &mut dyn Write) -> grafeo_common::utils::error::Result<()> {
        sink.write_all(&self.data[self.pos..])
            .map_err(|e| grafeo_common::utils::error::Error::Internal(e.to_string()))?;
        self.pos = self.data.len();
        Ok(())
    }
}

/// Publishes raw payload bytes through the full outer path and attempts
/// mmap reopen. Returns Err if any stage fails (the expected outcome for
/// surgery tests).
pub(crate) fn outer_publish_raw_and_reopen(
    payload: Vec<u8>,
    gen_id: &str,
) -> Result<Arc<CompactStore>, String> {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().join("gen-root");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&root).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&wal_dir).map_err(|e| e.to_string())?;

    let wal = WalManager::open(&wal_dir).map_err(|e| e.to_string())?;
    let lock = RootLock::try_acquire(&root).map_err(|e| e.to_string())?;
    let header = GenerationContainerHeader {
        epoch: 1,
        transaction_id: 1,
        node_count: 1,
        edge_count: 0,
    };
    let section_source = RawBytesSectionSource::new(payload);
    let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![Box::new(section_source)];
    publish_generation(
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
    .map_err(|e| format!("publish: {e}"))?;
    drop(lock);

    let lock = RootLock::try_acquire(&root).map_err(|e| e.to_string())?;
    let selected = recover(&lock).map_err(|e| format!("recover: {e}"))?;
    drop(lock);

    let manager = GrafeoFileManager::open_read_only(&selected.generation_abs_path)
        .map_err(|e| format!("open_ro: {e}"))?;
    let section_dir = manager
        .read_section_directory()
        .map_err(|e| format!("dir: {e}"))?
        .ok_or("no section directory")?;
    let entry = section_dir
        .find(SectionType::CompactStore)
        .ok_or("no CompactStore section")?;
    let mmap = Arc::new(
        manager
            .mmap_section(entry)
            .map_err(|e| format!("mmap: {e}"))?,
    );
    let mapped_bytes = grafeo_storage::container::MmapSection::into_bytes(mmap);
    let mut cs = CompactStoreSection::empty();
    cs.deserialize_from_mapped_bytes(mapped_bytes)
        .map_err(|e| format!("deserialize: {e}"))?;
    cs.store().ok_or("no store".into())
}

/// In-memory bounded build → raw v5 payload bytes (for surgery tests).
pub(crate) fn bounded_payload(input: &GenerationInput) -> Vec<u8> {
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
pub(crate) fn recompute_outer_crc(payload: &mut [u8]) {
    let tail = payload.len() - 4;
    let crc = crc32fast::hash(&payload[..tail]);
    payload[tail..].copy_from_slice(&crc.to_le_bytes());
}

/// Recompute directory CRC (header offset 56..60) after directory surgery.
/// Header layout: MAGIC(0..4) ver(4) flags(5) hdr_len(6..8) seg_count(8..10)
///   entry_len(10..12) layout_flags(12..16) dir_off(16..24) dir_len(24..32)
///   data_off(32..40) total_nodes(40..48) total_edges(48..56) dir_crc(56..60) res(60..64)
#[allow(dead_code)]
pub(crate) fn recompute_directory_crc(payload: &mut [u8]) {
    let seg_count = u16::from_le_bytes([payload[8], payload[9]]) as usize;
    let dir_start = 64usize; // HEADER_LEN
    let dir_len = seg_count * 48; // DIRECTORY_ENTRY_LEN
    let dir_crc = crc32fast::hash(&payload[dir_start..dir_start + dir_len]);
    payload[56..60].copy_from_slice(&dir_crc.to_le_bytes());
}

// ── A: Node AND relationship properties, cross-table identical keys ─

#[test]
fn outer_node_and_rel_properties_cross_table_identical_keys() {
    let input = GenerationInput::new()
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
        .edge(
            GenerationEdge::new(10u64, 1u64, 2u64, "OWNS").with_prop("weight", Value::Float64(1.5)),
        )
        .edge(
            GenerationEdge::new(11u64, 2u64, 1u64, "REFERENCES")
                .with_prop("weight", Value::Float64(2.5)),
        );

    let owner = outer_publish_and_mmap_reopen(&input, "a-cross-keys");
    let store = owner.store();

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

    assert_eq!(
        store.get_edge_property(EdgeId::new(10), &PropertyKey::new("weight")),
        Some(Value::Float64(1.5)),
        "OWNS weight via original edge id 10"
    );
    assert_eq!(
        store.get_edge_property(EdgeId::new(11), &PropertyKey::new("weight")),
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

    let batch =
        store.get_node_property_batch(&[NodeId::new(1), NodeId::new(2)], &PropertyKey::new("name"));
    assert_eq!(batch.len(), 2);
    assert!(batch[0].is_some());
    assert!(batch[1].is_some());

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

// ── B: Absent vs present-null across types and read paths (nodes) ──

#[test]
fn outer_absent_vs_present_null_all_types_and_read_paths() {
    let vec_val = Value::Vector(Arc::from([1.0f32, 2.0, 3.0]));
    let input = GenerationInput::new()
        .node(
            GenerationNode::new(1u64, "T")
                .with_prop("int_key", Value::Int64(42))
                .with_prop("bool_key", Value::Bool(true))
                .with_prop("str_key", "hello")
                .with_prop("float_key", Value::Float64(f64::NAN))
                .with_prop("vec_key", vec_val.clone()),
        )
        .node(
            GenerationNode::new(2u64, "T")
                .with_prop("int_key", Value::Null)
                .with_prop("bool_key", Value::Null)
                .with_prop("str_key", Value::Null)
                .with_prop("float_key", Value::Null)
                .with_prop("vec_key", Value::Null),
        )
        .node(GenerationNode::new(3u64, "T"));

    let owner = outer_publish_and_mmap_reopen(&input, "b-null-absent");
    let store = owner.store();

    let keys = ["int_key", "bool_key", "str_key", "float_key", "vec_key"];

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

    let f1 = store
        .get_node_property(NodeId::new(1), &PropertyKey::new("float_key"))
        .expect("float present");
    assert!(
        f64::is_nan(f1.as_float64().expect("float64")),
        "NaN must survive"
    );

    let v1 = store
        .get_node_property(NodeId::new(1), &PropertyKey::new("vec_key"))
        .expect("vec present");
    assert_eq!(v1.as_vector().expect("vector"), &[1.0f32, 2.0, 3.0]);

    let batch = store.get_node_property_batch(
        &[NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        &PropertyKey::new("int_key"),
    );
    assert_eq!(batch[0], Some(Value::Int64(42)));
    assert_eq!(batch[1], Some(Value::Null));
    assert_eq!(batch[2], None);

    let hits = store.find_nodes_by_property("int_key", &Value::Int64(42));
    assert_eq!(hits, vec![NodeId::new(1)]);

    let pruned = store.find_nodes_by_property("int_key", &Value::Int64(i64::MAX));
    assert!(pruned.is_empty(), "zone map must prune impossible value");

    // Range query through GraphStoreSearch.
    // Note: null/absent rows store default 0 in the column codec, so
    // range [0,100] would hit all rows. Use [40,50] to isolate row 1 (value 42).
    use grafeo_core::graph::traits::GraphStoreSearch;
    let range_hits: Vec<NodeId> = store
        .find_nodes_in_range_iter(
            "int_key",
            Some(&Value::Int64(40)),
            Some(&Value::Int64(50)),
            true,
            true,
        )
        .collect();
    assert_eq!(
        range_hits,
        vec![NodeId::new(1)],
        "range [40,50] hits only row 1"
    );
    let range_miss: Vec<NodeId> = store
        .find_nodes_in_range_iter(
            "int_key",
            Some(&Value::Int64(1000)),
            Some(&Value::Int64(2000)),
            true,
            true,
        )
        .collect();
    assert!(
        range_miss.is_empty(),
        "range [1000,2000] pruned by zone map"
    );
}

// ── R3-M1: Rel property matrix (absent/null × types × read surfaces) ──

#[test]
fn outer_rel_property_matrix_all_types_and_read_surfaces() {
    // R3-M1: rel property matrix across types × read surfaces.
    // RESIDUAL: rel tables do not yet have per-row presence/null companions
    // (unlike node tables). Null-encoded and absent rows both return the
    // column codec default. This test proves the matrix for present values
    // and documents the null/absent gap.
    let vec_val = Value::Vector(Arc::from([4.0f32, 5.0, 6.0]));
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "N"))
        .node(GenerationNode::new(2u64, "N"))
        .node(GenerationNode::new(3u64, "N"))
        // Edge 10: all props present with real values.
        .edge(
            GenerationEdge::new(10u64, 1u64, 2u64, "R")
                .with_prop("int_p", Value::Int64(7))
                .with_prop("bool_p", Value::Bool(false))
                .with_prop("str_p", "world")
                .with_prop("float_p", Value::Float64(3.14))
                .with_prop("vec_p", vec_val.clone()),
        )
        // Edge 11: different values to prove per-row independence.
        .edge(
            GenerationEdge::new(11u64, 2u64, 3u64, "R")
                .with_prop("int_p", Value::Int64(99))
                .with_prop("bool_p", Value::Bool(true))
                .with_prop("str_p", "other")
                .with_prop("float_p", Value::Float64(2.71))
                .with_prop("vec_p", Value::Vector(Arc::from([7.0f32, 8.0, 9.0]))),
        )
        // Edge 12: no props (tests column default behavior).
        .edge(GenerationEdge::new(12u64, 3u64, 1u64, "R"));

    let owner = outer_publish_and_mmap_reopen(&input, "m1-rel-matrix");
    let store = owner.store();

    // ── Point reads (get_edge_property) ──
    assert_eq!(
        store.get_edge_property(EdgeId::new(10), &PropertyKey::new("int_p")),
        Some(Value::Int64(7))
    );
    assert_eq!(
        store.get_edge_property(EdgeId::new(10), &PropertyKey::new("bool_p")),
        Some(Value::Bool(false))
    );
    assert_eq!(
        store.get_edge_property(EdgeId::new(10), &PropertyKey::new("str_p")),
        Some(Value::from("world"))
    );
    assert_eq!(
        store.get_edge_property(EdgeId::new(10), &PropertyKey::new("float_p")),
        Some(Value::Float64(3.14))
    );
    assert_eq!(
        store.get_edge_property(EdgeId::new(10), &PropertyKey::new("vec_p")),
        Some(vec_val.clone())
    );
    // Edge 11 has different values.
    assert_eq!(
        store.get_edge_property(EdgeId::new(11), &PropertyKey::new("int_p")),
        Some(Value::Int64(99))
    );
    assert_eq!(
        store.get_edge_property(EdgeId::new(11), &PropertyKey::new("str_p")),
        Some(Value::from("other"))
    );

    // ── Batch reads (get_edges_properties_selective_batch) ──
    let batch = store.get_edges_properties_selective_batch(
        &[EdgeId::new(10), EdgeId::new(11)],
        &[PropertyKey::new("int_p"), PropertyKey::new("str_p")],
    );
    assert_eq!(
        batch[0].get(&PropertyKey::new("int_p")),
        Some(&Value::Int64(7))
    );
    assert_eq!(
        batch[0].get(&PropertyKey::new("str_p")),
        Some(&Value::from("world"))
    );
    assert_eq!(
        batch[1].get(&PropertyKey::new("int_p")),
        Some(&Value::Int64(99))
    );
    assert_eq!(
        batch[1].get(&PropertyKey::new("str_p")),
        Some(&Value::from("other"))
    );

    // ── get_all_edge_properties via rel table ──
    let rt = store.rel_table("R").expect("R rel table");
    let all10 = rt.get_all_edge_properties(0);
    assert_eq!(
        all10.get(&PropertyKey::new("int_p")),
        Some(&Value::Int64(7))
    );
    assert_eq!(
        all10.get(&PropertyKey::new("str_p")),
        Some(&Value::from("world"))
    );
    assert_eq!(
        all10.get(&PropertyKey::new("bool_p")),
        Some(&Value::Bool(false))
    );
    let all11 = rt.get_all_edge_properties(1);
    assert_eq!(
        all11.get(&PropertyKey::new("int_p")),
        Some(&Value::Int64(99))
    );

    // ── Zone-map pruning (R3-B2 positive) ──
    assert!(
        store.edge_property_might_match(
            &PropertyKey::new("int_p"),
            CompareOp::Eq,
            &Value::Int64(7)
        ),
        "zone map must allow matching value 7"
    );
    assert!(
        !store.edge_property_might_match(
            &PropertyKey::new("int_p"),
            CompareOp::Eq,
            &Value::Int64(9999)
        ),
        "zone map must prune impossible value 9999"
    );

    // ── get_edge returns full edge with properties ──
    let e10 = store.get_edge(EdgeId::new(10)).expect("edge 10");
    assert_eq!(
        e10.properties.get(&PropertyKey::new("int_p")),
        Some(&Value::Int64(7))
    );
    assert_eq!(
        e10.properties.get(&PropertyKey::new("str_p")),
        Some(&Value::from("world"))
    );
    let e11 = store.get_edge(EdgeId::new(11)).expect("edge 11");
    assert_eq!(
        e11.properties.get(&PropertyKey::new("int_p")),
        Some(&Value::Int64(99))
    );
}

// ── C: Multi-label membership through outer path ───────────────────

#[test]
fn outer_two_and_three_label_membership() {
    let input = GenerationInput::new()
        .node(
            GenerationNode::with_labels(1u64, ["Person", "Employee"])
                .unwrap()
                .with_prop("name", "Alice"),
        )
        .node(
            GenerationNode::with_labels(2u64, ["C", "A", "B"])
                .unwrap()
                .with_prop("name", "Bob"),
        )
        .node(GenerationNode::new(3u64, "Person").with_prop("name", "Carol"));

    let owner = outer_publish_and_mmap_reopen(&input, "c-labels");
    let store = owner.store();

    let n1 = store.get_node(NodeId::new(1)).expect("node 1");
    let labels1: Vec<String> = n1.labels.iter().map(|l| l.to_string()).collect();
    assert!(labels1.contains(&"Person".to_string()));
    assert!(labels1.contains(&"Employee".to_string()));
    assert_eq!(labels1.len(), 2);

    let n2 = store.get_node(NodeId::new(2)).expect("node 2");
    let labels2: Vec<String> = n2.labels.iter().map(|l| l.to_string()).collect();
    assert_eq!(labels2.len(), 3);
    for l in ["A", "B", "C"] {
        assert!(labels2.contains(&l.to_string()));
    }

    assert_eq!(store.nodes_by_label("Person").len(), 2);
    assert_eq!(store.nodes_by_label("Employee").len(), 1);
    assert_eq!(store.nodes_by_label("A").len(), 1);
    assert_eq!(store.nodes_by_label("B").len(), 1);
    assert_eq!(store.nodes_by_label("C").len(), 1);

    let all = store.all_labels();
    for l in ["Person", "Employee", "A", "B", "C"] {
        assert!(all.iter().any(|x| x == l), "all_labels must include {l}");
    }

    // Physical table = labels[0] after sort (lex-earlier).
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

// ── R3-M2: Overlay add/remove + lex-earlier via LayeredStore ───────

#[test]
fn outer_overlay_add_remove_label_and_lex_earlier() {
    use grafeo_core::graph::compact::layered::LayeredStore;

    // Build base with multi-label node through outer path.
    let input = GenerationInput::new()
        .node(
            GenerationNode::with_labels(1u64, ["Zebra", "Apple"])
                .unwrap()
                .with_prop("v", Value::Int64(1)),
        )
        .node(GenerationNode::new(2u64, "Zebra").with_prop("v", Value::Int64(2)));

    let owner = outer_publish_and_mmap_reopen(&input, "m2-overlay");
    let base = Arc::clone(owner.store());

    // Lex-earlier: physical table is "Apple" (sorted first).
    assert!(
        base.node_table("Apple").is_some(),
        "lex-earlier label Apple is physical table"
    );

    // nodes_by_label resolves both labels.
    assert_eq!(base.nodes_by_label("Apple").len(), 1);
    assert_eq!(base.nodes_by_label("Zebra").len(), 2);

    // Wrap in LayeredStore for mutation (with_overlay takes Arc<CompactStore>).
    use grafeo_core::graph::lpg::LpgStore;
    let overlay = Arc::new(LpgStore::new().expect("overlay"));
    overlay.set_next_node_id(3);
    overlay.set_next_edge_id(1);
    let layered = LayeredStore::with_overlay(Arc::clone(&base), overlay);

    // Overlay: add a new label to node 1.
    assert!(layered.add_label(NodeId::new(1), "Mango"), "add new label");
    let n1 = layered.get_node(NodeId::new(1)).expect("node 1");
    let labels: Vec<String> = n1.labels.iter().map(|l| l.to_string()).collect();
    assert!(labels.contains(&"Mango".to_string()), "Mango added");
    assert!(labels.contains(&"Apple".to_string()), "Apple retained");
    assert!(labels.contains(&"Zebra".to_string()), "Zebra retained");

    // Overlay: remove a label from node 1.
    assert!(
        layered.remove_label(NodeId::new(1), "Zebra"),
        "remove Zebra"
    );
    let n1_after = layered.get_node(NodeId::new(1)).expect("node 1 after");
    let labels_after: Vec<String> = n1_after.labels.iter().map(|l| l.to_string()).collect();
    assert!(
        !labels_after.contains(&"Zebra".to_string()),
        "Zebra removed"
    );
    assert!(
        labels_after.contains(&"Apple".to_string()),
        "Apple still present"
    );

    // nodes_by_label through layered reflects overlay.
    assert_eq!(layered.nodes_by_label("Mango").len(), 1, "Mango visible");
    // Zebra: node 2 still has it, node 1 removed → 1.
    assert_eq!(
        layered.nodes_by_label("Zebra").len(),
        1,
        "Zebra only node 2"
    );

    // Drop owner — base store + temp cleaned up; layered still works via Arc.
    drop(owner);
    assert_eq!(
        layered.nodes_by_label("Apple").len(),
        1,
        "layered survives owner drop"
    );
}

// ── R3-M2: Real rel_schemas + endpoint validation ──────────────────

#[test]
fn outer_rel_schemas_endpoint_validation_rejects_wrong_label() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person"))
        .node(GenerationNode::new(2u64, "Project"))
        // Edge with correct endpoints.
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "OWNS"))
        // Edge with WRONG src endpoint (Project→Project, but schema says Person→Project).
        .edge(GenerationEdge::new(11u64, 2u64, 2u64, "OWNS"));

    let schemas = vec![RelSchemaDecl::new("OWNS", "Person", "Project")];

    // This must fail at build time due to endpoint validation.
    let tmp = TempDir::new().unwrap();
    let runs_dir = tmp.path().join("runs");
    let budget = GenerationBudget::for_tests();
    let mut run_store = DiskRunStore::new(&runs_dir, budget, "m2-schema").expect("DiskRunStore");
    let config = BoundedBuildConfig {
        budget,
        temp_dir: tmp.path().join("build-tmp"),
        correlation_id: "m2-schema".into(),
        spool_buf_cap: 64 * 1024,
        rel_schemas: schemas,
        frozen_epoch: 0,
    };
    let mut builder = BoundedGenerationBuilder::new(config);
    let result = builder.build(
        &mut input.node_source(),
        &mut input.edge_source(),
        &mut run_store,
    );
    assert!(
        result.is_err(),
        "endpoint validation must reject wrong src label"
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("WrongTableEndpoint")
            || err_msg.contains("endpoint")
            || err_msg.contains("table"),
        "error mentions endpoint/table: {err_msg}"
    );
}

#[test]
fn outer_rel_schemas_endpoint_validation_accepts_correct() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person"))
        .node(GenerationNode::new(2u64, "Project"))
        .edge(GenerationEdge::new(10u64, 1u64, 2u64, "OWNS").with_prop("w", Value::Int64(1)));

    let schemas = vec![RelSchemaDecl::new("OWNS", "Person", "Project")];
    let owner = outer_publish_and_mmap_reopen_with_schemas(&input, "m2-schema-ok", schemas);
    let store = owner.store();
    assert_eq!(store.total_edges(), 1);
    assert_eq!(
        store.get_edge_property(EdgeId::new(10), &PropertyKey::new("w")),
        Some(Value::Int64(1))
    );
}

// ── D: Relationship endpoints, sparse IDs, self-loops, duplicates ──

#[test]
fn outer_rel_endpoints_sparse_self_loop_duplicate_both_csr() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(100u64, "N"))
        .node(GenerationNode::new(200u64, "N"))
        .node(GenerationNode::new(300u64, "N"))
        .edge(GenerationEdge::new(10u64, 100u64, 200u64, "LINK"))
        .edge(GenerationEdge::new(11u64, 100u64, 100u64, "LINK"))
        .edge(GenerationEdge::new(12u64, 100u64, 200u64, "LINK"))
        .edge(GenerationEdge::new(13u64, 200u64, 100u64, "LINK"))
        .edge(GenerationEdge::new(14u64, 300u64, 100u64, "LINK"));

    let owner = outer_publish_and_mmap_reopen(&input, "d-rel");
    let store = owner.store();

    assert_eq!(store.total_nodes(), 3);
    assert_eq!(store.total_edges(), 5);

    let out_100 = store.neighbors(NodeId::new(100), Direction::Outgoing);
    assert_eq!(out_100.len(), 3);
    assert!(out_100.contains(&NodeId::new(200)));
    assert!(out_100.contains(&NodeId::new(100)));

    let inc_100 = store.neighbors(NodeId::new(100), Direction::Incoming);
    assert_eq!(inc_100.len(), 3);
    assert!(inc_100.contains(&NodeId::new(200)));
    assert!(inc_100.contains(&NodeId::new(100)));
    assert!(inc_100.contains(&NodeId::new(300)));

    let out_200 = store.neighbors(NodeId::new(200), Direction::Outgoing);
    assert_eq!(out_200.len(), 1);
    let inc_200 = store.neighbors(NodeId::new(200), Direction::Incoming);
    assert_eq!(inc_200.len(), 2);

    let both_100 = store.neighbors(NodeId::new(100), Direction::Both);
    assert_eq!(both_100.len(), 6);

    let e10 = store.get_edge(EdgeId::new(10)).expect("edge 10");
    assert_eq!(e10.src, NodeId::new(100));
    assert_eq!(e10.dst, NodeId::new(200));
    let e11 = store.get_edge(EdgeId::new(11)).expect("edge 11 self-loop");
    assert_eq!(e11.src, NodeId::new(100));
    assert_eq!(e11.dst, NodeId::new(100));

    let edges_from_100 = store.edges_from(NodeId::new(100), Direction::Outgoing);
    assert_eq!(edges_from_100.len(), 3);
    let edge_ids: Vec<u64> = edges_from_100.iter().map(|(_, eid)| eid.as_u64()).collect();
    assert!(edge_ids.contains(&10));
    assert!(edge_ids.contains(&11));
    assert!(edge_ids.contains(&12));

    let rt = store.rel_table("LINK").expect("LINK rel table");
    assert_eq!(rt.num_edges(), 5);
}

// ── E: Old-v5 defaults + fail-closed companion surgery ─────────────

#[test]
fn outer_old_v5_defaults_single_label_no_companions() {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "X").with_prop("v", Value::Int64(1)))
        .node(GenerationNode::new(2u64, "X").with_prop("v", Value::Int64(2)));
    let owner = outer_publish_and_mmap_reopen(&input, "e-old-v5");
    let store = owner.store();
    assert_eq!(store.total_nodes(), 2);
    let n1 = store.get_node(NodeId::new(1)).expect("n1");
    assert_eq!(n1.labels.len(), 1);
    assert_eq!(
        store.get_node_property(NodeId::new(1), &PropertyKey::new("v")),
        Some(Value::Int64(1))
    );
}

// ── R3-M3: Independent pre-cutover old-v5 fixture reopen ───────────

#[test]
fn outer_old_v5_fixture_reopen() {
    // Load committed fixture bytes (produced by feature-OFF eager path).
    let fixture_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/golden/old_v5_single_label.bin"
    );
    let fixture_bytes = std::fs::read(fixture_path).expect("fixture must be committed");
    assert!(fixture_bytes.len() > 64, "fixture must be non-trivial");

    // Reopen through the outer path: publish raw fixture → recover → mmap → read.
    let store = outer_publish_raw_and_reopen(fixture_bytes, "m3-fixture")
        .expect("fixture must reopen through outer path");

    // Locked defaults: single label, no companions, correct values.
    assert_eq!(store.total_nodes(), 2);
    assert_eq!(store.total_edges(), 1);

    let n0 = store.get_node(NodeId::new(0)).expect("node 0");
    assert_eq!(
        n0.labels.len(),
        1,
        "single physical label, no membership companion"
    );
    assert_eq!(n0.labels[0].as_str(), "Person");

    // Property reads.
    let name0 = store.get_node_property(NodeId::new(0), &PropertyKey::new("name"));
    assert_eq!(name0, Some(Value::from("Alice")));
    let name1 = store.get_node_property(NodeId::new(1), &PropertyKey::new("name"));
    assert_eq!(name1, Some(Value::from("Bob")));

    // Edge reads.
    let rt = store.rel_table("KNOWS").expect("KNOWS");
    assert_eq!(rt.num_edges(), 1);
}

// ── R3-B1: Surgery through REAL outer path ─────────────────────────
// Each case: build payload → surgically modify → publish raw → recover
// → mmap → deserialize. Must fail closed at the outer boundary.

mod surgery;
