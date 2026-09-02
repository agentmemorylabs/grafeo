//! G-GEM0.SRV1 Gap B RED repro — a published generation's vector index must
//! be SERVABLE after read-only generation-root reopen (H-ADOPT.7 Wave 4 G2
//! run 6 frontier finding).
//!
//! Frontier symptom (verified on the 3.08 GB published container):
//! after `build_and_publish_generation` + `GrafeoDB::open_generation_root(
//! root, true)`, am-graph `vector_index_health(CODE_RETRIEVAL_UNIT)` reports
//! `dims=None searchable=false vectors_present=0 method=none` even though
//! - the published container HAS a VectorStore v2 section (16 MB = HNSW
//!   topology only; raw vectors live in the CompactStore embedding column),
//! - Cypher `r.embedding IS NOT NULL` returns all 128,094 rows (the columns
//!   ARE readable through the query predicate path), and
//! - node/edge serving otherwise works.
//!
//! The invariant under test: after RO generation-root reopen, the published
//! vector index must be servable — the exact vector-value read path
//! (`graph_store().get_node(id).get_property(prop)` → `Value::Vector`), the
//! registered dims, and a k=1 nearest-neighbor probe that returns results.
//! This mirrors the am-graph health probes (dimension detection via property
//! sampling + unconditional k=1 HNSW probe) at engine-test scale.
//!
//! Three variants:
//! - `published_plain_vector_index_serves_after_ro_reopen` — heap-backed
//!   index (Auto tier), the shape every pre-existing engine test publishes.
//! - `published_forcedisk_vector_index_serves_after_ro_reopen` — ForceDisk
//!   tier with the inline column actually drained to a spill sidecar before
//!   publish, the production AMH shape. The spill dir is NOT part of the
//!   generation root, so the reopened RO DB must serve from the published
//!   property column alone.
//! - `published_scalar_quantized_vector_index_serves_after_ro_reopen` — the
//!   production quantization mode (scalar), proving the catalog-restored
//!   quantized shell + restored topology serve without a payload rehydrate.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "vector-index",
    not(feature = "temporal")
))]

use grafeo_common::storage::{SectionType, TierOverride};
use grafeo_common::types::Value;
use grafeo_engine::{Config, GrafeoDB, generation_build_request};
use tempfile::TempDir;

const LABEL: &str = "RetrievalUnit";
const PROP: &str = "embedding";
const DIMS: usize = 16;
const NODES: usize = 96;

fn seeded_vector(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut raw: Vec<f32> = (0..dim)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            ((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0
        })
        .collect();
    let norm: f32 = raw.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut raw {
            *x /= norm;
        }
    }
    raw
}

/// Frontier label order: RetrievalUnit is table 2 (created third), matching
/// the frontier shape where the vector-bearing label is NOT the first table.
fn seed_source(db: &GrafeoDB) -> Vec<grafeo_common::types::NodeId> {
    seed_source_with_quantization(db, None)
}

fn seed_source_with_quantization(
    db: &GrafeoDB,
    quantization: Option<&str>,
) -> Vec<grafeo_common::types::NodeId> {
    // Table 0: CodeDocument (no vectors).
    db.create_node_with_props(&["CodeDocument"], [("path", Value::from("a.rs"))])
        .expect("doc");
    // Table 1: CodeSymbol.
    db.create_node_with_props(&["CodeSymbol"], [("name", Value::from("f1"))])
        .expect("sym");
    // Table 2: RetrievalUnit with embedding property columns.
    let mut nodes = Vec::with_capacity(NODES);
    for i in 0..NODES {
        let id = db
            .create_node_with_props(
                &[LABEL],
                [(PROP, Value::Vector(seeded_vector(i as u64, DIMS).into()))],
            )
            .expect("create RetrievalUnit");
        nodes.push(id);
    }
    db.create_vector_index(
        LABEL,
        PROP,
        Some(DIMS),
        Some("cosine"),
        None,
        None,
        quantization,
    )
    .expect("create vector index");
    nodes
}

/// Mirrors the am-graph dimension-detection probe: Cypher property sampling
/// then `get_node().get_property(prop)` must yield a `Value::Vector`.
fn assert_health_probes_see_vectors(db: &GrafeoDB, expected: i64) {
    // (1) Cypher predicate sees the column (frontier: 128,094 rows).
    let rows = db
        .session()
        .execute(&format!(
            "MATCH (n:{LABEL}) WHERE n.{PROP} IS NOT NULL RETURN count(n)"
        ))
        .expect("cypher embedding predicate");
    let got: i64 = rows
        .rows()
        .first()
        .and_then(|r| r.first())
        .and_then(Value::as_int64)
        .unwrap_or(-1);
    assert_eq!(
        got, expected,
        "Cypher embedding predicate must see all vectors"
    );

    // (2) The exact value path the health dimension probe uses: node property
    // materialization over the reopened layered store.
    let store = db.graph_store();
    let mut seen = 0i64;
    for nid in store.nodes_by_label(LABEL) {
        let node = store
            .get_node(nid)
            .unwrap_or_else(|| panic!("reopened DB must serve node {nid:?}"));
        match node.get_property(PROP) {
            Some(Value::Vector(v)) if v.len() == DIMS => seen += 1,
            other => panic!(
                "reopened DB property read for :{LABEL}({PROP}) node {nid:?} \
                 must be a {DIMS}-dim vector, got {other:?}"
            ),
        }
    }
    assert_eq!(seen, expected, "exact property read must see every vector");

    // (3) The am-graph `detect_stored_embedding_dimension` probe verbatim:
    // Cypher `id(n)` sampling, then `get_node(id).get_property(prop)` must
    // yield a vector whose width becomes the reported dims.
    let sampled = db
        .session()
        .execute(&format!(
            "MATCH (n:{LABEL}) WHERE n.{PROP} IS NOT NULL RETURN id(n) AS node_id LIMIT 100"
        ))
        .expect("cypher health dimension sampling");
    let mut dims: Option<usize> = None;
    for row in sampled.rows() {
        let Some(node_id) = row.first().and_then(Value::as_int64) else {
            continue;
        };
        let Some(node) = db.get_node(grafeo_common::types::NodeId::from(node_id as u64)) else {
            continue;
        };
        if let Some(d) = node.get_property(PROP).and_then(Value::vector_dimensions) {
            dims = Some(d);
            break;
        }
    }
    assert_eq!(
        dims,
        Some(DIMS),
        "health dimension sampling must detect the stored width (frontier symptom: None)"
    );
}

fn assert_k1_serves(db: &GrafeoDB, nodes: &[grafeo_common::types::NodeId], target: usize) {
    // Shell registered with the right width.
    assert!(
        db.has_vector_index(LABEL, PROP),
        "vector index must be registered after RO reopen"
    );
    assert_eq!(
        db.vector_index_dimensions(LABEL, PROP),
        Some(DIMS),
        "registered dims must survive RO reopen"
    );
    // Unconditional k=1 probe (same as am-graph probe_vector_index_ready).
    let query = seeded_vector(target as u64, DIMS);
    let hits = db
        .vector_search(LABEL, PROP, &query, 1, None, None)
        .expect("k=1 vector search after RO reopen");
    assert!(
        !hits.is_empty(),
        "k=1 probe must return results after RO reopen (frontier symptom: empty)"
    );
    assert_eq!(
        hits[0].0, nodes[target],
        "k=1 probe must return the exact target vector"
    );
    assert!(
        hits[0].1 < 1.0e-6,
        "k=1 probe distance must be ~0, got {}",
        hits[0].1
    );
}

#[test]
fn published_plain_vector_index_serves_after_ro_reopen() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("srv1-gapb-plain.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    let source = GrafeoDB::new_in_memory();
    let nodes = seed_source(&source);
    source
        .build_and_publish_generation(generation_build_request(&root, "srv1-gapb-plain-g1"))
        .expect("publish generation");
    drop(source);

    let db = GrafeoDB::open_generation_root(&root, true).expect("open generation root RO");
    assert_health_probes_see_vectors(&db, NODES as i64);
    assert_k1_serves(&db, &nodes, 7);
}

#[test]
fn published_forcedisk_vector_index_serves_after_ro_reopen() {
    let dir = TempDir::new().expect("temp dir");
    let db_path = dir.path().join("srv1-gapb-fd-source.grafeo");
    let spill_path = dir.path().join("srv1-gapb-fd.spill");
    let root = dir.path().join("srv1-gapb-fd.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    let config = Config::persistent(&db_path)
        .with_spill_path(&spill_path)
        .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk);
    let source = GrafeoDB::with_config(config).expect("open source db");
    let nodes = seed_source(&source);

    // Force the drain: embeddings move out of the inline property column into
    // the spill sidecar (the AMH ForceDisk shape).
    source.buffer_manager().spill_all();
    assert_eq!(
        source.storage_tiers().get(&SectionType::VectorStore),
        Some(&grafeo_common::memory::StorageTier::OnDisk),
        "ForceDisk tier must be OnDisk after spill"
    );
    let prop_key = grafeo_common::types::PropertyKey::new(PROP);
    let drained = nodes.iter().all(|id| {
        source
            .get_node(*id)
            .and_then(|n| n.properties.get(&prop_key).cloned())
            .is_none()
    });
    assert!(drained, "inline embedding column must be drained by spill");
    // Search still works pre-publish via the spill-aware accessor.
    let pre = source
        .vector_search(LABEL, PROP, &seeded_vector(7, DIMS), 1, None, None)
        .expect("pre-publish search");
    assert_eq!(pre.first().map(|h| h.0), Some(nodes[7]));

    source
        .build_and_publish_generation(generation_build_request(&root, "srv1-gapb-fd-g1"))
        .expect("publish generation");
    drop(source);

    // The generation root carries NO spill sidecar: the reopened RO DB must
    // serve the index purely from the published CompactStore column.
    let db = GrafeoDB::open_generation_root(&root, true).expect("open generation root RO");
    assert_health_probes_see_vectors(&db, NODES as i64);
    assert_k1_serves(&db, &nodes, 7);
}

#[test]
fn published_scalar_quantized_vector_index_serves_after_ro_reopen() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("srv1-gapb-quant.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    let source = GrafeoDB::new_in_memory();
    let nodes = seed_source_with_quantization(&source, Some("scalar"));
    assert_eq!(
        source.vector_index_quantization(LABEL, PROP),
        Some(grafeo_core::index::vector::QuantizationType::Scalar),
        "scalar quantization must be registered pre-publish"
    );
    source
        .build_and_publish_generation(generation_build_request(&root, "srv1-gapb-quant-g1"))
        .expect("publish generation");
    drop(source);

    let db = GrafeoDB::open_generation_root(&root, true).expect("open generation root RO");
    assert_eq!(
        db.vector_index_quantization(LABEL, PROP),
        Some(grafeo_core::index::vector::QuantizationType::Scalar),
        "quantization mode must survive RO reopen (sticky catalog)"
    );
    assert_health_probes_see_vectors(&db, NODES as i64);
    assert_k1_serves(&db, &nodes, 7);
}
