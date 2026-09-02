//! Disk reopen tests for durable vector index quantization (V2-QUANT).
//!
//! Layer 2 acceptance:
//! create(mode) → insert vectors → checkpoint → close → reopen →
//! inspect mode matches + search returns the seeded top-1 neighbor.
//!
//! Product quant is schema-supported; reopen oracle deferred for cost
//! (schema field still round-trips via Catalog unit tests).

#![cfg(all(feature = "vector-index", feature = "grafeo-file", feature = "lpg"))]

use grafeo_common::storage::section::Section;
use grafeo_common::types::Value;
use grafeo_core::index::vector::{
    DistanceMetric, HnswConfig, HnswIndex, QuantizationType, VectorIndexKind, VectorStoreSection,
};
use grafeo_engine::{Config, GrafeoDB};
use std::sync::Arc;

use bytes::Bytes;

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

fn reopen_mode_and_search(mode: Option<&str>, expected: QuantizationType) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    // Prefer /data/tmp when TMPDIR is set by the host; tempfile honors TMPDIR.
    let path = dir
        .path()
        .join(format!("v2_quant_{}.grafeo", expected.name()));
    let dim = 16;
    let n = 40;
    let query = seeded_vector(0, dim);

    let (top1_before, heap_before) = {
        let db = GrafeoDB::with_config(Config::persistent(&path)).expect("create db");
        let mut first_id = None;
        for i in 0..n {
            let node = db.create_node(&["Doc"]).expect("node");
            if i == 0 {
                first_id = Some(node);
            }
            db.set_node_property(
                node,
                "emb",
                Value::Vector(seeded_vector(i as u64, dim).into()),
            )
            .expect("emb");
        }
        db.create_vector_index("Doc", "emb", Some(dim), Some("cosine"), None, None, mode)
            .expect("create vector index");

        assert!(db.has_vector_index("Doc", "emb"));
        assert_eq!(
            db.vector_index_quantization("Doc", "emb"),
            Some(expected),
            "inspect after create"
        );

        let results = db
            .vector_search("Doc", "emb", &query, 3, None, None)
            .expect("search before");
        assert!(!results.is_empty(), "search ready before close");
        let top1 = results[0].0;
        assert_eq!(top1, first_id.expect("first"), "query0 should prefer seed0");
        let heap = db
            .vector_index_heap_memory_bytes("Doc", "emb")
            .expect("heap");

        db.wal_checkpoint().expect("checkpoint");
        db.close().expect("close");
        (top1, heap)
    };

    let db = GrafeoDB::open(&path).expect("reopen");
    assert!(
        db.has_vector_index("Doc", "emb"),
        "index registered after reopen"
    );
    assert_eq!(
        db.vector_index_quantization("Doc", "emb"),
        Some(expected),
        "mode sticky after reopen (Catalog authority)"
    );

    let results = db
        .vector_search("Doc", "emb", &query, 3, None, None)
        .expect("search after reopen");
    assert!(!results.is_empty(), "search ready after reopen");
    assert_eq!(
        results[0].0, top1_before,
        "top-1 neighbor identity preserved for synthetic fixture"
    );

    let heap_after = db
        .vector_index_heap_memory_bytes("Doc", "emb")
        .expect("heap after");
    // Not a pass/fail RSS gate — record order-of-magnitude sanity only.
    assert!(
        heap_after > 0 && heap_before > 0,
        "heap helpers must report non-zero after rehydrate/create (before={heap_before} after={heap_after})"
    );

    db.close().ok();
}

#[test]
fn quant_reopen_none_mode() {
    reopen_mode_and_search(None, QuantizationType::None);
    reopen_mode_and_search(Some("none"), QuantizationType::None);
}

#[test]
fn quant_reopen_scalar_mode() {
    reopen_mode_and_search(Some("scalar"), QuantizationType::Scalar);
}

#[test]
fn quant_reopen_binary_mode() {
    reopen_mode_and_search(Some("binary"), QuantizationType::Binary);
}

#[test]
fn empty_vector_index_survives_checkpoint_and_reopen() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = dir.path().join("empty_vector_index.grafeo");

    {
        let db = GrafeoDB::with_config(Config::persistent(&path)).expect("create db");
        db.create_vector_index(
            "SessionSummary",
            "embedding",
            Some(16),
            Some("cosine"),
            None,
            None,
            None,
        )
        .expect("create empty vector index");
        assert!(db.has_vector_index("SessionSummary", "embedding"));
        db.wal_checkpoint().expect("checkpoint");
        db.close().expect("close");
    }

    let db = GrafeoDB::open(&path).expect("reopen empty vector index");
    assert!(
        db.has_vector_index("SessionSummary", "embedding"),
        "empty vector index remains registered after reopen"
    );
    assert_eq!(
        db.vector_index_quantization("SessionSummary", "embedding"),
        Some(QuantizationType::None),
        "empty vector index retains its catalog mode"
    );
    let results = db
        .vector_search("SessionSummary", "embedding", &[0.0; 16], 1, None, None)
        .expect("search empty vector index");
    assert!(results.is_empty(), "empty vector index returns no hits");
    db.close().expect("close reopened db");
}

/// Builds a v2 VectorStore envelope whose single topology blob declares an
/// entry point (`has_entry_point = 1`) but contains zero nodes — a state
/// legitimate writers never produce (the entry point is set on first insert
/// and cleared on last removal). Used to prove the restore paths fail closed
/// on structurally inconsistent headers while still accepting valid empties
/// (`entry_point = None`, zero nodes).
fn v2_envelope_with_inconsistent_empty_topology() -> Vec<u8> {
    let config = HnswConfig::new(16, DistanceMetric::Cosine);
    let index = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config)));
    let section = VectorStoreSection::new(vec![("SessionSummary:embedding".to_string(), index)]);
    let mut bytes = section.serialize().expect("serialize empty topology");

    // Directory entry 0 sits at bytes 16..48: meta_off (16..24), meta_len
    // (24..32), topology_off (32..40), topology_len (40..48). An empty
    // topology serializes as a bare 32-byte GTOP header.
    let topo_off = u64::from_le_bytes(bytes[32..40].try_into().expect("dir offset")) as usize;
    let topo_len = u64::from_le_bytes(bytes[40..48].try_into().expect("dir len")) as usize;
    assert_eq!(topo_len, 32, "empty topology must be a bare GTOP header");
    assert_eq!(&bytes[topo_off..topo_off + 4], b"GTOP");

    // Patch the header: has_entry_point = 1, entry_point = 42, n_nodes = 0.
    bytes[topo_off + 5] = 1;
    bytes[topo_off + 24..topo_off + 32].copy_from_slice(&42u64.to_le_bytes());
    bytes
}

/// An inconsistent topology (entry point declared, zero nodes) must fail
/// closed on BOTH restore paths: the heap path used by writable open and
/// the mmap path used by read-only open.
#[test]
fn inconsistent_entry_point_with_zero_nodes_fails_closed() {
    let bytes = v2_envelope_with_inconsistent_empty_topology();

    // Heap restore (writable open path).
    let config = HnswConfig::new(16, DistanceMetric::Cosine);
    let heap_index = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config)));
    let mut heap_section =
        VectorStoreSection::new(vec![("SessionSummary:embedding".to_string(), heap_index)]);
    let heap_err = heap_section
        .deserialize(&bytes)
        .expect_err("heap restore must reject entry_point=Some with zero nodes");
    assert!(
        format!("{heap_err}").contains("entry point"),
        "heap error must name the inconsistency: {heap_err}"
    );

    // Mmap restore (read-only open path).
    let config = HnswConfig::new(16, DistanceMetric::Cosine);
    let mmap_index = Arc::new(VectorIndexKind::Hnsw(HnswIndex::new(config)));
    let mut mmap_section =
        VectorStoreSection::new(vec![("SessionSummary:embedding".to_string(), mmap_index)]);
    let mmap_err = mmap_section
        .restore_from_mapped_bytes(Bytes::from(bytes))
        .expect_err("mmap restore must reject entry_point=Some with zero nodes");
    assert!(
        format!("{mmap_err}").contains("entry point"),
        "mmap error must name the inconsistency: {mmap_err}"
    );
}

/// Empty scalar-quantized index (the production-default quantization mode)
/// survives checkpoint → close → reopen: registration and scalar mode stay
/// sticky and search on the empty index returns cleanly.
#[test]
fn empty_scalar_quantized_index_survives_checkpoint_and_reopen() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = dir.path().join("empty_scalar_vector_index.grafeo");

    {
        let db = GrafeoDB::with_config(Config::persistent(&path)).expect("create db");
        db.create_vector_index(
            "SessionSummary",
            "embedding",
            Some(16),
            Some("cosine"),
            None,
            None,
            Some("scalar"),
        )
        .expect("create empty scalar vector index");
        assert!(db.has_vector_index("SessionSummary", "embedding"));
        assert_eq!(
            db.vector_index_quantization("SessionSummary", "embedding"),
            Some(QuantizationType::Scalar),
            "scalar mode after create"
        );
        db.wal_checkpoint().expect("checkpoint");
        db.close().expect("close");
    }

    let db = GrafeoDB::open(&path).expect("reopen empty scalar vector index");
    assert!(
        db.has_vector_index("SessionSummary", "embedding"),
        "empty scalar vector index remains registered after reopen"
    );
    assert_eq!(
        db.vector_index_quantization("SessionSummary", "embedding"),
        Some(QuantizationType::Scalar),
        "empty scalar vector index retains its catalog mode after reopen"
    );
    let results = db
        .vector_search("SessionSummary", "embedding", &[0.0; 16], 1, None, None)
        .expect("search empty scalar vector index");
    assert!(
        results.is_empty(),
        "empty scalar vector index returns no hits"
    );
    db.close().expect("close reopened db");
}

#[test]
fn inspect_api_missing_vs_plain() {
    let db = GrafeoDB::new_in_memory();
    assert!(!db.has_vector_index("Doc", "emb"));
    assert_eq!(db.vector_index_quantization("Doc", "emb"), None);
    assert_eq!(db.vector_index_heap_memory_bytes("Doc", "emb"), None);

    let node = db.create_node(&["Doc"]).unwrap();
    db.set_node_property(node, "emb", Value::Vector(seeded_vector(1, 4).into()))
        .unwrap();
    db.create_vector_index("Doc", "emb", Some(4), Some("cosine"), None, None, None)
        .unwrap();
    assert!(db.has_vector_index("Doc", "emb"));
    assert_eq!(
        db.vector_index_quantization("Doc", "emb"),
        Some(QuantizationType::None)
    );
}
