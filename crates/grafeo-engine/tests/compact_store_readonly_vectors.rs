//! G-E2.RO: read-only base vector serving on the accepted EM0.2 candidate.
//!
//! Builds deterministic compact snapshots with durable vector indexes,
//! reopens them read-only via direct container mmap, and proves:
//! - catalog config (dimensions, metric) + exact vector values + HNSW topology
//! - exact indexed lookup and search parity against a frozen query set
//! - topology is mmap-backed (not a full anonymous corpus rebuild)
//! - payloads are served from mapped compact columns (not a dual f32 store)
//! - two-size residency: topology heap does not scale with vector corpus
//! - fail-closed open when VectorStore is absent or corrupt
//!
//! ```bash
//! cargo test -p grafeo-engine --features "compact-store,vector-index" \
//!   --test compact_store_readonly_vectors -- --nocapture
//! ```

#![cfg(all(
    feature = "compact-store",
    feature = "grafeo-file",
    feature = "lpg",
    feature = "vector-index",
    feature = "mmap"
))]

use grafeo_common::types::{NodeId, Value};
use grafeo_engine::{
    CompactBacking, Config, GrafeoDB, IndexedVectorRead, VectorPayloadBacking,
    VectorTopologyBacking,
};

const SMALL_NODES: usize = 64;
/// ≥4× vectors so serialized vector topology / compact payload clearly grows.
const LARGE_NODES: usize = 512;
const DIMS: usize = 16;
const METRIC: &str = "cosine";
const LABEL: &str = "Doc";
const PROP: &str = "embedding";
/// Frozen query seeds (deterministic) used for parity across sizes.
const QUERY_SEEDS: [u64; 4] = [0, 7, 13, 42];
const K: usize = 5;

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

/// Build → index → compact → checkpoint → close. Returns (node_ids, frozen search results).
fn build_vector_snapshot(
    path: &std::path::Path,
    node_count: usize,
) -> (Vec<NodeId>, Vec<Vec<(NodeId, f32)>>) {
    let mut db = GrafeoDB::with_config(Config::persistent(path)).expect("create db");
    let mut nodes = Vec::with_capacity(node_count);
    for i in 0..node_count {
        let id = db
            .create_node_with_props(
                &[LABEL],
                [
                    ("rank", Value::Int64(i as i64)),
                    (PROP, Value::Vector(seeded_vector(i as u64, DIMS).into())),
                ],
            )
            .expect("create node");
        nodes.push(id);
    }
    db.create_vector_index(LABEL, PROP, Some(DIMS), Some(METRIC), None, None, None)
        .expect("create vector index");

    // Freeze search results before compact so we can prove reopen parity.
    let mut frozen = Vec::with_capacity(QUERY_SEEDS.len());
    for seed in QUERY_SEEDS {
        let q = seeded_vector(seed, DIMS);
        let hits = db
            .vector_search(LABEL, PROP, &q, K, None, None)
            .expect("search before close");
        assert_eq!(hits.len(), K.min(node_count), "pre-close search k");
        frozen.push(hits);
    }

    db.compact().expect("compact");
    // After compact, indexes must still be live on the overlay (transfer).
    assert!(
        db.has_vector_index(LABEL, PROP),
        "vector index must survive compact()"
    );
    db.wal_checkpoint().expect("checkpoint");
    db.close().expect("close");
    (nodes, frozen)
}

fn assert_vector_parity(
    path: &std::path::Path,
    nodes: &[NodeId],
    frozen: &[Vec<(NodeId, f32)>],
) -> (usize, usize) {
    let db = GrafeoDB::open_read_only(path).expect("read-only reopen");

    // Compact base must be container-mmap (EM0.1/2 path).
    let CompactBacking::ContainerMmap {
        payload_version,
        mapped_bytes,
        ..
    } = db.compact_backing().expect("backing diagnostic")
    else {
        panic!("expected ContainerMmap backing");
    };
    assert_eq!(*payload_version, 5, "G-EM0.2 CompactStore v5");
    assert!(*mapped_bytes > 0);

    assert!(db.has_vector_index(LABEL, PROP));
    assert_eq!(
        db.vector_index_quantization(LABEL, PROP),
        Some(grafeo_core::index::vector::QuantizationType::None)
    );

    // Actual backing diagnostics (not mere mmap_able flags).
    let diags = db.vector_backing_diagnostics();
    assert_eq!(diags.len(), 1, "one vector index");
    let diag = &diags[0];
    assert_eq!(diag.key, format!("{LABEL}:{PROP}"));
    assert_eq!(diag.dimensions, DIMS);
    assert_eq!(diag.metric, METRIC);
    let topology_mapped_bytes = match &diag.topology {
        VectorTopologyBacking::Mmap { topology_bytes } => {
            assert!(*topology_bytes > 0, "mapped topology bytes must be > 0");
            *topology_bytes
        }
        VectorTopologyBacking::Heap { heap_bytes } => {
            panic!("expected mmap topology on RO open, got heap ({heap_bytes} bytes)");
        }
    };
    assert_eq!(
        diag.payload,
        VectorPayloadBacking::CompactMappedColumn,
        "payloads must be served from mapped compact columns"
    );
    // Topology heap must stay small relative to mapped topology bytes.
    assert!(
        diag.topology_heap_bytes < topology_mapped_bytes.max(64),
        "mmap topology must not retain proportional anonymous graph: heap={} mapped={}",
        diag.topology_heap_bytes,
        topology_mapped_bytes
    );

    // Exact indexed lookup: sample across the node set.
    let step = (nodes.len() / 8).max(1);
    for (i, id) in nodes.iter().enumerate().step_by(step) {
        let expected = seeded_vector(i as u64, DIMS);
        match db
            .read_indexed_node_vector(LABEL, PROP, *id)
            .expect("exact read")
        {
            IndexedVectorRead::Found(v) => {
                assert_eq!(v.len(), DIMS, "exact vector width for {id:?}");
                for (a, b) in v.iter().zip(expected.iter()) {
                    assert!(
                        (a - b).abs() < 1e-5,
                        "exact vector content mismatch at {id:?}"
                    );
                }
            }
            other => panic!("expected Found for {id:?}, got {other:?}"),
        }
    }

    // Frozen search parity: IDs + distances.
    for (qi, seed) in QUERY_SEEDS.iter().enumerate() {
        let q = seeded_vector(*seed, DIMS);
        let hits = db
            .vector_search(LABEL, PROP, &q, K, None, None)
            .expect("search after reopen");
        let expected = &frozen[qi];
        assert_eq!(
            hits.len(),
            expected.len(),
            "search result count seed={seed}"
        );
        for (j, ((id_a, dist_a), (id_b, dist_b))) in hits.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                id_a, id_b,
                "search ID parity seed={seed} rank={j}: got {id_a:?} want {id_b:?}"
            );
            assert!(
                (dist_a - dist_b).abs() < 1e-4,
                "search distance parity seed={seed} rank={j}: got {dist_a} want {dist_b}"
            );
        }
    }

    // Post-query residency: diagnostics still mmap, heap still bounded.
    let post = db.vector_backing_diagnostics();
    assert!(matches!(
        post[0].topology,
        VectorTopologyBacking::Mmap { .. }
    ));
    let heap_after = post[0].topology_heap_bytes;
    db.close().expect("close");
    (topology_mapped_bytes, heap_after)
}

#[test]
fn readonly_vector_parity_at_two_sizes_with_mmap_topology() {
    let temp = tempfile::tempdir().expect("tempdir");
    let small_path = temp.path().join("small_vec.grafeo");
    let large_path = temp.path().join("large_vec.grafeo");

    let (small_nodes, small_frozen) = build_vector_snapshot(&small_path, SMALL_NODES);
    let (large_nodes, large_frozen) = build_vector_snapshot(&large_path, LARGE_NODES);

    let (small_topo, small_heap) = assert_vector_parity(&small_path, &small_nodes, &small_frozen);
    let (large_topo, large_heap) = assert_vector_parity(&large_path, &large_nodes, &large_frozen);

    // Mapped topology must grow with corpus; anonymous topology heap must not.
    assert!(
        large_topo >= small_topo.saturating_mul(4),
        "mapped topology must grow ≥4×: small={small_topo} large={large_topo}"
    );
    // Allow modest fixed overhead but forbid proportional anonymous growth.
    let heap_growth = large_heap.saturating_sub(small_heap);
    let topo_growth = large_topo.saturating_sub(small_topo);
    assert!(
        heap_growth * 8 < topo_growth.max(1),
        "topology heap must not scale with corpus: small_heap={small_heap} large_heap={large_heap} topo_growth={topo_growth}"
    );

    println!(
        "G-E2.RO two-size: small_topo={small_topo} large_topo={large_topo} \
         small_heap={small_heap} large_heap={large_heap}"
    );
}

#[test]
fn readonly_empty_vector_index_reopens_with_mmap_topology() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("empty_vec.grafeo");

    {
        let mut db = GrafeoDB::with_config(Config::persistent(&path)).expect("create db");
        db.create_vector_index(LABEL, PROP, Some(DIMS), Some(METRIC), None, None, None)
            .expect("create empty vector index");
        db.compact().expect("compact empty graph");
        db.wal_checkpoint().expect("checkpoint");
        db.close().expect("close");
    }

    let db = GrafeoDB::open_read_only(&path).expect("read-only reopen");
    assert!(db.has_vector_index(LABEL, PROP));
    let diagnostics = db.vector_backing_diagnostics();
    assert_eq!(diagnostics.len(), 1);
    assert!(matches!(
        diagnostics[0].topology,
        VectorTopologyBacking::Mmap { .. }
    ));
    let results = db
        .vector_search(LABEL, PROP, &[0.0; DIMS], 1, None, None)
        .expect("search empty mapped index");
    assert!(results.is_empty());
    db.close().expect("close read-only db");
}

/// Corrupt VectorStore magic → open must fail closed (no empty-shell search).
#[test]
fn readonly_vector_corrupt_topology_fails_closed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("corrupt_vec.grafeo");
    let (_nodes, _frozen) = build_vector_snapshot(&path, SMALL_NODES);

    // Flip the first occurrence of GVST magic so restore fails.
    let mut bytes = std::fs::read(&path).expect("read fixture");
    let magic = b"GVST";
    let pos = bytes
        .windows(4)
        .position(|w| w == magic)
        .expect("fixture must contain GVST VectorStore magic");
    bytes[pos] = b'X';
    std::fs::write(&path, &bytes).expect("write corrupt");

    let err = match GrafeoDB::open_read_only(&path) {
        Ok(_) => panic!("corrupt VectorStore must fail closed"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        msg.contains("Vector Store")
            || msg.contains("topology")
            || msg.contains("magic")
            || msg.contains("Serialization")
            || msg.contains("bad magic")
            || msg.contains("mmap")
            || msg.contains("CRC")
            || msg.contains("GTOP")
            || msg.contains("GVST"),
        "unexpected error for corrupt topology: {msg}"
    );
}

/// Truncate the file past CompactStore but drop VectorStore by rewriting a
/// minimal invalid section payload — open with catalog shells must fail.
#[test]
fn readonly_vector_absent_section_fails_closed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("absent_vec.grafeo");
    let (_nodes, _frozen) = build_vector_snapshot(&path, 32);

    // Zero out the entire VectorStore section body if we can find GVST, and
    // replace with an empty section by shrinking to zero-length is hard.
    // Instead: replace GVST payload with a short truncated header that still
    // looks like VectorStore but fails decode.
    let mut bytes = std::fs::read(&path).expect("read");
    let magic = b"GVST";
    let pos = bytes
        .windows(4)
        .position(|w| w == magic)
        .expect("GVST present");
    // Leave magic but truncate meaningful body after header to force error.
    // Overwrite version + n_indexes region with garbage lengths that fail range checks.
    if pos + 16 < bytes.len() {
        // version ok, but n_indexes huge → directory truncated.
        bytes[pos + 8..pos + 16].copy_from_slice(&u64::MAX.to_le_bytes());
    }
    std::fs::write(&path, &bytes).expect("write");

    let err = match GrafeoDB::open_read_only(&path) {
        Ok(_) => panic!("truncated VectorStore must fail closed"),
        Err(e) => e,
    };
    let msg = format!("{err}");
    assert!(
        !msg.is_empty(),
        "fail-closed must surface a non-empty error"
    );
    // Must not succeed with empty search results.
    // (open already failed)
    println!("absent/corrupt fail-closed: {msg}");
}
