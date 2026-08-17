//! G-VECBUILD.1 acceptance — production-scale RO reopen k=1 parity probe.
//!
//! Opens the REAL published generation root (SRV1 Gap B shape) read-only and
//! proves the served HNSW index returns each sampled node's own vector for a
//! k=1 ANN query built from the node's stored embedding. Env-gated: the
//! generation root path comes from `AM_VECBUILD_REOPEN_PROBE_ROOT`; the test
//! is skipped (not failed) when the env var is absent, so normal CI never
//! touches production artifacts.
//!
//! ```bash
//! AM_VECBUILD_REOPEN_PROBE_ROOT=/data/tmp/g-vecbuild-1-accept/code-index/am-personal/agent-memory-hosted/agent-memory-hosted.grafeo.d \
//! cargo test -p grafeo-engine --test vecbuild_reopen_probe \
//!   --features "lpg,vector-index,mmap,compact-store,generation,generation-streaming,grafeo-file" -- --nocapture
//! ```

#![cfg(all(
    feature = "lpg",
    feature = "vector-index",
    feature = "mmap",
    feature = "compact-store",
    feature = "generation",
    feature = "generation-streaming",
    feature = "grafeo-file",
    not(feature = "temporal")
))]

use grafeo_engine::GrafeoDB;

const LABEL: &str = "RetrievalUnit";
const PROP: &str = "embedding";
const SAMPLE: usize = 10;

#[test]
fn production_reopen_k1_parity_probe() {
    let Some(root) = std::env::var_os("AM_VECBUILD_REOPEN_PROBE_ROOT") else {
        eprintln!("AM_VECBUILD_REOPEN_PROBE_ROOT unset — skipping production probe");
        return;
    };

    let db = GrafeoDB::open_generation_root(std::path::PathBuf::from(root), true)
        .expect("RO reopen of published generation root");
    assert!(db.has_vector_index(LABEL, PROP), "index survives reopen");

    let nodes = db.graph_store().nodes_by_label(LABEL);
    assert!(
        nodes.len() > SAMPLE,
        "generation holds the frontier RetrievalUnit population: {}",
        nodes.len()
    );

    // Stride-sample across the population (start, mid, end).
    let stride = nodes.len() / SAMPLE;
    let mut probed = 0usize;
    for i in 0..SAMPLE {
        let id = nodes[i * stride];
        let vector = match db
            .read_indexed_node_vector(LABEL, PROP, id)
            .unwrap_or_else(|e| panic!("exact vector read for node {}: {e}", id.0))
        {
            grafeo_engine::IndexedVectorRead::Found(v) => v,
            other => panic!("node {} not index-readable: {other:?}", id.0),
        };
        let hits = db
            .vector_search(LABEL, PROP, &vector, 1, None, None)
            .unwrap_or_else(|e| panic!("k=1 search for node {}: {e}", id.0));
        assert_eq!(
            hits.first().map(|h| h.0),
            Some(id),
            "k=1 probe must return the exact vector's own node (sample {i})"
        );
        probed += 1;
    }
    println!(
        "REOPEN PROBE OK: {} RetrievalUnit nodes, {} k=1 probes passed",
        nodes.len(),
        probed
    );
}
