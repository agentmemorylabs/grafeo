//! Release benchmark: row-oriented edge writes vs offline bulk edge loader.
//!
//! Compares `LpgStore::create_edge_with_props` (per-row) against
//! `LpgStore::bulk_create_edges_with_props_unindexed` (single bulk call),
//! isolating the storage-level difference without session/WAL overhead.
//! This mirrors the node bulk loader precedent (plan §5 performance gate).
//!
//! Run with the shared NVMe target + sccache (release):
//!
//! ```bash
//! CARGO_TARGET_DIR=/data/cargo-targets/jfrie-grafeo-em0-3a/target \
//! SCCACHE_DIR=/data/sccache TMPDIR=/data/tmp RUSTC_WRAPPER=sccache \
//!   cargo bench -p grafeo-engine --bench offline_bulk_bench
//! ```
// reason: criterion_group! expansion does not carry doc comments.
#![allow(missing_docs)]
// reason: benchmark loop counters are bounded by NODES/EDGES, fit i64.
#![allow(clippy::cast_possible_wrap)]

#[cfg(not(any(feature = "tiered-storage", feature = "temporal")))]
mod inner {
    use criterion::{Criterion, Throughput};
    use grafeo_common::types::{PropertyKey, TransactionId, Value};
    use grafeo_common::utils::hash::FxHashMap;
    use grafeo_core::graph::lpg::LpgStore;

    /// Nodes per benchmark iteration.
    const NODES: usize = 5_000;
    /// Edges created (chained), per iteration.
    const EDGES: usize = 5_000;

    pub fn bench_edge_row_vs_bulk(c: &mut Criterion) {
        let mut group = c.benchmark_group("offline_bulk_edge_write");
        group.throughput(Throughput::Elements(EDGES as u64));
        group.sample_size(20);

        let tx = TransactionId::new(7);

        group.bench_function("row_oriented_edges", |b| {
            b.iter(|| {
                let store = LpgStore::new().unwrap();
                let epoch = store.new_epoch();
                let node_ids: Vec<_> = (0..NODES)
                    .map(|_| store.create_node_versioned(&["N"], epoch, tx))
                    .collect();

                for i in 0..EDGES {
                    let src = node_ids[i % NODES];
                    let dst = node_ids[(i + 1) % NODES];
                    store.create_edge_with_props(src, dst, "NEXT", [("w", Value::from(i as i64))]);
                }
                std::hint::black_box(&node_ids);
            });
        });

        group.bench_function("bulk_edges_with_props", |b| {
            b.iter(|| {
                let store = LpgStore::new().unwrap();
                let epoch = store.new_epoch();
                let node_ids: Vec<_> = (0..NODES)
                    .map(|_| store.create_node_versioned(&["N"], epoch, tx))
                    .collect();

                let edge_specs: Vec<(
                    grafeo_common::types::NodeId,
                    grafeo_common::types::NodeId,
                    &str,
                    FxHashMap<PropertyKey, Value>,
                )> = (0..EDGES)
                    .map(|i| {
                        let src = node_ids[i % NODES];
                        let dst = node_ids[(i + 1) % NODES];
                        let props = FxHashMap::from_iter([
                            (PropertyKey::new("w"), Value::from(i as i64)),
                            (PropertyKey::new("idx"), Value::from(i as i64)),
                        ]);
                        (src, dst, "NEXT", props)
                    })
                    .collect();

                let eids = store
                    .bulk_create_edges_with_props_unindexed(&edge_specs)
                    .expect("bulk edges");
                std::hint::black_box(eids);
            });
        });

        group.finish();
    }
}

#[cfg(not(any(feature = "tiered-storage", feature = "temporal")))]
use inner::bench_edge_row_vs_bulk;

#[cfg(not(any(feature = "tiered-storage", feature = "temporal")))]
criterion::criterion_group!(benches, bench_edge_row_vs_bulk);

#[cfg(not(any(feature = "tiered-storage", feature = "temporal")))]
criterion::criterion_main!(benches);

#[cfg(any(feature = "tiered-storage", feature = "temporal"))]
fn main() {
    eprintln!("offline_bulk_bench: not available with tiered-storage or temporal");
}
