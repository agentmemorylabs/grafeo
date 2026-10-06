//! Filtered vector search: results against the exact filtered top-k, for
//! plain and quantized HNSW, every quantization type and every filtered entry
//! point; plus an ignored release-mode latency report.

use super::hnsw::filtered_scan_stats;
use super::{
    DistanceMetric, FILTERED_EXACT_SCAN_THRESHOLD, HnswConfig, HnswIndex, QuantizationType,
    QuantizedHnswIndex, VectorAccessor, VectorIndexKind, brute_force_knn_filtered,
};
use grafeo_common::types::NodeId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// SplitMix64: small, fixed-seed generator so data, queries and allowlists
/// are reproducible without depending on `rand`'s stream stability.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in [-1, 1).
    fn next_f32(&mut self) -> f32 {
        ((self.next_u64() >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    }

    // reason: the result is below `n`, which is a usize
    #[allow(clippy::cast_possible_truncation)]
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

fn random_vectors(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng::new(seed);
    (0..n)
        .map(|_| (0..dim).map(|_| rng.next_f32()).collect())
        .collect()
}

fn node(i: usize) -> NodeId {
    NodeId::new(i as u64 + 1)
}

fn store(vectors: &[Vec<f32>]) -> HashMap<NodeId, Arc<[f32]>> {
    vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (node(i), Arc::from(v.as_slice())))
        .collect()
}

fn map_accessor(store: &HashMap<NodeId, Arc<[f32]>>) -> impl VectorAccessor + '_ {
    move |id: NodeId| store.get(&id).cloned()
}

/// `count` distinct ids drawn uniformly from the index.
fn random_allowlist(n: usize, count: usize, rng: &mut Rng) -> HashSet<NodeId> {
    let mut ids: Vec<usize> = (0..n).collect();
    for i in 0..count {
        let j = i + rng.below(n - i);
        ids.swap(i, j);
    }
    ids[..count].iter().map(|&i| node(i)).collect()
}

/// The `count` ids furthest from `query`: the allowlist sits in a region the
/// unfiltered beam never reaches (the shape of #30's reproduction).
fn far_allowlist(vectors: &[Vec<f32>], query: &[f32], count: usize) -> HashSet<NodeId> {
    let mut by_dist: Vec<(usize, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| {
            (
                i,
                super::compute_distance(query, v, DistanceMetric::Euclidean),
            )
        })
        .collect();
    by_dist.sort_by(|a, b| b.1.total_cmp(&a.1));
    by_dist[..count].iter().map(|&(i, _)| node(i)).collect()
}

fn exact(
    vectors: &[Vec<f32>],
    query: &[f32],
    k: usize,
    allowlist: &HashSet<NodeId>,
) -> Vec<NodeId> {
    brute_force_knn_filtered(
        vectors
            .iter()
            .enumerate()
            .map(|(i, v)| (node(i), v.as_slice())),
        query,
        k,
        DistanceMetric::Euclidean,
        |id| allowlist.contains(&id),
    )
    .into_iter()
    .map(|(id, _)| id)
    .collect()
}

fn ids(results: &[(NodeId, f32)]) -> Vec<NodeId> {
    results.iter().map(|(id, _)| *id).collect()
}

fn recall(got: &[NodeId], want: &[NodeId]) -> f64 {
    if want.is_empty() {
        return 1.0;
    }
    let want: HashSet<_> = want.iter().collect();
    got.iter().filter(|id| want.contains(id)).count() as f64 / want.len() as f64
}

const QUANTIZATIONS: [QuantizationType; 4] = [
    QuantizationType::None,
    QuantizationType::Scalar,
    QuantizationType::Binary,
    QuantizationType::Product { num_subvectors: 4 },
];

/// How the index gets its vectors.
#[derive(Clone, Copy, Debug)]
enum Load {
    /// `insert` with an external accessor: the production path (no codes
    /// retained, search reads vectors through the accessor).
    Production,
    /// `test_insert`: trains the quantizer and keeps codes, exercising the
    /// code-scoring and rescoring branches.
    Codes,
}

fn build_quantized(
    vectors: &[Vec<f32>],
    q: QuantizationType,
    load: Load,
    seed: u64,
    acc: &impl VectorAccessor,
) -> QuantizedHnswIndex {
    let config = HnswConfig::new(vectors[0].len(), DistanceMetric::Euclidean);
    let index = QuantizedHnswIndex::with_seed(config, q, seed).with_training_threshold(256);
    for (i, v) in vectors.iter().enumerate() {
        match load {
            Load::Production => index.insert(node(i), v, acc),
            Load::Codes => index.test_insert(node(i), v),
        }
    }
    index
}

fn build_plain(vectors: &[Vec<f32>], seed: u64, acc: &impl VectorAccessor) -> HnswIndex {
    let config = HnswConfig::new(vectors[0].len(), DistanceMetric::Euclidean);
    let index = HnswIndex::with_seed(config, seed);
    for (i, v) in vectors.iter().enumerate() {
        index.insert(node(i), v, acc);
    }
    index
}

/// Every filtered entry point of a `VectorIndexKind`, single-query results.
fn all_entry_points(
    kind: &VectorIndexKind,
    query: &[f32],
    k: usize,
    allowlist: &HashSet<NodeId>,
    acc: &impl VectorAccessor,
) -> [(&'static str, Vec<NodeId>); 4] {
    let queries = vec![query.to_vec()];
    [
        (
            "search_with_filter",
            ids(&kind.search_with_filter(query, k, allowlist, acc)),
        ),
        (
            "search_with_ef_and_filter",
            ids(&kind.search_with_ef_and_filter(query, k, 64, allowlist, acc)),
        ),
        (
            "batch_search_with_filter",
            ids(&kind.batch_search_with_filter(&queries, k, allowlist, acc)[0]),
        ),
        (
            "batch_search_with_ef_and_filter",
            ids(&kind.batch_search_with_ef_and_filter(&queries, k, 64, allowlist, acc)[0]),
        ),
    ]
}

/// #30's reproduction, extended: 30 vectors, query = vector 0, allowlist =
/// the ten furthest ids, k = 5. Before the fix every quantized entry point
/// returned nothing.
#[test]
fn far_allowlist_returns_min_k_for_every_kind_and_entry_point() {
    let vectors = random_vectors(30, 4, 7);
    let st = store(&vectors);
    let acc = map_accessor(&st);
    let query = vectors[0].clone();
    let allowlist = far_allowlist(&vectors, &query, 10);
    let expected = exact(&vectors, &query, 5, &allowlist);
    assert_eq!(expected.len(), 5);

    let mut kinds: Vec<(String, VectorIndexKind)> =
        vec![("Hnsw".into(), build_plain(&vectors, 42, &acc).into())];
    for q in QUANTIZATIONS {
        for load in [Load::Production, Load::Codes] {
            kinds.push((
                format!("{q:?}/{load:?}"),
                build_quantized(&vectors, q, load, 42, &acc).into(),
            ));
        }
    }
    for (name, kind) in &kinds {
        for (entry, got) in all_entry_points(kind, &query, 5, &allowlist, &acc) {
            assert_eq!(got, expected, "{name} {entry}");
        }
    }
}

#[test]
fn filtered_edge_cases() {
    let vectors = random_vectors(200, 8, 11);
    let st = store(&vectors);
    let acc = map_accessor(&st);
    let query = vectors[3].clone();

    let mut kinds: Vec<(String, VectorIndexKind)> =
        vec![("Hnsw".into(), build_plain(&vectors, 1, &acc).into())];
    for q in QUANTIZATIONS {
        kinds.push((
            format!("{q:?}"),
            build_quantized(&vectors, q, Load::Codes, 1, &acc).into(),
        ));
    }

    let empty = HashSet::new();
    let larger_than_k: HashSet<NodeId> = (0..40).map(node).collect();
    let not_indexed: HashSet<NodeId> = (1000..1010).map(NodeId::new).collect();
    let mixed: HashSet<NodeId> = (0..3)
        .map(node)
        .chain((1000..1010).map(NodeId::new))
        .collect();
    let three: HashSet<NodeId> = [node(150), node(20), node(99)].into_iter().collect();

    for (name, kind) in &kinds {
        for (entry, got) in all_entry_points(kind, &query, 5, &empty, &acc) {
            assert!(got.is_empty(), "{name} {entry}: empty allowlist");
        }
        for (entry, got) in all_entry_points(kind, &query, 5, &larger_than_k, &acc) {
            assert_eq!(
                got,
                exact(&vectors, &query, 5, &larger_than_k),
                "{name} {entry}: allowlist larger than k"
            );
        }
        for (entry, got) in all_entry_points(kind, &query, 5, &not_indexed, &acc) {
            assert!(got.is_empty(), "{name} {entry}: ids not in the index");
        }
        for (entry, got) in all_entry_points(kind, &query, 5, &mixed, &acc) {
            assert_eq!(
                got,
                exact(&vectors, &query, 5, &mixed),
                "{name} {entry}: only indexed ids come back"
            );
        }
        for (entry, got) in all_entry_points(kind, &query, 10, &three, &acc) {
            assert_eq!(
                got,
                exact(&vectors, &query, 10, &three),
                "{name} {entry}: k larger than the allowlist returns all allowed"
            );
            assert_eq!(got.len(), 3);
        }
        assert!(
            kind.search_with_filter(&query, 0, &larger_than_k, &acc)
                .is_empty(),
            "{name}: k = 0"
        );
    }
}

/// Mean recall@k of unfiltered `search` over random queries: the accuracy a
/// filtered traversal can be expected to match.
fn unfiltered_recall(
    kind: &VectorIndexKind,
    vectors: &[Vec<f32>],
    k: usize,
    acc: &impl VectorAccessor,
) -> f64 {
    let all: HashSet<NodeId> = (0..vectors.len()).map(node).collect();
    let dim = vectors[0].len();
    let mut total = 0.0;
    for seed in 0..50 {
        let mut rng = Rng::new(seed + 10_000);
        let query: Vec<f32> = (0..dim).map(|_| rng.next_f32()).collect();
        total += recall(
            &ids(&kind.search(&query, k, acc)),
            &exact(vectors, &query, k, &all),
        );
    }
    total / 50.0
}

/// Seeded recall against the exact filtered top-k at about 1%, 10%, 33% and
/// 90% selectivity, for random allowlists and far-region ones (the allowlist
/// is the part of the index furthest from the query). 200 seeds per case.
///
/// * Default threshold: every allowlist here (at most 900 ids) takes the exact
///   scan, so results must equal the exact filtered top-k.
/// * Threshold lowered to 64 on this thread: the 10%, 33% and 90% allowlists
///   go through the in-traversal filter and must never come back short, with
///   mean recall at least 0.95 (0.5 where the unfiltered pipeline is itself
///   approximate: product quantization with codes).
#[test]
fn seeded_filtered_recall_matches_exact() {
    const N: usize = 1000;
    const DIM: usize = 16;
    const K: usize = 10;
    const SEEDS: u64 = 200;
    let vectors = random_vectors(N, DIM, 2026);
    let st = store(&vectors);
    let acc = map_accessor(&st);

    let mut kinds: Vec<(String, VectorIndexKind)> =
        vec![("Hnsw".into(), build_plain(&vectors, 5, &acc).into())];
    for q in QUANTIZATIONS {
        kinds.push((
            format!("{q:?}/Production"),
            build_quantized(&vectors, q, Load::Production, 5, &acc).into(),
        ));
        kinds.push((
            format!("{q:?}/Codes"),
            build_quantized(&vectors, q, Load::Codes, 5, &acc).into(),
        ));
    }
    let baselines: Vec<f64> = kinds
        .iter()
        .map(|(_, kind)| unfiltered_recall(kind, &vectors, K, &acc))
        .collect();

    for pct in [1usize, 10, 33, 90] {
        let count = N * pct / 100;
        for far in [false, true] {
            let cases: Vec<(Vec<f32>, HashSet<NodeId>, Vec<NodeId>)> = (0..SEEDS)
                .map(|seed| {
                    let mut rng = Rng::new(seed * 7919 + pct as u64);
                    let query: Vec<f32> = (0..DIM).map(|_| rng.next_f32()).collect();
                    let allowlist = if far {
                        far_allowlist(&vectors, &query, count)
                    } else {
                        random_allowlist(N, count, &mut rng)
                    };
                    let want = exact(&vectors, &query, K, &allowlist);
                    (query, allowlist, want)
                })
                .collect();
            for ((name, kind), baseline) in kinds.iter().zip(&baselines) {
                for threshold in [FILTERED_EXACT_SCAN_THRESHOLD, 64] {
                    let exact_expected = count <= threshold;
                    let mut total_recall = 0.0;
                    filtered_scan_stats::with_threshold(threshold, || {
                        for (seed, (query, allowlist, want)) in cases.iter().enumerate() {
                            let single = ids(&kind.search_with_filter(query, K, allowlist, &acc));
                            let with_ef =
                                ids(&kind.search_with_ef_and_filter(query, K, 50, allowlist, &acc));
                            for (entry, got) in
                                [("search_with_filter", &single), ("with_ef", &with_ef)]
                            {
                                let ctx = format!(
                                    "{name} {pct}% far={far} threshold={threshold} seed={seed} {entry}"
                                );
                                assert_eq!(got.len(), want.len(), "{ctx}: short result");
                                assert!(got.iter().all(|id| allowlist.contains(id)), "{ctx}");
                                if exact_expected {
                                    assert_eq!(got, want, "{ctx}: small allowlist must be exact");
                                }
                                total_recall += recall(got, want);
                            }
                        }
                    });
                    let mean = total_recall / (2 * cases.len()) as f64;
                    // Product quantization with codes ranks candidates by PQ
                    // distance and truncates to k before rescoring (unchanged
                    // here; production `insert` keeps no PQ codes), so it is
                    // approximate even unfiltered, and more so when all the
                    // allowlisted candidates sit at similar distances.
                    let bound = if *baseline >= 0.95 { 0.95 } else { 0.5 };
                    assert!(
                        mean >= bound,
                        "{name} {pct}% far={far} threshold={threshold}: mean recall@{K} \
                         {mean:.3} below {bound:.3} (unfiltered {baseline:.3})"
                    );
                }
            }
        }
    }
}

/// Large allowlists keep using the graph: no exact scan of either kind for
/// random allowlists at 33% and 90% selectivity once they are above the
/// threshold (lowered here so a small index has "large" allowlists).
/// Allowlists at or below the default threshold take the scan.
#[test]
fn large_allowlists_use_the_index_not_a_scan() {
    const N: usize = 2000;
    const THRESHOLD: usize = 64;
    let vectors = random_vectors(N, 16, 99);
    let st = store(&vectors);
    let acc = map_accessor(&st);
    let plain = build_plain(&vectors, 3, &acc);
    let quantized = build_quantized(
        &vectors,
        QuantizationType::Scalar,
        Load::Production,
        3,
        &acc,
    );

    filtered_scan_stats::with_threshold(THRESHOLD, || {
        for pct in [33usize, 90] {
            let count = N * pct / 100;
            for seed in 0..50u64 {
                let mut rng = Rng::new(seed);
                let query: Vec<f32> = (0..16).map(|_| rng.next_f32()).collect();
                let allowlist = random_allowlist(N, count, &mut rng);
                let before = filtered_scan_stats::get();
                assert_eq!(
                    plain.search_with_filter(&query, 10, &allowlist, &acc).len(),
                    10
                );
                assert_eq!(
                    plain
                        .search_with_ef_and_filter(&query, 10, 50, &allowlist, &acc)
                        .len(),
                    10
                );
                assert_eq!(
                    quantized
                        .search_with_filter(&query, 10, &allowlist, &acc)
                        .len(),
                    10
                );
                assert_eq!(
                    quantized
                        .search_with_ef_and_filter(&query, 10, 50, &allowlist, &acc)
                        .len(),
                    10
                );
                assert_eq!(
                    filtered_scan_stats::get(),
                    before,
                    "{pct}% seed={seed}: large allowlist fell back to an exact scan"
                );
            }
        }
    });

    // At the default threshold, the same 33% allowlist (660 ids) is small.
    let allowlist = random_allowlist(N, N / 3, &mut Rng::new(1));
    assert!(allowlist.len() <= FILTERED_EXACT_SCAN_THRESHOLD);
    let (small_before, _) = filtered_scan_stats::get();
    let _ = plain.search_with_filter(&vectors[0], 10, &allowlist, &acc);
    let _ = quantized.search_with_filter(&vectors[0], 10, &allowlist, &acc);
    assert_eq!(filtered_scan_stats::get().0, small_before + 2);
}

/// Shortfall fallback, both index kinds: the layer-0 graph has two components
/// and the entry point is in the one with no allowlisted nodes, so the
/// traversal finds nothing. The search must fall back to the exact scan and
/// return the true filtered top-k rather than an empty list.
#[test]
fn unreachable_allowlist_falls_back_to_exact_scan() {
    const N: usize = 400;
    const HALF: usize = N / 2;
    let vectors = random_vectors(N, 8, 5);
    let st = store(&vectors);
    let acc = map_accessor(&st);
    // Component A = ids of 0..HALF, component B = ids of HALF..N, each a ring.
    // |B| = 200 is above both visit estimates (50 / 0.5 and 20 / 0.5) and the
    // lowered threshold, so the traversal runs first.
    let topology: Vec<(NodeId, Vec<Vec<NodeId>>)> = (0..N)
        .map(|i| {
            let (lo, hi) = if i < HALF { (0, HALF) } else { (HALF, N) };
            let next = lo + (i - lo + 1) % (hi - lo);
            let prev = lo + (i - lo + hi - lo - 1) % (hi - lo);
            (node(i), vec![vec![node(next), node(prev)]])
        })
        .collect();
    let config = HnswConfig::new(8, DistanceMetric::Euclidean);
    let plain = HnswIndex::with_seed(config.clone(), 1);
    plain.restore_topology(Some(node(0)), 0, topology.clone());
    let mut kinds: Vec<(String, VectorIndexKind)> = vec![("Hnsw".into(), plain.into())];
    for q in QUANTIZATIONS {
        let index = QuantizedHnswIndex::with_seed(config.clone(), q, 1);
        index.restore_topology(Some(node(0)), 0, topology.clone());
        kinds.push((format!("{q:?}"), index.into()));
    }

    let allowlist: HashSet<NodeId> = (HALF..N).map(node).collect();
    let query = vectors[0].clone();
    let want = exact(&vectors, &query, 5, &allowlist);
    assert_eq!(want.len(), 5);

    filtered_scan_stats::with_threshold(10, || {
        for (name, kind) in &kinds {
            let before = filtered_scan_stats::get().1;
            assert_eq!(
                ids(&kind.search_with_filter(&query, 5, &allowlist, &acc)),
                want,
                "{name} search_with_filter"
            );
            assert_eq!(
                ids(&kind.search_with_ef_and_filter(&query, 5, 20, &allowlist, &acc)),
                want,
                "{name} search_with_ef_and_filter"
            );
            assert_eq!(
                filtered_scan_stats::get().1,
                before + 2,
                "{name}: both searches took the shortfall fallback"
            );
        }
    });

    // The traversal on its own really does come back empty here.
    if let Some(plain) = kinds[0].1.as_hnsw() {
        let traversal = plain.filtered_traversal(&query, 5, 20, &allowlist, &acc);
        assert_eq!(traversal.len(), 0, "{traversal:?}");
    }
}

/// Release-mode latency and recall report (10k vectors, fixed seeds).
/// `cargo test --release -p grafeo-core --lib --features vector-index \
///  filtered_search_latency_report -- --ignored --nocapture`
#[test]
#[ignore = "latency report; run in release with --nocapture"]
fn filtered_search_latency_report() {
    use std::time::Instant;
    const N: usize = 10_000;
    const DIM: usize = 64;
    const K: usize = 10;
    const QUERIES: usize = 200;
    let vectors = random_vectors(N, DIM, 2026);
    let st = store(&vectors);
    let acc = map_accessor(&st);
    let mut qrng = Rng::new(77);
    let queries: Vec<Vec<f32>> = (0..QUERIES)
        .map(|_| (0..DIM).map(|_| qrng.next_f32()).collect())
        .collect();

    let kinds: Vec<(&str, VectorIndexKind)> = vec![
        ("Hnsw", build_plain(&vectors, 9, &acc).into()),
        (
            "Scalar (production insert)",
            build_quantized(
                &vectors,
                QuantizationType::Scalar,
                Load::Production,
                9,
                &acc,
            )
            .into(),
        ),
    ];

    for (name, kind) in &kinds {
        // Warm up.
        for q in queries.iter().take(20) {
            let _ = kind.search(q, K, &acc);
        }
        // Best of five passes, to keep the unfiltered baseline stable.
        let us = (0..5)
            .map(|_| {
                let t = Instant::now();
                for q in &queries {
                    std::hint::black_box(kind.search(q, K, &acc));
                }
                t.elapsed().as_secs_f64() * 1e6 / QUERIES as f64
            })
            .fold(f64::MAX, f64::min);
        let all: HashSet<NodeId> = (0..N).map(node).collect();
        let rec = queries
            .iter()
            .map(|q| recall(&ids(&kind.search(q, K, &acc)), &exact(&vectors, q, K, &all)))
            .sum::<f64>()
            / QUERIES as f64;
        println!("| {name} | unfiltered | - | {us:.1} | {rec:.3} |");

        for pct in [1usize, 10, 33, 90] {
            for far in [false, true] {
                let count = N * pct / 100;
                let mut arng = Rng::new(pct as u64);
                let allowlists: Vec<HashSet<NodeId>> = queries
                    .iter()
                    .map(|q| {
                        if far {
                            far_allowlist(&vectors, q, count)
                        } else {
                            random_allowlist(N, count, &mut arng)
                        }
                    })
                    .collect();
                let mut total_recall = 0.0;
                let mut elapsed = 0.0;
                for (q, allow) in queries.iter().zip(&allowlists) {
                    let t = Instant::now();
                    let res = std::hint::black_box(kind.search_with_filter(q, K, allow, &acc));
                    elapsed += t.elapsed().as_secs_f64();
                    total_recall += recall(&ids(&res), &exact(&vectors, q, K, allow));
                }
                let us = elapsed * 1e6 / QUERIES as f64;
                let rec = total_recall / QUERIES as f64;
                let shape = if far { "far region" } else { "random" };
                println!("| {name} | {pct}% ({count}) | {shape} | {us:.1} | {rec:.3} |");
                if let (false, Some(plain)) = (far, kind.as_hnsw()) {
                    // Cost of answering the same queries by exact scan alone.
                    let t = Instant::now();
                    for (q, allow) in queries.iter().zip(&allowlists) {
                        std::hint::black_box(plain.filtered_exact_scan(
                            q,
                            K,
                            allow,
                            &acc,
                            super::hnsw::FilteredScanReason::SmallAllowlist,
                        ));
                    }
                    let us = t.elapsed().as_secs_f64() * 1e6 / QUERIES as f64;
                    println!("| (exact scan only) | {pct}% ({count}) | random | {us:.1} | 1.000 |");
                }
            }
        }
    }
}
