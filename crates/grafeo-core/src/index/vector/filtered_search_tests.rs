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
    /// `insert`, then `rehydrate_payloads_from_vectors` as on reopen: a
    /// trained quantizer with codes and no internal f32 copy (AMH's steady
    /// state after a restart).
    Reopened,
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
            Load::Production | Load::Reopened => index.insert(node(i), v, acc),
            Load::Codes => index.test_insert(node(i), v),
        }
    }
    if let Load::Reopened = load {
        index.rehydrate_payloads_from_vectors(
            vectors
                .iter()
                .enumerate()
                .map(|(i, v)| (node(i), v.clone())),
        );
    }
    index
}

/// Plain HNSW plus every quantization type under every load.
fn all_kinds(
    vectors: &[Vec<f32>],
    seed: u64,
    acc: &impl VectorAccessor,
) -> Vec<(String, VectorIndexKind)> {
    let mut kinds: Vec<(String, VectorIndexKind)> =
        vec![("Hnsw".into(), build_plain(vectors, seed, acc).into())];
    for q in QUANTIZATIONS {
        for load in [Load::Production, Load::Codes, Load::Reopened] {
            kinds.push((
                format!("{q:?}/{load:?}"),
                build_quantized(vectors, q, load, seed, acc).into(),
            ));
        }
    }
    kinds
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

    let kinds = all_kinds(&vectors, 42, &acc);
    for (name, kind) in &kinds {
        for (entry, got) in all_entry_points(kind, &query, 5, &allowlist, &acc) {
            assert_eq!(got, expected, "{name} {entry}");
        }
    }
}

/// Edge cases, through the exact scan (default gate) and through the walk
/// (gate forced to 0, so any non-empty allowlist walks the graph).
#[test]
fn filtered_edge_cases() {
    let vectors = random_vectors(200, 8, 11);
    let st = store(&vectors);
    let acc = map_accessor(&st);
    let query = vectors[3].clone();
    let kinds = all_kinds(&vectors, 1, &acc);

    let empty = HashSet::new();
    let larger_than_k: HashSet<NodeId> = (0..40).map(node).collect();
    let not_indexed: HashSet<NodeId> = (1000..1010).map(NodeId::new).collect();
    let mixed: HashSet<NodeId> = (0..3)
        .map(node)
        .chain((1000..1010).map(NodeId::new))
        .collect();
    let three: HashSet<NodeId> = [node(150), node(20), node(99)].into_iter().collect();
    // An indexed, allowlisted node whose vector the accessor cannot supply
    // (property removed or wrong width) is returned by neither path.
    let missing = node(4);
    let mut st_missing = st.clone();
    st_missing.remove(&missing);
    let acc_missing = map_accessor(&st_missing);
    let with_missing: HashSet<NodeId> = (0..12).map(node).collect();
    let without: HashSet<NodeId> = with_missing
        .iter()
        .copied()
        .filter(|&id| id != missing)
        .collect();

    for gate in [None, Some(0)] {
        let run = || {
            for (name, kind) in &kinds {
                let ctx = format!("{name} gate={gate:?}");
                for (entry, got) in all_entry_points(kind, &query, 5, &empty, &acc) {
                    assert!(got.is_empty(), "{ctx} {entry}: empty allowlist");
                }
                for (entry, got) in all_entry_points(kind, &query, 5, &larger_than_k, &acc) {
                    assert_eq!(
                        got,
                        exact(&vectors, &query, 5, &larger_than_k),
                        "{ctx} {entry}: allowlist larger than k"
                    );
                }
                for (entry, got) in all_entry_points(kind, &query, 5, &not_indexed, &acc) {
                    assert!(got.is_empty(), "{ctx} {entry}: ids not in the index");
                }
                for (entry, got) in all_entry_points(kind, &query, 5, &mixed, &acc) {
                    assert_eq!(
                        got,
                        exact(&vectors, &query, 5, &mixed),
                        "{ctx} {entry}: only indexed ids come back"
                    );
                }
                for (entry, got) in all_entry_points(kind, &query, 10, &three, &acc) {
                    assert_eq!(
                        got,
                        exact(&vectors, &query, 10, &three),
                        "{ctx} {entry}: k larger than the allowlist returns all allowed"
                    );
                }
                // Indexes that keep their own f32 copy (quantization None, or
                // the code-keeping test loader) can still score the node, on
                // every path; the others cannot, on any path.
                let own_copy = name.starts_with("None") || name.ends_with("/Codes");
                let expected = if own_copy { &with_missing } else { &without };
                for (entry, got) in all_entry_points(kind, &query, 20, &with_missing, &acc_missing)
                {
                    assert_eq!(
                        got,
                        exact(&vectors, &query, 20, expected),
                        "{ctx} {entry}: a node with no vector is not returned"
                    );
                }
                assert!(
                    kind.search_with_filter(&query, 0, &larger_than_k, &acc)
                        .is_empty(),
                    "{ctx}: k = 0"
                );
            }
        };
        match gate {
            Some(g) => filtered_scan_stats::with_threshold(g, run),
            None => run(),
        }
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

/// Seeded recall against the exact filtered top-k at about 1%, 3%, 10%, 33%
/// and 90% selectivity, for random allowlists and far-region ones (the
/// allowlist is the part of the index furthest from the query). 200 seeds
/// per case, over plain HNSW, None (production), Scalar (production, codes,
/// reopened), and Binary and Product (codes, reopened).
///
/// * Default gate: every allowlist here (at most 900 ids) takes the exact
///   scan, so results must equal the exact filtered top-k.
/// * Gate replaced by `|A| <= 49` on this thread (no visit-estimate term, so
///   only allowlists smaller than ef are scanned): the 10%, 33% and 90%
///   allowlists go through the walk, including the low-selectivity regime a
///   large production index sends there. They must never come back short,
///   and mean recall must be at least 0.95 (0.5 where the unfiltered pipeline
///   is itself approximate: product quantization with codes).
#[test]
fn seeded_filtered_recall_matches_exact() {
    const N: usize = 1000;
    const DIM: usize = 8;
    const K: usize = 10;
    const SEEDS: u64 = 200;
    // Below ef (50) an allowlist is always scanned, as in production.
    const WALK_GATE: usize = 49;
    let vectors = random_vectors(N, DIM, 2026);
    let st = store(&vectors);
    let acc = map_accessor(&st);
    // Production loads of Binary and Product search exactly like plain HNSW
    // (no codes, accessor distances), and None keeps the same internal copy
    // under both loaders, so those duplicates are left out here; the
    // edge-case and far-allowlist tests cover every kind and load.
    let kinds: Vec<(String, VectorIndexKind)> = all_kinds(&vectors, 5, &acc)
        .into_iter()
        .filter(|(name, _)| {
            !(name.starts_with("Binary/Production")
                || name.starts_with("Product") && name.ends_with("/Production")
                || name == "None/Codes")
        })
        .collect();
    let baselines: Vec<f64> = kinds
        .iter()
        .map(|(_, kind)| unfiltered_recall(kind, &vectors, K, &acc))
        .collect();

    for pct in [1usize, 3, 10, 33, 90] {
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
                for gate in [None, Some(WALK_GATE)] {
                    // The default-gate pass scans every allowlist here; the
                    // scan has two implementations (plain, and quantized with
                    // its internal-first lookup), so one kind per lookup
                    // source covers it. The walk pass covers every kind, and
                    // is skipped where it would scan too.
                    let scan_representative = matches!(
                        name.as_str(),
                        "Hnsw" | "None/Production" | "Scalar/Reopened" | "Binary/Codes"
                    );
                    match gate {
                        None if !scan_representative => continue,
                        Some(g) if count <= g => continue,
                        _ => {}
                    }
                    let exact_expected = gate.is_none_or(|g| count <= g);
                    let mut total_recall = 0.0;
                    let mut calls = 0usize;
                    let mut run = || {
                        for (seed, (query, allowlist, want)) in cases.iter().enumerate() {
                            // The default ef is 50, so `search_with_filter` is
                            // `search_with_ef_and_filter(.., 50, ..)`; the walk
                            // pass also checks a wider beam on 20 seeds.
                            let mut results = vec![(
                                "search_with_filter",
                                ids(&kind.search_with_filter(query, K, allowlist, &acc)),
                            )];
                            if gate.is_some() && seed < 20 {
                                results.push((
                                    "with_ef(100)",
                                    ids(&kind
                                        .search_with_ef_and_filter(query, K, 100, allowlist, &acc)),
                                ));
                            }
                            for (entry, got) in &results {
                                calls += 1;
                                let ctx = format!(
                                    "{name} {pct}% far={far} gate={gate:?} seed={seed} {entry}"
                                );
                                assert_eq!(got.len(), want.len(), "{ctx}: short result");
                                assert!(got.iter().all(|id| allowlist.contains(id)), "{ctx}");
                                if exact_expected {
                                    assert_eq!(got, want, "{ctx}: small allowlist must be exact");
                                }
                                total_recall += recall(got, want);
                            }
                        }
                    };
                    filtered_scan_stats::reset();
                    match gate {
                        Some(g) => filtered_scan_stats::with_threshold(g, run),
                        None => run(),
                    }
                    if !exact_expected {
                        assert!(
                            filtered_scan_stats::get().scored > 0,
                            "{name} {pct}% far={far}: the walk never ran"
                        );
                    }
                    let mean = total_recall / calls as f64;
                    // Product quantization with codes ranks candidates by PQ
                    // distance and truncates to k before rescoring (unchanged
                    // here; production `insert` keeps no PQ codes), so it is
                    // approximate even unfiltered, and more so when all the
                    // allowlisted candidates sit at similar distances.
                    let bound = if *baseline >= 0.95 { 0.95 } else { 0.5 };
                    assert!(
                        mean >= bound,
                        "{name} {pct}% far={far} gate={gate:?}: mean recall@{K} \
                         {mean:.3} below {bound:.3} (unfiltered {baseline:.3})"
                    );
                }
            }
        }
    }
}

/// Large allowlists keep using the graph, and the walk stays cheap: for
/// random allowlists at 50% and 90% selectivity (gate lowered so a small
/// index has "large" allowlists) no exact scan of any kind runs, and every
/// walk scores fewer nodes than the scan would (`|A|`) and than half the index. At 33% (1,320 ids) a healthy walk
/// on this 4,000-node graph scores more nodes than the scan would, so the
/// budget hands it to the scan; `walk_budget_falls_back_to_exact_scan` checks
/// that case. Covers plain HNSW, untrained production Scalar and
/// trained-after-reopen Scalar.
#[test]
fn large_allowlists_use_the_index_not_a_scan() {
    const N: usize = 4000;
    const GATE: usize = 64;
    let vectors = random_vectors(N, 8, 99);
    let st = store(&vectors);
    let acc = map_accessor(&st);
    let kinds: Vec<(&str, VectorIndexKind)> = vec![
        ("Hnsw", build_plain(&vectors, 3, &acc).into()),
        (
            "Scalar/Production",
            build_quantized(
                &vectors,
                QuantizationType::Scalar,
                Load::Production,
                3,
                &acc,
            )
            .into(),
        ),
        (
            "Scalar/Reopened",
            build_quantized(&vectors, QuantizationType::Scalar, Load::Reopened, 3, &acc).into(),
        ),
    ];

    filtered_scan_stats::with_threshold(GATE, || {
        for (name, kind) in &kinds {
            for pct in [50usize, 90] {
                let count = N * pct / 100;
                filtered_scan_stats::reset();
                for seed in 0..50u64 {
                    let mut rng = Rng::new(seed);
                    let query: Vec<f32> = (0..8).map(|_| rng.next_f32()).collect();
                    let allowlist = random_allowlist(N, count, &mut rng);
                    assert_eq!(
                        kind.search_with_filter(&query, 10, &allowlist, &acc).len(),
                        10
                    );
                    assert_eq!(
                        kind.search_with_ef_and_filter(&query, 10, 50, &allowlist, &acc)
                            .len(),
                        10
                    );
                }
                let stats = filtered_scan_stats::get();
                assert_eq!(
                    (stats.small, stats.shortfall, stats.budget),
                    (0, 0, 0),
                    "{name} {pct}%: large allowlist fell back to an exact scan"
                );
                assert!(
                    stats.max_walk < count.min(N / 2),
                    "{name} {pct}%: a walk scored {} of {N} nodes (allowlist {count})",
                    stats.max_walk
                );
            }
        }
    });

    // At the default gate, a 33% allowlist (1320 ids) is small.
    let allowlist = random_allowlist(N, N / 3, &mut Rng::new(1));
    assert!(allowlist.len() <= FILTERED_EXACT_SCAN_THRESHOLD);
    filtered_scan_stats::reset();
    for (_, kind) in &kinds {
        let _ = kind.search_with_filter(&vectors[0], 10, &allowlist, &acc);
    }
    let stats = filtered_scan_stats::get();
    assert_eq!((stats.small, stats.scored), (kinds.len(), 0));
}

/// The gate counts allowlisted ids **in the index**: an engine allowlist of
/// 3,000 label nodes of which only 40 have an embedding is a small allowlist
/// and is scanned exactly, without walking the graph.
#[test]
fn gate_counts_only_indexed_ids() {
    const N: usize = 2000;
    let vectors = random_vectors(N, 8, 21);
    let st = store(&vectors);
    let acc = map_accessor(&st);
    let query = vectors[7].clone();
    let allowlist: HashSet<NodeId> = (0..40)
        .map(|i| node(i * 50))
        .chain((100_000..102_960).map(NodeId::new))
        .collect();
    assert_eq!(allowlist.len(), 3000);
    assert!(allowlist.len() > FILTERED_EXACT_SCAN_THRESHOLD);
    let want = exact(&vectors, &query, 10, &allowlist);

    let kinds: Vec<(&str, VectorIndexKind)> = vec![
        ("Hnsw", build_plain(&vectors, 4, &acc).into()),
        (
            "Scalar/Reopened",
            build_quantized(&vectors, QuantizationType::Scalar, Load::Reopened, 4, &acc).into(),
        ),
    ];
    for (name, kind) in &kinds {
        filtered_scan_stats::reset();
        assert_eq!(
            ids(&kind.search_with_filter(&query, 10, &allowlist, &acc)),
            want,
            "{name}"
        );
        let stats = filtered_scan_stats::get();
        assert_eq!((stats.small, stats.scored), (1, 0), "{name}: {stats:?}");
    }
}

/// The walk's work budget: when a walk scores more nodes than its budget, the
/// search falls back to the exact scan and still returns the exact filtered
/// top-k.
/// * Forced: a far-region allowlist above the gate with the budget set to 100
///   makes every walk hit it, for every kind.
/// * Default budget: random and far-region 33% allowlists on a 4,000-node
///   index. No walk scores more than `|A|` nodes (the default budget there),
///   results are never short, recall stays high, and far-region walks, which
///   would otherwise expand most of the graph, do hit the budget.
#[test]
fn walk_budget_falls_back_to_exact_scan() {
    const N: usize = 1000;
    const BUDGET: usize = 100;
    let vectors = random_vectors(N, 8, 33);
    let st = store(&vectors);
    let acc = map_accessor(&st);
    let kinds = all_kinds(&vectors, 6, &acc);

    for seed in 0..10u64 {
        let mut rng = Rng::new(seed);
        let query: Vec<f32> = (0..8).map(|_| rng.next_f32()).collect();
        let allowlist = far_allowlist(&vectors, &query, N / 3);
        let want = exact(&vectors, &query, 10, &allowlist);
        filtered_scan_stats::with_threshold(64, || {
            filtered_scan_stats::with_budget(BUDGET, || {
                for (name, kind) in &kinds {
                    filtered_scan_stats::reset();
                    let got = ids(&kind.search_with_filter(&query, 10, &allowlist, &acc));
                    let stats = filtered_scan_stats::get();
                    assert_eq!(got, want, "{name} seed={seed}");
                    assert_eq!(stats.budget, 1, "{name} seed={seed}: {stats:?}");
                    assert!(stats.max_walk <= BUDGET, "{name} seed={seed}: {stats:?}");
                }
            });
        });
    }

    // Default budget, gate lowered so the 33% allowlists walk.
    const N2: usize = 4000;
    let vectors = random_vectors(N2, 8, 99);
    let st = store(&vectors);
    let acc = map_accessor(&st);
    let kinds: Vec<(&str, VectorIndexKind)> = vec![
        ("Hnsw", build_plain(&vectors, 3, &acc).into()),
        (
            "Scalar/Reopened",
            build_quantized(&vectors, QuantizationType::Scalar, Load::Reopened, 3, &acc).into(),
        ),
    ];
    let count = N2 / 3;
    for far in [false, true] {
        for (name, kind) in &kinds {
            let mut total_recall = 0.0;
            filtered_scan_stats::reset();
            filtered_scan_stats::with_threshold(64, || {
                for seed in 0..50u64 {
                    let mut rng = Rng::new(seed);
                    let query: Vec<f32> = (0..8).map(|_| rng.next_f32()).collect();
                    let allowlist = if far {
                        far_allowlist(&vectors, &query, count)
                    } else {
                        random_allowlist(N2, count, &mut rng)
                    };
                    let want = exact(&vectors, &query, 10, &allowlist);
                    let got = ids(&kind.search_with_filter(&query, 10, &allowlist, &acc));
                    assert_eq!(got.len(), 10, "{name} far={far} seed={seed}");
                    total_recall += recall(&got, &want);
                }
            });
            let stats = filtered_scan_stats::get();
            assert!(
                stats.max_walk <= count,
                "{name} far={far}: a walk scored past its budget: {stats:?}"
            );
            let mean = total_recall / 50.0;
            assert!(mean >= 0.95, "{name} far={far}: recall {mean:.3}");
            if far {
                assert!(
                    stats.budget > 0,
                    "{name}: far-region walks never hit the budget: {stats:?}"
                );
            }
        }
    }
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

    // Gate lowered so the |B| = 200 allowlist walks; the walk exhausts
    // component A (200 nodes) well within its budget.
    filtered_scan_stats::with_threshold(10, || {
        for (name, kind) in &kinds {
            filtered_scan_stats::reset();
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
            let stats = filtered_scan_stats::get();
            assert_eq!(
                (stats.shortfall, stats.budget),
                (2, 0),
                "{name}: both searches took the shortfall fallback: {stats:?}"
            );
        }
    });

    // The traversal on its own really does come back empty here.
    if let Some(plain) = kinds[0].1.as_hnsw() {
        let traversal = plain.filtered_traversal(&query, 5, 20, &allowlist, usize::MAX, &acc);
        assert_eq!(traversal.map(|t| t.len()), Some(0));
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
        (
            "Scalar (reopened, trained)",
            build_quantized(&vectors, QuantizationType::Scalar, Load::Reopened, 9, &acc).into(),
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
                filtered_scan_stats::reset();
                // BENCH_GATE / BENCH_BUDGET override the gate and the walk
                // budget, to measure the walk on its own.
                let gate = std::env::var("BENCH_GATE")
                    .ok()
                    .and_then(|v| v.parse().ok());
                let budget = std::env::var("BENCH_BUDGET")
                    .ok()
                    .and_then(|v| v.parse().ok());
                let mut run = || {
                    for (q, allow) in queries.iter().zip(&allowlists) {
                        let t = Instant::now();
                        let res = std::hint::black_box(kind.search_with_filter(q, K, allow, &acc));
                        elapsed += t.elapsed().as_secs_f64();
                        total_recall += recall(&ids(&res), &exact(&vectors, q, K, allow));
                    }
                };
                match (gate, budget) {
                    (Some(g), Some(b)) => filtered_scan_stats::with_threshold(g, || {
                        filtered_scan_stats::with_budget(b, run);
                    }),
                    (Some(g), None) => filtered_scan_stats::with_threshold(g, run),
                    (None, Some(b)) => filtered_scan_stats::with_budget(b, run),
                    (None, None) => run(),
                }
                let stats = filtered_scan_stats::get();
                let us = elapsed * 1e6 / QUERIES as f64;
                let rec = total_recall / QUERIES as f64;
                let shape = if far { "far region" } else { "random" };
                println!(
                    "| {name} | {pct}% ({count}) | {shape} | {us:.1} | {rec:.3} | {} | {} | {}/{}/{} |",
                    stats.scored / QUERIES,
                    stats.max_walk,
                    stats.small,
                    stats.budget,
                    stats.shortfall
                );
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
                    println!(
                        "| (exact scan only) | {pct}% ({count}) | random | {us:.1} | 1.000 | 0 | 0 | - |"
                    );
                }
            }
        }
    }
}
