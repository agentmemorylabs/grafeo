//! AMH #174: on a writable generation root, updating or deleting the
//! indexed vector of a base node must not drop other nodes from the ANN
//! index.
//!
//! Before the fix the layered write path upserted a vector by `index.remove(id)`
//! then `index.insert(id, ..)`, and deleted with `index.remove(id)`. The plain
//! HNSW `remove` deletes the node and its incoming links without reconnecting
//! its former neighbours, so nodes reachable only through it fell out of
//! search (exact reads still worked). Upserts and deletes now go through
//! `remove_with_accessor`, which reconnects and prunes them.
//!
//! ```text
//! cargo test -p grafeo-engine --features generation,generation-streaming,compact-store,lpg,mmap,wal,vector-index \
//!   --test generation_root_vector_update_recall
//! ```

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "compact-store",
    feature = "lpg",
    feature = "mmap",
    feature = "wal",
    feature = "vector-index",
    not(feature = "temporal")
))]

use std::collections::HashMap;
use std::path::Path;

use grafeo_common::storage::{SectionType, TierOverride};
use grafeo_common::types::{NodeId, Value};
use grafeo_engine::{Config, GrafeoDB, generation_build_request};
use tempfile::tempdir;

const DIMS: usize = 16;

/// Deterministic pseudo-random vector for `seed` (splitmix64).
fn vector(seed: u64) -> Vec<f32> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    (0..DIMS)
        .map(|_| {
            x ^= x >> 30;
            x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            x ^= x >> 27;
            x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
            x ^= x >> 31;
            (x % 10_000) as f32 / 10_000.0 - 0.5
        })
        .collect()
}

/// Points on a line (the issue's probe data). HNSW's diversity heuristic
/// links colinear points as a chain, so a node removed without reconnecting
/// its neighbours splits the graph: the adversarial case for this bug.
fn line_vector(seed: u64) -> Vec<f32> {
    (0..DIMS as u64)
        .map(|d| (seed * 10 + d) as f32 + 0.5)
        .collect()
}

fn dist2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

#[derive(Clone, Copy, Debug)]
enum Tier {
    Plain,
    ForceDisk,
}

fn open(root: &Path, spill: &Path, tier: Tier) -> GrafeoDB {
    let config = Config::persistent(root);
    let config = match tier {
        Tier::Plain => config,
        Tier::ForceDisk => config
            .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk)
            .with_spill_path(spill),
    };
    GrafeoDB::open_generation_root_with_config(config).expect("open generation root")
}

/// Builds `n` base `:Doc` nodes, indexes them, publishes a generation root.
fn publish_base(
    root: &Path,
    n: u64,
    quantization: Option<&str>,
    make: fn(u64) -> Vec<f32>,
) -> HashMap<NodeId, Vec<f32>> {
    std::fs::create_dir_all(root).unwrap();
    let source = GrafeoDB::new_in_memory();
    let mut live = HashMap::new();
    for seed in 0..n {
        let v = make(seed);
        let id = source
            .create_node_with_props(&["Doc"], [("embedding", Value::Vector(v.clone().into()))])
            .unwrap();
        live.insert(id, v);
    }
    source
        .create_vector_index(
            "Doc",
            "embedding",
            Some(DIMS),
            Some("euclidean"),
            None,
            None,
            quantization,
        )
        .unwrap();
    source
        .build_and_publish_generation(generation_build_request(root, "g1"))
        .unwrap();
    live
}

/// Every live node is found by its own vector (top-k, distance 0), and the
/// mean recall@10 against an exact oracle over `queries` is reported.
fn check(db: &GrafeoDB, live: &HashMap<NodeId, Vec<f32>>, what: &str) -> f64 {
    let mut missing = Vec::new();
    for (id, v) in live {
        let hits = db
            .vector_search("Doc", "embedding", v, 10, Some(128), None)
            .unwrap();
        if !hits.iter().any(|h| h.0 == *id && h.1 < 1e-4) {
            missing.push(*id);
        }
    }
    let mut recall_sum = 0.0;
    let queries = 64u64;
    for q in 0..queries {
        let query = vector(1_000_000 + q);
        let mut exact: Vec<(NodeId, f32)> =
            live.iter().map(|(id, v)| (*id, dist2(&query, v))).collect();
        exact.sort_by(|a, b| a.1.total_cmp(&b.1));
        let truth: Vec<NodeId> = exact.iter().take(10).map(|h| h.0).collect();
        let got = db
            .vector_search("Doc", "embedding", &query, 10, Some(128), None)
            .unwrap();
        let hit = got.iter().filter(|h| truth.contains(&h.0)).count();
        recall_sum += hit as f64 / truth.len().min(10) as f64;
    }
    let recall = recall_sum / queries as f64;
    missing.sort_by_key(|id| id.as_u64());
    assert!(
        missing.is_empty(),
        "{what}: {} of {} live nodes not found by their own vector: {:?} (recall@10 {recall:.3})",
        missing.len(),
        live.len(),
        missing
    );
    recall
}

/// The issue's repro: 6 base nodes on a line, one base-vector update.
fn small_graph(tier: Tier, quantization: Option<&str>) {
    let dir = tempdir().unwrap();
    let root = dir.path().join("s.grafeo.d");
    let spill = dir.path().join("s.spill");
    let mut live = publish_base(&root, 6, quantization, line_vector);
    let db = open(&root, &spill, tier);
    check(&db, &live, "after open");
    let mut ids: Vec<NodeId> = live.keys().copied().collect();
    ids.sort_by_key(|id| id.as_u64());
    let v = line_vector(1);
    db.set_node_property(ids[1], "embedding", Value::Vector(v.clone().into()))
        .unwrap();
    live.insert(ids[1], v);
    check(
        &db,
        &live,
        &format!("{tier:?} {quantization:?}: after one base-vector update"),
    );
}

/// 300 base nodes; update 40 base vectors and delete 10 base nodes, then
/// check membership + recall, also after a reopen and after a compaction.
fn larger_graph(tier: Tier, quantization: Option<&str>) {
    let dir = tempdir().unwrap();
    let root = dir.path().join("l.grafeo.d");
    let spill = dir.path().join("l.spill");
    let mut live = publish_base(&root, 300, quantization, vector);
    let mut ids: Vec<NodeId> = live.keys().copied().collect();
    ids.sort_by_key(|id| id.as_u64());
    let tag = format!("{tier:?} {quantization:?}");
    {
        let db = open(&root, &spill, tier);
        for (i, id) in ids.iter().step_by(7).take(40).enumerate() {
            let v = vector(500_000 + i as u64);
            db.set_node_property(*id, "embedding", Value::Vector(v.clone().into()))
                .unwrap();
            live.insert(*id, v);
        }
        for id in ids.iter().skip(3).step_by(29).take(10) {
            assert!(db.delete_node(*id).unwrap());
            live.remove(id);
        }
        let r = check(&db, &live, &format!("{tag}: after writes"));
        assert!(r >= 0.95, "{tag}: recall@10 after writes {r:.3}");
        let report = db
            .run_epoch_handoff(generation_build_request(&root, "g2"))
            .unwrap();
        db.publish_and_install_handoff(report).unwrap();
        let r = check(&db, &live, &format!("{tag}: after compaction"));
        assert!(r >= 0.95, "{tag}: recall@10 after compaction {r:.3}");
        db.close().unwrap();
    }
    let db = open(&root, &spill, tier);
    let r = check(&db, &live, &format!("{tag}: after reopen"));
    assert!(r >= 0.95, "{tag}: recall@10 after reopen {r:.3}");
    eprintln!("#174 {tag}: recall@10 after reopen {r:.3}");
}

#[test]
fn base_vector_update_keeps_other_nodes_searchable_plain() {
    small_graph(Tier::Plain, None);
}

#[test]
fn base_vector_update_keeps_other_nodes_searchable_forcedisk() {
    small_graph(Tier::ForceDisk, None);
}

#[test]
fn base_vector_update_keeps_other_nodes_searchable_scalar() {
    small_graph(Tier::Plain, Some("scalar"));
}

/// 200 base nodes on a line; 20 base-vector updates (same value) and 5
/// deletes. On a chain-shaped graph every unreconnected removal splits it.
#[test]
fn base_vector_updates_on_a_chain_keep_every_node_searchable() {
    for tier in [Tier::Plain, Tier::ForceDisk] {
        let dir = tempdir().unwrap();
        let root = dir.path().join("c.grafeo.d");
        let spill = dir.path().join("c.spill");
        let mut live = publish_base(&root, 200, None, line_vector);
        let mut ids: Vec<NodeId> = live.keys().copied().collect();
        ids.sort_by_key(|id| id.as_u64());
        let db = open(&root, &spill, tier);
        for id in ids.iter().skip(5).step_by(9).take(20) {
            let v = live[id].clone();
            db.set_node_property(*id, "embedding", Value::Vector(v.into()))
                .unwrap();
        }
        for id in ids.iter().skip(2).step_by(37).take(5) {
            assert!(db.delete_node(*id).unwrap());
            live.remove(id);
        }
        check(&db, &live, &format!("{tier:?} chain: after writes"));
    }
}

#[test]
fn base_vector_updates_and_deletes_keep_recall_plain() {
    larger_graph(Tier::Plain, None);
}

#[test]
fn base_vector_updates_and_deletes_keep_recall_forcedisk() {
    larger_graph(Tier::ForceDisk, None);
}

#[test]
fn base_vector_updates_and_deletes_keep_recall_scalar_forcedisk() {
    larger_graph(Tier::ForceDisk, Some("scalar"));
}
