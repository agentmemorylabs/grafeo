//! AMH #175: on a plain `.grafeo` file opened with the ForceDisk vector tier
//! (AMH's production default), a vector written in-session must be
//! searchable, must not damage the rest of the ANN graph, and must survive a
//! reopen.
//!
//! The open spills every vector-indexed column to an mmap file, so existing
//! nodes' vectors are no longer inline properties. Before the fix, the
//! write-side HNSW inserts read neighbour vectors through a property-only
//! accessor: every neighbour looked vectorless, so the new node was never
//! linked (unfindable) and neighbour pruning dropped edges of existing nodes
//! (recall collapse).
//!
//! ```text
//! cargo test -p grafeo-engine --features lpg,mmap,wal,grafeo-file,vector-index \
//!   --test plain_forcedisk_in_session_vectors
//! ```

#![cfg(all(
    feature = "lpg",
    feature = "mmap",
    feature = "wal",
    feature = "grafeo-file",
    feature = "vector-index",
    not(feature = "temporal")
))]

use std::collections::HashMap;
use std::path::Path;

use grafeo_common::storage::{SectionType, TierOverride};
use grafeo_common::types::{NodeId, PropertyKey, Value};
use grafeo_core::graph::traits::GraphStore;
use grafeo_engine::{Config, GrafeoDB};
use tempfile::tempdir;

const DIMS: usize = 16;
const BASE: u64 = 300;

/// Seeded pseudo-random vector; distinct seeds give distinct directions, so
/// a node's own vector is its unique nearest neighbour under cosine.
fn vector(seed: u64) -> Vec<f32> {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    (0..DIMS)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % 2001) as f32 / 1000.0 - 1.0
        })
        .collect()
}

fn vval(seed: u64) -> Value {
    Value::Vector(vector(seed).into())
}

/// AMH's production open config for a writable plain graph
/// (`am_graph::grafeo::runtime_core::open`).
fn force_disk(path: &Path, spill: &Path) -> Config {
    Config::persistent(path)
        .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk)
        .with_spill_path(spill)
}

/// Nodes whose own vector is not their top-1 hit (distance ~0).
fn not_self_found(db: &GrafeoDB, nodes: &[(&str, NodeId, u64)]) -> Vec<String> {
    nodes
        .iter()
        .filter_map(|(what, id, seed)| {
            let hits = db
                .vector_search("Doc", "embedding", &vector(*seed), 10, Some(64), None)
                .unwrap();
            match hits.first() {
                Some((hit, d)) if hit == id && *d < 1e-4 => None,
                other => Some(format!("{what} {id:?}: top hit {other:?}")),
            }
        })
        .collect()
}

fn assert_all_found(db: &GrafeoDB, nodes: &[(&str, NodeId, u64)], when: &str) {
    let missing = not_self_found(db, nodes);
    assert!(
        missing.is_empty(),
        "{when}: {} of {} nodes not found by their own vector:\n{}",
        missing.len(),
        nodes.len(),
        missing.join("\n")
    );
}

#[test]
fn forcedisk_in_session_vectors_are_searchable_and_survive_reopen() {
    run(None);
}

/// AMH's production index: cosine HNSW with scalar quantization.
#[test]
fn forcedisk_in_session_vectors_scalar_quantized() {
    run(Some("scalar"));
}

fn run(quantization: Option<&str>) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("acct.grafeo");
    let spill = dir.path().join("acct.grafeo.spill");
    std::fs::create_dir_all(&spill).unwrap();

    // Base graph, written under the default (Auto) tier.
    let mut nodes: Vec<(&str, NodeId, u64)> = Vec::new();
    {
        let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
        db.create_vector_index(
            "Doc",
            "embedding",
            Some(DIMS),
            Some("cosine"),
            None,
            None,
            quantization,
        )
        .unwrap();
        for s in 0..BASE {
            let id = db
                .create_node_with_props(&["Doc"], [("embedding", vval(s))])
                .unwrap();
            nodes.push(("base", id, s));
        }
        db.close().unwrap();
    }

    let db = GrafeoDB::with_config(force_disk(&path, &spill)).unwrap();
    // Precondition: the open spilled the column, so base vectors are no
    // longer inline properties.
    let key = PropertyKey::new("embedding");
    assert!(
        db.graph_store()
            .get_node_property(nodes[0].1, &key)
            .is_none(),
        "precondition: ForceDisk open spills the indexed column"
    );
    assert_all_found(&db, &nodes, "after ForceDisk open");

    // One write through every vector-insert path.
    let mut seed = 10_000;
    let mut next = || {
        seed += 1;
        seed
    };
    let session = db.session();
    let s = next();
    let id = session
        .create_node_with_props(&["Doc"], [("embedding", vval(s))])
        .unwrap();
    nodes.push(("session create", id, s));
    let s = next();
    session
        .set_node_property(nodes[5].1, "embedding", vval(s))
        .unwrap();
    nodes[5] = ("session re-embed", nodes[5].1, s);
    drop(session);

    let s = next();
    let id = db
        .create_node_with_props(&["Doc"], [("embedding", vval(s))])
        .unwrap();
    nodes.push(("db create", id, s));
    let s = next();
    db.set_node_property(nodes[6].1, "embedding", vval(s))
        .unwrap();
    nodes[6] = ("db re-embed", nodes[6].1, s);

    let seeds: Vec<u64> = (0..3).map(|_| next()).collect();
    let ids = db.batch_create_nodes(
        "Doc",
        "embedding",
        seeds.iter().map(|s| vector(*s)).collect(),
    );
    nodes.extend(
        ids.into_iter()
            .zip(&seeds)
            .map(|(id, s)| ("batch create", id, *s)),
    );

    let seeds: Vec<u64> = (0..3).map(|_| next()).collect();
    let props = seeds
        .iter()
        .map(|s| HashMap::from([(PropertyKey::new("embedding"), vval(*s))]))
        .collect();
    let ids = db.batch_create_nodes_with_props("Doc", props);
    nodes.extend(
        ids.into_iter()
            .zip(&seeds)
            .map(|(id, s)| ("batch create with props", id, *s)),
    );

    let s = next();
    let id = db
        .create_node_with_props(&["Pending"], [("embedding", vval(s))])
        .unwrap();
    assert!(db.add_node_label(id, "Doc"));
    nodes.push(("add label", id, s));

    // Many in-session inserts: neighbour pruning must not drop base nodes.
    let session = db.session();
    for _ in 0..200 {
        let s = next();
        let id = session
            .create_node_with_props(&["Doc"], [("embedding", vval(s))])
            .unwrap();
        nodes.push(("session create (bulk)", id, s));
    }
    drop(session);

    assert_all_found(&db, &nodes, "in-session, ForceDisk");
    db.close().unwrap();
    drop(db);

    let db = GrafeoDB::with_config(force_disk(&path, &spill)).unwrap();
    assert_all_found(&db, &nodes, "after ForceDisk reopen");
    db.close().unwrap();
    drop(db);

    let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
    assert_all_found(&db, &nodes, "after Auto reopen");
}
