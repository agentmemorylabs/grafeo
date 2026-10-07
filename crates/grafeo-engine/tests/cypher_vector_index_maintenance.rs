//! AMH #187: Cypher mutations of an indexed vector must maintain the HNSW
//! index, as the direct (session API) writes do.
//!
//! Before the fix the Cypher mutation operators wrote the property store
//! only: after `SET n.embedding = $v` search kept the old vector and missed
//! the new one, after `REMOVE n.embedding` or `DETACH DELETE n` the node
//! stayed in the index, and `CREATE`/`MERGE` with a vector, or adding the
//! indexed label, never indexed it. Part 2: removing a base-only node's
//! vector on a writable generation root must reach the index too.
//!
//! ```text
//! cargo test -p grafeo-engine --features cypher,vector-index,generation,generation-streaming,compact-store,mmap,wal \
//!   --test cypher_vector_index_maintenance
//! ```

// reason: node keys and seeds are small non-negative test indices
#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
#![cfg(all(
    feature = "cypher",
    feature = "lpg",
    feature = "vector-index",
    feature = "mmap",
    feature = "wal",
    feature = "grafeo-file",
    not(feature = "temporal")
))]

use std::collections::HashMap;
use std::path::Path;

use grafeo_common::storage::{SectionType, TierOverride};
use grafeo_common::types::{NodeId, Value};
use grafeo_engine::{Config, GrafeoDB};
use tempfile::tempdir;

const DIMS: usize = 16;
const BASE: u64 = 60;

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

fn vval(seed: u64) -> Value {
    Value::Vector(vector(seed).into())
}

fn params(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

/// Top hit for `seed`'s vector, if it is at distance ~0.
fn index_len(db: &GrafeoDB) -> Option<usize> {
    db.store()
        .get_vector_index("Doc", "embedding")
        .map(|index| index.len())
}

fn exact_hit(db: &GrafeoDB, seed: u64) -> Option<NodeId> {
    db.vector_search("Doc", "embedding", &vector(seed), 5, Some(128), None)
        .unwrap()
        .into_iter()
        .find(|(_, d)| *d < 1e-4)
        .map(|(id, _)| id)
}

fn node_by_key(db: &GrafeoDB, key: i64) -> NodeId {
    let r = db
        .execute_cypher_with_params(
            "MATCH (n {key: $key}) RETURN id(n)",
            params(&[("key", Value::Int64(key))]),
        )
        .unwrap();
    match r.rows()[0][0] {
        Value::Int64(i) => NodeId::new(i as u64),
        ref other => panic!("id(n) returned {other:?}"),
    }
}

/// `BASE` `:Doc` nodes `{key: i, embedding: vector(i)}` and a cosine index.
fn seed_graph(db: &GrafeoDB) {
    for i in 0..BASE {
        db.create_node_with_props(
            &["Doc"],
            [("key", Value::Int64(i as i64)), ("embedding", vval(i))],
        )
        .unwrap();
    }
    db.create_vector_index(
        "Doc",
        "embedding",
        Some(DIMS),
        Some("cosine"),
        None,
        None,
        None,
    )
    .unwrap();
}

/// Every surviving base node is still found by its own vector.
fn assert_base_intact(db: &GrafeoDB, skip: &[i64], what: &str) {
    let missing: Vec<i64> = (0..BASE as i64)
        .filter(|k| !skip.contains(k))
        .filter(|k| exact_hit(db, *k as u64).is_none())
        .collect();
    assert!(
        missing.is_empty(),
        "{what}: base nodes lost from search: {missing:?}"
    );
}

fn run_cypher_suite(db: &GrafeoDB, what: &str) {
    let mut errors: Vec<String> = Vec::new();
    let mut expect = |ok: bool, msg: &str| {
        if !ok {
            errors.push(format!("{what}: {msg}"));
        }
    };

    // SET replaces the vector: the new one is found, the old one is gone.
    db.execute_cypher_with_params(
        "MATCH (n:Doc {key: 1}) SET n.embedding = $v",
        params(&[("v", vval(1001))]),
    )
    .unwrap();
    let n1 = node_by_key(db, 1);
    expect(exact_hit(db, 1001) == Some(n1), "SET vector not searchable");
    expect(
        exact_hit(db, 1).is_none(),
        "SET left the old vector in the index",
    );

    // REMOVE drops the node from the index.
    db.execute_cypher("MATCH (n:Doc {key: 2}) REMOVE n.embedding")
        .unwrap();
    expect(
        exact_hit(db, 2).is_none(),
        "REMOVE left the vector in the index",
    );

    // DETACH DELETE drops the node from the index.
    let before = index_len(db);
    db.execute_cypher("MATCH (n:Doc {key: 3}) DETACH DELETE n")
        .unwrap();
    expect(
        exact_hit(db, 3).is_none(),
        "DETACH DELETE: deleted node still a hit",
    );
    expect(
        index_len(db) == before.map(|n| n - 1),
        "DETACH DELETE left a dead index entry",
    );

    // CREATE with a vector while the index exists.
    db.execute_cypher_with_params(
        "CREATE (:Doc {key: 2000, embedding: $v})",
        params(&[("v", vval(2000))]),
    )
    .unwrap();
    expect(
        exact_hit(db, 2000) == Some(node_by_key(db, 2000)),
        "CREATE vector not searchable",
    );

    // MERGE that creates.
    db.execute_cypher_with_params(
        "MERGE (n:Doc {key: 2001}) ON CREATE SET n.embedding = $v",
        params(&[("v", vval(2001))]),
    )
    .unwrap();
    expect(
        exact_hit(db, 2001) == Some(node_by_key(db, 2001)),
        "MERGE vector not searchable",
    );

    // Adding the indexed label indexes an existing vector; removing it
    // un-indexes.
    db.execute_cypher_with_params(
        "CREATE (:Pending {key: 2002, embedding: $v})",
        params(&[("v", vval(2002))]),
    )
    .unwrap();
    db.execute_cypher("MATCH (n:Pending {key: 2002}) SET n:Doc")
        .unwrap();
    expect(
        exact_hit(db, 2002) == Some(node_by_key(db, 2002)),
        "SET label did not index the vector",
    );
    db.execute_cypher("MATCH (n:Doc {key: 4}) REMOVE n:Doc")
        .unwrap();
    expect(
        exact_hit(db, 4).is_none(),
        "REMOVE label left the node in the index",
    );

    let lost: Vec<i64> = (0..BASE as i64)
        .filter(|k| ![1, 2, 3, 4].contains(k))
        .filter(|k| exact_hit(db, *k as u64).is_none())
        .collect();
    expect(
        lost.is_empty(),
        &format!("base nodes lost from search: {lost:?}"),
    );
    drop(expect);
    assert!(
        errors.is_empty(),
        "{} failure(s):\n{}",
        errors.len(),
        errors.join("\n")
    );
}

#[test]
fn cypher_mutations_maintain_the_vector_index() {
    let db = GrafeoDB::new_in_memory();
    seed_graph(&db);
    run_cypher_suite(&db, "in-memory");
}

/// A rolled-back Cypher SET leaves the index as it was.
#[test]
fn rolled_back_cypher_set_leaves_the_index_unchanged() {
    let db = GrafeoDB::new_in_memory();
    seed_graph(&db);
    let mut session = db.session();
    session.begin_transaction().unwrap();
    session
        .execute_language(
            "MATCH (n:Doc {key: 5}) SET n.embedding = $v",
            "cypher",
            Some(params(&[("v", vval(5005))])),
        )
        .unwrap();
    session.rollback().unwrap();
    drop(session);
    assert_eq!(
        exact_hit(&db, 5005),
        None,
        "rolled-back vector is searchable"
    );
    assert_eq!(
        exact_hit(&db, 5),
        Some(node_by_key(&db, 5)),
        "original vector lost"
    );
    assert_base_intact(&db, &[], "after rollback");
}

/// The same suite on a plain file reopened with the ForceDisk tier (AMH's
/// production config): neighbours are spill-only (#175), and the
/// maintenance must survive a reopen.
#[test]
fn cypher_mutations_maintain_the_index_under_forcedisk() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("g.grafeo");
    let spill = dir.path().join("g.grafeo.spill");
    std::fs::create_dir_all(&spill).unwrap();
    {
        let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
        seed_graph(&db);
        db.close().unwrap();
    }
    let force_disk = |p: &Path| {
        Config::persistent(p)
            .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk)
            .with_spill_path(&spill)
    };
    {
        let db = GrafeoDB::with_config(force_disk(&path)).unwrap();
        run_cypher_suite(&db, "ForceDisk");
        db.close().unwrap();
    }
    let db = GrafeoDB::with_config(force_disk(&path)).unwrap();
    assert_eq!(
        exact_hit(&db, 1001),
        Some(node_by_key(&db, 1)),
        "after reopen: SET"
    );
    assert_eq!(exact_hit(&db, 2), None, "after reopen: REMOVE");
    assert_eq!(exact_hit(&db, 3), None, "after reopen: DETACH DELETE");
    assert_base_intact(&db, &[1, 2, 3, 4], "after ForceDisk reopen");
}

/// Part 2: on a writable generation root, removing a base-only node's
/// vector (no overlay row) must remove it from the index, through the
/// direct API and through Cypher.
#[cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "compact-store"
))]
#[test]
fn generation_root_base_only_vector_remove_reaches_the_index() {
    use grafeo_engine::generation_build_request;
    let dir = tempdir().unwrap();
    let root = dir.path().join("g.grafeo.d");
    std::fs::create_dir_all(&root).unwrap();
    let source = GrafeoDB::new_in_memory();
    seed_graph(&source);
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .unwrap();
    let db = GrafeoDB::open_generation_root(&root, false).unwrap();
    assert_base_intact(&db, &[], "fresh root");

    let n6 = node_by_key(&db, 6);
    assert!(
        db.remove_node_property(n6, "embedding"),
        "direct remove reported nothing"
    );
    assert_eq!(
        exact_hit(&db, 6),
        None,
        "direct remove of a base-only vector left it indexed"
    );

    db.execute_cypher("MATCH (n:Doc {key: 7}) REMOVE n.embedding")
        .unwrap();
    assert_eq!(
        exact_hit(&db, 7),
        None,
        "Cypher REMOVE of a base-only vector left it indexed"
    );

    db.execute_cypher("MATCH (n:Doc {key: 8}) DETACH DELETE n")
        .unwrap();
    assert_eq!(
        exact_hit(&db, 8),
        None,
        "Cypher DETACH DELETE of a base node left it indexed"
    );
    assert_base_intact(&db, &[6, 7, 8], "root after removals");
}

/// Re-embedding every node through Cypher must re-link the HNSW: a search
/// reads current vectors through the accessor, so a stale topology (links
/// from the old positions) only shows as lost recall at scale.
#[test]
fn cypher_reembed_relinks_the_hnsw() {
    const N: u64 = 400;
    let db = GrafeoDB::new_in_memory();
    for i in 0..N {
        db.create_node_with_props(
            &["Doc"],
            [("key", Value::Int64(i as i64)), ("embedding", vval(i))],
        )
        .unwrap();
    }
    db.create_vector_index(
        "Doc",
        "embedding",
        Some(DIMS),
        Some("cosine"),
        None,
        None,
        None,
    )
    .unwrap();
    for i in 0..N {
        db.execute_cypher_with_params(
            "MATCH (n:Doc {key: $key}) SET n.embedding = $v",
            params(&[("key", Value::Int64(i as i64)), ("v", vval(100_000 + i))]),
        )
        .unwrap();
    }
    let ids: Vec<NodeId> = (0..N).map(|i| node_by_key(&db, i as i64)).collect();
    let cos = |a: &[f32], b: &[f32]| {
        let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
        let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        1.0 - dot / (na * nb)
    };
    let current: Vec<Vec<f32>> = (0..N).map(|i| vector(100_000 + i)).collect();
    let mut total = 0.0;
    let mut not_self = 0;
    for (qi, q) in current.iter().enumerate() {
        let mut exact: Vec<(f32, NodeId)> = current
            .iter()
            .zip(&ids)
            .map(|(v, id)| (cos(q, v), *id))
            .collect();
        exact.sort_by(|a, b| a.0.total_cmp(&b.0));
        let want: std::collections::HashSet<NodeId> = exact.iter().take(10).map(|x| x.1).collect();
        let hits = db
            .vector_search("Doc", "embedding", q, 10, None, None)
            .unwrap();
        if hits.first().map(|h| h.0) != Some(ids[qi]) {
            not_self += 1;
        }
        total += hits.iter().filter(|h| want.contains(&h.0)).count() as f64 / 10.0;
    }
    let recall = total / N as f64;
    assert!(
        recall >= 0.95 && not_self == 0,
        "after a Cypher re-embed of every node: recall@10 {recall:.3}, {not_self} not their own top hit"
    );
}

/// AMH #189: on a writable generation root, nodes created by
/// `batch_create_nodes` / `batch_create_nodes_with_props` must be linked
/// into the HNSW (their neighbours are base nodes, readable only through the
/// merged view), and stay findable after a reopen. Fixed by fork #45
/// (`build_vector_accessor`); this pins it.
#[cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "compact-store"
))]
#[test]
fn generation_root_batch_created_vectors_are_searchable() {
    use grafeo_engine::generation_build_request;
    let dir = tempdir().unwrap();
    let root = dir.path().join("g.grafeo.d");
    std::fs::create_dir_all(&root).unwrap();
    let source = GrafeoDB::new_in_memory();
    seed_graph(&source);
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .unwrap();

    let batch: Vec<u64> = (3000..3012).collect();
    let props: Vec<u64> = (4000..4012).collect();
    let check = |db: &GrafeoDB, what: &str| {
        let missing: Vec<u64> = batch
            .iter()
            .chain(&props)
            .copied()
            .filter(|seed| exact_hit(db, *seed).is_none())
            .collect();
        assert!(
            missing.is_empty(),
            "{what}: batch-created seeds not found: {missing:?}"
        );
        assert_base_intact(db, &[], what);
    };
    {
        let db = GrafeoDB::open_generation_root(&root, false).unwrap();
        let ids = db.batch_create_nodes(
            "Doc",
            "embedding",
            batch.iter().map(|s| vector(*s)).collect(),
        );
        assert_eq!(ids.len(), batch.len());
        let rows = props
            .iter()
            .map(|s| {
                HashMap::from([
                    (
                        grafeo_common::types::PropertyKey::new("key"),
                        Value::Int64(*s as i64),
                    ),
                    (
                        grafeo_common::types::PropertyKey::new("embedding"),
                        vval(*s),
                    ),
                ])
            })
            .collect();
        assert_eq!(
            db.batch_create_nodes_with_props("Doc", rows).len(),
            props.len()
        );
        check(&db, "root, in-session");
        db.close().unwrap();
    }
    let db = GrafeoDB::open_generation_root(&root, false).unwrap();
    check(&db, "root, after reopen");
}
