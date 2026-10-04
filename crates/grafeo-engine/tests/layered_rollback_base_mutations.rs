//! A rolled-back transaction on a generation root must leave the LIVE
//! database exactly as it was before the transaction began.
//!
//! A generation root serves a layered store: a read-only `CompactStore` base
//! (the published generation) under a writable `LpgStore` overlay. Mutating a
//! base entity does not touch the base; instead the layered store records:
//!
//! * a base **tombstone** (`deleted_from_base_*`) for a deleted base node/edge,
//! * a **copy-up** of a base node/edge into the overlay (marked dirty) before
//!   a SET / label change / new incident edge,
//! * the SET itself, applied to the overlay copy.
//!
//! The overlay `LpgStore` versions its own rows by transaction id, so its
//! rollback is exact. These tests pin the layered bookkeeping to the same
//! contract: after `ROLLBACK`, the same live handle must report the
//! pre-transaction node/edge counts, properties, labels and adjacency (both
//! directions), and so must a close + reopen.
//!
//! ```bash
//! cargo test -p grafeo-engine --test layered_rollback_base_mutations \
//!   --features generation,generation-streaming,compact-store,lpg,mmap,wal,cypher
//! ```

#![cfg(all(
    feature = "generation",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::collections::BTreeMap;
use std::path::Path;

use grafeo_common::types::{NodeId, Value};
use grafeo_core::graph::Direction;
use grafeo_core::graph::GraphStore;
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::tempdir;

/// Base graph published as the generation:
///
/// ```text
///   alix -KNOWS-> gus -KNOWS-> vincent -KNOWS-> jules -LIKES-> mia -KNOWS-> alix
///   gus -KNOWS-> jules
/// ```
const NAMES: [&str; 5] = ["alix", "gus", "vincent", "jules", "mia"];

fn publish_base(root: &Path) {
    std::fs::create_dir_all(root).expect("create root dir");
    let source = GrafeoDB::new_in_memory();
    let mut ids = Vec::new();
    for (i, name) in NAMES.iter().enumerate() {
        let id = source
            .create_node_with_props(
                &["Person"],
                [
                    ("name", Value::from(*name)),
                    ("age", Value::from(30 + i as i64)),
                ],
            )
            .expect("create base node");
        ids.push(id);
    }
    let edges = [
        (0, 1, "KNOWS"),
        (1, 2, "KNOWS"),
        (2, 3, "KNOWS"),
        (3, 4, "LIKES"),
        (4, 0, "KNOWS"),
        (1, 3, "KNOWS"),
    ];
    for (i, (s, d, t)) in edges.iter().enumerate() {
        source.create_edge_with_props(ids[*s], ids[*d], t, [("w", Value::from(i as i64))]);
    }
    source
        .build_and_publish_generation(generation_build_request(root, "g-base"))
        .expect("publish base generation");
    drop(source);
}

/// Everything a reader can observe about the graph, keyed by stable names.
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    node_count: usize,
    edge_count: usize,
    cypher_node_count: i64,
    cypher_edge_count: i64,
    /// id -> (sorted labels, sorted properties)
    nodes: BTreeMap<u64, (Vec<String>, BTreeMap<String, String>)>,
    /// id -> sorted (neighbor id, edge id, edge type) per direction
    outgoing: BTreeMap<u64, Vec<(u64, u64, String)>>,
    incoming: BTreeMap<u64, Vec<(u64, u64, String)>>,
    /// Cypher-visible edges: sorted (src name, type, dst name)
    cypher_edges: Vec<(String, String, String)>,
    /// label -> sorted ids
    by_label: BTreeMap<String, Vec<u64>>,
}

fn cypher_count(db: &GrafeoDB, query: &str) -> i64 {
    let result = db.session().execute(query).expect("count query");
    match &result.rows()[0][0] {
        Value::Int64(v) => *v,
        other => panic!("expected integer count, got {other:?}"),
    }
}

fn snapshot(db: &GrafeoDB) -> Snapshot {
    let layered = db.layered_store().expect("generation root is layered");
    let store: &dyn GraphStore = layered.as_ref();
    let mut node_ids = store.node_ids();
    node_ids.sort_unstable();

    let mut nodes = BTreeMap::new();
    let mut outgoing = BTreeMap::new();
    let mut incoming = BTreeMap::new();
    for id in &node_ids {
        let node = store
            .get_node(*id)
            .unwrap_or_else(|| panic!("node_ids lists {id:?} but get_node misses it"));
        let mut labels: Vec<String> = node.labels.iter().map(|l| l.to_string()).collect();
        labels.sort_unstable();
        let props: BTreeMap<String, String> = node
            .properties
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), format!("{v:?}")))
            .collect();
        nodes.insert(id.as_u64(), (labels, props));
        for (dir, map) in [
            (Direction::Outgoing, &mut outgoing),
            (Direction::Incoming, &mut incoming),
        ] {
            let mut adj: Vec<(u64, u64, String)> = store
                .edges_from(*id, dir)
                .into_iter()
                .map(|(other, eid)| {
                    let ty = store
                        .edge_type(eid)
                        .map(|t| t.to_string())
                        .unwrap_or_else(|| "<missing>".to_string());
                    (other.as_u64(), eid.as_u64(), ty)
                })
                .collect();
            adj.sort_unstable();
            map.insert(id.as_u64(), adj);
        }
    }

    let mut by_label = BTreeMap::new();
    for label in ["Person", "Temp"] {
        let mut ids: Vec<u64> = store
            .nodes_by_label(label)
            .into_iter()
            .map(|id| id.as_u64())
            .collect();
        ids.sort_unstable();
        by_label.insert(label.to_string(), ids);
    }

    let edge_rows = db
        .session()
        .execute("MATCH (a)-[r]->(b) RETURN a.name, type(r), b.name")
        .expect("edge scan");
    let mut cypher_edges: Vec<(String, String, String)> = edge_rows
        .rows()
        .iter()
        .map(|row| {
            (
                format!("{:?}", row[0]),
                format!("{:?}", row[1]),
                format!("{:?}", row[2]),
            )
        })
        .collect();
    cypher_edges.sort_unstable();

    Snapshot {
        node_count: store.node_count(),
        edge_count: store.edge_count(),
        cypher_node_count: cypher_count(db, "MATCH (n) RETURN count(n)"),
        cypher_edge_count: cypher_count(db, "MATCH ()-[r]->() RETURN count(r)"),
        nodes,
        outgoing,
        incoming,
        cypher_edges,
        by_label,
    }
}

/// The four base mutations from the bug report, as Cypher in one transaction:
/// DETACH DELETE a base node, SET on another base node, a new edge onto a
/// third base node (from a new overlay node), and delete of a base edge.
const MUTATIONS: [&str; 4] = [
    "MATCH (n:Person {name: 'alix'}) DETACH DELETE n",
    "MATCH (n:Person {name: 'gus'}) SET n.age = 99, n.name = 'gus-renamed'",
    "MATCH (v:Person {name: 'vincent'}) CREATE (:Temp {name: 'tmp'})-[:NEW]->(v)",
    "MATCH (:Person {name: 'jules'})-[r:LIKES]->() DELETE r",
];

fn open_root(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, false).expect("open generation root writable")
}

fn assert_snapshot_eq(context: &str, actual: &Snapshot, expected: &Snapshot) {
    assert_eq!(
        actual, expected,
        "{context}: live state differs from the pre-transaction snapshot"
    );
}

#[test]
fn cypher_rollback_restores_live_state_and_reopen() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("root");
    publish_base(&root);

    let db = open_root(&root);
    let before = snapshot(&db);
    assert_eq!(before.node_count, 5, "sanity: base nodes");
    assert_eq!(before.edge_count, 6, "sanity: base edges");

    let mut session = db.session();
    session.begin_transaction().expect("begin");
    for q in MUTATIONS {
        session.execute(q).expect(q);
    }
    session.rollback().expect("rollback");
    drop(session);

    assert_snapshot_eq("same live handle after ROLLBACK", &snapshot(&db), &before);

    // A retry of the identical operations must behave like a first attempt.
    let mut session = db.session();
    session.begin_transaction().expect("begin retry");
    for q in MUTATIONS {
        session.execute(q).expect(q);
    }
    session.rollback().expect("rollback retry");
    drop(session);
    assert_snapshot_eq(
        "after a second rolled-back attempt",
        &snapshot(&db),
        &before,
    );

    db.close().expect("close");
    drop(db);
    let db = open_root(&root);
    assert_snapshot_eq("after close + reopen", &snapshot(&db), &before);
}

/// The session's direct mutation API (`Session::set_node_property`,
/// `delete_node`, ...) inside an explicit transaction, then rollback.
#[test]
fn session_direct_api_rollback_restores_live_state() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("root");
    publish_base(&root);
    let db = open_root(&root);
    let before = snapshot(&db);
    let layered = std::sync::Arc::clone(db.layered_store().expect("layered"));
    let find =
        |name: &str| -> NodeId { layered.find_nodes_by_property("name", &Value::from(name))[0] };

    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .set_node_property(find("gus"), "age", Value::from(99i64))
        .expect("set");
    let tmp = session.create_node(&["Temp"]);
    session.create_edge(tmp, find("vincent"), "NEW");
    session.delete_node(find("mia"));
    session.rollback().expect("rollback");
    drop(session);

    assert_snapshot_eq("session direct API", &snapshot(&db), &before);
    db.close().expect("close");
    drop(db);
    let db = open_root(&root);
    assert_snapshot_eq("session direct API after reopen", &snapshot(&db), &before);
}

/// A statement fails half-way through an explicit transaction (after an
/// earlier statement and part of itself already mutated base entities); the
/// caller then rolls back.
#[test]
fn rollback_after_partial_failure_mid_transaction() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("root");
    publish_base(&root);
    let db = open_root(&root);
    let before = snapshot(&db);

    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session.execute(MUTATIONS[1]).expect("SET");
    session.execute(MUTATIONS[2]).expect("CREATE edge");
    session.execute(MUTATIONS[3]).expect("DELETE edge");
    // A NODETACH delete of every node deletes the edge-less ones it reaches
    // first (the new isolated node) and then fails on a node that still has
    // edges, leaving the statement half-applied.
    session
        .execute("CREATE (:Temp {name: 'isolated'})")
        .expect("create isolated");
    let err = session.execute("MATCH (n) DELETE n");
    assert!(
        err.is_err(),
        "NODETACH delete over connected nodes must fail"
    );
    session.rollback().expect("rollback");
    drop(session);

    assert_snapshot_eq("after partial failure + ROLLBACK", &snapshot(&db), &before);
    db.close().expect("close");
    drop(db);
    let db = open_root(&root);
    assert_snapshot_eq("after partial failure, reopen", &snapshot(&db), &before);
}

/// Regression guard: COMMIT of the same operations still applies them, on the
/// live handle and after reopen.
#[test]
fn commit_of_same_operations_still_applies() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("root");
    publish_base(&root);
    let db = open_root(&root);

    let mut session = db.session();
    session.begin_transaction().expect("begin");
    for q in MUTATIONS {
        session.execute(q).expect(q);
    }
    session.commit().expect("commit");
    drop(session);

    let check = |db: &GrafeoDB, context: &str| {
        let snap = snapshot(db);
        assert_eq!(snap.node_count, 5, "{context}: 5 - alix + tmp");
        assert_eq!(snap.cypher_node_count, 5, "{context}: cypher node count");
        // 6 base - 2 (alix detached) - 1 (LIKES) + 1 (NEW)
        assert_eq!(snap.edge_count, 4, "{context}: edge count");
        let names: Vec<String> = snap
            .nodes
            .values()
            .map(|(_, p)| p.get("name").cloned().unwrap_or_default())
            .collect();
        assert!(
            !names.iter().any(|n| n.contains("\"alix\"")),
            "{context}: alix deleted: {names:?}"
        );
        let gus = snap
            .nodes
            .values()
            .find(|(_, p)| p.get("name").is_some_and(|n| n.contains("gus-renamed")))
            .unwrap_or_else(|| panic!("{context}: gus renamed: {names:?}"));
        assert!(
            gus.1.get("age").is_some_and(|a| a.contains("99")),
            "{context}: gus.age = 99: {:?}",
            gus.1
        );
        assert_eq!(snap.by_label["Temp"].len(), 1, "{context}: tmp node");
        assert!(
            snap.cypher_edges
                .iter()
                .any(|(s, t, d)| s.contains("tmp") && t.contains("NEW") && d.contains("vincent")),
            "{context}: NEW edge: {:?}",
            snap.cypher_edges
        );
        assert!(
            !snap
                .cypher_edges
                .iter()
                .any(|(_, t, _)| t.contains("LIKES")),
            "{context}: LIKES deleted: {:?}",
            snap.cypher_edges
        );
        // Both directions agree on every edge.
        for (src, out) in &snap.outgoing {
            for (dst, eid, _) in out {
                assert!(
                    snap.incoming[dst]
                        .iter()
                        .any(|(s, e, _)| s == src && e == eid),
                    "{context}: edge {eid} {src}->{dst} missing from incoming adjacency"
                );
            }
        }
        snap
    };
    let live = check(&db, "live after COMMIT");
    db.close().expect("close");
    drop(db);
    let db = open_root(&root);
    let reopened = check(&db, "after COMMIT + reopen");
    assert_eq!(live.nodes, reopened.nodes, "node payloads survive reopen");
}
