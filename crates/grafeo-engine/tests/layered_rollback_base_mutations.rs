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
    /// edge id -> sorted properties
    edge_props: BTreeMap<u64, BTreeMap<String, String>>,
    /// `find_nodes_by_property("name", v)` for every base name (this is the
    /// property-index path when an index on `name` exists)
    name_lookup: BTreeMap<String, Vec<u64>>,
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
    let mut edge_props = BTreeMap::new();
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
            for (_, eid, _) in &adj {
                let props: BTreeMap<String, String> = store
                    .get_edge(grafeo_common::types::EdgeId::new(*eid))
                    .map(|e| {
                        e.properties
                            .iter()
                            .map(|(k, v)| (k.as_str().to_string(), format!("{v:?}")))
                            .collect()
                    })
                    .unwrap_or_default();
                edge_props.insert(*eid, props);
            }
            map.insert(id.as_u64(), adj);
        }
    }

    let mut by_label = BTreeMap::new();
    for label in ["Person", "Temp", "Extra"] {
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

    let mut name_lookup = BTreeMap::new();
    // The base names plus every name the test transactions write, so an
    // index left with a stale posting (old or new value) shows up.
    for &name in NAMES.iter().chain(&["gus-renamed", "tmp", "isolated", "b"]) {
        let mut ids: Vec<u64> = store
            .find_nodes_by_property("name", &Value::from(name))
            .into_iter()
            .map(|id| id.as_u64())
            .collect();
        ids.sort_unstable();
        name_lookup.insert(name.to_string(), ids);
    }

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
        edge_props,
        name_lookup,
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
    // `name` is indexed and MUTATIONS[1] renames gus: the rollback must
    // leave lookups of both the old and the new value as before.
    db.create_property_index("name");
    let before = snapshot(&db);
    assert_eq!(before.name_lookup["gus"].len(), 1, "sanity: gus indexed");
    assert!(before.name_lookup["gus-renamed"].is_empty(), "sanity");
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
    db.create_property_index("name");
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
    // Strip alix's base edges so alix is edge-less, then one NODETACH
    // DELETE over alix (id 0, scanned first) and jules (still connected):
    // alix is deleted, then the statement fails on jules.
    session
        .execute("MATCH (:Person {name: 'alix'})-[r]-() DELETE r")
        .expect("delete alix's edges");
    let err = session.execute("MATCH (n:Person) WHERE n.name IN ['alix', 'jules'] DELETE n");
    assert!(
        err.is_err(),
        "NODETACH delete of a connected node must fail"
    );
    // The failure really left the statement half-applied: alix's delete,
    // made before the failing row, is visible inside the transaction.
    let alix_left = session
        .execute("MATCH (n:Person {name: 'alix'}) RETURN count(n)")
        .expect("count alix");
    assert_eq!(
        alix_left.rows()[0][0],
        Value::Int64(0),
        "the first half of the failed statement must have been applied"
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

/// Engine-level test for the review's high-priority finding: on a layered
/// DB, `create_property_index` builds the overlay index with base postings
/// included. A rolled-back SET on a base node must not remove that node's
/// postings, so indexed lookups still find it.
#[test]
fn indexed_lookup_after_rollback_still_finds_base_node() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("root");
    publish_base(&root);
    let db = open_root(&root);
    db.create_property_index("name");
    let before = snapshot(&db);
    assert_eq!(before.name_lookup["gus"].len(), 1, "sanity: gus is indexed");

    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute("MATCH (n:Person {name: 'gus'}) SET n.age = 99")
        .expect("SET");
    session.rollback().expect("rollback");
    drop(session);

    assert_snapshot_eq("indexed lookup after ROLLBACK", &snapshot(&db), &before);
    let rows = db
        .session()
        .execute("MATCH (n:Person {name: 'gus'}) RETURN n.age")
        .expect("cypher lookup");
    assert_eq!(rows.rows().len(), 1, "Cypher property lookup finds gus");
}

/// Label SET/REMOVE, property REMOVE and edge-property SET through Cypher
/// (all go through `WalGraphStore`'s versioned mutators and copy-ups), then
/// ROLLBACK.
#[test]
fn cypher_label_remove_and_edge_set_rollback() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("root");
    publish_base(&root);
    let db = open_root(&root);
    let before = snapshot(&db);

    let mut session = db.session();
    session.begin_transaction().expect("begin");
    for q in [
        "MATCH (n:Person {name: 'gus'}) SET n:Extra",
        "MATCH (n:Person {name: 'vincent'}) REMOVE n.age",
        "MATCH (n:Person {name: 'mia'}) REMOVE n:Person",
        "MATCH ()-[r:KNOWS]->() SET r.w = 100",
    ] {
        session.execute(q).expect(q);
    }
    session.rollback().expect("rollback");
    drop(session);

    assert_snapshot_eq("labels/REMOVE/edge SET rollback", &snapshot(&db), &before);
    db.close().expect("close");
    drop(db);
    let db = open_root(&root);
    assert_snapshot_eq("labels/REMOVE/edge SET, reopen", &snapshot(&db), &before);
}

/// Live state after deleting only `alix` (the first MUTATION), for the
/// savepoint and nested-transaction tests.
fn snapshot_after_alix_only(root_parent: &Path) -> Snapshot {
    let root = root_parent.join("reference");
    publish_base(&root);
    let db = open_root(&root);
    db.session().execute(MUTATIONS[0]).expect("delete alix");
    snapshot(&db)
}

/// Savepoint rollback undoes only the layered changes made after the
/// savepoint; a full ROLLBACK afterwards restores everything.
#[test]
fn savepoint_rollback_undoes_only_later_base_mutations() {
    let dir = tempdir().expect("tempdir");
    let expected_mid = snapshot_after_alix_only(dir.path());
    let root = dir.path().join("root");
    publish_base(&root);
    let db = open_root(&root);
    let before = snapshot(&db);

    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session.execute(MUTATIONS[0]).expect("delete alix");
    session.savepoint("sp").expect("savepoint");
    for q in &MUTATIONS[1..] {
        session.execute(q).expect(q);
    }
    session
        .execute("MATCH ()-[r:KNOWS]->() SET r.w = 100")
        .expect("edge SET");
    session
        .rollback_to_savepoint("sp")
        .expect("rollback to savepoint");
    drop_session_view_check(&db, &expected_mid, "after ROLLBACK TO SAVEPOINT");
    session.rollback().expect("rollback");
    drop(session);

    assert_snapshot_eq("savepoint then full ROLLBACK", &snapshot(&db), &before);
    db.close().expect("close");
    drop(db);
    let db = open_root(&root);
    assert_snapshot_eq(
        "savepoint then full ROLLBACK, reopen",
        &snapshot(&db),
        &before,
    );
}

/// Compares the store-level (transaction-agnostic) view against `expected`.
/// Inside an open transaction the overlay's own uncommitted rows are hidden
/// from other sessions, so only the store-level fields are compared.
fn drop_session_view_check(db: &GrafeoDB, expected: &Snapshot, context: &str) {
    let layered = db.layered_store().expect("layered");
    let store: &dyn GraphStore = layered.as_ref();
    for (name, ids) in &expected.name_lookup {
        let mut got: Vec<u64> = store
            .find_nodes_by_property("name", &Value::from(name.as_str()))
            .into_iter()
            .map(|id| id.as_u64())
            .collect();
        got.sort_unstable();
        assert_eq!(&got, ids, "{context}: lookup of {name}");
    }
    assert_eq!(
        store.node_count(),
        expected.node_count,
        "{context}: node count"
    );
    assert_eq!(
        store.edge_count(),
        expected.edge_count,
        "{context}: edge count"
    );
    for (id, (_, props)) in &expected.nodes {
        let node = store
            .get_node(NodeId::new(*id))
            .unwrap_or_else(|| panic!("{context}: node {id} missing"));
        let got: BTreeMap<String, String> = node
            .properties
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), format!("{v:?}")))
            .collect();
        assert_eq!(&got, props, "{context}: node {id} properties");
    }
}

/// A nested transaction is an auto-savepoint: rolling the inner one back
/// must undo only its base mutations, and the outer COMMIT keeps the rest.
#[test]
fn nested_transaction_rollback_keeps_outer_base_mutations() {
    let dir = tempdir().expect("tempdir");
    let expected = snapshot_after_alix_only(dir.path());
    let root = dir.path().join("root");
    publish_base(&root);
    let db = open_root(&root);

    let mut session = db.session();
    session.begin_transaction().expect("begin outer");
    session.execute(MUTATIONS[0]).expect("delete alix");
    session.begin_transaction().expect("begin inner");
    for q in &MUTATIONS[1..] {
        session.execute(q).expect(q);
    }
    session.rollback().expect("rollback inner");
    session.commit().expect("commit outer");
    drop(session);

    // Live state only: the fork's WAL has no savepoint marker, so a reopen
    // after a partial rollback replays the undone records (see the PR's
    // write-up); that is out of scope here.
    assert_snapshot_eq("nested rollback + outer commit", &snapshot(&db), &expected);
}

/// Two sessions share a copy-up: the first rolls back, the second commits.
/// The committed edge must survive and the rolled-back SET must vanish.
#[test]
fn concurrent_sessions_share_a_copy_up() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("root");
    publish_base(&root);
    let db = open_root(&root);
    db.create_property_index("name");

    let mut a = db.session();
    let mut b = db.session();
    a.begin_transaction().expect("begin a");
    b.begin_transaction().expect("begin b");
    // `a` copies gus up for a SET; `b` relies on the same copy-up for a new
    // edge onto gus (no write-write conflict: an edge create does not record
    // a write on its endpoints).
    a.execute("MATCH (n:Person {name: 'gus'}) SET n.age = 40")
        .expect("a SET");
    b.execute("MATCH (g:Person {name: 'gus'}) CREATE (:Temp {name: 'b'})-[:NEW]->(g)")
        .expect("b CREATE edge");
    a.rollback().expect("rollback a");
    b.commit().expect("commit b");
    drop(a);
    drop(b);

    let check = |db: &GrafeoDB, context: &str| {
        let snap = snapshot(db);
        assert_eq!(snap.name_lookup["gus"].len(), 1, "{context}: gus indexed");
        let gus = &snap.nodes[&snap.name_lookup["gus"][0]].1;
        assert_eq!(
            gus.get("age").map(String::as_str),
            Some("Int64(31)"),
            "{context}"
        );
        let gus_id = snap.name_lookup["gus"][0];
        assert!(
            snap.incoming[&gus_id].iter().any(|(_, _, t)| t == "NEW"),
            "{context}: committed edge onto gus survives: {:?}",
            snap.incoming[&gus_id]
        );
    };
    // Live state only. A reopen loses `b`'s committed edge, but that is a
    // separate WAL bug that also reproduces on a plain single-file
    // database: WAL records carry no transaction id, so recovery drops the
    // committed records of `b` that interleave with the aborted `a` (see the
    // PR write-up).
    check(&db, "live");
}

/// Review round 2: when an overlay reset / merge drops a transaction's
/// pending base changes, its rollback cannot undo them. That must reach the
/// caller as an error (not only a log line an embedder may compile out),
/// for a full rollback and for a savepoint rollback.
#[test]
fn rollback_after_overlay_reset_reports_unrestored_changes() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("root");
    publish_base(&root);
    let db = open_root(&root);
    let layered = std::sync::Arc::clone(db.layered_store().expect("layered"));

    // Full rollback.
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session.execute(MUTATIONS[0]).expect("delete alix");
    layered.reset_overlay();
    let err = session
        .rollback()
        .expect_err("rollback must report the change it could not undo");
    assert!(
        err.to_string().contains("rollback incomplete"),
        "unexpected error: {err}"
    );
    assert!(!session.in_transaction(), "the transaction still ended");
    drop(session);

    // Savepoint rollback.
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session.savepoint("sp").expect("savepoint");
    session.execute(MUTATIONS[3]).expect("delete LIKES");
    layered.reset_overlay();
    let err = session
        .rollback_to_savepoint("sp")
        .expect_err("savepoint rollback must report it too");
    assert!(
        err.to_string().contains("rollback incomplete") && err.to_string().contains("still open"),
        "unexpected error: {err}"
    );
    assert!(
        session.in_transaction(),
        "a savepoint rollback leaves the transaction open"
    );
    // The loss stays recorded, so the full rollback reports it as well and
    // still ends the transaction.
    let err = session
        .rollback()
        .expect_err("the full rollback reports the same loss");
    assert!(
        err.to_string().contains("rollback incomplete") && !err.to_string().contains("still open"),
        "unexpected error: {err}"
    );
    assert!(!session.in_transaction());
    assert!(layered.forgotten_layer_changes() >= 2);
}

// ── Upstream #409 port: a commit that fails validation aborts fully ──────

/// Counts `TransactionAbort` records in the root WAL after the published
/// generation's boundary.
fn wal_abort_count(root: &Path) -> usize {
    use grafeo_storage::generation::manifest::read_manifest;
    use grafeo_storage::generation::wal_cursor::{WalReplayCursor, replay_stream_from};
    use grafeo_storage::wal::WalRecord;

    let (_, slot) = read_manifest(&root.join("manifest.bin")).expect("read manifest");
    let cursor = WalReplayCursor {
        log_sequence: slot.wal_log_sequence,
        byte_offset: slot.wal_byte_offset,
        epoch: slot.overlay_epoch,
        transaction_id: slot.transaction_id,
    };
    replay_stream_from(&root.join("wal"), &cursor)
        .expect("stream from boundary")
        .filter(|frame| {
            matches!(
                frame.as_ref().expect("frame decodes").record,
                WalRecord::TransactionAbort { .. }
            )
        })
        .count()
}

/// The losing transaction of [`failed_commit_on_generation_root_aborts_fully`]:
/// every kind of base mutation (node and edge tombstones, a copy-up SET, a
/// new overlay node with an edge onto a base node), then the write that
/// conflicts with the already committed winner.
const LOSER_MUTATIONS: [&str; 5] = [
    "MATCH (n:Person {name: 'alix'}) DETACH DELETE n",
    "MATCH (n:Person {name: 'vincent'}) SET n.age = 99",
    "MATCH (v:Person {name: 'vincent'}) CREATE (:Temp {name: 'tmp'})-[:NEW]->(v)",
    "MATCH (:Person {name: 'jules'})-[r:LIKES]->() DELETE r",
    "MATCH (n:Person {name: 'gus'}) SET n.age = 42",
];

/// Runs the write-write conflict on a generation root: `winner` commits a
/// SET on gus, then `loser` (begun earlier) applies [`LOSER_MUTATIONS`].
/// Returns the loser, with its commit not yet attempted, and the live
/// snapshot taken after the winner committed.
fn loser_after_winner(db: &GrafeoDB) -> (grafeo_engine::session::Session, Snapshot) {
    let mut loser = db.session();
    let mut winner = db.session();
    loser.begin_transaction().expect("begin loser");
    winner.begin_transaction().expect("begin winner");
    winner
        .execute("MATCH (n:Person {name: 'gus'}) SET n.age = 41")
        .expect("winner SET");
    winner.commit().expect("winner commits");
    drop(winner);
    let after_winner = snapshot(db);

    for q in LOSER_MUTATIONS {
        loser.execute(q).expect(q);
    }
    (loser, after_winner)
}

/// A commit that fails validation on a generation root is a full abort:
/// overlay versions are discarded, base tombstones and copy-ups are undone,
/// a `TransactionAbort` is logged, the session has no transaction left, the
/// entities are released so a retry succeeds, and a later commit marker does
/// not settle any of the failed transaction's records on replay.
#[test]
fn failed_commit_on_generation_root_aborts_fully() {
    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("root");
    publish_base(&root);
    let db = open_root(&root);
    db.create_property_index("name");

    let (mut loser, after_winner) = loser_after_winner(&db);
    let aborts_before = wal_abort_count(&root);
    let err = loser
        .commit()
        .expect_err("the second writer of gus must fail validation");
    assert!(
        err.to_string().to_lowercase().contains("conflict"),
        "unexpected error: {err}"
    );

    assert!(
        !loser.in_transaction(),
        "the failed commit ended the transaction"
    );
    assert!(
        loser.rollback().is_err(),
        "there is no transaction left to roll back"
    );
    assert_eq!(
        wal_abort_count(&root),
        aborts_before + 1,
        "the failed commit logs exactly one TransactionAbort"
    );
    assert_snapshot_eq(
        "live state after the failed commit",
        &snapshot(&db),
        &after_winner,
    );

    // The entities the loser wrote are released: the same session retries
    // writes to all of them and commits.
    loser.begin_transaction().expect("begin retry");
    loser
        .execute("MATCH (n:Person {name: 'vincent'}) SET n.age = 50")
        .expect("retry SET vincent");
    loser
        .execute("MATCH (n:Person {name: 'gus'}) SET n.age = 43")
        .expect("retry SET gus");
    loser
        .execute("MATCH (n:Person {name: 'alix'}) DETACH DELETE n")
        .expect("retry DELETE alix");
    loser.commit().expect("retry commits");
    drop(loser);

    let after_retry = snapshot(&db);
    assert!(after_retry.by_label["Temp"].is_empty(), "no Temp node");
    assert!(
        after_retry
            .cypher_edges
            .iter()
            .any(|(_, t, _)| t.contains("LIKES")),
        "the LIKES tombstone was undone"
    );

    // The retry's commit marker follows the failed transaction's records in
    // the WAL; replay must not settle them as committed.
    db.close().expect("close");
    drop(db);
    let db = open_root(&root);
    db.create_property_index("name");
    assert_snapshot_eq("after close + reopen", &snapshot(&db), &after_retry);
}

/// A failed commit whose abort marker cannot be appended does not pass
/// silently: the WAL is poisoned (a later commit marker could otherwise
/// settle the failed transaction's records on replay), later writes are
/// refused, and the failed transaction is not there after reopen.
#[cfg(feature = "testing-crash-injection")]
#[test]
fn failed_commit_whose_abort_marker_fails_poisons_the_wal() {
    use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_from};
    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("root");
    publish_base(&root);
    let after_winner;
    {
        let db = open_root(&root);
        let (mut loser, snap) = loser_after_winner(&db);
        after_winner = snap;
        // Validation fails before anything is appended, so the abort
        // marker is the next append.
        enable_io_failure_from(1);
        let r = loser.commit();
        disable_io_failure();
        let err = r.expect_err("the commit still fails");
        assert!(
            err.to_string().to_lowercase().contains("conflict"),
            "the caller still gets the conflict: {err}"
        );
        assert!(!loser.in_transaction());
        assert_snapshot_eq("live, abort marker failed", &snapshot(&db), &after_winner);
        assert!(
            db.session()
                .execute("MATCH (n:Person {name: 'gus'}) SET n.age = 43")
                .is_err(),
            "the WAL refuses writes after the abort marker failed"
        );
        drop(loser);
        drop(db);
    }
    let db = open_root(&root);
    assert_snapshot_eq("after reopen", &snapshot(&db), &after_winner);
}

/// An explicit rollback whose abort marker cannot be appended reports an
/// error instead of `Ok`, still ends the transaction, and poisons the WAL.
#[cfg(feature = "testing-crash-injection")]
#[test]
fn rollback_whose_abort_marker_fails_reports_an_error() {
    use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_from};
    let dir = tempdir().expect("tempdir");
    let root = dir.path().join("root");
    publish_base(&root);
    let db = open_root(&root);
    let before = snapshot(&db);

    let mut session = db.session();
    session.begin_transaction().expect("begin");
    for q in MUTATIONS {
        session.execute(q).expect(q);
    }
    enable_io_failure_from(1);
    let r = session.rollback();
    disable_io_failure();
    let err = r.expect_err("a lost abort marker must not be reported as success");
    assert!(
        err.to_string().contains("abort marker"),
        "unexpected error: {err}"
    );
    assert!(!session.in_transaction(), "the transaction still ended");
    assert_snapshot_eq("live after rollback", &snapshot(&db), &before);
    assert!(
        session.execute("INSERT (:After)").is_err(),
        "the WAL is poisoned"
    );
    // Rolling back once the WAL is already poisoned is not an error: the
    // poison already refuses every later append.
    session.begin_transaction().expect("begin after poison");
    session
        .rollback()
        .expect("rollback on an already poisoned WAL");
}
