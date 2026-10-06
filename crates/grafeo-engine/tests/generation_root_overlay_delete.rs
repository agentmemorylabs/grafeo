//! Deleting overlay rows on a writable generation root (AMH #161).
//!
//! On a writable generation root a node or edge written to the overlay after
//! open could not be deleted by a later transaction: `Session::delete_edge`
//! and `delete_node` returned `false`, and a Cypher `MATCH ... DELETE r`
//! returned `Ok` and deleted nothing. Base rows and plain databases were fine.
//!
//! Cases A-G are the case table from AMH #161 (found by the lane-2
//! equivalence checker). The rest pin that a delete of an overlay row
//! survives reopen and an epoch handoff, and that rolling a delete back
//! restores the row.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::path::{Path, PathBuf};

use grafeo_common::types::{EdgeId, NodeId, Value};
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::TempDir;

const REL: &str = "MemoryEntityRelation";

/// Publishes a base generation with `MemoryEntity` nodes `a`, `b` and one
/// base edge `a -[:MemoryEntityRelation {rel_type: 'base'}]-> b`.
fn fresh_root() -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("overlay-delete.grafeo.d");
    std::fs::create_dir_all(&root).expect("create root");
    let source = GrafeoDB::new_in_memory();
    source
        .execute_cypher(
            "CREATE (a:MemoryEntity {name: 'a'}), (b:MemoryEntity {name: 'b'}), \
             (a)-[:MemoryEntityRelation {rel_type: 'base'}]->(b)",
        )
        .expect("seed base");
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .expect("publish base generation");
    (dir, root)
}

fn open(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, false).expect("open generation root writable")
}

fn as_u64(value: &Value) -> u64 {
    match value {
        Value::Int64(v) => u64::try_from(*v).expect("non-negative id"),
        other => panic!("expected an integer id, got {other:?}"),
    }
}

fn node(db: &GrafeoDB, name: &str) -> NodeId {
    let r = db
        .execute_cypher(&format!(
            "MATCH (n:MemoryEntity {{name: '{name}'}}) RETURN id(n)"
        ))
        .expect("node lookup");
    assert_eq!(r.row_count(), 1, "exactly one node named {name}");
    NodeId::new(as_u64(&r.rows()[0][0]))
}

fn count(db: &GrafeoDB, query: &str) -> i64 {
    let r = db.execute_cypher(query).expect(query);
    match &r.rows()[0][0] {
        Value::Int64(v) => *v,
        other => panic!("expected a count, got {other:?}"),
    }
}

/// The AMH-shaped relation match, `a -[rel_type]-> b`.
fn rel_match(rel_type: &str) -> String {
    format!(
        "MATCH (a:MemoryEntity {{name: 'a'}})-[r:{REL} {{rel_type: '{rel_type}'}}]->\
         (b:MemoryEntity {{name: 'b'}})"
    )
}

fn rel_count(db: &GrafeoDB, rel_type: &str) -> i64 {
    count(db, &format!("{} RETURN count(r)", rel_match(rel_type)))
}

fn entity_count(db: &GrafeoDB, name: &str) -> i64 {
    count(
        db,
        &format!("MATCH (n:MemoryEntity {{name: '{name}'}}) RETURN count(n)"),
    )
}

/// Creates overlay edge `a -[rel_type]-> b`, in its own transaction when
/// `in_txn`, otherwise in autocommit.
fn create_rel(db: &GrafeoDB, rel_type: &str, in_txn: bool) -> EdgeId {
    let (a, b) = (node(db, "a"), node(db, "b"));
    let mut session = db.session();
    if in_txn {
        session.begin_transaction().expect("begin");
    }
    let id = session
        .create_edge_with_props(a, b, REL, [("rel_type", Value::from(rel_type))])
        .expect("create overlay edge");
    if in_txn {
        session.commit().expect("commit create");
    }
    assert_eq!(
        rel_count(db, rel_type),
        1,
        "overlay edge {rel_type} visible"
    );
    id
}

/// Creates overlay node `name` in its own committed transaction.
fn create_entity(db: &GrafeoDB, name: &str) -> NodeId {
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    let id = session
        .create_node_with_props(&["MemoryEntity"], [("name", Value::from(name))])
        .expect("create overlay node");
    session.commit().expect("commit create");
    assert_eq!(entity_count(db, name), 1, "overlay node {name} visible");
    id
}

/// Deletes edge `id` directly, in a transaction when `in_txn`.
fn delete_edge(db: &GrafeoDB, id: EdgeId, in_txn: bool) -> bool {
    let mut session = db.session();
    if !in_txn {
        return session.delete_edge(id);
    }
    session.begin_transaction().expect("begin");
    let deleted = session.delete_edge(id);
    session.commit().expect("commit delete");
    deleted
}

fn assert_edge_gone(db: &GrafeoDB, id: EdgeId, rel_type: &str, stage: &str) {
    assert!(db.session().get_edge(id).is_none(), "[{stage}] get_edge");
    assert_eq!(rel_count(db, rel_type), 0, "[{stage}] Cypher count");
}

// ── Case table from AMH #161 ──────────────────────────────────────────

/// A: edge created in a committed transaction, deleted in a later one.
#[test]
fn a_overlay_edge_from_committed_txn_deletes_in_txn() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    let e = create_rel(&db, "x", true);
    assert!(delete_edge(&db, e, true), "delete_edge returned false");
    assert_edge_gone(&db, e, "x", "A");
}

/// B: the AMH-shaped Cypher `DELETE r` in a later transaction.
#[test]
fn b_overlay_edge_cypher_delete_in_txn() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    let e = create_rel(&db, "x", true);
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher(&format!("{} DELETE r", rel_match("x")))
        .expect("Cypher DELETE r");
    session.commit().expect("commit");
    assert_edge_gone(&db, e, "x", "B");
}

/// B, autocommit: the same Cypher `DELETE r` with no explicit transaction.
#[test]
fn b_overlay_edge_cypher_delete_autocommit() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    let e = create_rel(&db, "x", true);
    db.execute_cypher(&format!("{} DELETE r", rel_match("x")))
        .expect("Cypher DELETE r");
    assert_edge_gone(&db, e, "x", "B autocommit");
}

/// C: edge created in autocommit, deleted in a transaction.
#[test]
fn c_overlay_edge_from_autocommit_deletes_in_txn() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    let e = create_rel(&db, "x", false);
    assert!(delete_edge(&db, e, true), "delete_edge returned false");
    assert_edge_gone(&db, e, "x", "C");
}

/// D: edge created in a transaction, deleted in autocommit.
#[test]
fn d_overlay_edge_from_txn_deletes_in_autocommit() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    let e = create_rel(&db, "x", true);
    assert!(delete_edge(&db, e, false), "delete_edge returned false");
    assert_edge_gone(&db, e, "x", "D");
}

/// E (guard): a base edge deletes in a transaction.
#[test]
fn e_base_edge_deletes_in_txn() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    let r = db
        .execute_cypher(&format!("{} RETURN id(r)", rel_match("base")))
        .expect("base edge id");
    let e = EdgeId::new(as_u64(&r.rows()[0][0]));
    assert!(
        delete_edge(&db, e, true),
        "delete_edge(base) returned false"
    );
    assert_edge_gone(&db, e, "base", "E");
}

/// F: a node created in a committed transaction, deleted in a later one.
#[test]
fn f_overlay_node_from_committed_txn_deletes_in_txn() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    let n = create_entity(&db, "c");
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    assert!(session.delete_node(n), "delete_node returned false");
    session.commit().expect("commit");
    assert!(db.session().get_node(n).is_none(), "get_node");
    assert_eq!(entity_count(&db, "c"), 0, "Cypher count");
}

/// F, Cypher: `DETACH DELETE` of an overlay node with an overlay edge.
#[test]
fn f_overlay_node_cypher_detach_delete_in_txn() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    let c = create_entity(&db, "c");
    let a = node(&db, "a");
    let e = db
        .session()
        .create_edge_with_props(a, c, REL, [("rel_type", Value::from("to_c"))])
        .expect("create overlay edge to c");
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher("MATCH (n:MemoryEntity {name: 'c'}) DETACH DELETE n")
        .expect("Cypher DETACH DELETE");
    session.commit().expect("commit");
    assert_eq!(entity_count(&db, "c"), 0, "node c");
    assert!(db.session().get_edge(e).is_none(), "edge to c");
    assert_eq!(entity_count(&db, "a"), 1, "base node a untouched");
}

/// G (guard): case A on an in-memory database.
#[test]
fn g_in_memory_edge_from_committed_txn_deletes_in_txn() {
    let db = GrafeoDB::new_in_memory();
    db.execute_cypher("CREATE (:MemoryEntity {name: 'a'}), (:MemoryEntity {name: 'b'})")
        .expect("seed");
    let e = create_rel(&db, "x", true);
    assert!(delete_edge(&db, e, true), "delete_edge returned false");
    assert_edge_gone(&db, e, "x", "G");
}

// ── Durability: reopen, epoch handoff, rollback ───────────────────────

/// Overlay deletes (direct and Cypher, edge and node) survive close + reopen.
#[test]
fn overlay_deletes_survive_reopen() {
    let (_dir, root) = fresh_root();
    let (x, c) = {
        let db = open(&root);
        let x = create_rel(&db, "x", true);
        let _y = create_rel(&db, "y", true);
        let c = create_entity(&db, "c");
        assert!(delete_edge(&db, x, true), "delete_edge(x)");
        db.execute_cypher(&format!("{} DELETE r", rel_match("y")))
            .expect("Cypher DELETE y");
        assert!(db.session().delete_node(c), "delete_node(c)");
        db.close().expect("close");
        (x, c)
    };
    let db = open(&root);
    assert_edge_gone(&db, x, "x", "reopen");
    assert_eq!(rel_count(&db, "y"), 0, "[reopen] y");
    assert!(db.session().get_node(c).is_none(), "[reopen] get_node(c)");
    assert_eq!(entity_count(&db, "c"), 0, "[reopen] c");
    assert_eq!(rel_count(&db, "base"), 1, "[reopen] base edge untouched");
}

/// Runs one epoch handoff and installs the new base.
fn handoff(db: &GrafeoDB, root: &Path, generation: &str) {
    let report = db
        .run_epoch_handoff(generation_build_request(root, generation))
        .expect("epoch handoff");
    db.publish_and_install_handoff(report)
        .expect("publish and install handoff");
}

/// Overlay rows deleted before an epoch handoff stay deleted in the new base
/// and after reopen.
#[test]
fn overlay_deletes_survive_epoch_handoff() {
    let (_dir, root) = fresh_root();
    {
        let db = open(&root);
        let x = create_rel(&db, "x", true);
        let c = create_entity(&db, "c");
        assert!(delete_edge(&db, x, true), "delete_edge(x)");
        let mut session = db.session();
        session.begin_transaction().expect("begin");
        assert!(session.delete_node(c), "delete_node(c)");
        session.commit().expect("commit");
        handoff(&db, &root, "g2");
        assert_eq!(rel_count(&db, "x"), 0, "[handoff] x");
        assert_eq!(entity_count(&db, "c"), 0, "[handoff] c");
        db.close().expect("close");
    }
    let db = open(&root);
    assert_eq!(rel_count(&db, "x"), 0, "[reopen] x");
    assert_eq!(entity_count(&db, "c"), 0, "[reopen] c");
    assert_eq!(rel_count(&db, "base"), 1, "[reopen] base edge untouched");
}

/// Overlay rows a handoff absorbed into the new base (and that stay, not
/// dirty, in the overlay after the install) delete after the install and stay
/// deleted after reopen.
#[test]
fn overlay_rows_absorbed_by_handoff_delete_after_install() {
    let (_dir, root) = fresh_root();
    {
        let db = open(&root);
        let x = create_rel(&db, "x", true);
        let _y = create_rel(&db, "y", false);
        let c = create_entity(&db, "c");
        handoff(&db, &root, "g2");
        assert_eq!(rel_count(&db, "x"), 1, "[install] x absorbed");
        assert!(delete_edge(&db, x, true), "delete_edge(absorbed x)");
        db.execute_cypher(&format!("{} DELETE r", rel_match("y")))
            .expect("Cypher DELETE absorbed y");
        let mut session = db.session();
        session.begin_transaction().expect("begin");
        assert!(session.delete_node(c), "delete_node(absorbed c)");
        session.commit().expect("commit");
        assert_eq!(rel_count(&db, "x"), 0, "[delete] x");
        assert_eq!(rel_count(&db, "y"), 0, "[delete] y");
        assert_eq!(entity_count(&db, "c"), 0, "[delete] c");
        db.close().expect("close");
    }
    let db = open(&root);
    assert_eq!(rel_count(&db, "x"), 0, "[reopen] x");
    assert_eq!(rel_count(&db, "y"), 0, "[reopen] y");
    assert_eq!(entity_count(&db, "c"), 0, "[reopen] c");
}

/// Not #161: writes between `run_epoch_handoff` (which retires the handoff)
/// and `publish_and_install_handoff` are outside the install's quiesced
/// contract, but nothing tracks or refuses them, so the install's repair
/// swap undirties the frozen row and the new base serves it again. Pinned
/// here with rows created through Cypher (dirty from the start), which
/// #161 never affected, to show the gap is independent of this fix.
#[test]
#[ignore = "pre-existing gap: writes between handoff retire and install are \
            not tracked; see the fork PR for #161"]
fn delete_between_handoff_retire_and_install_is_kept() {
    let (_dir, root) = fresh_root();
    let db = open(&root);
    db.execute_cypher(
        "MATCH (a:MemoryEntity {name: 'a'}), (b:MemoryEntity {name: 'b'}) \
         CREATE (a)-[:MemoryEntityRelation {rel_type: 'x'}]->(b)",
    )
    .expect("Cypher create x");
    let report = db
        .run_epoch_handoff(generation_build_request(&root, "g2"))
        .expect("epoch handoff");
    db.execute_cypher(&format!("{} DELETE r", rel_match("x")))
        .expect("Cypher DELETE x after retire");
    assert_eq!(rel_count(&db, "x"), 0, "[before install] x");
    db.publish_and_install_handoff(report)
        .expect("publish and install handoff");
    assert_eq!(rel_count(&db, "x"), 0, "[install] x");
}

/// Rolling back a delete of a committed overlay row restores it, also after
/// reopen (fork #14: rollback must not drop or keep the wrong rows).
#[test]
fn rollback_of_overlay_delete_restores_rows() {
    let (_dir, root) = fresh_root();
    let (x, c) = {
        let db = open(&root);
        let x = create_rel(&db, "x", true);
        let c = create_entity(&db, "c");
        let mut session = db.session();
        session.begin_transaction().expect("begin");
        assert!(session.delete_edge(x), "delete_edge(x)");
        assert!(session.delete_node(c), "delete_node(c)");
        assert!(session.get_edge(x).is_none(), "own delete visible in txn");
        session
            .execute_cypher(&format!("{} DELETE r", rel_match("base")))
            .expect("Cypher DELETE base");
        session.rollback().expect("rollback");
        assert!(db.session().get_edge(x).is_some(), "[rollback] get_edge(x)");
        assert_eq!(rel_count(&db, "x"), 1, "[rollback] x");
        assert_eq!(entity_count(&db, "c"), 1, "[rollback] c");
        assert_eq!(rel_count(&db, "base"), 1, "[rollback] base");
        db.close().expect("close");
        (x, c)
    };
    let db = open(&root);
    assert!(db.session().get_edge(x).is_some(), "[reopen] get_edge(x)");
    assert!(db.session().get_node(c).is_some(), "[reopen] get_node(c)");
    assert_eq!(rel_count(&db, "x"), 1, "[reopen] x");
    assert_eq!(rel_count(&db, "base"), 1, "[reopen] base");
    // The restored rows are still deletable.
    assert!(delete_edge(&db, x, true), "delete_edge(x) after rollback");
    assert_edge_gone(&db, x, "x", "delete after rollback");
}
