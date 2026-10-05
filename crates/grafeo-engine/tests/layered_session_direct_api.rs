//! Session direct APIs on a reopened generation root.
//!
//! A database opened with `GrafeoDB::open_generation_root` is *layered*: an
//! immutable, mmap-backed `CompactStore` **base** (the published generation)
//! plus a mutable `LpgStore` **overlay** that holds post-open writes. Cypher /
//! GQL queries read and write through a `LayeredStore` that merges the two.
//!
//! The session's **direct APIs** (`Session::get_node`, `get_edge`,
//! `get_neighbors_*`, `set_node_property`, `set_edge_property`,
//! `delete_node`, `delete_edge`) bypass query planning. These tests pin that
//! they see and mutate *base* elements exactly as Cypher does — before this
//! fix they went to the overlay only, so on a freshly reopened root base
//! elements were invisible and writes to them were silently dropped.
//!
//! The last test is a regression guard for a plain single-file database, whose
//! behaviour must be unchanged.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::path::Path;

use grafeo_common::types::{EdgeId, NodeId, Value};
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::tempdir;

/// Base ids resolved through Cypher on the reopened root.
struct BaseIds {
    ada: NodeId,
    grace: NodeId,
    linus: NodeId,
    /// `Ada -[:KNOWS {since: 2020}]-> Grace`
    knows: EdgeId,
    /// `Grace -[:MENTORS]-> Linus`
    mentors: EdgeId,
}

/// Publish a small base generation at `root` and return nothing; ids are
/// resolved after reopen so the test never depends on build-time numbering.
fn publish_base(root: &Path) {
    std::fs::create_dir_all(root).expect("create generation root");
    let source = GrafeoDB::new_in_memory();
    let ada = source
        .create_node_with_props(
            &["Person"],
            [
                ("name", Value::from("Ada")),
                ("source_hash", Value::from("h0")),
            ],
        )
        .expect("create Ada");
    let grace = source
        .create_node_with_props(&["Person"], [("name", Value::from("Grace"))])
        .expect("create Grace");
    let linus = source
        .create_node_with_props(&["Person"], [("name", Value::from("Linus"))])
        .expect("create Linus");
    source.create_edge_with_props(ada, grace, "KNOWS", [("since", Value::from(2020i64))]);
    source.create_edge(grace, linus, "MENTORS");
    source
        .build_and_publish_generation(generation_build_request(root, "direct-api-g1"))
        .expect("publish base generation");
    drop(source);
}

fn open_root(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, false).expect("open generation root writable")
}

fn as_u64(value: &Value) -> u64 {
    match value {
        Value::Int64(v) => u64::try_from(*v).expect("non-negative id"),
        other => panic!("expected integer id, got {other:?}"),
    }
}

fn base_ids(db: &GrafeoDB) -> BaseIds {
    let session = db.session();
    let row = |q: &str| {
        let r = session.execute_cypher(q).expect(q);
        assert_eq!(r.row_count(), 1, "exactly one row for {q}");
        r.rows()[0].clone()
    };
    let k = row(
        "MATCH (a:Person {name: 'Ada'})-[e:KNOWS]->(b:Person {name: 'Grace'}) \
         RETURN id(a), id(e), id(b)",
    );
    let m = row("MATCH (:Person {name: 'Grace'})-[e:MENTORS]->(c:Person) RETURN id(e), id(c)");
    BaseIds {
        ada: NodeId::new(as_u64(&k[0])),
        knows: EdgeId::new(as_u64(&k[1])),
        grace: NodeId::new(as_u64(&k[2])),
        mentors: EdgeId::new(as_u64(&m[0])),
        linus: NodeId::new(as_u64(&m[1])),
    }
}

/// Cypher view of a single string property on Ada (by name match).
fn cypher_ada_hash(db: &GrafeoDB) -> Option<Value> {
    let r = db
        .session()
        .execute_cypher("MATCH (n:Person {name: 'Ada'}) RETURN n.source_hash")
        .expect("query Ada hash");
    assert_eq!(r.row_count(), 1, "Ada must be visible to Cypher");
    match &r.rows()[0][0] {
        Value::Null => None,
        v => Some(v.clone()),
    }
}

fn cypher_count(db: &GrafeoDB, q: &str) -> i64 {
    let r = db.session().execute_cypher(q).expect(q);
    match &r.rows()[0][0] {
        Value::Int64(v) => *v,
        other => panic!("expected count, got {other:?}"),
    }
}

/// Fresh root, published base, reopened writable. Returns (tempdir guard, root path).
fn fresh_root() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("direct-api.grafeo.d");
    publish_base(&root);
    (dir, root)
}

// ── Reads ─────────────────────────────────────────────────────────────

#[test]
fn get_node_and_get_edge_see_base_elements() {
    let (_dir, root) = fresh_root();
    let db = open_root(&root);
    let ids = base_ids(&db);
    let session = db.session();

    let ada = session.get_node(ids.ada).expect("get_node(base Ada)");
    assert_eq!(ada.get_property("name"), Some(&Value::from("Ada")));
    assert!(session.node_exists(ids.grace), "node_exists(base Grace)");
    assert_eq!(
        session.get_node_property(ids.ada, "source_hash"),
        Some(Value::from("h0"))
    );

    let knows = session.get_edge(ids.knows).expect("get_edge(base KNOWS)");
    assert_eq!(knows.src, ids.ada);
    assert_eq!(knows.dst, ids.grace);
    assert_eq!(knows.edge_type.as_str(), "KNOWS");
    assert_eq!(knows.get_property("since"), Some(&Value::from(2020i64)));
    assert!(
        session.edge_exists(ids.mentors),
        "edge_exists(base MENTORS)"
    );
}

#[test]
fn get_neighbors_see_base_edges() {
    let (_dir, root) = fresh_root();
    let db = open_root(&root);
    let ids = base_ids(&db);
    let session = db.session();

    assert_eq!(
        session.get_neighbors_outgoing(ids.ada),
        vec![(ids.grace, ids.knows)]
    );
    assert_eq!(
        session.get_neighbors_incoming(ids.grace),
        vec![(ids.ada, ids.knows)]
    );
    assert_eq!(
        session.get_neighbors_outgoing_by_type(ids.grace, "MENTORS"),
        vec![(ids.linus, ids.mentors)]
    );
    assert_eq!(
        session.get_neighbors_outgoing_by_type(ids.grace, "KNOWS"),
        Vec::<(NodeId, EdgeId)>::new()
    );
    assert_eq!(session.get_degree(ids.grace), (1, 1));
}

// ── Property writes ───────────────────────────────────────────────────

#[test]
fn set_node_property_on_base_node_is_visible_and_durable() {
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        let session = db.session();
        session
            .set_node_property(ids.ada, "source_hash", Value::from("h1"))
            .expect("set_node_property");

        // Immediately visible through the direct API and through Cypher.
        assert_eq!(
            session.get_node_property(ids.ada, "source_hash"),
            Some(Value::from("h1")),
            "direct API must see its own write on a base node"
        );
        assert_eq!(
            cypher_ada_hash(&db),
            Some(Value::from("h1")),
            "Cypher must see the direct-API write on a base node"
        );
        db.close().expect("close");
    }
    // In-memory state before close and the WAL replayed on reopen agree.
    let db = open_root(&root);
    let ids = base_ids(&db);
    assert_eq!(
        db.session().get_node_property(ids.ada, "source_hash"),
        Some(Value::from("h1"))
    );
    assert_eq!(cypher_ada_hash(&db), Some(Value::from("h1")));
}

/// A direct auto-commit write must be committed on its own. Before the fix
/// it logged its data record with no `TransactionCommit`, so generation-root
/// replay held it as pending; an unrelated transaction that later rolled back
/// logged `TransactionAbort`, which discards everything pending, so the write
/// was lost on reopen (a clean `close()` otherwise papers over this by logging
/// a blanket commit).
#[test]
fn auto_commit_direct_write_survives_a_later_rolled_back_transaction() {
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        db.session()
            .set_node_property(ids.ada, "source_hash", Value::from("h3"))
            .expect("set_node_property");

        let mut other = db.session();
        other.begin_transaction().expect("begin");
        other
            .execute_cypher("CREATE (:Scratch {n: 1})")
            .expect("scratch write");
        other.rollback().expect("rollback");
        db.close().expect("close");
    }
    let db = open_root(&root);
    assert_eq!(
        cypher_ada_hash(&db),
        Some(Value::from("h3")),
        "auto-commit direct write must survive replay past a later abort"
    );
    assert_eq!(cypher_count(&db, "MATCH (n:Scratch) RETURN count(n)"), 0);
}

#[test]
fn set_node_property_on_base_node_in_committed_transaction() {
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        let mut session = db.session();
        session.begin_transaction().expect("begin");
        session
            .set_node_property(ids.ada, "source_hash", Value::from("h2"))
            .expect("set_node_property in tx");
        assert_eq!(
            session.get_node_property(ids.ada, "source_hash"),
            Some(Value::from("h2")),
            "own write visible inside the transaction"
        );
        session.commit().expect("commit");

        assert_eq!(
            db.session().get_node_property(ids.ada, "source_hash"),
            Some(Value::from("h2"))
        );
        assert_eq!(cypher_ada_hash(&db), Some(Value::from("h2")));
        db.close().expect("close");
    }
    let db = open_root(&root);
    let ids = base_ids(&db);
    assert_eq!(
        db.session().get_node_property(ids.ada, "source_hash"),
        Some(Value::from("h2"))
    );
    assert_eq!(cypher_ada_hash(&db), Some(Value::from("h2")));
}

#[test]
fn set_node_property_on_base_node_in_rolled_back_transaction() {
    // Light guard only: a rolled-back write must not become visible. Deeper
    // rollback semantics for base mutations are tracked separately.
    let (_dir, root) = fresh_root();
    let db = open_root(&root);
    let ids = base_ids(&db);
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .set_node_property(ids.ada, "source_hash", Value::from("rolled-back"))
        .expect("set_node_property in tx");
    session.rollback().expect("rollback");

    assert_ne!(
        db.session().get_node_property(ids.ada, "source_hash"),
        Some(Value::from("rolled-back"))
    );
    assert_ne!(cypher_ada_hash(&db), Some(Value::from("rolled-back")));
}

#[test]
fn set_edge_property_on_base_edge_is_visible_and_durable() {
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        let session = db.session();
        session
            .set_edge_property(ids.knows, "since", Value::from(1999i64))
            .expect("set_edge_property");
        assert_eq!(
            session
                .get_edge(ids.knows)
                .and_then(|e| e.get_property("since").cloned()),
            Some(Value::from(1999i64))
        );
        assert_eq!(
            cypher_count(
                &db,
                "MATCH (:Person)-[e:KNOWS]->(:Person) WHERE e.since = 1999 RETURN count(e)"
            ),
            1
        );
        db.close().expect("close");
    }
    let db = open_root(&root);
    assert_eq!(
        cypher_count(
            &db,
            "MATCH (:Person)-[e:KNOWS]->(:Person) WHERE e.since = 1999 RETURN count(e)"
        ),
        1
    );
}

// ── Deletes ───────────────────────────────────────────────────────────

#[test]
fn delete_edge_on_base_edge_takes_effect() {
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        let session = db.session();
        assert!(session.delete_edge(ids.knows), "delete_edge(base KNOWS)");
        assert!(session.get_edge(ids.knows).is_none());
        assert_eq!(
            session.get_neighbors_outgoing(ids.ada),
            Vec::<(NodeId, EdgeId)>::new()
        );
        assert_eq!(
            cypher_count(&db, "MATCH ()-[e:KNOWS]->() RETURN count(e)"),
            0
        );
        assert_eq!(
            cypher_count(&db, "MATCH ()-[e:MENTORS]->() RETURN count(e)"),
            1
        );
        db.close().expect("close");
    }
    let db = open_root(&root);
    assert_eq!(
        cypher_count(&db, "MATCH ()-[e:KNOWS]->() RETURN count(e)"),
        0
    );
    assert_eq!(
        cypher_count(&db, "MATCH ()-[e:MENTORS]->() RETURN count(e)"),
        1
    );
}

#[test]
fn delete_node_on_base_node_takes_effect() {
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        let session = db.session();
        // Detach first so the delete is well-defined regardless of DETACH
        // semantics of the direct API.
        assert!(session.delete_edge(ids.mentors));
        assert!(session.delete_node(ids.linus), "delete_node(base Linus)");
        assert!(session.get_node(ids.linus).is_none());
        assert_eq!(
            cypher_count(&db, "MATCH (n:Person {name: 'Linus'}) RETURN count(n)"),
            0
        );
        assert_eq!(cypher_count(&db, "MATCH (n:Person) RETURN count(n)"), 2);
        db.close().expect("close");
    }
    let db = open_root(&root);
    assert_eq!(
        cypher_count(&db, "MATCH (n:Person {name: 'Linus'}) RETURN count(n)"),
        0
    );
    assert_eq!(cypher_count(&db, "MATCH (n:Person) RETURN count(n)"), 2);
}

#[test]
fn delete_on_base_elements_in_committed_transaction() {
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        let mut session = db.session();
        session.begin_transaction().expect("begin");
        assert!(session.delete_edge(ids.mentors));
        assert!(session.delete_node(ids.linus));
        assert!(
            session.get_node(ids.linus).is_none(),
            "own delete visible in tx"
        );
        session.commit().expect("commit");

        assert!(db.session().get_node(ids.linus).is_none());
        assert!(db.session().get_edge(ids.mentors).is_none());
        assert_eq!(cypher_count(&db, "MATCH (n:Person) RETURN count(n)"), 2);
        assert_eq!(
            cypher_count(&db, "MATCH ()-[e:MENTORS]->() RETURN count(e)"),
            0
        );
        db.close().expect("close");
    }
    let db = open_root(&root);
    assert_eq!(cypher_count(&db, "MATCH (n:Person) RETURN count(n)"), 2);
    assert_eq!(
        cypher_count(&db, "MATCH ()-[e:MENTORS]->() RETURN count(e)"),
        0
    );
}

#[test]
fn delete_on_base_elements_in_rolled_back_transaction_does_not_panic() {
    // Light guard only: rollback must return and the session/database must
    // stay usable. Whether base deletes are undone on rollback is owned by the
    // separate rollback-of-base-mutations work and is not asserted here.
    let (_dir, root) = fresh_root();
    let db = open_root(&root);
    let ids = base_ids(&db);
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    assert!(session.delete_edge(ids.knows));
    session.rollback().expect("rollback");
    assert!(db.session().get_node(ids.ada).is_some());
    assert_eq!(cypher_count(&db, "MATCH (n:Person) RETURN count(n)"), 3);
}

// ── Overlay elements still behave ─────────────────────────────────────

#[test]
fn overlay_nodes_created_after_reopen_still_work() {
    let (_dir, root) = fresh_root();
    let db = open_root(&root);
    let ids = base_ids(&db);
    let session = db.session();
    let new = session
        .create_node_with_props(&["Person"], [("name", Value::from("Margaret"))])
        .expect("create overlay node");
    let e = session.create_edge(ids.ada, new, "KNOWS");
    assert!(session.get_node(new).is_some());
    assert!(session.get_edge(e).is_some());
    let mut out = session.get_neighbors_outgoing(ids.ada);
    out.sort_unstable_by_key(|(_, eid)| *eid);
    assert_eq!(out, vec![(ids.grace, ids.knows), (new, e)]);
    session
        .set_node_property(new, "source_hash", Value::from("m1"))
        .expect("set on overlay node");
    assert_eq!(
        cypher_count(
            &db,
            "MATCH (n:Person {name: 'Margaret'}) WHERE n.source_hash = 'm1' RETURN count(n)"
        ),
        1
    );
}

// ── Single-file regression guard ──────────────────────────────────────

#[test]
fn single_file_database_direct_api_unchanged() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("single.grafeo");
    let (a, b, e) = {
        let db = GrafeoDB::open(&path).expect("open single-file db");
        let session = db.session();
        let a = session
            .create_node_with_props(&["Person"], [("name", Value::from("Ada"))])
            .expect("create a");
        let b = session.create_node(&["Person"]);
        let e = session.create_edge(a, b, "KNOWS");
        assert_eq!(session.get_neighbors_outgoing(a), vec![(b, e)]);
        assert_eq!(session.get_neighbors_incoming(b), vec![(a, e)]);
        session
            .set_node_property(a, "source_hash", Value::from("s1"))
            .expect("set");
        session
            .set_edge_property(e, "since", Value::from(1i64))
            .expect("set edge");
        assert_eq!(
            session.get_node_property(a, "source_hash"),
            Some(Value::from("s1"))
        );
        db.close().expect("close");
        (a, b, e)
    };
    let db = GrafeoDB::open(&path).expect("reopen single-file db");
    let session = db.session();
    assert_eq!(
        session.get_node_property(a, "source_hash"),
        Some(Value::from("s1"))
    );
    assert_eq!(
        session
            .get_edge(e)
            .and_then(|x| x.get_property("since").cloned()),
        Some(Value::from(1i64))
    );
    assert!(session.delete_edge(e));
    assert!(session.delete_node(b));
    assert!(session.get_node(b).is_none());
    assert_eq!(
        session.get_neighbors_outgoing(a),
        Vec::<(NodeId, EdgeId)>::new()
    );
}

// ── Promotion keeps base edges (Cypher-only, no direct API) ───────────

/// Writing a property on a base node copies ("promotes") the node into the
/// overlay. Its base edges must stay visible: `LayeredStore::edges_from` used
/// to skip base adjacency for promoted nodes, but promotion copies no edges,
/// so a plain Cypher `SET` made the node's edges disappear.
#[test]
fn cypher_set_on_base_node_keeps_its_base_edges() {
    let (_dir, root) = fresh_root();
    let knows_from_ada =
        "MATCH (:Person {name: 'Ada'})-[e:KNOWS]->(:Person {name: 'Grace'}) RETURN count(e)";
    {
        let db = open_root(&root);
        db.session()
            .execute_cypher("MATCH (n:Person {name: 'Ada'}) SET n.source_hash = 'c1'")
            .expect("cypher SET");
        assert_eq!(cypher_count(&db, knows_from_ada), 1, "live");
        assert_eq!(cypher_count(&db, "MATCH ()-[e]->() RETURN count(e)"), 2);
        db.close().expect("close");
    }
    let db = open_root(&root);
    assert_eq!(cypher_count(&db, knows_from_ada), 1, "after reopen");
    assert_eq!(cypher_count(&db, "MATCH ()-[e]->() RETURN count(e)"), 2);
}

// ── Review round 1 (PR #13) ───────────────────────────────────────────

/// Item 1: a copy-up must be visible to snapshots older than the copy.
/// Transaction A starts, another session commits (the epoch advances), then A
/// writes a base node. The copy-up used to be created at the current epoch,
/// so A's own snapshot no longer saw the node at all.
#[test]
fn copy_up_stays_visible_to_an_older_snapshot() {
    let (_dir, root) = fresh_root();
    let db = open_root(&root);
    let ids = base_ids(&db);

    let mut a = db.session();
    a.begin_transaction().expect("begin A");
    db.session()
        .execute_cypher("CREATE (:Other {n: 1})")
        .expect("B commits");

    a.set_node_property(ids.ada, "source_hash", Value::from("s1"))
        .expect("A writes base node");
    let ada = a
        .get_node(ids.ada)
        .expect("A still sees Ada after its copy-up");
    assert_eq!(ada.get_property("source_hash"), Some(&Value::from("s1")));
    let r = a
        .execute_cypher("MATCH (n:Person {name: 'Ada'}) RETURN n.source_hash")
        .expect("A's Cypher");
    assert_eq!(r.row_count(), 1, "A's Cypher still sees Ada");

    a.set_edge_property(ids.knows, "since", Value::from(1990i64))
        .expect("A writes base edge");
    assert_eq!(
        a.get_edge(ids.knows)
            .and_then(|e| e.get_property("since").cloned()),
        Some(Value::from(1990i64)),
        "A still sees KNOWS after its copy-up"
    );
    a.commit().expect("commit A");
    assert_eq!(cypher_ada_hash(&db), Some(Value::from("s1")));
}

/// Item 2: the `GrafeoDB` handle's own direct CRUD methods reach base
/// elements and survive replay past a later abort.
#[test]
fn database_handle_direct_crud_on_base_elements() {
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);

        db.set_node_property(ids.ada, "source_hash", Value::from("d1"))
            .expect("db.set_node_property");
        assert_eq!(
            db.get_node(ids.ada)
                .and_then(|n| n.get_property("source_hash").cloned()),
            Some(Value::from("d1"))
        );
        assert_eq!(cypher_ada_hash(&db), Some(Value::from("d1")));

        assert!(db.get_edge(ids.knows).is_some(), "db.get_edge(base)");
        db.set_edge_property(ids.knows, "since", Value::from(1999i64));
        assert_eq!(
            db.get_edge(ids.knows)
                .and_then(|e| e.get_property("since").cloned()),
            Some(Value::from(1999i64))
        );

        assert!(db.delete_edge(ids.mentors), "db.delete_edge(base)");
        assert!(
            db.delete_node(ids.linus).expect("db.delete_node"),
            "db.delete_node(base)"
        );
        assert_eq!(cypher_count(&db, "MATCH (n:Person) RETURN count(n)"), 2);

        // A later unrelated rollback must not discard the writes above.
        let mut other = db.session();
        other.begin_transaction().expect("begin");
        other
            .execute_cypher("CREATE (:Scratch {n: 1})")
            .expect("scratch");
        other.rollback().expect("rollback");
        db.close().expect("close");
    }
    let db = open_root(&root);
    assert_eq!(cypher_ada_hash(&db), Some(Value::from("d1")));
    assert_eq!(
        cypher_count(
            &db,
            "MATCH ()-[e:KNOWS]->() WHERE e.since = 1999 RETURN count(e)"
        ),
        1
    );
    assert_eq!(
        cypher_count(&db, "MATCH ()-[e:MENTORS]->() RETURN count(e)"),
        0
    );
    assert_eq!(cypher_count(&db, "MATCH (n:Person) RETURN count(n)"), 2);
}

/// Item 3a: writing a property on an id that does not exist is an error.
#[test]
fn set_property_on_missing_id_is_an_error() {
    let (_dir, root) = fresh_root();
    let db = open_root(&root);
    let session = db.session();
    assert!(
        session
            .set_node_property(NodeId::new(9_999), "k", Value::from(1i64))
            .is_err()
    );
    assert!(
        session
            .set_edge_property(EdgeId::new(9_999), "k", Value::from(1i64))
            .is_err()
    );
}

/// Item 3b: a write to a deleted base node must not bring it back.
#[test]
fn write_to_deleted_base_node_is_an_error_and_does_not_resurrect() {
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        let session = db.session();
        assert!(session.delete_edge(ids.mentors));
        assert!(session.delete_node(ids.linus));
        assert!(
            session
                .set_node_property(ids.linus, "name", Value::from("Zombie"))
                .is_err(),
            "write to a deleted base node must fail"
        );
        assert!(session.get_node(ids.linus).is_none());
        assert_eq!(cypher_count(&db, "MATCH (n:Person) RETURN count(n)"), 2);
        assert_eq!(cypher_count(&db, "MATCH (n) RETURN count(n)"), 2);
        db.close().expect("close");
    }
    let db = open_root(&root);
    assert_eq!(cypher_count(&db, "MATCH (n) RETURN count(n)"), 2);
}

/// Item 3c: direct writes on a read-only root fail and change nothing.
#[test]
fn direct_writes_on_read_only_root_fail() {
    let (_dir, root) = fresh_root();
    let db = GrafeoDB::open_generation_root(&root, true).expect("open read-only");
    let ids = base_ids(&db);
    let session = db.session();
    assert!(
        session
            .set_node_property(ids.ada, "source_hash", Value::from("ro"))
            .is_err()
    );
    assert!(
        session
            .set_edge_property(ids.knows, "since", Value::from(1i64))
            .is_err()
    );
    assert!(!session.delete_edge(ids.mentors));
    assert!(!session.delete_node(ids.linus));
    assert_eq!(cypher_ada_hash(&db), Some(Value::from("h0")));
    assert_eq!(cypher_count(&db, "MATCH ()-[e]->() RETURN count(e)"), 2);
    assert_eq!(cypher_count(&db, "MATCH (n) RETURN count(n)"), 3);
}

/// Item 4a: concurrent auto-commit direct writes must leave a root that
/// reopens. A commit marker and its epoch advance used to be two separate WAL
/// appends, so another session's record could land between them, which replay
/// rejects as unrecoverable.
#[test]
fn concurrent_direct_writes_leave_a_root_that_reopens() {
    const THREADS: usize = 8;
    const WRITES: usize = 150;
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        let targets = [ids.ada, ids.grace, ids.linus];
        let ok = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for t in 0..THREADS {
                let (db, ok) = (&db, &ok);
                scope.spawn(move || {
                    let session = db.session();
                    for i in 0..WRITES {
                        let id = targets[(t + i) % targets.len()];
                        // Two implicit transactions writing the same node can
                        // conflict; that is ordinary MVCC and not under test.
                        if session
                            .set_node_property(
                                id,
                                "w",
                                Value::from(i64::try_from(t * WRITES + i).unwrap()),
                            )
                            .is_ok()
                        {
                            ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert!(ok.into_inner() > 0, "some concurrent writes succeed");
        db.close().expect("close");
    }
    let db = GrafeoDB::open_generation_root(&root, false)
        .expect("root written by concurrent direct writes must reopen");
    assert_eq!(cypher_count(&db, "MATCH (n:Person) RETURN count(n)"), 3);
}

/// Item 4b: copy-ups and concurrent node creates must not hand out the same
/// id. Copy-ups used to lower the overlay's id counter, create, and restore
/// it, so a create racing with them could take a base node's id.
#[test]
fn copy_ups_and_concurrent_creates_get_distinct_ids() {
    const BASE: usize = 400;
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("race.grafeo.d");
    std::fs::create_dir_all(&root).expect("create root");
    {
        let source = GrafeoDB::new_in_memory();
        for i in 0..BASE {
            source
                .create_node_with_props(&["B"], [("i", Value::from(i64::try_from(i).unwrap()))])
                .expect("base node");
        }
        source
            .build_and_publish_generation(generation_build_request(&root, "race-g1"))
            .expect("publish");
    }
    let db = open_root(&root);
    let r = db
        .session()
        .execute_cypher("MATCH (n:B) RETURN id(n), n.i")
        .expect("base ids");
    let base: Vec<(NodeId, i64)> = r
        .rows()
        .iter()
        .map(|row| {
            let Value::Int64(i) = row[1] else {
                panic!("int")
            };
            (NodeId::new(as_u64(&row[0])), i)
        })
        .collect();
    assert_eq!(base.len(), BASE);

    let created = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for t in 0..4 {
            let (db, base) = (&db, &base);
            scope.spawn(move || {
                let session = db.session();
                for (k, (id, _)) in base.iter().enumerate() {
                    if k % 4 == t {
                        session
                            .set_node_property(*id, "touched", Value::from(true))
                            .expect("copy-up write");
                    }
                }
            });
        }
        for _ in 0..4 {
            let (db, created) = (&db, &created);
            scope.spawn(move || {
                let session = db.session();
                let mut mine = Vec::new();
                for _ in 0..BASE {
                    mine.push(session.create_node(&["New"]));
                }
                created.lock().unwrap().extend(mine);
            });
        }
    });
    let created = created.into_inner().unwrap();
    let mut all: Vec<u64> = created.iter().map(|id| id.as_u64()).collect();
    all.extend(base.iter().map(|(id, _)| id.as_u64()));
    let n = all.len();
    all.sort_unstable();
    all.dedup();
    assert_eq!(all.len(), n, "a created node reused an existing id");
    let session = db.session();
    for (id, i) in &base {
        let node = session.get_node(*id).expect("base node still there");
        assert_eq!(node.get_property("i"), Some(&Value::from(*i)));
    }
}

/// Item 5: a direct write that cannot commit must not leave an implicit
/// transaction open behind it.
#[test]
fn failed_implicit_commit_leaves_no_transaction_open() {
    let (_dir, root) = fresh_root();
    let db = open_root(&root);
    let ids = base_ids(&db);
    let session = db.session();
    {
        let _stream = session
            .execute_streaming("MATCH (n:Person) RETURN n.name")
            .expect("open a stream");
        assert!(
            session
                .set_node_property(ids.ada, "source_hash", Value::from("x"))
                .is_err(),
            "auto-commit write while a stream is open must fail"
        );
        assert!(
            !session.in_transaction(),
            "no implicit transaction left open"
        );
    }
    session
        .set_node_property(ids.ada, "source_hash", Value::from("y"))
        .expect("write after the stream is dropped");
    assert!(!session.in_transaction());
    assert_eq!(cypher_ada_hash(&db), Some(Value::from("y")));
}

/// Item 6: a transaction on a generation root sees the typed edges it created
/// itself before committing (upstream 2bc6da09).
#[test]
fn transaction_sees_its_own_new_typed_edge() {
    let (_dir, root) = fresh_root();
    let db = open_root(&root);
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher(
            "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Linus'}) CREATE (a)-[:ADVISES]->(b)",
        )
        .expect("create edge in tx");
    let r = session
        .execute_cypher("MATCH (:Person {name: 'Ada'})-[:ADVISES]->(b) RETURN b.name")
        .expect("typed expand in tx");
    assert_eq!(r.row_count(), 1, "typed expansion sees the tx's own edge");
    let r = session
        .execute_cypher(
            "MATCH (:Person {name: 'Ada'})-[r]->(:Person {name: 'Linus'}) RETURN type(r)",
        )
        .expect("type(r) in tx");
    assert_eq!(r.rows()[0][0], Value::from("ADVISES"));
    session.commit().expect("commit");
    assert_eq!(
        cypher_count(&db, "MATCH ()-[e:ADVISES]->() RETURN count(e)"),
        1
    );
}

// ── Review round 2 (PR #13) ───────────────────────────────────────────

/// Item 1: on a layered database the handle's write methods must report a
/// WAL failure instead of applying the write and returning success.
#[cfg(feature = "testing-crash-injection")]
#[test]
fn handle_writes_report_wal_failures() {
    use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
    let (_dir, root) = fresh_root();
    let db = open_root(&root);
    let ids = base_ids(&db);

    // The data record's append fails: an error, and nothing applied.
    enable_io_failure_at(1);
    let r = db.set_node_property(ids.ada, "source_hash", Value::from("e1"));
    disable_io_failure();
    assert!(r.is_err(), "failed data-record append must be reported");
    assert_eq!(cypher_ada_hash(&db), Some(Value::from("h0")));

    // The commit marker's append fails: still an error.
    enable_io_failure_at(2);
    let r = db.set_node_property(ids.ada, "source_hash", Value::from("e2"));
    disable_io_failure();
    assert!(r.is_err(), "failed commit-marker append must be reported");

    // delete_node keeps its error channel.
    enable_io_failure_at(1);
    let r = db.delete_node(ids.linus);
    disable_io_failure();
    assert!(r.is_err(), "failed delete append must be reported");
    assert_eq!(
        cypher_count(&db, "MATCH (n:Person {name: 'Linus'}) RETURN count(n)"),
        1,
        "and the delete must not be applied"
    );
}

/// The `wal_<seq>.log` files of a root and every complete frame in them.
fn wal_frames(root: &Path) -> Vec<grafeo_storage::generation::wal_cursor::ReplayFrame> {
    use grafeo_storage::generation::wal_cursor::{WalReplayCursor, replay_stream_from};
    let dir = root.join("wal");
    let first = std::fs::read_dir(&dir)
        .expect("wal dir")
        .filter_map(Result::ok)
        .filter_map(|e| {
            e.file_name()
                .to_str()?
                .strip_prefix("wal_")?
                .strip_suffix(".log")?
                .parse::<u64>()
                .ok()
        })
        .min()
        .expect("a wal file");
    let cursor = WalReplayCursor {
        log_sequence: first,
        byte_offset: 0,
        epoch: 0,
        transaction_id: 0,
    };
    replay_stream_from(&dir, &cursor)
        .expect("stream")
        .map(|f| f.expect("frame"))
        .collect()
}

fn truncate_wal(root: &Path, seq: u64, len: u64) {
    let path = root.join("wal").join(format!("wal_{seq:08}.log"));
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open wal file");
    file.set_len(len).expect("truncate");
}

/// Item 2: a crash between a commit frame and its epoch frame. The commit is
/// incomplete, so replay must not apply it, and the root must keep opening
/// after later writes and clean closes.
fn commit_without_epoch_recovers(cut_inside_epoch: bool) {
    use grafeo_storage::wal::WalRecord;
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        db.session()
            .set_node_property(ids.ada, "source_hash", Value::from("c1"))
            .expect("write");
        db.close().expect("close");
    }
    let frames = wal_frames(&root);
    let data = frames
        .iter()
        .position(|f| {
            matches!(&f.record, WalRecord::SetNodeProperty { value, .. }
                if *value == Value::from("c1"))
        })
        .expect("data record");
    let commit = (data..frames.len())
        .find(|&k| matches!(frames[k].record, WalRecord::TransactionCommit { .. }))
        .expect("its commit");
    let epoch = &frames[commit + 1];
    assert!(matches!(epoch.record, WalRecord::EpochAdvance { .. }));
    assert_eq!(epoch.log_sequence, frames[commit].log_sequence);
    let cut = if cut_inside_epoch {
        epoch.byte_offset + 3
    } else {
        epoch.byte_offset
    };
    truncate_wal(&root, epoch.log_sequence, cut);

    {
        let db = GrafeoDB::open_generation_root(&root, false)
            .expect("reopen after a crash between commit and epoch frames");
        assert_eq!(
            cypher_ada_hash(&db),
            Some(Value::from("h0")),
            "a commit without its epoch frame is not applied"
        );
        let ids = base_ids(&db);
        db.session()
            .set_node_property(ids.ada, "source_hash", Value::from("c2"))
            .expect("write after recovery");
        db.close().expect("close");
    }
    let db = GrafeoDB::open_generation_root(&root, false)
        .expect("root reopens after recovery, a write and a clean close");
    assert_eq!(cypher_ada_hash(&db), Some(Value::from("c2")));
}

#[test]
fn crash_right_after_commit_frame_recovers() {
    commit_without_epoch_recovers(false);
}

#[test]
fn crash_inside_epoch_frame_recovers() {
    commit_without_epoch_recovers(true);
}

/// Item 2 (same mechanism): uncommitted records left by a crash must stay
/// discarded once a later transaction commits.
#[test]
fn uncommitted_tail_stays_discarded_after_a_later_commit() {
    use grafeo_storage::wal::WalRecord;
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        let mut session = db.session();
        session.begin_transaction().expect("begin");
        session
            .set_node_property(ids.ada, "source_hash", Value::from("u1"))
            .expect("write in tx");
        session.rollback().expect("rollback");
        db.close().expect("close");
    }
    // Simulate a crash right after the uncommitted data record.
    let frames = wal_frames(&root);
    let data = frames
        .iter()
        .position(|f| {
            matches!(&f.record, WalRecord::SetNodeProperty { value, .. }
                if *value == Value::from("u1"))
        })
        .expect("data record");
    let next = &frames[data + 1];
    truncate_wal(&root, next.log_sequence, next.byte_offset);

    {
        let db = open_root(&root);
        assert_eq!(cypher_ada_hash(&db), Some(Value::from("h0")));
        let ids = base_ids(&db);
        db.session()
            .set_node_property(ids.grace, "source_hash", Value::from("g1"))
            .expect("later committed write");
        db.close().expect("close");
    }
    let db = open_root(&root);
    assert_eq!(
        cypher_ada_hash(&db),
        Some(Value::from("h0")),
        "the crashed transaction's record must not ride a later commit"
    );
}

/// Item 5: a direct write whose vector-index update is invalid must fail
/// before it commits, not after.
#[cfg(feature = "vector-index")]
#[test]
fn invalid_vector_write_fails_before_commit() {
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        db.create_vector_index("Person", "emb", Some(3), None, None, None, None)
            .expect("vector index");
        let session = db.session();
        let r = session.set_node_property(ids.ada, "emb", Value::Vector(vec![1.0_f32, 2.0].into()));
        assert!(r.is_err(), "dimension mismatch must fail the write");
        assert!(!session.in_transaction());
        assert_eq!(
            session
                .get_node(ids.ada)
                .and_then(|n| n.get_property("emb").cloned()),
            None,
            "a failed write is not committed"
        );
        db.close().expect("close");
    }
    let db = open_root(&root);
    let ids = base_ids(&db);
    assert_eq!(
        db.session()
            .get_node(ids.ada)
            .and_then(|n| n.get_property("emb").cloned()),
        None,
        "nor made durable"
    );
}

// ── Review round 3 (PR #13) ───────────────────────────────────────────

/// Item B: a commit marker that fails once is retried, so the write is
/// durable and reported as done; a later unrelated rollback cannot drop it.
#[cfg(feature = "testing-crash-injection")]
#[test]
fn failed_commit_marker_is_retried() {
    use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        // Append 1 is the data record, append 2 the commit pair.
        enable_io_failure_at(2);
        let r = db.set_node_property(ids.ada, "source_hash", Value::from("r1"));
        disable_io_failure();
        r.expect("a commit marker that fails once is retried");

        let mut other = db.session();
        other.begin_transaction().expect("begin");
        other
            .execute_cypher("CREATE (:Scratch {n: 1})")
            .expect("scratch");
        other.rollback().expect("rollback");
        db.close().expect("close");
    }
    let db = open_root(&root);
    assert_eq!(cypher_ada_hash(&db), Some(Value::from("r1")));
}

/// Items A and B: when the commit marker cannot be written even on retry,
/// the error says durability is unconfirmed (it does not claim the write is
/// lost), the WAL refuses every later write until the database is reopened,
/// and the root reopens showing only what reached the disk.
#[cfg(feature = "testing-crash-injection")]
#[test]
fn unrecoverable_commit_marker_failure_refuses_further_writes() {
    use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_from};
    let (_dir, root) = fresh_root();
    {
        let db = open_root(&root);
        let ids = base_ids(&db);
        enable_io_failure_from(2);
        let r = db.set_node_property(ids.ada, "source_hash", Value::from("x1"));
        disable_io_failure();
        let message = r.expect_err("commit marker failed twice").to_string();
        assert!(
            message.contains("durability unconfirmed"),
            "error must not claim more than is known: {message}"
        );

        assert!(
            db.set_node_property(ids.grace, "source_hash", Value::from("g1"))
                .is_err(),
            "further writes are refused until reopen"
        );
        drop(db);
    }
    let db = GrafeoDB::open_generation_root(&root, false)
        .expect("root reopens after an unrecoverable commit-marker failure");
    let ids = base_ids(&db);
    assert_eq!(
        cypher_ada_hash(&db),
        Some(Value::from("h0")),
        "the commit pair never reached the disk, so the write is not there"
    );
    assert_eq!(
        db.session().get_node_property(ids.grace, "source_hash"),
        None,
        "the refused write is not there either"
    );
}
