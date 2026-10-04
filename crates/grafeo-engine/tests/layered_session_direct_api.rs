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
