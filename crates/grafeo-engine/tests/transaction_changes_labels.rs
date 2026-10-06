//! Undoing part of a transaction restores the labels of nodes the same
//! transaction created (review of fork PR #26).
//!
//! Since #410 a transaction can delete a node it created itself. Rolling
//! that delete back (to a savepoint, or a nested transaction's rollback)
//! must give the node its labels back, although the node is still PENDING.
//! The same holds for a label removed or added after the savepoint.
//!
//! ```bash
//! cargo test -p grafeo-engine --test transaction_changes_labels \
//!   --features generation,generation-streaming,compact-store,lpg,mmap,wal,cypher
//! ```

use std::sync::Arc;

use grafeo_common::types::NodeId;
use grafeo_core::graph::lpg::LpgStore;
use grafeo_engine::GrafeoDB;
use grafeo_engine::session::Session;

/// The databases to run on: a plain in-memory store, and (with the
/// features) a generation root whose overlay holds the new nodes.
fn databases(dir: &std::path::Path) -> Vec<(&'static str, GrafeoDB, Arc<LpgStore>)> {
    let plain = GrafeoDB::new_in_memory();
    let plain_store = Arc::clone(plain.store());
    #[allow(unused_mut)]
    let mut dbs = vec![("plain", plain, plain_store)];
    #[cfg(all(
        feature = "generation",
        feature = "compact-store",
        feature = "mmap",
        feature = "wal"
    ))]
    {
        use grafeo_engine::generation_build_request;
        let root = dir.join("root");
        std::fs::create_dir_all(&root).unwrap();
        let source = GrafeoDB::new_in_memory();
        source.create_node(&["Base"]);
        source
            .build_and_publish_generation(generation_build_request(&root, "base"))
            .unwrap();
        let db = GrafeoDB::open_generation_root(&root, false).unwrap();
        let overlay = db.layered_store().unwrap().overlay_store();
        dbs.push(("generation root", db, overlay));
    }
    #[cfg(not(all(
        feature = "generation",
        feature = "compact-store",
        feature = "mmap",
        feature = "wal"
    )))]
    let _ = dir;
    dbs
}

/// Opens an inner scope: a savepoint, or a nested transaction.
fn open_inner(session: &mut Session, nested: bool) {
    if nested {
        session.begin_transaction().unwrap();
    } else {
        session.savepoint("inner").unwrap();
    }
}

/// Undoes the inner scope.
fn undo_inner(session: &mut Session, nested: bool) {
    if nested {
        session.rollback().unwrap();
    } else {
        session.rollback_to_savepoint("inner").unwrap();
    }
}

fn labels(session: &Session, id: NodeId) -> Vec<String> {
    let mut labels: Vec<String> = session
        .get_node(id)
        .unwrap_or_else(|| panic!("node {id:?} is gone"))
        .labels
        .iter()
        .map(ToString::to_string)
        .collect();
    labels.sort();
    labels
}

fn new_node(session: &mut Session, store: &LpgStore, name: &str) -> NodeId {
    session
        .execute(&format!("INSERT (:Person:Extra {{name: '{name}'}})"))
        .unwrap();
    // The label index lists the new node while it is still PENDING; ids
    // only grow, so it is the largest `Person`.
    *store.nodes_by_label("Person").iter().max().unwrap()
}

/// Committed view: labels by name, and the label counts the planner reads.
fn committed(db: &GrafeoDB, store: &LpgStore, name: &str) -> (Vec<String>, u64, u64) {
    let rows = db
        .execute(&format!(
            "MATCH (n:Person {{name: '{name}'}}) RETURN labels(n)"
        ))
        .unwrap();
    let mut labels: Vec<String> = match &rows.rows()[0][0] {
        grafeo_common::types::Value::List(items) => items
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect(),
        other => panic!("labels(n) returned {other:?}"),
    };
    labels.sort();
    store.ensure_statistics_fresh();
    let stats = store.statistics();
    let count = |l: &str| stats.get_label(l).map_or(0, |s| s.node_count);
    (labels, count("Person"), count("Extra"))
}

#[test]
fn undone_delete_of_a_node_created_in_the_transaction_keeps_its_labels() {
    let dir = tempfile::tempdir().unwrap();
    for (kind, db, store) in databases(dir.path()) {
        for nested in [false, true] {
            let name = format!("r26-delete-{nested}");
            let mut session = db.session();
            session.begin_transaction().unwrap();
            let id = new_node(&mut session, &store, &name);
            open_inner(&mut session, nested);
            session
                .execute(&format!("MATCH (n:Person {{name: '{name}'}}) DELETE n"))
                .unwrap();
            assert!(
                session.get_node(id).is_none(),
                "{kind}, nested={nested}: the delete took effect"
            );
            undo_inner(&mut session, nested);
            assert_eq!(
                labels(&session, id),
                ["Extra", "Person"],
                "{kind}, nested={nested}: inside the transaction"
            );
            session.commit().unwrap();
            let (labels, ..) = committed(&db, &store, &name);
            assert_eq!(labels, ["Extra", "Person"], "{kind}, nested={nested}");
            assert!(
                store.nodes_by_label("Person").contains(&id),
                "{kind}, nested={nested}: label index"
            );
        }
        let (_, people, extras) = committed(&db, &store, "r26-delete-false");
        assert_eq!((people, extras), (2, 2), "{kind}: planner label counts");
    }
}

#[test]
fn undone_label_changes_on_a_node_created_in_the_transaction() {
    let dir = tempfile::tempdir().unwrap();
    for (kind, db, store) in databases(dir.path()) {
        for nested in [false, true] {
            let name = format!("r26-labels-{nested}");
            let mut session = db.session();
            session.begin_transaction().unwrap();
            let id = new_node(&mut session, &store, &name);
            open_inner(&mut session, nested);
            session
                .execute(&format!(
                    "MATCH (n:Person {{name: '{name}'}}) REMOVE n:Extra"
                ))
                .unwrap();
            session
                .execute(&format!("MATCH (n:Person {{name: '{name}'}}) SET n:Added"))
                .unwrap();
            assert_eq!(
                labels(&session, id),
                ["Added", "Person"],
                "{kind}, nested={nested}: after the changes"
            );
            undo_inner(&mut session, nested);
            assert_eq!(
                labels(&session, id),
                ["Extra", "Person"],
                "{kind}, nested={nested}: inside the transaction"
            );
            session.commit().unwrap();
            let (labels, ..) = committed(&db, &store, &name);
            assert_eq!(labels, ["Extra", "Person"], "{kind}, nested={nested}");
            assert!(
                !store.nodes_by_label("Added").contains(&id),
                "{kind}, nested={nested}: label index"
            );
        }
    }
}
