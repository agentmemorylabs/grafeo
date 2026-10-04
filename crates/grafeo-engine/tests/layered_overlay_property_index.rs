//! D5: property-index lookups on a REOPENED generation root must see overlay
//! writes made after the reopen.
//!
//! A generation root (`*.grafeo.d`) opens as a layered store: a read-only
//! CompactStore *base* (the published generation) plus a writable LpgStore
//! *overlay* that holds every write since the publish. The base generation
//! carries a PropertyIndex section; on reopen its postings are installed on
//! the overlay as a file-backed mapped index.
//!
//! Found downstream (agent-memory-hosted PR #89, probe
//! `rebuilt_index_after_reopen_finds_new_overlay_nodes`): index lookups
//! missed every node created after the reopen while a label scan saw them, so
//! a re-import could not find (and purge) the previous import's rows.
//!
//! Every lookup shape is compared with a label-scan + filter oracle:
//! Cypher `IN`, Cypher equality, inline-map match, the engine's
//! `find_nodes_by_property`, and `GraphStore::find_nodes_by_properties`
//! (single and multi predicate). The state is checked live, after close +
//! reopen (WAL replay), and after a second reopen.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use grafeo_common::types::{NodeId, Value};
use grafeo_engine::session::TransactionalNodeCreate;
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::tempdir;

const KEY: &str = "k";

/// Publish a base generation of `:L` nodes with a property index on `k` (so
/// the generation carries a PropertyIndex section).
fn publish_indexed_base(root: &Path) {
    std::fs::create_dir_all(root).expect("create generation root");
    let source = GrafeoDB::new_in_memory();
    source.create_property_index(KEY);
    source
        .execute_cypher(
            "CREATE (:L {name: 'b1', k: 'b1', tag: 't'}), \
                    (:L {name: 'b2', k: 'b2', tag: 't'}), \
                    (:L {name: 'b3', k: 'b3', tag: 't'}), \
                    (:L {name: 'b4', k: 'b4', tag: 't'}), \
                    (:L {name: 'b5', k: 'b5', tag: 't'})",
        )
        .expect("seed base");
    source
        .build_and_publish_generation(generation_build_request(root, "g1"))
        .expect("publish base generation");
    drop(source);
}

fn open_with(root: &Path, read_only: bool) -> GrafeoDB {
    let db = GrafeoDB::open_generation_root(root, read_only).expect("open generation root");
    assert!(
        db.layered_store().is_some(),
        "generation root opens layered"
    );
    assert!(
        db.has_property_index(KEY),
        "the base generation's property index is registered after open"
    );
    db
}

fn open(root: &Path) -> GrafeoDB {
    open_with(root, false)
}

/// id -> name for every live `:L` node, read through a label scan.
fn names_by_id(db: &GrafeoDB) -> BTreeMap<u64, String> {
    let result = db
        .execute_cypher("MATCH (n:L) RETURN id(n), n.name")
        .expect("label scan");
    result
        .rows()
        .iter()
        .map(|row| {
            let id = row[0].as_int64().expect("id") as u64;
            let name = row[1].as_str().expect("name").to_string();
            (id, name)
        })
        .collect()
}

/// Oracle: label scan + filter on the merged view.
fn oracle(db: &GrafeoDB, value: &str) -> Vec<String> {
    let result = db
        .execute_cypher("MATCH (n:L) RETURN n.name, n.k")
        .expect("label scan");
    let mut names: Vec<String> = result
        .rows()
        .iter()
        .filter(|row| row[1].as_str() == Some(value))
        .map(|row| row[0].as_str().expect("name").to_string())
        .collect();
    names.sort();
    names
}

fn ids_to_names(names: &BTreeMap<u64, String>, ids: &[NodeId]) -> Vec<String> {
    let mut out: Vec<String> = ids
        .iter()
        .map(|id| {
            names
                .get(&id.as_u64())
                .cloned()
                .unwrap_or_else(|| format!("<not live: {}>", id.as_u64()))
        })
        .collect();
    out.sort();
    out
}

fn cypher_names(db: &GrafeoDB, query: &str) -> Vec<String> {
    let result = db.execute_cypher(query).expect(query);
    let mut names: Vec<String> = result
        .rows()
        .iter()
        .map(|row| row[0].as_str().expect("name").to_string())
        .collect();
    names.sort();
    names
}

/// Expected state: every probed `k` value -> names of the live `:L` nodes
/// holding it. Values that must match nothing stay in the map with an empty
/// list so they are still probed. `None` probes a value only for index ==
/// oracle, without asserting what the data itself holds (used where a
/// pre-existing data defect, pinned by an ignored test below, makes the
/// label-scan result itself wrong).
#[derive(Default)]
struct Expected(BTreeMap<&'static str, Option<Vec<&'static str>>>);

impl Expected {
    fn set(&mut self, value: &'static str, names: &[&'static str]) {
        let mut names = names.to_vec();
        names.sort_unstable();
        self.0.insert(value, Some(names));
    }

    fn index_matches_oracle_only(&mut self, value: &'static str) {
        self.0.insert(value, None);
    }
}

/// Compare every lookup shape with the oracle, and the oracle with the
/// explicit expectation. Returns the failures instead of panicking so that
/// every stage of a test reports independently.
fn check(db: &GrafeoDB, stage: &str, expected: &Expected) -> Vec<String> {
    let names = names_by_id(db);
    let graph = db.graph_store();
    let mut failures = Vec::new();
    for (value, want) in &expected.0 {
        let value = *value;
        let oracle = oracle(db, value);
        if let Some(want) = want
            && oracle != *want
        {
            failures.push(format!(
                "[{stage}] label-scan oracle k = {value:?}: got {oracle:?}, expected {want:?}"
            ));
        }

        let shapes: Vec<(&str, Vec<String>)> = vec![
            (
                "cypher IN",
                cypher_names(
                    db,
                    &format!("MATCH (n:L) WHERE n.k IN ['{value}'] RETURN n.name"),
                ),
            ),
            (
                "cypher =",
                cypher_names(
                    db,
                    &format!("MATCH (n:L) WHERE n.k = '{value}' RETURN n.name"),
                ),
            ),
            (
                "cypher inline map",
                cypher_names(db, &format!("MATCH (n:L {{k: '{value}'}}) RETURN n.name")),
            ),
            (
                "GrafeoDB::find_nodes_by_property",
                ids_to_names(&names, &db.find_nodes_by_property(KEY, &Value::from(value))),
            ),
            (
                "GraphStore::find_nodes_by_properties (single)",
                ids_to_names(
                    &names,
                    &graph.find_nodes_by_properties(&[(KEY, Value::from(value))]),
                ),
            ),
            (
                "GraphStore::find_nodes_by_properties (multi)",
                ids_to_names(
                    &names,
                    &graph.find_nodes_by_properties(&[
                        (KEY, Value::from(value)),
                        ("tag", Value::from("t")),
                    ]),
                ),
            ),
        ];
        for (shape, got) in shapes {
            if got != oracle {
                failures.push(format!(
                    "[{stage}] {shape} k = {value:?}: got {got:?}, oracle {oracle:?}"
                ));
            }
        }
    }
    failures
}

fn assert_no_failures(failures: &[String]) {
    assert!(
        failures.is_empty(),
        "{} index lookup(s) disagree with the label-scan oracle or the expected state:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// The writes of the first post-reopen session, applied to the database and
/// to the expected state.
fn first_session_writes(db: &GrafeoDB, expected: &mut Expected) {
    for base in ["b1", "b2", "b3", "b4", "b5"] {
        expected.set(base, &[base]);
    }

    // (a) creates through three APIs; n1 is also updated in place.
    db.execute_cypher("CREATE (:L {name: 'n1', k: 'n1-old', tag: 't'})")
        .expect("create n1");
    db.execute_cypher("MATCH (n:L {name: 'n1'}) SET n.k = 'n1'")
        .expect("update n1");
    expected.set("n1", &["n1"]);
    expected.set("n1-old", &[]);

    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .create_nodes_with_props_transactional(&[TransactionalNodeCreate {
            labels: vec!["L".to_string()],
            properties: vec![
                ("name".to_string(), Value::from("n2")),
                (KEY.to_string(), Value::from("n2")),
                ("tag".to_string(), Value::from("t")),
            ],
        }])
        .expect("batch create n2");
    session.commit().expect("commit");
    drop(session);
    expected.set("n2", &["n2"]);

    db.create_node_with_props(
        &["L"],
        [
            ("name", Value::from("n3")),
            (KEY, Value::from("n3")),
            ("tag", Value::from("t")),
        ],
    )
    .expect("create n3");
    expected.set("n3", &["n3"]);

    // (b) base update, (c) base delete.
    db.execute_cypher("MATCH (n:L {name: 'b2'}) SET n.k = 'b2-new'")
        .expect("update base b2");
    expected.set("b2", &[]);
    expected.set("b2-new", &["b2"]);
    db.execute_cypher("MATCH (n:L {name: 'b3'}) DETACH DELETE n")
        .expect("delete base b3");
    expected.set("b3", &[]);

    // (d) create then delete.
    db.execute_cypher("CREATE (:L {name: 'n4', k: 'n4', tag: 't'})")
        .expect("create n4");
    db.execute_cypher("MATCH (n:L {name: 'n4'}) DETACH DELETE n")
        .expect("delete n4");
    expected.set("n4", &[]);

    // REMOVE on a base node and on an overlay node.
    db.execute_cypher("MATCH (n:L {name: 'b4'}) REMOVE n.k")
        .expect("remove base b4.k");
    expected.set("b4", &[]);
    db.execute_cypher("CREATE (:L {name: 'n6', k: 'n6', tag: 't'})")
        .expect("create n6");
    db.execute_cypher("MATCH (n:L {name: 'n6'}) REMOVE n.k")
        .expect("remove n6.k");
    expected.set("n6", &[]);

    // MERGE goes through the index-backed multi-property lookup: a new key
    // is created once, and existing base / overlay keys are matched rather
    // than duplicated.
    for _ in 0..2 {
        db.execute_cypher("MERGE (n:L {k: 'm1'}) ON CREATE SET n.name = 'm1', n.tag = 't'")
            .expect("merge m1");
    }
    expected.set("m1", &["m1"]);
    db.execute_cypher("MERGE (n:L {k: 'n2'}) ON CREATE SET n.name = 'n2-dup', n.tag = 't'")
        .expect("merge existing overlay n2");
    db.execute_cypher("MERGE (n:L {k: 'b1'}) ON CREATE SET n.name = 'b1-dup', n.tag = 't'")
        .expect("merge existing base b1");

    // Rolled-back writes leave no trace in the index: a base update, an
    // overlay update and a create.
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher("MATCH (n:L {name: 'b5'}) SET n.k = 'b5-rb'")
        .expect("update base b5 in tx");
    session
        .execute_cypher("MATCH (n:L {name: 'n3'}) SET n.k = 'n3-rb'")
        .expect("update overlay n3 in tx");
    session
        .execute_cypher("CREATE (:L {name: 'rb1', k: 'rb1', tag: 't'})")
        .expect("create rb1 in tx");
    session.rollback().expect("rollback");
    drop(session);
    expected.set("rb1", &[]);
    // Pre-existing on d63e3708: the live handle keeps a rolled-back SET (see
    // `rolled_back_set_is_undone_on_live_handle`). The index must still agree
    // with the label scan; the replayed reopen below asserts the true values.
    for value in ["b5", "b5-rb", "n3", "n3-rb"] {
        expected.index_matches_oracle_only(value);
    }
}

#[test]
fn reopened_generation_root_index_sees_overlay_writes() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("d5.grafeo.d");
    publish_indexed_base(&root);
    let mut expected = Expected::default();
    let mut failures = Vec::new();

    // Live handle, writes made after the first reopen.
    let db = open(&root);
    first_session_writes(&db, &mut expected);
    expected.set("n5", &[]);
    expected.set("n7", &[]);
    failures.extend(check(&db, "live after reopen", &expected));
    db.close().expect("close");
    drop(db);

    // (e) WAL replay: the overlay is rebuilt from the WAL on top of the
    // restored mapped postings. Nothing of the rolled-back transaction
    // reached the WAL, so its values are exact again from here on.
    let db = open(&root);
    expected.set("b5", &["b5"]);
    expected.set("b5-rb", &[]);
    expected.set("n3", &["n3"]);
    expected.set("n3-rb", &[]);
    failures.extend(check(&db, "after close + reopen (WAL replay)", &expected));
    db.execute_cypher("CREATE (:L {name: 'n5', k: 'n5', tag: 't'})")
        .expect("create n5");
    expected.set("n5", &["n5"]);
    failures.extend(check(&db, "write after replayed reopen", &expected));
    db.close().expect("close");
    drop(db);

    let db = open(&root);
    failures.extend(check(&db, "after second reopen", &expected));
    db.close().expect("close");
    drop(db);

    // Read-only reopen: the mapped index is served without a writable
    // overlay of its own; replayed writes must still be found.
    let db = open_with(&root, true);
    failures.extend(check(&db, "read-only reopen", &expected));
    drop(db);

    // Epoch handoff: publish the overlay into a new base generation from
    // the live handle, then keep writing.
    let db = open(&root);
    db.run_epoch_handoff(generation_build_request(&root, "g2"))
        .expect("epoch handoff");
    failures.extend(check(&db, "after epoch handoff", &expected));
    db.execute_cypher("CREATE (:L {name: 'n7', k: 'n7', tag: 't'})")
        .expect("create n7");
    db.execute_cypher("MATCH (n:L {name: 'n1'}) SET n.k = 'n1-post-handoff'")
        .expect("update n1 after handoff");
    expected.set("n7", &["n7"]);
    expected.set("n1", &[]);
    expected.set("n1-post-handoff", &["n1"]);
    failures.extend(check(&db, "writes after epoch handoff", &expected));
    db.close().expect("close");
    drop(db);

    // Pre-existing on d63e3708: writes made after an epoch handoff are lost
    // on reopen (see `writes_after_epoch_handoff_survive_reopen`). The index
    // must still agree with the label scan.
    for value in ["n1", "n1-post-handoff", "n7"] {
        expected.index_matches_oracle_only(value);
    }
    let db = open(&root);
    failures.extend(check(&db, "reopen after epoch handoff", &expected));

    assert_no_failures(&failures);
}

/// A UNIQUE constraint is checked through the property index: it must see
/// overlay rows created after the reopen and must not see stale values of
/// base rows changed after the reopen.
#[test]
fn unique_constraint_on_reopened_generation_root() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("unique.grafeo.d");
    publish_indexed_base(&root);
    let db = open(&root);
    db.execute_cypher("CREATE CONSTRAINT uk FOR (n:L) REQUIRE n.k IS UNIQUE")
        .expect("create constraint");

    db.execute_cypher("CREATE (:L {name: 'u1', k: 'u1', tag: 't'})")
        .expect("create u1");
    assert!(
        db.execute_cypher("CREATE (:L {name: 'u1-dup', k: 'u1', tag: 't'})")
            .is_err(),
        "duplicate of an overlay row created after reopen is rejected"
    );
    assert!(
        db.execute_cypher("CREATE (:L {name: 'b1-dup', k: 'b1', tag: 't'})")
            .is_err(),
        "duplicate of a base row is rejected"
    );
    db.execute_cypher("MATCH (n:L {name: 'b2'}) SET n.k = 'b2-moved'")
        .expect("move base b2");
    db.execute_cypher("CREATE (:L {name: 'b2-again', k: 'b2', tag: 't'})")
        .expect("the old value of a changed base row is free again");
    assert_eq!(oracle(&db, "u1"), vec!["u1".to_string()]);
    assert_eq!(oracle(&db, "b1"), vec!["b1".to_string()]);
    assert_eq!(oracle(&db, "b2"), vec!["b2-again".to_string()]);
}

/// Pre-existing data defect pinned for the record (not an index defect):
/// on d63e3708 a Cypher SET inside an explicit transaction on a reopened
/// generation root is not undone by `rollback()` on the live handle, for a
/// base node and for an overlay node alike. A reopen restores the values
/// (nothing reached the WAL).
#[test]
#[ignore = "pre-existing on d63e3708: rollback does not undo a SET on a reopened generation root"]
fn rolled_back_set_is_undone_on_live_handle() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("rollback.grafeo.d");
    publish_indexed_base(&root);
    let db = open(&root);
    db.execute_cypher("CREATE (:L {name: 'n3', k: 'n3', tag: 't'})")
        .expect("create n3");
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher("MATCH (n:L {name: 'b5'}) SET n.k = 'b5-rb'")
        .expect("update base b5 in tx");
    session
        .execute_cypher("MATCH (n:L {name: 'n3'}) SET n.k = 'n3-rb'")
        .expect("update overlay n3 in tx");
    session.rollback().expect("rollback");
    drop(session);
    assert_eq!(oracle(&db, "b5"), vec!["b5".to_string()], "base SET undone");
    assert_eq!(
        oracle(&db, "n3"),
        vec!["n3".to_string()],
        "overlay SET undone"
    );
}

/// Pre-existing data defect pinned for the record (not an index defect):
/// on d63e3708 writes made on the live handle after `run_epoch_handoff`
/// are gone after close + reopen.
#[test]
#[ignore = "pre-existing on d63e3708: writes after an epoch handoff are lost on reopen"]
fn writes_after_epoch_handoff_survive_reopen() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("handoff.grafeo.d");
    publish_indexed_base(&root);
    let db = open(&root);
    db.execute_cypher("CREATE (:L {name: 'n1', k: 'n1', tag: 't'})")
        .expect("create n1");
    db.run_epoch_handoff(generation_build_request(&root, "g2"))
        .expect("epoch handoff");
    db.execute_cypher("CREATE (:L {name: 'n7', k: 'n7', tag: 't'})")
        .expect("create n7");
    db.execute_cypher("MATCH (n:L {name: 'n1'}) SET n.k = 'n1-post-handoff'")
        .expect("update n1 after handoff");
    assert_eq!(oracle(&db, "n7"), vec!["n7".to_string()], "live handle");
    db.close().expect("close");
    drop(db);
    let db = open(&root);
    assert_eq!(oracle(&db, "n7"), vec!["n7".to_string()], "create survives");
    assert_eq!(
        oracle(&db, "n1-post-handoff"),
        vec!["n1".to_string()],
        "update survives"
    );
}

/// Same scenario through the downstream probe's exact shape: the overlay
/// node is created only through the transactional batch API and looked up
/// with an `IN` list, both on the live handle and after a reopen.
#[test]
fn batch_created_overlay_node_found_by_in_lookup_after_reopen() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("probe.grafeo.d");
    publish_indexed_base(&root);

    let count = |db: &GrafeoDB, q: &str| -> i64 {
        db.execute_cypher(q).expect(q).rows()[0][0]
            .as_int64()
            .expect("count")
    };
    let db = open(&root);
    let mut s = db.session();
    s.begin_transaction().expect("begin");
    s.create_nodes_with_props_transactional(&[TransactionalNodeCreate {
        labels: vec!["Z".to_string()],
        properties: vec![(KEY.to_string(), Value::from("z1"))],
    }])
    .expect("batch create");
    s.commit().expect("commit");
    drop(s);
    assert_eq!(count(&db, "MATCH (z:Z) RETURN count(z)"), 1, "label scan");
    assert_eq!(
        count(&db, "MATCH (z:Z) WHERE z.k IN ['z1'] RETURN count(z)"),
        1,
        "index IN lookup finds the batch-created overlay node"
    );
    assert_eq!(
        count(&db, "MATCH (n:L) WHERE n.k IN ['b1'] RETURN count(n)"),
        1,
        "base rows still served by the restored index"
    );
    db.close().expect("close");
    drop(db);

    let db = open(&root);
    assert_eq!(
        count(&db, "MATCH (z:Z) WHERE z.k IN ['z1'] RETURN count(z)"),
        1,
        "index IN lookup finds the replayed overlay node after reopen"
    );
    let mut params = HashMap::new();
    params.insert(
        "ids".to_string(),
        Value::from(vec![Value::from("z1"), Value::from("b1")]),
    );
    let both = db
        .session()
        .execute_language(
            "MATCH (n) WHERE n.k IN $ids RETURN count(n)",
            "cypher",
            Some(params),
        )
        .expect("parameterised IN");
    assert_eq!(
        both.rows()[0][0].as_int64(),
        Some(2),
        "parameterised IN over base + overlay values"
    );
}
