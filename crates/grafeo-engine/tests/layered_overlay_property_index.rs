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
use grafeo_engine::{GrafeoDB, GraphStore, generation_build_request};
use tempfile::tempdir;

const KEY: &str = "k";

/// Publish a base generation of three `:L` nodes with a property index on
/// `k` (so the generation carries a PropertyIndex section).
fn publish_indexed_base(root: &Path) {
    std::fs::create_dir_all(root).expect("create generation root");
    let source = GrafeoDB::new_in_memory();
    source.create_property_index(KEY);
    source
        .execute_cypher(
            "CREATE (:L {name: 'b1', k: 'b1', tag: 't'}), \
                    (:L {name: 'b2', k: 'b2', tag: 't'}), \
                    (:L {name: 'b3', k: 'b3', tag: 't'})",
        )
        .expect("seed base");
    source
        .build_and_publish_generation(generation_build_request(root, "g1"))
        .expect("publish base generation");
    drop(source);
}

fn open(root: &Path) -> GrafeoDB {
    let db = GrafeoDB::open_generation_root(root, false).expect("open generation root");
    assert!(db.layered_store().is_some(), "generation root opens layered");
    assert!(
        db.has_property_index(KEY),
        "the base generation's property index is registered after open"
    );
    db
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

/// Expected live state after the post-reopen writes, keyed by `k` value.
fn expected(after_second_session: bool) -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        ("b1", vec!["b1"]),     // untouched base node
        ("b2", vec![]),         // (b) old value of an updated base node
        ("b2-new", vec!["b2"]), // (b) new value of an updated base node
        ("b3", vec![]),         // (c) deleted base node
        ("n1", vec!["n1"]),     // (a) created via Cypher CREATE, then updated
        ("n1-old", vec![]),     // first value of that overlay node
        ("n2", vec!["n2"]),     // (a) created via the transactional batch API
        ("n3", vec!["n3"]),     // (a) created via GrafeoDB::create_node_with_props
        ("n4", vec![]),         // (d) created then deleted after reopen
        // created only after the replayed reopen
        ("n5", if after_second_session { vec!["n5"] } else { vec![] }),
    ]
}

/// Check every lookup shape against the oracle and the explicit expectation.
fn check(db: &GrafeoDB, stage: &str, after_second_session: bool) {
    let names = names_by_id(db);
    let graph = db.graph_store();
    let mut failures = Vec::new();
    for (value, want) in expected(after_second_session) {
        let want: Vec<String> = want.iter().map(|s| (*s).to_string()).collect();
        let oracle = oracle(db, value);
        assert_eq!(oracle, want, "[{stage}] oracle for k = {value:?}");

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
    assert!(
        failures.is_empty(),
        "{} index lookup(s) disagree with the label-scan oracle:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// The writes of the first post-reopen session: (a) creates through three
/// APIs, (b) a base update, (c) a base delete, (d) create-then-delete, plus
/// an in-place update of an overlay node.
fn first_session_writes(db: &GrafeoDB) {
    db.execute_cypher("CREATE (:L {name: 'n1', k: 'n1-old', tag: 't'})")
        .expect("create n1");
    db.execute_cypher("MATCH (n:L {name: 'n1'}) SET n.k = 'n1'")
        .expect("update n1");

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

    db.create_node_with_props(
        &["L"],
        [
            ("name", Value::from("n3")),
            (KEY, Value::from("n3")),
            ("tag", Value::from("t")),
        ],
    )
    .expect("create n3");

    db.execute_cypher("MATCH (n:L {name: 'b2'}) SET n.k = 'b2-new'")
        .expect("update base b2");
    db.execute_cypher("MATCH (n:L {name: 'b3'}) DETACH DELETE n")
        .expect("delete base b3");

    db.execute_cypher("CREATE (:L {name: 'n4', k: 'n4', tag: 't'})")
        .expect("create n4");
    db.execute_cypher("MATCH (n:L {name: 'n4'}) DETACH DELETE n")
        .expect("delete n4");
}

#[test]
fn reopened_generation_root_index_sees_overlay_writes() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("d5.grafeo.d");
    publish_indexed_base(&root);

    // Live handle, writes made after the first reopen.
    let db = open(&root);
    first_session_writes(&db);
    check(&db, "live after reopen", false);
    db.close().expect("close");
    drop(db);

    // (e) WAL replay: the overlay is rebuilt from the WAL on top of the
    // restored mapped postings.
    let db = open(&root);
    check(&db, "after close + reopen (WAL replay)", false);
    // A write after the replayed reopen must be indexed too.
    db.execute_cypher("CREATE (:L {name: 'n5', k: 'n5', tag: 't'})")
        .expect("create n5");
    check(&db, "write after replayed reopen", true);
    db.close().expect("close");
    drop(db);

    let db = open(&root);
    check(&db, "after second reopen", true);
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
    params.insert("ids".to_string(), Value::from(vec![Value::from("z1"), Value::from("b1")]));
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
