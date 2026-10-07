//! AMH #183: building a CompactStore must keep an absent property absent.
//!
//! When some rows of a label (or edge type) lack a property that others
//! carry, the in-memory CompactStore builder used to fill the gap with the
//! column's type default: `""`, `0`, `0.0`, `false`, or an all-zero vector.
//! After `compact()` (or any build through that builder) the property then
//! read back as present. A property removed before compaction came back the
//! same way.
//!
//! Every test snapshots the whole graph (labels and exact property maps of
//! every node and edge) before the operation and requires the same graph
//! after it, then checks that property lookups do not match absent rows.

#![cfg(all(feature = "lpg", feature = "compact-store", feature = "cypher"))]

use std::collections::BTreeMap;

use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};
use grafeo_engine::GrafeoDB;

type Props = BTreeMap<String, Value>;

/// Every node (id → sorted labels + properties) and edge (id → type +
/// properties).
#[derive(Debug, PartialEq)]
struct Snapshot {
    nodes: BTreeMap<u64, (Vec<String>, Props)>,
    edges: BTreeMap<u64, (String, u64, u64, Props)>,
}

fn ids(db: &GrafeoDB, query: &str) -> Vec<u64> {
    db.execute_cypher(query)
        .expect(query)
        .rows()
        .iter()
        .map(|r| match r[0] {
            Value::Int64(v) => v as u64,
            ref other => panic!("{query}: {other:?}"),
        })
        .collect()
}

fn props(p: &grafeo_common::types::PropertyMap) -> Props {
    p.iter()
        .map(|(k, v)| (k.as_str().to_string(), v.clone()))
        .collect()
}

fn snapshot(db: &GrafeoDB) -> Snapshot {
    let store = db.graph_store();
    let mut nodes = BTreeMap::new();
    for id in ids(db, "MATCH (n) RETURN id(n)") {
        // Through the graph store the session reads (a tier-chain view
        // during a build), not only the layered base + overlay.
        let n = store.get_node(NodeId::new(id)).expect("node");
        let mut labels: Vec<String> = n.labels.iter().map(|l| l.to_string()).collect();
        labels.sort();
        nodes.insert(id, (labels, props(&n.properties)));
    }
    let mut edges = BTreeMap::new();
    for id in ids(db, "MATCH ()-[r]->() RETURN id(r)") {
        let e = store.get_edge(EdgeId::new(id)).expect("edge");
        edges.insert(
            id,
            (
                e.edge_type.to_string(),
                e.src.as_u64(),
                e.dst.as_u64(),
                props(&e.properties),
            ),
        );
    }
    Snapshot { nodes, edges }
}

fn vector(x: f32) -> Value {
    Value::Vector(vec![x, x + 0.1, x + 0.2, x + 0.3].into())
}

/// A label whose rows carry different property sets, every value type, and
/// an edge type whose edges do too. `full` carries everything, `bare` only
/// its name.
fn seed(db: &GrafeoDB) {
    let full = db
        .create_node_with_props(
            &["Doc"],
            [
                ("name", Value::from("full")),
                ("s", Value::from("text")),
                ("n", Value::Int64(5)),
                ("neg", Value::Int64(-5)),
                ("f", Value::Float64(1.5)),
                ("b", Value::Bool(true)),
                ("v", vector(0.5)),
            ],
        )
        .expect("full");
    let bare = db
        .create_node_with_props(&["Doc"], [("name", Value::from("bare"))])
        .expect("bare");
    let other = db
        .create_node_with_props(
            &["Doc"],
            [("name", Value::from("other")), ("n", Value::Int64(7))],
        )
        .expect("other");
    let e1 = db.create_edge(full, bare, "LINK");
    db.set_edge_property(e1, "since", Value::Int64(2020));
    db.set_edge_property(e1, "w", Value::Float64(0.5));
    db.set_edge_property(e1, "tag", Value::from("t"));
    let _e2 = db.create_edge(bare, other, "LINK");
    // Two edges from one source, created in reverse target order with
    // different property sets: a builder that mixed up insertion order and
    // CSR order would swap their properties.
    let late = db
        .create_node_with_props(&["Doc"], [("name", Value::from("late"))])
        .expect("late");
    let e3 = db.create_edge(other, late, "LINK");
    db.set_edge_property(e3, "tag", Value::from("to-late"));
    let e4 = db.create_edge(other, full, "LINK");
    db.set_edge_property(e4, "since", Value::Int64(1999));
}

/// Property lookups never match a row that lacks the property.
fn assert_no_absent_matches(db: &GrafeoDB, stage: &str) {
    let store = db.graph_store();
    let bare = NodeId::new(ids(db, "MATCH (n:Doc {name: 'bare'}) RETURN id(n)")[0]);
    for (key, default) in [
        ("s", Value::from("")),
        ("n", Value::Int64(0)),
        ("neg", Value::Int64(0)),
        ("f", Value::Float64(0.0)),
        ("b", Value::Bool(false)),
    ] {
        assert!(
            !store.find_nodes_by_property(key, &default).contains(&bare),
            "[{stage}] {key} = {default:?} matched a row without {key}"
        );
        assert!(
            !store
                .find_nodes_in_range(key, None, None, true, true)
                .contains(&bare),
            "[{stage}] an open range on {key} matched a row without {key}"
        );
        assert_eq!(
            store.get_node_property(bare, &PropertyKey::new(key)),
            None,
            "[{stage}] get_node_property {key}"
        );
    }
    let count = |q: &str| match &db.execute_cypher(q).expect(q).rows()[0][0] {
        Value::Int64(v) => *v,
        other => panic!("{q}: {other:?}"),
    };
    assert_eq!(
        count("MATCH (n:Doc) WHERE n.v IS NULL RETURN count(n)"),
        3,
        "[{stage}] only `full` has a vector"
    );
    assert_eq!(
        count("MATCH ()-[r:LINK]->() WHERE r.since IS NULL RETURN count(r)"),
        2,
        "[{stage}] two of the four edges have `since`"
    );
}

#[test]
fn compact_keeps_absent_properties_absent() {
    let mut db = GrafeoDB::new_in_memory();
    seed(&db);
    let before = snapshot(&db);
    db.compact().expect("compact");
    assert_eq!(snapshot(&db), before, "graph changed by compact()");
    assert_no_absent_matches(&db, "compact");
}

#[test]
fn compact_after_a_removal_keeps_it_removed() {
    let mut db = GrafeoDB::new_in_memory();
    seed(&db);
    // `other` gets everything, then loses it again (string, vector, int).
    db.execute_cypher(
        "MATCH (n:Doc {name: 'other'}) SET n.s = 'x', n.v = vector([1.0, 1.0, 1.0, 1.0]), n.f = 2.5",
    )
    .expect("set");
    db.execute_cypher("MATCH (n:Doc {name: 'other'}) REMOVE n.s, n.v, n.f")
        .expect("remove");
    let before = snapshot(&db);
    db.compact().expect("compact");
    assert_eq!(snapshot(&db), before, "graph changed by compact()");
    assert_no_absent_matches(&db, "compact after removal");
}

#[cfg(feature = "grafeo-file")]
#[test]
fn compact_file_keeps_absent_properties_absent_across_reopen() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("absent.grafeo");
    let before = {
        let mut db = GrafeoDB::open(&path).expect("open");
        seed(&db);
        let before = snapshot(&db);
        db.compact().expect("compact");
        assert_eq!(snapshot(&db), before, "graph changed by compact()");
        db.close().expect("close");
        before
    };
    let db = GrafeoDB::open(&path).expect("reopen");
    assert_eq!(snapshot(&db), before, "graph changed by close + reopen");
    assert_no_absent_matches(&db, "reopen");
}

#[cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "mmap"
))]
mod generation {
    use super::*;
    use grafeo_engine::generation_build_request;

    /// The generation build from a live graph (streaming builder), reopened.
    #[test]
    fn generation_build_keeps_absent_properties_absent() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("absent.grafeo.d");
        std::fs::create_dir_all(&root).expect("root");
        let source = GrafeoDB::new_in_memory();
        seed(&source);
        let before = snapshot(&source);
        source
            .build_and_publish_generation(generation_build_request(&root, "g1"))
            .expect("publish");
        drop(source);
        let db = GrafeoDB::open_generation_root(&root, true).expect("open root");
        assert_eq!(
            snapshot(&db),
            before,
            "graph changed by the generation build"
        );
        assert_no_absent_matches(&db, "generation build");
    }

    /// AMH's code-index sidecar path: an empty compacted builder DB, a
    /// mid-build drain to a tier, then the final tier-chain build.
    #[test]
    fn tier_drain_build_keeps_absent_properties_absent() {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("tiers.grafeo.d");
        std::fs::create_dir_all(&root).expect("root");
        let mut db = GrafeoDB::new_in_memory();
        db.compact().expect("compact (builder path)");
        seed(&db);
        let before = snapshot(&db);
        db.drain_overlay_to_tier(&dir.path().join("tiers"), "absent-drain")
            .expect("drain");
        assert_eq!(snapshot(&db), before, "graph changed by the tier drain");
        db.build_and_publish_generation(generation_build_request(&root, "g1"))
            .expect("publish");
        drop(db);
        let db = GrafeoDB::open_generation_root(&root, true).expect("open root");
        assert_eq!(
            snapshot(&db),
            before,
            "graph changed by the tier-chain build"
        );
        assert_no_absent_matches(&db, "tier-chain build");
    }
}

/// Found alongside #183: `compact()` mapped a source's edges to the wrong
/// CSR rows when they were created out of target order. CSR rows keep
/// insertion order within a source, while `from_graph_store_preserving_ids`
/// assigned edge ids in (source, target) order, so each edge read another's
/// target and properties. Every edge here carries the same key set, so this
/// fails on trunk for that reason alone.
#[test]
fn compact_keeps_each_edge_its_own_target_and_properties() {
    let mut db = GrafeoDB::new_in_memory();
    let src = db
        .create_node_with_props(&["Doc"], [("name", Value::from("src"))])
        .expect("src");
    let t1 = db
        .create_node_with_props(&["Doc"], [("name", Value::from("t1"))])
        .expect("t1");
    let t2 = db
        .create_node_with_props(&["Doc"], [("name", Value::from("t2"))])
        .expect("t2");
    // Created to the later target first.
    for (target, tag) in [(t2, "to-t2"), (t1, "to-t1")] {
        let e = db.create_edge(src, target, "LINK");
        db.set_edge_property(e, "tag", Value::from(tag));
    }
    let before = snapshot(&db);
    db.compact().expect("compact");
    assert_eq!(
        snapshot(&db),
        before,
        "an edge changed target or properties"
    );
}
