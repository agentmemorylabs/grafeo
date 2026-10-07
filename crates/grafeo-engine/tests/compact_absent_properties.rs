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

    use grafeo_engine::{CompactPayloadVersion, Config};

    /// Exactly the nodes a vector index on `Doc.v` serves: only `full`
    /// carries `v`. Built anew on `db` (plain and scalar-quantized), so a
    /// fabricated zero vector would be indexed as a real one.
    #[cfg(feature = "vector-index")]
    fn assert_vector_membership(db: &GrafeoDB, stage: &str) {
        use grafeo_engine::IndexedVectorRead;
        for quantization in [None, Some("scalar")] {
            let _ = db.drop_vector_index("Doc", "v");
            db.create_vector_index(
                "Doc",
                "v",
                Some(4),
                Some("cosine"),
                None,
                None,
                quantization,
            )
            .expect("vector index");
            for name in ["full", "bare", "other", "late"] {
                let id = grafeo_common::types::NodeId::new(
                    ids(
                        db,
                        &format!("MATCH (n:Doc {{name: '{name}'}}) RETURN id(n)"),
                    )[0],
                );
                let read = db
                    .read_indexed_node_vector("Doc", "v", id)
                    .expect("indexed read");
                match (name, read) {
                    ("full", IndexedVectorRead::Found(v)) => {
                        assert_eq!(Value::Vector(v.into()), vector(0.5), "[{stage}] full");
                    }
                    (n, IndexedVectorRead::Absent) if n != "full" => {}
                    (n, other) => panic!("[{stage}] {quantization:?}: {n} reads {other:?}"),
                }
            }
        }
    }

    /// A compacted source, so the build reads the compact base through its
    /// row cursors, in `version`.
    fn compacted_source(version: CompactPayloadVersion) -> GrafeoDB {
        let mut source =
            GrafeoDB::with_config(Config::in_memory().with_compact_payload_version(version))
                .expect("source");
        seed(&source);
        let before = snapshot(&source);
        source.compact().expect("compact");
        assert_eq!(snapshot(&source), before, "graph changed by compact()");
        source
    }

    /// Review r1 must-fix 1: a generation built from a compact base (the
    /// base row cursors) keeps absent properties absent, in the default and
    /// the forced-v6 payload, and a vector index rebuilt on it serves only
    /// the node that has a vector.
    #[test]
    fn compact_base_then_generation_keeps_absent_properties_absent() {
        for version in [CompactPayloadVersion::Auto, CompactPayloadVersion::V6] {
            let dir = tempfile::tempdir().expect("temp dir");
            let root = dir.path().join("base.grafeo.d");
            std::fs::create_dir_all(&root).expect("root");
            let source = compacted_source(version);
            let before = snapshot(&source);
            source
                .build_and_publish_generation(generation_build_request(&root, "g1"))
                .expect("publish");
            drop(source);
            let db = GrafeoDB::open_generation_root(&root, false).expect("open root");
            let stage = format!("{version:?} compact base -> generation");
            assert_eq!(snapshot(&db), before, "{stage}");
            assert_no_absent_matches(&db, &stage);
            #[cfg(feature = "vector-index")]
            assert_vector_membership(&db, &stage);
        }
    }

    /// Review r1 must-fix 1: an epoch handoff over such a root (untouched
    /// rows through the base cursors, a dirty row through the overlay) and a
    /// second generation keep every row exact, across reopen.
    #[test]
    fn handoff_over_a_sparse_base_keeps_absent_properties_absent() {
        for version in [CompactPayloadVersion::Auto, CompactPayloadVersion::V6] {
            let dir = tempfile::tempdir().expect("temp dir");
            let root = dir.path().join("handoff.grafeo.d");
            std::fs::create_dir_all(&root).expect("root");
            let source = compacted_source(version);
            source
                .build_and_publish_generation(generation_build_request(&root, "g1"))
                .expect("publish");
            drop(source);
            let open = || {
                GrafeoDB::open_generation_root_with_config(
                    Config::persistent(&root).with_compact_payload_version(version),
                )
                .expect("open root")
            };
            let stage = format!("{version:?} handoff");
            let want = {
                let db = open();
                // A dirty row with a new, sparse column; the rest untouched.
                db.execute_cypher("MATCH (n:Doc {name: 'bare'}) SET n.extra = 1")
                    .expect("dirty write");
                let want = snapshot(&db);
                let report = db
                    .run_epoch_handoff(generation_build_request(&root, "g2"))
                    .expect("handoff");
                db.publish_and_install_handoff(report).expect("install");
                assert_eq!(snapshot(&db), want, "[{stage}] after install");
                assert_no_absent_matches(&db, &stage);
                db.close().expect("close");
                want
            };
            let db = open();
            assert_eq!(snapshot(&db), want, "[{stage}] reopen");
            assert_no_absent_matches(&db, &format!("{stage} reopen"));
            assert_eq!(
                db.graph_store()
                    .find_nodes_by_property("extra", &Value::Int64(0)),
                Vec::<grafeo_common::types::NodeId>::new(),
                "[{stage}] rows without extra"
            );
            #[cfg(feature = "vector-index")]
            assert_vector_membership(&db, &format!("{stage} reopen"));
        }
    }

    /// Lane 3's reproduction (review r1 must-fix 1, handoff path): a sparse
    /// string column on a mapped generation base. `extra` is written on B0
    /// only; the first handoff carries it as an overlay row, but every later
    /// handoff with no writes rebuilds B1..B4 through the base row cursors,
    /// which read the column body's placeholder (on trunk, the dictionary
    /// string `"B"`) as a stored value. Exact whole-graph oracle at every
    /// generation and after reopen; also with `extra` removed again between
    /// handoffs.
    #[test]
    fn repeated_handoffs_over_a_mapped_base_keep_a_sparse_column_sparse() {
        for remove in [false, true] {
            let dir = tempfile::tempdir().expect("temp dir");
            let root = dir.path().join("sparse.grafeo.d");
            std::fs::create_dir_all(&root).expect("root");
            let source = GrafeoDB::new_in_memory();
            let ids: Vec<_> = (0..5_i64)
                .map(|i| {
                    source
                        .create_node_with_props(
                            &["B"],
                            [
                                ("name", Value::from(format!("B{i}"))),
                                ("k", Value::Int64(i)),
                                ("tag", Value::from("base")),
                            ],
                        )
                        .expect("node")
                })
                .collect();
            source
                .build_and_publish_generation(generation_build_request(&root, "g1"))
                .expect("publish g1");
            drop(source);
            let db = GrafeoDB::open_generation_root(&root, false).expect("open");
            db.set_node_property(ids[0], "extra", Value::from("pre"))
                .expect("extra on B0");
            let mut want = snapshot(&db);
            let stage = |g: &str| format!("remove={remove}, {g}");
            for (n, generation) in ["g2", "g3", "g4"].into_iter().enumerate() {
                if remove && n == 1 {
                    assert!(db.remove_node_property(ids[0], "extra"), "remove extra");
                    want = snapshot(&db);
                }
                let report = db
                    .run_epoch_handoff(generation_build_request(&root, generation))
                    .expect("handoff");
                db.publish_and_install_handoff(report).expect("install");
                assert_eq!(snapshot(&db), want, "[{}]", stage(generation));
                for id in &ids[1..] {
                    assert_eq!(
                        db.graph_store()
                            .get_node_property(*id, &PropertyKey::new("extra")),
                        None,
                        "[{}] {id:?} has no extra",
                        stage(generation)
                    );
                }
            }
            db.close().expect("close");
            drop(db);
            let db = GrafeoDB::open_generation_root(&root, true).expect("reopen");
            assert_eq!(snapshot(&db), want, "[{}]", stage("reopen"));
        }
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

/// Review r1 should-fix: a list or map property cannot be stored faithfully
/// in a compact column (its Dict fallback stored the formatted string), so
/// `compact()` refuses it with a typed error and leaves the graph as it was.
#[test]
fn compact_refuses_list_and_map_values() {
    for (what, value) in [
        (
            "list",
            Value::List(vec![Value::Int64(1), Value::Int64(2)].into()),
        ),
        (
            "map",
            Value::Map(std::sync::Arc::new(
                [(PropertyKey::new("k"), Value::Int64(1))]
                    .into_iter()
                    .collect(),
            )),
        ),
    ] {
        let mut db = GrafeoDB::new_in_memory();
        seed(&db);
        db.create_node_with_props(&["Doc"], [("name", Value::from("p")), ("p", value)])
            .expect("node");
        let before = snapshot(&db);
        let err = db.compact().expect_err("compact() over a list/map value");
        assert!(
            err.to_string().contains("unsupported value"),
            "{what}: {err}"
        );
        assert_eq!(
            snapshot(&db),
            before,
            "{what}: graph changed by the refused compact()"
        );
    }
}

/// Review r1 must-fix 2: the lazy range reader of a `CompactStore` rechecks
/// each candidate on its stored value, as the eager one does: an absent row
/// and a present-null row hold a placeholder body (`0`, `""`, `false`) and
/// must not match; a genuinely stored `0`/`""`/`false` must.
mod range_parity {
    use super::*;
    use grafeo_core::graph::traits::{GraphStore, GraphStoreSearch};

    /// `zero` stores genuine zero values, `none` lacks the keys, `null`
    /// stores them as `Null` (whatever the LPG keeps of that, the oracle is
    /// the eager reader on the same store).
    fn seed_ranges(db: &GrafeoDB) {
        db.create_node_with_props(
            &["Doc"],
            [
                ("name", Value::from("five")),
                ("n", Value::Int64(5)),
                ("s", Value::from("text")),
                ("b", Value::Bool(true)),
            ],
        )
        .expect("five");
        db.create_node_with_props(
            &["Doc"],
            [
                ("name", Value::from("zero")),
                ("n", Value::Int64(0)),
                ("s", Value::from("")),
                ("b", Value::Bool(false)),
            ],
        )
        .expect("zero");
        db.create_node_with_props(&["Doc"], [("name", Value::from("none"))])
            .expect("none");
        db.create_node_with_props(
            &["Doc"],
            [
                ("name", Value::from("null")),
                ("n", Value::Null),
                ("s", Value::Null),
                ("b", Value::Null),
            ],
        )
        .expect("null");
    }

    fn names(
        db: &GrafeoDB,
        ids: impl IntoIterator<Item = grafeo_common::types::NodeId>,
    ) -> Vec<String> {
        let mut out: Vec<String> = ids
            .into_iter()
            .map(|id| {
                match db
                    .graph_store()
                    .get_node_property(id, &PropertyKey::new("name"))
                {
                    Some(Value::String(s)) => s.to_string(),
                    other => panic!("{id:?}: {other:?}"),
                }
            })
            .collect();
        out.sort();
        out
    }

    /// Eager and lazy readers of the compact base agree, and match exactly
    /// the rows that store a value in range.
    fn assert_parity(db: &GrafeoDB, stage: &str) {
        let base = db.layered_store().expect("layered").base_store_arc();
        let cases: [(&str, Option<Value>, Option<Value>, &[&str]); 6] = [
            ("n", None, None, &["five", "zero"]),
            ("n", Some(Value::Int64(0)), Some(Value::Int64(0)), &["zero"]),
            ("n", None, Some(Value::Int64(3)), &["zero"]),
            ("s", Some(Value::from("")), Some(Value::from("")), &["zero"]),
            ("s", None, None, &["five", "zero"]),
            (
                "b",
                Some(Value::Bool(false)),
                Some(Value::Bool(false)),
                &["zero"],
            ),
        ];
        for (key, min, max, want) in cases {
            let eager = base.find_nodes_in_range(key, min.as_ref(), max.as_ref(), true, true);
            let lazy: Vec<_> = base
                .find_nodes_in_range_iter(key, min.as_ref(), max.as_ref(), true, true)
                .collect();
            let (eager, lazy) = (names(db, eager), names(db, lazy));
            assert_eq!(eager, want, "[{stage}] eager {key} in {min:?}..={max:?}");
            assert_eq!(lazy, want, "[{stage}] lazy {key} in {min:?}..={max:?}");
        }
    }

    /// v5: a compacted single file, after `compact()` and after reopen.
    #[cfg(feature = "grafeo-file")]
    #[test]
    fn eager_and_lazy_ranges_agree_on_a_compact_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("ranges.grafeo");
        {
            let mut db = GrafeoDB::open(&path).expect("open");
            seed_ranges(&db);
            db.compact().expect("compact");
            assert_parity(&db, "after compact");
            db.close().expect("close");
        }
        assert_parity(&GrafeoDB::open(&path).expect("reopen"), "v5 reopen");
    }

    /// v5 and forced v6 generation payloads, after reopen.
    #[cfg(all(
        feature = "generation",
        feature = "generation-streaming",
        feature = "mmap"
    ))]
    #[test]
    fn eager_and_lazy_ranges_agree_on_generation_payloads() {
        use grafeo_engine::{CompactPayloadVersion, Config, generation_build_request};
        for version in [CompactPayloadVersion::Auto, CompactPayloadVersion::V6] {
            let dir = tempfile::tempdir().expect("temp dir");
            let root = dir.path().join("ranges.grafeo.d");
            std::fs::create_dir_all(&root).expect("root");
            let source =
                GrafeoDB::with_config(Config::in_memory().with_compact_payload_version(version))
                    .expect("source");
            seed_ranges(&source);
            source
                .build_and_publish_generation(generation_build_request(&root, "g1"))
                .expect("publish");
            drop(source);
            let db = GrafeoDB::open_generation_root(&root, true).expect("open root");
            assert_parity(&db, &format!("{version:?} generation"));
        }
    }
}

/// Review r1 should-fix: the read-only audit reads the stored values
/// themselves. It reports a nonempty all-zero vector (with its index
/// membership) and an empty string under an audited key, wherever they came
/// from, and an edge whose type, endpoints or properties differ from a
/// trusted baseline; a matching baseline and real values report nothing.
#[test]
fn audit_reports_stored_defaults_and_edge_drift() {
    use grafeo_engine::{EdgeIdentity, EmptyString, ZeroVector};

    let db = GrafeoDB::new_in_memory();
    let real = db
        .create_node_with_props(
            &["Unit"],
            [
                ("name", Value::from("real")),
                ("qualified_name", Value::from("a::b")),
                ("embedding", vector(0.5)),
            ],
        )
        .expect("real");
    let zero = db
        .create_node_with_props(
            &["Unit"],
            [
                ("name", Value::from("zero")),
                ("qualified_name", Value::from("")),
                ("embedding", Value::Vector(vec![0.0f32; 4].into())),
            ],
        )
        .expect("zero");
    #[cfg(feature = "vector-index")]
    db.create_vector_index(
        "Unit",
        "embedding",
        Some(4),
        Some("euclidean"),
        None,
        None,
        None,
    )
    .expect("vector index");
    let e = db.create_edge(real, zero, "CALLS");
    db.set_edge_property(e, "w", Value::Int64(1));
    let baseline = |src, dst, w| EdgeIdentity {
        id: e,
        edge_type: "CALLS".into(),
        src,
        dst,
        properties: Some([("w".to_string(), Value::Int64(w))].into_iter().collect()),
    };

    let audit =
        db.audit_fabricated_defaults(&["Unit"], &["qualified_name"], &[baseline(real, zero, 1)]);
    assert_eq!(audit.nodes_scanned, 2);
    assert_eq!(audit.edges_checked, 1);
    #[cfg(feature = "vector-index")]
    let indexed = Some(true);
    #[cfg(not(feature = "vector-index"))]
    let indexed = None;
    assert_eq!(
        audit.zero_vectors,
        vec![ZeroVector {
            node: zero,
            label: "Unit".into(),
            property: "embedding".into(),
            dimensions: 4,
            indexed,
        }]
    );
    assert_eq!(
        audit.empty_strings,
        vec![EmptyString {
            node: zero,
            label: "Unit".into(),
            property: "qualified_name".into(),
        }]
    );
    assert!(
        audit.edge_mismatches.is_empty(),
        "{:?}",
        audit.edge_mismatches
    );

    // A swapped target, and swapped properties, against the baseline.
    for wrong in [baseline(real, real, 1), baseline(real, zero, 2)] {
        let audit = db.audit_fabricated_defaults(&["Unit"], &[], std::slice::from_ref(&wrong));
        assert!(audit.empty_strings.is_empty(), "no audited string keys");
        assert_eq!(audit.edge_mismatches.len(), 1, "{wrong:?}");
        assert_eq!(audit.edge_mismatches[0].expected, wrong);
        let found = audit.edge_mismatches[0]
            .found
            .as_ref()
            .expect("edge exists");
        assert_eq!((found.src, found.dst), (real, zero));
    }
}

fn doc(
    db: &GrafeoDB,
    name: &str,
    extra: Vec<(&'static str, Value)>,
) -> grafeo_common::types::NodeId {
    let mut props = vec![("name", Value::from(name))];
    props.extend(extra);
    db.create_node_with_props(&["Doc"], props).expect("doc")
}

fn edge(
    db: &GrafeoDB,
    src: grafeo_common::types::NodeId,
    dst: grafeo_common::types::NodeId,
    kind: &str,
    props: Vec<(&str, Value)>,
) {
    let e = db.create_edge(src, dst, kind);
    for (k, v) in props {
        db.set_edge_property(e, k, v);
    }
}

/// Review r1 coverage: edge identity across several edge types and label
/// pairs, a self loop and parallel edges with different properties, and
/// explicit `Null`s in every supported column family (nodes and edges),
/// through `compact()`, overlay writes and a second `compact()` (base plus
/// overlay), then a generation build from that base.
#[test]
fn identity_and_nulls_survive_compact_recompact_and_generation() {
    let mut db = GrafeoDB::new_in_memory();
    let a = doc(
        &db,
        "a",
        vec![
            ("s", Value::from("x")),
            ("n", Value::Int64(1)),
            ("f", Value::Float64(0.5)),
            ("b", Value::Bool(true)),
            ("v", vector(0.5)),
        ],
    );
    let b = doc(
        &db,
        "b",
        vec![
            ("s", Value::Null),
            ("n", Value::Null),
            ("f", Value::Null),
            ("b", Value::Null),
            ("v", Value::Null),
        ],
    );
    let c = doc(&db, "c", vec![]);
    let tag = db
        .create_node_with_props(&["Tag"], [("name", Value::from("t"))])
        .expect("tag");
    // Parallel edges a -> b, created out of property order.
    edge(
        &db,
        a,
        b,
        "LINK",
        vec![("w", Value::Int64(2)), ("tag", Value::from("second"))],
    );
    edge(&db, a, b, "LINK", vec![("w", Value::Int64(1))]);
    edge(
        &db,
        a,
        b,
        "LINK",
        vec![("w", Value::Null), ("tag", Value::Null)],
    );
    // Self loop, another source, another type and label pair.
    edge(&db, c, c, "LINK", vec![("tag", Value::from("loop"))]);
    edge(&db, c, a, "LINK", vec![]);
    edge(&db, a, tag, "TAGGED", vec![("score", Value::Float64(0.9))]);
    edge(&db, b, tag, "TAGGED", vec![]);

    let before = snapshot(&db);
    db.compact().expect("compact");
    assert_eq!(snapshot(&db), before, "graph changed by compact()");

    // Overlay writes over the compact base, then compact base plus overlay.
    db.execute_cypher("MATCH (n:Doc {name: 'c'}) SET n.s = 'late'")
        .expect("overlay SET");
    edge(&db, b, a, "LINK", vec![("w", Value::Int64(9))]);
    edge(&db, c, tag, "TAGGED", vec![("score", Value::Null)]);
    let before = snapshot(&db);
    db.compact().expect("recompact");
    assert_eq!(
        snapshot(&db),
        before,
        "graph changed by the second compact()"
    );

    #[cfg(all(
        feature = "generation",
        feature = "generation-streaming",
        feature = "mmap"
    ))]
    {
        use grafeo_engine::generation_build_request;
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("ids.grafeo.d");
        std::fs::create_dir_all(&root).expect("root");
        db.build_and_publish_generation(generation_build_request(&root, "g1"))
            .expect("publish");
        drop(db);
        let db = GrafeoDB::open_generation_root(&root, true).expect("open root");
        assert_eq!(
            snapshot(&db),
            before,
            "graph changed by the generation build"
        );
    }
}
