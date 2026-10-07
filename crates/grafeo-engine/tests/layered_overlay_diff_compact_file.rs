//! D10 on compact files: an overlay row of a base entity is persisted whole
//! (base merged in, removed properties absent), which is what binaries
//! before D10 wrote too, and turned back into a diff on load
//! (`LayeredStore::adopt_persisted_full_rows`). A base property the
//! persisted row lacks was removed and must stay removed.
//!
//! `fixtures/legacy_layered_overlay.grafeo` was written by a binary WITHOUT
//! D10 (fork trunk `850e69f3`): a compacted file whose overlay holds full
//! copy-ups of node `a` and of its `LINK` edge with a key PHYSICALLY removed
//! (`SET a.p = 8`, then `GrafeoDB::remove_node_property(a, "q")`; `SET r.w =
//! 0.9`, then `remove_edge_property(r, "since")`), and node `c` after a Cypher
//! `REMOVE c.q` (stored as `Null` by that binary). Reading a row with a
//! physically removed key as a diff would bring `a.q` / `r.since` back.

#![cfg(all(
    feature = "lpg",
    feature = "compact-store",
    feature = "grafeo-file",
    feature = "cypher"
))]

use std::collections::BTreeMap;
use std::path::Path;

use grafeo_common::types::{EdgeId, NodeId, Value};
use grafeo_engine::GrafeoDB;

const SEED: &str = "CREATE (a:Doc {name: 'a', p: 7, q: 'keep', embedding: vector([0.1, 0.2, 0.3, 0.4])}), \
     (b:Doc {name: 'b', p: 1, q: 'b', embedding: vector([0.5, 0.6, 0.7, 0.8])}), \
     (c:Doc {name: 'c', p: 3, q: 'x', embedding: vector([0.9, 1.0, 1.1, 1.2])}), \
     (a)-[:LINK {since: 2020, w: 0.5}]->(b)";

fn one_id(db: &GrafeoDB, query: &str) -> u64 {
    let r = db.execute_cypher(query).expect(query);
    assert_eq!(r.row_count(), 1, "one row: {query}");
    match r.rows()[0][0] {
        Value::Int64(v) => v as u64,
        ref other => panic!("{query}: {other:?}"),
    }
}

fn node_id(db: &GrafeoDB, name: &str) -> NodeId {
    NodeId::new(one_id(
        db,
        &format!("MATCH (n:Doc {{name: '{name}'}}) RETURN id(n)"),
    ))
}

fn edge_id(db: &GrafeoDB) -> EdgeId {
    EdgeId::new(one_id(
        db,
        "MATCH (:Doc {name: 'a'})-[r:LINK]->(:Doc {name: 'b'}) RETURN id(r)",
    ))
}

fn as_map(props: &grafeo_common::types::PropertyMap) -> BTreeMap<String, Value> {
    props
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.clone()))
        .collect()
}

fn map(entries: &[(&str, Value)]) -> BTreeMap<String, Value> {
    entries
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

fn vec4(x: [f32; 4]) -> Value {
    Value::Vector(x.to_vec().into())
}

/// The exact state both scenarios must show: `a.q`, `c.q` and `r.since`
/// removed, `b` untouched.
fn assert_state(db: &GrafeoDB, stage: &str) {
    let node = |name: &str| as_map(&db.get_node(node_id(db, name)).expect("node").properties);
    assert_eq!(
        node("a"),
        map(&[
            ("name", Value::from("a")),
            ("p", Value::Int64(8)),
            ("embedding", vec4([0.1, 0.2, 0.3, 0.4])),
        ]),
        "[{stage}] a"
    );
    assert_eq!(
        node("b"),
        map(&[
            ("name", Value::from("b")),
            ("p", Value::Int64(1)),
            ("q", Value::from("b")),
            ("embedding", vec4([0.5, 0.6, 0.7, 0.8])),
        ]),
        "[{stage}] b (untouched)"
    );
    assert_eq!(
        node("c"),
        map(&[
            ("name", Value::from("c")),
            ("p", Value::Int64(3)),
            ("embedding", vec4([0.9, 1.0, 1.1, 1.2])),
        ]),
        "[{stage}] c"
    );
    assert_eq!(
        as_map(&db.get_edge(edge_id(db)).expect("edge").properties),
        map(&[("w", Value::Float64(0.9))]),
        "[{stage}] LINK"
    );
}

/// Open, check, close; reopen, check, close; reopen, check. Each close
/// checkpoints the overlay again (persisted whole, converted back on load).
fn check_cycles(path: &Path) {
    for stage in ["open", "reopen", "second reopen"] {
        let db = GrafeoDB::open(path).expect("open");
        assert_state(&db, stage);
        db.close().expect("close");
    }
}

#[test]
fn legacy_full_copy_rows_keep_their_removals() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("legacy.grafeo");
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy_layered_overlay.grafeo"),
        &path,
    )
    .expect("copy fixture");
    check_cycles(&path);
}

/// The same scenario written by this binary (diff rows and tombstones,
/// persisted whole).
#[test]
fn diff_rows_round_trip_through_a_compact_file() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("d10.grafeo");
    {
        let mut db = GrafeoDB::open(&path).expect("open");
        db.execute_cypher(SEED).expect("seed");
        db.compact().expect("compact");
        let a = node_id(&db, "a");
        let r = edge_id(&db);
        db.execute_cypher("MATCH (n:Doc {name: 'a'}) SET n.p = 8")
            .expect("SET a.p");
        assert!(db.remove_node_property(a, "q"), "remove a.q");
        db.execute_cypher("MATCH (:Doc {name: 'a'})-[r:LINK]->() SET r.w = 0.9")
            .expect("SET r.w");
        assert!(db.remove_edge_property(r, "since"), "remove r.since");
        db.execute_cypher("MATCH (n:Doc {name: 'c'}) REMOVE n.q")
            .expect("REMOVE c.q");
        assert_state(&db, "before close");
        db.close().expect("close");
    }
    check_cycles(&path);
}

/// `compact()` over the layered file keeps every removal: the rebuilt
/// CompactStore marks absent cells absent (AMH #183) instead of filling them
/// with the column's type default (`""`, a zero vector).
#[test]
fn compact_after_a_removal_keeps_it_removed() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("compact.grafeo");
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy_layered_overlay.grafeo"),
        &path,
    )
    .expect("copy fixture");
    let mut db = GrafeoDB::open(&path).expect("open");
    db.compact().expect("compact");
    assert_state(&db, "after compact");
}

/// A compact file's diff-row vector survives a `ForceDisk` open. The open
/// wires the layered store before it registers the vector consumer, so the
/// consumer knows which rows are base nodes' diff rows and keeps their
/// vectors in the overlay. Spilled, the diff row would lose its vector key
/// and the merged read would serve the stale base vector instead. Checked
/// through the exact indexed read and ANN, not only the inline property.
#[cfg(all(feature = "vector-index", feature = "mmap", not(feature = "temporal")))]
#[test]
fn force_disk_open_keeps_a_diff_row_vector() {
    use grafeo_common::storage::{SectionType, TierOverride};
    use grafeo_engine::{Config, IndexedVectorRead};

    const DIMS: usize = 16;
    let vector =
        |seed: usize| -> Vec<f32> { (0..DIMS).map(|d| (seed * 31 + d) as f32 * 0.001).collect() };
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("fd.grafeo");
    let spill = dir.path().join("spill");
    let v1 = vector(99);
    {
        let mut db = GrafeoDB::open(&path).expect("open");
        for i in 0..3 {
            db.create_node_with_props(
                &["Doc"],
                [
                    ("name", Value::from(format!("d{i}"))),
                    ("embedding", Value::Vector(vector(i).into())),
                ],
            )
            .expect("create");
        }
        db.create_vector_index(
            "Doc",
            "embedding",
            Some(DIMS),
            Some("cosine"),
            None,
            None,
            None,
        )
        .expect("vector index");
        db.compact().expect("compact");
        let d0 = node_id(&db, "d0");
        db.session()
            .set_node_property(d0, "embedding", Value::Vector(v1.clone().into()))
            .expect("new vector");
        // An overlay-only node: its vector is the one the spill takes.
        db.create_node_with_props(
            &["Doc"],
            [
                ("name", Value::from("d3")),
                ("embedding", Value::Vector(vector(50).into())),
            ],
        )
        .expect("create d3");
        db.close().expect("close");
    }
    let db = GrafeoDB::with_config(
        Config::persistent(&path)
            .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk)
            .with_spill_path(&spill),
    )
    .expect("ForceDisk open");
    let d0 = node_id(&db, "d0");
    let d1 = node_id(&db, "d1");
    let d3 = node_id(&db, "d3");
    // The spill ran: d3's vector left the overlay for the spill file, while
    // d0's diff-row vector stayed in the overlay.
    assert!(
        std::fs::read_dir(&spill).is_ok_and(|mut d| d.next().is_some()),
        "the ForceDisk open spilled the vector column"
    );
    let overlay = db.layered_store().expect("layered").overlay_store();
    let key = grafeo_common::types::PropertyKey::new("embedding");
    assert_eq!(overlay.get_node_property(d3, &key), None, "d3 spilled");
    assert_eq!(
        overlay.get_node_property(d0, &key),
        Some(Value::Vector(v1.clone().into())),
        "d0's diff row keeps its vector"
    );
    let embedding = |id: NodeId| {
        db.get_node(id)
            .expect("node")
            .properties
            .get(&grafeo_common::types::PropertyKey::new("embedding"))
            .cloned()
    };
    assert_eq!(
        embedding(d0),
        Some(Value::Vector(v1.clone().into())),
        "d0 row"
    );
    match db
        .read_indexed_node_vector("Doc", "embedding", d0)
        .expect("indexed read")
    {
        IndexedVectorRead::Found(got) => assert_eq!(&got[..], &v1[..], "indexed read of d0"),
        other => panic!("indexed read of d0: {other:?}"),
    }
    for (id, want, what) in [
        (d1, vector(1), "d1 (base)"),
        (d3, vector(50), "d3 (spilled)"),
    ] {
        match db
            .read_indexed_node_vector("Doc", "embedding", id)
            .expect("indexed read")
        {
            IndexedVectorRead::Found(got) => {
                assert_eq!(&got[..], &want[..], "indexed read of {what}");
            }
            other => panic!("indexed read of {what}: {other:?}"),
        }
    }
    // Served the stale base vector, d0 would lose the V1 query to d2.
    for (id, query) in [(d0, v1.clone()), (d1, vector(1)), (d3, vector(50))] {
        let hits = db
            .vector_search("Doc", "embedding", &query, 4, Some(64), None)
            .expect("vector search");
        assert_eq!(
            hits.first().map(|(hit, _)| *hit),
            Some(id),
            "{id:?} is the nearest neighbour of its own vector: {hits:?}"
        );
    }
}
