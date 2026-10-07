//! D10 slice 1: on a writable generation root, a write to a base node keeps
//! an overlay *diff row* (labels plus the written properties) instead of
//! copying the whole node, embedding included, into the overlay.
//!
//! These tests pin the merged view across every path that used to rely on
//! the copy: Cypher and direct reads, property removal (tombstones), rollback,
//! property lookups (with and without an index), relation creates, reopen
//! (WAL replay), an epoch handoff (the capture materializes whole rows) and a
//! ForceDisk reopen (a diff row's new vector is not spilled behind a stale
//! base vector). The last test measures what one small update now costs.
//! Slice 2 adds the same for a base edge's properties.
//!
//! See `docs/architecture/storage/layered-overlay-diff.md`.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use grafeo_common::types::{NodeId, PropertyKey, Value};
use grafeo_core::graph::traits::GraphStore;
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::TempDir;

const DIMS: usize = 16;

fn vector(seed: usize, dims: usize) -> Value {
    Value::Vector(
        (0..dims)
            .map(|d| (seed * 31 + d) as f32 * 0.001)
            .collect::<Vec<f32>>()
            .into(),
    )
}

/// Publishes `count` `MemoryEntity` nodes `e0..` (AMH's shape: identity,
/// observations, provenance, an embedding of `dims`) plus one relation
/// `e0 -> e1`.
fn publish(root: &Path, count: usize, dims: usize) {
    publish_with(root, count, dims, false);
}

/// [`publish`], optionally with a vector index on `MemoryEntity.embedding`
/// carried in the generation (so it exists at every open).
fn publish_with(root: &Path, count: usize, dims: usize, vector_index: bool) {
    std::fs::create_dir_all(root).expect("create root");
    let source = GrafeoDB::new_in_memory();
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let id = source
            .create_node_with_props(
                &["MemoryEntity"],
                [
                    ("name", Value::from(format!("e{i}"))),
                    ("account_id", Value::from("acct")),
                    ("observations_json", Value::from("[\"o0\"]")),
                    ("embedding_provider", Value::from("voyage")),
                    ("updated_at_ms", Value::Int64(1_000 + i as i64)),
                    ("embedding", vector(i, dims)),
                ],
            )
            .expect("create entity");
        ids.push(id);
    }
    if count > 1 {
        source.create_edge_with_props(
            ids[0],
            ids[1],
            "MemoryEntityRelation",
            [
                ("rel_type", Value::from("base")),
                ("since", Value::Int64(2020)),
                ("weight", Value::Float64(0.5)),
            ],
        );
    }
    #[cfg(feature = "vector-index")]
    if vector_index {
        source
            .create_vector_index(
                "MemoryEntity",
                "embedding",
                Some(dims),
                Some("cosine"),
                None,
                None,
                None,
            )
            .expect("vector index");
    }
    #[cfg(not(feature = "vector-index"))]
    let _ = vector_index;
    source
        .build_and_publish_generation(generation_build_request(root, "g1"))
        .expect("publish base generation");
}

fn fresh_root(count: usize, dims: usize) -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("diff.grafeo.d");
    publish(&root, count, dims);
    (dir, root)
}

fn open(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, false).expect("open generation root writable")
}

fn handoff(db: &GrafeoDB, root: &Path, generation: &str) {
    let report = db
        .run_epoch_handoff(generation_build_request(root, generation))
        .expect("epoch handoff");
    db.publish_and_install_handoff(report)
        .expect("publish and install handoff");
}

fn id_of(db: &GrafeoDB, name: &str) -> NodeId {
    let r = db
        .execute_cypher(&format!(
            "MATCH (n:MemoryEntity {{name: '{name}'}}) RETURN id(n)"
        ))
        .expect("lookup");
    assert_eq!(r.row_count(), 1, "exactly one {name}");
    match &r.rows()[0][0] {
        Value::Int64(v) => NodeId::new(*v as u64),
        other => panic!("id: {other:?}"),
    }
}

fn count(db: &GrafeoDB, query: &str) -> i64 {
    match &db.execute_cypher(query).expect(query).rows()[0][0] {
        Value::Int64(v) => *v,
        other => panic!("{query}: {other:?}"),
    }
}

fn prop(db: &GrafeoDB, id: NodeId, key: &str) -> Option<Value> {
    db.graph_store()
        .get_node_property(id, &PropertyKey::new(key))
}

/// A property map in a comparable, ordered form.
fn props_map(props: &grafeo_common::types::PropertyMap) -> BTreeMap<String, Value> {
    props
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.clone()))
        .collect()
}

/// `e{i}` as published.
fn published_entity(i: usize) -> BTreeMap<String, Value> {
    published_entity_of(i, DIMS)
}

/// `e{i}` as published with a `dims`-wide embedding.
fn published_entity_of(i: usize, dims: usize) -> BTreeMap<String, Value> {
    [
        ("name", Value::from(format!("e{i}"))),
        ("account_id", Value::from("acct")),
        ("observations_json", Value::from("[\"o0\"]")),
        ("embedding_provider", Value::from("voyage")),
        ("updated_at_ms", Value::Int64(1_000 + i as i64)),
        ("embedding", vector(i, dims)),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

/// Exact-state oracle: `e{i}` as published with `overrides` applied (`Some`
/// = written value, `None` = removed) is exactly what every reader sees:
/// `get_node` (labels and the whole property map, no extra or missing key),
/// `get_node_property` per key (removed keys absent), the epoch read at the
/// current epoch, and the latest history entry when there is one.
fn assert_entity(
    db: &GrafeoDB,
    id: NodeId,
    i: usize,
    overrides: &[(&str, Option<Value>)],
    stage: &str,
) {
    let mut want = published_entity(i);
    for (key, value) in overrides {
        match value {
            Some(v) => want.insert((*key).to_string(), v.clone()),
            None => want.remove(*key),
        };
    }
    let node = db.get_node(id).unwrap_or_else(|| panic!("[{stage}] node"));
    let mut labels: Vec<String> = node.labels.iter().map(|l| l.to_string()).collect();
    labels.sort();
    assert_eq!(labels, vec!["MemoryEntity".to_string()], "[{stage}] labels");
    assert_eq!(props_map(&node.properties), want, "[{stage}] get_node");
    for key in published_entity(i).keys().chain(want.keys()) {
        assert_eq!(
            prop(db, id, key).as_ref(),
            want.get(key),
            "[{stage}] get_node_property {key}"
        );
    }
    let at_epoch = db
        .get_node_at_epoch(id, db.current_epoch())
        .unwrap_or_else(|| panic!("[{stage}] get_node_at_epoch"));
    assert_eq!(
        props_map(&at_epoch.properties),
        want,
        "[{stage}] get_node_at_epoch"
    );
    // History: the overlay's versions of a dirty row, merged with the base.
    // A clean base node, or one whose row an install absorbed, has none
    // (base rows carry no versions); a dirty one has at least one.
    let has_row = db
        .layered_store()
        .expect("layered")
        .snapshot_dirty_node_ids()
        .contains(&id);
    for (what, history) in [
        ("db", db.get_node_history(id)),
        ("session", db.session().get_node_history(id)),
    ] {
        assert_eq!(
            !history.is_empty(),
            has_row,
            "[{stage}] {what} history exists exactly when the row is dirty: {history:?}"
        );
        assert!(
            history.windows(2).all(|w| w[0].0 <= w[1].0),
            "[{stage}] {what} history out of epoch order"
        );
        for (epoch, _, entry) in &history {
            assert_eq!(entry.id, id, "[{stage}] {what} history entry id");
            let mut labels: Vec<String> = entry.labels.iter().map(ToString::to_string).collect();
            labels.sort();
            assert_eq!(
                labels,
                vec!["MemoryEntity".to_string()],
                "[{stage}] {what} history labels at {epoch:?}"
            );
            assert!(
                entry.properties.iter().all(|(_, v)| !v.is_null()),
                "[{stage}] {what} history at {epoch:?} shows a tombstone"
            );
        }
        if let Some((_, deleted, latest)) = history.last() {
            assert_eq!(*deleted, None, "[{stage}] {what} latest entry is live");
            assert_eq!(
                props_map(&latest.properties),
                want,
                "[{stage}] {what} history"
            );
        }
    }
}

/// The diff row of a base node holds only what was written.
fn assert_not_copied(db: &GrafeoDB, id: NodeId, key: &str, stage: &str) {
    let overlay = db.layered_store().expect("layered").overlay_store();
    assert!(
        overlay
            .get_node_property(id, &PropertyKey::new(key))
            .is_none(),
        "[{stage}] base property {key} was copied into the overlay"
    );
}

/// Merged reads after a SET; the overlay holds only the SET property, and
/// the merged view survives reopen and an epoch handoff.
#[test]
fn set_keeps_unchanged_base_properties() {
    let (_dir, root) = fresh_root(3, DIMS);
    {
        let db = open(&root);
        let e0 = id_of(&db, "e0");
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e0'}) SET m.observations_json = 'new'")
            .expect("SET");
        assert_eq!(prop(&db, e0, "observations_json"), Some(Value::from("new")));
        assert_entity(
            &db,
            e0,
            0,
            &[("observations_json", Some(Value::from("new")))],
            "after SET",
        );
        assert_not_copied(&db, e0, "embedding", "after SET");
        assert_eq!(
            count(
                &db,
                "MATCH (m:MemoryEntity) WHERE m.observations_json = 'new' AND m.name = 'e0' RETURN count(m)"
            ),
            1
        );
        db.close().expect("close");
    }
    let db = open(&root);
    let e0 = id_of(&db, "e0");
    assert_eq!(
        prop(&db, e0, "observations_json"),
        Some(Value::from("new")),
        "[reopen]"
    );
    assert_entity(
        &db,
        e0,
        0,
        &[("observations_json", Some(Value::from("new")))],
        "reopen",
    );
    assert_not_copied(&db, e0, "embedding", "reopen");
    handoff(&db, &root, "g2");
    assert_eq!(
        prop(&db, e0, "observations_json"),
        Some(Value::from("new")),
        "[handoff]"
    );
    assert_entity(
        &db,
        e0,
        0,
        &[("observations_json", Some(Value::from("new")))],
        "handoff",
    );
    db.close().expect("close");
    // The root lock is released when the handle drops, not at close.
    drop(db);
    let db = open(&root);
    let e0 = id_of(&db, "e0");
    assert_eq!(
        prop(&db, e0, "observations_json"),
        Some(Value::from("new")),
        "[reopen 2]"
    );
    assert_entity(
        &db,
        e0,
        0,
        &[("observations_json", Some(Value::from("new")))],
        "reopen 2",
    );
}

/// AMH's provenance `REMOVE` on a base entity: the property is gone from
/// every view, nothing else is, and the removal survives reopen and an
/// epoch handoff.
#[test]
fn remove_of_a_base_property_is_a_tombstone() {
    let (_dir, root) = fresh_root(3, DIMS);
    let check = |db: &GrafeoDB, stage: &str| {
        let e1 = id_of(db, "e1");
        assert_eq!(
            prop(db, e1, "embedding_provider"),
            None,
            "[{stage}] removed"
        );
        let node = db.get_node(e1).expect("node");
        assert!(
            node.properties
                .get(&PropertyKey::new("embedding_provider"))
                .is_none(),
            "[{stage}] get_node still has the removed property"
        );
        assert_eq!(
            count(
                db,
                "MATCH (m:MemoryEntity {name: 'e1'}) WHERE m.embedding_provider IS NULL RETURN count(m)"
            ),
            1,
            "[{stage}] Cypher IS NULL"
        );
        assert_entity(db, e1, 1, &[("embedding_provider", None)], stage);
    };
    {
        let db = open(&root);
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e1'}) REMOVE m.embedding_provider")
            .expect("REMOVE");
        check(&db, "after REMOVE");
        assert_not_copied(&db, id_of(&db, "e1"), "embedding", "after REMOVE");
        db.close().expect("close");
    }
    let db = open(&root);
    check(&db, "reopen");
    handoff(&db, &root, "g2");
    check(&db, "handoff");
    db.close().expect("close");
    drop(db);
    check(&open(&root), "reopen 2");
}

/// A rolled-back SET and REMOVE on a base node leave its base values.
#[test]
fn rollback_restores_the_base_view() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    let e2 = id_of(&db, "e2");
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher(
            "MATCH (m:MemoryEntity {name: 'e2'}) SET m.observations_json = 'tx' \
             REMOVE m.embedding_provider",
        )
        .expect("SET + REMOVE");
    session.rollback().expect("rollback");
    assert_entity(&db, e2, 2, &[], "rollback");
    drop(session);
    db.close().expect("close");
    drop(db);
    assert_entity(&open(&root), e2, 2, &[], "reopen");
}

/// Property lookups merge base postings of diff rows (the base value still
/// holds) with overlay values, with and without a property index.
#[test]
fn property_lookups_see_the_merged_values() {
    for indexed in [false, true] {
        let (_dir, root) = fresh_root(3, DIMS);
        let db = open(&root);
        if indexed {
            db.create_property_index("name");
        }
        let stage = if indexed { "indexed" } else { "scan" };
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e0'}) SET m.observations_json = 'x'")
            .expect("SET observations");
        assert_eq!(
            count(&db, "MATCH (m:MemoryEntity {name: 'e0'}) RETURN count(m)"),
            1,
            "[{stage}] unchanged key of a diff row"
        );
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e0'}) SET m.name = 'e0b'")
            .expect("SET name");
        assert_eq!(
            count(&db, "MATCH (m:MemoryEntity {name: 'e0'}) RETURN count(m)"),
            0,
            "[{stage}] old"
        );
        assert_eq!(
            count(&db, "MATCH (m:MemoryEntity {name: 'e0b'}) RETURN count(m)"),
            1,
            "[{stage}] new"
        );
        assert_eq!(
            count(
                &db,
                "MATCH (m:MemoryEntity {account_id: 'acct', name: 'e0b'}) RETURN count(m)"
            ),
            1,
            "[{stage}] multi-key, one key changed"
        );
        assert_eq!(
            count(
                &db,
                "MATCH (m:MemoryEntity {account_id: 'acct'}) RETURN count(m)"
            ),
            3,
            "[{stage}] multi-key base value of every row"
        );
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e1'}) SET m.updated_at_ms = 5000")
            .expect("SET updated_at");
        assert_eq!(
            count(
                &db,
                "MATCH (m:MemoryEntity) WHERE m.updated_at_ms >= 1001 AND m.updated_at_ms < 2000 RETURN count(m)"
            ),
            1,
            "[{stage}] range: only e2 (e1 moved out)"
        );
        assert_eq!(
            count(
                &db,
                "MATCH (m:MemoryEntity) WHERE m.updated_at_ms >= 5000 RETURN count(m)"
            ),
            1,
            "[{stage}] range: e1 moved in"
        );
    }
}

/// A relation between two base entities leaves both whole (it used to copy
/// both up, embeddings included).
#[test]
fn relation_create_between_base_entities_keeps_both() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    db.execute_cypher(
        "MATCH (a:MemoryEntity {name: 'e1'}), (b:MemoryEntity {name: 'e2'}) \
         CREATE (a)-[:MemoryEntityRelation {rel_type: 'x'}]->(b)",
    )
    .expect("create relation");
    let (e1, e2) = (id_of(&db, "e1"), id_of(&db, "e2"));
    assert_entity(&db, e1, 1, &[], "src");
    assert_entity(&db, e2, 2, &[], "dst");
    assert_not_copied(&db, e1, "embedding", "src");
    assert_not_copied(&db, e2, "embedding", "dst");
    // Slice 3: an endpoint whose only change is a new edge gets no overlay
    // row at all.
    let overlay = db.layered_store().expect("layered").overlay_store();
    assert!(overlay.get_node(e1).is_none(), "src has an overlay row");
    assert!(overlay.get_node(e2).is_none(), "dst has an overlay row");
    assert_eq!(
        count(
            &db,
            "MATCH (:MemoryEntity {name: 'e1'})-[r:MemoryEntityRelation]->(:MemoryEntity {name: 'e2'}) RETURN count(r)"
        ),
        1
    );
    handoff(&db, &root, "g2");
    assert_entity(&db, e1, 1, &[], "handoff src");
    assert_entity(&db, e2, 2, &[], "handoff dst");
}

/// Adjacency of the three published entities, exactly: per node, sorted
/// `(neighbour name, relation name)` lists for both directions, and both
/// degrees.
type Adjacency = BTreeMap<String, (Vec<(String, String)>, Vec<(String, String)>, usize, usize)>;

fn adjacency(db: &GrafeoDB) -> Adjacency {
    use grafeo_core::graph::Direction;
    let store = db.graph_store();
    let name = |id: NodeId| match prop(db, id, "name") {
        Some(Value::String(n)) => n.to_string(),
        other => panic!("{id:?} name: {other:?}"),
    };
    let rel = |id: grafeo_common::types::EdgeId| match store
        .get_edge_property(id, &PropertyKey::new("rel_type"))
    {
        Some(Value::String(t)) => t.to_string(),
        other => panic!("{id:?} rel_type: {other:?}"),
    };
    let mut out = BTreeMap::new();
    for n in ["e0", "e1", "e2"] {
        let r = db
            .execute_cypher(&format!(
                "MATCH (m:MemoryEntity {{name: '{n}'}}) RETURN id(m)"
            ))
            .expect("lookup");
        let Some(Value::Int64(raw)) = r.rows().first().map(|row| row[0].clone()) else {
            continue; // deleted
        };
        let id = NodeId::new(raw as u64);
        let side = |direction| {
            let mut v: Vec<(String, String)> = store
                .edges_from(id, direction)
                .into_iter()
                .map(|(other, e)| (name(other), rel(e)))
                .collect();
            v.sort();
            v
        };
        out.insert(
            n.to_string(),
            (
                side(Direction::Outgoing),
                side(Direction::Incoming),
                store.out_degree(id),
                store.in_degree(id),
            ),
        );
    }
    out
}

fn adj(entries: &[(&str, &[(&str, &str)], &[(&str, &str)])]) -> Adjacency {
    let pairs = |v: &[(&str, &str)]| -> Vec<(String, String)> {
        v.iter()
            .map(|(a, b)| ((*a).to_string(), (*b).to_string()))
            .collect()
    };
    entries
        .iter()
        .map(|(n, o, i)| ((*n).to_string(), (pairs(o), pairs(i), o.len(), i.len())))
        .collect()
}

/// Slice 3 with no endpoint rows: a relation between base entities is
/// served by adjacency alone. Exact adjacency and degrees through rollback,
/// commit, a refused plain DELETE, DETACH DELETE, reopen and a handoff.
#[test]
fn relation_without_endpoint_rows_adjacency_and_deletes() {
    const RELATE: &str = "MATCH (a:MemoryEntity {name: 'e1'}), (b:MemoryEntity {name: 'e2'}) \
                          CREATE (a)-[:MemoryEntityRelation {rel_type: 'x'}]->(b)";
    let (_dir, root) = fresh_root(3, DIMS);
    let published = adj(&[
        ("e0", &[("e1", "base")], &[]),
        ("e1", &[], &[("e0", "base")]),
        ("e2", &[], &[]),
    ]);
    let related = adj(&[
        ("e0", &[("e1", "base")], &[]),
        ("e1", &[("e2", "x")], &[("e0", "base")]),
        ("e2", &[], &[("e1", "x")]),
    ]);
    let detached = adj(&[
        ("e0", &[("e1", "base")], &[]),
        ("e1", &[], &[("e0", "base")]),
    ]);
    {
        let db = open(&root);
        assert_eq!(adjacency(&db), published, "published");
        let mut session = db.session();
        session.begin_transaction().expect("begin");
        session.execute_cypher(RELATE).expect("relate");
        assert_eq!(adjacency(&db), related, "inside the transaction");
        session.rollback().expect("rollback");
        assert_eq!(adjacency(&db), published, "rollback");
        drop(session);

        db.execute_cypher(RELATE).expect("relate");
        assert_eq!(adjacency(&db), related, "committed");
        let overlay = db.layered_store().expect("layered").overlay_store();
        for n in ["e1", "e2"] {
            assert!(
                overlay.get_node(id_of(&db, n)).is_none(),
                "{n} has an overlay row"
            );
        }
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e2'}) DELETE m")
            .expect_err("a plain DELETE of a node with a relation is refused");
        assert_eq!(adjacency(&db), related, "refused DELETE");
        assert_entity(&db, id_of(&db, "e2"), 2, &[], "refused DELETE");
        db.close().expect("close");
    }
    let db = open(&root);
    assert_eq!(adjacency(&db), related, "reopen");
    db.execute_cypher("MATCH (m:MemoryEntity {name: 'e2'}) DETACH DELETE m")
        .expect("DETACH DELETE");
    assert_eq!(adjacency(&db), detached, "DETACH DELETE");
    assert_eq!(
        count(&db, "MATCH ()-[r:MemoryEntityRelation]->() RETURN count(r)"),
        1,
        "relations after DETACH DELETE"
    );
    db.close().expect("close");
    drop(db);
    let db = open(&root);
    assert_eq!(adjacency(&db), detached, "reopen after DETACH DELETE");
    handoff(&db, &root, "g2");
    assert_eq!(adjacency(&db), detached, "handoff");
    assert_entity(&db, id_of(&db, "e1"), 1, &[], "handoff e1");
    db.close().expect("close");
    drop(db);
    assert_eq!(adjacency(&open(&root)), detached, "reopen after handoff");
}

/// The DB-level label and property-removal APIs edit a base node through
/// its diff row.
#[test]
fn database_label_and_removal_apis_on_base_nodes() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    let e0 = id_of(&db, "e0");
    assert!(db.add_node_label(e0, "Pinned"), "add_node_label");
    assert!(
        db.remove_node_property(e0, "embedding_provider"),
        "remove_node_property"
    );
    assert_eq!(prop(&db, e0, "embedding_provider"), None);
    let mut labels = db.get_node_labels(e0).expect("labels");
    labels.sort();
    assert_eq!(
        labels,
        vec!["MemoryEntity".to_string(), "Pinned".to_string()]
    );
    assert!(db.remove_node_label(e0, "Pinned"), "remove_node_label");
    assert_eq!(
        db.get_node_labels(e0).expect("labels"),
        vec!["MemoryEntity".to_string()]
    );
    assert_entity(
        &db,
        e0,
        0,
        &[("embedding_provider", None)],
        "after label + remove",
    );
}

/// A diff row's new vector survives a ForceDisk reopen. The base carries
/// the vector index, so the open's ForceDisk spill runs over the replayed
/// overlay; a diff row's vector must stay in the overlay, or the merged read
/// would serve the stale base vector. Checked through the exact indexed read
/// and ANN membership, not only the inline property.
#[cfg(all(feature = "vector-index", not(feature = "temporal")))]
#[test]
fn force_disk_reopen_keeps_a_diff_row_vector() {
    use grafeo_common::storage::{SectionType, TierOverride};
    use grafeo_engine::{Config, IndexedVectorRead};

    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("fd.grafeo.d");
    publish_with(&root, 3, DIMS, true);
    let spill = dir.path().join("spill");
    let force_disk = || {
        GrafeoDB::open_generation_root_with_config(
            Config::persistent(&root)
                .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk)
                .with_spill_path(&spill),
        )
        .expect("ForceDisk open")
    };
    let new_vector = vector(99, DIMS);
    {
        let db = force_disk();
        let e0 = id_of(&db, "e0");
        db.session()
            .set_node_property(e0, "embedding", new_vector.clone())
            .expect("new vector");
        db.close().expect("close");
    }
    let db = force_disk();
    let e0 = id_of(&db, "e0");
    let e1 = id_of(&db, "e1");
    assert_entity(
        &db,
        e0,
        0,
        &[("embedding", Some(new_vector.clone()))],
        "ForceDisk reopen",
    );
    assert_entity(&db, e1, 1, &[], "untouched base node");
    let Value::Vector(want) = &new_vector else {
        unreachable!()
    };
    match db
        .read_indexed_node_vector("MemoryEntity", "embedding", e0)
        .expect("indexed read")
    {
        IndexedVectorRead::Found(got) => assert_eq!(&got[..], &want[..], "indexed read of e0"),
        other => panic!("indexed read of e0: {other:?}"),
    }
    for (id, query) in [(e0, new_vector.clone()), (e1, vector(1, DIMS))] {
        let Value::Vector(q) = query else {
            unreachable!()
        };
        let hits = db
            .vector_search("MemoryEntity", "embedding", &q, 3, Some(64), None)
            .expect("vector search");
        assert_eq!(
            hits.first().map(|(hit, _)| *hit),
            Some(id),
            "{id:?} is the nearest neighbour of its own vector: {hits:?}"
        );
    }
}

/// What one small update of a base entity costs. Before D10 every touched
/// entity copied its whole row (here a 2048-dim, 8 KiB embedding) into the
/// overlay; now the overlay holds the written property and a labels-only
/// row. WAL bytes were already per-property (the copy was never logged);
/// this pins that too.
#[test]
fn small_update_of_a_base_entity_does_not_copy_its_embedding() {
    const N: usize = 400;
    const WIDE: usize = 2048;
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("cost.grafeo.d");
    publish(&root, N, WIDE);
    let db = open(&root);
    let layered = db.layered_store().expect("layered").clone();
    let wal_bytes = || dir_bytes(&root.join("wal"));
    let (overlay_before, wal_before, rss_before) =
        (layered.overlay_memory_bytes(), wal_bytes(), rss_anon_kib());

    for i in 0..N {
        db.execute_cypher(&format!(
            "MATCH (m:MemoryEntity {{name: 'e{i}'}}) SET m.observations_json = '[\"o0\",\"o1\"]'"
        ))
        .expect("append observation");
    }

    let overlay_per = (layered.overlay_memory_bytes() - overlay_before) / N;
    let wal_per = (wal_bytes() - wal_before) / N as u64;
    let rss_per_kib = rss_anon_kib().saturating_sub(rss_before) as f64 / N as f64;
    eprintln!(
        "D10 cost per small update of a base entity with a {WIDE}-dim embedding \
         ({} B vector): overlay {overlay_per} B, WAL {wal_per} B, RssAnon {rss_per_kib:.2} KiB",
        WIDE * 4
    );
    assert!(
        overlay_per < 1024,
        "overlay grows {overlay_per} B per small update; the {} B embedding is being copied",
        WIDE * 4
    );
    assert!(wal_per < 1024, "WAL grows {wal_per} B per small update");
    assert_not_copied(&db, id_of(&db, "e7"), "embedding", "cost");
    assert_eq!(
        prop(&db, id_of(&db, "e7"), "embedding"),
        Some(vector(7, WIDE))
    );
}

fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok()?.metadata().ok())
                .filter(|m| m.is_file())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

/// `RssAnon` of this process in KiB (Linux), 0 elsewhere. Reported, not
/// asserted: the allocator makes it noisy at this size.
fn rss_anon_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("RssAnon:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

// ── Slice 2: edge property diffs ──────────────────────────────────────

const BASE_REL: &str = "MATCH (:MemoryEntity {name: 'e0'})-[r:MemoryEntityRelation {rel_type: 'base'}]->(:MemoryEntity {name: 'e1'})";

fn base_rel_id(db: &GrafeoDB) -> grafeo_common::types::EdgeId {
    let r = db
        .execute_cypher(&format!("{BASE_REL} RETURN id(r)"))
        .expect("base relation id");
    assert_eq!(r.row_count(), 1, "one base relation");
    match &r.rows()[0][0] {
        Value::Int64(v) => grafeo_common::types::EdgeId::new(*v as u64),
        other => panic!("id: {other:?}"),
    }
}

fn edge_prop(db: &GrafeoDB, key: &str) -> Option<Value> {
    db.graph_store()
        .get_edge_property(base_rel_id(db), &PropertyKey::new(key))
}

/// Exact-state oracle for the base relation: as published with `overrides`
/// applied, through `get_edge` (whole map), `get_edge_property` per key, and
/// the epoch read at the current epoch.
fn assert_base_rel(db: &GrafeoDB, overrides: &[(&str, Option<Value>)], stage: &str) {
    let published: BTreeMap<String, Value> = [
        ("rel_type", Value::from("base")),
        ("since", Value::Int64(2020)),
        ("weight", Value::Float64(0.5)),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    let mut want = published.clone();
    for (key, value) in overrides {
        match value {
            Some(v) => want.insert((*key).to_string(), v.clone()),
            None => want.remove(*key),
        };
    }
    let id = base_rel_id(db);
    let edge = db
        .graph_store()
        .get_edge(id)
        .unwrap_or_else(|| panic!("[{stage}] edge"));
    assert_eq!(props_map(&edge.properties), want, "[{stage}] get_edge");
    for key in published.keys() {
        assert_eq!(
            edge_prop(db, key).as_ref(),
            want.get(key),
            "[{stage}] get_edge_property {key}"
        );
    }
    let (e0, e1) = (id_of(db, "e0"), id_of(db, "e1"));
    let identity = |edge: &grafeo_core::graph::lpg::Edge, what: &str| {
        assert_eq!(
            (edge.id, edge.src, edge.dst, edge.edge_type.as_str()),
            (id, e0, e1, "MemoryEntityRelation"),
            "[{stage}] {what}: identity, endpoints and type"
        );
    };
    identity(&edge, "get_edge");
    let at_epoch = db
        .get_edge_at_epoch(id, db.current_epoch())
        .unwrap_or_else(|| panic!("[{stage}] get_edge_at_epoch"));
    identity(&at_epoch, "get_edge_at_epoch");
    assert_eq!(
        props_map(&at_epoch.properties),
        want,
        "[{stage}] get_edge_at_epoch"
    );
    // History, as for nodes (`assert_entity`): present exactly when the
    // edge is dirty, in epoch order, no tombstones, the
    // latest entry exact.
    let has_row = db
        .layered_store()
        .expect("layered")
        .snapshot_dirty_edge_ids()
        .contains(&id);
    for (what, history) in [
        ("db", db.get_edge_history(id)),
        ("session", db.session().get_edge_history(id)),
    ] {
        assert_eq!(
            !history.is_empty(),
            has_row,
            "[{stage}] {what} edge history exists exactly when the row is dirty: {history:?}"
        );
        assert!(
            history.windows(2).all(|w| w[0].0 <= w[1].0),
            "[{stage}] {what} edge history out of epoch order"
        );
        for (epoch, _, entry) in &history {
            identity(entry, &format!("{what} edge history at {epoch:?}"));
            assert!(
                entry.properties.iter().all(|(_, v)| !v.is_null()),
                "[{stage}] {what} edge history at {epoch:?} shows a tombstone"
            );
        }
        if let Some((_, deleted, latest)) = history.last() {
            assert_eq!(*deleted, None, "[{stage}] {what} latest edge entry is live");
            assert_eq!(
                props_map(&latest.properties),
                want,
                "[{stage}] {what} edge history"
            );
        }
    }
}

/// SET and REMOVE on a base relation keep its other properties, never copy
/// them into the overlay, and survive reopen and an epoch handoff.
#[test]
fn edge_set_and_remove_keep_the_other_base_properties() {
    let (_dir, root) = fresh_root(3, DIMS);
    let check = |db: &GrafeoDB, stage: &str| {
        assert_eq!(
            edge_prop(db, "weight"),
            Some(Value::Float64(0.9)),
            "[{stage}] weight"
        );
        assert_eq!(edge_prop(db, "since"), None, "[{stage}] since removed");
        assert_eq!(
            count(
                db,
                &format!("{BASE_REL} WHERE r.since IS NULL AND r.weight = 0.9 RETURN count(r)")
            ),
            1,
            "[{stage}] Cypher"
        );
        assert_base_rel(
            db,
            &[("weight", Some(Value::Float64(0.9))), ("since", None)],
            stage,
        );
    };
    {
        let db = open(&root);
        db.execute_cypher(&format!("{BASE_REL} SET r.weight = 0.9 REMOVE r.since"))
            .expect("SET + REMOVE");
        check(&db, "after write");
        let overlay = db.layered_store().expect("layered").overlay_store();
        assert!(
            overlay
                .get_edge_property(base_rel_id(&db), &PropertyKey::new("rel_type"))
                .is_none(),
            "base edge property rel_type was copied into the overlay"
        );
        db.close().expect("close");
    }
    let db = open(&root);
    check(&db, "reopen");
    handoff(&db, &root, "g2");
    check(&db, "handoff");
    db.close().expect("close");
    drop(db);
    check(&open(&root), "reopen 2");
}

/// A rolled-back write on a base relation leaves its base properties.
#[test]
fn edge_rollback_restores_the_base_view() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher(&format!("{BASE_REL} SET r.weight = 0.1 REMOVE r.since"))
        .expect("SET + REMOVE");
    session.rollback().expect("rollback");
    assert_base_rel(&db, &[], "rollback");
}

/// The DB-level edge property removal goes through the diff row too.
#[test]
fn database_edge_property_removal_on_a_base_relation() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    assert!(
        db.remove_edge_property(base_rel_id(&db), "weight"),
        "remove_edge_property"
    );
    assert_eq!(edge_prop(&db, "weight"), None);
    assert_base_rel(&db, &[("weight", None)], "after removal");
}

/// What one relation between two base entities costs. Before D10 it copied
/// both endpoints whole (2 × the 8 KiB embedding); slice 1 left two
/// labels-only endpoint rows (~206 B each); slice 3 drops them, so the
/// overlay holds only the edge.
#[test]
fn relation_between_base_entities_does_not_copy_their_embeddings() {
    const N: usize = 400;
    const WIDE: usize = 2048;
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("rel-cost.grafeo.d");
    publish(&root, N, WIDE);
    let db = open(&root);
    let layered = db.layered_store().expect("layered").clone();
    let overlay_before = layered.overlay_memory_bytes();
    for i in 0..N / 2 {
        db.execute_cypher(&format!(
            "MATCH (a:MemoryEntity {{name: 'e{}'}}), (b:MemoryEntity {{name: 'e{}'}}) \
             CREATE (a)-[:MemoryEntityRelation {{rel_type: 'r'}}]->(b)",
            2 * i,
            2 * i + 1
        ))
        .expect("relate");
    }
    let per = (layered.overlay_memory_bytes() - overlay_before) / (N / 2);
    eprintln!(
        "D10 cost per relation between two base entities with a {WIDE}-dim embedding: overlay {per} B"
    );
    assert!(
        per < 2048,
        "overlay grows {per} B per relation; endpoints are being copied"
    );
}

// ── r2: review follow-ups ─────────────────────────────────────────────

/// A removed base property is never a property-lookup match, indexed or
/// not: not for `Null`, not for an open range.
#[test]
fn removed_property_is_not_a_lookup_match() {
    for indexed in [false, true] {
        let (_dir, root) = fresh_root(3, DIMS);
        let db = open(&root);
        if indexed {
            db.create_property_index("updated_at_ms");
        }
        let stage = if indexed { "indexed" } else { "scan" };
        db.execute_cypher("MATCH (m:MemoryEntity {name: 'e1'}) REMOVE m.updated_at_ms")
            .expect("REMOVE");
        let e1 = id_of(&db, "e1");
        let store = db.graph_store();
        assert!(
            !store
                .find_nodes_by_property("updated_at_ms", &Value::Null)
                .contains(&e1),
            "[{stage}] Null equality matched a removed property"
        );
        assert!(
            !store
                .find_nodes_in_range("updated_at_ms", None, None, true, true)
                .contains(&e1),
            "[{stage}] open range matched a removed property"
        );
        let mut in_range = store.find_nodes_in_range("updated_at_ms", None, None, true, true);
        in_range.sort();
        let mut want = vec![id_of(&db, "e0"), id_of(&db, "e2")];
        want.sort();
        assert_eq!(in_range, want, "[{stage}] open range: the other two");
        assert_entity(&db, e1, 1, &[("updated_at_ms", None)], stage);
    }
}

/// Rolling back a second write restores an already-committed, non-empty
/// diff (not the bare base row).
#[test]
fn rollback_to_a_committed_diff() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    db.execute_cypher(
        "MATCH (m:MemoryEntity {name: 'e0'}) SET m.observations_json = 'one' REMOVE m.embedding_provider",
    )
    .expect("committed diff");
    let committed = [
        ("observations_json", Some(Value::from("one"))),
        ("embedding_provider", None),
    ];
    let e0 = id_of(&db, "e0");
    assert_entity(&db, e0, 0, &committed, "committed");
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher(
            "MATCH (m:MemoryEntity {name: 'e0'}) SET m.observations_json = 'two', \
             m.embedding_provider = 'other' REMOVE m.account_id",
        )
        .expect("second write");
    session.rollback().expect("rollback");
    assert_entity(&db, e0, 0, &committed, "rollback");
    drop(session);
    db.close().expect("close");
    drop(db);
    let db = open(&root);
    assert_entity(&db, id_of(&db, "e0"), 0, &committed, "reopen");
}

/// SET and REMOVE between the freeze and the install: the install refuses
/// (writes during the window), and a reopen replays them over the
/// published base.
#[test]
fn writes_after_the_freeze_are_refused_at_install_and_replayed() {
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    db.execute_cypher("MATCH (m:MemoryEntity {name: 'e0'}) SET m.observations_json = 'pre'")
        .expect("pre-freeze write");
    let handle = db.freeze_epoch_for_handoff(&root).expect("freeze");
    db.execute_cypher(
        "MATCH (m:MemoryEntity {name: 'e0'}) SET m.observations_json = 'post' REMOVE m.embedding_provider",
    )
    .expect("post-freeze write on a frozen diff row");
    db.execute_cypher("MATCH (m:MemoryEntity {name: 'e1'}) SET m.observations_json = 'post'")
        .expect("post-freeze write on a clean base row");
    let want_e0 = [
        ("observations_json", Some(Value::from("post"))),
        ("embedding_provider", None),
    ];
    let want_e1 = [("observations_json", Some(Value::from("post")))];
    let report = db
        .complete_epoch_handoff(handle, generation_build_request(&root, "g2"))
        .expect("complete handoff");
    let err = db
        .publish_and_install_handoff(report)
        .expect_err("install must refuse writes made after the freeze");
    assert!(
        err.to_string()
            .contains("writes occurred during the handoff window"),
        "{err}"
    );
    assert_entity(&db, id_of(&db, "e0"), 0, &want_e0, "refused install");
    assert_entity(&db, id_of(&db, "e1"), 1, &want_e1, "refused install");
    db.close().expect("close");
    drop(db);
    let db = open(&root);
    assert_entity(&db, id_of(&db, "e0"), 0, &want_e0, "reopen");
    assert_entity(&db, id_of(&db, "e1"), 1, &want_e1, "reopen");
    assert_entity(&db, id_of(&db, "e2"), 2, &[], "reopen, untouched");
}

/// The freeze holds diff rows only: capturing N touched base entities with a
/// 2048-dim embedding must not hold N embeddings (R1). The build merges each
/// row with its base row as it consumes it, and the result is whole.
#[test]
fn freeze_captures_diffs_not_inherited_embeddings() {
    const N: usize = 400;
    const WIDE: usize = 2048;
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("freeze.grafeo.d");
    publish(&root, N, WIDE);
    let db = open(&root);
    for i in 0..N {
        db.execute_cypher(&format!(
            "MATCH (m:MemoryEntity {{name: 'e{i}'}}) SET m.observations_json = 'x{i}'"
        ))
        .expect("touch");
    }
    let handle = db.freeze_epoch_for_handoff(&root).expect("freeze");
    assert_eq!(
        handle.frozen_nodes.len(),
        N,
        "the freeze captured one diff row per touched entity"
    );
    let captured: usize = handle
        .frozen_nodes
        .iter()
        .map(|n| {
            n.properties
                .iter()
                .map(|(k, v)| {
                    k.as_str().len()
                        + match v {
                            Value::Vector(x) => x.len() * 4,
                            Value::String(x) => x.len(),
                            _ => 8,
                        }
                })
                .sum::<usize>()
        })
        .sum();
    eprintln!(
        "D10 freeze capture: {} frozen nodes, {captured} B of property payload ({} B per node; a {WIDE}-dim embedding is {} B)",
        handle.frozen_nodes.len(),
        captured / N,
        WIDE * 4
    );
    assert!(
        captured < N * 1024,
        "the freeze holds {captured} B for {N} small diffs: inherited embeddings are being captured"
    );
    let report = db
        .complete_epoch_handoff(handle, generation_build_request(&root, "g2"))
        .expect("complete handoff");
    db.publish_and_install_handoff(report).expect("install");
    db.close().expect("close");
    drop(db);
    // Every entity of the new base is whole: its written value plus every
    // inherited property, the embedding included.
    let db = open(&root);
    assert_eq!(
        count(&db, "MATCH (m:MemoryEntity) RETURN count(m)"),
        N as i64,
        "entity count"
    );
    for i in 0..N {
        let mut want = published_entity_of(i, WIDE);
        want.insert("observations_json".into(), Value::from(format!("x{i}")));
        let node = db.get_node(id_of(&db, &format!("e{i}"))).expect("node");
        let labels: Vec<String> = node.labels.iter().map(ToString::to_string).collect();
        assert_eq!(labels, vec!["MemoryEntity".to_string()], "e{i} labels");
        assert_eq!(props_map(&node.properties), want, "e{i} after the build");
    }
}

/// A tier drain needs an empty base: the final tier-chain build carries no
/// original base rows, and an overlay row of a base node is a diff. It is
/// refused before any state changes.
#[test]
fn tier_drain_refuses_a_non_empty_base() {
    let (dir, root) = fresh_root(3, DIMS);
    let mut db = open(&root);
    db.execute_cypher("MATCH (m:MemoryEntity {name: 'e0'}) SET m.observations_json = 'd'")
        .expect("write");
    let err = db
        .drain_overlay_to_tier(&dir.path().join("tiers"), "refused")
        .expect_err("drain over a non-empty base");
    assert!(err.to_string().contains("requires an empty base"), "{err}");
    assert!(
        !dir.path().join("tiers").exists()
            || std::fs::read_dir(dir.path().join("tiers")).map_or(true, |mut d| d.next().is_none()),
        "no tier written"
    );
    assert_entity(
        &db,
        id_of(&db, "e0"),
        0,
        &[("observations_json", Some(Value::from("d")))],
        "after refusal",
    );
}

/// Exact text-search result: the names of every hit, sorted.
#[cfg(feature = "text-index")]
fn text_hits(db: &GrafeoDB, query: &str) -> Vec<String> {
    let mut names: Vec<String> = db
        .text_search("MemoryEntity", "observations_json", query, 10)
        .expect("text search")
        .into_iter()
        .map(|(id, _)| match prop(db, id, "name") {
            Some(Value::String(name)) => name.to_string(),
            other => panic!("hit {id:?} has name {other:?}"),
        })
        .collect();
    names.sort();
    names
}

/// The text index serves exactly `inherited` for the base text and
/// `written` for the replacement text.
#[cfg(feature = "text-index")]
fn assert_text(db: &GrafeoDB, inherited: &[&str], written: &[&str], stage: &str) {
    assert_eq!(text_hits(db, "o0"), inherited, "[{stage}] base text");
    assert_eq!(
        text_hits(db, "replacementterm"),
        written,
        "[{stage}] replacement text"
    );
}

/// A rolled-back SET of an inherited text property gives the node its base
/// document back. The diff row has no old value for it, so the overlay undo
/// only drops the replacement document; the rollback rebuilds the document
/// from the merged row. Full rollback, savepoint rollback, and a rollback
/// onto an already-committed non-empty diff.
#[cfg(feature = "text-index")]
#[test]
fn rollback_of_an_inherited_text_property_restores_its_document() {
    const ALL: [&str; 3] = ["e0", "e1", "e2"];
    const SET_E0: &str =
        "MATCH (m:MemoryEntity {name: 'e0'}) SET m.observations_json = 'replacementterm'";
    let (_dir, root) = fresh_root(3, DIMS);
    let db = open(&root);
    db.create_text_index("MemoryEntity", "observations_json")
        .expect("text index");
    assert_text(&db, &ALL, &[], "indexed");
    let e0 = id_of(&db, "e0");
    let mut session = db.session();

    // Full rollback of a SET on a clean base node.
    session.begin_transaction().expect("begin");
    session.execute_cypher(SET_E0).expect("SET");
    assert_text(&db, &["e1", "e2"], &["e0"], "inside the transaction");
    session.rollback().expect("rollback");
    assert_text(&db, &ALL, &[], "rollback");
    assert_entity(&db, e0, 0, &[], "rollback");

    // Savepoint rollback, then the rest of the transaction commits.
    session.begin_transaction().expect("begin");
    session
        .execute_cypher("MATCH (m:MemoryEntity {name: 'e1'}) SET m.updated_at_ms = 7")
        .expect("write before the savepoint");
    session.savepoint("s").expect("savepoint");
    session.execute_cypher(SET_E0).expect("SET");
    session
        .rollback_to_savepoint("s")
        .expect("rollback to savepoint");
    assert_text(&db, &ALL, &[], "savepoint rollback");
    session.commit().expect("commit");
    assert_text(&db, &ALL, &[], "commit after savepoint rollback");
    assert_entity(&db, e0, 0, &[], "savepoint rollback");

    // A committed non-text diff on e0, then a rolled-back text SET on it.
    db.execute_cypher("MATCH (m:MemoryEntity {name: 'e0'}) SET m.updated_at_ms = 5")
        .expect("committed diff");
    let committed = [("updated_at_ms", Some(Value::Int64(5)))];
    session.begin_transaction().expect("begin");
    session.execute_cypher(SET_E0).expect("SET");
    session.rollback().expect("rollback");
    assert_text(&db, &ALL, &[], "rollback onto a committed diff");
    assert_entity(&db, e0, 0, &committed, "rollback onto a committed diff");

    // A committed text SET survives a rolled-back REMOVE of it.
    db.execute_cypher(SET_E0).expect("committed text SET");
    session.begin_transaction().expect("begin");
    session
        .execute_cypher("MATCH (m:MemoryEntity {name: 'e0'}) REMOVE m.observations_json")
        .expect("REMOVE");
    assert_text(&db, &["e1", "e2"], &[], "inside the REMOVE transaction");
    session.rollback().expect("rollback");
    assert_text(&db, &["e1", "e2"], &["e0"], "rollback of the REMOVE");
}

/// Batch creates on a generation root whose base nodes have diff rows. The
/// batch HNSW inserts (`GrafeoDB::batch_create_nodes`,
/// `batch_create_nodes_with_props`) read neighbour vectors through the merged
/// view (fork #45), so a base node's inherited embedding is readable whether
/// or not it has a diff row. Over a clean and over a touched base, every
/// node, base or batch-created, is the nearest neighbour of its own vector
/// and reachable from every query. (Before #45 the inserts read the overlay
/// alone and batch-created nodes were never found.) The batch vectors are
/// orthogonal, so no two nodes tie for nearest under cosine.
#[cfg(feature = "vector-index")]
#[test]
fn batch_creates_over_base_diff_rows_are_searchable() {
    const BASE: usize = 6;
    let as_f32 = |v: Value| match v {
        Value::Vector(v) => v.to_vec(),
        other => panic!("{other:?}"),
    };
    let axis = |k: usize| -> Vec<f32> { (0..DIMS).map(|d| f32::from(u8::from(d == k))).collect() };
    for touched in [false, true] {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("batch.grafeo.d");
        publish_with(&root, BASE, DIMS, true);
        let db = open(&root);
        if touched {
            db.execute_cypher("MATCH (m:MemoryEntity) SET m.observations_json = 'touched'")
                .expect("touch every base node");
        }
        let mut nodes: Vec<(NodeId, Vec<f32>)> = (0..BASE)
            .map(|i| (id_of(&db, &format!("e{i}")), as_f32(vector(i, DIMS))))
            .collect();
        let plain: Vec<Vec<f32>> = (0..3).map(axis).collect();
        let ids = db.batch_create_nodes("MemoryEntity", "embedding", plain.clone());
        assert_eq!(ids.len(), 3, "batch_create_nodes");
        nodes.extend(ids.into_iter().zip(plain));
        let with_props: Vec<Vec<f32>> = (3..6).map(axis).collect();
        let ids = db.batch_create_nodes_with_props(
            "MemoryEntity",
            with_props
                .iter()
                .map(|v| {
                    [(
                        PropertyKey::new("embedding"),
                        Value::Vector(v.clone().into()),
                    )]
                    .into_iter()
                    .collect()
                })
                .collect(),
        );
        assert_eq!(ids.len(), 3, "batch_create_nodes_with_props");
        nodes.extend(ids.into_iter().zip(with_props));
        let mut all: Vec<NodeId> = nodes.iter().map(|(id, _)| *id).collect();
        all.sort();
        for (id, v) in &nodes {
            let hits = db
                .vector_search("MemoryEntity", "embedding", v, nodes.len(), Some(64), None)
                .expect("vector search");
            assert_eq!(
                hits.first().map(|(hit, _)| *hit),
                Some(*id),
                "touched={touched}: {id:?} is the nearest neighbour of its own vector: {hits:?}"
            );
            let mut reachable: Vec<NodeId> = hits.iter().map(|(hit, _)| *hit).collect();
            reachable.sort();
            assert_eq!(
                reachable, all,
                "touched={touched}: every node is reachable from {id:?}'s query"
            );
        }
    }
}
