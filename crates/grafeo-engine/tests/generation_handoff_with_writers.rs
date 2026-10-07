//! DESIGN G2: an epoch handoff with writers running.
//!
//! Writes keep landing between the freeze and the install (and between
//! retire and install). The install's repair rule makes N+1 win over the new
//! base, which holds the frozen epoch-N values:
//!
//! - a property removed after the freeze, or one only the frozen copy had,
//!   stays removed (a `Null` tombstone over the new base);
//! - an entity deleted after the freeze stays deleted (a base tombstone);
//! - every other post-freeze value wins over the frozen one.
//!
//! Each test checks the live view after the install and again after a
//! reopen (WAL replay over the published generation).

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};
use grafeo_common::utils::error::Error;
use grafeo_core::graph::traits::GraphStore;
use grafeo_engine::{EpochHandoffReport, GrafeoDB, generation_build_request};
use tempfile::TempDir;

type Props = BTreeMap<String, Value>;

/// Expected state: `None` = deleted.
#[derive(Default, Clone)]
struct Model {
    nodes: BTreeMap<NodeId, Option<Props>>,
    edges: BTreeMap<EdgeId, Option<Props>>,
    keys: BTreeSet<String>,
}

impl Model {
    fn node(&mut self, id: NodeId, props: &[(&str, Value)]) {
        let map = props
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        self.keys
            .extend(props.iter().map(|(k, _)| (*k).to_string()));
        self.nodes.insert(id, Some(map));
    }
    fn edge(&mut self, id: EdgeId, props: &[(&str, Value)]) {
        let map = props
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        self.keys
            .extend(props.iter().map(|(k, _)| (*k).to_string()));
        self.edges.insert(id, Some(map));
    }
    fn set_node(&mut self, id: NodeId, key: &str, value: Value) {
        self.keys.insert(key.to_string());
        self.nodes
            .get_mut(&id)
            .and_then(Option::as_mut)
            .expect("live node")
            .insert(key.to_string(), value);
    }
    fn remove_node_key(&mut self, id: NodeId, key: &str) {
        self.nodes
            .get_mut(&id)
            .and_then(Option::as_mut)
            .expect("live node")
            .remove(key);
    }
    fn set_edge(&mut self, id: EdgeId, key: &str, value: Value) {
        self.keys.insert(key.to_string());
        self.edges
            .get_mut(&id)
            .and_then(Option::as_mut)
            .expect("live edge")
            .insert(key.to_string(), value);
    }
    fn remove_edge_key(&mut self, id: EdgeId, key: &str) {
        self.edges
            .get_mut(&id)
            .and_then(Option::as_mut)
            .expect("live edge")
            .remove(key);
    }
}

fn props_of(map: &grafeo_common::types::PropertyMap) -> Props {
    map.iter()
        .map(|(k, v)| (k.as_str().to_string(), v.clone()))
        .collect()
}

/// Every read path agrees with the model: whole rows, per-key reads (removed
/// keys absent), and the live node/edge sets.
fn check(db: &GrafeoDB, model: &Model, stage: &str) {
    let store = db.graph_store();
    for (id, want) in &model.nodes {
        let got = store.get_node(*id).map(|n| props_of(&n.properties));
        assert_eq!(got.as_ref(), want.as_ref(), "[{stage}] node {id:?}");
        for key in &model.keys {
            let got = store.get_node_property(*id, &PropertyKey::new(key.as_str()));
            let want = want.as_ref().and_then(|w| w.get(key));
            assert_eq!(got.as_ref(), want, "[{stage}] node {id:?}.{key}");
        }
    }
    for (id, want) in &model.edges {
        let got = store.get_edge(*id).map(|e| props_of(&e.properties));
        assert_eq!(got.as_ref(), want.as_ref(), "[{stage}] edge {id:?}");
        for key in &model.keys {
            let got = store.get_edge_property(*id, &PropertyKey::new(key.as_str()));
            let want = want.as_ref().and_then(|w| w.get(key));
            assert_eq!(got.as_ref(), want, "[{stage}] edge {id:?}.{key}");
        }
    }
    let live_nodes: BTreeSet<NodeId> = model
        .nodes
        .iter()
        .filter(|(_, p)| p.is_some())
        .map(|(id, _)| *id)
        .collect();
    let seen: BTreeSet<NodeId> = store.node_ids().into_iter().collect();
    assert_eq!(seen, live_nodes, "[{stage}] live node ids");
    let live_edges = model.edges.values().filter(|p| p.is_some()).count();
    let r = db
        .execute_cypher("MATCH ()-[r]->() RETURN count(r)")
        .expect("edge count");
    assert_eq!(
        r.rows()[0][0],
        Value::Int64(live_edges as i64),
        "[{stage}] live edge count"
    );
}

/// A root whose published base holds `B0..B4` (`B4` isolated) and the edge
/// `B0 -> B1`.
fn seeded_root() -> (TempDir, PathBuf, Model, Vec<NodeId>, EdgeId) {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("g2.grafeo.d");
    std::fs::create_dir_all(&root).expect("create root");
    let source = GrafeoDB::new_in_memory();
    let mut model = Model::default();
    let mut base = Vec::new();
    for i in 0..5_i64 {
        let props = [
            ("name", Value::from(format!("B{i}"))),
            ("k", Value::Int64(i)),
            ("tag", Value::from("base")),
        ];
        let id = source
            .create_node_with_props(&["B"], props.clone())
            .expect("base node");
        model.node(id, &props);
        base.push(id);
    }
    let edge_props = [("w", Value::Int64(1)), ("note", Value::from("base"))];
    let base_edge = source.create_edge_with_props(base[0], base[1], "R", edge_props.clone());
    model.edge(base_edge, &edge_props);
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .expect("publish base");
    (dir, root, model, base, base_edge)
}

fn open(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, false).expect("open generation root")
}

fn retry<T>(what: &str, mut step: impl FnMut() -> grafeo_common::utils::error::Result<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match step() {
            Ok(v) => return v,
            Err(Error::AdmissionRetryable(_)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(e) => panic!("{what}: {e}"),
        }
    }
}

fn freeze(db: &GrafeoDB, root: &Path) -> grafeo_engine::FrozenEpochHandle {
    retry("freeze", || db.freeze_epoch_for_handoff(root))
}

fn install(db: &GrafeoDB, report: EpochHandoffReport) {
    let mut report = Some(report);
    retry("install", || {
        match db.publish_and_install_handoff(report.clone().expect("report")) {
            Ok(r) => {
                report = None;
                Ok(r)
            }
            Err(e) => Err(e),
        }
    });
}

fn handoff(db: &GrafeoDB, root: &Path, generation: &str) {
    let handle = freeze(db, root);
    let report = db
        .complete_epoch_handoff(handle, generation_build_request(root, generation))
        .expect("complete handoff");
    install(db, report);
}

/// Every class of post-freeze write, made between freeze and retire and
/// between retire and install, survives the install and a reopen.
#[test]
fn post_freeze_writes_of_every_class_survive_install_and_reopen() {
    every_class_scenario(false);
}

/// [`post_freeze_writes_of_every_class_survive_install_and_reopen`], then a
/// handoff after the install and another after the reopen.
#[test]
#[ignore = "needs fork #47 r2: a handoff over a mapped base turns a node \
            string property only some rows have into a dictionary string \
            for the rest (B1..B4 read `extra = \"B\"`)"]
fn post_freeze_writes_survive_later_handoffs() {
    every_class_scenario(true);
}

fn every_class_scenario(later_handoffs: bool) {
    let (_dir, root, mut model, base, base_edge) = seeded_root();
    let db = open(&root);

    // ── Epoch N: writes the freeze captures ──────────────────────────────
    let mk = |db: &GrafeoDB, model: &mut Model, name: &str, extra: &[(&str, Value)]| {
        let mut props = vec![("name", Value::from(name))];
        props.extend(extra.iter().cloned());
        let id = db
            .create_node_with_props(&["N"], props.clone())
            .expect("create");
        model.node(id, &props);
        id
    };
    let n1 = mk(
        &db,
        &mut model,
        "n1",
        &[("a", Value::Int64(1)), ("b", Value::Int64(2))],
    );
    let n2 = mk(&db, &mut model, "n2", &[("a", Value::Int64(1))]);
    let n3 = mk(&db, &mut model, "n3", &[]);
    let n4 = mk(&db, &mut model, "n4", &[]);
    let n6 = mk(&db, &mut model, "n6", &[("a", Value::Int64(6))]);
    db.set_node_property(base[0], "extra", Value::from("pre"))
        .expect("set");
    model.set_node(base[0], "extra", Value::from("pre"));
    db.set_node_property(base[1], "k", Value::Int64(100))
        .expect("set");
    model.set_node(base[1], "k", Value::Int64(100));
    let e1_props = [("w", Value::Int64(1)), ("x", Value::from("pre"))];
    let e1 = db.create_edge_with_props(n1, n2, "R", e1_props.clone());
    model.edge(e1, &e1_props);
    let e2_props = [("w", Value::Int64(2))];
    let e2 = db.create_edge_with_props(n2, n3, "R", e2_props.clone());
    model.edge(e2, &e2_props);
    db.set_edge_property(base_edge, "y", Value::from("pre"));
    model.set_edge(base_edge, "y", Value::from("pre"));

    let handle = freeze(&db, &root);

    // ── Epoch N+1, before retire ─────────────────────────────────────────
    // An N-created row loses a key the frozen copy has.
    assert!(db.remove_node_property(n1, "b"));
    model.remove_node_key(n1, "b");
    // An N-created node is deleted.
    assert!(db.delete_node(n4).expect("delete n4"));
    model.nodes.insert(n4, None);
    // An N-created node gains a key.
    db.set_node_property(n3, "a", Value::Int64(5)).expect("set");
    model.set_node(n3, "a", Value::Int64(5));
    // A base row loses a key only epoch N added.
    assert!(db.remove_node_property(base[0], "extra"));
    model.remove_node_key(base[0], "extra");
    // A frozen base diff row and a clean base row get new values.
    db.set_node_property(base[1], "k", Value::Int64(200))
        .expect("set");
    model.set_node(base[1], "k", Value::Int64(200));
    db.set_node_property(base[2], "k", Value::Int64(300))
        .expect("set");
    model.set_node(base[2], "k", Value::Int64(300));
    // A base node is deleted.
    assert!(db.delete_node(base[4]).expect("delete B4"));
    model.nodes.insert(base[4], None);
    // Edges: an N-created edge loses a key, another is deleted, the base
    // edge loses an N key and gets a new value.
    assert!(db.remove_edge_property(e1, "x"));
    model.remove_edge_key(e1, "x");
    assert!(db.delete_edge(e2));
    model.edges.insert(e2, None);
    assert!(db.remove_edge_property(base_edge, "y"));
    model.remove_edge_key(base_edge, "y");
    db.set_edge_property(base_edge, "w", Value::Int64(9));
    model.set_edge(base_edge, "w", Value::Int64(9));

    let report = db
        .complete_epoch_handoff(handle, generation_build_request(&root, "g2"))
        .expect("complete handoff");

    // ── Between retire and install ───────────────────────────────────────
    let n5 = mk(&db, &mut model, "n5", &[]);
    assert!(db.remove_node_property(n3, "name"));
    model.remove_node_key(n3, "name");
    db.set_node_property(base[1], "k", Value::Int64(201))
        .expect("set");
    model.set_node(base[1], "k", Value::Int64(201));
    check(&db, &model, "before install");

    install(&db, report);
    check(&db, &model, "after install");

    // ── After the install: writes to absorbed rows ───────────────────────
    db.set_node_property(n6, "a", Value::Int64(7)).expect("set");
    model.set_node(n6, "a", Value::Int64(7));
    db.set_node_property(n5, "a", Value::Int64(8)).expect("set");
    model.set_node(n5, "a", Value::Int64(8));
    db.set_node_property(base[2], "k", Value::Int64(301))
        .expect("set");
    model.set_node(base[2], "k", Value::Int64(301));
    check(&db, &model, "writes after install");
    if later_handoffs {
        handoff(&db, &root, "g3");
        check(&db, &model, "second handoff");
    }

    db.close().expect("close");
    drop(db);
    let db = open(&root);
    check(&db, &model, "reopen");
    if later_handoffs {
        handoff(&db, &root, "g4");
        check(&db, &model, "handoff after reopen");
    }
}

/// An open write transaction defers the freeze and the install (retryable);
/// once it ends they proceed, and a rolled-back write is not captured.
#[test]
fn open_write_transactions_defer_freeze_and_install() {
    let (_dir, root, mut model, base, _) = seeded_root();
    let db = open(&root);

    let mut session = db.session();
    session.begin_transaction().expect("begin");
    session
        .execute_cypher("MATCH (b:B {name: 'B3'}) DETACH DELETE b")
        .expect("delete in txn");
    match db.freeze_epoch_for_handoff(&root) {
        Err(Error::AdmissionRetryable(_)) => {}
        other => panic!("freeze with an open writer: {:?}", other.map(|_| ())),
    }
    session.rollback().expect("rollback");
    let handle = freeze(&db, &root);
    let report = db
        .complete_epoch_handoff(handle, generation_build_request(&root, "g2"))
        .expect("complete handoff");

    session.begin_transaction().expect("begin");
    session
        .execute_cypher("MATCH (b:B {name: 'B2'}) SET b.k = 99")
        .expect("set in txn");
    match db.publish_and_install_handoff(report.clone()) {
        Err(Error::AdmissionRetryable(_)) => {}
        other => panic!("install with an open writer: {:?}", other.map(|_| ())),
    }
    session.commit().expect("commit");
    model.set_node(base[2], "k", Value::Int64(99));
    install(&db, report);
    check(&db, &model, "after install");
    drop(session);
    db.close().expect("close");
    drop(db);
    check(&open(&root), &model, "reopen");
}

/// Writers on two threads (direct writes and session transactions, with
/// rollbacks) run through several handoffs; afterwards the live view and a
/// reopen match exactly what they acknowledged.
#[test]
fn writer_threads_run_through_repeated_handoffs() {
    let (_dir, root, model, base, _) = seeded_root();
    let db = Arc::new(open(&root));
    let model = Arc::new(Mutex::new(model));
    let stop = Arc::new(AtomicBool::new(false));

    let direct = {
        let (db, model, stop, base) = (
            Arc::clone(&db),
            Arc::clone(&model),
            Arc::clone(&stop),
            base.clone(),
        );
        std::thread::spawn(move || {
            let mut mine: Vec<NodeId> = Vec::new();
            let mut i: i64 = 0;
            while !stop.load(Ordering::Acquire) {
                i += 1;
                let props = [("seq", Value::Int64(i)), ("v", Value::Int64(0))];
                let id = db
                    .create_node_with_props(&["W"], props.clone())
                    .expect("create");
                model.lock().unwrap().node(id, &props);
                mine.push(id);
                // Touch an older node: by now often frozen or absorbed.
                let older = mine[(i as usize * 7) % mine.len()];
                if model.lock().unwrap().nodes[&older].is_some() {
                    match i % 5 {
                        0 => {
                            assert!(db.delete_node(older).expect("delete"));
                            model.lock().unwrap().nodes.insert(older, None);
                        }
                        1 => {
                            if db.remove_node_property(older, "v") {
                                model.lock().unwrap().remove_node_key(older, "v");
                            }
                        }
                        _ => {
                            db.set_node_property(older, "v", Value::Int64(i))
                                .expect("set");
                            model.lock().unwrap().set_node(older, "v", Value::Int64(i));
                        }
                    }
                }
                let b = base[(i as usize) % 4];
                db.set_node_property(b, "k", Value::Int64(i))
                    .expect("set base");
                model.lock().unwrap().set_node(b, "k", Value::Int64(i));
            }
            i
        })
    };
    let sessions = {
        let (db, model, stop) = (Arc::clone(&db), Arc::clone(&model), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut mine: Vec<NodeId> = Vec::new();
            let mut i: i64 = 0;
            let mut session = db.session();
            while !stop.load(Ordering::Acquire) {
                i += 1;
                session.begin_transaction().expect("begin");
                let r = session
                    .execute_cypher(&format!("CREATE (t:T {{seq: {i}, v: 0}}) RETURN id(t)"))
                    .expect("create");
                let id = match &r.rows()[0][0] {
                    Value::Int64(v) => NodeId::new(*v as u64),
                    other => panic!("id: {other:?}"),
                };
                let older = mine.get((i as usize * 3) % mine.len().max(1)).copied();
                if let Some(older) = older {
                    session
                        .execute_cypher(&format!(
                            "MATCH (t:T) WHERE id(t) = {} SET t.v = {i}",
                            older.as_u64()
                        ))
                        .expect("set");
                }
                if i % 7 == 0 {
                    session.rollback().expect("rollback");
                    continue;
                }
                session.commit().expect("commit");
                let mut m = model.lock().unwrap();
                m.node(id, &[("seq", Value::Int64(i)), ("v", Value::Int64(0))]);
                if let Some(older) = older {
                    m.set_node(older, "v", Value::Int64(i));
                }
                mine.push(id);
            }
            i
        })
    };

    for cycle in 0..4 {
        std::thread::sleep(Duration::from_millis(50));
        handoff(&db, &root, &format!("g{}", cycle + 2));
    }
    stop.store(true, Ordering::Release);
    let direct_ops = direct.join().expect("direct writer");
    let session_ops = sessions.join().expect("session writer");
    assert!(
        direct_ops > 20 && session_ops > 20,
        "writers made progress across the handoffs ({direct_ops}, {session_ops})"
    );

    let model = model.lock().unwrap().clone();
    check(&db, &model, "live");
    let db = Arc::into_inner(db).expect("sole owner");
    db.close().expect("close");
    drop(db);
    let db = open(&root);
    check(&db, &model, "reopen");
}

/// ForceDisk: a spilled vector is held outside its overlay row, so the row
/// lacking it says nothing about its value. A post-freeze write to such a
/// node must not make the install tombstone the vector the new base holds.
#[cfg(all(feature = "vector-index", not(feature = "temporal")))]
#[test]
fn install_keeps_spilled_vectors_of_rows_written_after_the_freeze() {
    use grafeo_common::storage::{SectionType, TierOverride};
    use grafeo_engine::{Config, IndexedVectorRead};

    const DIMS: usize = 4;
    let vector = |seed: u64| -> Vec<f32> {
        (0..DIMS as u64)
            .map(|d| (seed * 10 + d) as f32 + 0.5)
            .collect()
    };
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("fd.grafeo.d");
    let spill = dir.path().join("fd.spill");
    std::fs::create_dir_all(&root).expect("create root");
    let force_disk = || {
        Config::persistent(&root)
            .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk)
            .with_spill_path(&spill)
    };
    {
        let source = GrafeoDB::new_in_memory();
        source
            .create_node_with_props(&["Doc"], [("embedding", Value::Vector(vector(0).into()))])
            .expect("base doc");
        source
            .create_vector_index(
                "Doc",
                "embedding",
                Some(DIMS),
                Some("euclidean"),
                None,
                None,
                None,
            )
            .expect("vector index");
        source
            .build_and_publish_generation(generation_build_request(&root, "g1"))
            .expect("publish base");
    }
    // Overlay docs written, then replayed and spilled by a ForceDisk open.
    let mut docs = Vec::new();
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk()).expect("open");
        for seed in 1..=3 {
            let id = db
                .create_node_with_props(
                    &["Doc"],
                    [
                        ("embedding", Value::Vector(vector(seed).into())),
                        ("name", Value::from(format!("d{seed}"))),
                    ],
                )
                .expect("doc");
            docs.push((id, vector(seed)));
        }
        db.close().expect("close");
    }
    let db = GrafeoDB::open_generation_root_with_config(force_disk()).expect("reopen");
    let spilled = std::fs::read_dir(&spill)
        .map(|rd| rd.filter_map(Result::ok).count())
        .unwrap_or(0);
    assert!(spilled > 0, "overlay vectors spilled");

    let handle = freeze(&db, &root);
    // Not a vector write: no reload, the vectors stay spilled.
    for (id, _) in &docs {
        db.set_node_property(*id, "name", Value::from("renamed"))
            .expect("rename");
    }
    let report = db
        .complete_epoch_handoff(handle, generation_build_request(&root, "g2"))
        .expect("complete handoff");
    install(&db, report);
    let vectors_found = |db: &GrafeoDB, stage: &str| {
        for (id, want) in &docs {
            match db.read_indexed_node_vector("Doc", "embedding", *id) {
                Ok(IndexedVectorRead::Found(got)) => assert_eq!(&got, want, "[{stage}] {id:?}"),
                other => panic!("[{stage}] {id:?} vector: {other:?}"),
            }
        }
    };
    vectors_found(&db, "after install");
    handoff(&db, &root, "g3");
    vectors_found(&db, "after a second handoff");
    db.close().expect("close");
    drop(db);

    let db = open(&root);
    let store = db.graph_store();
    for (id, want) in &docs {
        match store.get_node_property(*id, &PropertyKey::new("embedding")) {
            Some(Value::Vector(got)) => assert_eq!(got.as_ref(), want.as_slice(), "{id:?}"),
            other => panic!("reopen: {id:?} embedding {other:?}"),
        }
        assert_eq!(
            store.get_node_property(*id, &PropertyKey::new("name")),
            Some(Value::from("renamed")),
            "reopen: {id:?} name"
        );
    }
}
