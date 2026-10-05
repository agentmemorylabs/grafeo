//! Vector and text indexes on a reopened writable generation root must serve
//! the writes made since the last publication.
//!
//! A writable generation root restores its vector/text indexes from the
//! published base sections, then replays the post-boundary WAL into a fresh
//! overlay. WAL replay writes the store directly; the index maintenance that
//! the write APIs do (HNSW insert/remove, BM25 postings) does not run there.
//! So without a reconcile step, every write since the last publication drops
//! out of the indexes after any restart, silently: new vectors are not found,
//! updated vectors are found under their old value, and deleted nodes are
//! still returned.
//!
//! Each case checks the live handle first (the write path's own index
//! maintenance), then the same expectations after close + reopen.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher",
    feature = "vector-index",
    feature = "text-index"
))]

use std::path::Path;

use grafeo_common::types::{NodeId, Value};
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::tempdir;

const LABEL: &str = "Doc";
const VEC: &str = "embedding";
const TEXT: &str = "body";
const DIMS: usize = 8;
const BASE_NODES: u64 = 16;

fn seeded_vector(seed: u64) -> Vec<f32> {
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut raw: Vec<f32> = (0..DIMS)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            ((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0
        })
        .collect();
    let norm: f32 = raw.iter().map(|x| x * x).sum::<f32>().sqrt();
    for x in &mut raw {
        *x /= norm;
    }
    raw
}

fn vector(seed: u64) -> Value {
    Value::Vector(seeded_vector(seed).into())
}

/// Publishes `BASE_NODES` `:Doc` nodes with an embedding and a body, plus a
/// vector index and a text index, into `root`. Returns the base node IDs
/// (a generation base preserves source IDs).
fn publish_base(root: &Path) -> Vec<NodeId> {
    std::fs::create_dir_all(root).expect("root dir");
    let source = GrafeoDB::new_in_memory();
    let ids = (0..BASE_NODES)
        .map(|i| {
            source
                .create_node_with_props(
                    &[LABEL],
                    [
                        (VEC, vector(i)),
                        (TEXT, Value::from(format!("base document number{i}"))),
                    ],
                )
                .expect("create base node")
        })
        .collect();
    source
        .create_vector_index(LABEL, VEC, Some(DIMS), Some("cosine"), None, None, None)
        .expect("create vector index");
    source
        .create_text_index(LABEL, TEXT)
        .expect("create text index");
    source
        .build_and_publish_generation(generation_build_request(root, "g1"))
        .expect("publish base generation");
    ids
}

fn open(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, false).expect("open generation root")
}

fn nearest(db: &GrafeoDB, seed: u64, k: usize) -> Vec<NodeId> {
    db.vector_search(LABEL, VEC, &seeded_vector(seed), k, None, None)
        .expect("vector search")
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

fn text_hits(db: &GrafeoDB, query: &str) -> Vec<NodeId> {
    db.text_search(LABEL, TEXT, query, 64)
        .expect("text search")
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

/// Seeds for the vectors written on the root (far from the base seeds).
const NEW_SEED: u64 = 1_000;
const UPDATED_SEED: u64 = 2_000;

struct Writes {
    created: NodeId,
    updated: NodeId,
    deleted: NodeId,
}

/// On the open root: create a node with a new vector, replace a base node's
/// vector, delete another base node.
fn write_vectors(db: &GrafeoDB, base: &[NodeId]) -> Writes {
    let created = db
        .create_node_with_props(&[LABEL], [(VEC, vector(NEW_SEED))])
        .expect("create overlay node");
    let updated = base[3];
    db.set_node_property(updated, VEC, vector(UPDATED_SEED))
        .expect("update base vector");
    let deleted = base[5];
    assert!(
        db.delete_node(deleted).expect("delete"),
        "base node deleted"
    );
    Writes {
        created,
        updated,
        deleted,
    }
}

fn check_vectors(db: &GrafeoDB, w: &Writes, stage: &str, failures: &mut Vec<String>) {
    let all = BASE_NODES as usize + 1;
    let top = nearest(db, NEW_SEED, 1);
    if top != [w.created] {
        failures.push(format!(
            "[{stage}] new vector: nearest is {top:?}, want [{:?}]",
            w.created
        ));
    }
    let top = nearest(db, UPDATED_SEED, 1);
    if top != [w.updated] {
        failures.push(format!(
            "[{stage}] updated vector: nearest is {top:?}, want [{:?}]",
            w.updated
        ));
    }
    // The deleted node is never returned, even when it is the exact match.
    let hits = nearest(db, 5, all);
    if hits.contains(&w.deleted) {
        failures.push(format!(
            "[{stage}] deleted node {:?} returned: {hits:?}",
            w.deleted
        ));
    }
    // The updated node is scored by its new vector: its old vector's exact
    // match (distance 0) must not still be served for it.
    let scored = db
        .vector_search(LABEL, VEC, &seeded_vector(3), all, None, None)
        .expect("vector search");
    if let Some((_, distance)) = scored.iter().find(|(id, _)| *id == w.updated)
        && *distance < 1e-4
    {
        failures.push(format!(
            "[{stage}] updated node {:?} still matches its old vector (distance {distance})",
            w.updated
        ));
    }
}

fn assert_no_failures(failures: &[String]) {
    assert!(
        failures.is_empty(),
        "{} failure(s):\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// Vectors written since the last publication are served by `vector_search`
/// after reopen: a new vector is found, an updated vector replaces the base
/// one, a deleted node is not returned.
#[test]
fn overlay_vectors_are_searchable_after_reopen() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("vec.grafeo.d");
    let base = publish_base(&root);
    let mut failures = Vec::new();

    let writes = {
        let db = open(&root);
        let writes = write_vectors(&db, &base);
        check_vectors(&db, &writes, "live", &mut failures);
        db.close().expect("close");
        writes
    };

    for cycle in 1..=2 {
        let db = open(&root);
        check_vectors(&db, &writes, &format!("reopen {cycle}"), &mut failures);
        db.close().expect("close");
    }
    assert_no_failures(&failures);
}

/// The same after a further write on the reopened handle: replayed overlay
/// vectors and fresh ones are served together.
#[test]
fn overlay_vectors_survive_reopen_then_more_writes() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("vec2.grafeo.d");
    let base = publish_base(&root);
    let mut failures = Vec::new();

    let writes = {
        let db = open(&root);
        let writes = write_vectors(&db, &base);
        db.close().expect("close");
        writes
    };
    let later = {
        let db = open(&root);
        let later = db
            .create_node_with_props(&[LABEL], [(VEC, vector(3_000))])
            .expect("create after reopen");
        db.close().expect("close");
        later
    };

    let db = open(&root);
    check_vectors(&db, &writes, "second reopen", &mut failures);
    let top = nearest(&db, 3_000, 1);
    if top != [later] {
        failures.push(format!(
            "[second reopen] later vector: nearest is {top:?}, want [{later:?}]"
        ));
    }
    assert_no_failures(&failures);
}

/// Text written since the last publication is served by `text_search`, live
/// and after reopen: a new body is found, an updated body is found under its
/// new text only, a deleted node is not returned.
#[test]
fn overlay_text_is_searchable_live_and_after_reopen() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("text.grafeo.d");
    let base = publish_base(&root);
    let mut failures = Vec::new();

    let check = |db: &GrafeoDB, created: NodeId, stage: &str, failures: &mut Vec<String>| {
        let hits = text_hits(db, "zebra");
        if hits != [created] {
            failures.push(format!(
                "[{stage}] new body: hits {hits:?}, want [{created:?}]"
            ));
        }
        let hits = text_hits(db, "giraffe");
        if hits != [base[3]] {
            failures.push(format!(
                "[{stage}] updated body: hits {hits:?}, want [{:?}]",
                base[3]
            ));
        }
        let hits = text_hits(db, "number3");
        if hits.contains(&base[3]) {
            failures.push(format!(
                "[{stage}] updated node {:?} still found by its old body",
                base[3]
            ));
        }
        let hits = text_hits(db, "number5");
        if hits.contains(&base[5]) {
            failures.push(format!("[{stage}] deleted node {:?} returned", base[5]));
        }
    };

    let created = {
        let db = open(&root);
        let created = db
            .create_node_with_props(&[LABEL], [(TEXT, Value::from("a zebra document"))])
            .expect("create overlay node");
        db.set_node_property(base[3], TEXT, Value::from("a giraffe document"))
            .expect("update base body");
        assert!(
            db.delete_node(base[5]).expect("delete"),
            "base node deleted"
        );
        check(&db, created, "live", &mut failures);
        db.close().expect("close");
        created
    };

    let db = open(&root);
    check(&db, created, "reopen", &mut failures);
    assert_no_failures(&failures);
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create dir copy");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("dir entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).expect("copy file");
        }
    }
}

/// The single-file format replays its sidecar WAL on open after a crash.
/// Vectors written after the last checkpoint must be searchable there too.
/// The crash is simulated by copying the `.grafeo` file and its synced
/// sidecar WAL while the database is still open, then opening the copy.
#[test]
fn single_file_sidecar_replay_keeps_vectors_searchable() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("single.grafeo");
    let mut failures = Vec::new();

    let db = GrafeoDB::with_config(grafeo_engine::Config::persistent(&path)).expect("create db");
    let base: Vec<NodeId> = (0..BASE_NODES)
        .map(|i| {
            db.create_node_with_props(&[LABEL], [(VEC, vector(i))])
                .expect("create node")
        })
        .collect();
    db.create_vector_index(LABEL, VEC, Some(DIMS), Some("cosine"), None, None, None)
        .expect("create vector index");
    db.wal_checkpoint().expect("checkpoint to file");

    let writes = write_vectors(&db, &base);
    check_vectors(&db, &writes, "live", &mut failures);
    db.wal().expect("sidecar WAL").sync().expect("sync WAL");

    let crashed = dir.path().join("crashed.grafeo");
    std::fs::copy(&path, &crashed).expect("copy .grafeo");
    copy_dir(
        &dir.path().join("single.grafeo.wal"),
        &dir.path().join("crashed.grafeo.wal"),
    );
    drop(db);

    let db = GrafeoDB::with_config(grafeo_engine::Config::persistent(&crashed))
        .expect("open crashed copy");
    check_vectors(&db, &writes, "after crash replay", &mut failures);
    assert_no_failures(&failures);
}
