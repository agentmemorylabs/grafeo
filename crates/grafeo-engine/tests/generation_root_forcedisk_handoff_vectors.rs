//! AMH #167: an epoch handoff on a generation root opened with the
//! ForceDisk vector tier must carry the overlay's spilled vectors into the
//! new base, read them per node during the build (not hold them in the
//! freeze), and fail before publication when a spilled vector can't be read.
//!
//! A generation-root open replays the post-boundary WAL into the overlay and
//! then applies `TierOverride::ForceDisk`, which drains the overlay's
//! vector-indexed property columns into mmap spill files. Before the fix the
//! freeze captured overlay nodes from the property store only, so every
//! overlay node (copy-ups of base nodes and new nodes) reached the new base
//! without its vector: permanent loss for every later reader.
//!
//! ```text
//! cargo test -p grafeo-engine --features generation,generation-streaming,compact-store,lpg,mmap,wal,vector-index \
//!   --test generation_root_forcedisk_handoff_vectors
//! ```

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "compact-store",
    feature = "lpg",
    feature = "mmap",
    feature = "wal",
    feature = "vector-index",
    not(feature = "temporal")
))]

use std::path::{Path, PathBuf};

use grafeo_common::storage::{SectionType, TierOverride};
use grafeo_common::types::{NodeId, PropertyKey, Value};
use grafeo_core::graph::traits::GraphStore;
use grafeo_engine::{Config, GrafeoDB, IndexedVectorRead, generation_build_request};
use tempfile::tempdir;

/// Two indexed vector properties of different widths on `:Doc`.
const PROPS: [(&str, usize); 2] = [("embedding", 4), ("summary", 3)];

/// Distinct points (euclidean): a node's own vector is its unique nearest
/// neighbour.
fn vector(prop: usize, seed: u64) -> Vec<f32> {
    let dims = PROPS[prop].1 as u64;
    (0..dims)
        .map(|d| (seed * 10 + d) as f32 + 0.5 + prop as f32 * 1000.0)
        .collect()
}

/// AMH's production open config for a writable graph: ForceDisk vector tier
/// plus a spill directory (`am_graph::grafeo::runtime_core::open`).
fn force_disk(root: &Path, spill: &Path) -> Config {
    Config::persistent(root)
        .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk)
        .with_spill_path(spill)
}

#[derive(Clone)]
struct Expect {
    id: NodeId,
    vectors: [Vec<f32>; 2],
}

/// Every expected vector reads back through the spill-aware indexed read,
/// (when `inline`) is an inline property, which is what the generation build
/// sees, and (when `ann`) is a member of the ANN index at distance 0.
fn assert_vectors(db: &GrafeoDB, expected: &[Expect], what: &str, inline: bool, ann: bool) {
    let store = db.graph_store();
    let mut wrong = Vec::new();
    for e in expected {
        for (p, (prop, _)) in PROPS.iter().enumerate() {
            let want = &e.vectors[p];
            match db.read_indexed_node_vector("Doc", prop, e.id) {
                Ok(IndexedVectorRead::Found(got)) if &got == want => {}
                other => wrong.push(format!("{:?} {prop} indexed read: {other:?}", e.id)),
            }
            if inline {
                match store.get_node_property(e.id, &PropertyKey::new(*prop)) {
                    Some(Value::Vector(got)) if got.as_ref() == want.as_slice() => {}
                    other => wrong.push(format!("{:?} {prop} property: {other:?}", e.id)),
                }
            }
            if ann {
                match db.vector_search("Doc", prop, want, 64, Some(256), None) {
                    Ok(hits) if hits.iter().any(|h| h.0 == e.id && h.1 < 1e-3) => {}
                    other => wrong.push(format!("{:?} {prop} ANN membership: {other:?}", e.id)),
                }
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{what}: {} wrong/missing:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

fn spill_files(spill: &Path) -> Vec<(String, u64)> {
    let mut files: Vec<(String, u64)> = std::fs::read_dir(spill)
        .map(|rd| {
            rd.filter_map(Result::ok)
                .filter(|e| e.file_name().to_string_lossy().starts_with("vectors_"))
                .map(|e| {
                    (
                        e.file_name().to_string_lossy().into_owned(),
                        e.metadata().map_or(0, |m| m.len()),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    files
}

fn generation_files(root: &Path) -> usize {
    std::fs::read_dir(root.join("generations"))
        .map(|rd| rd.filter_map(Result::ok).count())
        .unwrap_or(0)
}

fn create_doc(db: &GrafeoDB, seed: u64) -> Expect {
    let vectors = [vector(0, seed), vector(1, seed)];
    let id = db
        .create_node_with_props(
            &["Doc"],
            [
                ("title", Value::from(format!("doc-{seed}"))),
                (PROPS[0].0, Value::Vector(vectors[0].clone().into())),
                (PROPS[1].0, Value::Vector(vectors[1].clone().into())),
            ],
        )
        .unwrap();
    Expect { id, vectors }
}

fn create_indexes(db: &GrafeoDB) {
    for (prop, dims) in PROPS {
        db.create_vector_index("Doc", prop, Some(dims), Some("euclidean"), None, None, None)
            .unwrap();
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    spill: PathBuf,
    expected: Vec<Expect>,
    new_ids: [NodeId; 3],
}

/// Base of 6 `:Doc` nodes with two indexed vectors, published as a
/// generation root; then one ForceDisk session writes overlay rows:
/// a property-only copy-up, a copy-up with a new vector, an edge between two
/// base nodes, and three new nodes with vectors.
///
/// With `update_base_vector`, base node 1's embedding is also replaced. That
/// write triggers a separate, pre-existing engine bug (a vector update on a
/// base node of a writable generation root drops untouched base nodes from
/// that ANN index; reproduced on trunk `83710123`, reported on the PR), so
/// ANN membership is only asserted in the scenario without it.
fn fixture(update_base_vector: bool) -> Fixture {
    let dir = tempdir().unwrap();
    let root = dir.path().join("g.grafeo.d");
    let spill = dir.path().join("g.spill");
    std::fs::create_dir_all(&root).unwrap();

    let source = GrafeoDB::new_in_memory();
    let mut expected: Vec<Expect> = (0..6u64).map(|i| create_doc(&source, i)).collect();
    create_indexes(&source);
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .unwrap();
    drop(source);

    let new_ids;
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&root, &spill)).unwrap();
        db.set_node_property(expected[0].id, "title", Value::from("doc-0 v2"))
            .unwrap();
        if update_base_vector {
            db.set_node_property(
                expected[1].id,
                "embedding",
                Value::Vector(vector(0, 101).into()),
            )
            .unwrap();
            expected[1].vectors[0] = vector(0, 101);
        }
        db.create_edge(expected[2].id, expected[3].id, "LINKS");
        let fresh: Vec<Expect> = (200..203u64).map(|s| create_doc(&db, s)).collect();
        new_ids = [fresh[0].id, fresh[1].id, fresh[2].id];
        expected.extend(fresh);
        assert_vectors(&db, &expected, "writer session", true, !update_base_vector);
        db.close().unwrap();
    }
    Fixture {
        _dir: dir,
        root,
        spill,
        expected,
        new_ids,
    }
}

/// The #167 sequence: reopen under ForceDisk (replay → spill), checkpoint,
/// freeze, build, publish, install; then reopen without and with ForceDisk.
#[test]
fn forcedisk_handoff_keeps_spilled_overlay_vectors() {
    forcedisk_handoff(true);
}

/// Same sequence without a base-vector update, so the ANN index can be
/// checked too: every recovered vector is a member after the reopens.
#[test]
fn forcedisk_handoff_indexes_recovered_vectors() {
    forcedisk_handoff(false);
}

fn forcedisk_handoff(update_base_vector: bool) {
    let ann = !update_base_vector;
    let f = fixture(update_base_vector);
    let mut expected = f.expected.clone();
    let [kept_new, updated_new, deleted_new] = f.new_ids;
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&f.root, &f.spill)).unwrap();
        let spilled = spill_files(&f.spill);
        assert_eq!(
            spilled.len(),
            2,
            "both indexed columns spilled: {spilled:?}"
        );
        assert!(spilled.iter().all(|(_, len)| *len > 64), "{spilled:?}");
        assert_vectors(
            &db,
            &expected,
            "ForceDisk reopen, before handoff",
            false,
            ann,
        );

        // After the spill: a new inline vector on a spilled node must win
        // over its spilled copy, and a node deleted after the spill must not
        // come back. `kept_new` stays spill-only.
        db.set_node_property(
            updated_new,
            "embedding",
            Value::Vector(vector(0, 300).into()),
        )
        .unwrap();
        expected
            .iter_mut()
            .find(|e| e.id == updated_new)
            .unwrap()
            .vectors[0] = vector(0, 300);
        assert!(db.delete_node(deleted_new).unwrap());
        expected.retain(|e| e.id != deleted_new);
        db.wal_checkpoint().unwrap();
        assert_vectors(&db, &expected, "ForceDisk, writes after spill", false, ann);

        let handle = db.freeze_epoch_for_handoff(&f.root).expect("freeze");
        // The freeze snapshots the spill but does not hold the vectors.
        assert_eq!(handle.spilled_vectors.column_count(), 2);
        let frozen = handle
            .frozen_nodes
            .iter()
            .find(|n| n.id.as_u64() == kept_new.as_u64())
            .expect("kept node frozen");
        assert!(
            PROPS
                .iter()
                .all(|(p, _)| !frozen.properties.contains_key(&PropertyKey::new(*p))),
            "spill-only vectors must not be materialized in the freeze"
        );
        let report = db
            .complete_epoch_handoff(handle, generation_build_request(&f.root, "g2"))
            .expect("build + publish");
        db.publish_and_install_handoff(report).expect("install");
        assert_vectors(
            &db,
            &expected,
            "ForceDisk, after handoff install",
            false,
            ann,
        );
        db.close().unwrap();
    }
    for reopen in ["Auto reopen 1", "Auto reopen 2"] {
        let db = GrafeoDB::open_generation_root(&f.root, false).unwrap();
        assert_vectors(&db, &expected, reopen, true, ann);
        assert!(
            db.graph_store().get_node(deleted_new).is_none(),
            "{reopen}: deleted node came back"
        );
        db.close().unwrap();
    }
    let db = GrafeoDB::open_generation_root_with_config(force_disk(&f.root, &f.spill)).unwrap();
    assert_vectors(&db, &expected, "ForceDisk reopen after handoff", false, ann);
}

/// Control: the same sequence under the Auto tier never lost vectors.
#[test]
fn auto_tier_handoff_keeps_overlay_vectors() {
    let f = fixture(true);
    {
        let db = GrafeoDB::open_generation_root(&f.root, false).unwrap();
        let report = db
            .run_epoch_handoff(generation_build_request(&f.root, "g2"))
            .unwrap();
        db.publish_and_install_handoff(report).unwrap();
        db.close().unwrap();
    }
    let db = GrafeoDB::open_generation_root(&f.root, false).unwrap();
    assert_vectors(&db, &f.expected, "Auto reopen", true, false);
}

/// Review P1: a spilled vector that exists but can't be read must fail the
/// handoff before publication, not publish a base without it.
///
/// `MmapStorage` caches up to 10,000 vectors and evicts half when an insert
/// finds the cache full, so spilling 10,050 vectors leaves about 5,000 cold
/// entries; truncating the spill file then makes them unreadable.
#[test]
fn unreadable_spilled_vector_fails_the_handoff_before_publication() {
    const ROWS: u64 = 10_050;
    let dir = tempdir().unwrap();
    let root = dir.path().join("cold.grafeo.d");
    let spill = dir.path().join("cold.spill");
    std::fs::create_dir_all(&root).unwrap();
    let source = GrafeoDB::new_in_memory();
    create_doc(&source, 0);
    create_indexes(&source);
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .unwrap();
    drop(source);

    let mut expected = Vec::new();
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&root, &spill)).unwrap();
        for seed in 1..=ROWS {
            expected.push(create_doc(&db, seed));
        }
        db.close().unwrap();
    }
    let generations_before = generation_files(&root);
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&root, &spill)).unwrap();
        for (name, _) in spill_files(&spill) {
            std::fs::OpenOptions::new()
                .write(true)
                .open(spill.join(&name))
                .unwrap()
                .set_len(64)
                .unwrap();
        }
        let err = db
            .run_epoch_handoff(generation_build_request(&root, "g2"))
            .expect_err("an unreadable spilled vector must fail the handoff")
            .to_string();
        assert!(err.contains("exists but cannot be read"), "{err}");
        db.close().unwrap();
    }
    assert_eq!(
        generation_files(&root),
        generations_before,
        "nothing published"
    );
    // The WAL still holds every vector: an Auto reopen replays them inline.
    let db = GrafeoDB::open_generation_root(&root, false).unwrap();
    let store = db.graph_store();
    let key = PropertyKey::new("embedding");
    for e in expected.iter().step_by(97) {
        match store.get_node_property(e.id, &key) {
            Some(Value::Vector(v)) if v.as_ref() == e.vectors[0].as_slice() => {}
            other => panic!("{:?} after failed handoff: {other:?}", e.id),
        }
    }
}
