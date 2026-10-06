//! AMH #167 — an epoch handoff on a generation root opened with the
//! ForceDisk vector tier must carry the overlay's spilled vectors into the
//! new base.
//!
//! A generation-root open replays the post-boundary WAL into the overlay and
//! then applies `TierOverride::ForceDisk`, which drains the overlay's
//! vector-indexed property columns into mmap spill files. The handoff's
//! freeze capture read overlay nodes from the property store only, so every
//! overlay node (copy-ups of base nodes and new nodes) reached the new base
//! without its vector: permanent loss for every later reader.
//!
//! ```text
//! cargo test -p grafeo-engine --features generation,generation-streaming,compact-store,lpg,mmap,wal,cypher,vector-index \
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

const DIMS: usize = 4;

fn vector(seed: u64) -> Vec<f32> {
    (0..DIMS as u64)
        .map(|d| (seed * 10 + d) as f32 + 0.5)
        .collect()
}

/// AMH's production open config for a writable graph: ForceDisk vector tier
/// plus a spill directory (`am_graph::grafeo::runtime_core::open`).
fn force_disk(root: &Path, spill: &Path) -> Config {
    Config::persistent(root)
        .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk)
        .with_spill_path(spill)
}

/// Every `(node, expected vector)` must read back both inline (what the
/// generation build and a non-spilled reader see) and through the
/// spill-aware indexed read.
fn assert_vectors(db: &GrafeoDB, expected: &[(NodeId, Vec<f32>)], what: &str, inline: bool) {
    let key = PropertyKey::new("embedding");
    let store = db.graph_store();
    let mut missing = Vec::new();
    for (id, want) in expected {
        match db.read_indexed_node_vector("Doc", "embedding", *id) {
            Ok(IndexedVectorRead::Found(got)) if &got == want => {}
            other => missing.push(format!("{id:?} indexed read: {other:?}")),
        }
        if inline {
            match store.get_node_property(*id, &key) {
                Some(Value::Vector(got)) if got.as_ref() == want.as_slice() => {}
                other => missing.push(format!("{id:?} property: {other:?}")),
            }
        }
    }
    assert!(
        missing.is_empty(),
        "{what}: {} wrong/missing vectors:\n{}",
        missing.len(),
        missing.join("\n")
    );
}

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    spill: PathBuf,
    expected: Vec<(NodeId, Vec<f32>)>,
}

/// Base of 6 `:Doc` nodes with an indexed embedding, published as a
/// generation root; then one ForceDisk session writes overlay rows:
/// a property-only copy-up, a copy-up with a new vector, an edge between two
/// base nodes, and two new nodes with vectors.
fn fixture() -> Fixture {
    let dir = tempdir().unwrap();
    let root = dir.path().join("g.grafeo.d");
    let spill = dir.path().join("g.spill");
    std::fs::create_dir_all(&root).unwrap();

    let source = GrafeoDB::new_in_memory();
    let mut expected = Vec::new();
    for i in 0..6u64 {
        let id = source
            .create_node_with_props(
                &["Doc"],
                [
                    ("title", Value::from(format!("doc-{i}"))),
                    ("embedding", Value::Vector(vector(i).into())),
                ],
            )
            .unwrap();
        expected.push((id, vector(i)));
    }
    source
        .create_vector_index(
            "Doc",
            "embedding",
            Some(DIMS),
            Some("cosine"),
            None,
            None,
            None,
        )
        .unwrap();
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .unwrap();
    drop(source);

    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&root, &spill)).unwrap();
        // Copy-up, vector unchanged.
        db.set_node_property(expected[0].0, "title", Value::from("doc-0 v2"))
            .unwrap();
        // Copy-up with a new vector.
        db.set_node_property(
            expected[1].0,
            "embedding",
            Value::Vector(vector(101).into()),
        )
        .unwrap();
        expected[1].1 = vector(101);
        // Edge between two base nodes.
        db.create_edge(expected[2].0, expected[3].0, "LINKS");
        // New nodes.
        for i in 200..202u64 {
            let id = db
                .create_node_with_props(
                    &["Doc"],
                    [
                        ("title", Value::from(format!("new-{i}"))),
                        ("embedding", Value::Vector(vector(i).into())),
                    ],
                )
                .unwrap();
            expected.push((id, vector(i)));
        }
        assert_vectors(&db, &expected, "writer session", true);
        db.close().unwrap();
    }
    Fixture {
        _dir: dir,
        root,
        spill,
        expected,
    }
}

/// The #167 sequence: reopen under ForceDisk (replay → spill), compact via
/// epoch handoff, then reopen without ForceDisk and read the new base.
#[test]
fn forcedisk_handoff_keeps_spilled_overlay_vectors() {
    let f = fixture();
    let mut expected = f.expected.clone();
    let mut deleted_after_spill = None;
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&f.root, &f.spill)).unwrap();
        // Before compaction the vectors are readable through the spill.
        assert_vectors(&db, &f.expected, "ForceDisk reopen, before handoff", false);
        // After the spill: a new inline vector on a spilled node must win
        // over its spilled copy, and a node deleted after the spill must
        // not come back.
        db.set_node_property(
            expected[6].0,
            "embedding",
            Value::Vector(vector(300).into()),
        )
        .unwrap();
        expected[6].1 = vector(300);
        let (deleted, _) = expected.pop().unwrap();
        assert!(db.delete_node(deleted).unwrap());
        deleted_after_spill = Some(deleted);
        assert_vectors(&db, &expected, "ForceDisk, writes after spill", false);
        let report = db
            .run_epoch_handoff(generation_build_request(&f.root, "g2"))
            .expect("epoch handoff");
        db.publish_and_install_handoff(report)
            .expect("publish and install");
        assert_vectors(&db, &expected, "ForceDisk, after handoff install", false);
        db.close().unwrap();
    }
    // Plain (Auto-tier) reopen: the vectors must now be in the new base.
    for reopen in ["Auto reopen 1", "Auto reopen 2"] {
        let db = GrafeoDB::open_generation_root(&f.root, false).unwrap();
        assert_vectors(&db, &expected, reopen, true);
        let gone = deleted_after_spill.unwrap();
        assert!(
            db.graph_store().get_node(gone).is_none(),
            "{reopen}: deleted node {gone:?} came back"
        );
        db.close().unwrap();
    }
    let db = GrafeoDB::open_generation_root_with_config(force_disk(&f.root, &f.spill)).unwrap();
    assert_vectors(&db, &expected, "ForceDisk reopen after handoff", false);
}

/// Control: the same sequence under the Auto tier never lost vectors.
#[test]
fn auto_tier_handoff_keeps_overlay_vectors() {
    let f = fixture();
    {
        let db = GrafeoDB::open_generation_root(&f.root, false).unwrap();
        let report = db
            .run_epoch_handoff(generation_build_request(&f.root, "g2"))
            .unwrap();
        db.publish_and_install_handoff(report).unwrap();
        db.close().unwrap();
    }
    let db = GrafeoDB::open_generation_root(&f.root, false).unwrap();
    assert_vectors(&db, &f.expected, "Auto reopen", true);
}
