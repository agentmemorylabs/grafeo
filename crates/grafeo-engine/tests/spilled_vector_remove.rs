//! AMH #174 (part 2): removing a vector that ForceDisk spilled must really
//! remove it.
//!
//! Before: `GrafeoDB::remove_node_property` only looked at the inline
//! property store, so a spill-only vector was a silent no-op (returned
//! `false`, no WAL record, still read and searched, back after reopen); and
//! after a Cypher `REMOVE` (a `Null` inline) the spill-aware accessor fell
//! back to the spilled copy, so the same session still read the old vector.
//!
//! ```text
//! cargo test -p grafeo-engine --features generation,generation-streaming,compact-store,lpg,mmap,wal,cypher,vector-index \
//!   --test spilled_vector_remove
//! ```

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "compact-store",
    feature = "lpg",
    feature = "mmap",
    feature = "wal",
    feature = "cypher",
    feature = "vector-index",
    not(feature = "temporal")
))]

use std::path::Path;

use grafeo_common::storage::{SectionType, TierOverride};
use grafeo_common::types::{NodeId, Value};
use grafeo_engine::{Config, GrafeoDB, IndexedVectorRead, generation_build_request};
use tempfile::tempdir;

fn v(seed: u64) -> Vec<f32> {
    (0..4).map(|d| (seed * 10 + d) as f32 + 0.5).collect()
}

fn force_disk(path: &Path, spill: &Path) -> Config {
    Config::persistent(path)
        .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk)
        .with_spill_path(spill)
}

fn indexed(db: &GrafeoDB, id: NodeId) -> bool {
    matches!(
        db.read_indexed_node_vector("Doc", "embedding", id),
        Ok(IndexedVectorRead::Found(_))
    )
}

fn in_ann(db: &GrafeoDB, id: NodeId, seed: u64) -> bool {
    db.vector_search("Doc", "embedding", &v(seed), 10, Some(64), None)
        .unwrap()
        .iter()
        .any(|h| h.0 == id)
}

/// The node to remove (seed 1) plus a neighbour (seed 0) in a plain file,
/// written then reopened under ForceDisk so both vectors are spilled.
fn plain_file(dir: &Path) -> (std::path::PathBuf, NodeId, NodeId) {
    let file = dir.join("p.grafeo");
    let spill = dir.join("sp");
    let db = GrafeoDB::with_config(force_disk(&file, &spill)).unwrap();
    let keep = db
        .create_node_with_props(&["Doc"], [("embedding", Value::Vector(v(0).into()))])
        .unwrap();
    let gone = db
        .create_node_with_props(&["Doc"], [("embedding", Value::Vector(v(1).into()))])
        .unwrap();
    db.create_vector_index(
        "Doc",
        "embedding",
        Some(4),
        Some("euclidean"),
        None,
        None,
        None,
    )
    .unwrap();
    db.close().unwrap();
    (file, keep, gone)
}

/// The same pair on a generation root: `keep` in the base, `gone` written
/// in a session and closed, so the next ForceDisk open replays and spills it.
fn root(dir: &Path) -> (std::path::PathBuf, NodeId, NodeId) {
    let root = dir.join("g.grafeo.d");
    let spill = dir.join("sp");
    std::fs::create_dir_all(&root).unwrap();
    let src = GrafeoDB::new_in_memory();
    let keep = src
        .create_node_with_props(&["Doc"], [("embedding", Value::Vector(v(0).into()))])
        .unwrap();
    src.create_vector_index(
        "Doc",
        "embedding",
        Some(4),
        Some("euclidean"),
        None,
        None,
        None,
    )
    .unwrap();
    src.build_and_publish_generation(generation_build_request(&root, "g1"))
        .unwrap();
    drop(src);
    let db = GrafeoDB::open_generation_root_with_config(force_disk(&root, &spill)).unwrap();
    let gone = db
        .create_node_with_props(&["Doc"], [("embedding", Value::Vector(v(1).into()))])
        .unwrap();
    db.close().unwrap();
    drop(db);
    (root, keep, gone)
}

#[test]
fn direct_remove_of_a_spilled_vector_is_real_plain_file() {
    let dir = tempdir().unwrap();
    let spill = dir.path().join("sp");
    let (file, keep, gone) = plain_file(dir.path());
    {
        let db = GrafeoDB::with_config(force_disk(&file, &spill)).unwrap();
        assert!(
            indexed(&db, gone) && in_ann(&db, gone, 1),
            "spilled vector served before"
        );
        assert!(
            db.remove_node_property(gone, "embedding"),
            "removal reported"
        );
        assert!(!indexed(&db, gone), "exact read after removal");
        assert!(!in_ann(&db, gone, 1), "ANN after removal");
        assert!(indexed(&db, keep) && in_ann(&db, keep, 0), "neighbour kept");
        db.close().unwrap();
    }
    let db = GrafeoDB::with_config(force_disk(&file, &spill)).unwrap();
    assert!(!indexed(&db, gone), "removal survives reopen (WAL-logged)");
    assert!(
        indexed(&db, keep) && in_ann(&db, keep, 0),
        "neighbour kept after reopen"
    );
}

#[test]
fn direct_remove_of_a_spilled_vector_is_real_generation_root() {
    let dir = tempdir().unwrap();
    let spill = dir.path().join("sp");
    let (root, keep, gone) = root(dir.path());
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&root, &spill)).unwrap();
        assert!(indexed(&db, gone), "spilled overlay vector served before");
        assert!(
            db.remove_node_property(gone, "embedding"),
            "removal reported"
        );
        assert!(!indexed(&db, gone), "exact read after removal");
        assert!(!in_ann(&db, gone, 1), "ANN after removal");
        let report = db
            .run_epoch_handoff(generation_build_request(&root, "g2"))
            .unwrap();
        db.publish_and_install_handoff(report).unwrap();
        assert!(!indexed(&db, gone), "after handoff");
        db.close().unwrap();
    }
    let db = GrafeoDB::open_generation_root(&root, false).unwrap();
    assert!(
        !indexed(&db, gone),
        "new base is vectorless for the removed node"
    );
    assert!(indexed(&db, keep), "base neighbour kept");
}

#[test]
fn cypher_remove_hides_the_spilled_copy_in_session() {
    let dir = tempdir().unwrap();
    let spill = dir.path().join("sp");
    let (file, _keep, gone) = plain_file(dir.path());
    let db = GrafeoDB::with_config(force_disk(&file, &spill)).unwrap();
    assert!(indexed(&db, gone));
    db.execute_cypher(&format!(
        "MATCH (n:Doc) WHERE id(n) = {} REMOVE n.embedding",
        gone.as_u64()
    ))
    .unwrap();
    assert!(
        !indexed(&db, gone),
        "the inline Null from REMOVE must win over the spilled copy"
    );
}

#[test]
fn direct_remove_of_an_inline_vector_leaves_the_ann_index() {
    let dir = tempdir().unwrap();
    let db = GrafeoDB::with_config(Config::persistent(dir.path().join("a.grafeo"))).unwrap();
    let keep = db
        .create_node_with_props(&["Doc"], [("embedding", Value::Vector(v(0).into()))])
        .unwrap();
    let gone = db
        .create_node_with_props(&["Doc"], [("embedding", Value::Vector(v(1).into()))])
        .unwrap();
    db.create_vector_index(
        "Doc",
        "embedding",
        Some(4),
        Some("euclidean"),
        None,
        None,
        None,
    )
    .unwrap();
    assert!(in_ann(&db, gone, 1));
    assert!(db.remove_node_property(gone, "embedding"));
    assert!(
        !in_ann(&db, gone, 1),
        "removed vector left in the ANN index"
    );
    assert!(in_ann(&db, keep, 0));
}
