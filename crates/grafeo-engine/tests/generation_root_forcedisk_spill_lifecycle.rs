//! AMH #167 review round 2: spill lifecycle around an epoch handoff.
//!
//! - removing the label a vector was spilled under keeps the vector;
//! - a reload that meets an unreadable spilled vector fails and keeps the
//!   spill authority (registry + file), and the handoff then fails before
//!   publication;
//! - a reload + re-spill between freeze and build never truncates the file
//!   the frozen snapshot reads (unique spill files, `create_new`);
//! - a reload whose unlink fails keeps the registry entry (inline values
//!   win), and the handoff still publishes the right vectors.

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
use grafeo_core::graph::traits::GraphStore as _;
use grafeo_engine::{Config, GrafeoDB, IndexedVectorRead, generation_build_request};
use tempfile::tempdir;

const DIMS: usize = 4;
const CONSUMER: &str = "section:VectorStore";

fn vector(seed: u64) -> Vec<f32> {
    (0..DIMS as u64)
        .map(|d| (seed * 10 + d) as f32 + 0.5)
        .collect()
}

fn force_disk(root: &Path, spill: &Path) -> Config {
    Config::persistent(root)
        .with_section_tier(SectionType::VectorStore, TierOverride::ForceDisk)
        .with_spill_path(spill)
}

fn spill_files(spill: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(spill)
        .map(|rd| {
            rd.filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .is_some_and(|n| n.to_string_lossy().starts_with("vectors_"))
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

/// Inline property after an Auto reopen (what the new base holds).
fn assert_inline(db: &GrafeoDB, expected: &[(NodeId, Vec<f32>)], what: &str) {
    let key = PropertyKey::new("embedding");
    let store = db.graph_store();
    for (id, want) in expected {
        match store.get_node_property(*id, &key) {
            Some(Value::Vector(got)) if got.as_ref() == want.as_slice() => {}
            other => panic!("{what}: {id:?} embedding {other:?}, want {want:?}"),
        }
    }
}

struct Root {
    _dir: tempfile::TempDir,
    root: PathBuf,
    spill: PathBuf,
}

/// A root with one indexed base node, then `rows` overlay nodes (labels
/// from `labels`) written in a ForceDisk session and closed, so the next
/// ForceDisk open replays and spills them.
fn root_with_overlay(rows: u64, labels: &[&str]) -> (Root, Vec<(NodeId, Vec<f32>)>) {
    let dir = tempdir().unwrap();
    let root = dir.path().join("r.grafeo.d");
    let spill = dir.path().join("r.spill");
    std::fs::create_dir_all(&root).unwrap();
    let source = GrafeoDB::new_in_memory();
    source
        .create_node_with_props(&["Doc"], [("embedding", Value::Vector(vector(0).into()))])
        .unwrap();
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
        .unwrap();
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .unwrap();
    drop(source);
    let mut expected = Vec::new();
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&root, &spill)).unwrap();
        for seed in 1..=rows {
            let id = db
                .create_node_with_props(labels, [("embedding", Value::Vector(vector(seed).into()))])
                .unwrap();
            expected.push((id, vector(seed)));
        }
        db.close().unwrap();
    }
    (
        Root {
            _dir: dir,
            root,
            spill,
        },
        expected,
    )
}

/// Must-fix 1: the vector of a node whose index label was removed after the
/// spill still reaches the new base.
#[test]
fn removing_the_spill_label_keeps_the_vector() {
    let (r, expected) = root_with_overlay(3, &["Doc", "Other"]);
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&r.root, &r.spill)).unwrap();
        assert!(!spill_files(&r.spill).is_empty(), "overlay vectors spilled");
        assert!(db.remove_node_label(expected[0].0, "Doc"));
        let report = db
            .run_epoch_handoff(generation_build_request(&r.root, "g2"))
            .unwrap();
        db.publish_and_install_handoff(report).unwrap();
        db.close().unwrap();
    }
    let db = GrafeoDB::open_generation_root(&r.root, false).unwrap();
    let node = db.graph_store().get_node(expected[0].0).expect("node kept");
    assert!(
        !node.labels.iter().any(|l| l.as_str() == "Doc"),
        "label removed"
    );
    assert_inline(&db, &expected, "after label removal + handoff");
}

/// Must-fix 2: a reload that meets an unreadable spilled vector fails and
/// keeps the registry and file; the handoff then fails before publication.
#[test]
fn reload_with_an_unreadable_entry_fails_and_keeps_authority() {
    // More than MmapStorage's 10,000-entry cache, so some entries are cold.
    let (r, expected) = root_with_overlay(10_050, &["Doc"]);
    let before = generation_files(&r.root);
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&r.root, &r.spill)).unwrap();
        let files = spill_files(&r.spill);
        assert_eq!(files.len(), 1);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&files[0])
            .unwrap()
            .set_len(64)
            .unwrap();
        assert!(
            db.prepare_vector_mutation().is_err(),
            "reload must fail on an unreadable spilled vector"
        );
        assert_eq!(spill_files(&r.spill), files, "spill file kept");
        // Registry authority kept: a cached entry still reads through it.
        let found = expected.iter().any(|(id, want)| {
            matches!(
                db.read_indexed_node_vector("Doc", "embedding", *id),
                Ok(IndexedVectorRead::Found(ref got)) if got == want
            )
        });
        assert!(found, "spill registry still serves cached entries");
        let err = db
            .run_epoch_handoff(generation_build_request(&r.root, "g2"))
            .expect_err("unreadable spilled vector must fail the handoff");
        assert!(
            err.to_string().contains("exists but cannot be read"),
            "{err}"
        );
        db.close().unwrap();
    }
    assert_eq!(generation_files(&r.root), before, "nothing published");
    let db = GrafeoDB::open_generation_root(&r.root, false).unwrap();
    let sample: Vec<_> = expected.iter().step_by(101).cloned().collect();
    assert_inline(&db, &sample, "WAL still holds every vector");
}

/// Must-fix 3: reload + re-spill between freeze and build creates a new
/// spill file; the frozen snapshot keeps reading its own (unlinked) file.
#[test]
fn respill_after_freeze_does_not_reuse_the_frozen_file() {
    let (r, expected) = root_with_overlay(5, &["Doc"]);
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&r.root, &r.spill)).unwrap();
        let first = spill_files(&r.spill);
        assert_eq!(first.len(), 1);
        let handle = db.freeze_epoch_for_handoff(&r.root).unwrap();
        // Between freeze and build: reload (unlinks the frozen file and puts
        // the vectors inline again), then spill again. Writers stay quiesced:
        // the handoff refuses writes between freeze and install.
        db.prepare_vector_mutation().unwrap();
        assert!(spill_files(&r.spill).is_empty(), "reload unlinked the file");
        db.buffer_manager().spill_consumer_by_name(CONSUMER);
        let second = spill_files(&r.spill);
        assert_eq!(second.len(), 1);
        assert_ne!(first, second, "a re-spill uses a new file");
        let report = db
            .complete_epoch_handoff(handle, generation_build_request(&r.root, "g2"))
            .unwrap();
        db.publish_and_install_handoff(report).unwrap();
        db.close().unwrap();
    }
    let db = GrafeoDB::open_generation_root(&r.root, false).unwrap();
    assert_inline(&db, &expected, "after re-spill during the build");
}

/// Must-fix 3, failed unlink: the reload restores inline values, keeps the
/// registry entry and reports the error; the handoff still publishes the
/// right vectors (inline wins), and no file is reused.
#[cfg(unix)]
#[test]
fn reload_with_failed_unlink_keeps_the_registry_and_publishes_correctly() {
    use std::os::unix::fs::PermissionsExt;
    let (r, expected) = root_with_overlay(5, &["Doc"]);
    {
        let db = GrafeoDB::open_generation_root_with_config(force_disk(&r.root, &r.spill)).unwrap();
        let files = spill_files(&r.spill);
        assert_eq!(files.len(), 1);
        std::fs::set_permissions(&r.spill, std::fs::Permissions::from_mode(0o555)).unwrap();
        let reload = db.prepare_vector_mutation();
        std::fs::set_permissions(&r.spill, std::fs::Permissions::from_mode(0o755)).unwrap();
        if reload.is_ok() {
            // Running as root ignores directory permissions; nothing to test.
            eprintln!("skipping: unlink was not refused (running as root?)");
            return;
        }
        assert_eq!(
            spill_files(&r.spill),
            files,
            "file kept after failed unlink"
        );
        let report = db
            .run_epoch_handoff(generation_build_request(&r.root, "g2"))
            .unwrap();
        db.publish_and_install_handoff(report).unwrap();
        db.close().unwrap();
    }
    let db = GrafeoDB::open_generation_root(&r.root, false).unwrap();
    assert_inline(&db, &expected, "after failed unlink + handoff");
}
