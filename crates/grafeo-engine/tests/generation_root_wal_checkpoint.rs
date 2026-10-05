//! `wal_checkpoint()` on a writable generation root must not lose writes.
//!
//! A generation root (`*.grafeo.d`) keeps no snapshot of its overlay: the
//! published base plus the WAL from the manifest's boundary on is the only
//! copy of every write since the last publication. Reopen replays the WAL
//! from that boundary and fails (`MissingFile` / `SequenceGap`) if any file
//! from the boundary to the newest one is gone.
//!
//! The legacy checkpoint path writes checkpoint metadata and deletes log
//! files more than two behind the current one. On a generation root that
//! deletes WAL the published base does not contain, so after enough
//! rotations a `wal_checkpoint()` makes the next open fail and loses every
//! write in the deleted files. Old-WAL deletion on a generation root belongs
//! to publication alone, which only deletes files below its own boundary.
//!
//! The WAL rotates every 64 MiB in production. These tests force the same
//! rotation path with `rotate()` after each write so a handful of small writes
//! spans many log files.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::path::Path;

use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::tempdir;

/// Log rotations forced after the last publication (well past the two
/// files the legacy truncation keeps).
const ROTATIONS: usize = 6;

/// Publish a base generation of two `:L` nodes into `root`.
fn publish_base(root: &Path) {
    std::fs::create_dir_all(root).expect("root dir");
    let source = GrafeoDB::new_in_memory();
    source
        .execute_cypher("CREATE (:L {name: 'b1'}), (:L {name: 'b2'})")
        .expect("seed base");
    source
        .build_and_publish_generation(generation_build_request(root, "g1"))
        .expect("publish base generation");
}

fn open(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, false).expect("open generation root")
}

/// Sorted `name`s of every `:L` node.
fn names(db: &GrafeoDB) -> Vec<String> {
    let result = db
        .execute_cypher("MATCH (n:L) RETURN n.name")
        .expect("label scan");
    let mut out: Vec<String> = result
        .rows()
        .iter()
        .map(|row| row[0].as_str().expect("name").to_string())
        .collect();
    out.sort();
    out
}

fn sorted(names: Vec<String>) -> Vec<String> {
    let mut names = names;
    names.sort();
    names
}

/// Creates `:L {name: '<prefix><i>'}` and rotates the WAL after each write.
/// Returns the names written.
fn write_and_rotate(db: &GrafeoDB, prefix: &str) -> Vec<String> {
    let wal = db.wal().expect("a writable generation root has a WAL");
    let mut written = Vec::new();
    for i in 0..ROTATIONS {
        let name = format!("{prefix}{i}");
        db.execute_cypher(&format!("CREATE (:L {{name: '{name}'}})"))
            .expect("create");
        wal.rotate().expect("rotate WAL");
        written.push(name);
    }
    written
}

fn wal_sequences(root: &Path) -> Vec<u64> {
    let mut seqs: Vec<u64> = std::fs::read_dir(root.join("wal"))
        .expect("read wal dir")
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().into_string().ok()?;
            name.strip_prefix("wal_")?
                .strip_suffix(".log")?
                .parse()
                .ok()
        })
        .collect();
    seqs.sort_unstable();
    seqs
}

/// Writes spread over many rotated log files with no new publication survive
/// `wal_checkpoint()` + close + reopen, and the WAL files are all kept.
#[test]
fn wal_checkpoint_without_publication_keeps_unpublished_wal() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("ckpt.grafeo.d");
    publish_base(&root);

    let mut expected = vec!["b1".to_string(), "b2".to_string()];
    {
        let db = open(&root);
        expected.extend(write_and_rotate(&db, "w"));
        let before = wal_sequences(&root);

        db.wal_checkpoint().expect("wal_checkpoint");

        assert_eq!(
            wal_sequences(&root),
            before,
            "wal_checkpoint must not delete generation-root WAL files"
        );
        assert_eq!(names(&db), sorted(expected.clone()), "live view");
        db.close().expect("close");
    }

    let db = GrafeoDB::open_generation_root(&root, false)
        .expect("reopen after wal_checkpoint must succeed");
    assert_eq!(names(&db), sorted(expected.clone()), "after reopen");

    // A second checkpoint/close/reopen cycle stays correct.
    expected.extend(write_and_rotate(&db, "x"));
    db.wal_checkpoint().expect("second wal_checkpoint");
    db.close().expect("close");
    drop(db);
    let db = open(&root);
    assert_eq!(names(&db), sorted(expected), "after second reopen");
}

/// After a publication (epoch handoff), `wal_checkpoint()` still keeps every
/// WAL file from the new boundary on, and writes on both sides of the
/// publication survive reopen.
#[test]
fn wal_checkpoint_after_publication_keeps_post_boundary_wal() {
    for install in [false, true] {
        let dir = tempdir().expect("temp dir");
        let root = dir.path().join("pub.grafeo.d");
        publish_base(&root);

        let mut expected = vec!["b1".to_string(), "b2".to_string()];
        {
            let db = open(&root);
            expected.extend(write_and_rotate(&db, "pre"));
            let report = db
                .run_epoch_handoff(generation_build_request(&root, "g2"))
                .expect("epoch handoff");
            let boundary = report.wal_boundary.log_sequence;
            if install {
                db.publish_and_install_handoff(report)
                    .expect("publish and install handoff");
            }
            expected.extend(write_and_rotate(&db, "post"));

            db.wal_checkpoint().expect("wal_checkpoint");

            let seqs = wal_sequences(&root);
            let newest = *seqs.last().expect("a WAL file");
            for seq in boundary..=newest {
                assert!(
                    seqs.contains(&seq),
                    "[install: {install}] WAL file {seq} at or after boundary {boundary} \
                     was deleted; files: {seqs:?}"
                );
            }
            db.close().expect("close");
        }

        let db = GrafeoDB::open_generation_root(&root, false)
            .expect("reopen after wal_checkpoint must succeed");
        assert_eq!(
            names(&db),
            sorted(expected),
            "[install: {install}] after reopen"
        );
    }
}
