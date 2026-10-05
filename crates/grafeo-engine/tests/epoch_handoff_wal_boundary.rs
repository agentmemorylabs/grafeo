//! Writes made on a generation-root handle after an epoch handoff must
//! survive close + reopen.
//!
//! A writable generation root (`*.grafeo.d`) logs every write to its WAL in
//! `root/wal`. An epoch handoff freezes the overlay at a WAL boundary B,
//! builds and publishes a new generation from it, and records B in the
//! manifest; reopen replays the WAL from B. So every write made after the
//! handoff must land at or after B in the WAL the handle appends to, and a
//! later handoff's WAL truncation (which deletes files before its own
//! boundary) must not delete the file the handle is still appending to.
//!
//! Each case runs with and without `publish_and_install_handoff` (the base
//! swap downstream runs after a handoff).

use std::path::Path;

use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::tempdir;

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

fn sorted(names: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = names.iter().map(|s| (*s).to_string()).collect();
    out.sort();
    out
}

fn create(db: &GrafeoDB, name: &str) {
    db.execute_cypher(&format!("CREATE (:L {{name: '{name}'}})"))
        .expect("create");
}

/// One epoch handoff on `db`, then the base swap when `install` is set.
/// Records a failure when the handle's WAL does not append at the
/// handoff's boundary (checked after the data, so both show).
fn handoff(
    db: &GrafeoDB,
    root: &Path,
    generation: &str,
    install: bool,
    failures: &mut Vec<String>,
) {
    let report = db
        .run_epoch_handoff(generation_build_request(root, generation))
        .expect("epoch handoff");
    let current = db
        .wal()
        .expect("a writable generation root has a WAL")
        .current_sequence();
    if current != report.wal_boundary.log_sequence {
        failures.push(format!(
            "[{generation}, install: {install}] the handle's WAL appends to \
             sequence {current}, before the boundary {}",
            report.wal_boundary.log_sequence
        ));
    }
    if install {
        db.publish_and_install_handoff(report)
            .expect("publish and install handoff");
    }
}

/// Records a failure when the WAL file the handle appends to is gone.
fn check_current_wal_file_exists(
    db: &GrafeoDB,
    root: &Path,
    stage: &str,
    failures: &mut Vec<String>,
) {
    let wal = db.wal().expect("WAL");
    let file = root
        .join("wal")
        .join(format!("wal_{:08}.log", wal.current_sequence()));
    if !file.exists() {
        failures.push(format!(
            "[{stage}] the handle's current WAL file {} was deleted",
            file.display()
        ));
    }
}

/// Records a failure when `got` differs from `want`.
fn check(got: Vec<String>, want: &[String], stage: &str, failures: &mut Vec<String>) {
    if got != want {
        failures.push(format!("[{stage}] got {got:?}, want {want:?}"));
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

/// Writes after one handoff, a create and an update, survive close + reopen.
#[test]
fn writes_after_one_handoff_survive_reopen() {
    let mut failures = Vec::new();
    for install in [false, true] {
        let dir = tempdir().expect("temp dir");
        let root = dir.path().join("one.grafeo.d");
        publish_base(&root);

        let db = open(&root);
        create(&db, "before");
        handoff(&db, &root, "g2", install, &mut failures);
        create(&db, "after");
        db.execute_cypher("MATCH (n:L {name: 'b1'}) SET n.name = 'b1-after'")
            .expect("update after handoff");
        let want = sorted(&["b1-after", "b2", "before", "after"]);
        check(
            names(&db),
            &want,
            &format!("live, install: {install}"),
            &mut failures,
        );
        db.close().expect("close");
        drop(db);

        let db = open(&root);
        check(
            names(&db),
            &want,
            &format!("after reopen, install: {install}"),
            &mut failures,
        );
    }
    assert_no_failures(&failures);
}

/// Two handoffs on one handle: the second handoff's WAL truncation keeps
/// the file the handle appends to, and writes before, between and after
/// the handoffs all survive close + reopen.
#[test]
fn writes_around_two_handoffs_survive_reopen() {
    let mut failures = Vec::new();
    for install in [false, true] {
        let dir = tempdir().expect("temp dir");
        let root = dir.path().join("two.grafeo.d");
        publish_base(&root);

        let db = open(&root);
        create(&db, "w0");
        handoff(&db, &root, "g2", install, &mut failures);
        create(&db, "w1");
        handoff(&db, &root, "g3", install, &mut failures);
        let stage = format!("after second handoff, install: {install}");
        check_current_wal_file_exists(&db, &root, &stage, &mut failures);
        create(&db, "w2");
        let want = sorted(&["b1", "b2", "w0", "w1", "w2"]);
        check(
            names(&db),
            &want,
            &format!("live, install: {install}"),
            &mut failures,
        );
        db.close().expect("close");
        drop(db);

        let db = open(&root);
        check(
            names(&db),
            &want,
            &format!("after reopen, install: {install}"),
            &mut failures,
        );
    }
    assert_no_failures(&failures);
}
