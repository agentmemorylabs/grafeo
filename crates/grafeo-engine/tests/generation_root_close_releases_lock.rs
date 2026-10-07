//! AMH #176: `close()` on a generation root releases the root lock.
//!
//! A generation root's exclusive `root.lock` used to be released only when
//! the `GrafeoDB` was dropped, so closing and then reopening the root in the
//! same process (AMH's runtime keeps its handle after `close(&self)`) failed
//! with "root already locked". `close()` now releases it deterministically,
//! after fencing the closed handle: its WAL is sealed (later writes are
//! refused with [`WriteOutcome::DatabaseClosed`] before anything is applied,
//! and nothing is appended to the root's WAL), and handoff, publication and
//! backup refuse. A close over a poisoned WAL still refuses to checkpoint,
//! and still releases the lock.

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

use grafeo_common::types::Value;
use grafeo_common::utils::write_outcome::WriteOutcome;
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::TempDir;

fn fresh_root() -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("close.grafeo.d");
    std::fs::create_dir_all(&root).expect("create root");
    let source = GrafeoDB::new_in_memory();
    source
        .create_node_with_props(&["Doc"], [("name", Value::from("base"))])
        .expect("base node");
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .expect("publish base generation");
    (dir, root)
}

fn open(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, false).expect("open writable")
}

fn open_read_only(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, true).expect("open read-only")
}

/// Names of every `Doc`, sorted.
fn names(db: &GrafeoDB) -> Vec<String> {
    let mut names: Vec<String> = db
        .execute_cypher("MATCH (d:Doc) RETURN d.name")
        .expect("names")
        .rows()
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.to_string(),
            other => panic!("name: {other:?}"),
        })
        .collect();
    names.sort();
    names
}

fn create(db: &GrafeoDB, name: &str) {
    db.execute_cypher(&format!("CREATE (:Doc {{name: '{name}'}})"))
        .expect("create");
}

/// Every file under `root` with its length: what a closed handle must not
/// change.
fn tree(root: &Path) -> BTreeMap<PathBuf, u64> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let entry = entry.expect("entry");
            let meta = entry.metadata().expect("metadata");
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                out.insert(entry.path(), meta.len());
            }
        }
    }
    out
}

/// The issue's case: close a writable root, then open it read-only in the
/// same process without dropping the first handle.
#[test]
fn close_then_read_only_open_without_drop() {
    let (_dir, root) = fresh_root();
    let rw = open(&root);
    create(&rw, "written");
    rw.close().expect("close");
    let ro = GrafeoDB::open_generation_root(&root, true)
        .expect("read-only open after close, before drop");
    assert_eq!(names(&ro), ["base", "written"]);
    drop(ro);
    drop(rw);
}

/// Close, then a writable reopen while the closed handle is alive; the new
/// owner's writes survive its own close and reopen.
#[test]
fn close_then_writable_reopen_without_drop() {
    let (_dir, root) = fresh_root();
    let first = open(&root);
    create(&first, "first");
    first.close().expect("close");
    let second = GrafeoDB::open_generation_root(&root, false)
        .expect("writable open after close, before drop");
    assert_eq!(names(&second), ["base", "first"]);
    create(&second, "second");
    second.close().expect("close second");
    let third = open_read_only(&root);
    assert_eq!(names(&third), ["base", "first", "second"]);
    drop((first, second, third));
}

/// A read-only open holds the same exclusive lock; its close releases it.
#[test]
fn read_only_close_releases_the_lock() {
    let (_dir, root) = fresh_root();
    let ro = open_read_only(&root);
    ro.close().expect("close read-only");
    let rw = GrafeoDB::open_generation_root(&root, false)
        .expect("writable open after a read-only close, before drop");
    assert_eq!(names(&rw), ["base"]);
    drop((ro, rw));
}

/// A close over a poisoned WAL still refuses to checkpoint (a typed
/// `WalPoisoned` error), and still releases the lock: the reopen replays
/// the WAL.
#[test]
fn poisoned_close_still_releases_the_lock() {
    let (_dir, root) = fresh_root();
    let rw = open(&root);
    create(&rw, "logged");
    rw.wal()
        .expect("root WAL")
        .poison("test: poisoned before close");
    let err = rw.close().expect_err("a poisoned close fails");
    assert_eq!(
        err.write_outcome(),
        Some(WriteOutcome::WalPoisoned),
        "{err}"
    );
    let reopened = GrafeoDB::open_generation_root(&root, false)
        .expect("writable open after a poisoned close, before drop");
    assert_eq!(names(&reopened), ["base", "logged"]);
    drop((rw, reopened));
}

/// A closed handle refuses writes before applying them, appends nothing to
/// the root it no longer owns, and refuses handoff and backup, while
/// another handle owns the root.
#[test]
fn a_closed_handle_is_fenced_off_the_root() {
    let (dir, root) = fresh_root();
    let closed = open(&root);
    // A retirement authority taken before the close.
    let authority = closed.retirement_authority().expect("authority");
    closed.close().expect("close");
    assert!(closed.retirement_authority().is_none());
    let owner = open(&root);
    create(&owner, "owner");
    let before = tree(&root);

    let err = closed
        .execute_cypher("CREATE (:Doc {name: 'late'})")
        .expect_err("a write on a closed database");
    assert_eq!(
        err.write_outcome(),
        Some(WriteOutcome::DatabaseClosed),
        "{err}"
    );
    assert_eq!(
        names(&closed),
        ["base"],
        "the refused write was not applied"
    );
    let err = closed
        .session()
        .execute_cypher("CREATE (:Doc {name: 'late'})")
        .expect_err("a session write on a closed database");
    assert_eq!(
        err.write_outcome(),
        Some(WriteOutcome::DatabaseClosed),
        "{err}"
    );
    let node = closed
        .execute_cypher("MATCH (d:Doc) RETURN id(d)")
        .expect("read")
        .rows()[0][0]
        .clone();
    let Value::Int64(raw) = node else {
        panic!("{node:?}")
    };
    let err = closed
        .set_node_property(
            grafeo_common::types::NodeId::new(raw as u64),
            "name",
            Value::from("late"),
        )
        .expect_err("a direct write on a closed database");
    assert_eq!(
        err.write_outcome(),
        Some(WriteOutcome::DatabaseClosed),
        "{err}"
    );
    let other_root = dir.path().join("elsewhere.grafeo.d");
    std::fs::create_dir_all(&other_root).expect("other root");
    let refusals: Vec<(&str, grafeo_common::utils::error::Error)> = vec![
        (
            "handoff",
            closed
                .run_epoch_handoff(generation_build_request(&root, "g-late"))
                .expect_err("a handoff on a closed database"),
        ),
        (
            "freeze",
            closed
                .freeze_epoch_for_handoff(&root)
                .expect_err("a freeze on a closed database"),
        ),
        (
            "backup",
            closed
                .backup_generation_root(dir.path().join("backups"), "late")
                .expect_err("a backup of a closed database")
                .into(),
        ),
        (
            "build into its root",
            closed
                .build_and_publish_generation(generation_build_request(&root, "g-late"))
                .expect_err("a generation build from a closed database"),
        ),
        (
            "build elsewhere",
            closed
                .build_and_publish_generation(generation_build_request(&other_root, "g-late"))
                .expect_err("a generation build from a closed database"),
        ),
        (
            "retirement through an authority taken before close",
            grafeo_engine::collect_retirement(
                authority,
                &grafeo_engine::plan_retirement(authority).expect("plan"),
            )
            .expect_err("GC through a released authority")
            .into(),
        ),
    ];
    for (what, err) in refusals {
        assert_eq!(
            err.write_outcome(),
            Some(WriteOutcome::DatabaseClosed),
            "{what}: {err}"
        );
    }
    assert!(
        std::fs::read_dir(&other_root)
            .expect("read")
            .next()
            .is_none(),
        "nothing built elsewhere"
    );
    assert_eq!(tree(&root), before, "the closed handle changed the root");

    owner.close().expect("close owner");
    let check = open_read_only(&root);
    assert_eq!(names(&check), ["base", "owner"]);
    drop((closed, owner, check));
}

/// Closing twice is a no-op; the second close does not touch a root that a
/// new owner holds.
#[test]
fn second_close_is_a_no_op() {
    let (_dir, root) = fresh_root();
    let rw = open(&root);
    rw.close().expect("close");
    let owner = open(&root);
    let before = tree(&root);
    rw.close().expect("second close");
    assert_eq!(tree(&root), before);
    create(&owner, "owner");
    drop((rw, owner));
}

/// A close while a handoff is in flight on another thread (parked inside the
/// freeze, through the debug test seam) completes promptly and releases the
/// lock; the handoff, resumed, is refused before it builds or publishes.
/// Every wait is bounded.
#[cfg(debug_assertions)]
#[test]
fn close_during_an_in_flight_handoff_is_bounded() {
    use grafeo_engine::{FREEZE_STALL_BEFORE_CAPTURE, FREEZE_STALL_ENTERED};
    use std::sync::atomic::Ordering;
    use std::sync::mpsc::channel;
    use std::time::{Duration, Instant};
    const BOUND: Duration = Duration::from_secs(30);

    let (_dir, root) = fresh_root();
    let db = open(&root);
    create(&db, "written");
    FREEZE_STALL_BEFORE_CAPTURE.store(true, Ordering::Release);
    let (db, root) = (&db, &root);
    std::thread::scope(|scope| {
        let (frozen_tx, frozen_rx) = channel();
        let freezer = scope.spawn(move || {
            let handle = db.freeze_epoch_for_handoff(root);
            frozen_tx.send(()).ok();
            handle
        });
        let started = Instant::now();
        while !FREEZE_STALL_ENTERED.load(Ordering::Acquire) {
            assert!(started.elapsed() < BOUND, "the freeze never parked");
            std::thread::yield_now();
        }
        let (closed_tx, closed_rx) = channel();
        scope.spawn(move || closed_tx.send(db.close()).ok());
        let closed = closed_rx.recv_timeout(BOUND);
        // Unpark before asserting, so a failure cannot leave the freezer
        // parked.
        FREEZE_STALL_BEFORE_CAPTURE.store(false, Ordering::Release);
        closed
            .expect("close() returned while the handoff was in flight")
            .expect("close");
        let reopened = GrafeoDB::open_generation_root(root, true)
            .expect("the root reopens while the handoff is still in flight");
        assert_eq!(names(&reopened), ["base", "written"]);
        frozen_rx.recv_timeout(BOUND).expect("the freeze resumed");
        if let Ok(handle) = freezer.join().expect("freezer thread") {
            let err = db
                .complete_epoch_handoff(handle, generation_build_request(root, "g2"))
                .expect_err("completing a handoff of a closed database");
            assert_eq!(
                err.write_outcome(),
                Some(WriteOutcome::DatabaseClosed),
                "{err}"
            );
        }
        drop(reopened);
    });
}
