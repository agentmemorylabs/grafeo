//! WAL groups reach the log in commit order (fork adaptation of the #411
//! port).
//!
//! A transaction's records are only written at commit, after the commit is
//! applied in memory. A second transaction that sees the first one's write
//! and overwrites it must not have its group written before the first
//! one's, or replay applies the two writes in the wrong order and the older
//! value wins after reopen.
//!
//! The test parks the first commit right before its group write (a
//! debug-only seam, process-global, so this file holds a single test) and
//! commits the second transaction meanwhile.
//!
//! ```bash
//! cargo test -p grafeo-engine --test wal_commit_order \
//!   --features generation,generation-streaming,compact-store,lpg,mmap,wal,cypher,grafeo-file
//! ```

#![cfg(all(debug_assertions, feature = "wal", feature = "lpg", feature = "gql"))]
#![allow(missing_docs)]

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use grafeo_common::types::Value;
use grafeo_engine::{COMMIT_STALL_BEFORE_GROUP, COMMIT_STALL_PARKED, GrafeoDB};

fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let start = Instant::now();
    while !done() {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn value(db: &GrafeoDB) -> Value {
    db.session()
        .execute("MATCH (n:Person {name: 'n'}) RETURN n.v")
        .unwrap()
        .rows()[0][0]
        .clone()
}

/// `first` sets `n.v` and parks before its group write; `second`, begun
/// after that commit, overwrites `n.v` and commits; then `first` is
/// released.
fn run(db: &GrafeoDB) {
    db.session()
        .execute("INSERT (:Person {name: 'n', v: 'init'})")
        .unwrap();
    std::thread::scope(|scope| {
        COMMIT_STALL_BEFORE_GROUP.store(true, Ordering::Release);
        let first = scope.spawn(|| {
            let mut session = db.session();
            session.begin_transaction().unwrap();
            session
                .execute("MATCH (n:Person {name: 'n'}) SET n.v = 'first'")
                .unwrap();
            session.commit().unwrap();
        });
        wait_until("the first commit to park", || {
            COMMIT_STALL_PARKED.load(Ordering::Acquire)
        });
        // The first transaction is committed in memory: the second one sees
        // its value and overwrites it without a conflict.
        let second = scope.spawn(|| {
            let mut session = db.session();
            session.begin_transaction().unwrap();
            session
                .execute("MATCH (n:Person {name: 'n', v: 'first'}) SET n.v = 'second'")
                .unwrap();
            session.commit().unwrap();
        });
        // Give the second commit time to write its group, if it can.
        std::thread::sleep(Duration::from_millis(300));
        COMMIT_STALL_PARKED.store(false, Ordering::Release);
        first.join().unwrap();
        second.join().unwrap();
    });
    assert_eq!(value(db), Value::from("second"), "live value");
}

fn check_reopen(open: impl Fn() -> GrafeoDB) {
    let db = open();
    run(&db);
    db.close().unwrap();
    drop(db);
    let db = open();
    assert_eq!(
        value(&db),
        Value::from("second"),
        "after reopen the later commit's value wins"
    );
}

#[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
fn publish_root(dir: &std::path::Path) -> std::path::PathBuf {
    let root = dir.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let source = GrafeoDB::new_in_memory();
    source
        .create_node_with_props(&["Base"], [("name", Value::from("base"))])
        .unwrap();
    source
        .build_and_publish_generation(grafeo_engine::generation_build_request(&root, "g-base"))
        .unwrap();
    root
}

/// One test (the seam is process-global), covering every database kind in
/// turn.
#[test]
fn groups_are_written_in_commit_order() {
    #[cfg(feature = "grafeo-file")]
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.grafeo");
        check_reopen(|| GrafeoDB::open(&path).unwrap());
    }
    #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
    {
        let dir = tempfile::tempdir().unwrap();
        let root = publish_root(dir.path());
        check_reopen(|| GrafeoDB::open_generation_root(&root, false).unwrap());
    }
}
