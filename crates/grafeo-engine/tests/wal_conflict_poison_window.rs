//! A commit conflict on a WAL poisoned while the commit was validating is
//! never reported as a retryable conflict (fork, #411 port review).
//!
//! `commit` checks that the WAL is writable before it validates. Another
//! session can poison the WAL after that check (its group failed to
//! append). If the commit then fails validation, a caller that retries
//! conflicts would retry against a WAL that refuses every write, so the
//! commit returns the WAL's refusal, which is not retryable.
//!
//! The test parks the loser's commit between its writability check and its
//! validation (a debug-only seam, process-global, so this file holds a
//! single test), poisons the WAL from another session, and releases it.
//!
//! ```bash
//! cargo test -p grafeo-engine --test wal_conflict_poison_window \
//!   --features generation,generation-streaming,compact-store,lpg,mmap,wal,gql,grafeo-file,testing-crash-injection
//! ```

#![cfg(all(
    debug_assertions,
    feature = "wal",
    feature = "lpg",
    feature = "gql",
    feature = "testing-crash-injection"
))]
#![allow(missing_docs)]

use std::sync::Barrier;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
use grafeo_common::utils::error::Error;
use grafeo_engine::{COMMIT_STALL_BEFORE_VALIDATION, COMMIT_STALL_PARKED, GrafeoDB};

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

/// The loser begins, the winner writes `vincent` and commits, the loser
/// writes `vincent` too and commits: its commit parks before validation,
/// another session's group fails to append (the WAL is poisoned), and the
/// commit is released. Returns the loser's commit error.
fn run(db: &GrafeoDB) -> Error {
    db.session()
        .execute("INSERT (:Person {name: 'vincent'})")
        .unwrap();
    let begun = Barrier::new(2);
    let winner_done = Barrier::new(2);
    std::thread::scope(|scope| {
        let loser = scope.spawn(|| {
            let mut loser = db.session();
            loser.begin_transaction().unwrap();
            begun.wait();
            winner_done.wait();
            loser
                .execute("MATCH (n:Person {name: 'vincent'}) SET n.age = 42")
                .expect("the WAL is writable when the loser writes");
            COMMIT_STALL_BEFORE_VALIDATION.store(true, Ordering::Release);
            let r = loser.commit();
            assert!(!loser.in_transaction(), "the transaction ended");
            r.expect_err("the second writer of vincent cannot commit")
        });
        begun.wait();
        let mut winner = db.session();
        winner.begin_transaction().unwrap();
        winner
            .execute("MATCH (n:Person {name: 'vincent'}) SET n.age = 41")
            .unwrap();
        winner.commit().unwrap();
        winner_done.wait();
        wait_until("the loser's commit to park", || {
            COMMIT_STALL_PARKED.load(Ordering::Acquire)
        });
        // Poison the WAL while the loser's commit is past its check.
        enable_io_failure_at(1);
        let r = db.session().execute("INSERT (:Person {name: 'poison'})");
        disable_io_failure();
        let err = r.expect_err("the implicit group fails to append");
        assert!(
            err.to_string().contains("durability unconfirmed"),
            "unexpected error: {err}"
        );
        COMMIT_STALL_PARKED.store(false, Ordering::Release);
        loser.join().unwrap()
    })
}

fn check(kind: &str, db: &GrafeoDB) {
    let err = run(db);
    assert!(
        !err.error_code().is_retryable(),
        "{kind}: a conflict on a poisoned WAL must not look retryable: {err}"
    );
    assert!(
        err.to_string().contains("WAL refuses"),
        "{kind}: the caller learns the WAL refuses writes: {err}"
    );
}

/// One test (the seam is process-global), covering each database kind in
/// turn.
#[test]
fn conflict_after_the_wal_check_returns_the_wal_refusal() {
    {
        let dir = tempfile::tempdir().unwrap();
        let db = GrafeoDB::with_config(
            grafeo_engine::Config::persistent(dir.path().join("dir-db"))
                .with_storage_format(grafeo_engine::config::StorageFormat::WalDirectory),
        )
        .unwrap();
        check("wal-directory", &db);
    }
    #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
    {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        let source = GrafeoDB::new_in_memory();
        source
            .create_node_with_props(
                &["Base"],
                [("name", grafeo_common::types::Value::from("base"))],
            )
            .unwrap();
        source
            .build_and_publish_generation(grafeo_engine::generation_build_request(&root, "g-base"))
            .unwrap();
        drop(source);
        let db = GrafeoDB::open_generation_root(&root, false).unwrap();
        check("generation-root", &db);
    }
}
