//! The byte cap on a transaction's buffered WAL records (fork follow-up to
//! the #411 port).
//!
//! A transaction's WAL records stay in memory until it commits. A write that
//! would take them past `Config::wal_transaction_buffer_cap` fails with a
//! retryable error, nothing of the transaction reaches the WAL, the
//! transaction can be rolled back, and a later small transaction commits.
//! The cap is a stopgap until transaction buffers are charged to a memory
//! ledger.
//!
//! ```bash
//! cargo test -p grafeo-engine --test wal_transaction_buffer_cap \
//!   --features generation,generation-streaming,compact-store,lpg,mmap,wal,gql,grafeo-file
//! ```

#![cfg(all(feature = "wal", feature = "lpg", feature = "gql"))]
#![allow(missing_docs)]

use grafeo_common::types::Value;
use grafeo_engine::{Config, GrafeoDB};

/// Small enough that a few hundred inserts exceed it, large enough for one.
const CAP: usize = 4 * 1024;

fn names(db: &GrafeoDB) -> Vec<String> {
    db.session()
        .execute("MATCH (n:Person) RETURN n.name ORDER BY n.name")
        .unwrap()
        .rows()
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.to_string(),
            other => panic!("unexpected name {other:?}"),
        })
        .collect()
}

fn assert_cap_error(err: &grafeo_common::utils::error::Error) {
    assert!(
        err.error_code().is_retryable(),
        "the cap error is retryable: {err}"
    );
    let message = err.to_string();
    assert!(
        message.contains(&format!("{CAP}-byte transaction WAL buffer cap")),
        "the error names the cap: {message}"
    );
}

/// Runs the cap scenario on `db` and returns the names that must survive.
fn run(db: &GrafeoDB) -> Vec<String> {
    let big = "x".repeat(256);

    // An explicit transaction: the statement that crosses the cap fails,
    // the transaction cannot commit, and it rolls back cleanly.
    let mut session = db.session();
    session.begin_transaction().unwrap();
    session
        .execute("INSERT (:Person {name: 'gone'})")
        .expect("under the cap");
    let mut refused = None;
    for i in 0..100 {
        if let Err(e) = session.execute(&format!("INSERT (:Bulk {{i: {i}, pad: '{big}'}})")) {
            refused = Some(e);
            break;
        }
    }
    let err = refused.expect("a statement over the cap fails");
    assert_cap_error(&err);
    let err = session
        .commit()
        .expect_err("a transaction whose records were refused cannot commit");
    assert_cap_error(&err);
    assert!(!session.in_transaction(), "the failed commit rolled back");

    // The same in an explicit transaction that is rolled back by the caller.
    session.begin_transaction().unwrap();
    for i in 0..100 {
        if session
            .execute(&format!("INSERT (:Bulk {{i: {i}, pad: '{big}'}})"))
            .is_err()
        {
            break;
        }
    }
    session.rollback().expect("rollback after the cap");

    // An auto-commit statement over the cap fails and leaves nothing.
    let rows: Vec<String> = (0..100)
        .map(|i| format!("(:Bulk {{i: {i}, pad: '{big}'}})"))
        .collect();
    let err = db
        .session()
        .execute(&format!("INSERT {}", rows.join(", ")))
        .expect_err("one statement over the cap");
    assert_cap_error(&err);

    let count = db
        .session()
        .execute("MATCH (n:Bulk) RETURN count(n)")
        .unwrap()
        .rows()[0][0]
        .clone();
    assert_eq!(count, Value::Int64(0), "nothing over the cap is applied");

    // A small transaction afterwards commits.
    session.begin_transaction().unwrap();
    session.execute("INSERT (:Person {name: 'kept'})").unwrap();
    session.commit().expect("a small transaction commits");
    vec!["kept".to_string()]
}

fn check(open: impl Fn() -> GrafeoDB) {
    let expected = {
        let db = open();
        let expected = run(&db);
        assert_eq!(names(&db), expected, "live");
        db.close().unwrap();
        expected
    };
    let db = open();
    assert_eq!(names(&db), expected, "after reopen");
}

#[cfg(feature = "grafeo-file")]
#[test]
fn cap_on_a_single_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.grafeo");
    check(|| {
        GrafeoDB::with_config(Config::persistent(&path).with_wal_transaction_buffer_cap(CAP))
            .unwrap()
    });
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

#[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
#[test]
fn cap_on_a_generation_root() {
    let dir = tempfile::tempdir().unwrap();
    let root = publish_root(dir.path());
    check(|| {
        GrafeoDB::open_generation_root_with_config(
            Config::persistent(&root).with_wal_transaction_buffer_cap(CAP),
        )
        .unwrap()
    });
}

/// The default cap is on and large.
#[test]
fn default_cap_is_512_mib() {
    assert_eq!(
        Config::default().wal_transaction_buffer_cap,
        Some(512 * 1024 * 1024)
    );
}
