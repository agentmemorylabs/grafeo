//! SPARQL updates report a WAL group they could not write, and the
//! transaction WAL buffer cap, like LPG writes (fork, #411 port review).
//!
//! Covers the session RDF paths (`Session::execute_sparql`) inside and
//! outside a transaction, and the database-level `GrafeoDB::execute_sparql`.
//! The failure scenarios run in a child process that exits without
//! `close()`, and the reopen shows what reached the WAL.
//!
//! ```bash
//! cargo test -p grafeo-engine --test wal_rdf_group_failures \
//!   --features lpg,wal,grafeo-file,triple-store,sparql,testing-crash-injection
//! ```

#![cfg(all(
    feature = "wal",
    feature = "triple-store",
    feature = "sparql",
    feature = "testing-crash-injection"
))]
#![allow(missing_docs)]

use std::path::{Path, PathBuf};

use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
use grafeo_common::types::Value;
use grafeo_common::utils::error::Error;
use grafeo_engine::config::StorageFormat;
use grafeo_engine::{Config, GrafeoDB, GraphModel};

const SCENARIO_VAR: &str = "GRAFEO_WAL_RDF_FAILURE_SCENARIO";
const PATH_VAR: &str = "GRAFEO_WAL_RDF_FAILURE_PATH";
const FORMAT_VAR: &str = "GRAFEO_WAL_RDF_FAILURE_FORMAT";

/// A small transaction WAL buffer cap for the cap scenarios.
const SMALL_CAP: usize = 4 * 1024;

/// Serializes the tests of this binary: a forked child briefly holds copies
/// of the parent's open file descriptors, and the I/O failure injection is
/// process-global.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn formats(dir: &Path) -> Vec<(&'static str, PathBuf)> {
    let mut formats = vec![("wal-directory", dir.join("dir-db"))];
    #[cfg(feature = "grafeo-file")]
    formats.push(("single-file", dir.join("single.grafeo")));
    formats
}

fn open(path: &Path, format: &str, cap: Option<usize>) -> GrafeoDB {
    let storage = match format {
        "wal-directory" => StorageFormat::WalDirectory,
        "single-file" => StorageFormat::SingleFile,
        other => panic!("unknown format {other}"),
    };
    let mut config = Config::persistent(path)
        .with_storage_format(storage)
        .with_graph_model(GraphModel::Rdf);
    if let Some(bytes) = cap {
        config = config.with_wal_transaction_buffer_cap(bytes);
    }
    GrafeoDB::with_config(config).unwrap()
}

fn insert_data(subject: &str, object: &str) -> String {
    format!(r#"INSERT DATA {{ <http://ex.org/{subject}> <http://ex.org/p> "{object}" . }}"#)
}

/// Sorted subjects of every `<p>` triple.
fn subjects(db: &GrafeoDB) -> Vec<String> {
    let result = db
        .session()
        .execute_sparql("SELECT ?s WHERE { ?s <http://ex.org/p> ?o } ORDER BY ?s")
        .unwrap();
    result
        .rows()
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.to_string(),
            other => format!("{other:?}"),
        })
        .collect()
}

fn ex(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| format!("http://ex.org/{n}")).collect()
}

fn assert_unconfirmed(err: &Error) {
    assert!(
        err.to_string().contains("durability unconfirmed"),
        "unexpected error: {err}"
    );
}

fn assert_cap_error(err: &Error) {
    assert!(err.error_code().is_retryable(), "{err}");
    assert!(
        err.to_string().contains("transaction WAL buffer cap"),
        "not the cap error: {err}"
    );
}

/// The cap a scenario's database is opened with.
fn scenario_cap(scenario: &str) -> Option<usize> {
    scenario.starts_with("cap_").then_some(SMALL_CAP)
}

/// Runs `scenario`; returns the subjects that must survive a crash.
fn run_scenario(scenario: &str, db: &GrafeoDB) -> Vec<String> {
    match scenario {
        // A session SPARQL update outside a transaction whose implicit group
        // cannot be written.
        "session_failure" => {
            db.session()
                .execute_sparql(&insert_data("alix", "1"))
                .unwrap();
            let session = db.session();
            enable_io_failure_at(1);
            let r = session.execute_sparql(&insert_data("lost", "2"));
            disable_io_failure();
            assert_unconfirmed(&r.expect_err("a lost group is not Ok"));
            ex(&["alix"])
        }
        // A database-level SPARQL update whose group cannot be written.
        "db_failure" => {
            db.execute_sparql(&insert_data("alix", "1")).unwrap();
            enable_io_failure_at(1);
            let r = db.execute_sparql(&insert_data("lost", "2"));
            disable_io_failure();
            assert_unconfirmed(&r.expect_err("a lost group is not Ok"));
            ex(&["alix"])
        }
        // A session SPARQL update inside a transaction that goes over the
        // cap fails, the transaction cannot commit, and a rollback recovers
        // the session.
        "cap_session_transaction" => {
            db.session()
                .execute_sparql(&insert_data("alix", "1"))
                .unwrap();
            let mut s = db.session();
            s.begin_transaction().unwrap();
            s.execute_sparql(&insert_data("gus", "2")).unwrap();
            let err = s
                .execute_sparql(&insert_data("big", &"x".repeat(2 * SMALL_CAP)))
                .expect_err("over the cap");
            assert_cap_error(&err);
            let err = s.commit().expect_err("the transaction cannot commit");
            assert_cap_error(&err);
            assert!(!s.in_transaction());
            s.begin_transaction().unwrap();
            s.execute_sparql(&insert_data("mia", "3")).unwrap();
            s.commit().expect("a small transaction commits afterwards");
            ex(&["alix", "mia"])
        }
        // A database-level SPARQL update over the cap is refused with the
        // cap's error; nothing of it reaches the WAL.
        "cap_db" => {
            db.execute_sparql(&insert_data("alix", "1")).unwrap();
            let err = db
                .execute_sparql(&insert_data("big", &"x".repeat(2 * SMALL_CAP)))
                .expect_err("over the cap");
            assert_cap_error(&err);
            ex(&["alix"])
        }
        other => panic!("unknown scenario {other}"),
    }
}

/// Child-process entry for [`crash_after`]; a no-op when run directly.
#[test]
fn crash_child() {
    let Ok(scenario) = std::env::var(SCENARIO_VAR) else {
        return;
    };
    let path = PathBuf::from(std::env::var_os(PATH_VAR).unwrap());
    let format = std::env::var(FORMAT_VAR).unwrap();
    let db = open(&path, &format, scenario_cap(&scenario));
    run_scenario(&scenario, &db);
    // Crash: no close(), no destructors.
    std::process::exit(0);
}

fn crash_after(scenario: &str, path: &Path, format: &str) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--nocapture"])
        .env(SCENARIO_VAR, scenario)
        .env(PATH_VAR, path)
        .env(FORMAT_VAR, format)
        .status()
        .unwrap();
    assert!(status.success(), "{format}: scenario {scenario} failed");
}

/// Runs `scenario` live (checking its errors and the live subjects), then
/// in a crashing child, and checks the reopen.
fn check(scenario: &str) {
    let _serial = serial();
    let dir = tempfile::tempdir().unwrap();
    for (format, path) in formats(dir.path()) {
        let ref_dir = tempfile::tempdir().unwrap();
        let ref_path = ref_dir.path().join(path.file_name().unwrap());
        let db = open(&ref_path, format, scenario_cap(scenario));
        let expected = run_scenario(scenario, &db);
        drop(db);
        crash_after(scenario, &path, format);
        let db = open(&path, format, None);
        assert_eq!(
            subjects(&db),
            expected,
            "{format} {scenario}: after a crash"
        );
    }
}

#[test]
fn session_sparql_failure_is_reported() {
    check("session_failure");
}

#[test]
fn database_sparql_failure_is_reported() {
    check("db_failure");
}

#[test]
fn session_sparql_over_the_cap_in_a_transaction() {
    check("cap_session_transaction");
}

#[test]
fn database_sparql_over_the_cap() {
    check("cap_db");
}
