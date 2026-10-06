//! Interleaved transactions must replay to exactly the committed writes, on
//! a plain `.grafeo` file and on a generation root (fork port of #411).
//!
//! WAL records carry no transaction id. Before the transaction groups of
//! #411, sessions appended their records as they wrote them, so records of
//! concurrent transactions interleaved in the log and replay settled them
//! by position: a commit marker committed every record before it, and an
//! abort marker discarded every record before it. These tests interleave
//! two sessions where one rolls back, or fails commit validation, then
//! close (or crash) and reopen, and check which writes survived.
//!
//! The crash variants run the scenario in a child process that exits
//! without `close()` and without destructors.
//!
//! The old-format tests write the record shapes the pre-port code wrote
//! (records appended as they happen, bare commit/epoch pairs, abort
//! markers, a log left inside a named graph) and check the new code still
//! replays them.
//!
//! ```bash
//! cargo test -p grafeo-engine --test wal_interleaved_transactions \
//!   --features generation,generation-streaming,compact-store,lpg,mmap,wal,cypher,grafeo-file
//! ```

#![cfg(all(
    feature = "wal",
    feature = "lpg",
    feature = "gql",
    any(
        feature = "grafeo-file",
        all(feature = "generation", feature = "compact-store", feature = "mmap")
    )
))]
#![allow(missing_docs)]

use std::path::{Path, PathBuf};

use grafeo_common::types::Value;
use grafeo_engine::session::Session;
use grafeo_engine::{Config, GrafeoDB};

/// Serializes the tests of this binary. The crash tests fork child
/// processes, and a child briefly holds copies of the parent's open file
/// descriptors (between fork and exec), including another test's root lock,
/// which then fails a concurrent open with "root already locked".
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

const SCENARIO_VAR: &str = "GRAFEO_WAL_INTERLEAVE_SCENARIO";
const PATH_VAR: &str = "GRAFEO_WAL_INTERLEAVE_PATH";
const KIND_VAR: &str = "GRAFEO_WAL_INTERLEAVE_KIND";

/// Which kind of database a scenario runs on.
#[derive(Clone, Copy, Debug)]
enum Kind {
    /// A WAL-directory database (the WAL is the only copy of the data).
    WalDirectory,
    /// A single `.grafeo` file (sidecar WAL while open).
    #[cfg(feature = "grafeo-file")]
    SingleFile,
    /// A writable generation root (layered store, root WAL).
    #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
    GenerationRoot,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::WalDirectory => "wal-directory",
            #[cfg(feature = "grafeo-file")]
            Kind::SingleFile => "single-file",
            #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
            Kind::GenerationRoot => "generation-root",
        }
    }

    fn from_name(name: &str) -> Self {
        match name {
            "wal-directory" => Kind::WalDirectory,
            #[cfg(feature = "grafeo-file")]
            "single-file" => Kind::SingleFile,
            #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
            "generation-root" => Kind::GenerationRoot,
            other => panic!("unknown kind {other}"),
        }
    }

    /// Creates the database at `path` (a generation root gets an empty
    /// published base generation) and returns the path to open.
    fn create(self, dir: &Path) -> PathBuf {
        match self {
            Kind::WalDirectory => dir.join("dir-db"),
            #[cfg(feature = "grafeo-file")]
            Kind::SingleFile => dir.join("db.grafeo"),
            #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
            Kind::GenerationRoot => {
                let root = dir.join("root");
                std::fs::create_dir_all(&root).unwrap();
                let source = GrafeoDB::new_in_memory();
                source
                    .create_node_with_props(&["Base"], [("name", Value::from("base"))])
                    .unwrap();
                source
                    .build_and_publish_generation(grafeo_engine::generation_build_request(
                        &root, "g-base",
                    ))
                    .unwrap();
                root
            }
        }
    }

    fn open(self, path: &Path) -> GrafeoDB {
        self.open_with_cap(path, None)
    }

    /// Opens with a transaction WAL buffer cap of `cap` bytes (`None`: the
    /// default cap).
    fn open_with_cap(self, path: &Path, cap: Option<usize>) -> GrafeoDB {
        let with_cap = |config: Config| match cap {
            Some(bytes) => config.with_wal_transaction_buffer_cap(bytes),
            None => config,
        };
        match self {
            Kind::WalDirectory => GrafeoDB::with_config(with_cap(
                Config::persistent(path)
                    .with_storage_format(grafeo_engine::config::StorageFormat::WalDirectory),
            ))
            .unwrap(),
            #[cfg(feature = "grafeo-file")]
            Kind::SingleFile => GrafeoDB::with_config(with_cap(Config::persistent(path))).unwrap(),
            #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
            Kind::GenerationRoot => {
                GrafeoDB::open_generation_root_with_config(with_cap(Config::persistent(path)))
                    .unwrap()
            }
        }
    }
}

/// The kinds compiled in (one or both, by feature).
#[allow(unused_mut, clippy::vec_init_then_push)]
fn kinds() -> Vec<Kind> {
    let mut kinds = vec![Kind::WalDirectory];
    #[cfg(feature = "grafeo-file")]
    kinds.push(Kind::SingleFile);
    #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
    kinds.push(Kind::GenerationRoot);
    kinds
}

/// Sorted `n.name` of every `:Person`.
fn names_in(session: &Session) -> Vec<String> {
    let result = session
        .execute("MATCH (n:Person) RETURN n.name ORDER BY n.name")
        .unwrap();
    result
        .rows()
        .iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.to_string(),
            other => panic!("unexpected name {other:?}"),
        })
        .collect()
}

fn names(db: &GrafeoDB) -> Vec<String> {
    names_in(&db.session())
}

fn insert(session: &Session, name: &str) {
    session
        .execute(&format!("INSERT (:Person {{name: '{name}'}})"))
        .unwrap();
}

fn strings(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| (*s).to_string()).collect()
}

// ----------------------------------------------------------------------
// Scenarios. Each returns the names that must survive a reopen.
// ----------------------------------------------------------------------

fn run_scenario(scenario: &str, db: &GrafeoDB) -> Vec<String> {
    match scenario {
        // B rolls back between A's write and A's commit: B's abort must not
        // discard A's committed record.
        "rollback_between" => {
            let mut a = db.session();
            let mut b = db.session();
            a.begin_transaction().unwrap();
            insert(&a, "alix");
            b.begin_transaction().unwrap();
            insert(&b, "gus");
            b.rollback().unwrap();
            a.commit().unwrap();
            strings(&["alix"])
        }
        // The mirror: B commits while A is open, then A rolls back. B's
        // commit marker must not commit A's record.
        "commit_between" => {
            let mut a = db.session();
            let mut b = db.session();
            a.begin_transaction().unwrap();
            insert(&a, "alix");
            b.begin_transaction().unwrap();
            insert(&b, "gus");
            b.commit().unwrap();
            a.rollback().unwrap();
            strings(&["gus"])
        }
        // The loser writes, the winner writes the same node and commits, the
        // loser's commit fails validation. The winner's commit marker must
        // not settle the loser's earlier records.
        "failed_commit" => {
            insert(&db.session(), "vincent");
            let mut loser = db.session();
            let mut winner = db.session();
            loser.begin_transaction().unwrap();
            winner.begin_transaction().unwrap();
            insert(&loser, "jules");
            winner
                .execute("MATCH (n:Person {name: 'vincent'}) SET n.age = 41")
                .unwrap();
            insert(&winner, "mia");
            winner.commit().unwrap();
            loser
                .execute("MATCH (n:Person {name: 'vincent'}) SET n.age = 42")
                .unwrap();
            let err = loser.commit().expect_err("second writer of vincent");
            assert!(
                err.to_string().to_lowercase().contains("conflict"),
                "unexpected error: {err}"
            );
            // A later commit in the same session must not settle them either.
            insert(&loser, "django");
            strings(&["django", "mia", "vincent"])
        }
        // Writes through the `GrafeoDB`-level APIs, outside any session
        // (AMH's bulk ingest uses `batch_create_nodes_with_props`). Each call
        // must be durable on its own, without a later commit marker.
        "db_level_writes" => {
            let props = |name: &str| {
                std::collections::HashMap::from([(
                    grafeo_common::types::PropertyKey::new("name"),
                    Value::from(name),
                )])
            };
            let ids = db.batch_create_nodes_with_props("Person", vec![props("alix"), props("gus")]);
            assert_eq!(ids.len(), 2);
            db.create_node_with_props(&["Person"], [("name", Value::from("vincent"))])
                .unwrap();
            let jules = db
                .create_node_with_props(&["Other"], [("name", Value::from("jules"))])
                .unwrap();
            assert!(db.add_node_label(jules, "Person"));
            strings(&["alix", "gus", "jules", "vincent"])
        }
        // A commit whose WAL group cannot be written: the commit reports
        // durability unconfirmed, the WAL is poisoned, and the transaction
        // never reached the disk.
        #[cfg(feature = "testing-crash-injection")]
        "failed_group" => {
            use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
            insert(&db.session(), "alix");
            let mut s = db.session();
            s.begin_transaction().unwrap();
            insert(&s, "gus");
            // The transaction's group is the next WAL append.
            enable_io_failure_at(1);
            let r = s.commit();
            disable_io_failure();
            let err = r.expect_err("a lost WAL group must not report success");
            assert!(
                err.to_string().contains("durability unconfirmed"),
                "unexpected error: {err}"
            );
            let err = db
                .session()
                .execute("INSERT (:Person {name: 'refused'})")
                .expect_err("the WAL is poisoned");
            assert!(err.to_string().contains("WAL refuses"), "{err}");
            strings(&["alix"])
        }
        // A savepoint taken after a refused (over-cap) write would let a
        // rollback to it clear the fault while the refused write stays in
        // the transaction: it is refused, and so is the commit.
        "cap_savepoint_after_failure" => {
            let alix = db
                .session()
                .create_node_with_props(&["Person"], [("name", Value::from("alix"))])
                .unwrap();
            let mut s = db.session();
            s.begin_transaction().unwrap();
            let err = s
                .set_node_property(alix, "name", Value::from(big()))
                .expect_err("over the cap");
            assert_cap_error(&err);
            let err = s
                .savepoint("after_failure")
                .expect_err("no savepoint after a refused write");
            assert_cap_error(&err);
            let err = s.commit().expect_err("the transaction cannot commit");
            assert_cap_error(&err);
            assert!(!s.in_transaction());
            strings(&["alix"])
        }
        // A savepoint taken before the refused write still recovers the
        // transaction: rolling back to it undoes the write and the fault.
        "cap_savepoint_before_failure" => {
            let alix = db
                .session()
                .create_node_with_props(&["Person"], [("name", Value::from("alix"))])
                .unwrap();
            let mut s = db.session();
            s.begin_transaction().unwrap();
            insert(&s, "gus");
            s.savepoint("before").unwrap();
            let err = s
                .set_node_property(alix, "name", Value::from(big()))
                .expect_err("over the cap");
            assert_cap_error(&err);
            s.rollback_to_savepoint("before").unwrap();
            s.commit().expect("commits without the refused write");
            strings(&["alix", "gus"])
        }
        // The session's create-with-properties APIs report the cap like
        // every other write.
        "cap_direct_create_with_props" => {
            let mut s = db.session();
            s.begin_transaction().unwrap();
            let err = s
                .create_node_with_props(&["Person"], [("name", Value::from(big()))])
                .expect_err("node over the cap");
            assert_cap_error(&err);
            s.rollback().unwrap();
            s.begin_transaction().unwrap();
            let a = s
                .create_node_with_props(&["Thing"], [("k", Value::from(1_i64))])
                .unwrap();
            let err = s
                .create_edge_with_props(a, a, "SELF", [("pad", Value::from(big()))])
                .expect_err("edge over the cap");
            assert_cap_error(&err);
            s.rollback().unwrap();
            insert(&db.session(), "mia");
            strings(&["mia"])
        }
        // A write outside a transaction whose implicit group cannot be
        // written reports it (a direct write, then a query with auto-commit
        // off), instead of returning success.
        #[cfg(feature = "testing-crash-injection")]
        "implicit_group_failure" => {
            use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
            let alix = db
                .session()
                .create_node_with_props(&["Person"], [("name", Value::from("alix"))])
                .unwrap();
            enable_io_failure_at(1);
            let r = db
                .session()
                .set_node_property(alix, "name", Value::from("lost"));
            disable_io_failure();
            assert_unconfirmed(&r.expect_err("a lost implicit group is not Ok"));
            strings(&["alix"])
        }
        #[cfg(feature = "testing-crash-injection")]
        "implicit_query_failure" => {
            use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
            insert(&db.session(), "alix");
            let mut s = db.session();
            s.set_auto_commit(false);
            enable_io_failure_at(1);
            let r = s.execute("INSERT (:Person {name: 'lost'})");
            disable_io_failure();
            assert_unconfirmed(&r.expect_err("a lost implicit group is not Ok"));
            strings(&["alix"])
        }
        // The checked batch API reports a lost group; nothing reaches the
        // disk.
        #[cfg(feature = "testing-crash-injection")]
        "checked_batch_failure" => {
            use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
            insert(&db.session(), "alix");
            let props = |name: &str| {
                std::collections::HashMap::from([(
                    grafeo_common::types::PropertyKey::new("name"),
                    Value::from(name),
                )])
            };
            enable_io_failure_at(1);
            let r = db.try_batch_create_nodes_with_props("Person", vec![props("gus")]);
            disable_io_failure();
            assert_unconfirmed(&r.expect_err("a lost batch group is not Ok"));
            strings(&["alix"])
        }
        // Once the WAL is poisoned, every write API refuses before it
        // mutates anything, including the ones without an error channel.
        #[cfg(feature = "testing-crash-injection")]
        "writes_after_poison" => {
            use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
            let alix = db
                .create_node_with_props(&["Person"], [("name", Value::from("alix"))])
                .unwrap();
            let mut s = db.session();
            s.begin_transaction().unwrap();
            insert(&s, "gus");
            enable_io_failure_at(1);
            let r = s.commit();
            disable_io_failure();
            assert_unconfirmed(&r.expect_err("poisons the WAL"));
            // Applied in memory (durability unconfirmed); not on disk.
            let refused = |err: grafeo_common::utils::error::Error| {
                assert!(err.to_string().contains("WAL refuses"), "{err}");
            };
            let session = db.session();
            refused(
                session
                    .create_node_with_props(&["Person"], [("name", Value::from("s"))])
                    .expect_err("session create"),
            );
            refused(
                session
                    .create_edge_with_props(alix, alix, "E", [("k", Value::from(1_i64))])
                    .expect_err("session edge create"),
            );
            refused(db.create_node(&["Person"]).expect_err("db create_node"));
            refused(
                db.create_node_with_props(&["Person"], [("name", Value::from("d"))])
                    .expect_err("db create"),
            );
            assert!(!db.add_node_label(alix, "Extra"), "label refused");
            assert!(
                !db.remove_node_property(alix, "name"),
                "property removal refused"
            );
            let count = |q: &str| match &db.session().execute(q).unwrap().rows()[0][0] {
                Value::Int64(n) => *n,
                other => panic!("{other:?}"),
            };
            assert_eq!(
                count("MATCH (n:Extra) RETURN count(n)"),
                0,
                "no label added"
            );
            assert_eq!(
                count("MATCH (n:Person {name: 'alix'}) RETURN count(n)"),
                1,
                "the name is still there"
            );
            strings(&["alix"])
        }
        // A schema command whose implicit group cannot be written reports
        // it, and once the WAL is poisoned schema commands are refused
        // before they change anything. `CREATE GRAPH` is a session command;
        // `CREATE SCHEMA` goes through the schema DDL path.
        #[cfg(feature = "testing-crash-injection")]
        "schema_failure" => {
            use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
            insert(&db.session(), "alix");
            let session = db.session();
            enable_io_failure_at(1);
            let r = session.execute("CREATE GRAPH lost");
            disable_io_failure();
            assert_unconfirmed(&r.expect_err("a lost schema group is not Ok"));
            assert_schema_refused(db, &session);
            strings(&["alix"])
        }
        #[cfg(feature = "testing-crash-injection")]
        "schema_ddl_failure" => {
            use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
            insert(&db.session(), "alix");
            let session = db.session();
            enable_io_failure_at(1);
            let r = session.execute("CREATE SCHEMA lost_schema");
            disable_io_failure();
            assert_unconfirmed(&r.expect_err("a lost schema group is not Ok"));
            assert_schema_refused(db, &session);
            strings(&["alix"])
        }
        // A nested begin after a refused (over-cap) write is refused without
        // adding a nesting level: one rollback ends the whole transaction
        // and releases what it wrote.
        "cap_nested_begin" => {
            let alix = db
                .session()
                .create_node_with_props(&["Person"], [("name", Value::from("alix"))])
                .unwrap();
            let base_before = count(db, "MATCH (n:Base) RETURN count(n)");
            let mut s = db.session();
            s.begin_transaction().unwrap();
            insert(&s, "gus");
            s.execute("MATCH (n:Base) DETACH DELETE n").unwrap();
            let err = s
                .set_node_property(alix, "name", Value::from(big()))
                .expect_err("over the cap");
            assert_cap_error(&err);
            let err = s.begin_transaction().expect_err("nested begin refused");
            assert_cap_error(&err);
            s.rollback().expect("one rollback ends the transaction");
            assert!(!s.in_transaction(), "no phantom nesting level");
            assert_released(db, base_before);
            strings(&["alix"])
        }
        // The same, with the session dropped right after the refused nested
        // begin: the drop's rollback aborts the transaction fully.
        "cap_nested_begin_drop" => {
            let alix = db
                .session()
                .create_node_with_props(&["Person"], [("name", Value::from("alix"))])
                .unwrap();
            let base_before = count(db, "MATCH (n:Base) RETURN count(n)");
            let mut s = db.session();
            s.begin_transaction().unwrap();
            insert(&s, "gus");
            s.execute("MATCH (n:Base) DETACH DELETE n").unwrap();
            s.execute("MATCH (n:Person {name: 'alix'}) SET n.age = 1")
                .unwrap();
            let err = s
                .set_node_property(alix, "name", Value::from(big()))
                .expect_err("over the cap");
            assert_cap_error(&err);
            let err = s.begin_transaction().expect_err("nested begin refused");
            assert_cap_error(&err);
            drop(s);
            assert_released(db, base_before);
            strings(&["alix"])
        }
        other => panic!("unknown scenario {other}"),
    }
}

/// `n` from a `RETURN count(...)` query.
fn count(db: &GrafeoDB, query: &str) -> i64 {
    match &db.session().execute(query).unwrap().rows()[0][0] {
        Value::Int64(n) => *n,
        other => panic!("{other:?}"),
    }
}

/// After an aborted transaction that deleted the `:Base` nodes and wrote
/// `alix`: the live state is back (the base nodes are visible again), and
/// `alix` is released, so another transaction can write it and commit.
fn assert_released(db: &GrafeoDB, base_before: i64) {
    assert_eq!(
        count(db, "MATCH (n:Base) RETURN count(n)"),
        base_before,
        "the deleted base nodes are back"
    );
    let mut other = db.session();
    other.begin_transaction().unwrap();
    other
        .execute("MATCH (n:Person {name: 'alix'}) SET n.age = 2")
        .expect("alix is released");
    other
        .commit()
        .expect("no conflict with the aborted transaction");
}

/// Once the WAL is poisoned, `CREATE GRAPH` and `CREATE SCHEMA` are refused
/// and create nothing.
#[cfg(feature = "testing-crash-injection")]
fn assert_schema_refused(db: &GrafeoDB, session: &Session) {
    for ddl in ["CREATE GRAPH refused", "CREATE SCHEMA refused_schema"] {
        let err = session.execute(ddl).expect_err(ddl);
        assert!(err.to_string().contains("WAL refuses"), "{ddl}: {err}");
    }
    let graphs = db.list_graphs();
    assert!(
        !graphs.iter().any(|g| g.starts_with("refused")),
        "nothing was created: {graphs:?}"
    );
}

/// A small transaction WAL buffer cap for the scenarios that test it.
const SMALL_CAP: usize = 4 * 1024;

/// The cap a scenario's database is opened with.
fn scenario_cap(scenario: &str) -> Option<usize> {
    scenario.starts_with("cap_").then_some(SMALL_CAP)
}

/// A string well over [`SMALL_CAP`].
fn big() -> String {
    "x".repeat(2 * SMALL_CAP)
}

/// Asserts `err` is the cap's retryable error.
fn assert_cap_error(err: &grafeo_common::utils::error::Error) {
    assert!(err.error_code().is_retryable(), "{err}");
    assert!(
        err.to_string().contains("transaction WAL buffer cap"),
        "not the cap error: {err}"
    );
}

/// Asserts `err` reports a lost WAL group (not success, not a retry).
#[cfg(feature = "testing-crash-injection")]
fn assert_unconfirmed(err: &grafeo_common::utils::error::Error) {
    assert!(
        err.to_string().contains("durability unconfirmed"),
        "unexpected error: {err}"
    );
}

/// Child-process entry for [`crash_after`]; a no-op when run directly.
#[test]
fn crash_child() {
    let Ok(scenario) = std::env::var(SCENARIO_VAR) else {
        return;
    };
    let path = PathBuf::from(std::env::var_os(PATH_VAR).unwrap());
    let kind = Kind::from_name(&std::env::var(KIND_VAR).unwrap());
    let db = kind.open_with_cap(&path, scenario_cap(&scenario));
    run_scenario(&scenario, &db);
    // Crash: no close(), no destructors.
    std::process::exit(0);
}

/// Runs `scenario` in a child process that exits without closing the
/// database, like a crash.
fn crash_after(scenario: &str, path: &Path, kind: Kind) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--nocapture"])
        .env(SCENARIO_VAR, scenario)
        .env(PATH_VAR, path)
        .env(KIND_VAR, kind.name())
        .status()
        .unwrap();
    assert!(status.success(), "{kind:?}: scenario {scenario} failed");
}

/// Runs `scenario`, closes, reopens, and checks the survivors (live state is
/// checked too, before the close).
fn check_close_reopen(scenario: &str) {
    for kind in kinds() {
        let dir = tempfile::tempdir().unwrap();
        let path = kind.create(dir.path());
        let expected = {
            let db = kind.open(&path);
            let expected = run_scenario(scenario, &db);
            assert_eq!(names(&db), expected, "{kind:?} {scenario}: live state");
            db.close().unwrap();
            expected
        };
        let db = kind.open(&path);
        assert_eq!(
            names(&db),
            expected,
            "{kind:?} {scenario}: after close + reopen"
        );
        // A second clean cycle must not change anything either.
        db.close().unwrap();
        drop(db);
        assert_eq!(
            names(&kind.open(&path)),
            expected,
            "{kind:?} {scenario}: after a second reopen"
        );
    }
}

/// Runs `scenario` in a crashing child, reopens, and checks the survivors.
fn check_crash_reopen(scenario: &str) {
    for kind in kinds() {
        let dir = tempfile::tempdir().unwrap();
        let path = kind.create(dir.path());
        let expected = {
            // The expected survivors, from a run that is not crashed.
            let ref_dir = tempfile::tempdir().unwrap();
            let ref_path = kind.create(ref_dir.path());
            let db = kind.open_with_cap(&ref_path, scenario_cap(scenario));
            run_scenario(scenario, &db)
        };
        crash_after(scenario, &path, kind);
        let db = kind.open(&path);
        assert_eq!(names(&db), expected, "{kind:?} {scenario}: after a crash");
        // A commit after recovery must not pick up anything left over.
        insert(&db.session(), "after");
        db.close().unwrap();
        drop(db);
        let mut expected = expected;
        expected.push("after".to_string());
        expected.sort();
        assert_eq!(
            names(&kind.open(&path)),
            expected,
            "{kind:?} {scenario}: after the crash and one more commit"
        );
    }
}

#[test]
fn rollback_between_close_reopen() {
    let _serial = serial();
    check_close_reopen("rollback_between");
}

#[test]
fn commit_between_close_reopen() {
    let _serial = serial();
    check_close_reopen("commit_between");
}

#[test]
fn failed_commit_close_reopen() {
    let _serial = serial();
    check_close_reopen("failed_commit");
}

#[test]
fn rollback_between_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("rollback_between");
}

#[test]
fn commit_between_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("commit_between");
}

#[test]
fn db_level_writes_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("db_level_writes");
}

/// The crash (no close) keeps the in-memory transaction from reaching the
/// file through a close-time checkpoint, so the reopen shows what the WAL
/// holds.
#[cfg(feature = "testing-crash-injection")]
#[test]
fn failed_group_crash_reopen() {
    let _serial = serial();
    for kind in kinds() {
        let dir = tempfile::tempdir().unwrap();
        let path = kind.create(dir.path());
        crash_after("failed_group", &path, kind);
        let db = kind.open(&path);
        assert_eq!(names(&db), strings(&["alix"]), "{kind:?}");
        insert(&db.session(), "after");
        db.close().unwrap();
        drop(db);
        assert_eq!(
            names(&kind.open(&path)),
            strings(&["after", "alix"]),
            "{kind:?}: after one more commit"
        );
    }
}

#[test]
fn cap_savepoint_after_failure_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("cap_savepoint_after_failure");
}

#[test]
fn cap_savepoint_before_failure_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("cap_savepoint_before_failure");
}

#[test]
fn cap_direct_create_with_props_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("cap_direct_create_with_props");
}

#[cfg(feature = "testing-crash-injection")]
#[test]
fn implicit_group_failure_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("implicit_group_failure");
}

#[cfg(feature = "testing-crash-injection")]
#[test]
fn implicit_query_failure_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("implicit_query_failure");
}

#[cfg(feature = "testing-crash-injection")]
#[test]
fn checked_batch_failure_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("checked_batch_failure");
}

#[cfg(feature = "testing-crash-injection")]
#[test]
fn writes_after_poison_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("writes_after_poison");
}

/// After the crash, the schema objects whose group was lost (and the ones
/// refused afterwards) are gone.
#[cfg(feature = "testing-crash-injection")]
fn check_schema_crash_reopen(scenario: &str) {
    check_crash_reopen(scenario);
    for kind in kinds() {
        let dir = tempfile::tempdir().unwrap();
        let path = kind.create(dir.path());
        crash_after(scenario, &path, kind);
        let db = kind.open(&path);
        let graphs = db.list_graphs();
        assert!(
            !graphs
                .iter()
                .any(|g| g.starts_with("lost") || g.starts_with("refused")),
            "{kind:?} {scenario}: after a crash: {graphs:?}"
        );
    }
}

#[cfg(feature = "testing-crash-injection")]
#[test]
fn schema_failure_crash_reopen() {
    let _serial = serial();
    check_schema_crash_reopen("schema_failure");
}

#[cfg(feature = "testing-crash-injection")]
#[test]
fn schema_ddl_failure_crash_reopen() {
    let _serial = serial();
    check_schema_crash_reopen("schema_ddl_failure");
}

#[test]
fn cap_nested_begin_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("cap_nested_begin");
}

#[test]
fn cap_nested_begin_drop_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("cap_nested_begin_drop");
}

#[test]
fn failed_commit_crash_reopen() {
    let _serial = serial();
    check_crash_reopen("failed_commit");
}

// ----------------------------------------------------------------------
// Old-format WAL (record shapes written by the pre-port code)
// ----------------------------------------------------------------------

#[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
mod old_format {
    use super::*;
    use grafeo_common::types::{EpochId, NodeId, TransactionId};
    use grafeo_storage::wal::{WalManager, WalRecord};

    fn create(id: u64, name: &str) -> [WalRecord; 2] {
        [
            WalRecord::CreateNode {
                id: NodeId::new(id),
                labels: vec!["Person".to_string()],
            },
            WalRecord::SetNodeProperty {
                id: NodeId::new(id),
                key: "name".to_string(),
                value: Value::from(name),
            },
        ]
    }

    fn commit_pair(tx: u64, epoch: u64) -> [WalRecord; 2] {
        [
            WalRecord::TransactionCommit {
                transaction_id: TransactionId::new(tx),
            },
            WalRecord::EpochAdvance {
                epoch: EpochId::new(epoch),
            },
        ]
    }

    /// Appends records one frame at a time, as the pre-port code did.
    fn append_old(root: &Path, records: &[WalRecord]) {
        let wal = WalManager::open(root.join("wal")).unwrap();
        for record in records {
            wal.log(record).unwrap();
        }
        wal.sync().unwrap();
    }

    /// Ids above anything the base or the opens allocated.
    const ID: u64 = 10_000;

    /// A generation-root WAL written by the pre-port code: a committed
    /// transaction, a rolled-back one closed by its abort marker, a schema
    /// record that rode on the next commit, and another committed
    /// transaction. The new code replays it as before, and its own writes
    /// after it replay too.
    #[test]
    fn generation_root_replays_pre_port_wal() {
        let _serial = serial();
        let dir = tempfile::tempdir().unwrap();
        let root = Kind::GenerationRoot.create(dir.path());
        // Open once so the root's WAL directory exists, then close.
        Kind::GenerationRoot.open(&root).close().unwrap();

        let mut records = Vec::new();
        records.extend(create(ID, "alix"));
        records.extend(commit_pair(100, 50));
        records.extend(create(ID + 1, "gus"));
        records.push(WalRecord::TransactionAbort {
            transaction_id: TransactionId::new(101),
        });
        records.push(WalRecord::CreateNamedGraph {
            name: "g".to_string(),
        });
        records.extend(create(ID + 2, "vincent"));
        records.extend(commit_pair(102, 51));
        append_old(&root, &records);

        let db = Kind::GenerationRoot.open(&root);
        assert_eq!(names(&db), strings(&["alix", "vincent"]));
        assert_eq!(db.list_graphs(), vec!["g".to_string()]);
        insert(&db.session(), "jules");
        db.close().unwrap();
        drop(db);

        let db = Kind::GenerationRoot.open(&root);
        assert_eq!(names(&db), strings(&["alix", "jules", "vincent"]));
    }

    /// A pre-port generation-root WAL can end inside a named graph (the
    /// shared graph context did not switch back). Default-graph writes made
    /// after it must replay into the default graph.
    #[test]
    fn generation_root_pre_port_wal_ending_in_a_named_graph() {
        let _serial = serial();
        let dir = tempfile::tempdir().unwrap();
        let root = Kind::GenerationRoot.create(dir.path());
        Kind::GenerationRoot.open(&root).close().unwrap();

        let mut records = vec![
            WalRecord::CreateNamedGraph {
                name: "g".to_string(),
            },
            WalRecord::SwitchGraph {
                name: Some("g".to_string()),
            },
        ];
        records.extend(create(ID, "mia"));
        records.extend(commit_pair(100, 50));
        append_old(&root, &records);

        let db = Kind::GenerationRoot.open(&root);
        insert(&db.session(), "gus");
        db.close().unwrap();
        drop(db);

        let db = Kind::GenerationRoot.open(&root);
        assert_eq!(names(&db), strings(&["gus"]), "default graph");
        let session = db.session();
        session.use_graph("g");
        assert_eq!(names_in(&session), strings(&["mia"]), "graph g");
    }

    /// The newest WAL file of a generation root.
    fn active_wal_file(root: &Path) -> PathBuf {
        let mut files: Vec<PathBuf> = std::fs::read_dir(root.join("wal"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
            .collect();
        files.sort();
        files.pop().unwrap()
    }

    /// A crash inside the first frame of a group leaves bytes that are not a
    /// complete record and no pending record before them. The open must cut
    /// them off, or the next group lands behind them and cannot be read.
    #[test]
    fn generation_root_torn_first_frame_is_cut_at_open() {
        let _serial = serial();
        for garbage in [&[7u8, 0][..], &[40, 0, 0, 0, 1, 2, 3][..]] {
            let dir = tempfile::tempdir().unwrap();
            let root = Kind::GenerationRoot.create(dir.path());
            {
                let db = Kind::GenerationRoot.open(&root);
                insert(&db.session(), "alix");
                db.close().unwrap();
            }
            {
                use std::io::Write;
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(active_wal_file(&root))
                    .unwrap()
                    .write_all(garbage)
                    .unwrap();
            }
            {
                let db = Kind::GenerationRoot.open(&root);
                assert_eq!(names(&db), strings(&["alix"]), "{garbage:?}");
                insert(&db.session(), "gus");
                db.close().unwrap();
            }
            let db = GrafeoDB::open_generation_root(&root, false)
                .unwrap_or_else(|e| panic!("{garbage:?}: reopen after the cut: {e}"));
            assert_eq!(names(&db), strings(&["alix", "gus"]), "{garbage:?}");
        }
    }

    /// A pre-port generation-root WAL cut off by a crash in the middle of a
    /// transaction (records, no commit marker) still opens, drops the
    /// unfinished transaction, and a later commit does not pick it up.
    #[test]
    fn generation_root_pre_port_torn_tail() {
        let _serial = serial();
        let dir = tempfile::tempdir().unwrap();
        let root = Kind::GenerationRoot.create(dir.path());
        Kind::GenerationRoot.open(&root).close().unwrap();

        let mut records = Vec::new();
        records.extend(create(ID, "alix"));
        records.extend(commit_pair(100, 50));
        records.extend(create(ID + 1, "torn"));
        append_old(&root, &records);

        let db = Kind::GenerationRoot.open(&root);
        assert_eq!(names(&db), strings(&["alix"]));
        insert(&db.session(), "gus");
        db.close().unwrap();
        drop(db);
        assert_eq!(
            names(&Kind::GenerationRoot.open(&root)),
            strings(&["alix", "gus"])
        );
    }
}

/// A WAL-directory database written by the pre-port code (records appended
/// as they happen, bare commit/epoch pairs, an abort marker) replays as
/// before, and a log left inside a named graph does not pull new
/// default-graph writes into that graph.
#[test]
fn wal_directory_replays_pre_port_wal() {
    let _serial = serial();
    use grafeo_common::types::{EpochId, NodeId, TransactionId};
    use grafeo_engine::Config;
    use grafeo_engine::config::StorageFormat;
    use grafeo_storage::wal::{WalManager, WalRecord};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dir-db");
    let config = || Config::persistent(&path).with_storage_format(StorageFormat::WalDirectory);
    {
        let wal = WalManager::open(path.join("wal")).unwrap();
        let node = |id: u64, name: &str| {
            [
                WalRecord::CreateNode {
                    id: NodeId::new(id),
                    labels: vec!["Person".to_string()],
                },
                WalRecord::SetNodeProperty {
                    id: NodeId::new(id),
                    key: "name".to_string(),
                    value: Value::from(name),
                },
            ]
        };
        let mut records = Vec::new();
        records.extend(node(1, "alix"));
        records.push(WalRecord::TransactionCommit {
            transaction_id: TransactionId::new(2),
        });
        records.push(WalRecord::EpochAdvance {
            epoch: EpochId::new(1),
        });
        records.extend(node(2, "gus"));
        records.push(WalRecord::TransactionAbort {
            transaction_id: TransactionId::new(3),
        });
        records.push(WalRecord::CreateNamedGraph {
            name: "g".to_string(),
        });
        records.push(WalRecord::SwitchGraph {
            name: Some("g".to_string()),
        });
        records.extend(node(1, "mia"));
        records.push(WalRecord::TransactionCommit {
            transaction_id: TransactionId::new(4),
        });
        records.push(WalRecord::EpochAdvance {
            epoch: EpochId::new(2),
        });
        for record in &records {
            wal.log(record).unwrap();
        }
        wal.sync().unwrap();
    }

    let db = GrafeoDB::with_config(config()).unwrap();
    assert_eq!(names(&db), strings(&["alix"]));
    insert(&db.session(), "vincent");
    db.close().unwrap();
    drop(db);

    let db = GrafeoDB::with_config(config()).unwrap();
    assert_eq!(names(&db), strings(&["alix", "vincent"]), "default graph");
    let session = db.session();
    session.use_graph("g");
    assert_eq!(names_in(&session), strings(&["mia"]), "graph g");
}

// ----------------------------------------------------------------------
// Implicit groups on a generation root
// ----------------------------------------------------------------------

/// Writes outside a transaction form their own group with a system commit.
/// Generation-root replay requires an `EpochAdvance` right after every
/// commit, so these groups need one too, or the next group makes the root
/// unopenable.
#[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
mod implicit_groups {
    use super::*;

    fn reopen_twice(root: &Path, check: impl Fn(&GrafeoDB)) {
        for round in 0..2 {
            let db = GrafeoDB::open_generation_root(root, false)
                .unwrap_or_else(|e| panic!("reopen {round}: {e}"));
            check(&db);
            db.close().unwrap();
        }
    }

    /// A schema change outside a transaction, then a committed write.
    #[test]
    fn schema_change_then_commit_reopens() {
        let _serial = serial();
        let dir = tempfile::tempdir().unwrap();
        let root = Kind::GenerationRoot.create(dir.path());
        {
            let db = Kind::GenerationRoot.open(&root);
            db.session().execute("CREATE GRAPH g").unwrap();
            insert(&db.session(), "alix");
            db.close().unwrap();
        }
        reopen_twice(&root, |db| {
            assert_eq!(db.list_graphs(), vec!["g".to_string()]);
            assert_eq!(names(db), strings(&["alix"]));
        });
    }

    /// A schema change inside a transaction that rolls back is applied
    /// immediately and stays (as upstream's `schema_changes_in_a_rolled_back_
    /// transaction_match_memory`), and a later commit still reopens.
    #[test]
    fn schema_change_in_rolled_back_transaction_then_commit_reopens() {
        let _serial = serial();
        let dir = tempfile::tempdir().unwrap();
        let root = Kind::GenerationRoot.create(dir.path());
        {
            let db = Kind::GenerationRoot.open(&root);
            let mut a = db.session();
            a.begin_transaction().unwrap();
            a.execute("CREATE GRAPH h").unwrap();
            insert(&a, "gone");
            a.rollback().unwrap();
            insert(&db.session(), "gus");
            db.close().unwrap();
        }
        reopen_twice(&root, |db| {
            assert_eq!(db.list_graphs(), vec!["h".to_string()]);
            assert_eq!(names(db), strings(&["gus"]));
        });
    }

    /// A direct session write to a named graph (which lives in the overlay,
    /// outside the layered store's implicit transactions), then a committed
    /// default-graph write.
    #[test]
    fn named_graph_direct_write_then_commit_reopens() {
        let _serial = serial();
        let dir = tempfile::tempdir().unwrap();
        let root = Kind::GenerationRoot.create(dir.path());
        {
            let db = Kind::GenerationRoot.open(&root);
            db.session().execute("CREATE GRAPH g").unwrap();
            let session = db.session();
            session.use_graph("g");
            session
                .create_node_with_props(&["Person"], [("name", Value::from("hans"))])
                .unwrap();
            insert(&db.session(), "jules");
            db.close().unwrap();
        }
        reopen_twice(&root, |db| {
            assert_eq!(names(db), strings(&["jules"]), "default graph");
            let session = db.session();
            session.use_graph("g");
            assert_eq!(names_in(&session), strings(&["hans"]), "graph g");
        });
    }
}
