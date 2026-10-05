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

#![cfg(all(feature = "wal", feature = "lpg"))]
#![allow(missing_docs)]

use std::path::{Path, PathBuf};

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;
use grafeo_engine::session::Session;

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
            #[cfg(feature = "grafeo-file")]
            Kind::SingleFile => "single-file",
            #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
            Kind::GenerationRoot => "generation-root",
        }
    }

    fn from_name(name: &str) -> Self {
        match name {
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
        match self {
            #[cfg(feature = "grafeo-file")]
            Kind::SingleFile => GrafeoDB::open(path).unwrap(),
            #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
            Kind::GenerationRoot => GrafeoDB::open_generation_root(path, false).unwrap(),
        }
    }
}

fn kinds() -> Vec<Kind> {
    #[allow(unused_mut)]
    let mut kinds = Vec::new();
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
    let kind = Kind::from_name(&std::env::var(KIND_VAR).unwrap());
    let db = kind.open(&path);
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
            let db = kind.open(&ref_path);
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
