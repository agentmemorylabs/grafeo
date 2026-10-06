//! A large transaction's WAL buffer spills to disk instead of RAM.
//!
//! Since the #411 port, a transaction's WAL records are buffered until commit
//! and written as one group. These tests set a tiny spill threshold and check
//! that:
//!
//! - a transaction far larger than the threshold commits, survives a reopen
//!   with every record, and never holds more than about the threshold in RAM
//!   (the buffer's own counter, not wall-clock or RSS);
//! - a crash mid-transaction (spill file present, no commit) and a crash in
//!   the middle of the commit's copy both reopen to exactly the committed
//!   writes, and the leftover spill files are removed;
//! - a disk-full spill fails the statement with a retryable error and
//!   the transaction rolls back cleanly; the byte cap does the same;
//! - concurrent large transactions with small ones in between replay
//!   correctly;
//! - on a generation root, a live backup never copies a spill file, and an
//!   epoch handoff with a spilled transaction open keeps it.
//!
//! Each runs on a single `.grafeo` file, a WAL directory and (with the
//! generation features) a writable generation root.
//!
//! ```bash
//! cargo test -p grafeo-engine --test wal_spill \
//!   --features generation,generation-streaming,compact-store,lpg,mmap,wal,gql,grafeo-file,testing-crash-injection
//! ```

#![cfg(all(feature = "wal", feature = "lpg", feature = "gql"))]
#![allow(missing_docs)]

use std::path::{Path, PathBuf};

use grafeo_common::types::Value;
use grafeo_common::utils::error::Error;
use grafeo_engine::config::StorageFormat;
use grafeo_engine::session::Session;
use grafeo_engine::{Config, GrafeoDB};

/// Serializes the tests of this binary: the crash tests fork children, which
/// briefly hold copies of the parent's file descriptors (root locks).
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Spill threshold for these tests: a few records.
const THRESHOLD: usize = 4096;
/// Rows in a "large" transaction; with ~1 KiB per row, hundreds of times the
/// threshold.
const LARGE: usize = 600;
/// About one embedding's worth of payload per row.
const PAYLOAD: usize = 1024;

const SCENARIO_VAR: &str = "GRAFEO_WAL_SPILL_SCENARIO";
const PATH_VAR: &str = "GRAFEO_WAL_SPILL_PATH";
const KIND_VAR: &str = "GRAFEO_WAL_SPILL_KIND";

#[derive(Clone, Copy, Debug)]
enum Kind {
    #[cfg(feature = "grafeo-file")]
    SingleFile,
    WalDirectory,
    #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
    GenerationRoot,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            #[cfg(feature = "grafeo-file")]
            Kind::SingleFile => "single-file",
            Kind::WalDirectory => "wal-directory",
            #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
            Kind::GenerationRoot => "generation-root",
        }
    }

    fn from_name(name: &str) -> Self {
        kinds()
            .into_iter()
            .find(|kind| kind.name() == name)
            .unwrap_or_else(|| panic!("unknown kind {name}"))
    }

    /// Creates the database under `dir` and returns the path to open.
    fn create(self, dir: &Path) -> PathBuf {
        match self {
            #[cfg(feature = "grafeo-file")]
            Kind::SingleFile => dir.join("db.grafeo"),
            Kind::WalDirectory => dir.join("db-dir"),
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

    fn config(self, path: &Path) -> Config {
        let config = Config::persistent(path).with_wal_spill_threshold(THRESHOLD);
        match self {
            #[cfg(feature = "grafeo-file")]
            Kind::SingleFile => config.with_storage_format(StorageFormat::SingleFile),
            Kind::WalDirectory => config.with_storage_format(StorageFormat::WalDirectory),
            #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
            Kind::GenerationRoot => config,
        }
    }

    fn open_with(self, config: Config) -> GrafeoDB {
        match self {
            #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
            Kind::GenerationRoot => GrafeoDB::open_generation_root_with_config(config).unwrap(),
            _ => GrafeoDB::with_config(config).unwrap(),
        }
    }

    fn open(self, path: &Path) -> GrafeoDB {
        self.open_with(self.config(path))
    }

    /// The directory holding the WAL files.
    fn wal_dir(self, path: &Path) -> PathBuf {
        match self {
            #[cfg(feature = "grafeo-file")]
            Kind::SingleFile => {
                let mut sidecar = path.as_os_str().to_owned();
                sidecar.push(".wal");
                PathBuf::from(sidecar)
            }
            Kind::WalDirectory => path.join("wal"),
            #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
            Kind::GenerationRoot => path.join("wal"),
        }
    }
}

#[allow(unused_mut, clippy::vec_init_then_push)]
fn kinds() -> Vec<Kind> {
    let mut kinds = Vec::new();
    #[cfg(feature = "grafeo-file")]
    kinds.push(Kind::SingleFile);
    kinds.push(Kind::WalDirectory);
    #[cfg(all(feature = "generation", feature = "compact-store", feature = "mmap"))]
    kinds.push(Kind::GenerationRoot);
    kinds
}

/// Spill files under `wal_dir`.
fn spill_files(wal_dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(wal_dir.join(grafeo_storage::wal::SPILL_DIR))
        .map(|entries| entries.map(|e| e.unwrap().path()).collect())
        .unwrap_or_default()
}

fn count(session: &Session, label: &str) -> usize {
    let result = session
        .execute(&format!("MATCH (n:{label}) RETURN count(n)"))
        .unwrap();
    match &result.rows()[0][0] {
        Value::Int64(n) => usize::try_from(*n).unwrap(),
        other => panic!("unexpected count {other:?}"),
    }
}

/// Sorted `seq` of every node with `label`.
fn seqs(session: &Session, label: &str) -> Vec<i64> {
    let result = session
        .execute(&format!("MATCH (n:{label}) RETURN n.seq ORDER BY n.seq"))
        .unwrap();
    result
        .rows()
        .iter()
        .map(|row| match &row[0] {
            Value::Int64(n) => *n,
            other => panic!("unexpected seq {other:?}"),
        })
        .collect()
}

/// Inserts `rows` nodes with `label`, `seq` from `from` and a payload of
/// about [`PAYLOAD`] bytes each.
fn insert_rows(session: &Session, label: &str, from: usize, rows: usize) {
    for seq in from..from + rows {
        session
            .execute(&format!(
                "INSERT (:{label} {{seq: {seq}, payload: '{}'}})",
                "x".repeat(PAYLOAD)
            ))
            .unwrap();
    }
}

fn is_buffer_full(error: &Error) -> bool {
    matches!(error, Error::AdmissionRetryable(_)) && error.error_code().is_retryable()
}

// ----------------------------------------------------------------------
// Crash children
// ----------------------------------------------------------------------

/// Child-process entry for [`crash_after`]; a no-op when run directly.
#[test]
fn crash_child() {
    let Ok(scenario) = std::env::var(SCENARIO_VAR) else {
        return;
    };
    let path = PathBuf::from(std::env::var_os(PATH_VAR).unwrap());
    let kind = Kind::from_name(&std::env::var(KIND_VAR).unwrap());
    let db = open_for_session_case(kind, &path, &scenario);
    let mut session = db.session();
    insert_rows(&session, "Kept", 0, 3);
    if let Some(case) = scenario.strip_prefix("session_") {
        let err = failing_session_write(&session, case);
        assert!(is_buffer_full(&err), "{kind:?} {case}: {err}");
        // Crash: no close(), no destructors.
        std::process::exit(0);
    }
    match scenario.as_str() {
        // A spilled transaction that never commits.
        "mid_transaction" => {
            session.begin_transaction().unwrap();
            insert_rows(&session, "Lost", 0, LARGE);
            assert!(session.wal_buffer_is_spilled());
        }
        // A crash in the middle of copying the spill file into the WAL.
        #[cfg(feature = "testing-crash-injection")]
        "mid_copy" => {
            session.begin_transaction().unwrap();
            insert_rows(&session, "Lost", 0, LARGE);
            assert!(session.wal_buffer_is_spilled());
            // A real crash: no unwinding, no destructors, nothing flushed.
            let marker = path.with_extension("crashed");
            std::panic::set_hook(Box::new(move |_| {
                std::fs::write(&marker, b"crashed").unwrap();
                std::process::exit(0);
            }));
            // Every copied frame passes two crash points; the group has more
            // than two frames per row. Crash after about half the rows.
            grafeo_common::testing::crash::enable_crash_at(LARGE as u64);
            let _ = session.commit();
            panic!("the commit should have crashed");
        }
        other => panic!("unknown scenario {other}"),
    }
    // Crash: no close(), no destructors.
    std::process::exit(0);
}

/// Cap for the `session_cap` case: over one 8 KiB write, over the 1 KiB
/// `Kept` rows.
const SESSION_CAP: usize = 4096;

/// Opens the database for `scenario`: with [`SESSION_CAP`] for the cap case.
fn open_for_session_case(kind: Kind, path: &Path, scenario: &str) -> GrafeoDB {
    if scenario.ends_with("cap") {
        kind.open_with(
            kind.config(path)
                .with_wal_transaction_buffer_cap(SESSION_CAP),
        )
    } else {
        kind.open(path)
    }
}

/// A session write outside a transaction (`Lost`, one node) whose records
/// cannot be buffered, after the store applied it: the spill file cannot be
/// created (`spill_create`), a spill write fails (`spill_write`), or the
/// write is over the cap (`cap`). Returns the write's error.
fn failing_session_write(session: &Session, case: &str) -> Error {
    use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
    let payload = match case {
        // Over the spill threshold: the first push past it creates the file.
        "spill_create" | "cap" => 2 * THRESHOLD,
        // Over one write-buffer chunk: the frame itself is written out.
        "spill_write" => 100 * 1024,
        other => panic!("unknown session case {other}"),
    };
    // I/O calls of this write: creating the spill file (1), writing the
    // frames moved from RAM (2), writing the large frame (3).
    match case {
        "spill_create" => enable_io_failure_at(1),
        "spill_write" => enable_io_failure_at(3),
        _ => {}
    }
    let result = session.create_node_with_props(
        &["Lost"],
        [
            ("seq", Value::Int64(0)),
            ("payload", Value::from("x".repeat(payload))),
        ],
    );
    disable_io_failure();
    result.expect_err("the write's records cannot be buffered")
}

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

/// Reopens after a crash child ran `scenario`: only `Kept` survives, the
/// spill files are gone, and a later commit does not pick anything up.
fn check_crash(scenario: &str) {
    let _serial = serial();
    for kind in kinds() {
        let dir = tempfile::tempdir().unwrap();
        let path = kind.create(dir.path());
        crash_after(scenario, &path, kind);
        let wal_dir = kind.wal_dir(&path);
        if scenario == "mid_transaction" {
            assert_eq!(
                spill_files(&wal_dir).len(),
                1,
                "{kind:?}: the crash leaves the spill file behind"
            );
        } else {
            assert!(
                path.with_extension("crashed").exists(),
                "{kind:?}: the child did not crash in the commit"
            );
        }

        let db = kind.open(&path);
        assert!(
            spill_files(&wal_dir).is_empty(),
            "{kind:?}: reopening removes leftover spill files"
        );
        let session = db.session();
        assert_eq!(seqs(&session, "Kept"), vec![0, 1, 2], "{kind:?}");
        assert_eq!(count(&session, "Lost"), 0, "{kind:?}: not committed");
        insert_rows(&session, "After", 0, 1);
        db.close().unwrap();
        drop(db);

        let db = kind.open(&path);
        let session = db.session();
        assert_eq!(seqs(&session, "Kept"), vec![0, 1, 2], "{kind:?}");
        assert_eq!(seqs(&session, "After"), vec![0], "{kind:?}");
        assert_eq!(count(&session, "Lost"), 0, "{kind:?}: still not committed");
    }
}

#[test]
fn crash_mid_transaction_reopens_without_it_and_removes_the_spill_file() {
    check_crash("mid_transaction");
}

#[cfg(feature = "testing-crash-injection")]
#[test]
fn crash_mid_commit_copy_reopens_without_it() {
    check_crash("mid_copy");
}

// ----------------------------------------------------------------------
// Commit, reopen, memory bound
// ----------------------------------------------------------------------

#[test]
fn large_transaction_spills_commits_and_survives_reopen() {
    let _serial = serial();
    for kind in kinds() {
        let dir = tempfile::tempdir().unwrap();
        let path = kind.create(dir.path());
        let wal_dir = kind.wal_dir(&path);
        {
            let db = kind.open(&path);
            let mut session = db.session();
            session.begin_transaction().unwrap();
            insert_rows(&session, "Big", 0, LARGE);
            assert!(session.wal_buffer_is_spilled(), "{kind:?}");
            assert_eq!(spill_files(&wal_dir).len(), 1, "{kind:?}");
            session.commit().unwrap();
            assert!(
                spill_files(&wal_dir).is_empty(),
                "{kind:?}: the commit deletes the spill file"
            );

            // The group is hundreds of times the threshold; RAM stayed at
            // the threshold plus fixed I/O buffers (two 64 KiB chunks, the
            // encoding buffer and one record) the whole time, commit included.
            let buffered = LARGE * PAYLOAD;
            let peak = session.wal_buffer_peak_ram_bytes();
            assert!(buffered > 100 * THRESHOLD);
            assert!(
                peak <= THRESHOLD + 4 * 65_536,
                "{kind:?}: peak WAL buffer RAM {peak} bytes for {buffered} buffered bytes"
            );
            assert_eq!(count(&session, "Big"), LARGE, "{kind:?}: live");
            db.close().unwrap();
        }
        let db = kind.open(&path);
        let session = db.session();
        assert_eq!(
            seqs(&session, "Big"),
            (0..LARGE as i64).collect::<Vec<_>>(),
            "{kind:?}: every record after reopen"
        );
    }
}

#[test]
fn rollback_and_savepoints_of_a_spilled_transaction() {
    let _serial = serial();
    for kind in kinds() {
        let dir = tempfile::tempdir().unwrap();
        let path = kind.create(dir.path());
        let wal_dir = kind.wal_dir(&path);
        {
            let db = kind.open(&path);
            let mut session = db.session();
            // Rollback deletes the spill file and writes nothing.
            session.begin_transaction().unwrap();
            insert_rows(&session, "Gone", 0, LARGE / 2);
            assert!(session.wal_buffer_is_spilled());
            session.rollback().unwrap();
            assert!(spill_files(&wal_dir).is_empty(), "{kind:?}: rollback");

            // A savepoint inside the spilled part, rolled back to.
            session.begin_transaction().unwrap();
            insert_rows(&session, "Part", 0, 100);
            session.savepoint("sp").unwrap();
            insert_rows(&session, "Part", 100, 100);
            session.rollback_to_savepoint("sp").unwrap();
            insert_rows(&session, "Part", 1000, 1);
            session.commit().unwrap();
            db.close().unwrap();
        }
        let db = kind.open(&path);
        let session = db.session();
        assert_eq!(count(&session, "Gone"), 0, "{kind:?}");
        let mut expected: Vec<i64> = (0..100).collect();
        expected.push(1000);
        assert_eq!(seqs(&session, "Part"), expected, "{kind:?}");
    }
}

#[test]
fn concurrent_large_transactions_and_small_ones_replay_correctly() {
    let _serial = serial();
    for kind in kinds() {
        let dir = tempfile::tempdir().unwrap();
        let path = kind.create(dir.path());
        {
            let db = std::sync::Arc::new(kind.open(&path));
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let small = {
                let db = std::sync::Arc::clone(&db);
                let stop = std::sync::Arc::clone(&stop);
                std::thread::spawn(move || {
                    let session = db.session();
                    let mut n = 0;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) || n < 5 {
                        session
                            .execute(&format!("INSERT (:Small {{seq: {n}}})"))
                            .unwrap();
                        n += 1;
                    }
                    n
                })
            };
            let large: Vec<_> = ["BigA", "BigB"]
                .into_iter()
                .map(|label| {
                    let db = std::sync::Arc::clone(&db);
                    std::thread::spawn(move || {
                        let mut session = db.session();
                        session.begin_transaction().unwrap();
                        insert_rows(&session, label, 0, LARGE / 2);
                        assert!(session.wal_buffer_is_spilled());
                        session.commit().unwrap();
                    })
                })
                .collect();
            for handle in large {
                handle.join().unwrap();
            }
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let small_count = small.join().unwrap();
            assert!(spill_files(&kind.wal_dir(&path)).is_empty());
            db.close().unwrap();
            drop(db);

            let db = kind.open(&path);
            let session = db.session();
            let half = (0..(LARGE / 2) as i64).collect::<Vec<_>>();
            assert_eq!(seqs(&session, "BigA"), half, "{kind:?}");
            assert_eq!(seqs(&session, "BigB"), half, "{kind:?}");
            assert_eq!(
                seqs(&session, "Small"),
                (0..small_count).collect::<Vec<_>>(),
                "{kind:?}"
            );
        }
    }
}

// ----------------------------------------------------------------------
// Disk full, cap
// ----------------------------------------------------------------------

/// After a failed transaction: a small one commits, and a reopen shows only
/// the small one.
fn check_recovers_after_failure(kind: Kind, path: &Path, db: GrafeoDB) {
    let session = db.session();
    assert_eq!(
        count(&session, "Huge"),
        0,
        "{kind:?}: rolled back in memory"
    );
    insert_rows(&session, "Small", 0, 2);
    assert!(spill_files(&kind.wal_dir(path)).is_empty(), "{kind:?}");
    db.close().unwrap();
    drop(db);
    let db = kind.open(path);
    let session = db.session();
    assert_eq!(count(&session, "Huge"), 0, "{kind:?}");
    assert_eq!(seqs(&session, "Small"), vec![0, 1], "{kind:?}");
}

#[cfg(feature = "testing-crash-injection")]
#[test]
fn disk_full_while_spilling_is_retryable_and_rolls_back() {
    use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_from};
    let _serial = serial();
    for kind in kinds() {
        for finish_with_commit in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = kind.create(dir.path());
            let db = kind.open(&path);
            let mut session = db.session();
            session.begin_transaction().unwrap();
            // Every write on this thread fails from now on, like a full disk.
            // Inside the transaction nothing else writes, so the first one is
            // the spill.
            enable_io_failure_from(1);
            let mut failure = None;
            for seq in 0..LARGE {
                if let Err(e) = session.execute(&format!(
                    "INSERT (:Huge {{seq: {seq}, payload: '{}'}})",
                    "x".repeat(PAYLOAD)
                )) {
                    failure = Some(e);
                    break;
                }
            }
            disable_io_failure();
            let failure = failure.expect("the spill must fail");
            assert!(is_buffer_full(&failure), "{kind:?}: {failure}");
            assert!(failure.to_string().contains("spill"), "{kind:?}: {failure}");
            // Later statements are refused too: the transaction can only
            // roll back.
            assert!(session.execute("INSERT (:Huge {seq: -1})").is_err());
            if finish_with_commit {
                let err = session.commit().unwrap_err();
                assert!(is_buffer_full(&err), "{kind:?}: {err}");
                assert!(!session.in_transaction(), "a refused commit rolls back");
            } else {
                session.rollback().unwrap();
            }
            drop(session);
            check_recovers_after_failure(kind, &path, db);
        }
    }
}

/// Properties for `rows` nodes of about [`PAYLOAD`] bytes each.
fn batch_props(
    rows: usize,
) -> Vec<std::collections::HashMap<grafeo_common::types::PropertyKey, Value>> {
    (0..rows)
        .map(|seq| {
            let mut props = std::collections::HashMap::new();
            props.insert("seq".into(), Value::Int64(seq as i64));
            props.insert("payload".into(), Value::from("x".repeat(PAYLOAD)));
            props
        })
        .collect()
}

/// Reopens after the WAL was poisoned: the writes before it (`Kept`) are
/// there, the poisoning write (label `Lost`, `lost_rows` nodes) never reached
/// the WAL and is gone (close does not snapshot memory over a poisoned WAL),
/// and the database writes again.
fn check_reopen_after_poison(kind: Kind, path: &Path, db: GrafeoDB, lost_rows: usize) {
    assert!(
        db.session().execute("INSERT (:After {seq: 0})").is_err(),
        "{kind:?}: the WAL refuses writes once poisoned"
    );
    // No checkpoint of memory over a poisoned WAL: the close fails, and the
    // reopen replays the WAL as it is.
    assert!(db.close().is_err(), "{kind:?}: close over a poisoned WAL");
    drop(db);
    let db = kind.open(path);
    let session = db.session();
    assert_eq!(seqs(&session, "Kept"), vec![0, 1, 2], "{kind:?}");
    let lost = count(&session, "Lost");
    assert_eq!(
        lost, 0,
        "{kind:?}: {lost} of {lost_rows} never-logged nodes came back"
    );
    assert!(
        spill_files(&kind.wal_dir(path)).is_empty(),
        "{kind:?}: no spill file left"
    );
    insert_rows(&session, "After", 0, 1);
    assert_eq!(seqs(&session, "After"), vec![0], "{kind:?}");
}

/// A `GrafeoDB`-level batch is applied before its implicit group is
/// buffered; when spilling that group fails, the WAL is poisoned and the
/// error is never retryable (a retry would apply the batch twice).
#[cfg(feature = "testing-crash-injection")]
#[test]
fn implicit_group_spill_failure_poisons_and_is_not_retryable() {
    use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_from};
    let _serial = serial();
    for kind in kinds() {
        for legacy in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = kind.create(dir.path());
            let db = kind.open(&path);
            insert_rows(&db.session(), "Kept", 0, 3);
            enable_io_failure_from(1);
            if legacy {
                // The legacy API only logs the failure; the poison is what
                // keeps later writes from building on the lost batch.
                let ids = db.batch_create_nodes_with_props("Lost", batch_props(50));
                assert_eq!(ids.len(), 50);
            } else {
                let err = db
                    .try_batch_create_nodes_with_props("Lost", batch_props(50))
                    .expect_err("the spill fails");
                assert!(!err.error_code().is_retryable(), "{kind:?}: {err}");
                assert!(
                    err.to_string().contains("durability unconfirmed"),
                    "{kind:?}: {err}"
                );
            }
            disable_io_failure();
            check_reopen_after_poison(kind, &path, db, 50);
        }
    }
}

/// A session write outside a transaction whose spilled implicit group fails
/// in the commit's copy reports it, instead of returning success.
#[cfg(feature = "testing-crash-injection")]
#[test]
fn implicit_session_write_copy_failure_is_reported() {
    use grafeo_common::testing::crash::{disable_io_failure, enable_io_failure_at};
    let _serial = serial();
    for kind in kinds() {
        let dir = tempfile::tempdir().unwrap();
        let path = kind.create(dir.path());
        let db = kind.open(&path);
        insert_rows(&db.session(), "Kept", 0, 3);
        let session = db.session();
        // I/O calls of this write: creating the spill file (1), writing it
        // (2), then the WAL append (3), which fails.
        enable_io_failure_at(3);
        let result = session.create_node_with_props(
            &["Lost"],
            [
                ("seq", Value::Int64(0)),
                // Over the spill threshold, so the group spills.
                ("payload", Value::from("x".repeat(2 * THRESHOLD))),
            ],
        );
        disable_io_failure();
        let err = result.expect_err("the copy into the WAL fails");
        assert!(
            err.to_string().contains("durability unconfirmed"),
            "{kind:?}: {err}"
        );
        drop(session);
        check_reopen_after_poison(kind, &path, db, 1);
    }
}

/// A session write outside a transaction whose records cannot be buffered
/// (spill create, spill write, over the cap): the call fails with a
/// retryable error that says a reopen is needed, the next write is refused,
/// close does not checkpoint memory, and after close + reopen or crash +
/// reopen the write is gone, so a retry after reopen does not duplicate it.
#[cfg(feature = "testing-crash-injection")]
#[test]
fn session_write_outside_a_transaction_refused_records_are_gone_after_reopen() {
    let _serial = serial();
    for kind in kinds() {
        for case in ["spill_create", "spill_write", "cap"] {
            // Close + reopen.
            let dir = tempfile::tempdir().unwrap();
            let path = kind.create(dir.path());
            let scenario = format!("session_{case}");
            let db = open_for_session_case(kind, &path, &scenario);
            insert_rows(&db.session(), "Kept", 0, 3);
            let err = failing_session_write(&db.session(), case);
            assert!(is_buffer_full(&err), "{kind:?} {case}: {err}");
            assert!(
                err.to_string().contains("does not have the write"),
                "{kind:?} {case}: {err}"
            );
            assert_eq!(
                count(&db.session(), "Lost"),
                1,
                "{kind:?} {case}: applied in memory"
            );
            check_reopen_after_poison(kind, &path, db, 1);

            // Crash + reopen.
            let dir = tempfile::tempdir().unwrap();
            let path = kind.create(dir.path());
            crash_after(&scenario, &path, kind);
            let db = kind.open(&path);
            let session = db.session();
            assert_eq!(seqs(&session, "Kept"), vec![0, 1, 2], "{kind:?} {case}");
            assert_eq!(count(&session, "Lost"), 0, "{kind:?} {case}: after a crash");
        }
    }
}

/// In a spilled transaction, a savepoint taken right before a refused write
/// recovers the transaction; one taken after it is refused.
#[test]
fn savepoint_at_a_refused_write_in_a_spilled_transaction() {
    let _serial = serial();
    for kind in kinds() {
        let dir = tempfile::tempdir().unwrap();
        let path = kind.create(dir.path());
        let cap = 64 * 1024;
        {
            let db = kind.open_with(kind.config(&path).with_wal_transaction_buffer_cap(cap));
            let mut session = db.session();
            session.begin_transaction().unwrap();
            insert_rows(&session, "Part", 0, 20);
            assert!(session.wal_buffer_is_spilled(), "{kind:?}");
            session.savepoint("before").unwrap();
            let err = session
                .execute(&format!(
                    "INSERT (:Huge {{payload: '{}'}})",
                    "x".repeat(2 * cap)
                ))
                .expect_err("over the cap");
            assert!(is_buffer_full(&err), "{kind:?}: {err}");
            assert!(
                session.savepoint("after").is_err(),
                "{kind:?}: no savepoint after a refused write"
            );
            session.rollback_to_savepoint("before").unwrap();
            insert_rows(&session, "Part", 20, 1);
            session.commit().expect("commits without the refused write");
            db.close().unwrap();
        }
        let db = kind.open(&path);
        let session = db.session();
        assert_eq!(
            seqs(&session, "Part"),
            (0..21).collect::<Vec<_>>(),
            "{kind:?}"
        );
        assert_eq!(count(&session, "Huge"), 0, "{kind:?}");
    }
}

#[test]
fn byte_cap_bounds_the_spill_file() {
    let _serial = serial();
    for kind in kinds() {
        let dir = tempfile::tempdir().unwrap();
        let path = kind.create(dir.path());
        let cap = 64 * 1024;
        let db = kind.open_with(kind.config(&path).with_wal_transaction_buffer_cap(cap));
        let mut session = db.session();
        session.begin_transaction().unwrap();
        let mut failure = None;
        for seq in 0..LARGE {
            if let Err(e) = session.execute(&format!(
                "INSERT (:Huge {{seq: {seq}, payload: '{}'}})",
                "x".repeat(PAYLOAD)
            )) {
                failure = Some(e);
                break;
            }
        }
        let failure = failure.expect("the cap must be hit");
        assert!(is_buffer_full(&failure), "{kind:?}: {failure}");
        assert!(
            failure
                .to_string()
                .contains(&format!("{cap}-byte transaction WAL buffer cap")),
            "{kind:?}: {failure}"
        );
        // The spill file never grew past the cap (plus a frame's overhead).
        for file in spill_files(&kind.wal_dir(&path)) {
            let len = std::fs::metadata(&file).unwrap().len();
            assert!(len <= cap as u64 + 4096, "{kind:?}: spill file {len} bytes");
        }
        session.rollback().unwrap();
        drop(session);
        check_recovers_after_failure(kind, &path, db);
    }
}

// ----------------------------------------------------------------------
// Generation root: live backup, epoch handoff
// ----------------------------------------------------------------------

#[cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "compact-store",
    feature = "mmap"
))]
mod generation_root {
    use super::*;

    fn files_under(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    out.push(path.to_string_lossy().into_owned());
                }
            }
        }
        out
    }

    #[test]
    fn live_backup_never_copies_a_spill_file() {
        let _serial = serial();
        let kind = Kind::GenerationRoot;
        let dir = tempfile::tempdir().unwrap();
        let root = kind.create(dir.path());
        let db = kind.open(&root);
        insert_rows(&db.session(), "Kept", 0, 3);

        // A spilled transaction is open while the backup runs.
        let mut open_tx = db.session();
        open_tx.begin_transaction().unwrap();
        insert_rows(&open_tx, "Pending", 0, LARGE / 2);
        assert_eq!(spill_files(&root.join("wal")).len(), 1);

        let receipt = db
            .backup_generation_root(&dir.path().join("backups"), "spill-1")
            .expect("live backup");
        let copied = files_under(&receipt.backup_dir);
        assert!(
            !copied
                .iter()
                .any(|f| f.contains(grafeo_storage::wal::SPILL_DIR) || f.ends_with(".spill")),
            "the backup copied a spill file: {copied:?}"
        );

        // The open transaction commits after the cut: the restore has the
        // writes before it, the live root all of them.
        open_tx.commit().unwrap();
        let new_root = dir.path().join("restored");
        drop(grafeo_engine::restore_generation_root(&receipt.backup_dir, &new_root).unwrap());
        let restored = GrafeoDB::open_generation_root(&new_root, true).unwrap();
        let session = restored.session();
        assert_eq!(seqs(&session, "Kept"), vec![0, 1, 2]);
        assert_eq!(count(&session, "Pending"), 0);
        drop(session);
        drop(restored);

        db.close().unwrap();
        drop(db);
        let db = kind.open(&root);
        assert_eq!(count(&db.session(), "Pending"), LARGE / 2);
    }

    #[test]
    fn epoch_handoff_keeps_an_open_spilled_transaction() {
        let _serial = serial();
        let kind = Kind::GenerationRoot;
        let dir = tempfile::tempdir().unwrap();
        let root = kind.create(dir.path());
        {
            let db = kind.open(&root);
            insert_rows(&db.session(), "Kept", 0, 3);
            let mut open_tx = db.session();
            open_tx.begin_transaction().unwrap();
            insert_rows(&open_tx, "Pending", 0, LARGE / 2);
            assert!(open_tx.wal_buffer_is_spilled());

            let report = db
                .run_epoch_handoff(grafeo_engine::generation_build_request(&root, "g-next"))
                .expect("epoch handoff");
            db.publish_and_install_handoff(report).expect("install");
            // The handoff's own WAL managers leave the live spill file alone.
            assert_eq!(spill_files(&root.join("wal")).len(), 1);

            insert_rows(&open_tx, "Pending", LARGE / 2, 10);
            open_tx.commit().unwrap();
            assert!(spill_files(&root.join("wal")).is_empty());
            db.close().unwrap();
        }
        let db = kind.open(&root);
        let session = db.session();
        assert_eq!(seqs(&session, "Kept"), vec![0, 1, 2]);
        assert_eq!(
            seqs(&session, "Pending"),
            (0..(LARGE / 2 + 10) as i64).collect::<Vec<_>>()
        );
    }
}
