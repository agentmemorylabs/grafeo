//! Crash-atomicity of `.grafeo` checkpoints, tested with a real process death.
//!
//! The in-process crash tests in `crash_injection_single_file.rs` catch the
//! injected panic with `catch_unwind`, which lets `GrafeoDB::drop` run
//! `close()` again during unwinding and finish the interrupted checkpoint.
//! That hides torn writes. Here the checkpoint runs in a child process whose
//! panic hook calls `std::process::abort()`, so the injected crash kills the
//! process on the spot: no unwinding, no destructors, no second checkpoint,
//! only whatever bytes already reached the OS page cache. From the file's
//! point of view this is the same as SIGKILL or the kernel OOM killer.
//!
//! Every scenario first establishes a good checkpoint (clean close), then
//! reopens, writes more data, and dies at crash point `n` of the next
//! checkpoint, for every `n` until the child completes. The parent reopens
//! the file after each death and requires either the old or the new state
//! (plus WAL replay when the WAL is on), never an error.
//!
//! Requires both `grafeo-file` and `testing-crash-injection` features:
//!
//! ```text
//! cargo test -p grafeo-engine --features testing-crash-injection --test crash_atomic_checkpoint
//! ```

#![cfg(all(
    feature = "grafeo-file",
    feature = "wal",
    feature = "lpg",
    feature = "testing-crash-injection"
))]

use std::path::{Path, PathBuf};
use std::process::Command;

use grafeo_common::types::Value;
use grafeo_engine::{Config, GrafeoDB};

const ENV_SCENARIO: &str = "GRAFEO_CRASH_CHILD_SCENARIO";
const ENV_POINT: &str = "GRAFEO_CRASH_CHILD_POINT";
const ENV_PATH: &str = "GRAFEO_CRASH_CHILD_PATH";

/// Upper bound on crash points per scenario, so a regression that stops the
/// child from ever completing cannot loop forever.
const MAX_POINTS: u64 = 64;

/// Crash sites every checkpoint must pass through: either side of the
/// rename that publishes the new image.
const PUBLISH_SITES: &[&str] = &["checkpoint:before_rename", "checkpoint:after_rename"];

/// `wal_checkpoint()` also writes `checkpoint.meta` after the image is
/// durable; die between that and the old-log truncation too.
const WAL_CHECKPOINT_SITES: &[&str] = &[
    "checkpoint:before_rename",
    "checkpoint:after_rename",
    "wal_checkpoint:after_metadata",
];

const ROUND1: &[&str] = &["Alix", "Gus"];
const ROUND2: &[&str] = &["Jules", "Vincent"];

fn wal_disabled_config(path: &Path) -> Config {
    Config {
        wal_enabled: false,
        ..Config::persistent(path)
    }
}

fn config_for(scenario: &str, path: &Path) -> Config {
    if scenario == "close_wal_off" {
        wal_disabled_config(path)
    } else {
        Config::persistent(path)
    }
}

fn names(db: &GrafeoDB) -> Vec<String> {
    let result = db
        .session()
        .execute("MATCH (p:Person) RETURN p.name")
        .unwrap();
    let mut names: Vec<String> = result
        .rows()
        .iter()
        .filter_map(|r| match &r[0] {
            Value::String(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    names.sort();
    names
}

fn sorted(parts: &[&[&str]]) -> Vec<String> {
    let mut v: Vec<String> = parts
        .iter()
        .flat_map(|p| p.iter().map(|s| (*s).to_string()))
        .collect();
    v.sort();
    v
}

fn insert_people(db: &GrafeoDB, people: &[&str]) {
    let session = db.session();
    for name in people {
        session
            .execute(&format!("INSERT (:Person {{name: '{name}'}})"))
            .unwrap();
    }
}

/// Builds the "previous good checkpoint" that the crashed checkpoint must not
/// destroy. Some padding makes the image span several pages so a partial
/// overwrite is visible.
fn establish_round1(scenario: &str, path: &Path) {
    let db = GrafeoDB::with_config(config_for(scenario, path)).unwrap();
    insert_people(&db, ROUND1);
    let session = db.session();
    for i in 0..200 {
        session
            .execute(&format!(
                "INSERT (:Pad {{i: {i}, blob: '{}'}})",
                "r1".repeat(64)
            ))
            .unwrap();
    }
    db.close().unwrap();
}

/// Child side: reopen, write round 2, arm the crash, run the checkpoint.
fn run_child(scenario: &str, point: u64, path: &Path) {
    // Turn the injected panic into an immediate process death.
    std::panic::set_hook(Box::new(|info| {
        eprintln!("child: {info}");
        std::process::abort();
    }));

    let db = GrafeoDB::with_config(config_for(scenario, path)).unwrap();
    insert_people(&db, ROUND2);
    if scenario == "wal_checkpoint_rotated" {
        // Push the WAL past its 64 MiB rotation threshold so the round-2
        // records live in a log file older than the active one.
        let session = db.session();
        let blob = "x".repeat(1 << 20);
        for i in 0..70 {
            session
                .execute(&format!("INSERT (:Big {{i: {i}, blob: '{blob}'}})"))
                .unwrap();
        }
    }

    grafeo_common::testing::crash::enable_crash_at(point);
    match scenario {
        "close_wal_on" | "close_wal_off" => db.close().unwrap(),
        "wal_checkpoint" | "wal_checkpoint_rotated" => db.wal_checkpoint().unwrap(),
        other => panic!("unknown scenario {other}"),
    }
    grafeo_common::testing::crash::disable_crash();
    // Completed without hitting the crash point. Exit without running
    // destructors so the wal_checkpoint scenarios do not also close cleanly.
    std::process::exit(0);
}

/// Child entry point. A no-op unless the parent set the env vars.
#[test]
fn crash_child_entry() {
    let (Ok(scenario), Ok(point), Ok(path)) = (
        std::env::var(ENV_SCENARIO),
        std::env::var(ENV_POINT),
        std::env::var(ENV_PATH),
    ) else {
        return;
    };
    run_child(&scenario, point.parse().unwrap(), &PathBuf::from(path));
}

/// Returns the name of the crash point the child died at, or `None` if it ran
/// the checkpoint to completion.
fn spawn_child(scenario: &str, point: u64, path: &Path) -> Option<String> {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "crash_child_entry",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ENV_SCENARIO, scenario)
        .env(ENV_POINT, point.to_string())
        .env(ENV_PATH, path)
        .output()
        .unwrap();
    if output.status.success() {
        return None;
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let site = stderr
        .split("crash injection at: ")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next());
    let Some(site) = site else {
        panic!(
            "{scenario} point {point}: child failed without reaching the crash point: \
             status={:?}\n{stderr}",
            output.status
        );
    };
    Some(site.to_string())
}

/// Runs a scenario across all crash points and returns one line per point
/// where reopening failed or lost data. Every site in `required_sites` must
/// be among the crash points the child died at.
fn run_scenario(
    scenario: &str,
    required_sites: &[&str],
    accept: impl Fn(&[String]) -> bool,
) -> Vec<String> {
    let mut failures = Vec::new();
    let mut crashed_points = 0;
    let mut sites = Vec::new();
    for point in 1..=MAX_POINTS {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("crash.grafeo");
        establish_round1(scenario, &path);

        let site = spawn_child(scenario, point, &path);
        let crashed = site.is_some();
        let crash_site = if let Some(site) = site {
            crashed_points += 1;
            let line = format!("crash point {point} ({site})");
            sites.push(site);
            line
        } else {
            "completed".to_string()
        };
        eprintln!("{scenario}: {crash_site}");

        match GrafeoDB::with_config(config_for(scenario, &path)) {
            Ok(db) => {
                let got = names(&db);
                if !accept(&got) {
                    failures.push(format!("{scenario}: {crash_site}: wrong data {got:?}"));
                }
                db.close().unwrap();
            }
            Err(e) => failures.push(format!("{scenario}: {crash_site}: reopen failed: {e}")),
        }

        if !crashed {
            break;
        }
        assert!(point < MAX_POINTS, "{scenario}: child never completed");
    }
    assert!(crashed_points > 0, "{scenario}: no crash point was reached");
    for required in required_sites {
        assert!(
            sites.iter().any(|s| s == required),
            "{scenario}: crash site {required} was never reached; sites: {sites:?}"
        );
    }
    eprintln!("{scenario}: exercised {crashed_points} crash points");
    failures
}

fn report(failures: &[String]) {
    assert!(
        failures.is_empty(),
        "{} crash point(s) left an unusable or wrong database:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Close-time checkpoint with the sidecar WAL on. The WAL is synced before
/// the checkpoint starts, so round 2 must always survive.
#[test]
fn sigkill_during_close_checkpoint_never_corrupts_file() {
    let all = sorted(&[ROUND1, ROUND2]);
    report(&run_scenario("close_wal_on", PUBLISH_SITES, |got| {
        got == all
    }));
}

/// Explicit `wal_checkpoint()` with the database left open. Includes a death
/// inside the WAL checkpoint itself, after `checkpoint.meta` is written and
/// before old log files are truncated.
#[test]
fn sigkill_during_wal_checkpoint_never_corrupts_file() {
    let all = sorted(&[ROUND1, ROUND2]);
    report(&run_scenario(
        "wal_checkpoint",
        WAL_CHECKPOINT_SITES,
        |got| got == all,
    ));
}

/// Close-time checkpoint with the WAL off: the file is the only copy, so
/// a crash must leave either the old image or the new one.
#[test]
fn sigkill_during_close_checkpoint_wal_off_keeps_old_or_new() {
    let old = sorted(&[ROUND1]);
    let new = sorted(&[ROUND1, ROUND2]);
    report(&run_scenario("close_wal_off", PUBLISH_SITES, |got| {
        got == old || got == new
    }));
}

/// `wal_checkpoint()` after the WAL has rotated. The WAL must not be marked
/// checkpointed (which lets recovery skip older log files) until the new
/// image is durable, or round 2 is lost when the checkpoint dies.
#[test]
fn sigkill_during_wal_checkpoint_after_rotation_keeps_wal_records() {
    let all = sorted(&[ROUND1, ROUND2]);
    report(&run_scenario(
        "wal_checkpoint_rotated",
        WAL_CHECKPOINT_SITES,
        |got| got == all,
    ));
}
