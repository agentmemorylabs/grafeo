//! DESIGN G2: SIGKILL during an epoch handoff with a writer thread running.
//!
//! Each test runs [`sigkill_child`] in a child process: it opens a
//! generation root, starts a writer thread, and runs a handoff (freeze →
//! build → publish → retire → install). The handoff parks at one named point
//! (`GRAFEO_TEST_PARK_AT`, see `grafeo_common::testing::crash::park_point`)
//! while the writer keeps going where it is not blocked; the parent then
//! SIGKILLs the child and reopens the root.
//!
//! The writer prints `TRY <op>` before each write and `ACK <op>` after it
//! returns. The reopened state must equal every acknowledged write, plus at
//! most the one write in flight at the kill.
//!
//! Points: the freeze (locks held, writer blocked), mid-build (streaming
//! done, writer running), pre-commit (before the manifest slot write),
//! pre-WAL-truncate (manifest synced, WAL not yet truncated), post-commit
//! (retired, before the install), the install (writer stopped, before the
//! swap), and after the install.

#![cfg(all(
    unix,
    feature = "testing-crash-injection",
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal"
))]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use grafeo_common::testing::crash::{PARK_AT_ENV, PARKED_MARKER, park_point};
use grafeo_common::types::{NodeId, PropertyKey, Value};
use grafeo_common::utils::error::Error;
use grafeo_core::graph::traits::GraphStore;
use grafeo_engine::{GrafeoDB, generation_build_request};

const CHILD_ROOT_ENV: &str = "G2_SIGKILL_CHILD_ROOT";
const BASE_NODES: i64 = 4;

fn open(root: &Path) -> GrafeoDB {
    GrafeoDB::open_generation_root(root, false).expect("open generation root")
}

fn base_ids(db: &GrafeoDB) -> Vec<NodeId> {
    let mut ids = db.graph_store().nodes_by_label("B");
    ids.sort();
    ids
}

fn retry<T>(mut step: impl FnMut() -> grafeo_common::utils::error::Result<T>) -> T {
    loop {
        match step() {
            Ok(v) => return v,
            Err(Error::AdmissionRetryable(_)) => std::thread::sleep(Duration::from_millis(1)),
            Err(e) => panic!("handoff step: {e}"),
        }
    }
}

/// The child: a writer thread plus one handoff that parks at the point the
/// parent named. Does nothing unless run by a parent.
#[test]
#[ignore = "child process of the SIGKILL tests below"]
fn sigkill_child() {
    let Ok(root) = std::env::var(CHILD_ROOT_ENV) else {
        return;
    };
    let root = PathBuf::from(root);
    let db = Arc::new(open(&root));
    let base = base_ids(&db);

    let writer = {
        let db = Arc::clone(&db);
        std::thread::spawn(move || {
            let say = |line: String| {
                let mut out = std::io::stdout().lock();
                writeln!(out, "{line}").expect("stdout");
                out.flush().expect("flush");
            };
            let mut created: Vec<(i64, NodeId)> = Vec::new();
            for i in 1_i64.. {
                say(format!("TRY create {i}"));
                let id = db
                    .create_node_with_props(&["W"], [("seq", Value::Int64(i))])
                    .expect("create");
                say(format!("ACK create {i}"));
                created.push((i, id));
                let b = (i % BASE_NODES) as usize;
                say(format!("TRY set {b} {i}"));
                db.set_node_property(base[b], "k", Value::Int64(i))
                    .expect("set");
                say(format!("ACK set {b} {i}"));
                if i % 5 == 0 {
                    let (seq, id) = created.remove(created.len() / 2);
                    say(format!("TRY delete {seq}"));
                    assert!(db.delete_node(id).expect("delete"));
                    say(format!("ACK delete {seq}"));
                }
            }
        })
    };

    std::thread::sleep(Duration::from_millis(100));
    for cycle in 0..2 {
        let handle = retry(|| db.freeze_epoch_for_handoff(&root));
        let report = db
            .complete_epoch_handoff(
                handle,
                generation_build_request(&root, &format!("k{cycle}")),
            )
            .expect("complete handoff");
        retry(|| db.publish_and_install_handoff(report.clone()));
        park_point("installed");
        std::thread::sleep(Duration::from_millis(50));
    }
    drop(writer);
    // Not reached when the parent's point exists: the parent kills first.
    panic!("sigkill_child finished without parking");
}

/// One writer operation, as printed by the child.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    Create(i64),
    Set(usize, i64),
    Delete(i64),
}

fn parse(rest: &str) -> Op {
    let parts: Vec<&str> = rest.split(' ').collect();
    match parts.as_slice() {
        ["create", i] => Op::Create(i.parse().unwrap()),
        ["set", b, i] => Op::Set(b.parse().unwrap(), i.parse().unwrap()),
        ["delete", i] => Op::Delete(i.parse().unwrap()),
        other => panic!("op line {other:?}"),
    }
}

/// `W` nodes by `seq`, and each base node's `k`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct State {
    w: BTreeMap<i64, ()>,
    k: BTreeMap<usize, i64>,
}

impl State {
    fn apply(&mut self, op: &Op) {
        match op {
            Op::Create(i) => {
                self.w.insert(*i, ());
            }
            Op::Set(b, i) => {
                self.k.insert(*b, *i);
            }
            Op::Delete(i) => {
                self.w.remove(i);
            }
        }
    }
}

fn reopened_state(db: &GrafeoDB) -> State {
    let store = db.graph_store();
    let mut state = State::default();
    for id in store.nodes_by_label("W") {
        match store.get_node_property(id, &PropertyKey::new("seq")) {
            Some(Value::Int64(seq)) => {
                assert!(state.w.insert(seq, ()).is_none(), "duplicate W seq {seq}");
            }
            other => panic!("W node {id:?} seq {other:?}"),
        }
    }
    for (b, id) in base_ids(db).into_iter().enumerate() {
        match store.get_node_property(id, &PropertyKey::new("k")) {
            Some(Value::Int64(k)) => {
                state.k.insert(b, k);
            }
            other => panic!("base {b} k {other:?}"),
        }
    }
    state
}

/// Writes the child acknowledged after it parked (the writer keeps going
/// where the parked handoff does not block it).
fn acks_after_park(lines: &[(Instant, String)], parked_at: &Instant) -> usize {
    lines
        .iter()
        .filter(|(at, line)| at > parked_at && line.contains("ACK "))
        .count()
}

fn run_point(point: &str) {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("kill.grafeo.d");
    std::fs::create_dir_all(&root).expect("create root");
    let mut initial = State::default();
    {
        let source = GrafeoDB::new_in_memory();
        for b in 0..BASE_NODES {
            source
                .create_node_with_props(&["B"], [("k", Value::Int64(-1)), ("b", Value::Int64(b))])
                .expect("base");
            initial.k.insert(b as usize, -1);
        }
        source
            .build_and_publish_generation(generation_build_request(&root, "g1"))
            .expect("publish base");
    }

    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "sigkill_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_ROOT_ENV, &root)
        .env(PARK_AT_ENV, point)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn child");

    let stdout = child.stdout.take().expect("stdout");
    let lines = std::thread::spawn(move || {
        BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
            .map(|line| (Instant::now(), line))
            .collect::<Vec<(Instant, String)>>()
    });
    let (parked_tx, parked_rx) = mpsc::channel::<()>();
    let stderr = child.stderr.take().expect("stderr");
    let marker = format!("{PARKED_MARKER} {point}");
    let errors = std::thread::spawn(move || {
        let mut all = Vec::new();
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if line == marker {
                let _ = parked_tx.send(());
            }
            all.push(line);
        }
        all
    });

    if parked_rx.recv_timeout(Duration::from_secs(300)).is_err() {
        let _ = child.kill();
        let _ = child.wait();
        let errors = errors.join().unwrap_or_default();
        panic!(
            "child never parked at {point}; stderr:\n{}",
            errors.join("\n")
        );
    }
    let parked_at = Instant::now();
    // Let the writer run on where the parked handoff does not block it.
    std::thread::sleep(Duration::from_millis(300));
    child.kill().expect("SIGKILL child");
    let status = child.wait().expect("wait child");
    assert!(!status.success(), "child was killed");
    let lines = lines.join().expect("stdout reader");
    drop(errors);

    let mut acked = initial;
    let mut in_flight: Option<Op> = None;
    let mut acks = 0usize;
    // libtest prints `test sigkill_child ... ` without a newline before the
    // child's first line, so an op can start mid-line.
    let op_in = |line: &str, tag: &str| line.find(tag).map(|i| line[i + tag.len()..].to_string());
    for (_, line) in &lines {
        if let Some(rest) = op_in(line, "TRY ") {
            in_flight = Some(parse(&rest));
        } else if let Some(rest) = op_in(line, "ACK ") {
            let op = parse(&rest);
            assert_eq!(in_flight.take().as_ref(), Some(&op), "ACK without TRY");
            acked.apply(&op);
            acks += 1;
        }
    }
    assert!(acks > 0, "the writer acknowledged writes before the kill");

    let db = open(&root);
    let got = reopened_state(&db);
    eprintln!(
        "[{point}] {acks} writes acked ({} after the park), in flight {in_flight:?}; \
         reopened {} W nodes",
        acks_after_park(&lines, &parked_at),
        got.w.len()
    );
    let mut with_in_flight = acked.clone();
    if let Some(op) = &in_flight {
        with_in_flight.apply(op);
    }
    assert!(
        got == acked || got == with_in_flight,
        "[{point}] reopen after SIGKILL lost or invented writes \
         ({acks} acked, in flight {in_flight:?}):\n got  {got:?}\n want {acked:?}"
    );
    // The reopened root takes another handoff and keeps the same state.
    let report = db
        .run_epoch_handoff(generation_build_request(&root, "after-kill"))
        .expect("handoff after reopen");
    db.publish_and_install_handoff(report)
        .expect("install after reopen");
    assert_eq!(reopened_state(&db), got, "[{point}] handoff after reopen");
}

#[test]
fn sigkill_at_freeze() {
    run_point("handoff_freeze");
}

#[test]
fn sigkill_mid_build() {
    run_point("after_streaming");
}

#[test]
fn sigkill_pre_commit() {
    run_point("before_slot_write");
}

#[test]
fn sigkill_pre_wal_truncate() {
    run_point("during_wal_cleanup");
}

#[test]
fn sigkill_post_commit_before_install() {
    run_point("handoff_retired");
}

#[test]
fn sigkill_during_install() {
    run_point("handoff_install");
}

#[test]
fn sigkill_after_install() {
    run_point("installed");
}
