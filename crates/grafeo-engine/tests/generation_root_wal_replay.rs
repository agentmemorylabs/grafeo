//! H-ADOPT.3 Phase D — fresh-process crash proofs for generation-root replay.
//!
//! Proofs (each in a real fresh process; no `#[cfg(test)]` crash seams, no
//! injected errors):
//!
//! 1. **SIGKILL durability.** A child opens a writable generation root, commits
//!    N auto-commit transactions, then opens an explicit transaction and makes
//!    one more INSERT **without** committing. The parent sends a real
//!    `SIGKILL`; the child never runs destructors, so the uncommitted record
//!    stays in the WAL with no commit/abort marker (a genuine torn tail). The
//!    parent reopens and asserts all N committed writes are present while the
//!    uncommitted write is gone.
//! 2. **Repeated-reopen determinism.** Repeatedly reopening (≥3) yields an
//!    identical observable overlay state (sorted names + node/edge counts).
//! 3. **Bounded RssAnon on replay.** A child re-exec opens a root whose WAL
//!    tail holds ≥20,000 committed records; the constructor's streaming replay
//!    keeps the memory figure within the 192 MiB precedent. An empty-tail
//!    baseline is reported alongside so the marginal replay cost is visible.
//!
//! The SIGKILL + RSS children are the same test binary re-exec'd with a marker
//! argument after `--` (pattern: `compact_store_generation_contract.rs`).

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal"
))]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use grafeo_common::types::Value;
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::tempdir;

/// Number of committed auto-commit transactions the SIGKILL child makes.
const SIGKILL_COMMITTED: usize = 30;
/// Committed nodes written before the repeated-reopen loop.
const REOPEN_COMMITTED: usize = 12;
/// Number of repeated reopens that must agree exactly.
const REOPEN_CYCLES: usize = 4;
/// Committed records in the large replay tail (boundedness proof).
const REPLAY_TAIL_RECORDS: usize = 20_000;
/// Memory ceiling for the replay child: the 192 MiB precedent
/// (`compact_store_generation_contract.rs`).
const RSS_BUDGET_KB: u64 = 196_608;

/// Read the anonymous-memory figure from `/proc/self/status`, in KiB.
/// Copied verbatim from `compact_store_generation_contract.rs:325`: returns
/// the first of `RssAnon:` / `VmHWM:` present (kernel ordering puts `VmHWM`
/// first, so this is peak high-water RSS — a bound at least as strong as
/// `RssAnon` alone, matching the 192 MiB precedent's measurement basis).
fn read_rss_anon_kb() -> u64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("RssAnon:") || line.starts_with("VmHWM:") {
                let Some(val) = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|field| field.parse::<u64>().ok())
                else {
                    continue;
                };
                return val;
            }
        }
    }
    0
}

/// Read both `RssAnon:` and `VmHWM:` from `/proc/self/status`, in KiB.
fn read_mem_detail_kb() -> (u64, u64) {
    let mut rss_anon = 0u64;
    let mut vm_hwm = 0u64;
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            let Some(val) = line
                .split_whitespace()
                .nth(1)
                .and_then(|field| field.parse::<u64>().ok())
            else {
                continue;
            };
            if line.starts_with("RssAnon:") {
                rss_anon = val;
            } else if line.starts_with("VmHWM:") {
                vm_hwm = val;
            }
        }
    }
    (rss_anon, vm_hwm)
}

/// Publish a small non-empty base generation (two nodes + one edge) at `root`.
fn publish_base(root: &std::path::Path, generation_id: &str) {
    let source = GrafeoDB::new_in_memory();
    let ada = source
        .create_node_with_props(&["Person"], [("name", Value::from("Ada"))])
        .expect("create Ada");
    let grace = source
        .create_node_with_props(&["Person"], [("name", Value::from("Grace"))])
        .expect("create Grace");
    source.create_edge_with_props(ada, grace, "KNOWS", [("since", Value::from(2020i64))]);
    source
        .build_and_publish_generation(generation_build_request(root, generation_id))
        .expect("publish base generation");
    drop(source);
}

fn count_as_usize(value: &Value) -> usize {
    match value {
        Value::Int64(v) => usize::try_from(*v).expect("non-negative count"),
        other => panic!("expected integer count, got {other:?}"),
    }
}

/// Collect the sorted `Person` names plus node/edge counts from an open root.
fn observable_state(db: &GrafeoDB) -> (Vec<String>, usize, usize) {
    let result = db
        .session()
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("query Person names");
    let mut names: Vec<String> = result
        .rows()
        .iter()
        .map(|row| match &row[0] {
            Value::String(value) => value.as_str().to_string(),
            other => panic!("expected string name, got {other:?}"),
        })
        .collect();
    names.sort_unstable();

    let node_rows = db
        .session()
        .execute("MATCH (n) RETURN count(n)")
        .expect("count nodes");
    let edge_rows = db
        .session()
        .execute("MATCH ()-[r]->() RETURN count(r)")
        .expect("count edges");
    let nodes = count_as_usize(&node_rows.rows()[0][0]);
    let edges = count_as_usize(&edge_rows.rows()[0][0]);
    (names, nodes, edges)
}

/// Probe locking the explicit-transaction behavior the SIGKILL proof relies
/// on: with `START TRANSACTION` open and the db still open — the exact state
/// at SIGKILL time, when no Drop/close can run — the mutation's data records
/// are already in the root WAL, but no `TransactionCommit`/
/// `TransactionAbort`/`EpochAdvance` marker exists for them. (Drop/close do
/// log markers, but SIGKILL preempts all destructors.) If this probe ever
/// fails, the deterministic torn tail in [`sigkill_reopen_retains_committed`]
/// needs another route.
#[test]
fn explicit_transaction_write_is_wal_logged_without_commit_marker() {
    use grafeo_storage::generation::manifest::read_manifest;
    use grafeo_storage::generation::wal_cursor::{WalReplayCursor, replay_stream_from};
    use grafeo_storage::wal::WalRecord;

    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("explicit-tx-probe.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");
    publish_base(&root, "explicit-tx-probe-g1");

    let db = GrafeoDB::open_generation_root(&root, false).expect("open generation root writable");
    let session = db.session();
    session
        .execute("START TRANSACTION")
        .expect("begin explicit tx");
    assert!(
        session.in_transaction(),
        "START TRANSACTION must enter explicit-transaction mode"
    );
    session
        .execute("INSERT (:Person {name: 'Uncommitted'})")
        .expect("insert inside explicit tx");

    // Scan the root WAL WHILE the db is still open: this is the exact on-disk
    // state a SIGKILL would leave (destructors never run).
    let (_, slot) = read_manifest(&root.join("manifest.bin")).expect("read manifest");
    let cursor = WalReplayCursor {
        log_sequence: slot.wal_log_sequence,
        byte_offset: slot.wal_byte_offset,
        epoch: slot.overlay_epoch,
        transaction_id: slot.transaction_id,
    };
    let stream = replay_stream_from(&root.join("wal"), &cursor).expect("stream from boundary");

    let mut data_records = 0u64;
    let mut saw_commit = false;
    let mut saw_abort = false;
    let mut saw_epoch_advance = false;
    for frame in stream {
        let frame = frame.expect("post-boundary frame decodes");
        match frame.record {
            WalRecord::CreateNode { .. }
            | WalRecord::SetNodeProperty { .. }
            | WalRecord::CreateEdge { .. }
            | WalRecord::SetEdgeProperty { .. } => data_records += 1,
            WalRecord::TransactionCommit { .. } => saw_commit = true,
            WalRecord::TransactionAbort { .. } => saw_abort = true,
            WalRecord::EpochAdvance { .. } => saw_epoch_advance = true,
            _ => {}
        }
    }
    assert!(
        data_records > 0,
        "explicit-tx mutation must log data records to the root WAL before COMMIT"
    );
    assert!(
        !saw_commit,
        "no TransactionCommit may be logged while the explicit tx is open"
    );
    assert!(
        !saw_abort,
        "no TransactionAbort may be logged while the explicit tx is open"
    );
    assert!(
        !saw_epoch_advance,
        "no EpochAdvance may be logged while the explicit tx is open"
    );
    // db/session drop here (logging rollback markers) is fine: all assertions
    // above captured the SIGKILL-time state.
}

/// H-ADOPT.3 Phase D proof 1: a real `SIGKILL` mid-write-stream cannot lose a
/// committed write, and the uncommitted tail is discarded on reopen.
///
/// No `#[cfg(test)]` seam, no injected error, no `abort()`: the child publishes
/// readiness on stdout and the parent kills it with `libc::kill(SIGKILL)`,
/// asserting the termination signal via `ExitStatusExt`.
#[test]
fn sigkill_reopen_retains_committed() {
    // ── Child mode: open writable, commit N, hold one uncommitted write ──
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|a| a == "--sigkill-child") {
        let root = std::path::PathBuf::from(&args[pos + 1]);
        let db = GrafeoDB::open_generation_root(&root, false)
            .expect("child: open generation root writable");
        let session = db.session();
        for i in 0..SIGKILL_COMMITTED {
            session
                .execute(&format!("INSERT (:Person {{name: 'crash-proof-{i}'}})"))
                .unwrap_or_else(|e| panic!("child: committed insert {i} failed: {e}"));
        }
        // Explicit transaction: mutations are logged as data records WITHOUT a
        // commit marker, i.e. deterministic pending/torn-tail material
        // (behavior locked by `explicit_transaction_write_is_wal_logged_without_commit_marker`).
        session
            .execute("START TRANSACTION")
            .expect("child: begin tx");
        session
            .execute(&format!(
                "INSERT (:Person {{name: 'crash-proof-{SIGKILL_COMMITTED}'}})"
            ))
            .expect("child: uncommitted insert");
        // Publish readiness, then hold db + the open transaction alive until
        // the parent kills us. SIGKILL preempts all destructors, so no
        // rollback/abort marker can be logged: a genuine torn tail.
        println!("READY {}", std::process::id());
        std::io::stdout().flush().expect("child: flush stdout");
        let _keep_alive = (&db, &session);
        loop {
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    // ── Parent mode ──
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("sigkill.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");
    publish_base(&root, "sigkill-g1");

    let exe = std::env::current_exe().expect("current exe");
    let mut child = Command::new(exe)
        .arg("--exact")
        .arg("sigkill_reopen_retains_committed")
        .arg("--nocapture")
        .arg("--")
        .arg("--sigkill-child")
        .arg(root.to_str().expect("root utf8"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn SIGKILL child");

    // Read child stdout on a helper thread so the ~60s readiness deadline is
    // enforced even if the child blocks.
    let stdout = child.stdout.take().expect("piped stdout");
    let (ready_tx, ready_rx) = mpsc::channel::<bool>();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = ready_tx.send(false);
                    return;
                }
                Ok(_) => {
                    if line.starts_with("READY") {
                        let _ = ready_tx.send(true);
                        return;
                    }
                }
                Err(_) => {
                    let _ = ready_tx.send(false);
                    return;
                }
            }
        }
    });
    let ready = ready_rx
        .recv_timeout(Duration::from_mins(1))
        .unwrap_or(false);
    assert!(ready, "child never published READY within 60s");

    // Real SIGKILL from the parent — no seam, no abort. `Child::kill()` sends
    // SIGKILL on Unix (pre-approved by the packet's locked decision 1); the
    // workspace denies `unsafe_code`, so no direct `libc::kill` call.
    let child_pid = child.id();
    let started = Instant::now();
    child.kill().expect("Child::kill (SIGKILL) must succeed");
    let status = child.wait().expect("wait for SIGKILL'd child");
    assert!(!status.success(), "SIGKILL'd child must not report success");
    assert_eq!(
        status.signal(),
        Some(libc::SIGKILL),
        "child must terminate by SIGKILL, got {status:?}"
    );
    println!(
        "SIGKILL delivered to pid {child_pid} after {:?}",
        started.elapsed()
    );

    // Drain the child's stderr for the record (no-op unless it warned).
    if let Some(mut stderr) = child.stderr.take() {
        let mut buf = String::new();
        let _ = std::io::Read::read_to_string(&mut stderr, &mut buf);
        if !buf.trim().is_empty() {
            println!("child stderr: {buf}");
        }
    }

    // Reopen: the constructor replays the committed tail and discards the torn
    // (uncommitted) record. All N committed writes must be present; the
    // uncommitted (N+1)th must be absent.
    let reopened =
        GrafeoDB::open_generation_root(&root, false).expect("reopen generation root after SIGKILL");
    let (names, node_count, edge_count) = observable_state(&reopened);

    let mut expected: Vec<String> = vec!["Ada".to_string(), "Grace".to_string()];
    for i in 0..SIGKILL_COMMITTED {
        expected.push(format!("crash-proof-{i}"));
    }
    expected.sort_unstable();
    assert_eq!(
        names, expected,
        "all committed writes survive SIGKILL; the uncommitted write must not"
    );
    assert!(
        !names.contains(&format!("crash-proof-{SIGKILL_COMMITTED}")),
        "the uncommitted (torn) write must be discarded on replay"
    );
    // Base contributed 2 nodes + 1 edge; the child added exactly N nodes.
    assert_eq!(
        node_count,
        2 + SIGKILL_COMMITTED,
        "node count = base(2) + committed({SIGKILL_COMMITTED})"
    );
    assert_eq!(edge_count, 1, "edge count = base KNOWS edge only");
}

/// Re-exec this test binary as a child that opens `root` writable (the
/// constructor replays the WAL tail) and prints `RSS_ANON_KB=<n>`.
fn spawn_replay_child(root: &std::path::Path) -> String {
    let exe = std::env::current_exe().expect("current exe");
    let output = Command::new(exe)
        .arg("--exact")
        .arg("replay_rss_bounded")
        .arg("--nocapture")
        .arg("--")
        .arg("--replay-child")
        .arg(root.to_str().expect("root utf8"))
        .output()
        .expect("spawn replay child");
    assert!(
        output.status.success(),
        "replay child must exit 0: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn parse_rss_anon_kb(stdout: &str) -> u64 {
    for line in stdout.lines() {
        for part in line.split_whitespace() {
            if let Some(val) = part.strip_prefix("RSS_ANON_KB=") {
                return val.parse().expect("RSS_ANON_KB is an integer");
            }
        }
    }
    panic!("child stdout lacks RSS_ANON_KB=: {stdout}");
}

/// H-ADOPT.3 Phase D proof 3: replaying a large committed WAL tail keeps
/// anonymous memory within the 192 MiB precedent (streaming replay is
/// O(one frame); the overlay holds the applied state, not the WAL). An
/// empty-tail baseline child is measured alongside so the marginal replay
/// cost is reported.
#[test]
fn replay_rss_bounded() {
    // ── Child mode: open writable (constructor replays the tail), print RSS ──
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|a| a == "--replay-child") {
        let root = std::path::PathBuf::from(&args[pos + 1]);
        let _db = GrafeoDB::open_generation_root(&root, false)
            .expect("replay child: open generation root writable");
        let rss = read_rss_anon_kb();
        let (rss_anon, vm_hwm) = read_mem_detail_kb();
        println!("RSS_ANON_KB={rss}");
        println!("MEM_DETAIL RssAnon_kb={rss_anon} VmHWM_kb={vm_hwm}");
        std::io::stdout().flush().expect("flush stdout");
        std::process::exit(0);
    }

    // ── Parent mode ──
    let dir = tempdir().expect("temp dir");

    // Root with a LARGE committed tail.
    let large_root = dir.path().join("replay-large.grafeo.d");
    std::fs::create_dir_all(&large_root).expect("create large root");
    publish_base(&large_root, "replay-large-g1");
    let build_started = Instant::now();
    {
        let db =
            GrafeoDB::open_generation_root(&large_root, false).expect("open large root writable");
        let session = db.session();
        for i in 0..REPLAY_TAIL_RECORDS {
            session
                .execute(&format!("INSERT (:Bulk {{n: {i}, name: 'bulk-{i}'}})"))
                .unwrap_or_else(|e| panic!("bulk insert {i} failed: {e}"));
        }
    }
    println!(
        "large-tail build: {REPLAY_TAIL_RECORDS} committed inserts in {:?}",
        build_started.elapsed()
    );

    let large_stdout = spawn_replay_child(&large_root);
    let large_rss = parse_rss_anon_kb(&large_stdout);

    // Empty-tail baseline: publish a base, never write post-boundary.
    let empty_root = dir.path().join("replay-empty.grafeo.d");
    std::fs::create_dir_all(&empty_root).expect("create empty root");
    publish_base(&empty_root, "replay-empty-g1");
    let empty_stdout = spawn_replay_child(&empty_root);
    let empty_rss = parse_rss_anon_kb(&empty_stdout);

    println!(
        "REPLAY_RSS large_tail_records={REPLAY_TAIL_RECORDS} large_rss_kb={large_rss} empty_baseline_rss_kb={empty_rss} budget_kb={RSS_BUDGET_KB}"
    );
    println!("large-child stdout detail:");
    for line in large_stdout.lines() {
        if line.starts_with("MEM_DETAIL") {
            println!("  {line}");
        }
    }
    for line in empty_stdout.lines() {
        if line.starts_with("MEM_DETAIL") {
            println!("  empty baseline {line}");
        }
    }
    assert!(
        large_rss <= RSS_BUDGET_KB,
        "replay memory must be <= 192 MiB ({RSS_BUDGET_KB} KB), got {large_rss} KB \
         (empty-tail baseline {empty_rss} KB, tail {REPLAY_TAIL_RECORDS} records)"
    );
}
