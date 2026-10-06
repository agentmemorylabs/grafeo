//! Crash-only recovery of overlay deletes on a generation root (AMH #161,
//! coverage follow-up from fork #38's review).
//!
//! A child opens a writable root, creates overlay rows, deletes them in
//! committed transactions, then holds an *uncommitted* delete of a base edge
//! open and is killed with `SIGKILL` (no destructors, no close). Reopen must
//! replay the committed deletes and discard the uncommitted one.
//!
//! One test per binary: the child is this test binary re-exec'd, and a fork
//! racing another test's root `flock` would make that test's reopen fail
//! (see `generation_root_wal_replay.rs`).

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use grafeo_common::types::Value;
use grafeo_engine::{GrafeoDB, generation_build_request};

const REL_X: &str = "MATCH (:MemoryEntity {name: 'a'})-[r:MemoryEntityRelation {rel_type: 'x'}]->(:MemoryEntity {name: 'b'}) RETURN count(r)";
const REL_BASE: &str = "MATCH (:MemoryEntity {name: 'a'})-[r:MemoryEntityRelation {rel_type: 'base'}]->(:MemoryEntity {name: 'b'}) RETURN count(r)";
const NODE_C: &str = "MATCH (n:MemoryEntity {name: 'c'}) RETURN count(n)";

fn count(db: &GrafeoDB, query: &str) -> i64 {
    match &db.execute_cypher(query).expect(query).rows()[0][0] {
        Value::Int64(v) => *v,
        other => panic!("{query}: {other:?}"),
    }
}

fn child(root: &std::path::Path) -> ! {
    let db = GrafeoDB::open_generation_root(root, false).expect("child: open");
    db.execute_cypher(
        "MATCH (a:MemoryEntity {name: 'a'}), (b:MemoryEntity {name: 'b'}) \
         CREATE (a)-[:MemoryEntityRelation {rel_type: 'x'}]->(b), (:MemoryEntity {name: 'c'})",
    )
    .expect("child: create overlay rows");
    // Direct-API rows too: the ones #161 could not delete.
    let session = db.session();
    let c2 = session
        .create_node_with_props(&["MemoryEntity"], [("name", Value::from("c2"))])
        .expect("child: create c2");
    db.execute_cypher(
        "MATCH (:MemoryEntity {name: 'a'})-[r:MemoryEntityRelation {rel_type: 'x'}]->() DELETE r",
    )
    .expect("child: delete x");
    db.execute_cypher("MATCH (n:MemoryEntity {name: 'c'}) DETACH DELETE n")
        .expect("child: delete c");
    assert!(session.delete_node(c2), "child: delete c2");
    // Uncommitted: must not survive.
    let mut open_txn = db.session();
    open_txn.begin_transaction().expect("child: begin");
    open_txn
        .execute_cypher(
            "MATCH (:MemoryEntity {name: 'a'})-[r:MemoryEntityRelation {rel_type: 'base'}]->() DELETE r",
        )
        .expect("child: uncommitted base delete");
    println!("READY");
    std::io::stdout().flush().expect("child: flush");
    let _keep_alive = (&db, &session, &open_txn);
    loop {
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn sigkill_after_overlay_deletes_replays_them() {
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|a| a == "--overlay-delete-child") {
        child(std::path::Path::new(&args[pos + 1]));
    }

    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("crash.grafeo.d");
    std::fs::create_dir_all(&root).expect("root");
    {
        let source = GrafeoDB::new_in_memory();
        source
            .execute_cypher(
                "CREATE (a:MemoryEntity {name: 'a'}), (b:MemoryEntity {name: 'b'}), \
                 (a)-[:MemoryEntityRelation {rel_type: 'base'}]->(b)",
            )
            .expect("seed");
        source
            .build_and_publish_generation(generation_build_request(&root, "g1"))
            .expect("publish");
    }

    let mut child = Command::new(std::env::current_exe().expect("exe"))
        .args([
            "--exact",
            "sigkill_after_overlay_deletes_replays_them",
            "--nocapture",
            "--",
        ])
        .arg("--overlay-delete-child")
        .arg(&root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn child");
    let stdout = child.stdout.take().expect("stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            match line {
                Ok(l) if l.starts_with("READY") => {
                    let _ = tx.send(true);
                    return;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        let _ = tx.send(false);
    });
    let ready = rx.recv_timeout(Duration::from_secs(60)).unwrap_or(false);
    if !ready {
        let _ = child.kill();
        let mut err = String::new();
        if let Some(mut e) = child.stderr.take() {
            let _ = std::io::Read::read_to_string(&mut e, &mut err);
        }
        panic!("child never reported READY; stderr:\n{err}");
    }
    child.kill().expect("SIGKILL");
    let status = child.wait().expect("wait");
    assert_eq!(
        status.signal(),
        Some(libc::SIGKILL),
        "killed by SIGKILL: {status:?}"
    );

    let db = GrafeoDB::open_generation_root(&root, false).expect("reopen after SIGKILL");
    assert_eq!(
        count(&db, REL_X),
        0,
        "committed delete of overlay edge x replayed"
    );
    assert_eq!(
        count(&db, NODE_C),
        0,
        "committed delete of overlay node c replayed"
    );
    assert_eq!(
        count(&db, "MATCH (n:MemoryEntity {name: 'c2'}) RETURN count(n)"),
        0,
        "committed direct delete of overlay node c2 replayed"
    );
    assert_eq!(count(&db, REL_BASE), 1, "uncommitted base delete discarded");
}
