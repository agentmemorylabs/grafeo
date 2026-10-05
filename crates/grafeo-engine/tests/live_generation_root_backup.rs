//! Backup of a generation root that a live writable `GrafeoDB` is serving
//! (`GrafeoDB::backup_generation_root`).
//!
//! Proves:
//!
//! 1. A backup taken while another thread keeps writing restores into a
//!    fresh root that opens and is equivalent to the live database at the
//!    cut: node/edge counts, per-node property hashes, vectors (vector
//!    search results), and **every write acknowledged before the backup
//!    call is present**, with nothing partial (the restored `Item` sequence
//!    numbers form an unbroken prefix).
//! 2. The same holds with `NoSync` WAL durability (the cut flushes the WAL
//!    itself and does not rely on the commit path having fsynced).
//! 3. A backup requested while an epoch handoff is active is refused with a
//!    typed error and copies nothing; once the handoff finishes the backup
//!    succeeds and restores the new generation.
//! 4. A backup and a handoff racing each other either complete or refuse
//!    cleanly; every backup that succeeds restores and opens.
//! 5. The retirement authority of the database sees the backup's pin, and
//!    non-generation-root databases are refused.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "vector-index",
    feature = "gql"
))]

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use grafeo_common::types::{NodeId, PropertyKey, Value};
use grafeo_engine::GraphStore;
use grafeo_engine::config::DurabilityMode;
use grafeo_engine::{
    Config, EpochHandoffPhase, GrafeoDB, RetirementError, generation_build_request,
    restore_generation_root,
};
use tempfile::TempDir;

const LABEL: &str = "Doc";
const PROP: &str = "embedding";
const DIMS: usize = 8;
const BASE_DOCS: u64 = 64;

fn seeded_vector(seed: u64) -> Vec<f32> {
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut raw: Vec<f32> = (0..DIMS)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            ((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0
        })
        .collect();
    let norm: f32 = raw.iter().map(|x| x * x).sum::<f32>().sqrt();
    for x in &mut raw {
        *x /= norm;
    }
    raw
}

/// Publish a base generation with `BASE_DOCS` vector documents, a vector
/// index and a few edges at `root`.
fn publish_base(root: &Path) {
    let source = GrafeoDB::new_in_memory();
    let mut ids = Vec::new();
    for i in 0..BASE_DOCS {
        ids.push(
            source
                .create_node_with_props(
                    &[LABEL],
                    [
                        ("title", Value::from(format!("doc-{i}"))),
                        (PROP, Value::Vector(seeded_vector(i).into())),
                    ],
                )
                .expect("create doc"),
        );
    }
    for pair in ids.windows(2) {
        source.create_edge_with_props(pair[0], pair[1], "NEXT", [("w", Value::from(1i64))]);
    }
    source
        .create_vector_index(LABEL, PROP, Some(DIMS), Some("cosine"), None, None, None)
        .expect("create vector index");
    source
        .build_and_publish_generation(generation_build_request(root, "g-base"))
        .expect("publish base generation");
}

fn open_live(root: &Path, durability: DurabilityMode) -> GrafeoDB {
    let config = Config::persistent(root).with_wal_durability(durability);
    GrafeoDB::open_generation_root_with_config(config).expect("open live generation root")
}

/// Restore `backup_dir` into a fresh root under `dir` and open it read-only.
/// The restore's lock is released before the database opens the root.
fn restore_and_open(backup_dir: &Path, dir: &TempDir, name: &str) -> GrafeoDB {
    let new_root = dir.path().join(name);
    drop(restore_generation_root(backup_dir, &new_root).expect("restore backup"));
    GrafeoDB::open_generation_root(&new_root, true).expect("open restored root")
}

fn hash_of<T: Hash>(value: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// A stable, comparable view of one node: sorted labels and properties.
fn node_hash(db: &GrafeoDB, id: NodeId) -> u64 {
    let layered = db.layered_store().expect("layered store");
    let node = layered.get_node(id).expect("node exists");
    let mut labels: Vec<String> = node.labels.iter().map(ToString::to_string).collect();
    labels.sort_unstable();
    let mut props: Vec<(String, String)> = node
        .properties
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), format!("{v:?}")))
        .collect();
    props.sort_unstable();
    hash_of(&(labels, props))
}

/// Everything the equivalence check compares.
#[derive(Debug, PartialEq, Eq)]
struct Fingerprint {
    nodes: usize,
    edges: usize,
    node_hashes: BTreeMap<u64, u64>,
}

fn all_node_ids(db: &GrafeoDB) -> Vec<NodeId> {
    db.layered_store().expect("layered store").all_node_ids()
}

fn count(db: &GrafeoDB, query: &str) -> usize {
    match &db.session().execute(query).expect("count query").rows()[0][0] {
        Value::Int64(n) => usize::try_from(*n).expect("non-negative count"),
        other => panic!("expected integer count, got {other:?}"),
    }
}

fn fingerprint(db: &GrafeoDB) -> Fingerprint {
    Fingerprint {
        nodes: count(db, "MATCH (n) RETURN count(n)"),
        edges: count(db, "MATCH ()-[r]->() RETURN count(r)"),
        node_hashes: all_node_ids(db)
            .into_iter()
            .map(|id| (id.as_u64(), node_hash(db, id)))
            .collect(),
    }
}

/// The `seq` of every `Item` node, sorted.
fn item_seqs(db: &GrafeoDB) -> Vec<i64> {
    let layered = db.layered_store().expect("layered store");
    let mut seqs: Vec<i64> = all_node_ids(db)
        .into_iter()
        .filter_map(|id| layered.get_node(id))
        .filter_map(|n| match n.properties.get(&PropertyKey::new("seq")) {
            Some(Value::Int64(s)) => Some(*s),
            _ => None,
        })
        .collect();
    seqs.sort_unstable();
    seqs
}

/// The `Doc` base content: ids → hash, plus vector search results.
fn doc_view(db: &GrafeoDB) -> (BTreeMap<u64, u64>, Vec<Vec<u64>>) {
    let layered = db.layered_store().expect("layered store");
    let docs = all_node_ids(db)
        .into_iter()
        .filter(|id| {
            layered
                .get_node(*id)
                .is_some_and(|n| n.labels.iter().any(|l| l.as_str() == LABEL))
        })
        .map(|id| (id.as_u64(), node_hash(db, id)))
        .collect();
    let searches = [7u64, 42]
        .iter()
        .map(|seed| {
            db.vector_search(LABEL, PROP, &seeded_vector(*seed), 5, None, None)
                .expect("vector search")
                .into_iter()
                .map(|(id, _)| id.as_u64())
                .collect()
        })
        .collect();
    (docs, searches)
}

fn assert_prefix(seqs: &[i64], at_least: usize, what: &str) {
    let expected: Vec<i64> = (0..i64::try_from(seqs.len()).expect("len fits")).collect();
    assert_eq!(
        seqs, expected,
        "{what}: restored Item seqs must be an unbroken prefix 0..k"
    );
    assert!(
        seqs.len() >= at_least,
        "{what}: {at_least} writes were acknowledged before the backup call but only {} \
         were restored",
        seqs.len()
    );
}

fn dir_entries(path: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(path)
        .map(|rd| {
            rd.map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort_unstable();
    names
}

/// Back up a live root while another thread writes, restore it, and compare.
fn concurrent_writer_backup(durability: DurabilityMode) {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&root).expect("create root");
    publish_base(&root);
    let db = Arc::new(open_live(&root, durability));

    // Pre-existing WAL content so the copy has real bytes to cut.
    for i in 0..200i64 {
        db.session()
            .execute(&format!("INSERT (:Item {{seq: {i}}})"))
            .expect("seed write");
    }
    let acked = Arc::new(AtomicUsize::new(200));
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (db, acked, stop) = (Arc::clone(&db), Arc::clone(&acked), Arc::clone(&stop));
        std::thread::spawn(move || {
            let session = db.session();
            let mut next = 200i64;
            while !stop.load(Ordering::Relaxed) {
                session
                    .execute(&format!("INSERT (:Item {{seq: {next}}})"))
                    .expect("concurrent write");
                // Only counted as acknowledged once `execute` has returned.
                acked.store(usize::try_from(next).expect("fits") + 1, Ordering::SeqCst);
                next += 1;
            }
        })
    };

    // Let the writer get going, then back up while it keeps writing.
    std::thread::sleep(Duration::from_millis(50));
    let acked_before = acked.load(Ordering::SeqCst);
    let dest = dir.path().join("backups");
    let receipt = db
        .backup_generation_root(&dest, "live-1")
        .expect("live backup");
    let acked_during = acked.load(Ordering::SeqCst);

    // The writer is still going after the backup returned.
    let deadline = Instant::now() + Duration::from_secs(20);
    while acked.load(Ordering::SeqCst) <= acked_during + 5 {
        assert!(Instant::now() < deadline, "writer stalled after the backup");
        std::thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer thread");

    assert!(receipt.wal_files >= 1, "WAL captured: {receipt:?}");
    let restored = restore_and_open(&receipt.backup_dir, &dir, "restored");

    // Every write acknowledged before the call is present; nothing partial.
    let seqs = item_seqs(&restored);
    assert_prefix(&seqs, acked_before, "live backup");
    assert!(
        seqs.len() <= acked.load(Ordering::SeqCst),
        "restored more Items than were ever written"
    );

    // Base content (counts, per-node hashes, vectors) equals the live DB.
    assert_eq!(doc_view(&restored), doc_view(&db), "Doc nodes + vectors");

    // The restored root equals the live root at the cut: every restored
    // node (by id) hashes the same as the live node, and the counts agree
    // with what was restored.
    let restored_fp = fingerprint(&restored);
    let live_fp = fingerprint(&db);
    assert_eq!(
        restored_fp.nodes,
        usize::try_from(BASE_DOCS).unwrap() + seqs.len()
    );
    assert_eq!(restored_fp.edges, live_fp.edges);
    for (id, hash) in &restored_fp.node_hashes {
        assert_eq!(
            live_fp.node_hashes.get(id),
            Some(hash),
            "node {id} differs between restored root and live root"
        );
    }
    assert_eq!(
        item_seqs(&db).len(),
        live_fp.nodes - usize::try_from(BASE_DOCS).unwrap()
    );

    // The live database kept working and is unaffected.
    db.session()
        .execute("INSERT (:Item {seq: 99999999})")
        .expect("write after backup");
    // The pin is released again.
    assert!(db.retirement_authority().unwrap().active_pins().is_empty());
}

#[test]
fn backup_of_live_root_with_concurrent_writer_restores_equivalent() {
    concurrent_writer_backup(DurabilityMode::Sync);
}

#[test]
fn backup_of_live_root_is_consistent_with_nosync_wal() {
    concurrent_writer_backup(DurabilityMode::NoSync);
}

#[test]
fn restored_live_backup_is_writable_and_reopens_identically() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&root).expect("create root");
    publish_base(&root);
    let db = open_live(&root, DurabilityMode::Sync);
    for i in 0..50i64 {
        db.session()
            .execute(&format!("INSERT (:Item {{seq: {i}}})"))
            .expect("write");
    }
    let receipt = db
        .backup_generation_root(dir.path().join("backups"), "quiet")
        .expect("backup");

    // Quiesced: the backup equals the live database exactly.
    let new_root = dir.path().join("restored");
    drop(restore_generation_root(&receipt.backup_dir, &new_root).expect("restore"));
    let restored = GrafeoDB::open_generation_root(&new_root, false).expect("open writable");
    assert_eq!(fingerprint(&restored), fingerprint(&db));
    assert_eq!(doc_view(&restored), doc_view(&db));
    restored
        .session()
        .execute("INSERT (:Item {seq: 50})")
        .expect("restored root accepts writes");
    drop(restored);
    let reopened = GrafeoDB::open_generation_root(&new_root, true).expect("reopen");
    assert_eq!(item_seqs(&reopened), (0..=50).collect::<Vec<i64>>());
}

#[test]
fn backup_during_handoff_is_refused_cleanly_then_succeeds() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&root).expect("create root");
    publish_base(&root);
    let db = open_live(&root, DurabilityMode::Sync);
    for i in 0..10i64 {
        db.session()
            .execute(&format!("INSERT (:Item {{seq: {i}}})"))
            .expect("write");
    }
    let dest = dir.path().join("backups");

    // Freeze held: the handoff is mid-publication from the backup's view.
    let handle = db.freeze_epoch_for_handoff(&root).expect("freeze");
    assert_eq!(db.epoch_handoff_phase(), EpochHandoffPhase::FreezeCaptured);
    let err = db
        .backup_generation_root(&dest, "during")
        .expect_err("backup must refuse while a handoff is active");
    assert!(
        matches!(err, RetirementError::HandoffInProgress("freeze_captured")),
        "typed refusal expected, got {err:?}"
    );
    assert!(
        dir_entries(&dest).is_empty(),
        "a refused backup must leave nothing behind: {:?}",
        dir_entries(&dest)
    );
    assert!(db.retirement_authority().unwrap().active_pins().is_empty());

    // Finish the handoff; the backup now succeeds and carries the new
    // generation (sequence 2) plus the writes made after the freeze.
    let report = db
        .complete_epoch_handoff(handle, generation_build_request(&root, "g-hand"))
        .expect("complete handoff");
    db.publish_and_install_handoff(report).expect("install");
    db.session()
        .execute("INSERT (:Item {seq: 10})")
        .expect("write after handoff");
    let receipt = db
        .backup_generation_root(&dest, "after")
        .expect("backup after handoff");
    assert_eq!(receipt.generation_id, "g-hand");
    let restored = restore_and_open(&receipt.backup_dir, &dir, "restored");
    assert_prefix(&item_seqs(&restored), 11, "after handoff");
    assert_eq!(fingerprint(&restored), fingerprint(&db));
}

#[test]
fn backup_racing_handoffs_completes_or_refuses_cleanly() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&root).expect("create root");
    publish_base(&root);
    let db = Arc::new(open_live(&root, DurabilityMode::Sync));
    let written = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(2));

    // One thread both writes and hands off, so writers are drained at every
    // freeze (the documented requirement).
    let handoffs = {
        let (db, written, stop, start, root) = (
            Arc::clone(&db),
            Arc::clone(&written),
            Arc::clone(&stop),
            Arc::clone(&start),
            root.clone(),
        );
        std::thread::spawn(move || {
            start.wait();
            let (mut done, mut refused) = (0usize, 0usize);
            let mut next = 0i64;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..5 {
                    db.session()
                        .execute(&format!("INSERT (:Item {{seq: {next}}})"))
                        .expect("write");
                    next += 1;
                    written.store(usize::try_from(next).expect("fits"), Ordering::SeqCst);
                }
                match db
                    .run_epoch_handoff(generation_build_request(&root, format!("g-race-{done}")))
                {
                    Ok(report) => {
                        db.publish_and_install_handoff(report).expect("install");
                        done += 1;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        assert!(
                            msg.contains("backup is in progress"),
                            "handoff may only be refused by a running backup: {msg}"
                        );
                        refused += 1;
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
            }
            (done, refused)
        })
    };

    start.wait();
    let dest = dir.path().join("backups");
    let (mut taken, mut refused) = (0usize, 0usize);
    let mut kept: Vec<(usize, std::path::PathBuf)> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut n = 0usize;
    while Instant::now() < deadline || taken == 0 {
        let acked_before = written.load(Ordering::SeqCst);
        match db.backup_generation_root(&dest, &format!("race-{n}")) {
            Ok(receipt) => {
                taken += 1;
                kept.push((acked_before, receipt.backup_dir));
            }
            Err(RetirementError::HandoffInProgress(_)) => refused += 1,
            Err(other) => panic!("a racing backup may only succeed or be refused: {other:?}"),
        }
        n += 1;
        assert!(n < 10_000, "no backup ever succeeded");
        // Back-to-back backups would hold the gate continuously and starve
        // the handoff thread; real callers back up at intervals.
        std::thread::sleep(Duration::from_millis(15));
    }
    stop.store(true, Ordering::Relaxed);
    let (handoffs_done, handoffs_refused) = handoffs.join().expect("handoff thread");
    assert!(handoffs_done >= 1, "no handoff completed during the race");
    eprintln!(
        "race: backups ok={taken} refused={refused}; handoffs ok={handoffs_done} refused={handoffs_refused}"
    );

    // Every backup that succeeded restores, opens, and holds everything
    // acknowledged before it started.
    let mut seen = BTreeSet::new();
    for (i, (acked_before, backup_dir)) in kept.iter().enumerate() {
        let restored = restore_and_open(backup_dir, &dir, &format!("restored-{i}"));
        let seqs = item_seqs(&restored);
        assert_prefix(&seqs, *acked_before, &format!("race backup {i}"));
        seen.insert(seqs.len());
    }
    assert!(db.retirement_authority().unwrap().active_pins().is_empty());
}

#[test]
fn backup_pin_is_visible_to_the_database_retirement_authority() {
    // The pin is held for the copy and released on return; a refused call
    // never leaves one. (The pin itself is covered by the retirement tests;
    // this checks the database wires its authority to the backup path.)
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&root).expect("create root");
    publish_base(&root);
    let db = open_live(&root, DurabilityMode::Sync);
    let auth = db
        .retirement_authority()
        .expect("generation root authority");
    assert_eq!(auth.root(), std::fs::canonicalize(&root).unwrap());
    assert!(auth.active_pins().is_empty());
    db.backup_generation_root(dir.path().join("b"), "one")
        .expect("backup");
    assert!(auth.active_pins().is_empty(), "pin released after backup");

    // Existing backups are never overwritten.
    let err = db
        .backup_generation_root(dir.path().join("b"), "one")
        .expect_err("duplicate backup name");
    assert!(
        matches!(err, RetirementError::ValidationFailed(_)),
        "{err:?}"
    );
    // Destination inside the live root and unsafe names are refused.
    assert!(matches!(
        db.backup_generation_root(root.join("inside"), "x"),
        Err(RetirementError::DestinationInsideRoot)
    ));
    assert!(matches!(
        db.backup_generation_root(dir.path().join("b"), "../x"),
        Err(RetirementError::UnsafeName)
    ));
}

#[test]
fn non_generation_root_database_is_refused() {
    let db = GrafeoDB::new_in_memory();
    assert!(db.retirement_authority().is_none());
    let dir = TempDir::new().expect("temp dir");
    assert!(matches!(
        db.backup_generation_root(dir.path(), "x"),
        Err(RetirementError::NotGenerationRoot)
    ));
}
