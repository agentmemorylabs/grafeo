//! G-EM0.5c — concurrent overlay epoch and WAL handoff.
//!
//! Proves:
//! 1. Freeze epoch N at WAL boundary B while concurrent N+1 writes are admitted.
//! 2. Build G(N) from the freeze snapshot and publish with pre-cut boundary B.
//! 3. Retire only the frozen overlay prefix; N+1 mutations remain exactly once.
//! 4. Dual-epoch backpressure: frozen + next-epoch retained bytes both count.
//! 5. Fresh-process recovery after freeze/publication fault injection.
//! 6. Linearization: checkpoint/phase observability and pre-commit cancel.

#![cfg(all(feature = "generation", feature = "compact-store", feature = "lpg"))]

use std::fs;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use grafeo_common::storage::SectionType;
use grafeo_common::types::{NodeId, PropertyKey, Value};
use grafeo_core::graph::compact::overlay_budget::{
    OverlayAdmissionController, OverlayBudgetConfig, RetainedCategory,
};
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_core::graph::traits::{GraphStore, GraphStoreMut};
use grafeo_engine::{
    EpochHandoffPhase, GrafeoDB, generation_build_request, read_manifest_state,
    recover_generation_root,
};
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::generation::wal_cursor::validate_replayable;
use tempfile::TempDir;

const HELPER_ENV: &str = "GRAFEO5C_HELPER";

fn populate(db: &GrafeoDB, tag: &str) {
    let a = db
        .create_node_with_props(&["Person"], [("name", Value::from(format!("{tag}-a")))])
        .expect("node a");
    let b = db
        .create_node_with_props(&["Person"], [("name", Value::from(format!("{tag}-b")))])
        .expect("node b");
    let _e = db.create_edge(a, b, "KNOWS");
}

fn generation_person_names(path: &std::path::Path) -> Vec<String> {
    let manager = GrafeoFileManager::open_read_only(path).expect("open generation");
    let section_dir = manager
        .read_section_directory()
        .expect("section directory")
        .expect("directory present");
    let entry = section_dir
        .find(SectionType::CompactStore)
        .expect("CompactStore section present");
    let data = manager.read_section_data(entry).expect("read section");
    let mut cs_section = CompactStoreSection::empty();
    cs_section
        .deserialize_from_bytes(Bytes::from(data))
        .expect("deserialize");
    let store = cs_section.store().expect("store");
    let mut names = Vec::new();
    if let Some(person) = store.node_table("Person") {
        for offset in 0..person.len() {
            if let Some(Value::String(name)) =
                person.get_property(offset, &PropertyKey::new("name"))
            {
                names.push(name.as_str().to_string());
            }
        }
    }
    names.sort_unstable();
    names
}

fn in_any_child() -> bool {
    [HELPER_ENV, "GRAFEORECOV_HELPER", "GRAFEOPUB_HELPER"]
        .iter()
        .any(|v| std::env::var(v).is_ok())
}

fn db_layered_with_admission() -> (GrafeoDB, Arc<OverlayAdmissionController>) {
    let mut db = GrafeoDB::new_in_memory();
    populate(&db, "base");
    db.compact().expect("compact");
    let ctl = Arc::new(
        OverlayAdmissionController::new(OverlayBudgetConfig::for_tests()).expect("config"),
    );
    db.install_overlay_admission(Arc::clone(&ctl));
    (db, ctl)
}

// ── Happy path ─────────────────────────────────────────────────────

#[test]
fn freeze_build_publish_retires_only_epoch_n_prefix() {
    if in_any_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();
    let gen_root = fs::canonicalize(&gen_root).unwrap();

    let (db, _ctl) = db_layered_with_admission();
    // Overlay mutations at epoch N through LayeredStore (marks dirty + admission).
    let layered = db.layered_store().unwrap();
    let a = layered.create_node(&["Person"]);
    layered.set_node_property(a, "name", Value::from("epoch-n-a"));
    let b = layered.create_node(&["Person"]);
    layered.set_node_property(b, "name", Value::from("epoch-n-b"));
    let _e = layered.create_edge(a, b, "KNOWS");

    let handle = db
        .freeze_epoch_for_handoff(&gen_root)
        .expect("freeze epoch N");
    assert_eq!(db.epoch_handoff_phase(), EpochHandoffPhase::FreezeCaptured);
    assert!(db.epoch_handoff_active());
    assert_eq!(handle.next_epoch, handle.frozen_epoch + 1);
    assert!(
        !handle.freeze.overlay_node_ids.is_empty(),
        "freeze must capture epoch-N overlay nodes"
    );

    // Concurrent N+1 write after freeze through LayeredStore.
    let layered = db.layered_store().unwrap();
    let n1 = layered.create_node(&["Person"]);
    layered.set_node_property(n1, "name", Value::from("epoch-n1-only"));
    assert!(layered.get_node(n1).is_some());

    let report = db
        .complete_epoch_handoff(handle, generation_build_request(&gen_root, "g-handoff-1"))
        .expect("complete handoff");

    assert_eq!(report.phase, EpochHandoffPhase::EpochRetired);
    assert!(!db.epoch_handoff_active());
    assert!(report.publication.is_some());
    assert!(
        report.retained_next_epoch_nodes >= 1,
        "N+1 node must remain applied once: {report:?}"
    );
    assert!(
        report.absorbed_nodes >= 1,
        "frozen epoch-N nodes must be absorbed: {report:?}"
    );

    // Live overlay still shows N+1 node.
    let layered = db.layered_store().unwrap();
    let live_names: Vec<String> = layered
        .all_node_ids()
        .into_iter()
        .filter_map(|id| layered.get_node(id))
        .filter_map(|n| {
            n.properties
                .get(&PropertyKey::new("name"))
                .and_then(|v| match v {
                    Value::String(s) => Some(s.as_str().to_string()),
                    _ => None,
                })
        })
        .collect();
    assert!(
        live_names.iter().any(|n| n == "epoch-n1-only"),
        "N+1 must remain on live store: {live_names:?}"
    );

    // Published generation includes epoch-N data, not N+1-only.
    let pubn = report.publication.as_ref().unwrap();
    let names = generation_person_names(&pubn.generation_abs_path);
    assert!(
        names.iter().any(|n| n.contains("epoch-n-")),
        "G(N) must include frozen epoch-N names: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "epoch-n1-only"),
        "G(N) must NOT include post-freeze N+1-only node: {names:?}"
    );

    // Manifest boundary matches freeze cut and is replayable.
    let state = read_manifest_state(&gen_root).expect("manifest");
    assert_eq!(
        state.selected.wal_boundary, report.wal_boundary,
        "descriptor and manifest must agree on B"
    );
    validate_replayable(&gen_root.join("wal"), &report.wal_boundary.to_cursor())
        .expect("boundary B replayable");
}

#[test]
fn run_epoch_handoff_one_shot() {
    if in_any_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "oneshot");
    let report = db
        .run_epoch_handoff(generation_build_request(&gen_root, "g-oneshot"))
        .expect("one-shot handoff");
    assert_eq!(report.phase, EpochHandoffPhase::EpochRetired);
    assert_eq!(
        report
            .publication
            .as_ref()
            .unwrap()
            .publication
            .generation_id,
        "g-oneshot"
    );
    let recovery = recover_generation_root(&gen_root).expect("recover");
    assert_eq!(recovery.selected.slot.generation_id, "g-oneshot");
}

// ── Dual-epoch accounting ──────────────────────────────────────────

#[test]
fn dual_epoch_backpressure_counts_frozen_and_next() {
    if in_any_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let (db, ctl) = db_layered_with_admission();
    // Fill mutation payload under epoch N.
    for i in 0..20 {
        let n = db.layered_store().unwrap().create_node(&["Person"]);
        db.layered_store().unwrap().set_node_property(
            n,
            "bio",
            Value::String(arcstr::ArcStr::from(format!(
                "payload-{i}-{}",
                "x".repeat(64)
            ))),
        );
    }
    let before_freeze = ctl.snapshot();
    let frozen_payload =
        before_freeze.categories[RetainedCategory::MutationPayload.index()].current_bytes;
    assert!(frozen_payload > 0, "epoch N must charge MutationPayload");

    let _handle = db.freeze_epoch_for_handoff(&gen_root).expect("freeze");
    // Post-freeze writes charge NextEpoch.
    for i in 0..5 {
        let n = db.layered_store().unwrap().create_node(&["Person"]);
        db.layered_store().unwrap().set_node_property(
            n,
            "bio",
            Value::String(arcstr::ArcStr::from(format!("next-{i}-{}", "y".repeat(64)))),
        );
    }
    let after = ctl.snapshot();
    let next_bytes = after.categories[RetainedCategory::NextEpoch.index()].current_bytes;
    let frozen_still = after.categories[RetainedCategory::MutationPayload.index()].current_bytes;
    assert!(
        next_bytes > 0,
        "N+1 writes must charge NextEpoch, got snapshot={after:?}"
    );
    assert!(
        frozen_still > 0,
        "frozen MutationPayload must still count during handoff"
    );
    assert_eq!(
        after.total_bytes,
        after
            .categories
            .iter()
            .map(|c| c.current_bytes)
            .sum::<u64>(),
        "total must equal sum of categories (frozen + next)"
    );
    assert!(
        after.total_bytes >= frozen_still.saturating_add(next_bytes),
        "aggregate must include both epochs"
    );

    db.cancel_epoch_handoff();
    assert!(!db.epoch_handoff_active());
}

// ── Linearization / cancel ─────────────────────────────────────────

#[test]
fn cancel_before_publish_leaves_prior_recoverable() {
    if in_any_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "first");
    // Establish a prior generation via ordinary build.
    let first = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-prior"))
        .expect("prior publish");

    populate(&db, "pending");
    let _handle = db.freeze_epoch_for_handoff(&gen_root).expect("freeze");
    assert_eq!(db.epoch_handoff_phase(), EpochHandoffPhase::FreezeCaptured);
    db.cancel_epoch_handoff();
    assert_eq!(db.epoch_handoff_phase(), EpochHandoffPhase::Cancelled);

    let recovery = recover_generation_root(&gen_root).expect("recover after cancel");
    assert_eq!(
        recovery.selected.slot.generation_id, "g-prior",
        "cancel must not change selected generation"
    );
    assert_eq!(recovery.wal_boundary, first.publication.wal_boundary);
}

#[test]
fn phase_ordering_is_observable() {
    if in_any_child() {
        return;
    }
    assert!(EpochHandoffPhase::Idle < EpochHandoffPhase::FreezeCaptured);
    assert!(EpochHandoffPhase::FreezeCaptured < EpochHandoffPhase::Building);
    assert!(EpochHandoffPhase::Building < EpochHandoffPhase::Published);
    assert!(EpochHandoffPhase::Published < EpochHandoffPhase::EpochRetired);
    assert!(!EpochHandoffPhase::FreezeCaptured.is_post_commit());
    assert!(EpochHandoffPhase::Published.is_post_commit());
    assert!(EpochHandoffPhase::EpochRetired.is_post_commit());
}

// ── Fresh-process fault recovery ───────────────────────────────────

/// Parent: spawn a child that freezes then aborts after freeze; prove the
/// prior selected generation (if any) remains recoverable and no silent
/// promotion of a half-built generation occurs.
#[test]
fn fresh_process_abort_after_freeze_keeps_prior_selection() {
    if in_any_child() {
        // Child path.
        let root = std::env::var("GRAFEO5C_ROOT").expect("root");
        let mode = std::env::var("GRAFEO5C_MODE").expect("mode");
        let gen_root = std::path::PathBuf::from(&root);
        let db = GrafeoDB::new_in_memory();
        populate(&db, "child");
        match mode.as_str() {
            "abort_after_freeze" => {
                let _ = db.freeze_epoch_for_handoff(&gen_root).expect("freeze");
                // Simulate crash before complete.
                std::process::abort();
            }
            "abort_after_publication" => {
                // GRAFEO_5C_ABORT is set by the parent Command env.
                let _ = db
                    .run_epoch_handoff(generation_build_request(&gen_root, "g-crash-pub"))
                    .expect("should abort inside");
            }
            other => panic!("unknown mode {other}"),
        }
        return;
    }

    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    // Establish prior generation.
    {
        let db = GrafeoDB::new_in_memory();
        populate(&db, "prior");
        db.build_and_publish_generation(generation_build_request(&gen_root, "g-prior"))
            .expect("prior");
    }

    let status = Command::new(std::env::current_exe().expect("exe"))
        .env(HELPER_ENV, "1")
        .env("GRAFEO5C_ROOT", &gen_root)
        .env("GRAFEO5C_MODE", "abort_after_freeze")
        .env("RUST_BACKTRACE", "0")
        .status()
        .expect("spawn child");
    assert!(
        !status.success(),
        "child must abort (non-success): {status}"
    );

    let recovery = recover_generation_root(&gen_root).expect("recover after freeze abort");
    assert_eq!(
        recovery.selected.slot.generation_id, "g-prior",
        "freeze abort must leave prior generation selected"
    );
}

#[test]
fn fresh_process_abort_after_publication_selects_new() {
    if in_any_child() {
        return; // handled in the sibling helper test above when mode matches
    }

    // Only run the publication-abort path when debug_assertions is on
    // (GRAFEO_5C_ABORT is compiled out of release).
    #[cfg(not(debug_assertions))]
    {
        return;
    }

    #[cfg(debug_assertions)]
    {
        let dir = TempDir::new().unwrap();
        let gen_root = dir.path().join("live.grafeo.d");
        fs::create_dir_all(&gen_root).unwrap();

        {
            let db = GrafeoDB::new_in_memory();
            populate(&db, "prior");
            db.build_and_publish_generation(generation_build_request(&gen_root, "g-prior"))
                .expect("prior");
        }

        let status = Command::new(std::env::current_exe().expect("exe"))
            .env(HELPER_ENV, "1")
            .env("GRAFEO5C_ROOT", &gen_root)
            .env("GRAFEO5C_MODE", "abort_after_publication")
            .env("GRAFEO_5C_ABORT", "after_publication")
            .env("RUST_BACKTRACE", "0")
            .status()
            .expect("spawn child");
        assert!(!status.success(), "child must abort after publication");

        let recovery = recover_generation_root(&gen_root).expect("recover after pub abort");
        assert_eq!(
            recovery.selected.slot.generation_id, "g-crash-pub",
            "post-commit abort must leave NEW generation selected"
        );
        validate_replayable(&gen_root.join("wal"), &recovery.wal_boundary.to_cursor())
            .expect("new boundary replayable");
    }
}

#[test]
fn second_handoff_advances_sequence_and_retains_previous() {
    if in_any_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();
    let gen_root = fs::canonicalize(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "first");
    let r1 = db
        .run_epoch_handoff(generation_build_request(&gen_root, "g-1"))
        .expect("first");
    assert!(
        !db.epoch_handoff_active(),
        "first handoff must fully retire"
    );
    populate(&db, "second");
    let r2 = db
        .run_epoch_handoff(generation_build_request(&gen_root, "g-2"))
        .expect("second");

    assert_eq!(
        r1.publication
            .as_ref()
            .unwrap()
            .publication
            .publication_sequence,
        1
    );
    assert_eq!(
        r2.publication
            .as_ref()
            .unwrap()
            .publication
            .publication_sequence,
        2
    );

    let state = read_manifest_state(&gen_root).expect("manifest");
    assert_eq!(state.selected.generation_id, "g-2");
    assert_eq!(
        state.previous.as_ref().map(|p| p.generation_id.as_str()),
        Some("g-1")
    );
}

// ── MAJOR-1: freeze is a writer linearization point ──────────────

/// Freeze must be a writer linearization point (G-EM0.5c review MAJOR-1).
///
/// `freeze_epoch_for_handoff` holds the layered store's merge-guard WRITE
/// barrier across WAL cut → epoch sync → payload capture → `begin_epoch_handoff`,
/// so a concurrent `GraphStoreMut` writer (which holds the same guard as
/// `.read()` for the whole mutation) cannot land between the cut and the
/// capture. This test stalls the freeze *inside* the barrier right before the
/// capture and proves:
/// 1. a concurrent writer is completely blocked for the entire capture window
///    (pre-fix it would sail through and be half-captured or write a record
///    after boundary B into a captured entity — a torn/duplicated G(N));
/// 2. once the freeze installs and drops the barrier, the writer lands
///    strictly as an N+1 entity: excluded from the frozen snapshot, tracked in
///    `post_freeze_nodes`, retained live after retire, and NOT in G(N).
#[cfg(debug_assertions)]
#[test]
fn freeze_is_a_writer_linearization_point() {
    if in_any_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();
    let gen_root = fs::canonicalize(&gen_root).unwrap();

    let mut db = GrafeoDB::new_in_memory();
    populate(&db, "base");
    db.compact().expect("compact");
    let db = Arc::new(db);

    let layered = db.layered_store().unwrap();
    // Seed frozen epoch-N content through the layered store.
    let a = layered.create_node(&["Person"]);
    layered.set_node_property(a, "name", Value::from("frozen-lin-a"));
    let b = layered.create_node(&["Person"]);
    layered.set_node_property(b, "name", Value::from("frozen-lin-b"));

    // Arm the stall: freeze parks inside its writer barrier right before
    // capturing the overlay payloads (barrier still held).
    grafeo_engine::FREEZE_STALL_BEFORE_CAPTURE.store(true, Ordering::SeqCst);

    let db_f = Arc::clone(&db);
    let root_f = gen_root.clone();
    let freeze_thread = thread::spawn(move || {
        db_f.freeze_epoch_for_handoff(&root_f)
            .expect("freeze epoch N")
    });

    // Wait until the freeze is parked inside the critical section.
    let deadline = Instant::now() + Duration::from_secs(15);
    while !grafeo_engine::FREEZE_STALL_ENTERED.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "freeze never reached the stall");
        thread::sleep(Duration::from_millis(1));
    }

    // A concurrent writer attempts a mutation while the freeze holds the
    // writer barrier; it must be completely blocked across the capture.
    let layered_w = Arc::clone(&layered);
    let (tx_started, rx_started) = mpsc::channel();
    let (tx_done, rx_done) = mpsc::channel();
    let writer = thread::spawn(move || {
        let _ = tx_started.send(());
        let n = layered_w.create_node(&["Person"]);
        layered_w.set_node_property(n, "name", Value::from("concurrent-lin"));
        let _ = tx_done.send(n);
        n
    });
    rx_started.recv().expect("writer started");
    // Give the writer time to reach `merge_guard.read()` and block.
    thread::sleep(Duration::from_millis(200));
    assert!(
        rx_done.try_recv().is_err(),
        "concurrent writer must be blocked while the freeze holds the barrier"
    );

    // Release the stall: freeze completes cut → capture → install and drops
    // the barrier; only then does the writer land.
    grafeo_engine::FREEZE_STALL_BEFORE_CAPTURE.store(false, Ordering::SeqCst);

    let handle = freeze_thread.join().expect("freeze thread");
    let nid = writer.join().expect("writer thread");

    // The concurrent write is strictly post-freeze: excluded from the frozen
    // G(N) snapshot and tracked as an N+1-retained mutation.
    assert!(
        !handle.freeze.overlay_node_ids.contains(&nid.as_u64()),
        "concurrent write must NOT be in the frozen capture"
    );
    let live = db
        .layered_store()
        .unwrap()
        .handoff_live()
        .expect("live handoff");
    assert!(
        live.post_freeze_nodes.contains(&nid.as_u64()),
        "concurrent write must be tracked as post-freeze (N+1)"
    );

    let report = db
        .complete_epoch_handoff(handle, generation_build_request(&gen_root, "g-lin"))
        .expect("complete");
    let names = generation_person_names(&report.publication.as_ref().unwrap().generation_abs_path);
    assert!(
        names
            .iter()
            .any(|n| n == "frozen-lin-a" || n == "frozen-lin-b"),
        "G(N) must include the frozen content: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "concurrent-lin"),
        "G(N) must NOT include the concurrent write: {names:?}"
    );
    assert!(
        db.layered_store().unwrap().get_node(nid).is_some(),
        "concurrent write must remain live after retire (N+1 retained)"
    );
}

// ── MAJOR-2: retire is non-destructive on the live read view ─────

/// Retirement must preserve the live read view exactly (G-EM0.5c review
/// MAJOR-2).
///
/// The base `Arc` is not swapped until G-EM0.5d, so
/// `retire_frozen_overlay_prefix` must NOT delete overlay payloads or clear
/// `deleted_from_base_*` tombstones — doing so would regress a base-modified
/// frozen node to its stale base value, resurrect a base-deleted node, and
/// make an overlay-only node vanish. This test snapshots `get_node` for one
/// of each kind before retire and asserts the read view is *identical* after
/// retire, while absorbed/retained counts are still computed as pure set
/// arithmetic.
#[test]
fn retire_keeps_live_reads_identical() {
    if in_any_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();
    let gen_root = fs::canonicalize(&gen_root).unwrap();

    let (db, _ctl) = db_layered_with_admission();
    let layered = db.layered_store().unwrap();

    // `populate("base")` created two base nodes before compact.
    let base_ids: Vec<NodeId> = layered.base_store_arc().all_node_ids();
    assert!(base_ids.len() >= 2, "populate must seed two base nodes");
    let (base_modified, base_deleted) = (base_ids[0], base_ids[1]);

    let name_of = |layered: &Arc<grafeo_core::graph::compact::layered::LayeredStore>,
                   id: NodeId|
     -> Option<String> {
        layered
            .get_node(id)
            .and_then(|n| match n.properties.get(&PropertyKey::new("name")) {
                Some(Value::String(s)) => Some(s.as_str().to_string()),
                _ => None,
            })
    };

    // 1. Base-modified frozen node: overlay copy supersedes the base value.
    layered.set_node_property(base_modified, "name", Value::from("base-modified-v2"));
    // 2. Base-deleted frozen node: tombstone must keep it invisible. The base
    //    KNOWS edge from `populate("base")` references it, so tombstone that
    //    edge first (realistic cascade delete) or the generation build fails
    //    on a missing endpoint.
    layered.delete_node_edges(base_deleted);
    assert!(layered.delete_node(base_deleted), "delete base node");
    // 3. Overlay-only frozen node: exists only in the overlay.
    let overlay_only = layered.create_node(&["Person"]);
    layered.set_node_property(overlay_only, "name", Value::from("overlay-only"));

    let ids = [base_modified, base_deleted, overlay_only];
    let before: Vec<Option<String>> = ids.iter().map(|id| name_of(&layered, *id)).collect();
    assert_eq!(
        before[0].as_deref(),
        Some("base-modified-v2"),
        "precondition: base-modified must read as the overlay value"
    );
    assert_eq!(
        before[1], None,
        "precondition: base-deleted must be invisible"
    );
    assert_eq!(before[2].as_deref(), Some("overlay-only"));

    let handle = db.freeze_epoch_for_handoff(&gen_root).expect("freeze");
    // One N+1 write so both absorbed and retained paths are exercised.
    let n1 = layered.create_node(&["Person"]);
    layered.set_node_property(n1, "name", Value::from("n1-retained"));
    let report = db
        .complete_epoch_handoff(handle, generation_build_request(&gen_root, "g-nondestr"))
        .expect("complete");

    // Absorbed/retained counts remain correct (pure set arithmetic over the
    // freeze and post-freeze id sets — no live-state mutation involved).
    assert_eq!(
        report.absorbed_nodes, 2,
        "absorbed = frozen overlay nodes not re-mutated: {report:?}"
    );
    assert_eq!(
        report.retained_next_epoch_nodes, 1,
        "retained = post-freeze nodes: {report:?}"
    );

    // The live read view is EXACTLY what it was before retire for every entity.
    let after: Vec<Option<String>> = ids.iter().map(|id| name_of(&layered, *id)).collect();
    assert_eq!(after, before, "retire must not change live reads");
    assert_eq!(
        after[0].as_deref(),
        Some("base-modified-v2"),
        "base-modified must NOT regress to its stale base value"
    );
    assert_eq!(after[1], None, "base-deleted must NOT resurrect");
    assert_eq!(
        after[2].as_deref(),
        Some("overlay-only"),
        "overlay-only must NOT vanish"
    );
    assert!(
        layered.get_node(n1).is_some(),
        "N+1 write must remain applied once"
    );
}

// ── MAJOR-4: NextEpoch accounting must not ratchet the budget ────

/// Next-epoch charges must be re-attributed at retire, not leaked (G-EM0.5c
/// review MAJOR-4).
///
/// `charge_retained` reroutes post-freeze MutationPayload charges to
/// `NextEpoch`; on retire those surviving N+1 entities become the next
/// cycle's working set, so their bytes must move `NextEpoch -> MutationPayload`
/// (aggregate unchanged) while the absorbed frozen bytes are released. With no
/// release site the budget would ratchet every handoff. This test runs two
/// consecutive handoffs and asserts the controller's `total_bytes` after each
/// retire equals exactly that cycle's own post-freeze working set — no growth
/// beyond the genuine live working set, `NextEpoch` drained, no accounting
/// errors.
#[test]
fn next_epoch_reattribution_prevents_budget_ratchet() {
    if in_any_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();
    let gen_root = fs::canonicalize(&gen_root).unwrap();

    let (db, ctl) = db_layered_with_admission();
    let layered = db.layered_store().unwrap();

    let bio = |i: usize| Value::String(arcstr::ArcStr::from(format!("w-{i}-{}", "x".repeat(128))));

    // ── Cycle 1: 3 frozen nodes, 2 post-freeze nodes ────────────────────
    for i in 0..3 {
        let n = layered.create_node(&["Person"]);
        layered.set_node_property(n, "bio", bio(i));
    }
    let h1 = db.freeze_epoch_for_handoff(&gen_root).expect("freeze 1");
    for i in 3..5 {
        let n = layered.create_node(&["Person"]);
        layered.set_node_property(n, "bio", bio(i));
    }
    let mid1 = ctl.snapshot();
    let next1 = mid1.categories[RetainedCategory::NextEpoch.index()].current_bytes;
    let frozen_mp1 = mid1.categories[RetainedCategory::MutationPayload.index()].current_bytes;
    assert!(
        next1 > 0,
        "post-freeze cycle-1 writes must charge NextEpoch: {mid1:?}"
    );
    assert!(
        frozen_mp1 > 0,
        "frozen cycle-1 payload must still be charged: {mid1:?}"
    );
    let mid1_total = mid1.total_bytes;

    db.complete_epoch_handoff(h1, generation_build_request(&gen_root, "g-1"))
        .expect("complete 1");

    let after1 = ctl.snapshot();
    assert_eq!(
        after1.categories[RetainedCategory::NextEpoch.index()].current_bytes,
        0,
        "NextEpoch must drain on retire: {after1:?}"
    );
    assert_eq!(
        after1.categories[RetainedCategory::MutationPayload.index()].current_bytes,
        next1,
        "N+1 working-set bytes must re-attribute to MutationPayload: {after1:?}"
    );
    assert_eq!(
        after1.total_bytes,
        mid1_total.saturating_sub(frozen_mp1),
        "retire re-attribution must be total-neutral (release frozen only): {after1:?}"
    );

    // ── Cycle 2: freeze the retained working set, add 3 more post-freeze ──
    let h2 = db.freeze_epoch_for_handoff(&gen_root).expect("freeze 2");
    for i in 5..8 {
        let n = layered.create_node(&["Person"]);
        layered.set_node_property(n, "bio", bio(i));
    }
    let mid2 = ctl.snapshot();
    let next2 = mid2.categories[RetainedCategory::NextEpoch.index()].current_bytes;
    assert!(
        next2 > 0,
        "cycle-2 post-freeze writes must charge NextEpoch: {mid2:?}"
    );

    db.complete_epoch_handoff(h2, generation_build_request(&gen_root, "g-2"))
        .expect("complete 2");

    let after2 = ctl.snapshot();
    assert_eq!(
        after2.categories[RetainedCategory::NextEpoch.index()].current_bytes,
        0,
        "NextEpoch must drain on the second retire: {after2:?}"
    );
    assert_eq!(
        after2.total_bytes, next2,
        "budget must NOT ratchet across consecutive handoffs: {after2:?}"
    );
    assert_eq!(
        after2.total_bytes,
        after2
            .categories
            .iter()
            .map(|c| c.current_bytes)
            .sum::<u64>(),
        "total must equal the sum of categories: {after2:?}"
    );
    assert_eq!(
        after2.accounting_errors, 0,
        "no accounting drift: {after2:?}"
    );
}
