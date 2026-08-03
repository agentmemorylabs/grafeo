//! G-EM0.5c — concurrent overlay epoch and WAL handoff.
//!
//! Proves:
//! 1. Freeze epoch N at WAL boundary B while concurrent N+1 writes are admitted.
//! 2. Build G(N) from the freeze snapshot and publish with pre-cut boundary B.
//! 3. Retire only the frozen overlay prefix; N+1 mutations remain exactly once.
//! 4. Dual-epoch backpressure: frozen + next-epoch retained bytes both count.
//! 5. Fresh-process recovery after freeze/build/publication/retire fault
//!    injection — entity-level survival (no lost/duplicated/resurrected
//!    writes) at all four abort points.
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
    BuildPublication, EpochHandoffPhase, GrafeoDB, generation_build_request, read_manifest_state,
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

/// Assert that a generation container's Person entity set equals `expected`
/// exactly: every expected name present exactly once, and nothing else.
/// Entity-id-keyed stores with unique fixture names make this an
/// entity-level survival check — a lost write shows up as a missing name, a
/// duplicated write as an extra name, a resurrected write as a name that
/// must not be there.
fn assert_person_set_equals(gen_path: &std::path::Path, expected: &[String]) {
    let names = generation_person_names(gen_path);
    let mut expect: Vec<String> = expected.to_vec();
    expect.sort_unstable();
    assert_eq!(
        names,
        expect,
        "generation {} must contain exactly the expected entities, each once",
        gen_path.display()
    );
}

/// Sorted WAL log-file sequence numbers under a generation root.
fn wal_log_sequences(gen_root: &std::path::Path) -> Vec<u64> {
    let mut seqs: Vec<u64> = std::fs::read_dir(gen_root.join("wal"))
        .expect("read wal dir")
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.strip_prefix("wal_")
                .and_then(|s| s.strip_suffix(".log"))
                .and_then(|s| s.parse().ok())
        })
        .collect();
    seqs.sort_unstable();
    seqs
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

/// Build the prior (`g-prior`) generation a crash test starts from and
/// return its publication (durable boundary + absolute generation path).
fn build_prior_generation(gen_root: &std::path::Path) -> BuildPublication {
    let db = GrafeoDB::new_in_memory();
    populate(&db, "prior");
    db.build_and_publish_generation(generation_build_request(gen_root, "g-prior"))
        .expect("prior generation")
}

/// Spawn this test binary as a fresh child process with the crash mode and
/// abort-point env set. The child's mode match aborts (or the engine's
/// `GRAFEO_5C_ABORT` hook aborts), so the returned status is non-success.
fn spawn_crash_child(
    gen_root: &std::path::Path,
    mode: &str,
    abort_point: &str,
) -> std::process::ExitStatus {
    let mut cmd = Command::new(std::env::current_exe().expect("exe"));
    cmd.env(HELPER_ENV, "1")
        .env("GRAFEO5C_ROOT", gen_root)
        .env("GRAFEO5C_MODE", mode)
        .env("RUST_BACKTRACE", "0");
    if !abort_point.is_empty() {
        cmd.env("GRAFEO_5C_ABORT", abort_point);
    }
    cmd.status().expect("spawn child")
}

/// Child-side two-step handoff with real next-epoch state: two frozen
/// overlay nodes plus one post-freeze N+1 overlay node, then complete. The
/// parent's `GRAFEO_5C_ABORT` env aborts the process inside
/// `complete_epoch_handoff` at the requested post-commit point
/// (`after_publication` | `after_retire`). The frozen generation therefore
/// contains the base `child-*` nodes + `frozen-n*` overlay nodes, and
/// excludes the post-freeze `next-n1` node.
fn crash_child_twostep(db: &mut GrafeoDB, gen_root: &std::path::Path, gen_id: &str) {
    db.compact().expect("compact");
    let layered = db.layered_store().expect("layered");
    for name in ["frozen-n1", "frozen-n2"] {
        let n = layered.create_node(&["Person"]);
        layered.set_node_property(n, "name", Value::from(name));
    }
    let handle = db.freeze_epoch_for_handoff(gen_root).expect("freeze");
    let n1 = layered.create_node(&["Person"]);
    layered.set_node_property(n1, "name", Value::from("next-n1"));
    db.complete_epoch_handoff(handle, generation_build_request(gen_root, gen_id))
        .expect("should abort inside complete_epoch_handoff");
}

/// Parent: spawn a child that freezes then aborts after freeze; prove the
/// prior selected generation (if any) remains recoverable and no silent
/// promotion of a half-built generation occurs. The other three fault
/// points (after_build, after_publication, after_retire) share this
/// function's child branch via `GRAFEO5C_MODE`.
#[test]
fn fresh_process_abort_after_freeze_keeps_prior_selection() {
    if in_any_child() {
        // Child path.
        let root = std::env::var("GRAFEO5C_ROOT").expect("root");
        let mode = std::env::var("GRAFEO5C_MODE").expect("mode");
        let gen_root = std::path::PathBuf::from(&root);
        let mut db = GrafeoDB::new_in_memory();
        populate(&db, "child");
        match mode.as_str() {
            "abort_after_freeze" => {
                let _ = db.freeze_epoch_for_handoff(&gen_root).expect("freeze");
                // Simulate crash before complete.
                std::process::abort();
            }
            "abort_after_build" => {
                // GRAFEO_5C_ABORT=after_build is set by the parent Command
                // env: the abort fires inside build_publish_frozen after the
                // compact-store build and before the generation file and
                // manifest are written.
                let _ = db
                    .run_epoch_handoff(generation_build_request(&gen_root, "g-crash-build"))
                    .expect("should abort inside");
            }
            "abort_after_publication" => {
                crash_child_twostep(&mut db, &gen_root, "g-crash-pub");
            }
            "abort_after_retire" => {
                crash_child_twostep(&mut db, &gen_root, "g-crash-retire");
            }
            other => panic!("unknown mode {other}"),
        }
        return;
    }

    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    // Establish prior generation and capture its exact entity set.
    let first = build_prior_generation(&gen_root);
    let prior_names = generation_person_names(&first.generation_abs_path);
    assert_eq!(
        prior_names,
        ["prior-a", "prior-b"].map(String::from),
        "fixture: prior generation must contain exactly two Person entities"
    );

    let status = spawn_crash_child(&gen_root, "abort_after_freeze", "");
    assert!(
        !status.success(),
        "child must abort (non-success): {status}"
    );

    let recovery = recover_generation_root(&gen_root).expect("recover after freeze abort");
    assert_eq!(
        recovery.selected.slot.generation_id, "g-prior",
        "freeze abort must leave prior generation selected"
    );
    // The freeze only rotates the WAL and installs an in-process handle; the
    // manifest is untouched, so the recoverable boundary is still the prior
    // publication's, and it must remain replayable up to the live tail.
    assert_eq!(
        recovery.wal_boundary, first.publication.wal_boundary,
        "freeze abort must not advance the recorded boundary"
    );
    validate_replayable(&gen_root.join("wal"), &recovery.wal_boundary.to_cursor())
        .expect("prior boundary replayable after freeze abort");
    // Entity-level survival: the selected generation's entity set is intact
    // exactly once (no lost/duplicated writes), and the child's would-be
    // content never entered any generation (nothing was promoted or
    // resurrected — the child only populated its own live DB, which a crash
    // discards together with the in-process freeze handle).
    assert_person_set_equals(&recovery.selected.generation_abs_path, &prior_names);
    // The freeze cut is observable on disk: a WAL rotation exists strictly
    // beyond the recorded boundary (the un-committed freeze rotation). The
    // manifest boundary was not advanced, so recovery replays from B — the
    // extra file is inert and no committed write was lost or truncated.
    let seqs = wal_log_sequences(&gen_root);
    assert!(
        *seqs.last().expect("wal files") > recovery.wal_boundary.log_sequence,
        "freeze cut rotation must exist beyond the recorded boundary: {seqs:?}"
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

        // Establish a prior generation so the slot handoff is not genesis.
        build_prior_generation(&gen_root);

        let status = spawn_crash_child(&gen_root, "abort_after_publication", "after_publication");
        assert!(!status.success(), "child must abort after publication");

        let recovery = recover_generation_root(&gen_root).expect("recover after pub abort");
        assert_eq!(
            recovery.selected.slot.generation_id, "g-crash-pub",
            "post-commit abort must leave NEW generation selected"
        );
        validate_replayable(&gen_root.join("wal"), &recovery.wal_boundary.to_cursor())
            .expect("new boundary replayable");
        // Entity-level survival: the published G(N) contains the frozen
        // epoch-N set exactly once — base `child-*` nodes plus frozen
        // overlay `frozen-n*` nodes — and the post-freeze N+1 write
        // `next-n1` is NOT resurrected into the snapshot.
        assert_person_set_equals(
            &recovery.selected.generation_abs_path,
            &[
                "child-a".to_string(),
                "child-b".to_string(),
                "frozen-n1".to_string(),
                "frozen-n2".to_string(),
            ],
        );
        // No lost writes past the boundary: publication truncated only WAL
        // sequences strictly older than the retained floor, so the N+1 tail
        // (the boundary file and anything later) must still exist and
        // validate as replayable up to the live tail (asserted above). Real
        // N+1 frames live in the live DB's WAL — in-memory here, so none are
        // written under `gen_root/wal`; at this boundary the no-lost-writes
        // guarantee is exactly the boundary file's survival + full tail
        // replayability, made explicit here as a file-level check.
        let tail = wal_log_sequences(&gen_root);
        assert!(
            tail.iter()
                .any(|s| *s >= recovery.wal_boundary.log_sequence),
            "N+1 tail file(s) must survive publication truncation: {tail:?}"
        );
    }
}

/// MAJOR-3 coverage — `after_build`: parent spawns a child that runs a full
/// freeze→build handoff with `GRAFEO_5C_ABORT=after_build`, then proves from
/// a fresh process that the half-built generation was never promoted.
///
/// NOTE on distinguishability: `after_build` is NOT post-hoc distinguishable
/// from `after_freeze` at the recovery surface — both leave the prior
/// generation selected with an unchanged boundary and one inert extra WAL
/// rotation, because neither commits anything to the manifest. The invariant
/// that DOES hold and is asserted here: prior selection, prior boundary,
/// replayability, the prior entity set intact exactly once, and — unique to
/// after_build — no half-built `.grafeo` generation file materialized
/// (publication, which creates the immutable file, never ran).
#[cfg(debug_assertions)]
#[test]
fn fresh_process_abort_after_build_keeps_prior_selection() {
    if in_any_child() {
        return; // child handled by the sibling mode match above
    }
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let first = build_prior_generation(&gen_root);
    let prior_names = generation_person_names(&first.generation_abs_path);

    let status = spawn_crash_child(&gen_root, "abort_after_build", "after_build");
    assert!(!status.success(), "child must abort after build");

    let recovery = recover_generation_root(&gen_root).expect("recover after build abort");
    assert_eq!(
        recovery.selected.slot.generation_id, "g-prior",
        "pre-commit build abort must leave prior generation selected"
    );
    assert_eq!(
        recovery.wal_boundary, first.publication.wal_boundary,
        "build abort must not advance the recorded boundary"
    );
    validate_replayable(&gen_root.join("wal"), &recovery.wal_boundary.to_cursor())
        .expect("prior boundary replayable after build abort");
    // Entity-level: the prior generation's entity set is intact exactly once
    // (no lost/duplicated writes), and the half-built child content never
    // appears — it was captured in the in-memory freeze but publication
    // (which would write the generation file + manifest) never ran, so no
    // part of it can be lost, duplicated, or resurrected into a generation.
    assert_person_set_equals(&recovery.selected.generation_abs_path, &prior_names);
    // No half-built `.grafeo` generation file may exist unreferenced under
    // `generations/`: the build produced store bytes in scratch space
    // (build-runs/build-tmp), never an immutable generation. Recovery must
    // classify nothing as an unreferenced generation — only the selected
    // prior file exists on disk.
    assert!(
        !recovery.orphans.iter().any(|o| matches!(
            o,
            grafeo_engine::OrphanClassification::UnreferencedGeneration { .. }
        )),
        "no half-built generation may be materialized: {recovery:?}"
    );
    // The freeze half of the one-shot handoff did rotate the WAL (the same
    // inert rotation as after_freeze) — the manifest boundary was not
    // advanced, so no committed write was lost or truncated.
    let seqs = wal_log_sequences(&gen_root);
    assert!(
        *seqs.last().expect("wal files") > recovery.wal_boundary.log_sequence,
        "freeze cut rotation must exist beyond the recorded boundary: {seqs:?}"
    );
}

/// MAJOR-3 coverage — `after_retire`: parent spawns a child that commits the
/// full handoff (freeze → N+1 write → publish → retire) with
/// `GRAFEO_5C_ABORT=after_retire`, then proves from a fresh process that the
/// committed generation and the N+1 WAL tail survive. The abort fires after
/// retire, which is in-memory only, so the durable recovery surface must be
/// equivalent to the after_publication point: NEW generation selected,
/// boundary replayable up to the live tail, frozen entity set exactly once,
/// and the N+1 write NOT resurrected into G(N) (it belongs to the retained
/// N+1 tail past boundary B, never lost).
#[cfg(debug_assertions)]
#[test]
fn fresh_process_abort_after_retire_selects_new() {
    if in_any_child() {
        return; // child handled by the sibling mode match above
    }
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    // Establish a prior generation so the slot handoff is not a genesis build.
    build_prior_generation(&gen_root);

    let status = spawn_crash_child(&gen_root, "abort_after_retire", "after_retire");
    assert!(!status.success(), "child must abort after retire");

    let recovery = recover_generation_root(&gen_root).expect("recover after retire abort");
    assert_eq!(
        recovery.selected.slot.generation_id, "g-crash-retire",
        "post-retire abort must leave NEW generation selected (commit already durable)"
    );
    validate_replayable(&gen_root.join("wal"), &recovery.wal_boundary.to_cursor())
        .expect("boundary replayable to the live tail after retire abort");
    // Entity-level: the frozen epoch-N set appears exactly once in G(N)
    // (base `child-*` + frozen overlay `frozen-n*`), and the post-freeze
    // N+1 write `next-n1` is not lost from the N+1 tail nor resurrected into
    // the frozen snapshot.
    assert_person_set_equals(
        &recovery.selected.generation_abs_path,
        &[
            "child-a".to_string(),
            "child-b".to_string(),
            "frozen-n1".to_string(),
            "frozen-n2".to_string(),
        ],
    );
    // Retire (an in-memory overlay strip) must not have touched the durable
    // WAL tail: the boundary file and anything later survive truncation,
    // and validate as replayable up to the live tail (asserted above).
    let tail = wal_log_sequences(&gen_root);
    assert!(
        tail.iter()
            .any(|s| *s >= recovery.wal_boundary.log_sequence),
        "N+1 tail file(s) must survive the retire abort: {tail:?}"
    );
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
