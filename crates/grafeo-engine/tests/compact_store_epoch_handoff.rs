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

use bytes::Bytes;
use grafeo_common::storage::SectionType;
use grafeo_common::types::{PropertyKey, Value};
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
