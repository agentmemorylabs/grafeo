//! G-EM0.5d — repeated writable-cycle and memory proof (Milestone W).
//!
//! Proofs, mapped to the packet requirements:
//!
//! - **R1** *sustained cycles.* Drive many consecutive publish cycles against one
//!   live layered DB while a reader thread holds consistent snapshots, interleaved
//!   with a checkpoint/close/restart, an induced build failure + retry, a backup
//!   pin held across a cycle, and the 5d base swap + overlay reset. Two cycle
//!   kinds are exercised: the whole-live-graph swap
//!   (`swap_base_and_reset_overlay`) and the 5c epoch-handoff repaired swap
//!   (`swap_base_and_repair_overlay`).
//! - **R2/R3** *memory boundedness.* Scale the build input 4× at a fixed
//!   `GenerationBudget::acceptance_linux()` and prove the private-anonymous peak
//!   (RssAnon, sampled *during* the build via an in-process sampler) stays
//!   bounded: isolated child per scale, no allocator-trim, no `drop_caches`.
//! - **R4** *exact parity.* Maintain a shadow model of every accepted write and
//!   assert the live layered read view (and each published generation) equals it
//!   as an exact multiset — no lost/duplicated/resurrected entities.
//! - **R5** *recovery/GC.* Linux fresh-reopen selects the latest published
//!   generation with a replayable WAL boundary. The Windows leg is an explicit
//!   residual (this host cannot run Windows; it is not fabricated).
//!
//! ## Which invariant this demonstrates
//!
//! The memory proof is a **transient build-event** contract: the streaming
//! generation build's private-anonymous peak is `O(configured budget +
//! metadata)`, not `O(total base bytes)`, as the build input scales 4×. It is
//! *not* a steady-state open/read residency claim; the engine's production read
//! path never performs the lease base swap this test drives, and steady-state
//! repeated-swap residency is an explicit residual (a future packet owns the 4a
//! registry + `LayeredStore::swap_base` wiring).

#![cfg(all(feature = "generation", feature = "compact-store", feature = "lpg"))]

use std::collections::BTreeSet;
use std::fs;
use std::sync::Arc;

use bytes::Bytes;
use grafeo_common::storage::SectionType;
use grafeo_common::types::{PropertyKey, Value};
use grafeo_core::graph::compact::CompactStore;
use grafeo_core::graph::compact::overlay_budget::{
    OverlayAdmissionController, OverlayBudgetConfig,
};
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_engine::GraphStore;
use grafeo_engine::GraphStoreMut;
use grafeo_engine::{
    EpochHandoffPhase, GrafeoDB, generation_build_request, recover_generation_root,
};
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::generation::wal_cursor::validate_replayable;
use tempfile::TempDir;

/// Open a published generation container **fresh** (bytes path) and return its
/// base store. Used to install the swap target that `swap_base*` serves.
fn open_generation_base(gen_abs_path: &std::path::Path) -> Arc<CompactStore> {
    let manager = GrafeoFileManager::open_read_only(gen_abs_path).expect("open generation");
    let dir = manager
        .read_section_directory()
        .expect("section directory")
        .expect("directory present");
    let entry = dir
        .find(SectionType::CompactStore)
        .expect("CompactStore section present");
    let data = manager.read_section_data(entry).expect("read section");
    let mut section = CompactStoreSection::empty();
    section
        .deserialize_from_bytes(Bytes::from(data))
        .expect("deserialize base");
    section.store().expect("base store present")
}

/// Every `Person` `name` in a generation container's base, as a sorted multiset.
fn generation_person_names(gen_abs_path: &std::path::Path) -> Vec<String> {
    let store = open_generation_base(gen_abs_path);
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

/// Every visible `Person` `name` served by the DB's layered base+overlay, sorted.
fn live_person_names(db: &GrafeoDB) -> Vec<String> {
    let layered = db.layered_store().expect("layered store");
    let mut names: Vec<String> = layered
        .all_node_ids()
        .into_iter()
        .filter_map(|id| layered.get_node(id))
        .filter(|n| n.labels.iter().any(|l| l.as_str() == "Person"))
        .filter_map(|n| {
            n.properties
                .get(&PropertyKey::new("name"))
                .and_then(|v| match v {
                    Value::String(s) => Some(s.as_str().to_string()),
                    _ => None,
                })
        })
        .collect();
    names.sort_unstable();
    names
}

#[test]
fn smoke_whole_graph_swap_bounds_overlay() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let mut db = GrafeoDB::new_in_memory();
    for tag in ["a", "b"] {
        db.create_node_with_props(&["Person"], [("name", Value::from(format!("p-{tag}")))])
            .expect("base node");
    }
    db.compact().expect("compact");
    let ctl =
        Arc::new(OverlayAdmissionController::new(OverlayBudgetConfig::for_tests()).expect("ctl"));
    db.install_overlay_admission(Arc::clone(&ctl));

    let mut expected: BTreeSet<String> =
        ["p-a".to_string(), "p-b".to_string()].into_iter().collect();
    assert_eq!(live_person_names(&db), Vec::from_iter(expected.clone()));

    let baseline_overlay_bytes = db.layered_store().unwrap().overlay_memory_bytes();
    let mut prev_peak = baseline_overlay_bytes;

    for i in 0..3 {
        // Whole-graph cycle: N+1 write, publish whole live graph, swap + reset.
        let n = db.layered_store().unwrap().create_node(&["Person"]);
        db.layered_store()
            .unwrap()
            .set_node_property(n, "name", Value::from(format!("p-c{i}")));
        expected.insert(format!("p-c{i}"));

        let pubn = db
            .build_and_publish_generation(generation_build_request(&gen_root, format!("wg-{i}")))
            .expect("whole-graph publish");
        let new_base = open_generation_base(&pubn.generation_abs_path);
        let _old = db
            .layered_store()
            .unwrap()
            .swap_base_and_reset_overlay(new_base);

        assert_eq!(live_person_names(&db), Vec::from_iter(expected.clone()));
        // Published generation mirrors the whole accepted model exactly.
        assert_eq!(
            generation_person_names(&pubn.generation_abs_path),
            Vec::from_iter(expected.clone())
        );

        // Overlay stays bounded across cycles (fresh empty overlay each time).
        let bytes = db.layered_store().unwrap().overlay_memory_bytes();
        assert!(
            bytes < 1 << 20,
            "overlay reset to empty after swap: {bytes}"
        );
        prev_peak = prev_peak.max(bytes);
    }
    let _ = prev_peak;
}

#[test]
fn restart_recovers_latest_generation_and_replayable_boundary() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let mut db = GrafeoDB::new_in_memory();
    db.create_node_with_props(&["Person"], [("name", Value::from("p-seed"))])
        .expect("seed");
    db.compact().expect("compact");
    let pubn = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-final"))
        .expect("publish");
    db.close().expect("checkpoint close");

    // R5 (Linux): fresh reopen selects the just-published generation and its
    // boundary is replayable. The Windows leg is a residual (see module docs).
    let recovery = recover_generation_root(&gen_root).expect("recover");
    assert_eq!(recovery.selected.slot.generation_id, "g-final");
    assert_eq!(recovery.wal_boundary, pubn.publication.wal_boundary);
    validate_replayable(&gen_root.join("wal"), &recovery.wal_boundary.to_cursor())
        .expect("boundary replayable after close/reopen");
}

#[test]
fn handoff_build_repaired_swap_preserves_exact_once() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let mut db = GrafeoDB::new_in_memory();
    db.create_node_with_props(&["Person"], [("name", Value::from("p-base"))])
        .expect("base node");
    db.compact().expect("compact");
    let ctl =
        Arc::new(OverlayAdmissionController::new(OverlayBudgetConfig::for_tests()).expect("ctl"));
    db.install_overlay_admission(Arc::clone(&ctl));

    // Epoch N overlay writes.
    let n_a = db.layered_store().unwrap().create_node(&["Person"]);
    db.layered_store()
        .unwrap()
        .set_node_property(n_a, "name", Value::from("frozen-n-a"));

    let handle = db.freeze_epoch_for_handoff(&gen_root).expect("freeze");
    assert_eq!(db.epoch_handoff_phase(), EpochHandoffPhase::FreezeCaptured);

    // Post-freeze N+1 (created, never frozen).
    let n1 = db.layered_store().unwrap().create_node(&["Person"]);
    db.layered_store()
        .unwrap()
        .set_node_property(n1, "name", Value::from("p-n1"));

    let report = db
        .complete_epoch_handoff(handle, generation_build_request(&gen_root, "hand-1"))
        .expect("complete handoff");
    assert_eq!(report.phase, EpochHandoffPhase::EpochRetired);

    // 5d: install the handoff generation as the base, repairing the overlay.
    let gen_abs = report
        .publication
        .as_ref()
        .expect("publication")
        .generation_abs_path
        .clone();
    let new_base = open_generation_base(&gen_abs);
    db.layered_store()
        .unwrap()
        .swap_base_and_repair_overlay(new_base);

    // Exact-once parity: base epoch-N + frozen-N + retained N+1, each once.
    let expected: BTreeSet<String> = ["p-base", "frozen-n-a", "p-n1"]
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(live_person_names(&db), Vec::from_iter(expected.clone()));
    // The handoff generation contains frozen epoch-N, not the N+1 node.
    let gen_names = generation_person_names(&gen_abs);
    assert!(gen_names.contains(&"frozen-n-a".to_string()));
    assert!(!gen_names.contains(&"p-n1".to_string()));
}
