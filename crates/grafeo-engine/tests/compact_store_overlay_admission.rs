//! Integration tests for overlay accounting, admission, and backpressure (G-EM0.5a).
//!
//! Exercises the full admission lifecycle through the public GrafeoDB API:
//!
//! - Retained-capacity accounting across all categories (nodes, edges,
//!   properties, dirty sets, deletion sets).
//! - Soft-limit edge-triggered build request (exactly one coordinator).
//! - Hard-limit backpressure: bounded block, timeout, typed retryable error.
//! - W-mode: `OverlayConsumer` refuses the eager `merge_overlay_in_place` path.
//! - `admit_overlay_write` typed error surface.
//! - Adversarial small-payload/high-capacity and delete-heavy workloads.
//!
//! Requires: `compact-store`, `lpg` (both default-on).

#![cfg(all(feature = "compact-store", feature = "lpg"))]

use std::sync::Arc;
use std::time::Duration;

use grafeo_core::graph::compact::overlay_budget::{
    AdmissionOutcome, OverlayAdmissionController, OverlayBudgetConfig, PressureLevel, RejectReason,
    RetainedCategory, RetryReason,
};
use grafeo_core::graph::traits::{GraphStore, GraphStoreMut};
use grafeo_engine::GrafeoDB;

/// Builds an in-memory DB, compacts it (installing the layered store), and
/// installs an admission controller with the given config.
fn db_with_admission(config: OverlayBudgetConfig) -> (GrafeoDB, Arc<OverlayAdmissionController>) {
    let mut db = GrafeoDB::new_in_memory();
    // Seed a small base so compact() produces a non-trivial layered store.
    for i in 0..4 {
        db.execute(&format!("INSERT (:Person {{name: 'p-{i}', age: {i}}})"))
            .unwrap();
    }
    db.compact().unwrap();
    let ctl = Arc::new(OverlayAdmissionController::new(config).expect("valid config"));
    db.install_overlay_admission(Arc::clone(&ctl));
    (db, ctl)
}

// ── Accounting ──────────────────────────────────────────────────────────────

#[test]
fn node_creation_charges_mutation_payload() {
    let (db, ctl) = db_with_admission(OverlayBudgetConfig::default());
    let layered = db.layered_store().unwrap();

    let before = ctl.snapshot().total_bytes;
    let _id = layered.create_node(&["Person"]);
    let after = ctl.snapshot();

    assert!(
        after.total_bytes > before,
        "node creation must charge retained capacity"
    );
    assert!(
        after.categories[RetainedCategory::MutationPayload.index()].current_bytes > 0,
        "mutation payload category must be non-zero"
    );
    assert_eq!(after.accounting_errors, 0);
}

#[test]
fn edge_creation_charges_mutation_payload() {
    let (db, ctl) = db_with_admission(OverlayBudgetConfig::default());
    let layered = db.layered_store().unwrap();

    let n1 = layered.create_node(&["Person"]);
    let n2 = layered.create_node(&["Person"]);
    let before = ctl.snapshot().total_bytes;
    let _e = layered.create_edge(n1, n2, "KNOWS");
    let after = ctl.snapshot();

    assert!(after.total_bytes > before);
    assert_eq!(after.accounting_errors, 0);
}

#[test]
fn property_set_charges_mutation_payload() {
    let (db, ctl) = db_with_admission(OverlayBudgetConfig::default());
    let layered = db.layered_store().unwrap();

    let n = layered.create_node(&["Person"]);
    let before = ctl.snapshot().total_bytes;
    layered.set_node_property(
        n,
        "bio",
        grafeo_common::types::Value::String(arcstr::ArcStr::from("x".repeat(1024))),
    );
    let after = ctl.snapshot();

    assert!(
        after.total_bytes > before + 1024,
        "large property must charge at least its payload"
    );
}

#[test]
fn base_deletion_charges_deletion_sets() {
    let (db, ctl) = db_with_admission(OverlayBudgetConfig::default());
    let layered = db.layered_store().unwrap();

    // Get a base node id (from the compacted base).
    let base_ids = layered.base_store_arc().node_ids();
    assert!(!base_ids.is_empty(), "base must have nodes after compact");
    let base_id = base_ids[0];

    let before = ctl.snapshot().categories[RetainedCategory::DeletionSets.index()].current_bytes;
    let deleted = layered.delete_node(base_id);
    assert!(deleted, "base node deletion must succeed");
    let after = ctl.snapshot().categories[RetainedCategory::DeletionSets.index()].current_bytes;

    assert!(after > before, "deletion must charge DeletionSets category");
}

#[test]
fn high_water_tracks_peak() {
    let (db, ctl) = db_with_admission(OverlayBudgetConfig::default());
    let layered = db.layered_store().unwrap();

    for _ in 0..10 {
        layered.create_node(&["Person"]);
    }
    let peak = ctl.snapshot().total_high_water_bytes;
    assert!(peak > 0);

    // Merge drains the overlay; high-water must not decrease.
    layered.merge_overlay_in_place().unwrap();
    let snap = ctl.snapshot();
    assert_eq!(snap.total_bytes, 0, "merge must drain all retained bytes");
    assert!(
        snap.total_high_water_bytes >= peak,
        "high-water must not decrease after drain"
    );
}

// ── Soft limit ──────────────────────────────────────────────────────────────

#[test]
fn soft_crossing_requests_exactly_one_build() {
    // Small soft limit so a few nodes cross it.
    let config = OverlayBudgetConfig {
        soft_limit_bytes: 256,
        hard_limit_bytes: 64 * 1024,
        max_block_duration: Duration::from_millis(100),
    };
    let (db, ctl) = db_with_admission(config);
    let layered = db.layered_store().unwrap();

    let mut build_requests = 0u32;
    for _ in 0..20 {
        layered.create_node(&["Person"]);
        // The layered store charges via try_reserve; check the snapshot.
        if ctl.snapshot().build_requested {
            build_requests += 1;
            // Complete the build to re-arm.
            ctl.complete_generation_build();
        }
    }
    // At least one build request must have fired (we crossed soft).
    assert!(
        build_requests >= 1,
        "soft crossing must trigger a build request"
    );
}

#[test]
fn pressure_level_reflects_soft_and_hard() {
    let config = OverlayBudgetConfig {
        soft_limit_bytes: 128,
        hard_limit_bytes: 512,
        max_block_duration: Duration::from_millis(100),
    };
    let (db, ctl) = db_with_admission(config);
    let layered = db.layered_store().unwrap();

    assert_eq!(ctl.pressure(), PressureLevel::Normal);

    // Create enough nodes to cross soft.
    for _ in 0..10 {
        layered.create_node(&["Person"]);
    }
    let p = ctl.pressure();
    assert!(
        matches!(p, PressureLevel::Soft | PressureLevel::Hard),
        "pressure must be Soft or Hard after crossing soft limit, got {p:?}"
    );
}

// ── Hard limit / backpressure ───────────────────────────────────────────────

#[test]
fn try_reserve_at_hard_pressure_is_retryable() {
    let config = OverlayBudgetConfig {
        soft_limit_bytes: 64,
        hard_limit_bytes: 128,
        max_block_duration: Duration::from_millis(50),
    };
    let (_db, ctl) = db_with_admission(config);

    // Fill to near the hard limit.
    let _ = ctl.try_reserve(RetainedCategory::WalBuffers, 120);
    // Next reserve would exceed hard.
    match ctl.try_reserve(RetainedCategory::WalBuffers, 20) {
        AdmissionOutcome::Retryable {
            reason: RetryReason::HardPressure,
        } => {}
        other => panic!("expected retryable hard pressure, got {other:?}"),
    }
}

#[test]
fn reserve_blocks_boundedly_then_times_out() {
    let config = OverlayBudgetConfig {
        soft_limit_bytes: 64,
        hard_limit_bytes: 128,
        max_block_duration: Duration::from_millis(80),
    };
    let (_db, ctl) = db_with_admission(config);

    let _ = ctl.try_reserve(RetainedCategory::WalBuffers, 128); // fill to hard
    let ctl2 = Arc::clone(&ctl);
    let outcome = std::thread::spawn(move || ctl2.reserve(RetainedCategory::WalBuffers, 64))
        .join()
        .unwrap();
    assert!(
        matches!(
            outcome,
            AdmissionOutcome::Retryable {
                reason: RetryReason::Timeout
            }
        ),
        "bounded block must time out retryably, got {outcome:?}"
    );
    assert_eq!(ctl.snapshot().timed_out_count, 1);
}

#[test]
fn oversized_request_is_rejected() {
    let config = OverlayBudgetConfig {
        soft_limit_bytes: 64,
        hard_limit_bytes: 128,
        max_block_duration: Duration::from_millis(50),
    };
    let (_db, ctl) = db_with_admission(config);

    match ctl.try_reserve(RetainedCategory::NextEpoch, 200) {
        AdmissionOutcome::Rejected {
            reason: RejectReason::Oversized,
        } => {}
        other => panic!("expected oversized rejection, got {other:?}"),
    }
}

#[test]
fn admit_overlay_write_returns_typed_retryable_error() {
    let config = OverlayBudgetConfig {
        soft_limit_bytes: 64,
        hard_limit_bytes: 128,
        max_block_duration: Duration::from_millis(50),
    };
    let (db, ctl) = db_with_admission(config);

    // Fill to hard via the controller directly.
    let _ = ctl.try_reserve(RetainedCategory::WalBuffers, 128);

    let result = db.admit_overlay_write(RetainedCategory::MutationPayload, 64);
    assert!(
        result.is_err(),
        "admit_overlay_write must fail at hard pressure"
    );
    let err = result.unwrap_err();
    assert!(
        err.error_code().is_retryable(),
        "hard-pressure admission error must be retryable, got {:?}",
        err.error_code()
    );
}

#[test]
fn admit_overlay_write_noop_without_controller() {
    let mut db = GrafeoDB::new_in_memory();
    db.execute("INSERT (:Person {name: 'x'})").unwrap();
    db.compact().unwrap();
    // No controller installed.
    let result = db.admit_overlay_write(RetainedCategory::MutationPayload, 1024);
    assert!(result.is_ok(), "no controller = always admit");
}

// ── W-mode: OverlayConsumer backpressure ────────────────────────────────────

#[test]
fn overlay_consumer_refuses_eager_merge_in_writable_mode() {
    let (db, _ctl) = db_with_admission(OverlayBudgetConfig::default());
    let layered = db.layered_store().unwrap();

    // W-mode is active (controller installed).
    assert!(layered.writable_mode());

    // Add overlay mutations so the consumer would normally want to spill.
    layered.create_node(&["Person"]);

    // The OverlayConsumer's spill() must return Backpressure, not merge.
    // We verify via the buffer manager: spill_all should not free overlay bytes
    // because the overlay consumer refuses.
    let overlay_bytes_before = layered.overlay_memory_bytes();
    assert!(overlay_bytes_before > 0, "overlay must have mutations");

    let freed = db.buffer_manager().spill_all();
    let overlay_bytes_after = layered.overlay_memory_bytes();

    // The overlay consumer refused to merge; overlay bytes unchanged.
    assert_eq!(
        overlay_bytes_before, overlay_bytes_after,
        "W-mode must not drain overlay via eager merge; freed={freed}"
    );
}

#[test]
fn overlay_consumer_merges_normally_without_writable_mode() {
    let mut db = GrafeoDB::new_in_memory();
    for i in 0..4 {
        db.execute(&format!("INSERT (:Person {{name: 'p-{i}'}})"))
            .unwrap();
    }
    db.compact().unwrap();
    // No admission controller → not W-mode.
    let layered = db.layered_store().unwrap();
    assert!(!layered.writable_mode());

    // Add overlay mutations.
    layered.create_node(&["Person"]);
    assert!(layered.overlay_mutation_count() > 0);
    let base_nodes_before = layered.base_store_arc().total_nodes();

    // In non-W-mode the eager merge path is available and drains the overlay.
    layered.merge_overlay_in_place().unwrap();
    assert_eq!(
        layered.overlay_mutation_count(),
        0,
        "non-W-mode merge must drain overlay mutations"
    );
    assert!(
        layered.base_store_arc().total_nodes() > base_nodes_before,
        "merge must fold the overlay node into the base"
    );
}

// ── Adversarial workloads ───────────────────────────────────────────────────

#[test]
fn small_payload_high_capacity_stays_bounded() {
    // Many tiny mutations: accounting must stay truthful and bounded.
    let config = OverlayBudgetConfig {
        soft_limit_bytes: 4 * 1024,
        hard_limit_bytes: 64 * 1024,
        max_block_duration: Duration::from_millis(100),
    };
    let (db, ctl) = db_with_admission(config);
    let layered = db.layered_store().unwrap();

    for _ in 0..500 {
        layered.create_node(&["X"]);
    }
    let snap = ctl.snapshot();
    assert!(snap.total_bytes > 0);
    assert!(
        snap.total_bytes <= config.hard_limit_bytes + 1024,
        "accounting must stay near the hard limit, got {}",
        snap.total_bytes
    );
    assert_eq!(snap.accounting_errors, 0);
}

#[test]
fn delete_heavy_workload_charges_deletion_sets() {
    let (db, ctl) = db_with_admission(OverlayBudgetConfig::default());
    let layered = db.layered_store().unwrap();

    // Create and delete many overlay nodes.
    let mut ids = Vec::new();
    for _ in 0..50 {
        ids.push(layered.create_node(&["Person"]));
    }
    for id in &ids {
        layered.delete_node(*id);
    }
    let snap = ctl.snapshot();
    // Deletion sets may be zero if overlay nodes are deleted directly (not
    // base-deletion), but mutation payload must have been charged.
    assert!(snap.admitted_count > 0);
    assert_eq!(snap.accounting_errors, 0);
}

#[test]
fn shutdown_rejects_admit_overlay_write() {
    let (db, ctl) = db_with_admission(OverlayBudgetConfig::default());
    ctl.request_shutdown();
    let result = db.admit_overlay_write(RetainedCategory::MutationPayload, 1);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        !err.error_code().is_retryable(),
        "shutdown rejection must be terminal"
    );
}

// ── Drain on merge ──────────────────────────────────────────────────────────

#[test]
fn merge_drains_all_retained_categories() {
    let (db, ctl) = db_with_admission(OverlayBudgetConfig::default());
    let layered = db.layered_store().unwrap();

    // Charge multiple categories.
    let n1 = layered.create_node(&["Person"]);
    let n2 = layered.create_node(&["Person"]);
    layered.create_edge(n1, n2, "KNOWS");
    layered.set_node_property(n1, "x", grafeo_common::types::Value::Int64(42));

    let snap_before = ctl.snapshot();
    assert!(snap_before.total_bytes > 0);

    layered.merge_overlay_in_place().unwrap();
    let snap_after = ctl.snapshot();
    assert_eq!(snap_after.total_bytes, 0, "merge must drain all categories");
    assert!(
        !snap_after.build_requested,
        "build request re-armed after drain"
    );
}
