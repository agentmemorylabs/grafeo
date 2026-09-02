//! H-ADOPT.2 item 3 — session batch-versioned writes must route through the
//! layered write store (dirty marking, post-freeze identity, admission charge).
//!
//! Proves the correctness prerequisite for epoch-handoff production wiring:
//! `swap_base_and_repair_overlay` depends on LayeredStore's dirty/post_freeze
//! bookkeeping, and the session batch-versioned write path used to bypass it
//! entirely (writes went straight to the raw overlay `LpgStore`), which would
//! silently serve stale frozen values after a handoff swap.
//!
//! ```bash
//! cargo test -p grafeo-engine --test session_batch_layered_bookkeeping \
//!   --features generation,generation-streaming,compact-store,lpg,mmap,wal,triple-store
//! ```

#![cfg(all(feature = "compact-store", feature = "lpg"))]

use std::collections::HashSet;
use std::sync::Arc;

use grafeo_common::types::Value;
use grafeo_core::graph::compact::layered::OverlayHandoffLive;
use grafeo_core::graph::compact::overlay_budget::{
    OverlayAdmissionController, OverlayBudgetConfig,
};
use grafeo_engine::GrafeoDB;
use grafeo_engine::session::{TransactionalEdgeCreate, TransactionalNodeCreate};

/// Batch node+edge creates through a layered SESSION must mark the created
/// entities dirty in the overlay, record their post-freeze identity during an
/// active handoff, and charge overlay retention to the admission controller.
///
/// The pre-fix bypass wrote straight to the raw overlay `LpgStore` via
/// `Session::active_lpg_store()` and did none of these.
#[test]
fn session_batch_writes_route_through_layered_bookkeeping() {
    // Seed a compact base, then install the layered store + admission.
    let mut db = GrafeoDB::new_in_memory();
    db.create_node_with_props(&["Person"], [("name", Value::from("base-a"))])
        .expect("base node a");
    db.create_node_with_props(&["Person"], [("name", Value::from("base-b"))])
        .expect("base node b");
    db.compact().expect("compact");
    let ctl = Arc::new(
        OverlayAdmissionController::new(OverlayBudgetConfig::for_tests()).expect("controller"),
    );
    db.install_overlay_admission(Arc::clone(&ctl));
    let layered = Arc::clone(db.layered_store().expect("layered store"));

    // Active dual-epoch handoff: every N+1 write must record post-freeze
    // identity so `swap_base_and_repair_overlay` retains it as dirty.
    layered
        .begin_epoch_handoff(OverlayHandoffLive {
            frozen_epoch: 1,
            next_epoch: 2,
            ..OverlayHandoffLive::default()
        })
        .expect("begin handoff");

    // Batch node + edge create through the session's transactional batch path.
    let mut session = db.session();
    session.begin_transaction().expect("begin");
    let node_ids = session
        .create_nodes_with_props_transactional(&[
            TransactionalNodeCreate::new(["Person"]).with_property("name", Value::from("batch-1")),
            TransactionalNodeCreate::new(["Person"]).with_property("name", Value::from("batch-2")),
        ])
        .expect("batch nodes");
    let edge_ids = session
        .create_edges_with_props_transactional(&[TransactionalEdgeCreate::new(
            node_ids[0],
            node_ids[1],
            "KNOWS",
        )
        .with_property("since", Value::from(2026i64))])
        .expect("batch edges");
    session.commit().expect("commit");

    // (a) every created id must be dirty in the overlay.
    let dirty_nodes: HashSet<u64> = layered
        .snapshot_dirty_node_ids()
        .into_iter()
        .map(|id| id.as_u64())
        .collect();
    for id in &node_ids {
        assert!(
            dirty_nodes.contains(&id.as_u64()),
            "batch-created node {id} must be dirty in the overlay"
        );
    }
    let dirty_edges: HashSet<u64> = layered
        .snapshot_dirty_edge_ids()
        .into_iter()
        .map(|id| id.as_u64())
        .collect();
    for id in &edge_ids {
        assert!(
            dirty_edges.contains(&id.as_u64()),
            "batch-created edge {id} must be dirty in the overlay"
        );
    }

    // (b) with a handoff active, every created id must be recorded as a
    // post-freeze (epoch N+1) mutation.
    let live = layered.handoff_live().expect("handoff still live");
    for id in &node_ids {
        assert!(
            live.post_freeze_nodes.contains(&id.as_u64()),
            "batch-created node {id} must be recorded post-freeze"
        );
    }
    for id in &edge_ids {
        assert!(
            live.post_freeze_edges.contains(&id.as_u64()),
            "batch-created edge {id} must be recorded post-freeze"
        );
    }

    // (c) overlay admission must have been charged for the batch.
    let snap = ctl.snapshot();
    assert!(
        snap.total_bytes > 0,
        "batch writes must charge overlay retention"
    );
}
