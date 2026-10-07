//! H-ADOPT.2 item 4 — combined production handoff install
//! (`GrafeoDB::publish_and_install_handoff`).
//!
//! The caller opens a writable generation root (which holds the exclusive
//! `root.lock` and the lease registry for the DB lifetime), drives the epoch
//! handoff (freeze → build → publish → retire), then installs the published
//! generation as the live base through the registry's `publish` + the layered
//! repair swap. These tests prove the combined API end to end on a REAL
//! `open_generation_root` database:
//!
//! 1. Happy path: publish+install redirects the registry, swaps the layered
//!    base to the new generation container, and reads serve through it.
//! 2. Writes during the handoff window (DESIGN G2): a post-freeze write
//!    survives the install (N+1 wins over the new base), and the report
//!    cannot be installed twice.
//! 3. Fail-closed: a database without a generation root cannot install.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal"
))]

use std::fs;

use grafeo_common::types::{PropertyKey, Value};
use grafeo_core::graph::compact::layered::LayeredStore;
use grafeo_core::graph::traits::{GraphStore, GraphStoreMut};
use grafeo_engine::{EpochHandoffPhase, GrafeoDB, generation_build_request};
use tempfile::TempDir;

/// Publish the initial base generation into `gen_root` (the registry's first
/// selected base) from a scratch in-memory database.
fn publish_base_generation(gen_root: &std::path::Path) {
    let source = GrafeoDB::new_in_memory();
    source
        .create_node_with_props(&["Person"], [("name", Value::from("Ada"))])
        .expect("create Ada");
    source
        .create_node_with_props(&["Person"], [("name", Value::from("Grace"))])
        .expect("create Grace");
    source
        .build_and_publish_generation(generation_build_request(gen_root, "g-base"))
        .expect("publish base generation");
    drop(source);
}

/// Every `Person` `name` served by the layered store (base + overlay), sorted.
fn live_person_names(layered: &LayeredStore) -> Vec<String> {
    let mut names: Vec<String> = layered
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
    names.sort_unstable();
    names
}

// ── Happy path ─────────────────────────────────────────────────────

#[test]
fn publish_and_install_handoff_happy_path() {
    let dir = TempDir::new().expect("temp dir");
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).expect("create generation root");
    publish_base_generation(&gen_root);

    // Open the writable generation root: layered store over the base +
    // registry selected on the base generation.
    let db =
        GrafeoDB::open_generation_root(&gen_root, false).expect("open writable generation root");
    let old_base_nodes = db
        .layered_store()
        .expect("layered store")
        .base_store_arc()
        .total_nodes();
    assert_eq!(old_base_nodes, 2, "registry base = Ada + Grace");

    // Epoch-N overlay writes through the layered store (frozen into G(1)).
    let layered = db.layered_store().expect("layered store");
    let n1 = layered.create_node(&["Person"]);
    layered.set_node_property(n1, "name", Value::from("epoch-n-one"));

    // Full production cycle on the generation-root DB.
    let report = db
        .run_epoch_handoff(generation_build_request(&gen_root, "g-hand-1"))
        .expect("run epoch handoff");
    assert_eq!(report.phase, EpochHandoffPhase::EpochRetired);
    assert!(
        report.post_freeze_nodes.is_empty() && report.post_freeze_edges.is_empty(),
        "quiesced window: report must carry zero post-freeze writes: {report:?}"
    );

    // Combined publish → install: registry redirect + base swap.
    let install = db
        .publish_and_install_handoff(report.clone())
        .expect("publish and install handoff");
    let publication = report.publication.expect("publication present");
    assert_eq!(
        install.publication_sequence,
        publication.publication.publication_sequence
    );
    assert_eq!(install.generation_id, "g-hand-1");
    assert_eq!(install.generation_abs_path, publication.generation_abs_path);

    // The layered base now serves the NEW generation container: the frozen
    // snapshot absorbed into G(1) is base-resident (Ada, Grace + epoch-n-one).
    let base = db.layered_store().expect("layered store").base_store_arc();
    assert_eq!(
        base.total_nodes(),
        3,
        "swapped base must hold base + frozen epoch-N nodes"
    );
    assert_eq!(base.total_nodes(), install.base_node_count);
    assert_eq!(base.total_edges(), install.base_edge_count);

    // Reads work through the swapped base (dirty for absorbed ids was
    // selectively cleared, so dispatch falls through to the new base).
    let names = live_person_names(db.layered_store().expect("layered store"));
    assert_eq!(names, ["Ada", "Grace", "epoch-n-one"]);

    // Post-freeze bookkeeping clean: retire cleared the live handoff slot.
    assert!(
        db.layered_store()
            .expect("layered store")
            .handoff_live()
            .is_none(),
        "no live handoff bookkeeping may survive the install"
    );
}

// ── Writes during the handoff window (DESIGN G2) ───────────────────

#[test]
fn publish_and_install_keeps_writes_during_window() {
    let dir = TempDir::new().expect("temp dir");
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).expect("create generation root");
    publish_base_generation(&gen_root);

    let db =
        GrafeoDB::open_generation_root(&gen_root, false).expect("open writable generation root");
    let layered = || db.layered_store().expect("layered store");
    assert_eq!(layered().base_store_arc().total_nodes(), 2, "Ada + Grace");

    // Epoch N: absorbed into G(1).
    let n0 = layered().create_node(&["Person"]);
    layered().set_node_property(n0, "name", Value::from("epoch-n-absorbed"));

    // Two-step handoff with a post-freeze write (epoch N+1).
    let handle = db
        .freeze_epoch_for_handoff(&gen_root)
        .expect("freeze epoch N");
    let n1 = layered().create_node(&["Person"]);
    layered().set_node_property(n1, "name", Value::from("post-freeze-write"));
    // ...and a post-freeze write to the frozen N row.
    layered().set_node_property(n0, "name", Value::from("rewritten-at-n+1"));
    let report = db
        .complete_epoch_handoff(handle, generation_build_request(&gen_root, "g-hand-1"))
        .expect("complete handoff");
    assert_eq!(report.phase, EpochHandoffPhase::EpochRetired);
    assert!(
        report.post_freeze_nodes.contains(&n1.as_u64()),
        "the post-freeze write is recorded on the report: {report:?}"
    );

    db.publish_and_install_handoff(report.clone())
        .expect("install keeps the post-freeze writes");
    assert_eq!(
        layered().base_store_arc().total_nodes(),
        3,
        "the new base holds Ada, Grace and the epoch-N node"
    );
    let name = |id| {
        db.graph_store()
            .get_node_property(id, &PropertyKey::new("name"))
    };
    assert_eq!(name(n0), Some(Value::from("rewritten-at-n+1")), "N+1 wins");
    assert_eq!(name(n1), Some(Value::from("post-freeze-write")));
    assert!(
        layered().handoff_live().is_none(),
        "the install consumed the handoff"
    );

    let err = db
        .publish_and_install_handoff(report)
        .expect_err("a report installs once");
    assert!(
        err.to_string().contains("no retired handoff"),
        "typed nothing-to-install error expected, got: {err}"
    );
}

// ── Fail-closed: no generation root ────────────────────────────────

#[test]
fn publish_and_install_fails_closed_without_generation_root() {
    let dir = TempDir::new().expect("temp dir");
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).expect("create generation root");

    // A plain in-memory DB can still drive the handoff (pure-LPG freeze
    // path), but has no registry: the install must fail closed.
    let db = GrafeoDB::new_in_memory();
    db.create_node_with_props(&["Person"], [("name", Value::from("Ada"))])
        .expect("create Ada");
    let report = db
        .run_epoch_handoff(generation_build_request(&gen_root, "g-oneshot"))
        .expect("one-shot handoff");
    assert_eq!(report.phase, EpochHandoffPhase::EpochRetired);

    let err = db
        .publish_and_install_handoff(report)
        .expect_err("install without a generation root must fail closed");
    let msg = err.to_string();
    assert!(
        msg.contains("generation-root"),
        "typed not-a-generation-root error expected, got: {msg}"
    );
}
