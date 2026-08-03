//! H-ADOPT.2 — production generation-root open proof.
//!
//! Builds and publishes a real W generation, then opens it through
//! `GrafeoDB::open_generation_root_with_config` and serves a normal session
//! query from the mmap-backed CompactStore base.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap"
))]

use grafeo_common::types::Value;
use grafeo_engine::{Config, GrafeoDB, generation_build_request};
use tempfile::tempdir;

#[test]
fn generation_root_open_serves_base_and_preserves_config() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("runtime-open.grafeo.d");
    let spill = dir.path().join("bounded-spill");
    std::fs::create_dir_all(&root).expect("create generation root");

    let source = GrafeoDB::new_in_memory();
    source
        .create_node_with_props(&["Person"], [("name", Value::from("Ada"))])
        .expect("create Ada");
    source
        .create_node_with_props(&["Person"], [("name", Value::from("Grace"))])
        .expect("create Grace");
    source
        .build_and_publish_generation(generation_build_request(&root, "runtime-open-g1"))
        .expect("publish generation");
    drop(source);

    let config = Config::persistent(&root)
        .with_memory_limit(64 * 1024 * 1024)
        .with_spill_path(&spill)
        .with_threads(2);
    let db = GrafeoDB::open_generation_root_with_config(config)
        .expect("open generation root through production constructor");

    assert_eq!(db.config().memory_limit, Some(64 * 1024 * 1024));
    assert_eq!(db.config().spill_path.as_deref(), Some(spill.as_path()));
    assert_eq!(db.config().threads, 2);

    let result = db
        .session()
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("query generation base");
    let mut names: Vec<String> = result
        .rows()
        .iter()
        .map(|row| match &row[0] {
            Value::String(value) => value.as_str().to_string(),
            other => panic!("expected string name, got {other:?}"),
        })
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["Ada", "Grace"]);

    // The live DB must retain the process root lock + selected base lease.
    assert!(GrafeoDB::open_generation_root(&root, false).is_err());
    drop(db);

    // Releasing the database releases ownership; a clean reopen serves the
    // same immutable base (post-boundary WAL replay is H-ADOPT.3).
    let reopened = GrafeoDB::open_generation_root(&root, false).expect("reopen after drop");
    let result = reopened
        .session()
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("query reopened generation base");
    assert_eq!(result.row_count(), 2);
}

/// H-ADOPT.2 review M-1 regression: the fresh overlay installed by
/// `open_generation_root*` must have its node/edge ID allocators seeded
/// above the mapped base's preserved ID maxima BEFORE any write can occur.
///
/// Without seeding, the first overlay `create_node` returns `NodeId(0)`,
/// which collides with base node 0 in any non-empty base; the first
/// `create_edge` likewise collides with base edge 0.
#[test]
fn generation_root_first_writes_are_seeded_above_base_ids() {
    use grafeo_core::graph::Direction;

    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("seed-overlay.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    // Publish a non-empty base: two nodes (LpgStore ids 0, 1) + one edge (id 0).
    let source = GrafeoDB::new_in_memory();
    let ada = source
        .create_node_with_props(&["Person"], [("name", Value::from("Ada"))])
        .expect("create Ada");
    let grace = source
        .create_node_with_props(&["Person"], [("name", Value::from("Grace"))])
        .expect("create Grace");
    let knows =
        source.create_edge_with_props(ada, grace, "KNOWS", [("since", Value::from(2020i64))]);
    source
        .build_and_publish_generation(generation_build_request(&root, "seed-overlay-g1"))
        .expect("publish generation");
    drop(source);

    let db = GrafeoDB::open_generation_root(&root, false).expect("open generation root writable");
    let read = db.graph_store();

    // Base IDs and data are visible before any write.
    let base_node_ids = read.nodes_by_label("Person");
    assert_eq!(base_node_ids.len(), 2, "base serves both Person nodes");
    assert!(base_node_ids.contains(&ada), "base node Ada visible");
    assert!(base_node_ids.contains(&grace), "base node Grace visible");
    let base_max_node = base_node_ids.iter().map(|id| id.as_u64()).max().unwrap();
    let base_edges = read.edges_from(ada, Direction::Outgoing);
    assert_eq!(base_edges.len(), 1, "base serves the KNOWS edge");
    assert_eq!(base_edges[0].1, knows, "base edge id preserved");
    let base_max_edge = knows.as_u64();
    let ada_node = read.get_node(ada).expect("base node readable");
    assert_eq!(
        ada_node.properties.get(&"name".into()),
        Some(&Value::from("Ada")),
        "base properties readable"
    );

    // First writes through the installed layered store.
    let write = db
        .graph_store_mut()
        .expect("writable generation root exposes a write store");
    let new_node = write.create_node(&["Person"]);
    let new_edge = write.create_edge(ada, grace, "WORKS_WITH");

    // M-1 proof: no collision with any base ID, strictly above the base maxima.
    assert!(
        !base_node_ids.contains(&new_node),
        "first overlay node id {new_node:?} collides with a base node id"
    );
    assert!(
        new_node.as_u64() > base_max_node,
        "first overlay node id {new_node:?} must exceed base max {base_max_node}"
    );
    assert_ne!(
        new_edge, knows,
        "first overlay edge id collides with the base edge id"
    );
    assert!(
        new_edge.as_u64() > base_max_edge,
        "first overlay edge id {new_edge:?} must exceed base max {base_max_edge}"
    );

    // Base data remains fully visible after the first writes.
    let ada_after = read.get_node(ada).expect("base node still visible");
    assert_eq!(
        ada_after.properties.get(&"name".into()),
        Some(&Value::from("Ada")),
        "base node not replaced by the overlay write"
    );
    let knows_after = read.get_edge(knows).expect("base edge still visible");
    assert_eq!(knows_after.src, ada);
    assert_eq!(knows_after.dst, grace);
    let new_edge_read = read.get_edge(new_edge).expect("new edge readable");
    assert_eq!(new_edge_read.src, ada, "new edge endpoints are base nodes");
    assert_eq!(new_edge_read.dst, grace);
    let after_ids = read.nodes_by_label("Person");
    assert_eq!(
        after_ids.len(),
        3,
        "base nodes plus the new overlay node all visible"
    );
    assert!(after_ids.contains(&ada));
    assert!(after_ids.contains(&grace));
    assert!(after_ids.contains(&new_node));
}

/// Empty-base companion for the M-1 seeding: an empty generation base has no
/// IDs to avoid, so the overlay allocator starts at 0 (its natural default)
/// rather than failing or guessing.
#[test]
fn generation_root_empty_base_first_write_starts_at_zero() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("seed-empty.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    let source = GrafeoDB::new_in_memory();
    source
        .build_and_publish_generation(generation_build_request(&root, "seed-empty-g1"))
        .expect("publish empty generation");
    drop(source);

    let db =
        GrafeoDB::open_generation_root(&root, false).expect("open generation root with empty base");
    let write = db
        .graph_store_mut()
        .expect("writable generation root exposes a write store");
    let a = write.create_node(&["Empty"]);
    assert_eq!(a.as_u64(), 0, "empty base: node allocator starts at 0");
    let b = write.create_node(&["Empty"]);
    let e = write.create_edge(a, b, "LINK");
    assert_eq!(e.as_u64(), 0, "empty base: edge allocator starts at 0");

    let read = db.graph_store();
    assert!(read.get_node(a).is_some(), "first node readable");
    let edge = read.get_edge(e).expect("first edge readable");
    assert_eq!(edge.src, a);
    assert_eq!(edge.dst, b);
}

/// H-ADOPT.3 Phase C (D1 + amendment 2): query writes made through a session
/// on a writable generation root must reach the root's WAL. The layered
/// session branch must carry a WAL-logging write store plus commit/epoch
/// logging; without that wiring nothing is durable and a reopen cannot
/// replay anything.
#[test]
fn layered_session_query_writes_are_wal_logged() {
    use grafeo_storage::generation::manifest::read_manifest;
    use grafeo_storage::generation::wal_cursor::{WalReplayCursor, replay_stream_from};
    use grafeo_storage::wal::WalRecord;

    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("wal-logged.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    let source = GrafeoDB::new_in_memory();
    source
        .build_and_publish_generation(generation_build_request(&root, "wal-logged-g1"))
        .expect("publish empty generation");
    drop(source);

    {
        let db =
            GrafeoDB::open_generation_root(&root, false).expect("open generation root writable");
        db.session()
            .execute("INSERT (:Person {name: 'Ada'})")
            .expect("insert through layered session");
    }

    // Scan the root WAL from the durable publication boundary: the session
    // writes must appear as committed, epoch-advanced records.
    let (_, slot) = read_manifest(&root.join("manifest.bin")).expect("read manifest");
    let cursor = WalReplayCursor {
        log_sequence: slot.wal_log_sequence,
        byte_offset: slot.wal_byte_offset,
        epoch: slot.overlay_epoch,
        transaction_id: slot.transaction_id,
    };
    let stream = replay_stream_from(&root.join("wal"), &cursor).expect("stream from boundary");

    let mut saw_create_node = false;
    let mut saw_commit = false;
    let mut saw_epoch_advance = false;
    for frame in stream {
        let frame = frame.expect("post-boundary frame decodes");
        match frame.record {
            WalRecord::CreateNode { .. } => saw_create_node = true,
            WalRecord::TransactionCommit { .. } => saw_commit = true,
            WalRecord::EpochAdvance { .. } => saw_epoch_advance = true,
            _ => {}
        }
    }
    assert!(
        saw_create_node,
        "layered session INSERT must log CreateNode to the root WAL"
    );
    assert!(
        saw_commit,
        "layered session auto-commit must log TransactionCommit to the root WAL"
    );
    assert!(
        saw_epoch_advance,
        "layered session auto-commit must log EpochAdvance to the root WAL"
    );
}
