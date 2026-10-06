//! G4 — CompactStore v6 (64-bit column-body geometry) through the real
//! publish → recover → mmap → reopen paths.
//!
//! - `CompactPayloadVersion::V6` generations publish with payload byte,
//!   container section version and manifest slot all at 6, read back every
//!   value, and keep their ids across a v6 rebuild and two reopens.
//! - `CompactPayloadVersion::Auto` (the default) keeps publishing v5 when
//!   every field fits, and rebuilds a v6 base back into v5.
//! - The heavy past-4-GiB round trip is `compact_store_v6_payload_large`.
//!
//! ```text
//! cargo test -p grafeo-engine --features generation,generation-streaming,compact-store,lpg,mmap,wal,cypher \
//!   --test compact_store_v6_payload
//! ```

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "compact-store",
    feature = "lpg",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::path::Path;
use std::sync::Arc;

use grafeo_common::storage::SectionType;
use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};
use grafeo_core::graph::Direction;
use grafeo_core::graph::traits::GraphStore;
use grafeo_engine::{CompactPayloadVersion, Config, GrafeoDB, generation_build_request};
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::recovery::recover;
use tempfile::tempdir;

/// (manifest slot `compact_store_format_version`, container section
/// version, payload header byte) of the root's selected generation.
fn published_versions(root: &Path) -> (u16, u8, u8) {
    let lock = RootLock::try_acquire(root).expect("root lock");
    let selected = recover(&lock).expect("recover");
    drop(lock);
    let manager =
        GrafeoFileManager::open_read_only(&selected.generation_abs_path).expect("open generation");
    let dir = manager.read_section_directory().unwrap().unwrap();
    let entry = dir.find(SectionType::CompactStore).expect("CompactStore");
    let mmap = Arc::new(manager.mmap_section(entry).expect("mmap"));
    let bytes = grafeo_storage::container::MmapSection::into_bytes(mmap);
    (
        selected.slot.compact_store_format_version,
        entry.version,
        bytes[4],
    )
}

fn config(root: &Path, version: CompactPayloadVersion) -> Config {
    Config::persistent(root).with_compact_payload_version(version)
}

fn embedding(i: u64) -> Value {
    Value::Vector(vec![i as f32, 0.25, -(i as f32), 1.0 / (i as f32 + 1.0)].into())
}

/// Seeds `count` `:Doc` nodes chained by `:NEXT` edges; returns their ids.
fn seed(db: &GrafeoDB, count: u64) -> (Vec<NodeId>, Vec<EdgeId>) {
    let mut nodes = Vec::new();
    for i in 0..count {
        nodes.push(
            db.create_node_with_props(
                &["Doc"],
                [
                    ("title", Value::from(format!("doc-{i}"))),
                    ("rank", Value::from(i as i64 - 3)),
                    ("embedding", embedding(i)),
                ],
            )
            .expect("create node"),
        );
    }
    let edges = nodes
        .windows(2)
        .enumerate()
        .map(|(i, w)| db.create_edge_with_props(w[0], w[1], "NEXT", [("w", Value::from(i as i64))]))
        .collect();
    (nodes, edges)
}

/// Every seeded id still resolves to its own values.
fn assert_seeded(db: &GrafeoDB, nodes: &[NodeId], edges: &[EdgeId], what: &str) {
    let store = db.graph_store();
    for (i, &id) in nodes.iter().enumerate() {
        let node = store
            .get_node(id)
            .unwrap_or_else(|| panic!("{what}: node {id:?} lost"));
        let i = i as u64;
        assert_eq!(
            node.properties.get(&PropertyKey::new("title")),
            Some(&Value::from(format!("doc-{i}"))),
            "{what}: node {id:?} title"
        );
        assert_eq!(
            store.get_node_property(id, &PropertyKey::new("embedding")),
            Some(embedding(i)),
            "{what}: node {id:?} embedding"
        );
    }
    for (i, &id) in edges.iter().enumerate() {
        let edge = store
            .get_edge(id)
            .unwrap_or_else(|| panic!("{what}: edge {id:?} lost"));
        assert_eq!(
            (edge.src, edge.dst),
            (nodes[i], nodes[i + 1]),
            "{what}: edge {id:?}"
        );
        assert_eq!(
            edge.properties.get(&PropertyKey::new("w")),
            Some(&Value::from(i as i64)),
            "{what}: edge {id:?} w"
        );
    }
    let out = store.edges_from(nodes[0], Direction::Outgoing);
    assert_eq!(out, vec![(nodes[1], edges[0])], "{what}: adjacency");
}

#[test]
fn auto_keeps_publishing_v5_when_everything_fits() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("auto.grafeo.d");
    std::fs::create_dir_all(&root).unwrap();
    let source = GrafeoDB::new_in_memory();
    seed(&source, 8);
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .expect("publish");
    drop(source);
    assert_eq!(published_versions(&root), (5, 5, 5));
}

/// v6 build → reopen → write → v6 rebuild (epoch handoff from the mapped v6
/// base) → two reopens: every id keeps its values (cf. fork #18, DESIGN §7
/// step 6). Then an `Auto` rebuild of that v6 base writes v5 again.
#[test]
fn v6_generation_root_keeps_ids_across_a_rebuild_and_two_reopens() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("v6.grafeo.d");
    std::fs::create_dir_all(&root).unwrap();

    let source = GrafeoDB::with_config(
        Config::in_memory().with_compact_payload_version(CompactPayloadVersion::V6),
    )
    .expect("source db");
    let (mut nodes, mut edges) = seed(&source, 12);
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .expect("publish v6 base");
    drop(source);
    assert_eq!(published_versions(&root), (6, 6, 6));

    {
        let db =
            GrafeoDB::open_generation_root_with_config(config(&root, CompactPayloadVersion::V6))
                .expect("open v6 root");
        assert_seeded(&db, &nodes, &edges, "first open");
        let extra = db
            .create_node_with_props(
                &["Doc"],
                [
                    ("title", Value::from("doc-12")),
                    ("rank", Value::from(9i64)),
                    ("embedding", embedding(12)),
                ],
            )
            .expect("overlay node");
        let last = *nodes.last().unwrap();
        edges.push(db.create_edge_with_props(last, extra, "NEXT", [("w", Value::from(11i64))]));
        nodes.push(extra);
        let report = db
            .run_epoch_handoff(generation_build_request(&root, "g2"))
            .expect("v6 epoch handoff");
        db.publish_and_install_handoff(report)
            .expect("publish and install");
        assert_seeded(&db, &nodes, &edges, "after handoff");
        db.close().expect("close");
    }
    assert_eq!(published_versions(&root), (6, 6, 6));

    for reopen in ["reopen 1", "reopen 2"] {
        let db =
            GrafeoDB::open_generation_root_with_config(config(&root, CompactPayloadVersion::V6))
                .expect(reopen);
        assert_seeded(&db, &nodes, &edges, reopen);
        db.close().expect("close");
    }

    // A default (`Auto`) binary rebuilds the small v6 base as v5.
    {
        let db = GrafeoDB::open_generation_root(&root, false).expect("open with Auto");
        assert_seeded(&db, &nodes, &edges, "auto open of v6 base");
        let report = db
            .run_epoch_handoff(generation_build_request(&root, "g3"))
            .expect("auto handoff");
        db.publish_and_install_handoff(report).expect("install");
        db.close().expect("close");
    }
    assert_eq!(published_versions(&root), (5, 5, 5));
    let db = GrafeoDB::open_generation_root(&root, false).expect("reopen v5");
    assert_seeded(&db, &nodes, &edges, "after auto rebuild");
}
