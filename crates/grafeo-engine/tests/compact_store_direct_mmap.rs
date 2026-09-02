//! Read-only CompactStore container mapping regression coverage.
//!
//! Support matrix exercised here:
//! - plaintext, mmap-able CompactStore sections → `CompactBacking::ContainerMmap`
//! - graph point lookups and CSR traversal remain byte-identical after mapped open
//! - retained mapping outlives the database handle while any base Arc is held
//!
//! Encrypted / non-mmap-able layouts return `StorageError::DirectMmapUnavailable`
//! from `GrafeoFileManager::mmap_section` (covered in storage unit tests) and must
//! never claim `ContainerMmap`.
//!
//! ```bash
//! cargo test -p grafeo-engine --features compact-store \
//!   --test compact_store_direct_mmap -- --nocapture
//! ```

#![cfg(all(feature = "compact-store", feature = "grafeo-file", feature = "lpg"))]

use grafeo_common::storage::SectionType;
use grafeo_common::types::{NodeId, PropertyKey, Value};
use grafeo_core::graph::{Direction, traits::GraphStore};
use grafeo_engine::{CompactBacking, Config, GrafeoDB};
use grafeo_storage::file::GrafeoFileManager;

#[test]
fn readonly_reopen_owns_the_compact_container_mapping_and_preserves_graph_reads() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("direct-mapped.grafeo");

    // Build a small multi-node graph so reopen exercises deterministic random
    // property reads, not only the first two rows.
    let (nodes, knows): (Vec<(NodeId, String, i64)>, _) = {
        let mut db = GrafeoDB::with_config(Config::persistent(&path)).expect("create database");
        let mut nodes = Vec::new();
        for (name, rank) in [
            ("Alix", 1_i64),
            ("Gus", 2),
            ("Mara", 3),
            ("Ned", 4),
            ("Ora", 5),
        ] {
            let id = db
                .create_node_with_props(
                    &["Person"],
                    [("name", Value::from(name)), ("rank", Value::Int64(rank))],
                )
                .expect("create person");
            nodes.push((id, name.to_string(), rank));
        }
        let knows = db.create_edge(nodes[0].0, nodes[1].0, "KNOWS");
        let _also = db.create_edge(nodes[2].0, nodes[3].0, "KNOWS");
        db.compact().expect("compact base");
        db.close().expect("explicit close");
        (nodes, knows)
    };

    let expected_payload_bytes = {
        let manager = GrafeoFileManager::open_read_only(&path).expect("open container");
        let directory = manager
            .read_section_directory()
            .expect("read directory")
            .expect("section directory");
        let entry = directory
            .find(SectionType::CompactStore)
            .expect("CompactStore section");
        assert!(
            entry.flags.mmap_able,
            "CompactStore directory flag is necessary but not sufficient for ContainerMmap"
        );
        entry.length as usize
    };

    let db = GrafeoDB::open_read_only(&path).expect("read-only reopen");
    let CompactBacking::ContainerMmap {
        artifact_id,
        payload_version,
        mapped_bytes,
    } = db.compact_backing().expect("compact backing diagnostic")
    else {
        panic!("read-only CompactStore reopen must use ContainerMmap");
    };
    assert!(artifact_id.starts_with("container:"));
    assert_eq!(
        *payload_version, 5,
        "G-EM0.2 direct-mapped CompactStore payload is v5"
    );
    assert_eq!(*mapped_bytes, expected_payload_bytes);

    let base = db
        .layered_store()
        .expect("layered store after compact reopen")
        .base_store_arc();
    assert_eq!(base.mapped_backing_bytes(), Some(expected_payload_bytes));
    // Proportional structures are file-backed; split accounting is the
    // Milestone R evidence surface (not memory_bytes alone).
    let acc = base
        .memory_accounting()
        .expect("v5 mapped open must record split accounting");
    assert_eq!(acc.anonymous_proportional_structure_bytes, 0);
    assert!(acc.mapped_payload_index_bytes > 0);
    assert!(acc.is_disk_native_graph());

    // Deterministic pseudo-random order (fixed LCG) over node property reads.
    let mut state: u64 = 0xC0FFEE;
    for _ in 0..16 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = (state as usize) % nodes.len();
        let (id, name, rank) = &nodes[idx];
        assert_eq!(
            base.get_node_property(*id, &PropertyKey::new("name")),
            Some(Value::from(name.as_str())),
            "point property name must remain identical for node {idx}"
        );
        assert_eq!(
            base.get_node_property(*id, &PropertyKey::new("rank")),
            Some(Value::Int64(*rank)),
            "point property rank must remain identical for node {idx}"
        );
    }

    let (alix, gus) = (nodes[0].0, nodes[1].0);
    assert_eq!(
        base.edges_from(alix, Direction::Outgoing),
        vec![(gus, knows)]
    );
    assert_eq!(
        base.edges_from(gus, Direction::Incoming),
        vec![(alix, knows)]
    );
    assert_eq!(
        base.edges_from(nodes[2].0, Direction::Outgoing).len(),
        1,
        "second KNOW edge must survive mapped reopen"
    );

    // Release the shared RO lock before an exclusive writable open, but keep a
    // base Arc so the mapping owner outlives the database handle.
    db.close().expect("read-only close");
    assert_eq!(
        base.get_node_property(gus, &PropertyKey::new("rank")),
        Some(Value::Int64(2)),
        "mapped owner must outlive the closed database handle"
    );

    // Writable reopen of the same artifact remains the explicit legacy path and
    // must not claim Milestone R ContainerMmap evidence. v5 property strings
    // are file-backed, so a concurrent writable open of the same path can
    // invalidate an existing mapping; exercise LegacyEager on a byte-identical
    // copy instead so the RO base Arc remains valid.
    {
        let writable_path = temp.path().join("direct-mapped-writable-copy.grafeo");
        std::fs::copy(&path, &writable_path).expect("copy artifact");
        let writable = GrafeoDB::open(&writable_path).expect("writable reopen");
        match writable.compact_backing() {
            Some(CompactBacking::LegacyEager { payload_bytes, .. }) => {
                assert_eq!(*payload_bytes, expected_payload_bytes);
            }
            other => panic!("writable reopen must report LegacyEager, got {other:?}"),
        }
        writable.close().expect("close writable handle");
    }

    // The earlier base Arc still serves reads through its retained mapping.
    assert_eq!(
        base.get_node_property(alix, &PropertyKey::new("name")),
        Some(Value::from("Alix"))
    );
    drop(base);
}
