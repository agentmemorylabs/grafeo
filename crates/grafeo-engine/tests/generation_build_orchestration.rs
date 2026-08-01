//! G-EM0.3a — live engine orchestration for immutable generation build.
//!
//! Proves:
//! 1. Live `GrafeoDB` graph streams into W0 `generate_compact_store` without
//!    materializing `Vec<GenerationNode>` / `Vec<GenerationEdge>`.
//! 2. Publication goes through W0 `publish_generation` (11-step ordering).
//! 3. Legacy standalone `.grafeo` snapshots remain readable and are never
//!    transparently rewritten into the generation layout.
//! 4. The returned descriptor is validated/published but not selected by this
//!    packet (selection remains W0 recovery).

use std::fs;
use std::path::Path;

use bytes::Bytes;
use grafeo_common::storage::SectionType;
use grafeo_common::types::{PropertyKey, Value};
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_engine::GrafeoDB;
use grafeo_engine::database::generation_build::{
    generation_build_request, path_is_generation_root,
};
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::recovery::recover;
use tempfile::TempDir;

fn populate_live_graph(db: &GrafeoDB) {
    let ada = db
        .create_node_with_props(&["Person"], [("name", Value::from("Ada"))])
        .expect("ada");
    let bob = db
        .create_node_with_props(&["Person"], [("name", Value::from("Bob"))])
        .expect("bob");
    let project = db
        .create_node_with_props(&["Project"], [("title", Value::from("Grafeo"))])
        .expect("project");
    let _knows = db.create_edge_with_props(ada, bob, "KNOWS", [("since", Value::Int64(2020))]);
    let _works = db.create_edge(ada, project, "WORKS_ON");
}

fn write_legacy_standalone(path: &Path) {
    let db = GrafeoDB::new_in_memory();
    populate_live_graph(&db);
    db.save(path).expect("save standalone .grafeo");
}

/// Open a published generation container through the production read path and
/// verify the deserialized CompactStore contents (R1 review repair).
///
/// Reuses the exact mechanism of the W0 contract test
/// `compact_store_generation_contract`: `GrafeoFileManager::open_read_only` +
/// `CompactStoreSection::deserialize_from_bytes` via the public API — no second
/// reader implementation. Assertions are non-zero-count and real-data presence
/// checks, so a generation with zero records (e.g. a broken record source) or
/// missing overlay data fails here.
fn assert_published_generation_contents(
    path: &Path,
    expected_nodes: u64,
    expected_edges: u64,
    expected_person_names: &[&str],
    expected_edge_types: &[&str],
) {
    let manager = GrafeoFileManager::open_read_only(path).expect("open published generation");
    let section_dir = manager.read_section_directory().unwrap().unwrap();
    let entry = section_dir
        .find(SectionType::CompactStore)
        .expect("CompactStore section present");
    assert!(entry.length > 0);

    let data = manager
        .read_section_data(entry)
        .expect("read CompactStore section");
    let mut cs_section = CompactStoreSection::empty();
    cs_section
        .deserialize_from_bytes(Bytes::from(data))
        .expect("published v5 payload must deserialize through the public API");
    let store = cs_section.store().expect("store must be present");

    assert_eq!(store.total_nodes(), expected_nodes, "published node count");
    assert_eq!(store.total_edges(), expected_edges, "published edge count");

    let mut person_names: Vec<String> = Vec::new();
    if let Some(person) = store.node_table("Person") {
        for offset in 0..person.len() {
            if let Some(Value::String(name)) =
                person.get_property(offset, &PropertyKey::new("name"))
            {
                person_names.push(name.as_str().to_string());
            }
        }
    }
    for expected in expected_person_names {
        assert!(
            person_names.iter().any(|n| n.as_str() == *expected),
            "Person table must contain node named {expected:?}, got {person_names:?}"
        );
    }

    for edge_type in expected_edge_types {
        let table = store
            .rel_table(edge_type)
            .unwrap_or_else(|| panic!("rel table {edge_type:?} must exist in published store"));
        assert!(
            table.num_edges() > 0,
            "rel table {edge_type:?} must have edges"
        );
    }
}

#[test]
fn live_engine_publishes_immutable_generation_via_w0() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate_live_graph(&db);

    let request = generation_build_request(&gen_root, "g-live-3a");
    let descriptor = db
        .build_immutable_generation(request)
        .expect("build_immutable_generation");

    assert_eq!(descriptor.generation_id, "g-live-3a");
    assert_eq!(descriptor.publication_sequence, 1);
    assert!(descriptor.generation_length > 0);
    assert!(descriptor.generation_path.starts_with("generations/g-"));
    assert!(descriptor.generation_abs_path.is_file());
    assert!(path_is_generation_root(&gen_root));
    assert!(gen_root.join("manifest.bin").is_file());
    assert!(gen_root.join("generations").is_dir());

    // This packet does not select; W0 recovery does.
    let lock = RootLock::try_acquire(&gen_root).expect("re-lock for recover");
    let selected = recover(&lock).expect("recover selects published generation");
    assert_eq!(selected.slot.generation_id, "g-live-3a");
    assert_eq!(selected.slot.publication_sequence, 1);

    // Deserialize the published generation through the production read path
    // (same mechanism as the W0 contract test) and verify actual graph
    // contents: 3 nodes, 2 edges, and the known data ("Ada", "KNOWS").
    assert_published_generation_contents(&selected.generation_abs_path, 3, 2, &["Ada"], &["KNOWS"]);
}

#[test]
fn layered_live_graph_streams_base_plus_overlay() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("layered.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let mut db = GrafeoDB::new_in_memory();
    populate_live_graph(&db);
    db.compact().expect("compact base");
    // Overlay mutation after compact must appear in the frozen live view.
    db.create_node_with_props(&["Person"], [("name", Value::from("Carol"))])
        .expect("carol overlay");

    let descriptor = db
        .build_immutable_generation(generation_build_request(&gen_root, "g-layered"))
        .expect("layered generation");
    assert_eq!(descriptor.publication_sequence, 1);

    let lock = RootLock::try_acquire(&gen_root).unwrap();
    let selected = recover(&lock).unwrap();

    // Clause-2 layered proof: the published generation must contain the base
    // nodes AND the overlay node created after `db.compact()` (Carol) —
    // 4 nodes / 2 edges total.
    assert_published_generation_contents(
        &selected.generation_abs_path,
        4,
        2,
        &["Ada", "Carol"],
        &["KNOWS", "WORKS_ON"],
    );
}

#[test]
fn legacy_standalone_grafeo_remains_readable_and_untouched() {
    let dir = TempDir::new().unwrap();
    let legacy_path = dir.path().join("legacy.grafeo");
    let gen_root = dir.path().join("writable.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    write_legacy_standalone(&legacy_path);
    let legacy_before = fs::read(&legacy_path).expect("read legacy bytes");
    assert!(!legacy_before.is_empty());
    assert!(!path_is_generation_root(&legacy_path));

    // Build a generation from a separate in-memory DB into the generation root.
    let db = GrafeoDB::new_in_memory();
    populate_live_graph(&db);
    db.build_immutable_generation(generation_build_request(&gen_root, "g-parallel"))
        .expect("generation publish");

    let legacy_after = fs::read(&legacy_path).expect("re-read legacy");
    assert_eq!(
        legacy_before, legacy_after,
        "legacy standalone bytes must not change during generation publish"
    );
    assert!(
        !legacy_path.with_extension("d").join("generations").exists()
            && !dir.path().join("legacy.grafeo.d").exists(),
        "must not create a generation layout from the legacy path"
    );

    // Legacy reopen still works through the production database open path.
    let reopened = GrafeoDB::open_read_only(&legacy_path).expect("reopen legacy standalone");
    assert_eq!(reopened.node_count(), 3);
    assert_eq!(reopened.edge_count(), 2);
    drop(reopened);

    // And the generation root is a distinct layout.
    assert!(path_is_generation_root(&gen_root));
    assert!(gen_root.join("generations").is_dir());
}

#[test]
fn refuses_to_build_into_standalone_file_path() {
    let dir = TempDir::new().unwrap();
    let file_path = dir.path().join("not-a-root.grafeo");
    fs::write(&file_path, b"placeholder").unwrap();

    let db = GrafeoDB::new_in_memory();
    populate_live_graph(&db);
    let err = db
        .build_immutable_generation(generation_build_request(&file_path, "g-bad"))
        .expect_err("must refuse standalone file target");
    let msg = err.to_string();
    assert!(
        msg.contains("standalone") || msg.contains("generation root"),
        "unexpected error: {msg}"
    );
}
