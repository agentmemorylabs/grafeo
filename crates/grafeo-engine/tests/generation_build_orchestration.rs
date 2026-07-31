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

use grafeo_common::storage::SectionType;
use grafeo_common::types::Value;
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

    let manager = GrafeoFileManager::open_read_only(&selected.generation_abs_path)
        .expect("open published generation");
    let section_dir = manager.read_section_directory().unwrap().unwrap();
    let entry = section_dir
        .find(SectionType::CompactStore)
        .expect("CompactStore section present");
    assert!(entry.length > 0);
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
    let manager = GrafeoFileManager::open_read_only(&selected.generation_abs_path).unwrap();
    let section_dir = manager.read_section_directory().unwrap().unwrap();
    assert!(
        section_dir
            .find(SectionType::CompactStore)
            .expect("section")
            .length
            > 0
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
