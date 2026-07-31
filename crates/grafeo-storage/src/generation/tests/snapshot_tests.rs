//! Snapshot tests: publish + verify, path/name rejection, stale-source
//! rejection, and independence from live-root GC.

use crate::file::generation_writer::OsGenerationFileOps;
use crate::generation::lock::RootLock;
use crate::generation::publication::{PublicationInput, publish_generation};
use crate::generation::recovery::recover;
use crate::generation::snapshot::{SnapshotError, publish_snapshot};
use crate::generation::tests::support::{fixture_section, new_root};
use tempfile::TempDir;

fn publish_once(
    lock: &RootLock,
    fixture: &crate::generation::tests::support::RootFixture,
    generation_id: &str,
) {
    let (section, header) = fixture_section();
    let mut sections: Vec<Box<dyn crate::file::generation_writer::ExactSectionSource>> =
        vec![section];
    let input = PublicationInput {
        header,
        sections: &mut sections,
        generation_id: generation_id.to_string(),
        parent_generation_id: None,
        parent_publication_sequence: None,
    };
    publish_generation(lock, input, &fixture.wal, &OsGenerationFileOps, None).expect("publish");
}

fn selected_fixture() -> (
    crate::generation::tests::support::RootFixture,
    TempDir,
    crate::generation::recovery::SelectedGeneration,
) {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    publish_once(&lock, &fixture, "g-snap");
    drop(lock);
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    let selected = recover(&lock).expect("recover");
    let dest = TempDir::new().unwrap();
    (fixture, dest, selected)
}

#[test]
fn snapshot_publish_and_verify() {
    let (fixture, dest, selected) = selected_fixture();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();

    let provenance = publish_snapshot(
        &lock,
        &selected,
        dest.path(),
        "snapshot-1.grafeo",
        &OsGenerationFileOps,
        None,
    )
    .expect("snapshot must publish");

    assert_eq!(provenance.source_generation_id, "g-snap");
    assert_eq!(provenance.source_publication_sequence, 1);
    assert_eq!(provenance.source_sha256, selected.slot.generation_sha256);
    assert_eq!(provenance.format_version, 5);

    // Bytes match the source exactly.
    let src_bytes = std::fs::read(&selected.generation_abs_path).unwrap();
    let snap_bytes = std::fs::read(dest.path().join("snapshot-1.grafeo")).unwrap();
    assert_eq!(snap_bytes, src_bytes);
}

#[test]
fn snapshot_rejects_inside_root() {
    let (fixture, _dest, selected) = selected_fixture();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();

    let inside = fixture.root().join("generations");
    match publish_snapshot(
        &lock,
        &selected,
        &inside,
        "snap.grafeo",
        &OsGenerationFileOps,
        None,
    ) {
        Err(SnapshotError::DestinationInsideRoot) => {}
        other => panic!("expected DestinationInsideRoot, got {other:?}"),
    }
}

#[test]
fn snapshot_rejects_unsafe_name() {
    let (fixture, dest, selected) = selected_fixture();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();

    for bad in ["../escape", "/abs", "a/b", "", ".", ".."] {
        let err = publish_snapshot(
            &lock,
            &selected,
            dest.path(),
            bad,
            &OsGenerationFileOps,
            None,
        )
        .expect_err("unsafe name must fail");
        assert!(
            matches!(err, SnapshotError::UnsafeName),
            "name {bad:?}: got {err:?}"
        );
    }
}

#[test]
fn snapshot_rejects_stale_source() {
    let (fixture, dest, selected) = selected_fixture();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();

    // Corrupt the generation file after selection.
    std::fs::write(&selected.generation_abs_path, b"stale").unwrap();

    match publish_snapshot(
        &lock,
        &selected,
        dest.path(),
        "snap.grafeo",
        &OsGenerationFileOps,
        None,
    ) {
        Err(SnapshotError::SourceInvalid(_)) => {}
        other => panic!("expected SourceInvalid, got {other:?}"),
    }
}

#[test]
fn snapshot_independent_of_root_gc() {
    let (fixture, dest, selected) = selected_fixture();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();

    publish_snapshot(
        &lock,
        &selected,
        dest.path(),
        "snap.grafeo",
        &OsGenerationFileOps,
        None,
    )
    .expect("publish");

    // Delete the source generation from the live root; the snapshot must
    // remain readable and byte-identical.
    std::fs::remove_file(&selected.generation_abs_path).unwrap();
    let snap_bytes = std::fs::read(dest.path().join("snap.grafeo")).unwrap();
    assert!(!snap_bytes.is_empty());

    // Fresh-open the snapshot through the production reader.
    let manager =
        crate::file::GrafeoFileManager::open_read_only(dest.path().join("snap.grafeo")).unwrap();
    let dir = manager.read_section_directory().unwrap().unwrap();
    let entry = dir
        .find(grafeo_common::storage::SectionType::CompactStore)
        .expect("CompactStore section");
    assert!(entry.length > 0);
}
