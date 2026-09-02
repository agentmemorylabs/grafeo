//! Publication tests: genesis, second generation, existing-target failure.

use std::path::Path;

use crate::file::generation_writer::OsGenerationFileOps;
use crate::generation::lock::RootLock;
use crate::generation::publication::{PublicationInput, PublicationError, publish_generation};
use crate::generation::tests::support::{fixture_section, new_root};
use crate::generation::wal_cursor::validate_replayable;

/// Publish one generation on the fixture root with real file ops.
fn publish_once(
    lock: &RootLock,
    fixture: &crate::generation::tests::support::RootFixture,
    generation_id: &str,
) -> crate::generation::publication::PublicationResult {
    let (section, header) = fixture_section();
    let mut sections: Vec<Box<dyn crate::file::generation_writer::ExactSectionSource>> =
        vec![section];
    let input = PublicationInput {
        header,
        sections: &mut sections,
        generation_id: generation_id.to_string(),
        parent_generation_id: None,
        parent_publication_sequence: None,
        pre_cut_cursor: None,
    };
    publish_generation(lock, input, &fixture.wal, &OsGenerationFileOps, None).expect("publish")
}

#[test]
fn publication_genesis() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).expect("lock");

    let result = publish_once(&lock, &fixture, "g-genesis");

    // Slot 0 has seq 1; the generation file exists at the immutable path.
    assert_eq!(result.publication_sequence, 1);
    assert!(result.generation_path.starts_with("generations/g-00000000000000000001-"));
    assert!(result.generation_path.ends_with(".grafeo"));

    let gen_file = fixture.root().join(&result.generation_path);
    assert!(gen_file.exists(), "generation file must exist");
    assert_eq!(
        crate::generation::manifest::read_manifest(&fixture.root().join("manifest.bin"))
            .unwrap()
            .1
            .publication_sequence,
        1
    );

    // The recorded WAL cursor is replayable.
    validate_replayable(&fixture.wal_dir(), &result.wal_cursor).expect("cursor replayable");
}

#[test]
fn publication_second_generation() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).expect("lock");

    let first = publish_once(&lock, &fixture, "g-first");
    let second = publish_once(&lock, &fixture, "g-second");

    // Sequence monotonic and slot alternation: first in slot 0, second in slot 1.
    assert_eq!(first.publication_sequence, 1);
    assert_eq!(second.publication_sequence, 2);

    let (index, slot) =
        crate::generation::manifest::read_manifest(&fixture.root().join("manifest.bin")).unwrap();
    assert_eq!(index, 1);
    assert_eq!(slot.publication_sequence, 2);
    assert_eq!(slot.parent_publication_sequence, 1);
    assert_eq!(slot.generation_id, "g-second");
    assert_eq!(slot.parent_generation_id, "g-first");
    assert_eq!(slot.generation_sha256, second.generation_sha256);
    assert_eq!(slot.generation_length, second.generation_length);

    // Both immutable files exist; the second's file is the one referenced.
    let second_file = fixture.root().join(&second.generation_path);
    assert!(second_file.exists());
    let first_file = fixture.root().join(&first.generation_path);
    assert!(first_file.exists(), "previous generation is retained, not deleted");
}

#[test]
fn publication_existing_target_fails() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).expect("lock");

    // Pre-create the exact immutable target for seq 1. We must predict the
    // file name: the sha16 suffix depends on content, so publish once, then
    // reuse the SAME generation_id and simulate a colliding target by
    // deleting the manifest slot (so seq restarts) while leaving the file.
    let first = publish_once(&lock, &fixture, "g-collide");
    let first_path = fixture.root().join(&first.generation_path);

    // Remove the manifest so the next publication treats this as genesis,
    // but keep the old immutable file around → target exists error.
    std::fs::remove_file(fixture.root().join("manifest.bin")).unwrap();
    assert!(first_path.exists());

    let (section, header) = fixture_section();
    let mut sections: Vec<Box<dyn crate::file::generation_writer::ExactSectionSource>> =
        vec![section];
    let input = PublicationInput {
        header,
        sections: &mut sections,
        generation_id: "g-collide".to_string(),
        parent_generation_id: None,
        parent_publication_sequence: None,
        pre_cut_cursor: None,
    };
    let err = publish_generation(&lock, input, &fixture.wal, &OsGenerationFileOps, None)
        .expect_err("target exists must fail");
    assert!(
        matches!(err, PublicationError::TargetExists(_)),
        "expected TargetExists, got {err:?}"
    );
}

#[test]
fn publication_manifest_sync_is_commit_point_layout() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).expect("lock");

    publish_once(&lock, &fixture, "g-layout");

    // Root layout after a successful genesis publication.
    let root = fixture.root();
    assert!(root.join("generations").is_dir());
    assert!(root.join("wal").is_dir());
    assert!(root.join("manifest.bin").is_file());
    let manifest = std::fs::read(root.join("manifest.bin")).unwrap();
    assert_eq!(manifest.len(), 8192);

    // No unpublished dirs remain after successful publication.
    let leftovers: Vec<String> = std::fs::read_dir(root)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".unpublished-"))
        .collect();
    assert!(leftovers.is_empty(), "unpublished dirs must be cleaned up: {leftovers:?}");
}

#[allow(dead_code)]
fn _path_used(p: &Path) -> &Path {
    p
}
