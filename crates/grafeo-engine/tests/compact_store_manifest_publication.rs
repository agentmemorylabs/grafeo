//! G-EM0.3b — manifest publication and WAL boundary integration tests.
//!
//! Proves the engine-side manifest publication contract on top of W0:
//!
//! 1. Publishing a generation produces an extended descriptor carrying the
//!    durable WAL boundary, overlay epoch, and parent linkage.
//! 2. The manifest records the exact WAL boundary the descriptor reports —
//!    they agree with the dual-slot manifest on disk (no divergence).
//! 3. The recorded WAL boundary is replayable against the real WAL files.
//! 4. A second publication advances the sequence, keeps the previous
//!    generation selected as `previous`, and never truncates the WAL before
//!    the new manifest selection is durable.
//! 5. Publication phase ordering is observable: the commit point
//!    (`ManifestSync`) is the boundary after which WAL truncation is allowed.
//! 6. Errors surface through the typed publication/manifest error surface;
//!    `Drop` is not relied on for success.

use std::fs;

use grafeo_common::types::Value;
use grafeo_engine::{GrafeoDB, PublicationPhase, generation_build_request, read_manifest_state};
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::recovery::recover;
use grafeo_storage::generation::wal_cursor::validate_replayable;
use tempfile::TempDir;

/// List `wal_*.log` sequence numbers in a directory, sorted ascending.
///
/// Read-only test helper mirroring W0's internal listing; not part of the
/// publication contract, just used to observe which WAL files survive.
fn wal_sequences(wal_dir: &std::path::Path) -> std::io::Result<Vec<u64>> {
    let mut seqs = Vec::new();
    if !wal_dir.exists() {
        return Ok(seqs);
    }
    for entry in std::fs::read_dir(wal_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "log")
            && let Some(seq) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.strip_prefix("wal_"))
                .and_then(|s| s.parse::<u64>().ok())
        {
            seqs.push(seq);
        }
    }
    seqs.sort_unstable();
    Ok(seqs)
}

fn populate(db: &GrafeoDB, tag: &str) {
    let a = db
        .create_node_with_props(&["Person"], [("name", Value::from(format!("{tag}-a")))])
        .expect("node a");
    let b = db
        .create_node_with_props(&["Person"], [("name", Value::from(format!("{tag}-b")))])
        .expect("node b");
    let _e = db.create_edge(a, b, "KNOWS");
}

/// 1. Genesis publication exposes WAL boundary + overlay epoch, agrees with
///    the on-disk manifest, and the boundary is replayable.
#[test]
fn genesis_publication_records_replayable_wal_boundary() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "genesis");

    let publication = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-genesis"))
        .expect("build_and_publish_generation")
        .publication;

    // Extended descriptor carries the WAL boundary and overlay epoch.
    assert_eq!(publication.generation_id, "g-genesis");
    assert_eq!(publication.publication_sequence, 1);
    assert_eq!(publication.parent_publication_sequence, 0);
    assert!(publication.parent_generation_id.is_empty());
    let boundary = publication.wal_boundary;
    assert_eq!(boundary.overlay_epoch, publication.overlay_epoch);

    // 2. Descriptor agrees with the on-disk dual-slot manifest.
    let state = read_manifest_state(&gen_root).expect("read manifest state");
    assert_eq!(state.selected.generation_id, "g-genesis");
    assert_eq!(state.selected.publication_sequence, 1);
    assert_eq!(state.selected.wal_boundary, boundary);
    assert_eq!(state.selected.overlay_epoch, publication.overlay_epoch);
    assert!(
        state.previous.is_none(),
        "genesis has no previous generation"
    );

    // 3. The recorded WAL boundary is replayable against the real WAL files.
    let wal_dir = gen_root.join("wal");
    validate_replayable(&wal_dir, &boundary.to_cursor()).expect("boundary replayable");
}

/// 4. A second publication advances the sequence, exposes the first
///    generation as `previous`, and both immutable generations survive.
#[test]
fn second_publication_exposes_previous_and_retains_generations() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "first");
    let first = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-first"))
        .expect("first publish")
        .publication;

    // Mutate, then publish a second generation.
    populate(&db, "second");
    let second = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-second"))
        .expect("second publish")
        .publication;

    assert_eq!(second.publication_sequence, 2);
    assert_eq!(second.parent_publication_sequence, 1);
    assert_eq!(second.parent_generation_id, "g-first");

    // Manifest state: second selected, first retained as previous.
    let state = read_manifest_state(&gen_root).expect("read manifest state");
    assert_eq!(state.selected.generation_id, "g-second");
    assert_eq!(state.selected.publication_sequence, 2);
    let previous = state.previous.expect("previous generation retained");
    assert_eq!(previous.generation_id, "g-first");
    assert_eq!(previous.publication_sequence, 1);

    // Both immutable generation files survive (no early replacement).
    assert!(gen_root.join(&first.generation_path).is_file());
    assert!(gen_root.join(&second.generation_path).is_file());

    // WAL boundary advanced monotonically (log sequence non-decreasing).
    assert!(second.wal_boundary.log_sequence >= first.wal_boundary.log_sequence);

    // The selection transition is consistent with W0 recovery.
    let lock = RootLock::try_acquire(&gen_root).expect("re-lock");
    let selected = recover(&lock).expect("recover selects latest");
    assert_eq!(selected.slot.generation_id, "g-second");
    assert_eq!(selected.slot.publication_sequence, 2);
}

/// 5. Publication phase ordering: the commit point (`ManifestSync`) is the
///    boundary; every phase before it is pre-commit, every phase at/after it
///    may touch the WAL. This locks the observable ordering contract.
#[test]
fn publication_phase_commit_point_is_manifest_sync() {
    // Exactly 11 ordered phases.
    assert_eq!(PublicationPhase::ALL.len(), 11);

    // Phases are strictly ordered.
    for window in PublicationPhase::ALL.windows(2) {
        assert!(
            window[0] < window[1],
            "{:?} must precede {:?}",
            window[0],
            window[1]
        );
    }

    // The commit point: nothing before ManifestSync is post-commit; nothing
    // from ManifestSync onward is pre-commit.
    for phase in PublicationPhase::ALL {
        let post_commit = phase.is_post_commit();
        if phase == PublicationPhase::ManifestSync
            || phase == PublicationPhase::WalTruncate
            || phase == PublicationPhase::Cleanup
        {
            assert!(post_commit, "{phase:?} must be post-commit");
        } else {
            assert!(!post_commit, "{phase:?} must be pre-commit");
        }
    }

    // WAL truncation and cleanup are strictly after the manifest sync.
    assert!(PublicationPhase::ManifestSync < PublicationPhase::WalTruncate);
    assert!(PublicationPhase::ManifestSync < PublicationPhase::Cleanup);
}

/// 6. Publishing into a standalone `.grafeo` file path fails closed through
///    the typed error surface (not a panic, not a silent success).
#[test]
fn publish_into_standalone_file_fails_closed() {
    let dir = TempDir::new().unwrap();
    let standalone = dir.path().join("legacy.grafeo");

    let db = GrafeoDB::new_in_memory();
    populate(&db, "legacy");
    db.save(&standalone).expect("save standalone");
    assert!(standalone.is_file());

    let err = db
        .build_and_publish_generation(generation_build_request(&standalone, "g-refused"))
        .expect_err("standalone file target must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("standalone .grafeo file"),
        "error must name the standalone-file refusal, got: {msg}"
    );
}

/// 7. WAL retention honors the recorded boundary: the floor's own log file is
///    never deleted by post-commit truncation (`truncate_before` deletes only
///    files strictly older than the floor's log sequence). This proves the
///    boundary log survives publication and stays replayable — the observable
///    engine-side signal that WAL advancement follows the durable selection.
#[test]
fn wal_boundary_file_survives_publication() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let _first = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-one"))
        .expect("first publish")
        .publication;

    populate(&db, "two");
    let second = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-two"))
        .expect("second publish")
        .publication;

    // The boundary log file of the selected generation must exist and be
    // replayable; truncation never removes the floor's own file.
    let wal_dir = gen_root.join("wal");
    let seqs = wal_sequences(&wal_dir).expect("list wal sequences");
    assert!(
        seqs.contains(&second.wal_boundary.log_sequence),
        "selected boundary log {} must be retained, have {seqs:?}",
        second.wal_boundary.log_sequence
    );
    validate_replayable(&wal_dir, &second.wal_boundary.to_cursor())
        .expect("selected boundary replayable after second publication");
}

/// 8. The phase-tagged publication error has a real producer: a W0
///    `PublicationError` is tagged with its conservative failing phase and
///    preserves the source identity (review repair — closes the "no producer"
///    gap for req 5's "every publication phase/error").
#[test]
fn publication_error_is_phase_tagged() {
    use grafeo_engine::PublicationPhaseError;
    use grafeo_storage::generation::publication::PublicationError;

    // A pre-commit validation failure tags ReopenValidate (not post-commit).
    let err = PublicationPhaseError::from_publication(PublicationError::ValidationFailed(
        "mmap empty".into(),
    ));
    assert_eq!(err.phase, PublicationPhase::ReopenValidate);
    assert!(!err.is_post_commit());
    // Source identity preserved through the tag.
    assert!(matches!(err.source, PublicationError::ValidationFailed(_)));
    // Display surfaces the phase name + source.
    let msg = err.to_string();
    assert!(msg.contains("reopen_validate"), "phase named: {msg}");
    assert!(msg.contains("mmap empty"), "source preserved: {msg}");

    // A target-exists failure tags RenameImmutable (pre-commit).
    let err = PublicationPhaseError::from_publication(PublicationError::TargetExists("g".into()));
    assert_eq!(err.phase, PublicationPhase::RenameImmutable);
    assert!(!err.is_post_commit());

    // The phase-tagged error converts into the engine error surface.
    let engine_err: grafeo_common::utils::error::Error = err.into();
    assert!(engine_err.to_string().contains("rename_immutable"));
}
