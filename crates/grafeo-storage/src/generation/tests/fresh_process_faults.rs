//! Fresh-process fail-stop proof (W0 §14).
//!
//! A child process runs the REAL `publish_generation` and hard-aborts at a
//! named boundary (`std::process::abort` — not an injected error). The
//! parent then proves:
//!
//! - the root lock is released by the child's death (handle-close);
//! - production `recover()` selects the unique highest fully
//!   validating/replayable slot represented by the bytes that actually
//!   survived;
//! - a torn slot is never selected.
//!
//! Process abort is NOT a power cut: page cache survives, so an unsynced
//! write may later become durable. Recovery therefore may observe old or
//! new at a pre-sync boundary — but must be deterministic from the
//! surviving bytes and must never select a torn slot.

use std::process::Command;

use crate::file::generation_writer::{ExactSectionSource, OsGenerationFileOps};
use crate::generation::lock::RootLock;
use crate::generation::publication::{PublicationInput, publish_generation};
use crate::generation::recovery::recover;
use crate::generation::tests::support::{fixture_section, new_root};

const HELPER_ENV: &str = "GRAFEOPUB_HELPER";

fn child_main() {
    let root = std::env::var("GRAFEOPUB_ROOT").expect("child root env");
    let point = std::env::var("GRAFEOPUB_POINT").expect("child point env");
    let lock = RootLock::try_acquire(std::path::Path::new(&root)).expect("child lock");
    let wal_dir = std::path::Path::new(&root).join("wal");
    let wal = crate::wal::WalManager::open(&wal_dir).expect("child wal");

    let (section, header) = fixture_section();
    let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![section];
    let input = PublicationInput {
        header,
        sections: &mut sections,
        generation_id: "g-crashed".to_string(),
        parent_generation_id: None,
        parent_publication_sequence: None,
    };
    let hook = move |name: &str| {
        if name == point {
            // Hold the lock until the abort fires.
            std::process::abort();
        }
    };
    let _ = publish_generation(&lock, input, &wal, &OsGenerationFileOps, Some(&hook));
    std::process::exit(0);
}

/// Spawn a child that publishes (after one durable prior generation) and
/// aborts at `point`. Returns the parent's recovery selection.
fn crash_child_at(point: &str) -> crate::generation::recovery::SelectedGeneration {
    // Prior durable generation (seq 1) published by the parent.
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).expect("lock");
    let (section, header) = fixture_section();
    let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![section];
    let input = PublicationInput {
        header,
        sections: &mut sections,
        generation_id: "g-prev".to_string(),
        parent_generation_id: None,
        parent_publication_sequence: None,
    };
    publish_generation(&lock, input, &fixture.wal, &OsGenerationFileOps, None).expect("publish");
    drop(lock);

    let status = Command::new(std::env::current_exe().expect("current exe"))
        .env(HELPER_ENV, "1")
        .env("GRAFEOPUB_ROOT", fixture.root())
        .env("GRAFEOPUB_POINT", point)
        .status()
        .expect("spawn child");
    assert!(!status.success(), "child must abort at {point}");

    // Lock must be released by the child's death.
    let lock = RootLock::try_acquire(fixture.root()).expect("lock released after child abort");
    let selected = recover(&lock).expect("recovery must succeed on surviving bytes");
    drop(lock);
    let _ = fixture;
    selected
}

#[test]
fn fp_abort_after_streaming_selects_previous() {
    if std::env::var(HELPER_ENV).is_ok() {
        child_main();
        return;
    }
    if crate::generation::tests::support::in_any_child() {
        return;
    }
    let selected = crash_child_at("after_streaming");
    assert_eq!(
        selected.slot.publication_sequence, 1,
        "pre-rename abort cannot publish a new slot"
    );
}

#[test]
fn fp_abort_after_rename_never_selects_torn() {
    if std::env::var(HELPER_ENV).is_ok() {
        child_main();
        return;
    }
    if crate::generation::tests::support::in_any_child() {
        return;
    }
    let selected = crash_child_at("after_rename");
    // The manifest still points at seq 1 (rename happens before the slot
    // write), so recovery must select the previous generation.
    assert_eq!(selected.slot.publication_sequence, 1);
}

#[test]
fn fp_abort_during_slot_write_never_selects_torn() {
    if std::env::var(HELPER_ENV).is_ok() {
        child_main();
        return;
    }
    if crate::generation::tests::support::in_any_child() {
        return;
    }
    let selected = crash_child_at("during_slot_write");
    let seq = selected.slot.publication_sequence;
    assert!(
        seq == 1 || seq == 2,
        "abort during slot write may surface old or fully-written new slot, got {seq}"
    );
}

#[test]
fn fp_abort_after_manifest_sync_selects_new() {
    if std::env::var(HELPER_ENV).is_ok() {
        child_main();
        return;
    }
    if crate::generation::tests::support::in_any_child() {
        return;
    }
    let selected = crash_child_at("after_manifest_sync");
    assert_eq!(
        selected.slot.publication_sequence, 2,
        "commit point passed: the new slot must be selected"
    );
    assert_eq!(selected.slot.generation_id, "g-crashed");
}
