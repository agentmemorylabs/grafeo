//! Recovery tests: selection, fallback, typed errors, mtime immunity,
//! orphan non-promotion.

use crate::file::generation_writer::OsGenerationFileOps;
use crate::generation::lock::RootLock;
use crate::generation::publication::{PublicationInput, publish_generation};
use crate::generation::recovery::{RecoveryError, recover};
use crate::generation::tests::support::{fixture_section, new_root};

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
        pre_cut_cursor: None,
    };
    publish_generation(lock, input, &fixture.wal, &OsGenerationFileOps, None).expect("publish");
}

#[test]
fn recovery_genesis() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    publish_once(&lock, &fixture, "g-one");
    drop(lock);

    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    let selected = recover(&lock).expect("recovery must succeed");
    assert_eq!(selected.slot.publication_sequence, 1);
    assert_eq!(selected.slot.generation_id, "g-one");
    assert_eq!(selected.slot_index, 0);
    assert!(selected.generation_abs_path.exists());
}

#[test]
fn recovery_newest_valid_selected() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    publish_once(&lock, &fixture, "g-one");
    publish_once(&lock, &fixture, "g-two");
    drop(lock);

    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    let selected = recover(&lock).expect("recovery must succeed");
    assert_eq!(selected.slot.publication_sequence, 2);
    assert_eq!(selected.slot.generation_id, "g-two");
}

#[test]
fn recovery_fallback_to_previous() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    publish_once(&lock, &fixture, "g-one");
    publish_once(&lock, &fixture, "g-two");
    drop(lock);

    // Corrupt the newest generation file (truncate it).
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    let (_, newest) =
        crate::generation::manifest::read_manifest(&fixture.root().join("manifest.bin")).unwrap();
    let newest_path = fixture.root().join(&newest.generation_path);
    std::fs::write(&newest_path, b"torn").unwrap();

    let selected = recover(&lock).expect("fallback must succeed");
    assert_eq!(
        selected.slot.publication_sequence, 1,
        "must fall back to seq 1"
    );
    assert_eq!(selected.slot.generation_id, "g-one");
}

#[test]
fn recovery_both_invalid_typed_error() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    publish_once(&lock, &fixture, "g-one");
    publish_once(&lock, &fixture, "g-two");
    drop(lock);

    // Corrupt BOTH generation files.
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    let (_, newest) =
        crate::generation::manifest::read_manifest(&fixture.root().join("manifest.bin")).unwrap();
    std::fs::write(fixture.root().join(&newest.generation_path), b"torn").unwrap();
    let [slot_a, slot_b] =
        crate::generation::manifest::read_both_slots(&fixture.root().join("manifest.bin")).unwrap();
    let other_slot = match (slot_a, slot_b) {
        (Ok(s), _) => s,
        (_, Ok(s)) => s,
        _ => unreachable!(),
    };
    std::fs::write(fixture.root().join(&other_slot.generation_path), b"torn").unwrap();

    match recover(&lock) {
        Err(RecoveryError::NoValidGeneration(causes)) => {
            assert!(causes.contains("generation"), "causes: {causes}");
        }
        other => panic!("expected NoValidGeneration, got {other:?}"),
    }
}

#[test]
fn recovery_never_selects_by_mtime() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    publish_once(&lock, &fixture, "g-one");
    publish_once(&lock, &fixture, "g-two");
    drop(lock);

    // Give the OLD slot's generation file a future mtime.
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    let [slot0, slot1] =
        crate::generation::manifest::read_both_slots(&fixture.root().join("manifest.bin")).unwrap();
    let old = match (slot0, slot1) {
        (Ok(a), Ok(b)) => {
            if a.publication_sequence < b.publication_sequence {
                a
            } else {
                b
            }
        }
        _ => unreachable!(),
    };
    let old_path = fixture.root().join(&old.generation_path);
    // reason: from_days is unstable on this toolchain; allow the suboptimal-unit lint
    #[allow(clippy::duration_suboptimal_units)]
    let future = std::time::SystemTime::now() + std::time::Duration::from_secs(31_536_000);
    let _ = filetime_set(&old_path, future);

    let selected = recover(&lock).expect("recovery must succeed");
    assert_eq!(
        selected.slot.publication_sequence, 2,
        "sequence authority must beat mtime"
    );
}

#[test]
fn recovery_never_promotes_orphan() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    publish_once(&lock, &fixture, "g-one");
    drop(lock);

    // Drop a VALID container into generations/ that no slot references.
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    let (_, slot) =
        crate::generation::manifest::read_manifest(&fixture.root().join("manifest.bin")).unwrap();
    let orphan_path = fixture
        .root()
        .join("generations")
        .join("g-00000000000000000099-orphan.grafeo");
    std::fs::copy(fixture.root().join(&slot.generation_path), &orphan_path).unwrap();

    let selected = recover(&lock).expect("recovery must succeed");
    assert_eq!(selected.slot.publication_sequence, 1);
    assert_ne!(selected.generation_abs_path, orphan_path);
    assert_eq!(selected.slot.generation_id, "g-one");
}

/// Set a file's mtime without extra deps (utimensat via libc is not
/// available here; use the touch-style approach with std only).
fn filetime_set(path: &std::path::Path, time: std::time::SystemTime) -> std::io::Result<()> {
    let file = std::fs::File::options().write(true).open(path)?;
    let dur = time
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| std::io::Error::other("time before epoch"))?;
    #[allow(unsafe_code)]
    {
        let ts = libc::timespec {
            // reason: timespec.tv_sec is time_t (i64 on Linux); the test
            // uses a near-term timestamp, so the cast cannot wrap
            #[allow(clippy::cast_possible_wrap)]
            tv_sec: dur.as_secs() as libc::time_t,
            tv_nsec: dur.subsec_nanos() as libc::c_long,
        };
        // SAFETY: `file` is a valid open descriptor; utimensat with NULL path
        // operates on the fd's file and never closes it.
        let rc = unsafe { libc::futimens(file.as_raw_fd(), [ts, ts].as_ptr()) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[allow(unused_imports)]
use std::os::fd::AsRawFd;
