//! Deterministic power-loss crash matrix (W0 §14 / dispatch Module 7).
//!
//! Every test runs the REAL `publish_generation` against
//! [`DeterministicFileOps`] (real temp files + sync tracking), injects power
//! loss at one named boundary, then runs real `recover()` against the
//! surviving filesystem and asserts the exact expected selection.
//!
//! Expected recovery per injection point:
//!
//! | Point | Injection | Expected |
//! |-------|-----------|----------|
//! | 1 | after unpublished dir creation | previous slot |
//! | 2 | during section streaming (after N bytes) | previous slot |
//! | 3 | before generation file sync | previous slot |
//! | 4 | after generation file sync | previous slot |
//! | 5 | after reopen validation, before rename | previous slot |
//! | 6 | after rename, before generations/ fsync | previous slot |
//! | 7 | after generations/ fsync, before slot write | previous slot |
//! | 8 | during torn slot write | previous slot |
//! | 9 | after complete slot write, before manifest sync | previous slot |
//! | 10 | after manifest sync, before WAL cleanup | NEW slot |
//! | 11 | during WAL cleanup (some files deleted) | NEW slot |
//! | 12 | after WAL cleanup, before overlay retirement | NEW slot |
//! | 13 | during snapshot copy | source unaffected |
//! | 14 | after snapshot sync, before snapshot rename | no snapshot at destination |
//! | 15 | after snapshot rename, before dest dir fsync | snapshot not durable (model) |

use std::cell::RefCell;
use std::io::Write as _;
use std::path::Path;

use crate::file::generation_writer::{ExactSectionSource, GenerationFileOps, OsGenerationFileOps};
use crate::generation::faults::DeterministicFileOps;
use crate::generation::lock::RootLock;
use crate::generation::publication::{PublicationInput, PublicationResult, publish_generation};
use crate::generation::recovery::{SelectedGeneration, recover};
use crate::generation::snapshot::publish_snapshot;
use crate::generation::tests::support::{RootFixture, fixture_section, new_root};

/// GenerationFileOps for a RefCell-wrapped deterministic ops: publication
/// borrows immutably; the fault hook borrows mutably to inject power loss.
impl GenerationFileOps for RefCell<DeterministicFileOps> {
    fn create_new(&self, path: &Path) -> grafeo_common::utils::error::Result<std::fs::File> {
        self.borrow().create_new(path)
    }
    fn sync_all(&self, file: &std::fs::File) -> grafeo_common::utils::error::Result<()> {
        self.borrow().sync_all(file)
    }
    fn sync_dir(&self, path: &Path) -> grafeo_common::utils::error::Result<()> {
        self.borrow().sync_dir(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> grafeo_common::utils::error::Result<()> {
        self.borrow().rename(from, to)
    }
    fn remove(&self, path: &Path) -> grafeo_common::utils::error::Result<()> {
        self.borrow().remove(path)
    }
    fn path_exists(&self, path: &Path) -> bool {
        self.borrow().path_exists(path)
    }
    fn open_existing(&self, path: &Path) -> grafeo_common::utils::error::Result<std::fs::File> {
        self.borrow().open_existing(path)
    }
    fn read_to_end(&self, path: &Path) -> grafeo_common::utils::error::Result<Vec<u8>> {
        self.borrow().read_to_end(path)
    }
    fn create_dir_all(&self, path: &Path) -> grafeo_common::utils::error::Result<()> {
        self.borrow().create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> grafeo_common::utils::error::Result<Vec<String>> {
        self.borrow().read_dir(path)
    }
    fn sha256(&self, path: &Path) -> grafeo_common::utils::error::Result<[u8; 32]> {
        self.borrow().sha256(path)
    }
    fn file_len(&self, path: &Path) -> grafeo_common::utils::error::Result<u64> {
        self.borrow().file_len(path)
    }
    fn sync_path(&self, path: &Path) -> grafeo_common::utils::error::Result<()> {
        self.borrow().sync_path(path)
    }
    fn copy_bounded(
        &self,
        src: &Path,
        dst: &Path,
        buf_size: usize,
    ) -> grafeo_common::utils::error::Result<u64> {
        self.borrow().copy_bounded(src, dst, buf_size)
    }
}

/// Publish one generation with real durable file ops.
fn publish_real(lock: &RootLock, fixture: &RootFixture, id: &str) -> PublicationResult {
    let (section, header) = fixture_section();
    let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![section];
    let input = PublicationInput {
        header,
        sections: &mut sections,
        generation_id: id.to_string(),
        parent_generation_id: None,
        parent_publication_sequence: None,
    };
    publish_generation(lock, input, &fixture.wal, &OsGenerationFileOps, None).expect("publish")
}

/// Run a deterministic publication (after one durable prior generation)
/// with power loss injected at `point`, then recover on the survivors.
///
/// Injection simulates a power cut: the publisher STOPS at the boundary
/// (panic, caught here) and only the durable state survives.
fn crash_at(point: &str) -> SelectedGeneration {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).expect("lock");
    publish_real(&lock, &fixture, "g-prev");
    drop(lock);

    let lock = RootLock::try_acquire(fixture.root()).expect("lock");
    let dops = RefCell::new(DeterministicFileOps::new(fixture.root().to_path_buf()));
    let hook = |name: &str| {
        if name == point {
            dops.borrow_mut().inject_power_loss();
            // Power cut: the publisher dies here.
            panic!("power loss injected at {name}");
        }
    };

    let (section, header) = fixture_section();
    let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![section];
    let input = PublicationInput {
        header,
        sections: &mut sections,
        generation_id: "g-new".to_string(),
        parent_generation_id: None,
        parent_publication_sequence: None,
    };
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = publish_generation(&lock, input, &fixture.wal, &dops, Some(&hook));
    }));
    recover(&lock).expect("recovery must succeed after injection")
}

/// Section source that writes a small valid prefix, fires a fault, then
/// streams the real section. Used for injection point 2 (mid-stream).
/// The writer fails closed on the resulting length mismatch; the fault has
/// already deleted the volatile partial file.
struct MidStreamFaultSource {
    inner: Box<dyn ExactSectionSource>,
    trigger: Option<Box<dyn FnOnce()>>,
    fired: bool,
}

impl ExactSectionSource for MidStreamFaultSource {
    fn section_type(&self) -> grafeo_common::storage::section::SectionType {
        self.inner.section_type()
    }
    fn directory_version(&self) -> u8 {
        self.inner.directory_version()
    }
    fn exact_len(&self) -> u64 {
        self.inner.exact_len()
    }
    fn copy_to(
        &mut self,
        sink: &mut dyn std::io::Write,
    ) -> grafeo_common::utils::error::Result<()> {
        if !self.fired {
            self.fired = true;
            sink.write_all(&[0u8; 512])
                .map_err(grafeo_common::utils::error::Error::Io)?;
            if let Some(t) = self.trigger.take() {
                t();
            }
        }
        self.inner.copy_to(sink)
    }
}

// ── Publication crash points 1-12 ────────────────────────────────────

#[test]
fn fault_point_01_after_unpublished_dir() {
    let selected = crash_at("after_unpublished_dir");
    assert_eq!(selected.slot.publication_sequence, 1);
    assert_eq!(selected.slot.generation_id, "g-prev");
}

#[test]
fn fault_point_02_during_section_streaming() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    publish_real(&lock, &fixture, "g-prev");
    drop(lock);

    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    let dops = std::rc::Rc::new(RefCell::new(DeterministicFileOps::new(
        fixture.root().to_path_buf(),
    )));
    let (section, header) = fixture_section();
    let trigger_dops = std::rc::Rc::clone(&dops);
    let mid = MidStreamFaultSource {
        inner: section,
        trigger: Some(Box::new(move || {
            trigger_dops.borrow_mut().inject_power_loss();
        })),
        fired: false,
    };
    let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![Box::new(mid)];
    let input = PublicationInput {
        header,
        sections: &mut sections,
        generation_id: "g-new".to_string(),
        parent_generation_id: None,
        parent_publication_sequence: None,
    };
    let _ = publish_generation(&lock, input, &fixture.wal, dops.as_ref(), Some(&|_name| {}));
    let selected = recover(&lock).expect("recovery must succeed");
    assert_eq!(selected.slot.publication_sequence, 1);
    assert_eq!(selected.slot.generation_id, "g-prev");
}

#[test]
fn fault_point_03_before_gen_sync() {
    let selected = crash_at("before_gen_sync");
    assert_eq!(selected.slot.publication_sequence, 1);
}

#[test]
fn fault_point_04_after_gen_sync() {
    let selected = crash_at("after_gen_sync");
    assert_eq!(selected.slot.publication_sequence, 1);
}

#[test]
fn fault_point_05_after_reopen() {
    let selected = crash_at("after_reopen");
    assert_eq!(selected.slot.publication_sequence, 1);
}

#[test]
fn fault_point_06_after_rename() {
    let selected = crash_at("after_rename");
    assert_eq!(selected.slot.publication_sequence, 1);
}

#[test]
fn fault_point_07_after_gen_dir_sync() {
    let selected = crash_at("after_gen_dir_sync");
    assert_eq!(selected.slot.publication_sequence, 1);
}

#[test]
fn fault_point_08_during_slot_write() {
    let selected = crash_at("during_slot_write");
    assert_eq!(selected.slot.publication_sequence, 1);
}

#[test]
fn fault_point_09_after_slot_write() {
    let selected = crash_at("after_slot_write");
    assert_eq!(selected.slot.publication_sequence, 1);
}

#[test]
fn fault_point_10_after_manifest_sync() {
    let selected = crash_at("after_manifest_sync");
    assert_eq!(selected.slot.publication_sequence, 2, "commit point passed");
    assert_eq!(selected.slot.generation_id, "g-new");
}

#[test]
fn fault_point_11_during_wal_cleanup() {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    publish_real(&lock, &fixture, "g-prev");
    drop(lock);

    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    let dops = std::rc::Rc::new(RefCell::new(DeterministicFileOps::new(
        fixture.root().to_path_buf(),
    )));
    let wal_dir = fixture.wal_dir();
    let hook_dops = std::rc::Rc::clone(&dops);
    let hook = move |name: &str| {
        if name == "during_wal_cleanup" {
            // Simulate partial cleanup: delete the OLDEST wal file.
            let mut files: Vec<_> = std::fs::read_dir(&wal_dir)
                .unwrap()
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "log"))
                .collect();
            files.sort();
            if let Some(oldest) = files.first() {
                let _ = std::fs::remove_file(oldest);
            }
            hook_dops.borrow_mut().inject_power_loss();
        }
    };
    let (section, header) = fixture_section();
    let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![section];
    let input = PublicationInput {
        header,
        sections: &mut sections,
        generation_id: "g-new".to_string(),
        parent_generation_id: None,
        parent_publication_sequence: None,
    };
    let _ = publish_generation(&lock, input, &fixture.wal, dops.as_ref(), Some(&hook));
    let selected = recover(&lock).expect("recovery must succeed");
    assert_eq!(
        selected.slot.publication_sequence, 2,
        "partial WAL cleanup must not invalidate the new slot"
    );
}

#[test]
fn fault_point_12_after_wal_cleanup() {
    let selected = crash_at("after_wal_cleanup");
    assert_eq!(selected.slot.publication_sequence, 2);
}

// ── Snapshot crash points 13-15 ──────────────────────────────────────

/// Setup: two durable generations, recovery selects seq 2; returns the
/// destination tempdir + selected generation + held lock.
struct SnapshotCrashFixture {
    _fixture: RootFixture,
    dest: tempfile::TempDir,
    selected: SelectedGeneration,
    lock: RootLock,
}

fn snapshot_crash_setup() -> SnapshotCrashFixture {
    let fixture = new_root();
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    publish_real(&lock, &fixture, "g-one");
    publish_real(&lock, &fixture, "g-two");
    drop(lock);
    let lock = RootLock::try_acquire(fixture.root()).unwrap();
    let selected = recover(&lock).expect("recover");
    let dest = tempfile::TempDir::new().unwrap();
    SnapshotCrashFixture {
        _fixture: fixture,
        dest,
        selected,
        lock,
    }
}

#[test]
fn fault_point_13_during_snapshot_copy() {
    let f = snapshot_crash_setup();
    let dops = RefCell::new(DeterministicFileOps::new(f.dest.path().to_path_buf()));
    let hook = |name: &str| {
        if name == "before_snapshot_copy" {
            dops.borrow_mut().inject_power_loss();
            panic!("power loss injected at {name}");
        }
    };
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = publish_snapshot(
            &f.lock,
            &f.selected,
            f.dest.path(),
            "snap.grafeo",
            &dops,
            Some(&hook),
        );
    }));

    // Source generation is unaffected; the snapshot does not exist.
    assert!(f.selected.generation_abs_path.exists());
    assert!(!f.dest.path().join("snap.grafeo").exists());
}

#[test]
fn fault_point_14_after_snapshot_sync() {
    let f = snapshot_crash_setup();
    let dops = RefCell::new(DeterministicFileOps::new(f.dest.path().to_path_buf()));
    let hook = |name: &str| {
        if name == "after_snapshot_sync" {
            dops.borrow_mut().inject_power_loss();
            panic!("power loss injected at {name}");
        }
    };
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = publish_snapshot(
            &f.lock,
            &f.selected,
            f.dest.path(),
            "snap.grafeo",
            &dops,
            Some(&hook),
        );
    }));

    // No final snapshot: the rename never became durable.
    assert!(!f.dest.path().join("snap.grafeo").exists());
}

#[test]
fn fault_point_15_after_snapshot_rename() {
    let f = snapshot_crash_setup();
    let dops = RefCell::new(DeterministicFileOps::new(f.dest.path().to_path_buf()));
    let hook = |name: &str| {
        if name == "after_snapshot_rename" {
            dops.borrow_mut().inject_power_loss();
            panic!("power loss injected at {name}");
        }
    };
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = publish_snapshot(
            &f.lock,
            &f.selected,
            f.dest.path(),
            "snap.grafeo",
            &dops,
            Some(&hook),
        );
    }));

    // Deterministic model: the rename was not durable (dest dir unsynced),
    // so the final name is absent. The matrix allows either outcome; the
    // model pins it to "not durable".
    assert!(!f.dest.path().join("snap.grafeo").exists());

    // Control: without power loss the snapshot IS published.
    let f2 = snapshot_crash_setup();
    publish_snapshot(
        &f2.lock,
        &f2.selected,
        f2.dest.path(),
        "snap.grafeo",
        &OsGenerationFileOps,
        None,
    )
    .expect("control publish");
    assert!(f2.dest.path().join("snap.grafeo").exists());
}

// ── Sync distinction: file sync ≠ dir sync ───────────────────────────

#[test]
fn fault_file_sync_not_dir_sync() {
    // A file synced but its rename's parent dir NOT synced: the rename is
    // lost on power loss (the file survives only at its old path).
    let dir = tempfile::TempDir::new().unwrap();
    let dops = RefCell::new(DeterministicFileOps::new(dir.path().to_path_buf()));
    let root = dir.path();
    std::fs::create_dir_all(root.join("gens")).unwrap();
    let _ = dops.borrow().sync_dir(&root.join("gens"));

    let src = root.join("gens").join("a.partial");
    let dst = root.join("gens").join("a.grafeo");
    {
        let mut f = dops.borrow().create_new(&src).unwrap();
        f.write_all(b"payload").unwrap();
        dops.borrow().sync_all(&f).unwrap();
    }
    dops.borrow().rename(&src, &dst).unwrap();

    dops.borrow_mut().inject_power_loss();

    assert!(!dst.exists(), "unsynced rename must be lost");
    assert!(src.exists(), "synced file content survives at the old path");
}

#[test]
fn fault_dir_sync_not_file_sync() {
    // A directory synced but the file inside never synced: the file bytes
    // are lost even though the directory entry is durable.
    let dir = tempfile::TempDir::new().unwrap();
    let dops = RefCell::new(DeterministicFileOps::new(dir.path().to_path_buf()));
    let root = dir.path();
    std::fs::create_dir_all(root.join("gens")).unwrap();
    let _ = dops.borrow().sync_dir(&root.join("gens"));

    let file = root.join("gens").join("b.partial");
    {
        let mut f = dops.borrow().create_new(&file).unwrap();
        f.write_all(b"payload").unwrap();
    }

    dops.borrow_mut().inject_power_loss();

    assert!(!file.exists(), "unsynced file bytes must be lost");
    assert!(root.join("gens").exists(), "synced dir entry survives");
}
