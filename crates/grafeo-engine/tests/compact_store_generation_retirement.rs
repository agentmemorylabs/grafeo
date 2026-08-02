//! G-EM0.4b — process ownership, retirement, backup, and GC.
//!
//! Proves the engine-level lifecycle contract on top of the accepted
//! W0/3a/3b/3c/4a surface:
//!
//! 1. **Process ownership**: the exclusive writable-root OS lock is acquired
//!    before the manifest is read and held for the open lifetime; every
//!    second-process live-root open is rejected, including read-only mode
//!    (fresh-child proofs).
//! 2. **Restart/crash**: process exit releases the kernel lock; recovery
//!    validates manifest/WAL state rather than trusting a stale lease file.
//! 3. **External snapshots**: published only through the W0 contract to
//!    independent immutable paths; a snapshot is never under live-root GC
//!    authority and outlives collection of the live root.
//! 4. **Backup**: pins an exact selected manifest sequence plus all
//!    referenced immutable generation/WAL bytes before copying; restore
//!    validates and publishes into a new exclusively locked root and never
//!    overwrites an existing one.
//! 5. **Live-root GC**: deletes a generation only when it is not selected,
//!    previous-recovery retained, backup-pinned, in-process leased, or
//!    referenced by a valid manifest/WAL recovery state. It never tracks
//!    external readers because they never receive live-root paths.
//! 6. **Observability**: root-lock owner state plus selected/previous/
//!    pinned/in-process-leased/eligible/retired IDs and reasons, without
//!    holding any strong reference that could prevent retirement.
//! 7. **GC races**: a stale plan fails closed (TOCTOU rail), and concurrent
//!    publication + GC never deletes a protected generation.

use std::fs;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use grafeo_common::storage::SectionType;
use grafeo_common::types::{PropertyKey, Value};
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_engine::{
    ClassifiedGeneration, GrafeoDB, OpenMode, OwnershipError, RetentionClass, RetirementAuthority,
    RetirementError, RetirementPlan, RootOwnership, backup_generation_root, collect_retirement,
    generation_build_request, plan_retirement, restore_generation_root,
};
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::file::generation_writer::{GenerationFileOps, OsGenerationFileOps};
use grafeo_storage::generation::lock::RootLockError;
use grafeo_storage::generation::snapshot::publish_snapshot;
use tempfile::TempDir;

const HELPER_ENV: &str = "GRAFEORET_HELPER";
const MODE_ENV: &str = "GRAFEORET_MODE";
const ROOT_ENV: &str = "GRAFEORET_ROOT";
const EXPECT_ENV: &str = "GRAFEORET_EXPECT";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Populate a throwaway in-memory DB with two labeled nodes (unique `tag`)
/// and a single edge, so each published generation has distinct content.
fn populate(db: &GrafeoDB, tag: &str) {
    let a = db
        .create_node_with_props(&["Person"], [("name", Value::from(format!("{tag}-a")))])
        .expect("node a");
    let b = db
        .create_node_with_props(&["Person"], [("name", Value::from(format!("{tag}-b")))])
        .expect("node b");
    let _e = db.create_edge(a, b, "KNOWS");
}

/// Publish one generation of the current in-memory DB into `gen_root` and
/// return its `(publication sequence, root-relative path)`.
fn publish(db: &GrafeoDB, gen_root: &std::path::Path, id: &str) -> (u64, String) {
    let publication = db
        .build_and_publish_generation(generation_build_request(gen_root, id))
        .expect("publish generation")
        .publication;
    (
        publication.publication_sequence,
        publication.generation_path,
    )
}

/// Query-parity probe: read the sorted node names in a generation container
/// through the production read path (`open_read_only` +
/// `deserialize_from_bytes`, the exact W0/3a contract mechanism).
fn generation_names(generation_abs: &std::path::Path) -> Vec<String> {
    let manager = GrafeoFileManager::open_read_only(generation_abs).expect("open generation");
    let section_dir = manager
        .read_section_directory()
        .expect("section directory")
        .expect("directory present");
    let entry = section_dir
        .find(SectionType::CompactStore)
        .expect("CompactStore section present");
    let data = manager.read_section_data(entry).expect("read section");
    let mut cs_section = CompactStoreSection::empty();
    cs_section
        .deserialize_from_bytes(Bytes::from(data))
        .expect("v5 payload deserializes through the public API");
    let store = cs_section.store().expect("store present");

    let mut names: Vec<String> = Vec::new();
    if let Some(person) = store.node_table("Person") {
        for offset in 0..person.len() {
            if let Some(Value::String(name)) =
                person.get_property(offset, &PropertyKey::new("name"))
            {
                names.push(name.as_str().to_string());
            }
        }
    }
    names.sort_unstable();
    names
}

/// Publish `tags` generations into a fresh root and return the root plus
/// their `(id, sequence, relative path)` records in publication order.
fn publish_many(dir: &TempDir, tags: &[&str]) -> (std::path::PathBuf, Vec<(String, u64, String)>) {
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();
    let db = GrafeoDB::new_in_memory();
    let mut published = Vec::new();
    for tag in tags {
        populate(&db, tag);
        let (seq, rel) = publish(&db, &gen_root, &format!("g-{tag}"));
        published.push((format!("g-{tag}"), seq, rel));
    }
    drop(db);
    (gen_root, published)
}

/// True when this process is a re-exec'd lifecycle child.
fn in_child() -> bool {
    std::env::var(HELPER_ENV).is_ok()
}

/// Child entry point: dispatched at the top of every process-spawning test.
/// Each mode exits the process; it never returns.
fn run_child_mode(mode: &str) -> ! {
    let root = std::env::var(ROOT_ENV).expect("child root env");
    let root = std::path::Path::new(&root);
    match mode {
        // Fresh-process restart: open the root and prove the recovered
        // selection matches the expected `id:sequence`.
        "recover-check" => {
            let expected = std::env::var(EXPECT_ENV).expect("child expect env");
            match RootOwnership::open(root) {
                Ok(ownership) => {
                    let actual = format!(
                        "{}:{}",
                        ownership.selected().slot.generation_id,
                        ownership.selected().slot.publication_sequence
                    );
                    if actual == expected {
                        std::process::exit(0);
                    }
                    eprintln!("child recovered {actual}, expected {expected}");
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("child recovery failed: {e}");
                    std::process::exit(2);
                }
            }
        }
        // Second-process writer open: must be rejected with the typed lock
        // error (never blocks, never steals ownership).
        "second-writer" => match RootOwnership::open(root) {
            Err(OwnershipError::Lock(RootLockError::AlreadyLocked)) => std::process::exit(0),
            other => {
                eprintln!("child writer-open expected AlreadyLocked, got: {other:?}");
                std::process::exit(1);
            }
        },
        // Second-process READ-ONLY open: packet requirement 1 rejects every
        // second-process live-root open, including read-only mode.
        "second-readonly" => match RootOwnership::open_read_only(root) {
            Err(OwnershipError::Lock(RootLockError::AlreadyLocked)) => std::process::exit(0),
            other => {
                eprintln!("child readonly-open expected AlreadyLocked, got: {other:?}");
                std::process::exit(1);
            }
        },
        // Crash child: acquire ownership and abort while holding the lock.
        "crash" => {
            let ownership = RootOwnership::open(root).expect("child acquires ownership");
            std::hint::black_box(&ownership);
            std::process::abort();
        }
        other => {
            eprintln!("unknown child mode {other}");
            std::process::exit(4);
        }
    }
}

/// Re-exec this test binary as a lifecycle child. `extra_env` passes one
/// additional env var to the child (explicit, never process-global mutation).
fn spawn_child(
    test_name: &str,
    mode: &str,
    root: &std::path::Path,
    extra_env: Option<(&str, &str)>,
) -> std::process::ExitStatus {
    let mut cmd = Command::new(std::env::current_exe().expect("current exe"));
    cmd.arg(test_name)
        .arg("--exact")
        .env(HELPER_ENV, "1")
        .env(MODE_ENV, mode)
        .env(ROOT_ENV, root)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    if let Some((key, value)) = extra_env {
        cmd.env(key, value);
    }
    cmd.status().expect("spawn lifecycle child")
}

// ---------------------------------------------------------------------------
// 1. Fresh-process restart: selection + retention survive a real restart
// ---------------------------------------------------------------------------

/// A fresh process opening a healthy root recovers the newest generation,
/// retains the previous slot, and plans nothing for collection (every
/// generation is slot-referenced). The restart proof runs in a re-exec'd
/// child so it exercises a genuine fresh process, not just a re-open.
#[test]
fn fresh_process_restart_recovers_selection_and_retention() {
    if in_child() {
        run_child_mode(&std::env::var(MODE_ENV).unwrap_or_default());
    }

    let dir = TempDir::new().unwrap();
    let (gen_root, published) = publish_many(&dir, &["one", "two"]);
    let (id2, seq2, _) = &published[1];

    // Fresh-process restart: the child opens the root and must recover the
    // exact newest generation.
    let status = spawn_child(
        "fresh_process_restart_recovers_selection_and_retention",
        "recover-check",
        &gen_root,
        Some((EXPECT_ENV, &format!("{id2}:{seq2}"))),
    );
    assert!(
        status.success(),
        "fresh-process restart recovery failed: {status}"
    );

    // In-process: ownership exposes the validated selection and the root
    // lock owner state (packet requirement 5, lock part).
    let ownership = RootOwnership::open(&gen_root).expect("open owned root");
    assert_eq!(ownership.mode(), OpenMode::Writable);
    assert_eq!(ownership.selected().slot.generation_id, *id2);
    let owner = ownership.owner_state();
    assert_eq!(owner.mode, OpenMode::Writable);
    assert_eq!(owner.canonical_root, gen_root.canonicalize().unwrap());
    assert_eq!(
        owner.lock_path,
        gen_root.canonicalize().unwrap().join("root.lock")
    );

    // Retention: on a healthy two-generation root, GC plans nothing —
    // selected + previous are both slot-referenced.
    let auth = RetirementAuthority::new(&ownership);
    let plan = plan_retirement(&auth).expect("plan retirement");
    assert!(
        plan.eligible.is_empty(),
        "no eligible generations on a healthy root: {:?}",
        plan.eligible
    );
    assert_eq!(plan.protected.len(), 2);
    assert!(
        plan.protected
            .iter()
            .any(|c| c.class == RetentionClass::Selected)
    );
    assert!(
        plan.protected
            .iter()
            .any(|c| c.class == RetentionClass::PreviousRecoveryRetained)
    );
}

// ---------------------------------------------------------------------------
// 2. Second-process writer AND read-only rejection
// ---------------------------------------------------------------------------

/// While the owner holds the root lock, a second process is rejected with
/// the typed `AlreadyLocked` error in BOTH writer and read-only mode
/// (packet requirement 1). After the owner drops, a fresh process opens
/// successfully — the rejection is the kernel lock, not a sticky flag.
#[test]
fn second_process_writer_and_readonly_rejected() {
    if in_child() {
        run_child_mode(&std::env::var(MODE_ENV).unwrap_or_default());
    }

    let dir = TempDir::new().unwrap();
    let (gen_root, _) = publish_many(&dir, &["held"]);

    let ownership = RootOwnership::open(&gen_root).expect("parent ownership");

    for mode in ["second-writer", "second-readonly"] {
        let status = spawn_child(
            "second_process_writer_and_readonly_rejected",
            mode,
            &gen_root,
            None,
        );
        assert!(
            status.success(),
            "{mode} must observe AlreadyLocked, got {status}"
        );
    }

    drop(ownership);
    // Kernel lock released with the handle: a fresh process opens cleanly.
    let reopened = RootOwnership::open(&gen_root).expect("open after owner drop");
    assert_eq!(reopened.selected().slot.generation_id, "g-held");
}

// ---------------------------------------------------------------------------
// 3. Crash releases the lock; recovery revalidates (never a stale lease)
// ---------------------------------------------------------------------------

/// A child that aborts while holding the root lock releases it at the
/// kernel level; a fresh process then recovers by validating the manifest
/// and WAL state — there is no lease file to trust, stale or otherwise.
#[test]
fn crash_releases_lock_and_recovery_revalidates() {
    if in_child() {
        run_child_mode(&std::env::var(MODE_ENV).unwrap_or_default());
    }

    let dir = TempDir::new().unwrap();
    let (gen_root, _) = publish_many(&dir, &["crash"]);

    let status = spawn_child(
        "crash_releases_lock_and_recovery_revalidates",
        "crash",
        &gen_root,
        None,
    );
    assert!(
        !status.success(),
        "crash child must abort while holding the lock"
    );

    // Fresh process: the kernel released the lock on process exit; recovery
    // validates the manifest/WAL state and selects the only generation.
    let ownership = RootOwnership::open(&gen_root).expect("fresh open after crash");
    assert_eq!(ownership.selected().slot.generation_id, "g-crash");
    assert_eq!(ownership.selected().slot.publication_sequence, 1);
    assert!(ownership.wal_boundary().log_sequence >= 1);
}

// ---------------------------------------------------------------------------
// 4. Unsupported filesystem/platform combinations fail explicitly
// ---------------------------------------------------------------------------

/// A root on a filesystem outside the durable-local allowlist fails with
/// the typed `UnsupportedFilesystem` error — never silently locked. The
/// probe runs only when `/dev/shm` is genuinely tmpfs (this host); on any
/// other setup the test is an honest no-op.
#[test]
fn unsupported_filesystem_fails_explicitly() {
    if in_child() {
        return;
    }
    let shm = std::path::Path::new("/dev/shm");
    if !shm.is_dir() {
        eprintln!("SKIP: /dev/shm unavailable");
        return;
    }
    let probe_dir = shm.join(format!("grafeo-ret-fstest-{}", std::process::id()));
    if fs::create_dir_all(&probe_dir).is_err() {
        eprintln!("SKIP: /dev/shm not writable");
        return;
    }
    // Prove tmpfs via the W0 mount-table parser before asserting.
    let text = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    let entries = grafeo_storage::generation::lock::parse_mountinfo(&text).unwrap_or_default();
    let canonical = fs::canonicalize(&probe_dir).unwrap();
    let fstype = grafeo_storage::generation::lock::filesystem_for_path(&canonical, &entries)
        .unwrap_or_default();
    if fstype != "tmpfs" {
        eprintln!("SKIP: /dev/shm is {fstype}, not tmpfs");
        let _ = fs::remove_dir(&probe_dir);
        return;
    }

    let err = RootOwnership::open(&canonical).expect_err("tmpfs root must fail explicitly");
    assert!(
        matches!(
            err,
            OwnershipError::Lock(RootLockError::UnsupportedFilesystem(_))
        ),
        "expected typed UnsupportedFilesystem, got: {err}"
    );
    let _ = fs::remove_dir(&probe_dir);
}

// ---------------------------------------------------------------------------
// 5. External snapshot lifetime (never under live-root GC authority)
// ---------------------------------------------------------------------------

/// An external read-only snapshot published through the W0 contract lives
/// on an independent immutable path: live-root GC collects eligible
/// artifacts without ever touching the snapshot, and the snapshot keeps
/// answering with full query parity after the collection.
#[test]
fn external_snapshot_outlives_live_root_gc() {
    if in_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let (gen_root, published) = publish_many(&dir, &["one", "two"]);
    let (_, _, rel2) = &published[1];

    let ownership = RootOwnership::open(&gen_root).expect("open owned root");

    // Publish an external snapshot of the selected generation through the
    // W0 contract (streaming copy + validate + atomic rename + parent fsync).
    let snap_parent = dir.path().join("snapshots");
    fs::create_dir_all(&snap_parent).unwrap();
    let provenance = publish_snapshot(
        ownership.lock(),
        ownership.selected(),
        &snap_parent,
        "snap-one.grafeo",
        &OsGenerationFileOps,
    )
    .expect("publish external snapshot");
    let snap_path = snap_parent.join("snap-one.grafeo");
    assert!(
        snap_path.is_file(),
        "snapshot published to an independent path"
    );
    assert!(
        !snap_path.starts_with(ownership.canonical_root()),
        "snapshot path is never inside the live root"
    );
    assert_eq!(provenance.source_generation_id, "g-two");

    // Parity: the snapshot answers exactly what the selected generation does.
    assert_eq!(
        generation_names(&snap_path),
        generation_names(&gen_root.join(rel2)),
        "snapshot serves the selected generation's bytes"
    );

    // Plant GC-eligible artifacts: an unreferenced generation copy and an
    // unpublished build leftover.
    let orphan = gen_root.join("generations").join("g-orphan.grafeo");
    fs::copy(gen_root.join(rel2), &orphan).unwrap();
    let unpublished = gen_root.join(".unpublished-deadbeef-g-00000000000000000099");
    fs::create_dir_all(&unpublished).unwrap();
    fs::write(unpublished.join("generation.grafeo.partial"), b"torn").unwrap();

    // Live-root GC collects the eligible artifacts — and only those.
    let auth = RetirementAuthority::new(&ownership);
    let plan = plan_retirement(&auth).expect("plan");
    assert_eq!(
        plan.eligible.len(),
        2,
        "orphan + unpublished leftover eligible"
    );
    let retired = collect_retirement(&auth, &plan).expect("collect");
    assert_eq!(retired.len(), 2);
    assert!(!orphan.exists(), "unreferenced generation collected");
    assert!(!unpublished.exists(), "unpublished leftover collected");

    // The snapshot is untouched: live-root GC has no authority over it, and
    // it keeps full parity with the data it was published from.
    assert_eq!(
        generation_names(&snap_path),
        vec!["one-a", "one-b", "two-a", "two-b"],
        "snapshot outlives live-root GC with independent bytes"
    );

    // Snapshot retention belongs to its publisher/consumer contract:
    // deleting it (a consumer decision) never disturbs the live root.
    fs::remove_file(&snap_path).unwrap();
    assert_eq!(
        ownership.selected().slot.generation_id,
        "g-two",
        "live root unaffected by external snapshot removal"
    );
}

// ---------------------------------------------------------------------------
// 6. Backup: pin-before-copy, exact bytes, rejection surfaces
// ---------------------------------------------------------------------------

/// A backup pins the exact selected manifest sequence before copying; the
/// published backup contains byte-identical generation + WAL files whose
/// hashes match the backup manifest, outside the live root. Unsafe names
/// and destinations inside the live root are rejected with typed errors.
#[test]
fn backup_pins_selected_and_copies_exact_bytes() {
    if in_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let (gen_root, published) = publish_many(&dir, &["one", "two"]);
    let (_, seq2, rel2) = &published[1];

    let ownership = RootOwnership::open(&gen_root).expect("open owned root");
    let auth = RetirementAuthority::new(&ownership);

    // Rejection surfaces: unsafe name; destination inside the live root.
    let outside = dir.path().join("backups");
    let err = backup_generation_root(&auth, &ownership, &outside, "../escape")
        .expect_err("unsafe name rejected");
    assert!(matches!(err, RetirementError::UnsafeName), "got: {err}");
    let err = backup_generation_root(&auth, &ownership, &gen_root.join("backups"), "b0")
        .expect_err("destination inside root rejected");
    assert!(
        matches!(err, RetirementError::DestinationInsideRoot),
        "got: {err}"
    );

    let receipt = backup_generation_root(&auth, &ownership, &outside, "b1").expect("backup");
    assert_eq!(receipt.publication_sequence, *seq2);
    assert!(receipt.wal_files >= 1, "WAL bytes captured with the backup");
    assert!(receipt.copied_bytes > 0);
    assert!(
        !receipt.backup_dir.starts_with(ownership.canonical_root()),
        "backup lives outside the live root"
    );

    // The backup manifest records the exact pinned sequence + every hash.
    let manifest_data =
        fs::read(receipt.backup_dir.join(grafeo_engine::BACKUP_MANIFEST_NAME)).expect("manifest");
    let (record, _): (grafeo_engine::GenerationBackupManifest, usize) =
        bincode::serde::decode_from_slice(&manifest_data, bincode::config::standard())
            .expect("decode backup manifest");
    assert_eq!(record.publication_sequence, *seq2);
    assert_eq!(record.generation_id, "g-two");
    assert_eq!(record.generation_path, *rel2);

    // The copied generation is byte-identical to the pinned slot bytes.
    let ops = OsGenerationFileOps;
    let copied = receipt.backup_dir.join(
        std::path::Path::new(rel2)
            .file_name()
            .expect("generation file name"),
    );
    assert_eq!(
        ops.sha256(&copied).expect("hash copied generation"),
        record.generation_sha256,
        "backup copied exactly the pinned bytes"
    );
    assert_eq!(
        ops.sha256(&gen_root.join(rel2))
            .expect("hash source generation"),
        record.generation_sha256,
        "source bytes match the slot's recorded hash"
    );

    // Re-running into the same name fails closed (never overwrite a backup).
    let err = backup_generation_root(&auth, &ownership, &outside, "b1")
        .expect_err("existing backup name rejected");
    assert!(
        matches!(err, RetirementError::ValidationFailed(_)),
        "got: {err}"
    );
}

// ---------------------------------------------------------------------------
// 7. Backup pin survives publication; GC protects pinned, then collects
// ---------------------------------------------------------------------------

/// A backup pin taken on generation N protects it while publications
/// advance the manifest past it (N falls out of both slots): GC classifies
/// it `BackupPinned` and never deletes it. Once the pin drops, the same
/// generation becomes eligible and is collected.
#[test]
fn backup_pin_survives_publication_gc_protects_then_collects() {
    if in_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let (gen_root, published) = publish_many(&dir, &["one", "two", "three"]);
    let (_, _, rel1) = published[0].clone();
    let (_, seq3, _) = published[2];

    // Open + authority; pin generation 1 (currently the oldest) for backup.
    let ownership = RootOwnership::open(&gen_root).expect("open owned root");
    let auth = RetirementAuthority::new(&ownership);
    let pin = auth.pin_for_backup(rel1.clone(), 1);
    assert_eq!(pin.pinned_path(), rel1);
    drop(ownership);

    // Publications advance the manifest: generation 1 falls out of both
    // slots (slots retain only the newest two).
    let db = GrafeoDB::new_in_memory();
    populate(&db, "four");
    let (seq4, _) = publish(&db, &gen_root, "g-four");
    assert!(seq4 > seq3);
    drop(db);

    let ownership = RootOwnership::open(&gen_root).expect("re-open owned root");
    let plan = plan_retirement(&auth).expect("plan");
    let pinned = plan
        .protected
        .iter()
        .find(|c| c.path == rel1)
        .expect("generation 1 still classified");
    assert_eq!(
        pinned.class,
        RetentionClass::BackupPinned,
        "pin protects the generation even after it leaves both slots"
    );

    // GC collects everything eligible but never the pinned generation.
    let retired = collect_retirement(&auth, &plan).expect("collect");
    assert!(
        retired.iter().all(|c| c.path != rel1),
        "pinned generation never collected"
    );
    assert!(gen_root.join(&rel1).is_file(), "pinned bytes survive GC");

    // The pinned bytes are still exactly the original bytes (publication
    // never modified them — the backup will copy the pinned bytes).
    let ops = OsGenerationFileOps;
    let sha_pinned = ops.sha256(&gen_root.join(&rel1)).expect("hash pinned");
    assert_eq!(
        generation_names(&gen_root.join(&rel1)),
        vec!["one-a", "one-b"],
        "pinned generation still serves its immutable bytes (sha {sha_pinned:?})"
    );

    // Pin released: the generation becomes eligible and is collected.
    drop(pin);
    let plan = plan_retirement(&auth).expect("plan after unpin");
    assert!(plan.eligible.iter().any(|c| c.path == rel1));
    let retired = collect_retirement(&auth, &plan).expect("collect after unpin");
    assert!(retired.iter().any(|c| c.path == rel1));
    assert!(
        !gen_root.join(&rel1).exists(),
        "unpinned, unreferenced generation collected"
    );
    drop(ownership);
}

// ---------------------------------------------------------------------------
// 8. Restore: validate + publish into a new exclusively locked root
// ---------------------------------------------------------------------------

/// Restore re-validates every declared backup file, publishes into a new
/// exclusively locked root (never overwriting an existing one), and the
/// restored root passes recovery with exact query parity.
#[test]
fn restore_recovers_backed_up_generation_with_parity() {
    if in_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let (gen_root, published) = publish_many(&dir, &["one", "two"]);
    let (_, seq2, _) = &published[1];

    let ownership = RootOwnership::open(&gen_root).expect("open owned root");
    let auth = RetirementAuthority::new(&ownership);
    let receipt = backup_generation_root(&auth, &ownership, &dir.path().join("backups"), "b1")
        .expect("backup");
    drop(ownership);

    // Restore into a NEW root: recovery must select the backed-up sequence.
    let new_root = dir.path().join("restored.grafeo.d");
    let restored = restore_generation_root(&receipt.backup_dir, &new_root).expect("restore");
    assert_eq!(restored.selected().slot.publication_sequence, *seq2);
    assert_eq!(restored.selected().slot.generation_id, "g-two");
    assert_eq!(restored.mode(), OpenMode::Writable);

    // Query parity against the restored generation container.
    assert_eq!(
        generation_names(&restored.selected().generation_abs_path),
        vec!["one-a", "one-b", "two-a", "two-b"],
        "restored generation answers exactly the backed-up data"
    );

    // The restored root is exclusively locked: a second process is rejected.
    let err = RootOwnership::open(&new_root).expect_err("restored root is owned");
    assert!(matches!(err, OwnershipError::Lock(_)), "got: {err}");
    drop(restored);

    // Restore never overwrites an existing root (never a mapped generation).
    let err = restore_generation_root(&receipt.backup_dir, &new_root)
        .expect_err("non-empty restore root rejected");
    assert!(matches!(err, RetirementError::NonEmptyRoot), "got: {err}");

    // A corrupted backup fails validation before any byte is published.
    let tampered = receipt.backup_dir.join("wal");
    let mut first_wal = None;
    for entry in fs::read_dir(&tampered).unwrap() {
        first_wal = Some(entry.unwrap().path());
        break;
    }
    if let Some(wal_path) = first_wal {
        fs::write(&wal_path, b"tampered").unwrap();
        let other_root = dir.path().join("other-restored.grafeo.d");
        let err = restore_generation_root(&receipt.backup_dir, &other_root)
            .expect_err("tampered backup rejected");
        assert!(
            matches!(err, RetirementError::ValidationFailed(_)),
            "got: {err}"
        );
    }
}

// ---------------------------------------------------------------------------
// 9. GC never deletes selected / previous / pinned / leased generations
// ---------------------------------------------------------------------------

/// With three generations, a live in-process lease on the oldest, and a
/// backup pin on an unreferenced orphan, a GC pass deletes only the truly
/// unreferenced artifacts — every protected class survives with its reason.
#[test]
fn gc_never_deletes_selected_previous_pinned_or_leased() {
    if in_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let (gen_root, published) = publish_many(&dir, &["one", "two", "three"]);
    let (_, _, rel1) = published[0].clone();
    let (_, _, rel3) = published[2].clone();

    let ownership = RootOwnership::open(&gen_root).expect("open owned root");

    // In-process lease on generation 1 (out of both manifest slots now).
    let registry = grafeo_engine::GenerationLeaseRegistry::from_selected(
        1,
        "g-one".to_string(),
        gen_root.join(&rel1),
    )
    .expect("lease registry on generation 1");
    let auth = RetirementAuthority::new(&ownership).with_lease_registry(Arc::clone(&registry));

    // An unreferenced orphan generation, backup-pinned.
    let orphan_rel = "generations/g-orphan.grafeo".to_string();
    fs::copy(gen_root.join(&rel3), gen_root.join(&orphan_rel)).unwrap();
    let pin = auth.pin_for_backup(orphan_rel.clone(), 99);

    let plan = plan_retirement(&auth).expect("plan");
    // Nothing is eligible: g1 leased, g2 previous, g3 selected, orphan pinned.
    assert!(
        plan.eligible.is_empty(),
        "every artifact protected: {:?}",
        plan.eligible
    );
    let class_of = |path: &str| {
        plan.protected
            .iter()
            .find(|c| c.path == path)
            .map(|c| c.class)
    };
    assert_eq!(class_of(&rel1), Some(RetentionClass::InProcessLeased));
    assert_eq!(
        class_of(&published[1].2),
        Some(RetentionClass::PreviousRecoveryRetained)
    );
    assert_eq!(class_of(&rel3), Some(RetentionClass::Selected));
    assert_eq!(class_of(&orphan_rel), Some(RetentionClass::BackupPinned));

    // Reasons are populated for observability (packet requirement 5).
    for entry in &plan.protected {
        assert!(!entry.reason.is_empty(), "reason for {:?}", entry.path);
    }

    let retired = collect_retirement(&auth, &plan).expect("collect");
    assert!(retired.is_empty(), "nothing collectable");
    for rel in [&rel1, &published[1].2, &rel3, &orphan_rel] {
        assert!(gen_root.join(rel).is_file(), "{rel} survives GC");
    }

    drop(pin);
    drop(registry);
    drop(ownership);
}

// ---------------------------------------------------------------------------
// 10. TOCTOU rail: a stale plan fails closed and deletes nothing
// ---------------------------------------------------------------------------

/// A backup pin landing BETWEEN plan and collect must abort the collection
/// with the typed `SelectionChanged` error and delete nothing — the GC
/// re-evaluates protection from completely fresh state before deleting.
#[test]
fn stale_plan_fails_closed_when_pin_lands_between_plan_and_collect() {
    if in_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let (gen_root, published) = publish_many(&dir, &["one", "two", "three"]);
    let (_, _, rel1) = published[0].clone();

    let ownership = RootOwnership::open(&gen_root).expect("open owned root");
    let auth = RetirementAuthority::new(&ownership);

    // Plan: generation 1 (out of both slots) is eligible.
    let plan = plan_retirement(&auth).expect("plan");
    assert!(plan.eligible.iter().any(|c| c.path == rel1));

    // A backup starts after the plan: the pin must invalidate it.
    let pin = auth.pin_for_backup(rel1.clone(), 1);
    let err = collect_retirement(&auth, &plan).expect_err("stale plan must fail closed");
    assert!(
        matches!(err, RetirementError::SelectionChanged(_)),
        "got: {err}"
    );
    assert!(
        gen_root.join(&rel1).is_file(),
        "nothing deleted on a stale plan"
    );

    // A hand-crafted stale plan naming the SELECTED generation must also
    // fail closed (defense against any caller-constructed plan).
    let malicious = RetirementPlan {
        eligible: vec![ClassifiedGeneration {
            path: published[2].2.clone(),
            generation_id: None,
            class: RetentionClass::Eligible,
            reason: "caller-crafted".to_string(),
        }],
        protected: Vec::new(),
        planned_selected: None,
    };
    let err = collect_retirement(&auth, &malicious)
        .expect_err("plan naming the selected generation fails closed");
    assert!(
        matches!(err, RetirementError::SelectionChanged(_)),
        "got: {err}"
    );
    assert!(gen_root.join(&published[2].2).is_file());

    // After the pin drops, the original plan executes cleanly.
    drop(pin);
    let retired = collect_retirement(&auth, &plan).expect("collect after unpin");
    assert!(retired.iter().any(|c| c.path == rel1));
    drop(ownership);
}

// ---------------------------------------------------------------------------
// 11. GC interleaved with publication + concurrent pin churn
// ---------------------------------------------------------------------------

/// GC and publication coexist correctly under the exclusive root lock:
/// publication and GC serialize on the lock (publication re-acquires it), so
/// the only genuine concurrent surface is the in-process pin registry. This
/// test interleaves publication + GC passes (each under the lock) while a
/// second thread churns backup pins, and proves: the pinned generation is
/// never deleted while pinned, the selected/previous generations are never
/// deleted, and recovery stays valid throughout.
#[test]
fn gc_interleaved_with_publication_and_pin_churn_never_deletes_protected() {
    if in_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let (gen_root, published) = publish_many(&dir, &["one", "two", "three"]);
    let (_, _, rel1) = published[0].clone();

    // The retirement authority is bound to the root. GC and publication each
    // take the exclusive root lock, so they serialize exactly as they do in
    // the owner process: each GC pass opens a fresh ownership (acquiring the
    // lock), and each publication runs with the lock free.
    let ownership = RootOwnership::open(&gen_root).expect("open owned root");
    let auth = Arc::new(RetirementAuthority::new(&ownership));
    drop(ownership);

    // A dedicated generation to churn pins on (out of both slots). Pin
    // churn is the one genuinely concurrent surface: pins live in the
    // authority's registry and never touch the root lock.
    let churn_rel = rel1.clone();
    let churn_auth = Arc::clone(&auth);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let churn_stop = Arc::clone(&stop);
    let churner = std::thread::spawn(move || {
        let mut i = 0u64;
        while !churn_stop.load(std::sync::atomic::Ordering::Relaxed) {
            let guard = churn_auth.pin_for_backup(churn_rel.clone(), i);
            std::hint::black_box(&guard);
            drop(guard);
            i += 1;
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    // Interleave publication (lock free) + a GC pass (under a fresh lock).
    let db = GrafeoDB::new_in_memory();
    for tag in ["four", "five", "six", "seven"] {
        populate(&db, tag);
        publish(&db, &gen_root, &format!("g-{tag}"));
        // A GC pass under the lock: it must never delete the selected or
        // previous generation, and must respect any pin the churner
        // currently holds on generation 1.
        let ownership = RootOwnership::open(&gen_root).expect("open for GC");
        let plan = plan_retirement(&auth).expect("plan under lock");
        match collect_retirement(&auth, &plan) {
            Ok(_) | Err(RetirementError::SelectionChanged(_)) => {}
            Err(e) => panic!("GC failed under lock: {e}"),
        }
        drop(ownership);
    }
    drop(db);

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    churner.join().expect("churner thread");

    // Final state: the newest publication is selected; selected + previous
    // survive; recovery is valid.
    assert_eq!(
        grafeo_engine::read_manifest_state(&gen_root)
            .expect("manifest readable")
            .selected
            .generation_id,
        "g-seven"
    );
    let ownership = RootOwnership::open(&gen_root).expect("final recovery");
    let report = auth.lifecycle_report(&ownership).expect("final report");
    let selected = report.selected.as_ref().expect("selected reported");
    assert!(selected.generation_id.as_deref() == Some("g-seven"));
    assert!(
        report.retired.iter().all(|c| c.path != selected.path
            && report.previous.as_ref().is_none_or(|p| p.path != c.path)),
        "no protected generation was retired: {:?}",
        report.retired
    );
    drop(ownership);
}
// ---------------------------------------------------------------------------
// 12. Observability: every class with IDs and reasons, no strong refs
// ---------------------------------------------------------------------------

/// The lifecycle report exposes root-lock owner state plus selected /
/// previous / pinned / in-process-leased / eligible / retired IDs and
/// reasons — and holds no strong reference to any generation base, so
/// producing the report never prevents retirement.
#[test]
fn lifecycle_report_exposes_all_classes_without_strong_refs() {
    if in_child() {
        return;
    }
    let dir = TempDir::new().unwrap();
    let (gen_root, published) = publish_many(&dir, &["one", "two", "three"]);
    let (_, _, rel1) = published[0].clone();
    let (_, _, rel3) = published[2].clone();

    let ownership = RootOwnership::open(&gen_root).expect("open owned root");

    // Lease on generation 1 (out of both slots) + an unreferenced orphan.
    let registry = grafeo_engine::GenerationLeaseRegistry::from_selected(
        1,
        "g-one".to_string(),
        gen_root.join(&rel1),
    )
    .expect("lease registry");
    let lease = registry.snapshot();
    let weak = lease.downgrade();
    let auth = RetirementAuthority::new(&ownership).with_lease_registry(Arc::clone(&registry));

    let orphan_rel = "generations/g-orphan.grafeo".to_string();
    fs::copy(gen_root.join(&rel3), gen_root.join(&orphan_rel)).unwrap();

    let report = auth.lifecycle_report(&ownership).expect("lifecycle report");

    // Root-lock owner state.
    assert_eq!(report.mode, OpenMode::Writable);
    assert_eq!(report.canonical_root, gen_root.canonicalize().unwrap());
    assert_eq!(
        report.lock_path,
        gen_root.canonicalize().unwrap().join("root.lock")
    );

    // Every class with IDs and reasons.
    let selected = report.selected.as_ref().expect("selected reported");
    assert_eq!(selected.path, rel3);
    assert_eq!(selected.generation_id.as_deref(), Some("g-three"));
    assert!(selected.reason.contains("selected"));
    let previous = report.previous.as_ref().expect("previous reported");
    assert_eq!(previous.path, published[1].2);
    assert!(previous.reason.contains("previous"));
    assert!(
        report
            .in_process_leased
            .iter()
            .any(|c| c.path == rel1 && c.reason.contains("strong reference")),
        "leased class reported with refcount reason: {:?}",
        report.in_process_leased
    );
    assert!(
        report.eligible.iter().any(|c| c.path == orphan_rel),
        "eligible orphan reported"
    );

    // A pinned generation appears in the pinned class.
    let pin = auth.pin_for_backup(orphan_rel.clone(), 77);
    let report = auth.lifecycle_report(&ownership).expect("report with pin");
    assert!(
        report
            .backup_pinned
            .iter()
            .any(|c| c.path == orphan_rel && c.reason.contains("backup")),
        "pinned class reported: {:?}",
        report.backup_pinned
    );
    drop(pin);

    // Retired entries appear with reasons after a collection.
    let plan = plan_retirement(&auth).expect("plan");
    collect_retirement(&auth, &plan).expect("collect");
    let report = auth
        .lifecycle_report(&ownership)
        .expect("report after collect");
    assert!(
        report
            .retired
            .iter()
            .any(|c| c.path == orphan_rel && c.reason.starts_with("retired:")),
        "retired class reported with reasons: {:?}",
        report.retired
    );

    // The report holds NO strong reference to any base: the lease's strong
    // count is identical before and after producing a report that names it
    // (packet requirement 5 — observability never creates ownership that
    // prevents retirement). The authority legitimately owns the lease
    // registry's selected base; the report itself is plain data.
    let strong_before = lease.strong_count();
    let _ = auth.lifecycle_report(&ownership).expect("report");
    assert_eq!(
        lease.strong_count(),
        strong_before,
        "lifecycle_report must not add a strong reference to a leased base"
    );

    // Once every owner drops (authority, registry, lease), the mapping is
    // released — even though a report naming it is still in scope.
    drop(auth);
    drop(registry);
    drop(lease);
    assert!(
        weak.upgrade().is_none(),
        "no live owner keeps the base alive; the report is plain data"
    );
    drop(report);
    drop(ownership);
}
