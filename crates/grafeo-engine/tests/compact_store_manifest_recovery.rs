//! G-EM0.3c — recovery and publication fault proof (engine boundary).
//!
//! Proves, at the engine surface, that the accepted G-EM0.W0 state
//! transitions recover deterministically:
//!
//! 1. Engine-observable crash proofs: a fresh child process aborts at the
//!    two boundaries the ENGINE owns (pre-commit build complete;
//!    post-commit publication complete = the selection transition), and the
//!    parent proves the expected selected generation, a replayable exact
//!    WAL boundary, query parity against the selected generation, and
//!    correct orphan classification. The remaining per-transition crash
//!    points are W0's accepted proofs (`faults_tests.rs`,
//!    `fresh_process_faults.rs`) against the same `publish_generation` the
//!    engine calls; their expectations are locked here via
//!    `PublicationCrashPoint` (see `crash_point_surface_is_complete_and_ordered`).
//! 2. Torn/corrupt slots and generations fail or fall back exactly as
//!    specified: torn newest generation → previous fallback; both corrupt →
//!    typed fail-closed error preserving both causes.
//! 3. No recovery path selects by mtime or silently discards accepted
//!    writes: an orphaned future-mtime generation is classified, never
//!    promoted, and never deleted.
//! 4. WAL advancement proof: frames committed after a publication's recorded
//!    boundary survive post-commit truncation and remain replayable from
//!    that boundary (no accepted write is silently discarded).
//!
//! Selection authority, the power-loss semantics, and the `#[cfg(test)]`
//! fault hooks live in W0 (`grafeo-storage`); this suite wires the runtime
//! engine recovery surface and locks the per-point expectations.

use std::fs;
use std::process::Command;

use bytes::Bytes;
use grafeo_common::storage::SectionType;
use grafeo_common::types::{PropertyKey, Value};
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_engine::{
    GrafeoDB, OrphanClassification, PublicationCrashPoint, generation_build_request,
    read_manifest_state, recover_generation_root,
};
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::generation::manifest;
use grafeo_storage::generation::wal_cursor::validate_replayable;
use grafeo_storage::wal::{WalManager, WalRecord};
use tempfile::TempDir;

const HELPER_ENV: &str = "GRAFEORECOV_HELPER";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn populate(db: &GrafeoDB, tag: &str) {
    let a = db
        .create_node_with_props(&["Person"], [("name", Value::from(format!("{tag}-a")))])
        .expect("node a");
    let b = db
        .create_node_with_props(&["Person"], [("name", Value::from(format!("{tag}-b")))])
        .expect("node b");
    let _e = db.create_edge(a, b, "KNOWS");
}

/// Query-parity probe: open the selected generation through the production
/// read path (`GrafeoFileManager::open_read_only` + `CompactStoreSection::
/// deserialize_from_bytes` — the exact mechanism of the W0/3a contract
/// tests, no second reader implementation) and return the sorted node
/// names + edge count.
fn generation_contents(generation_abs: &std::path::Path) -> (Vec<String>, u64) {
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
    (names, store.total_edges())
}

/// True when this process is a re-exec'd crash child (any lifecycle marker).
fn in_any_child() -> bool {
    [
        HELPER_ENV,
        "GRAFEOLOCK_HELPER",
        "GRAFEOPUB_HELPER",
        "GRAFEOROOTPROC_HELPER",
    ]
    .iter()
    .any(|v| std::env::var(v).is_ok())
}

// ---------------------------------------------------------------------------
// 1. Runtime recovery wiring: selected generation + boundary + parity + orphans
// ---------------------------------------------------------------------------

/// Recovering a healthy two-generation root at the engine boundary selects
/// the newest generation, exposes its exact WAL boundary, returns query
/// parity against the selected generation, and classifies the previous
/// generation as retained (never as an orphan).
#[test]
fn recover_selects_newest_with_parity_and_retained_previous() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "first");
    let first = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-first"))
        .expect("first publish")
        .publication;

    populate(&db, "second");
    let second = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-second"))
        .expect("second publish")
        .publication;
    drop(db);

    let recovery = recover_generation_root(&gen_root).expect("recover generation root");
    assert_eq!(recovery.selected.slot.generation_id, "g-second");
    assert_eq!(recovery.selected.slot.publication_sequence, 2);
    assert_eq!(recovery.wal_boundary, second.wal_boundary);

    // Exact WAL replay range: the recorded boundary is replayable against
    // the surviving real WAL files.
    validate_replayable(&gen_root.join("wal"), &recovery.wal_boundary.to_cursor())
        .expect("selected boundary replayable");

    // Query parity: the selected generation answers through the production
    // read path with exactly the data published into it.
    let (names, edges) = generation_contents(&recovery.selected.generation_abs_path);
    assert_eq!(names, vec!["first-a", "first-b", "second-a", "second-b"]);
    assert_eq!(edges, 2);

    // Orphan classification: selected + retained previous, no unreferenced.
    assert!(
        recovery
            .orphans
            .contains(&OrphanClassification::SelectedGeneration {
                path: second.generation_path.clone()
            })
    );
    assert!(
        recovery
            .orphans
            .contains(&OrphanClassification::PreviousGeneration {
                path: first.generation_path.clone()
            })
    );
    assert_eq!(
        recovery.previous_generation_path.as_deref(),
        Some(first.generation_path.as_str())
    );
    assert!(
        !recovery
            .orphans
            .iter()
            .any(|c| matches!(c, OrphanClassification::UnreferencedGeneration { .. })),
        "no unreferenced generations on a healthy root: {:?}",
        recovery.orphans
    );
}

/// Recovering a genesis (all-zero manifest) root fails closed through the
/// typed W0 recovery error surface — never by guessing.
#[test]
fn recover_genesis_root_fails_closed() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("empty.grafeo.d");
    fs::create_dir_all(gen_root.join("wal")).unwrap();
    // Genesis layout: zeroed manifest, no generations.
    fs::write(
        gen_root.join("manifest.bin"),
        [0u8; manifest::MANIFEST_SIZE],
    )
    .unwrap();

    let err = recover_generation_root(&gen_root).expect_err("genesis must fail closed");
    // Typed identity: the W0 NoValidGeneration variant is preserved through
    // the engine error surface (never flattened to an opaque I/O error).
    let is_no_valid = matches!(
        &err,
        grafeo_engine::RecoveryViewError::Recovery(
            grafeo_storage::generation::recovery::RecoveryError::NoValidGeneration(_)
        )
    );
    assert!(is_no_valid, "expected NoValidGeneration, got: {err}");
}

/// A second PROCESS recovering while the root lock is held fails through the
/// typed `Lock` branch (Option S: one process owns the live root), never
/// blocks silently or steals ownership. Proven cross-process: the parent
/// holds the lock while a child attempts recovery and is rejected (flocks
/// are per-fd within one process, so same-process threads cannot exercise
/// this — the child is the honest Option-S boundary).
#[test]
fn recover_while_locked_fails_typed_lock_error() {
    if std::env::var(HELPER_ENV).is_ok() {
        // Child: attempt recovery on the parent's locked root; exit 0 only
        // if rejected with the typed Lock error.
        let root = std::env::var("GRAFEORECOV_ROOT").expect("child root env");
        match recover_generation_root(std::path::Path::new(&root)) {
            Err(grafeo_engine::RecoveryViewError::Lock(_)) => std::process::exit(0),
            other => {
                eprintln!("child expected typed Lock rejection, got: {other:?}");
                std::process::exit(1);
            }
        }
    }
    if in_any_child() {
        return;
    }

    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "held");
    db.build_and_publish_generation(generation_build_request(&gen_root, "g-held"))
        .expect("publish");
    drop(db);

    // Parent recovery holds the lock for its lifetime.
    let held = recover_generation_root(&gen_root).expect("parent recovery");

    // A second process attempting recovery is rejected with the typed Lock
    // error (exit 0 from the child = rejected as expected).
    let status = Command::new(std::env::current_exe().expect("current exe"))
        .arg("recover_while_locked_fails_typed_lock_error")
        .arg("--exact")
        .env(HELPER_ENV, "1")
        .env("GRAFEORECOV_ROOT", &gen_root)
        .status()
        .expect("spawn lock-contention child");
    assert!(
        status.success(),
        "child must be rejected with typed Lock error, got {status}"
    );
    drop(held);

    // After the parent's lock drops, recovery succeeds again.
    let after = recover_generation_root(&gen_root).expect("recovery after lock release");
    assert_eq!(after.selected.slot.generation_id, "g-held");
}

// ---------------------------------------------------------------------------
// 2. Torn/corrupt slots and generations: fail or fall back exactly as specified
// ---------------------------------------------------------------------------

/// A torn (corrupt) newest generation file must fall back to the explicitly
/// retained previous slot; the fallback boundary stays replayable and the
/// previous generation answers with query parity.
#[test]
fn torn_newest_generation_falls_back_to_previous() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let first = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-one"))
        .expect("first publish")
        .publication;
    populate(&db, "two");
    let second = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-two"))
        .expect("second publish")
        .publication;
    drop(db);

    // Tear the newest generation file (simulate a post-commit generation
    // corruption the manifest cannot know about).
    fs::write(gen_root.join(&second.generation_path), b"torn").unwrap();

    let recovery = recover_generation_root(&gen_root).expect("fallback recovery");
    assert_eq!(recovery.selected.slot.generation_id, "g-one");
    assert_eq!(recovery.selected.slot.publication_sequence, 1);
    assert_eq!(recovery.wal_boundary, first.wal_boundary);
    validate_replayable(&gen_root.join("wal"), &recovery.wal_boundary.to_cursor())
        .expect("fallback boundary replayable");

    // Parity of the fallback base: exactly the first generation's data.
    let (names, edges) = generation_contents(&recovery.selected.generation_abs_path);
    assert_eq!(names, vec!["one-a", "one-b"]);
    assert_eq!(edges, 1);

    // The torn file is still named by a structurally valid slot (g-two's
    // slot decodes; only its referenced bytes are corrupt), so it remains
    // slot-referenced and must NOT be classified unreferenced —
    // classification is slot-authority, never bytes-authority. W0 recovery
    // selected g-one; g-two stays the retained previous slot.
    assert!(
        !recovery
            .orphans
            .iter()
            .any(|c| matches!(c, OrphanClassification::UnreferencedGeneration { .. })),
        "a slot-referenced generation is never an orphan: {:?}",
        recovery.orphans
    );
}

/// When both slots' generations are corrupt, recovery fails closed with the
/// typed both-causes error — never a guess, never a silent pick.
#[test]
fn corrupt_both_generations_fail_closed_with_both_causes() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let first = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-one"))
        .expect("first publish")
        .publication;
    populate(&db, "two");
    let second = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-two"))
        .expect("second publish")
        .publication;
    drop(db);

    fs::write(gen_root.join(&first.generation_path), b"torn-a").unwrap();
    fs::write(gen_root.join(&second.generation_path), b"torn-b").unwrap();

    let err = recover_generation_root(&gen_root).expect_err("both corrupt must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("no valid generation"),
        "typed NoValidGeneration, got: {msg}"
    );
    // Both causes preserved (never flattened to a single opaque failure).
    assert!(msg.contains("slot 0"), "slot 0 cause preserved: {msg}");
    assert!(msg.contains("slot 1"), "slot 1 cause preserved: {msg}");
}

// ---------------------------------------------------------------------------
// 3. No mtime selection, no silent discard, orphan classification
// ---------------------------------------------------------------------------

/// An unreferenced generation file with a far-future mtime must be
/// classified as an orphan — never promoted to selected, never deleted —
/// while the manifest-selected generation keeps authority.
#[test]
fn orphan_future_mtime_generation_never_promoted() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let first = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-one"))
        .expect("first publish")
        .publication;
    populate(&db, "two");
    let second = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-two"))
        .expect("second publish")
        .publication;
    drop(db);

    // Plant an orphan: a full copy of the newest generation under an
    // unreferenced name, with a far-future mtime to tempt mtime selection.
    let orphan_name = "g-99999999999999999999-deadbeefdeadbeef.grafeo";
    let orphan_path = gen_root.join("generations").join(orphan_name);
    fs::copy(gen_root.join(&second.generation_path), &orphan_path).unwrap();
    let future = filetime::FileTime::from_unix_time(4_102_444_800, 0); // 2100-01-01
    filetime::set_file_mtime(&orphan_path, future).unwrap();
    // Also give the SELECTED generation a past mtime, reversing mtime order.
    let past = filetime::FileTime::from_unix_time(946_684_800, 0); // 2000-01-01
    filetime::set_file_mtime(gen_root.join(&second.generation_path), past).unwrap();

    let recovery = recover_generation_root(&gen_root).expect("recovery ignores mtime");
    assert_eq!(
        recovery.selected.slot.generation_id, "g-two",
        "manifest authority, not mtime, selects the generation"
    );
    assert_eq!(recovery.selected.slot.publication_sequence, 2);

    // The orphan is classified, never promoted, never deleted.
    assert!(
        recovery
            .orphans
            .contains(&OrphanClassification::UnreferencedGeneration {
                name: orphan_name.to_string()
            }),
        "orphan classified: {:?}",
        recovery.orphans
    );
    assert!(orphan_path.is_file(), "orphan never deleted by recovery");
    assert_eq!(
        recovery.selected.slot.generation_path, second.generation_path,
        "orphan never promoted"
    );

    // First generation remains the retained previous (not an orphan).
    assert!(
        recovery
            .orphans
            .contains(&OrphanClassification::PreviousGeneration {
                path: first.generation_path.clone()
            })
    );
}

// ---------------------------------------------------------------------------
// 4. WAL advancement: accepted writes after the boundary are never discarded
// ---------------------------------------------------------------------------

/// Frames committed after a publication's recorded boundary survive the
/// post-commit WAL truncation and remain replayable from that boundary —
/// the exact-replay-range proof that no accepted write is silently
/// discarded when the WAL advances.
#[test]
fn wal_advance_preserves_post_boundary_writes() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let first = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-one"))
        .expect("first publish")
        .publication;
    drop(db);

    // Simulate accepted writes AFTER the boundary: log + commit frames into
    // the root WAL, then publish a second generation (which cuts a new
    // boundary and truncates the WAL at the dual-slot retention floor).
    {
        let wal = WalManager::open(gen_root.join("wal")).expect("open wal");
        wal.log(&WalRecord::Checkpoint {
            transaction_id: grafeo_common::types::TransactionId::new(900),
        })
        .expect("log post-boundary frame");
        wal.sync().expect("sync wal");
    }

    let db = GrafeoDB::new_in_memory();
    populate(&db, "two");
    let second = db
        .build_and_publish_generation(generation_build_request(&gen_root, "g-two"))
        .expect("second publish")
        .publication;
    drop(db);

    let recovery = recover_generation_root(&gen_root).expect("recovery after wal advance");
    assert_eq!(recovery.selected.slot.generation_id, "g-two");

    // The selected boundary advanced past the first publication's boundary
    // (log sequence monotonic) and the replay range from the new boundary
    // validates against the surviving real WAL files.
    assert!(second.wal_boundary.log_sequence >= first.wal_boundary.log_sequence);
    validate_replayable(&gen_root.join("wal"), &recovery.wal_boundary.to_cursor())
        .expect("advanced boundary replayable; no accepted write discarded");

    // The post-boundary frame actually survived truncation: replaying from
    // the FIRST publication's boundary must still validate, which requires
    // the frame's file (between the two boundaries) to exist and parse.
    // This is the concrete "no accepted write silently discarded" proof —
    // validating only from the new boundary would pass even if the frame's
    // file had been wrongly truncated.
    validate_replayable(&gen_root.join("wal"), &first.wal_boundary.to_cursor())
        .expect("post-boundary frame's WAL file survived truncation");

    // The first generation's boundary log is still retained while its slot
    // survives (dual-slot retention floor).
    let state = read_manifest_state(&gen_root).expect("manifest state");
    let previous = state.previous.expect("previous slot retained");
    assert_eq!(previous.generation_id, "g-one");
}

// ---------------------------------------------------------------------------
// 5. Engine-observable crash matrix (fresh process per boundary)
// ---------------------------------------------------------------------------
//
// W0 owns the full 15-point deterministic power-loss matrix
// (`grafeo-storage/generation/tests/faults_tests.rs`) and 4 fixture-graph
// fresh-process abort proofs (`fresh_process_faults.rs`); the engine crash
// matrix does NOT re-run those — it proves the ENGINE build+publish path
// (live `GrafeoDB` graph → 3a record sources → W0 publication) recovers
// correctly when a fresh child process hard-aborts at the two boundaries
// the ENGINE owns:
//
// - `after_generation_build`: the generation build completed but W0
//   publication never started — strictly pre-commit, so recovery must
//   select the PRIOR generation and classify the crash's partial artifacts
//   (none escape the unpublished dir: the build only produced in-memory
//   output, so nothing new survives).
// - `after_publication`: W0 publication returned — the manifest fsync (the
//   durable commit point) has passed — strictly post-commit, so recovery
//   must select the NEW generation at the selection transition, with a
//   replayable boundary and full query parity.
//
// The engine-level `PublicationCrashPoint` surface (see
// `crash_point_surface_is_complete_and_ordered`) locks the per-point
// expectations for the complete accepted transition set; the two proofs
// below anchor that surface at the engine boundary in fresh processes.

/// Child entry point: run the REAL engine build+publish path and hard-abort
/// at the engine-observable boundary named by `GRAFEO_3C_ABORT` (wired
/// inside `build_generation_inner`; see generation_build.rs).
fn child_main() {
    let root = std::env::var("GRAFEORECOV_ROOT").expect("child root env");
    let root = std::path::PathBuf::from(root);

    let db = GrafeoDB::new_in_memory();
    populate(&db, "crashed");
    // `GRAFEO_3C_ABORT` is inherited from the parent; the engine aborts at
    // the named boundary inside `build_and_publish_generation`.
    let _ = db.build_and_publish_generation(generation_build_request(&root, "g-crashed"));
    std::process::exit(0);
}

/// One engine crash case: abort a fresh child at `boundary` (the value the
/// `GRAFEO_3C_ABORT` seam recognizes), then prove the locked recovery
/// expectation (selection, replayable boundary, parity, orphans). The child
/// is re-exec'd with `test_name` as its filter so ONLY the crash test runs
/// inside the child (an unfiltered re-exec would run the whole suite and
/// never reach `child_main`).
fn engine_crash_case(boundary: &str, test_name: &str, expect_new: bool) {
    // Prior durable generation (seq 1) published by the parent.
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();
    {
        let db = GrafeoDB::new_in_memory();
        populate(&db, "prev");
        db.build_and_publish_generation(generation_build_request(&gen_root, "g-prev"))
            .expect("prior publish");
    }

    let status = Command::new(std::env::current_exe().expect("current exe"))
        .arg(test_name)
        .arg("--exact")
        .env(HELPER_ENV, "1")
        .env("GRAFEORECOV_ROOT", &gen_root)
        .env("GRAFEO_3C_ABORT", boundary)
        .status()
        .expect("spawn child");
    // Pin the abort origin: the child must die by SIGABRT (the seam's
    // `std::process::abort`), not merely fail (a panic or unrelated error
    // exit would also be non-success but proves nothing about the boundary).
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        // SIGABRT = 6 on Linux (the generation root lock allowlist is
        // Linux-only); assert the signal directly without a libc dep.
        assert_eq!(
            status.signal(),
            Some(6),
            "child must die by SIGABRT at {boundary}, got {status}"
        );
    }
    #[cfg(not(unix))]
    assert!(!status.success(), "child must abort at {boundary}");

    // The lock is released by the child's death; recovery selects from the
    // surviving bytes exactly as the locked expectation demands.
    let recovery = recover_generation_root(&gen_root).expect("recovery on surviving bytes");
    let observed = recovery.selected.slot.publication_sequence;
    // The child builds its generation from ITS OWN fresh live graph (only
    // "crashed" nodes); the prior generation holds "prev" nodes. Parity is
    // proven against exactly what each generation was published with.
    let (expected_seq, expected_id, expected_names) = if expect_new {
        (2, "g-crashed", vec!["crashed-a", "crashed-b"])
    } else {
        (1, "g-prev", vec!["prev-a", "prev-b"])
    };
    assert_eq!(
        observed, expected_seq,
        "crash at {boundary} selected seq {observed}, expected {expected_seq}"
    );
    assert_eq!(recovery.selected.slot.generation_id, expected_id);

    // Whatever was selected, its boundary is replayable and its generation
    // answers with query parity — no torn slot, no discarded accepted write.
    validate_replayable(&gen_root.join("wal"), &recovery.wal_boundary.to_cursor())
        .expect("selected boundary replayable after crash");
    let (names, _edges) = generation_contents(&recovery.selected.generation_abs_path);
    assert_eq!(names, expected_names, "query parity at {boundary}");

    // Orphan classification stays slot-authoritative: the prior generation
    // is either selected (pre-commit) or the retained previous (post-commit),
    // and no crash artifact is ever promoted.
    assert!(
        !recovery
            .orphans
            .iter()
            .any(|c| matches!(c, OrphanClassification::UnreferencedGeneration { .. })),
        "no crash artifact promoted at {boundary}: {:?}",
        recovery.orphans
    );
}

#[test]
fn crash_after_generation_build_selects_previous() {
    if std::env::var(HELPER_ENV).is_ok() {
        child_main();
        return;
    }
    if in_any_child() {
        return;
    }
    engine_crash_case(
        "after_generation_build",
        "crash_after_generation_build_selects_previous",
        false,
    );
}

#[test]
fn crash_after_publication_selects_new_at_transition() {
    if std::env::var(HELPER_ENV).is_ok() {
        child_main();
        return;
    }
    if in_any_child() {
        return;
    }
    engine_crash_case(
        "after_publication",
        "crash_after_publication_selects_new_at_transition",
        true,
    );
}

/// The crash-point surface itself is locked: every accepted W0 publication
/// transition maps to a distinct fault hook, ordering and commit-point
/// classification are consistent with `PublicationPhase`, and every
/// pre-commit point expects the prior selection while every post-commit
/// point expects the new one. The per-point fresh-process/deterministic
/// proofs live in W0 (`faults_tests.rs`, `fresh_process_faults.rs`); this
/// locks the engine-level expectation surface they are judged against.
#[test]
fn crash_point_surface_is_complete_and_ordered() {
    use grafeo_engine::PublicationPhase;

    // 14 distinct points, all mapping to distinct W0 hooks.
    assert_eq!(PublicationCrashPoint::ALL.len(), 14);
    let mut hooks: Vec<&'static str> = PublicationCrashPoint::ALL
        .iter()
        .map(|p| p.hook_name())
        .collect();
    hooks.sort_unstable();
    hooks.dedup();
    assert_eq!(hooks.len(), 14, "every crash point has a distinct W0 hook");

    // Cross-check against the REAL W0 hook registry: every engine hook name
    // must literally appear as a `hook("<name>")` call in W0's
    // `publication.rs` source. A W0 hook rename/addition fails this test at
    // compile time of the suite instead of silently drifting. (W0 does not
    // export the hook list programmatically; `include_str!` on its source is
    // the no-new-surface drift pin.)
    const W0_PUBLICATION_SRC: &str =
        include_str!("../../grafeo-storage/src/generation/publication.rs");
    for point in PublicationCrashPoint::ALL {
        let needle = format!("hook(\"{}\")", point.hook_name());
        assert!(
            W0_PUBLICATION_SRC.contains(&needle),
            "W0 publication.rs must contain {needle} for {point:?}"
        );
    }

    // Strictly ordered.
    for window in PublicationCrashPoint::ALL.windows(2) {
        assert!(window[0] < window[1]);
    }

    // Commit-point classification agrees with the 3b phase contract: the
    // manifest sync is the boundary.
    for point in PublicationCrashPoint::ALL {
        assert_eq!(
            point.is_post_commit(),
            point >= PublicationCrashPoint::AfterManifestSync,
            "{point:?} commit classification"
        );
        let expectation = point.expected_selection(7);
        if point.is_post_commit() {
            assert!(
                expectation.admits(8) && !expectation.admits(7),
                "{point:?} expects new"
            );
        } else if point == PublicationCrashPoint::DuringSlotWrite {
            assert!(
                expectation.admits(7) && expectation.admits(8),
                "{point:?} admits old|new"
            );
        } else {
            assert!(
                expectation.admits(7) && !expectation.admits(8),
                "{point:?} expects prior"
            );
        }
    }

    // The phase ordering contract from 3b still holds (regression anchor).
    assert!(PublicationPhase::ManifestSync.is_post_commit());
    assert!(!PublicationPhase::WriteSlot.is_post_commit());
}
