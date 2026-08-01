//! G-EM0.4a — in-process generation leases and base transition.
//!
//! Proves the engine-level lease contract on top of the accepted 3a/3b/3c
//! publication/recovery surface:
//!
//! 1. Base, codec, and query snapshots carry an explicit in-process
//!    [`GenerationLease`] owning the selected mapping; publication atomically
//!    redirects new snapshots while existing readers finish against the old
//!    immutable bytes.
//! 2. Three reference classes (database-owner / selected-manifest /
//!    external read-snapshot) are distinct; mapping release is proven by
//!    **weak downgrade/drop evidence**, never by `ArcSwap::swap` or a changed
//!    generation ID alone.
//! 3. An old generation is never modified or deleted while an in-process
//!    lease on it exists; the old-reader → publish → new-reader →
//!    old-reader-completion → final-mapping-release sequence keeps the old
//!    bytes intact and readable throughout (the Windows
//!    `ERROR_USER_MAPPED_FILE` hazard is structurally unreachable because
//!    this path never mutates/deletes a mapped generation).
//! 4. Selected/previous and live lease counts are exposed **without** holding
//!    observability-only strong references (the registry tracks retired
//!    generations by `Weak` probe).
//! 5. `close_transition` returns transition errors; `Drop` is best-effort and
//!    never the tested success path.

use bytes::Bytes;
use grafeo_common::storage::SectionType;
use grafeo_common::types::{PropertyKey, Value};
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_engine::{
    GenerationLeaseRegistry, GrafeoDB, generation_build_request, recover_generation_root,
};
use grafeo_storage::file::GrafeoFileManager;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Populate a throwaway in-memory DB with one labeled node (unique `tag`) and
/// a single self-edge, so each published generation has distinct content.
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
/// return its manifest publication sequence + absolute path.
fn publish(db: &GrafeoDB, gen_root: &std::path::Path, id: &str) -> (u64, std::path::PathBuf) {
    let publication = db
        .build_and_publish_generation(generation_build_request(gen_root, id))
        .expect("publish generation")
        .publication;
    let abs = gen_root.join(&publication.generation_path);
    (publication.publication_sequence, abs)
}

/// Query-parity probe: read the sorted node names in a generation container
/// through the production read path (the exact mechanism the W0/3a contract
/// tests use — `open_read_only` + `deserialize_from_bytes`, no second reader).
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

/// Sorted node names served by a lease's live store snapshot.
fn lease_names(lease: &grafeo_engine::GenerationLease) -> Vec<String> {
    let store = lease.store();
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

// ---------------------------------------------------------------------------
// 1 + 2. Lease carries the selected mapping; publication redirects new snapshots
// ---------------------------------------------------------------------------

/// An old reader holding a lease on generation N keeps reading generation N's
/// immutable bytes after publication redirects the selected base to N+1; a new
/// reader sees N+1; when the old reader's lease drops, the old mapping is
/// released — proven by weak downgrade evidence, not by the swap alone.
#[test]
fn publish_redirects_new_snapshots_old_reader_finishes_then_releases() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&gen_root).unwrap();

    // Build two generations on disk (3a/3b surface).
    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let (seq1, abs1) = publish(&db, &gen_root, "g-one");
    populate(&db, "two");
    let (seq2, abs2) = publish(&db, &gen_root, "g-two");
    assert!(seq2 > seq1, "publication sequence is monotonic");

    // Select generation 1 as the live base (the engine boundary wraps the
    // validated selected generation from 3c recovery).
    let registry = GenerationLeaseRegistry::from_selected(seq1, "g-one".into(), abs1.clone())
        .expect("select first base");

    // OLD READER: acquire a lease on generation 1.
    let old_lease = registry.snapshot();
    assert_eq!(old_lease.publication_sequence(), seq1);
    assert_eq!(lease_names(&old_lease), generation_names(&abs1));
    // Weak probes to the OLD base — this is the release evidence.
    let old_weak = old_lease.downgrade();
    assert!(old_weak.upgrade().is_some(), "old base live while leased");

    // PUBLISH: atomically redirect the selected base to generation 2.
    let new_lease = registry
        .publish(seq2, "g-two".into(), abs2.clone())
        .expect("publish redirects base");
    assert_eq!(registry.selected_sequence(), seq2);
    assert_eq!(registry.transition_count(), 1);

    // NEW READER: sees generation 2.
    assert_eq!(new_lease.publication_sequence(), seq2);
    assert_eq!(lease_names(&new_lease), generation_names(&abs2));

    // OLD READER still finishes against generation 1's immutable bytes — the
    // publication did NOT modify/delete the old generation while leased.
    assert_eq!(old_lease.publication_sequence(), seq1);
    assert_eq!(lease_names(&old_lease), generation_names(&abs1));
    assert!(
        old_weak.upgrade().is_some(),
        "old mapping survives while the old reader's lease is held"
    );

    // OLD READER COMPLETES: drop its lease. The registry's strong ref was
    // already replaced by the swap, so the old mapping's last strong ref is
    // the old reader's lease.
    let old_strong_before = old_lease.strong_count();
    assert!(
        old_strong_before >= 1,
        "lease holds at least its own strong ref"
    );
    drop(old_lease);

    // FINAL MAPPING RELEASE: with the lease dropped and the registry no longer
    // holding a strong ref, the old base's mapping is released — proven by the
    // weak probe now failing to upgrade. This is the drop evidence the packet
    // requires (an ArcSwap::swap or changed generation ID alone is not proof).
    assert!(
        old_weak.upgrade().is_none(),
        "old mapping released once the last lease drops (weak evidence)"
    );

    // The OLD generation's bytes on disk are still intact and readable — we
    // never modified or deleted them (packet requirement 3).
    assert_eq!(generation_names(&abs1), vec!["one-a", "one-b"]);
}

/// F1 regression: `selected_stats` must never panic and never tear (return
/// fields describing two different bases) under a concurrent publish. Many
/// reader threads call `selected_stats` in a tight loop while the publisher
/// swaps the selected base; every returned snapshot must be internally
/// consistent (its sequence matches a base it could have served).
#[test]
fn selected_stats_is_consistent_under_concurrent_publish() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let (seq1, abs1) = publish(&db, &gen_root, "g-one");
    populate(&db, "two");
    let (seq2, abs2) = publish(&db, &gen_root, "g-two");

    let registry = GenerationLeaseRegistry::from_selected(seq1, "g-one".into(), abs1)
        .expect("select first base");

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    for _ in 0..8 {
        let reg = std::sync::Arc::clone(&registry);
        let stop = std::sync::Arc::clone(&stop);
        readers.push(std::thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let stats = reg.selected_stats();
                // Consistency invariant: the returned (sequence, path) pair
                // must be a VALID pair for some single generation. Generation
                // files are named `g-{seq:020}-{sha}.grafeo`, so the path must
                // embed the same sequence the stats report. A torn read (seq
                // from one base, path from another) violates this. A
                // stale-but-consistent read (an older seq with its own path)
                // is legitimate under a racing publish.
                let expected_prefix = format!("g-{:020}-", stats.publication_sequence);
                let file_name = stats
                    .generation_abs_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");
                assert!(
                    file_name.starts_with(&expected_prefix),
                    "torn seq/path: seq {} but file {file_name}",
                    stats.publication_sequence
                );
                // NOTE: `strong_refs` is NOT asserted here. A stale-but-
                // consistent retired base can legitimately report 0 refs once
                // its mapping is released; the consistency invariant is the
                // (sequence, path) pair validity above.
            }
        }));
    }

    // Race the publish against the readers.
    let _new = registry
        .publish(seq2, "g-two".into(), abs2)
        .expect("publish");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in readers {
        r.join().expect("reader thread did not panic");
    }
}

// ---------------------------------------------------------------------------
// 2. Three reference classes are distinct
// ---------------------------------------------------------------------------

/// The database-owner reference, the external read-snapshot reference, and the
/// selected-manifest identity are tracked independently: the owner can release
/// a retired base while a snapshot keeps it alive, and the manifest sequence
/// (selected on disk) is the lease's identity, not a strong reference.
#[test]
fn reference_classes_are_independently_tracked() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let (seq1, abs1) = publish(&db, &gen_root, "g-one");
    populate(&db, "two");
    let (seq2, abs2) = publish(&db, &gen_root, "g-two");

    let registry = GenerationLeaseRegistry::from_selected(seq1, "g-one".into(), abs1.clone())
        .expect("select first base");

    // Owner holds exactly one strong ref to the selected base; no snapshots yet.
    let owner_selected = registry.selected_stats();
    assert_eq!(owner_selected.publication_sequence, seq1);
    assert!(owner_selected.is_selected);
    assert_eq!(
        owner_selected.strong_refs, 1,
        "owner alone holds the selected base before any snapshot"
    );

    // An external read-snapshot adds a second, independent strong ref.
    let snap = registry.snapshot();
    assert_eq!(registry.selected_stats().strong_refs, 2);
    assert_eq!(snap.strong_count(), 2);

    // Publish: the owner's strong ref moves to generation 2. The snapshot's
    // strong ref to generation 1 survives independently of the owner.
    let _new = registry
        .publish(seq2, "g-two".into(), abs2.clone())
        .expect("publish");
    let weak1 = snap.downgrade();
    assert!(
        weak1.upgrade().is_some(),
        "snapshot keeps old base alive after owner released it"
    );
    assert_eq!(
        snap.strong_count(),
        1,
        "only the snapshot holds the retired base now"
    );

    // Observability does NOT hold a strong reference: reporting previous stats
    // must not resurrect or extend the retired base.
    let prev = registry
        .previous_stats()
        .expect("previous generation tracked");
    assert_eq!(prev.publication_sequence, seq1);
    assert!(!prev.is_selected);
    assert_eq!(prev.strong_refs, 1, "only the live snapshot holds it");

    // Drop the snapshot: the retired base is fully released.
    drop(snap);
    assert!(weak1.upgrade().is_none());
    assert_eq!(
        registry
            .previous_stats()
            .expect("still tracked")
            .strong_refs,
        0,
        "retired base fully released after snapshot drop"
    );
}

// ---------------------------------------------------------------------------
// 3. Old generation never modified/deleted while leased
// ---------------------------------------------------------------------------

/// Byte-level proof: publishing generation 2 while a lease on generation 1 is
/// held leaves generation 1's file untouched (same length + same readable
/// contents), so a Windows unmap/delete is structurally never attempted.
#[test]
fn publication_leaves_leased_old_generation_bytes_untouched() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let (seq1, abs1) = publish(&db, &gen_root, "g-one");
    populate(&db, "two");
    let (seq2, abs2) = publish(&db, &gen_root, "g-two");

    let len_before = std::fs::metadata(&abs1).expect("gen1 exists").len();

    let registry = GenerationLeaseRegistry::from_selected(seq1, "g-one".into(), abs1.clone())
        .expect("select first base");
    let old_lease = registry.snapshot();

    // Publish generation 2 while the old lease is held.
    let _new = registry
        .publish(seq2, "g-two".into(), abs2.clone())
        .expect("publish");

    // Generation 1's file is byte-identical (length + contents) and still
    // readable — never modified or deleted while leased.
    let len_after = std::fs::metadata(&abs1).expect("gen1 still present").len();
    assert_eq!(len_before, len_after, "old generation file not rewritten");
    assert!(abs1.exists(), "old generation file not deleted");
    assert_eq!(lease_names(&old_lease), generation_names(&abs1));

    drop(old_lease);
}

// ---------------------------------------------------------------------------
// 4. Lease counts without observability-only strong references
// ---------------------------------------------------------------------------

/// Live lease counts are observable for selected and previous generations;
/// the act of observing never keeps a retired base alive.
#[test]
fn lease_counts_exposed_without_strong_observability_refs() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let (seq1, abs1) = publish(&db, &gen_root, "g-one");
    populate(&db, "two");
    let (seq2, abs2) = publish(&db, &gen_root, "g-two");

    let registry = GenerationLeaseRegistry::from_selected(seq1, "g-one".into(), abs1.clone())
        .expect("select first base");
    let old_lease = registry.snapshot();
    let old_weak = old_lease.downgrade();

    let _new = registry
        .publish(seq2, "g-two".into(), abs2.clone())
        .expect("publish");

    // Two generations tracked: selected (seq2) + previous (seq1).
    let stats = registry.lease_stats();
    assert_eq!(stats.len(), 2);
    let selected = stats.iter().find(|s| s.is_selected).expect("selected");
    let previous = stats.iter().find(|s| !s.is_selected).expect("previous");
    assert_eq!(selected.publication_sequence, seq2);
    assert_eq!(previous.publication_sequence, seq1);
    assert_eq!(previous.strong_refs, 1, "only the old lease holds it");

    // Drop the old lease: previous count falls to zero and the mapping is
    // released — the earlier stats calls never held a strong ref that would
    // keep it alive.
    drop(old_lease);
    assert!(old_weak.upgrade().is_none());
    let stats_after = registry.lease_stats();
    let previous_after = stats_after
        .iter()
        .find(|s| !s.is_selected)
        .expect("previous still tracked");
    assert_eq!(previous_after.strong_refs, 0);
}

// ---------------------------------------------------------------------------
// 5. Checkpoint/close returns transition errors; Drop is best-effort
// ---------------------------------------------------------------------------

/// `close_transition` marks the registry closed and returns the transition
/// report; a publish after close is a typed transition error. `Drop` is never
/// the tested success path (it cannot return an error).
#[test]
fn close_transition_returns_report_and_rejects_later_publish() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let (seq1, abs1) = publish(&db, &gen_root, "g-one");
    populate(&db, "two");
    let (seq2, abs2) = publish(&db, &gen_root, "g-two");

    let registry = GenerationLeaseRegistry::from_selected(seq1, "g-one".into(), abs1.clone())
        .expect("select first base");
    let _new = registry
        .publish(seq2, "g-two".into(), abs2.clone())
        .expect("publish");

    // Checkpoint is a non-closing validation: report is produced, registry
    // stays open, and the retired base (no live snapshot) is already released.
    let checkpoint = registry.checkpoint_transition().expect("checkpoint");
    assert_eq!(checkpoint.transitions, 1);
    assert!(
        checkpoint.all_retired_released(),
        "no live snapshots on the retired base"
    );

    // Close returns the transition report and marks closed.
    let report = registry.close_transition().expect("close transition");
    assert_eq!(report.transitions, 1);

    // A publish after close is a typed transition error, not a silent no-op.
    let err = registry
        .publish(seq2 + 1, "g-three".into(), abs2.clone())
        .expect_err("publish after close must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("closed generation lease registry"),
        "typed transition error, got: {msg}"
    );
}

/// Closing while a read snapshot is still alive must still succeed (close is
/// about the transition surface, not about forcing readers off), and the
/// held snapshot keeps its base's mapping alive past close.
#[test]
fn close_with_live_snapshot_keeps_snapshot_readable() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let (seq1, abs1) = publish(&db, &gen_root, "g-one");

    let registry = GenerationLeaseRegistry::from_selected(seq1, "g-one".into(), abs1.clone())
        .expect("select first base");
    let lease = registry.snapshot();
    let weak = lease.downgrade();

    let report = registry.close_transition().expect("close");
    // The selected base is still held by `lease`, so it is not "released".
    assert_eq!(report.transitions, 0);

    // The held snapshot remains readable after close.
    assert_eq!(lease_names(&lease), generation_names(&abs1));
    assert!(weak.upgrade().is_some());

    // Graceful close never force-unmaps a base under a live reader: the
    // registry still holds its owner strong ref, so dropping the snapshot
    // alone does NOT release the mapping.
    drop(lease);
    assert!(
        weak.upgrade().is_some(),
        "close is graceful: the owner reference keeps the base mapped"
    );

    // The mapping is released only when the registry itself (the owner) drops.
    drop(registry);
    assert!(
        weak.upgrade().is_none(),
        "mapping released once the owner registry drops"
    );
}

// ---------------------------------------------------------------------------
// Recovery integration: the lease wraps the validated selected generation
// ---------------------------------------------------------------------------

/// The registry's selected base is exactly the generation 3c recovery
/// selected and validated — identity (sequence) and content agree.
#[test]
fn lease_selected_base_matches_recovered_generation() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("live.grafeo.d");
    std::fs::create_dir_all(&gen_root).unwrap();

    let db = GrafeoDB::new_in_memory();
    populate(&db, "one");
    let (_s1, _a1) = publish(&db, &gen_root, "g-one");
    populate(&db, "two");
    let (_s2, _a2) = publish(&db, &gen_root, "g-two");

    // 3c recovery selects + validates the newest generation.
    let recovery = recover_generation_root(&gen_root).expect("recover");
    let selected_seq = recovery.selected.slot.publication_sequence;
    let selected_abs = recovery.selected.generation_abs_path.clone();
    let selected_id = recovery.selected.slot.generation_id.clone();
    let registry =
        GenerationLeaseRegistry::from_selected(selected_seq, selected_id, selected_abs.clone())
            .expect("select recovered base");

    let lease = registry.snapshot();
    assert_eq!(lease.publication_sequence(), selected_seq);
    assert_eq!(lease_names(&lease), generation_names(&selected_abs));

    // Recovery lock is released when `recovery` drops at scope end.
    drop(recovery);
    drop(lease);
}

/// Arc must be Send+Sync so leases can move across query worker threads.
#[test]
fn lease_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<grafeo_engine::GenerationLease>();
    assert_send_sync::<GenerationLeaseRegistry>();
}
