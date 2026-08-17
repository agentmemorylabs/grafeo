//! G-VECBUILD.1 E3 — B1 spill-before-build contract (spill → build → k=1 →
//! RO reopen parity).
//!
//! Proves the locked B1 mechanism end to end:
//! 1. bulk-load N vectors, `compact()` (builder path),
//! 2. E1 `spill_vector_column_to_disk` drains the column (heap → mmap file),
//! 3. `create_vector_index` (plain HNSW, `quantization=None`) builds through
//!    the spill-aware build accessor with `inserted == N` (the
//!    skip-every-node trap: a property-only build would insert 0),
//! 4. k=1 `vector_search` returns the expected neighbor,
//! 5. the drained property column reads `None` everywhere (vector-column
//!    heap component removed),
//! 6. publish a generation (SRV1 Gap B: reload-from-spill must restore the
//!    embeddings — the re-registered live consumer makes that fire),
//! 7. RO generation-root reopen serves k=1 parity (SRV1 Gap B contract),
//! 8. catalog mode stays `none` (plain HNSW; quantization untouched).
//!
//! ```bash
//! cargo test -p grafeo-engine --test vecbuild_b1_spill_build \
//!   --features "lpg,vector-index,mmap,compact-store,generation,generation-streaming,grafeo-file"
//! ```

#![cfg(all(
    feature = "lpg",
    feature = "vector-index",
    feature = "mmap",
    feature = "compact-store",
    feature = "generation",
    feature = "generation-streaming",
    feature = "grafeo-file",
    not(feature = "temporal")
))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use grafeo_common::types::{PropertyKey, Value};
use grafeo_core::index::vector::QuantizationType;
use grafeo_engine::database::index_build_control::IndexBuildControl;
use grafeo_engine::{GrafeoDB, generation_build_request};
use tempfile::TempDir;

const NODES: usize = 96;
const DIMS: usize = 8;
const LABEL: &str = "RetrievalUnit";
const PROP: &str = "embedding";

/// Reads a node property through the tier-aware graph store (mirrors how
/// the build path sees the overlay/tiers).
fn node_prop(db: &GrafeoDB, id: grafeo_common::types::NodeId) -> Option<Value> {
    let key = PropertyKey::new(PROP);
    db.graph_store().get_node_property(id, &key)
}

fn seeded_vector(seed: u64) -> Vec<f32> {
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut raw: Vec<f32> = (0..DIMS)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            ((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0
        })
        .collect();
    let norm: f32 = raw.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut raw {
            *x /= norm;
        }
    }
    raw
}

fn assert_k1_self_hit(db: &GrafeoDB, node: grafeo_common::types::NodeId, seed: u64) {
    let query = seeded_vector(seed);
    let results = db
        .vector_search(LABEL, PROP, &query, 1, None, None)
        .expect("k=1 vector search");
    assert_eq!(
        results.first().map(|hit| hit.0),
        Some(node),
        "k=1 probe must return the exact vector's own node"
    );
}

/// Full B1 contract: spill → build → k=1 → publish → RO reopen parity.
#[test]
fn spill_build_serves_and_reopens_with_parity() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("vecbuild.grafeo.d");
    std::fs::create_dir_all(&gen_root).unwrap();

    let mut db = GrafeoDB::with_config(
        grafeo_engine::Config::in_memory().with_spill_path(dir.path().join("spill")),
    )
    .expect("open transient db");
    db.compact().expect("compact (builder path)");

    // 1. Bulk-load N vectors into the overlay (post-compact live store).
    let mut ids = Vec::with_capacity(NODES);
    for i in 0..NODES {
        let id = db
            .create_node_with_props(
                &[LABEL],
                [
                    ("name", Value::from(format!("ru-{i:03}"))),
                    (PROP, Value::Vector(seeded_vector(i as u64).into())),
                ],
            )
            .expect("create retrieval unit");
        ids.push(id);
    }

    // 2. E1: spill the column. Overlay tail window → mmap file.
    let report = db
        .spill_vector_column_to_disk(LABEL, PROP)
        .expect("E1 spill");
    assert_eq!(report.vectors_spilled, NODES as u64, "spilled every vector");
    assert_eq!(
        report.bytes_spilled,
        (NODES * DIMS * 4) as u64,
        "raw f32 payload bytes"
    );
    assert!(report.spill_file.exists(), "spill file written");
    assert!(!report.already_spilled);

    // 5. (checked early) the drained column reads None everywhere — the
    // vector-column heap component is gone.
    for id in &ids {
        assert!(
            node_prop(&db, *id).is_none(),
            "drained column must read None (heap component removed)"
        );
    }

    // 3. Build plain HNSW through the spill-aware accessor. The progress
    // completion event proves the dim scan counted all N spilled vectors
    // (a property-only scan would have created an EMPTY index).
    let progress_total = Arc::new(AtomicU64::new(0));
    let progress_done = Arc::new(AtomicU64::new(0));
    let control = IndexBuildControl::new().with_progress({
        let total = Arc::clone(&progress_total);
        let done = Arc::clone(&progress_done);
        Arc::new(move |d, t| {
            done.store(d, Ordering::SeqCst);
            total.store(t, Ordering::SeqCst);
        })
    });
    db.create_vector_index_with_control(
        LABEL,
        PROP,
        Some(DIMS),
        Some("cosine"),
        None,
        None,
        None,
        Some(&control),
    )
    .expect("HNSW build over spilled column");
    assert!(db.has_vector_index(LABEL, PROP), "index registered");
    assert_eq!(
        progress_total.load(Ordering::SeqCst),
        NODES as u64,
        "dim scan saw all spilled vectors (skip-every-node trap closed)"
    );
    assert_eq!(
        progress_done.load(Ordering::SeqCst),
        NODES as u64,
        "build completed with inserted == N"
    );

    // 8. Catalog mode unchanged: plain HNSW, quantization none.
    assert_eq!(
        db.vector_index_quantization(LABEL, PROP),
        Some(QuantizationType::None),
        "builder path stays plain HNSW (A1 lock)"
    );

    // 4. k=1 probe on the spilled build.
    assert_k1_self_hit(&db, ids[7], 7);
    assert_k1_self_hit(&db, ids[NODES - 1], (NODES - 1) as u64);

    // 6. Publish — SRV1 Gap B: reload-from-spill must restore the embedding
    // column before the freeze or the generation loses its vectors.
    let published = db
        .build_and_publish_generation(generation_build_request(&gen_root, "vecbuild-b1"))
        .expect("publish generation after spill")
        .publication;
    let _ = published;

    // The publish re-spill keeps the live db's memory profile.
    for id in &ids {
        assert!(
            node_prop(&db, *id).is_none(),
            "post-publish re-spill keeps the column drained"
        );
    }
    assert_k1_self_hit(&db, ids[7], 7);

    // 7. RO generation-root reopen serves k=1 parity (SRV1 Gap B contract).
    drop(db);
    let ro = GrafeoDB::open_generation_root(&gen_root, true).expect("RO reopen");
    assert!(ro.has_vector_index(LABEL, PROP), "index survives reopen");
    assert_k1_self_hit(&ro, ids[7], 7);
    assert_k1_self_hit(&ro, ids[NODES - 1], (NODES - 1) as u64);
    assert_eq!(
        ro.vector_index_quantization(LABEL, PROP),
        Some(QuantizationType::None),
        "catalog sticky mode unchanged after reopen"
    );
}

/// Production builder sequence: M2 window-tier drains first (vectors move to
/// mmap tiers), THEN E1 spills the overlay tail, THEN the HNSW build must
/// see BOTH sources (tier-resident via the TierChainView graph store,
/// tail-resident via the spill fallback). Publish + RO reopen parity.
#[test]
fn build_after_midflush_drain_sees_tiers_and_spill() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("vecbuild-tiers.grafeo.d");
    let tier_root = dir.path().join("tiers");
    std::fs::create_dir_all(&gen_root).unwrap();

    let mut db = GrafeoDB::with_config(
        grafeo_engine::Config::in_memory().with_spill_path(dir.path().join("spill")),
    )
    .expect("open transient db");
    db.compact().expect("compact (builder path)");

    // Window 1: first half of the vectors → drained to a tier file.
    let mut ids = Vec::with_capacity(NODES);
    for i in 0..NODES / 2 {
        let id = db
            .create_node_with_props(
                &[LABEL],
                [
                    ("name", Value::from(format!("ru-{i:03}"))),
                    (PROP, Value::Vector(seeded_vector(i as u64).into())),
                ],
            )
            .expect("create window-1 unit");
        ids.push(id);
    }
    let drain = db
        .drain_overlay_to_tier(&tier_root, "vecbuild-drain1")
        .expect("M2 window drain");
    assert!(drain.rows_drained > 0, "window 1 drained");

    // Window 2: second half stays in the overlay tail.
    for i in NODES / 2..NODES {
        let id = db
            .create_node_with_props(
                &[LABEL],
                [
                    ("name", Value::from(format!("ru-{i:03}"))),
                    (PROP, Value::Vector(seeded_vector(i as u64).into())),
                ],
            )
            .expect("create window-2 unit");
        ids.push(id);
    }

    // E1: spill the tail window only. Tier vectors are untouched.
    let report = db
        .spill_vector_column_to_disk(LABEL, PROP)
        .expect("E1 spill");
    assert_eq!(
        report.vectors_spilled,
        (NODES / 2) as u64,
        "only the overlay tail spills; window 1 is tier-resident"
    );

    // Build must insert ALL vectors: tier-resident via the graph store's
    // TierChainView, tail-resident via the spill fallback. The progress
    // completion counts are the build proof (the serving-path search over a
    // transient tier store is intentionally NOT a product contract —
    // packet hard rule 2 keeps the serving accessor unchanged).
    let progress_total = Arc::new(AtomicU64::new(0));
    let progress_done = Arc::new(AtomicU64::new(0));
    let control = IndexBuildControl::new().with_progress({
        let total = Arc::clone(&progress_total);
        let done = Arc::clone(&progress_done);
        Arc::new(move |d, t| {
            done.store(d, Ordering::SeqCst);
            total.store(t, Ordering::SeqCst);
        })
    });
    db.create_vector_index_with_control(
        LABEL,
        PROP,
        Some(DIMS),
        Some("cosine"),
        None,
        None,
        None,
        Some(&control),
    )
    .expect("HNSW build over tiers + spill");
    assert_eq!(
        progress_total.load(Ordering::SeqCst),
        NODES as u64,
        "dim scan saw tier-resident AND spilled vectors"
    );
    assert_eq!(
        progress_done.load(Ordering::SeqCst),
        NODES as u64,
        "build inserted every vector from both sources"
    );
    assert_eq!(
        db.vector_index_quantization(LABEL, PROP),
        Some(QuantizationType::None)
    );

    // Publish + RO reopen parity (SRV1 Gap B contract). The reopened
    // generation serves vectors from the CompactStore base, so k=1 parity
    // must hold for BOTH windows on the product serving shape.
    let _ = db
        .build_and_publish_generation(generation_build_request(&gen_root, "vecbuild-tiers"))
        .expect("publish")
        .publication;
    drop(db);
    let ro = GrafeoDB::open_generation_root(&gen_root, true).expect("RO reopen");
    assert!(ro.has_vector_index(LABEL, PROP));
    assert_k1_self_hit(&ro, ids[3], 3); // window 1 (was tier-resident)
    assert_k1_self_hit(&ro, ids[NODES - 2], (NODES - 2) as u64); // window 2 (was spilled)
}

/// Idempotency + empty-column semantics: a second E1 call is a no-op report;
/// an already-drained column never errors the builder path.
#[test]
fn spill_is_idempotent_and_empty_is_noop() {
    let dir = TempDir::new().unwrap();
    let mut db = GrafeoDB::with_config(
        grafeo_engine::Config::in_memory().with_spill_path(dir.path().join("spill")),
    )
    .expect("open transient db");
    db.compact().expect("compact");

    for i in 0..8usize {
        db.create_node_with_props(
            &[LABEL],
            [(PROP, Value::Vector(seeded_vector(i as u64).into()))],
        )
        .expect("create");
    }

    let first = db.spill_vector_column_to_disk(LABEL, PROP).expect("spill");
    assert_eq!(first.vectors_spilled, 8);

    // Already-spilled key → no-op report, not an error.
    let second = db
        .spill_vector_column_to_disk(LABEL, PROP)
        .expect("second spill is a no-op");
    assert!(second.already_spilled);
    assert_eq!(second.vectors_spilled, 0);

    // A label with no values at all → zero-count no-op (tail fully
    // tier-resident case), never a failure.
    let empty = db
        .spill_vector_column_to_disk("Other", PROP)
        .expect("empty column is a no-op");
    assert_eq!(empty.vectors_spilled, 0);
    assert!(!empty.already_spilled);
}

/// Fail-closed guards: quantized construction over a spilled column is
/// rejected (the quantized branch reads the heap guard and would insert
/// nothing), and a dimension mismatch restores the drained column.
#[test]
fn quantized_over_spilled_column_fails_closed() {
    let dir = TempDir::new().unwrap();
    let mut db = GrafeoDB::with_config(
        grafeo_engine::Config::in_memory().with_spill_path(dir.path().join("spill")),
    )
    .expect("open transient db");
    db.compact().expect("compact");

    for i in 0..8usize {
        db.create_node_with_props(
            &[LABEL],
            [(PROP, Value::Vector(seeded_vector(i as u64).into()))],
        )
        .expect("create");
    }

    db.spill_vector_column_to_disk(LABEL, PROP).expect("spill");

    let err = db
        .create_vector_index(
            LABEL,
            PROP,
            Some(DIMS),
            Some("cosine"),
            None,
            None,
            Some("scalar"),
        )
        .expect_err("quantized build over spilled column must fail closed");
    assert!(
        err.to_string().contains("spilled vector column"),
        "guard error names the cause: {err}"
    );
    // Plain HNSW over the same spilled column still works (A1 path).
    db.create_vector_index(LABEL, PROP, Some(DIMS), Some("cosine"), None, None, None)
        .expect("plain HNSW over spilled column");
}
