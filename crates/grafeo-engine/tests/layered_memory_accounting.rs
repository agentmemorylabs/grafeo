//! ENGINE-MEMORY-ACCOUNTING.1 — layered-base accounting regression.
//!
//! Guards the fix in `GrafeoDB::memory_usage()`: after `compact()` the DB
//! becomes layered (`lpg_store()` = overlay only), and the decoded
//! CompactStore base is real anonymous residency that must appear in
//! `StoreMemory::compact_base_bytes`. Before the fix, `memory_usage()`
//! summed only the overlay and silently dropped the whole base
//! (~700 MB on the frontier account graph, 2026-08-19 measurement).
//!
//! ```bash
//! cargo test -p grafeo-engine --features "compact-store,lpg" \
//!   --test layered_memory_accounting -- --nocapture
//! ```

#![cfg(all(feature = "compact-store", feature = "lpg"))]

use grafeo_common::types::Value;
use grafeo_engine::GrafeoDB;

const NODES: usize = 4_000;
const LONG_STRING_LEN: usize = 512;

fn build_graph(db: &GrafeoDB) {
    // Long heap-owned String payloads make the base non-trivial and prove
    // the accounting carries real data volume, not just a boolean flag.
    let pad = "x".repeat(LONG_STRING_LEN);
    let mut ids = Vec::with_capacity(NODES);
    for i in 0..NODES {
        let id = db.create_node(&["AccountingProbe"]).expect("create node");
        db.set_node_property(id, "idx", Value::Int64(i as i64))
            .expect("set idx");
        db.set_node_property(id, "payload", Value::String(format!("{pad}-{i}").into()))
            .expect("set payload");
        ids.push(id);
    }
    for pair in ids.windows(2) {
        let _edge_id = db.create_edge(pair[0], pair[1], "LINK");
    }
}

#[test]
fn memory_usage_counts_compact_base_after_compact() {
    let mut db = GrafeoDB::new_in_memory();
    build_graph(&db);

    // Pre-compact: pure LpgStore, no layered base.
    let before = db.memory_usage();
    assert_eq!(
        before.store.compact_base_bytes, 0,
        "non-layered DB must report zero compact base"
    );
    assert!(db.layered_store().is_none());

    db.compact().expect("compact");
    assert!(db.layered_store().is_some(), "compact() must layer the DB");

    let after = db.memory_usage();
    assert!(
        after.store.compact_base_bytes > 0,
        "layered DB must charge the decoded CompactStore base, got 0 \
         (the ENGINE-MEMORY-ACCOUNTING.1 regression)"
    );

    // NOTE: total_bytes may legitimately DROP after compact — the base is
    // columnar + dictionary-encoded and drops per-row MVCC/version-chain and
    // HashMap overhead. Truthful accounting reflects that; do not assert
    // monotonic totals. (Observed: 7.9 MB -> 3.2 MB on this fixture.)

    // The base must carry real payload volume: 4k nodes x ~512-byte strings.
    let min_expected = NODES * (LONG_STRING_LEN / 2);
    assert!(
        after.store.compact_base_bytes >= min_expected,
        "compact_base_bytes {} is too small for {} long-string nodes (min {})",
        after.store.compact_base_bytes,
        NODES,
        min_expected
    );

    // Overlay is additive on top of the base: a post-compact write must
    // appear in overlay accounting while the base stays charged.
    let new_id = db
        .create_node(&["AccountingProbe"])
        .expect("post-compact node");
    let pad = "y".repeat(LONG_STRING_LEN);
    db.set_node_property(new_id, "payload", Value::String(pad.into()))
        .expect("post-compact property");
    let with_overlay = db.memory_usage();
    assert_eq!(
        with_overlay.store.compact_base_bytes, after.store.compact_base_bytes,
        "overlay writes must not disturb base accounting"
    );
    assert!(
        with_overlay.store.total_bytes > after.store.total_bytes,
        "post-compact overlay write must add to store accounting ({} -> {})",
        after.store.total_bytes,
        with_overlay.store.total_bytes
    );

    println!(
        "ACCOUNTING GUARD OK: compact_base_bytes={} total_before={} total_after={} total_with_overlay={}",
        after.store.compact_base_bytes,
        before.total_bytes,
        after.total_bytes,
        with_overlay.store.total_bytes
    );
}

#[test]
fn detailed_stats_reflects_compact_base() {
    let mut db = GrafeoDB::new_in_memory();
    build_graph(&db);
    db.compact().expect("compact");
    let stats = db.detailed_stats();
    let usage = db.memory_usage();
    assert_eq!(
        stats.memory_bytes, usage.total_bytes,
        "detailed_stats().memory_bytes must flow from memory_usage().total_bytes"
    );
    assert!(stats.memory_bytes > usage.store.compact_base_bytes);
}
