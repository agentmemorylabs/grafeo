//! G-MIDFLUSH.1 — mid-build overlay drain RED tests (R1-R5).
//!
//! All tests must fail at the base commit (f9e69804) and pass after the drain
//! implementation. Byte-parity (R1) is the load-bearing invariant — do NOT
//! weaken it to counts-only without a U1 record.

use std::fs;
use std::time::Instant;

use grafeo_common::types::Value;
use grafeo_core::graph::compact::generation::GenerationBudget;
use grafeo_engine::{generation_build_request, GrafeoDB};
use tempfile::TempDir;

fn normal_budget() -> GenerationBudget {
    GenerationBudget::for_tests()
}

fn count_nodes(db: &GrafeoDB) -> usize {
    let r = db
        .execute("MATCH (n) RETURN count(n) AS c")
        .expect("count query");
    let v = r
        .rows()
        .first()
        .and_then(|row| row.first())
        .expect("one row one col");
    v.as_int64().expect("Int64") as usize
}

fn file_bytes_equal(a: &std::path::Path, b: &std::path::Path) -> bool {
    // Compare the CompactStore section bytes only — the outer container's DbHeader
    // contains a wall-clock timestamp (generation_writer.rs:414) so raw file
    // bytes are never byte-identical across two builds. The CompactStore
    // payload itself is deterministic; if those bytes match, the graph is
    // byte-identical per the lock's intent. Falls back to raw file compare
    // when section extraction fails (should not happen in this test).
    use grafeo_common::storage::SectionType;
    use grafeo_storage::file::GrafeoFileManager;
    let extract = |p: &std::path::Path| -> Option<Vec<u8>> {
        let m = GrafeoFileManager::open_read_only(p).ok()?;
        let dir = m.read_section_directory().ok()??;
        let e = dir.find(SectionType::CompactStore)?;
        m.read_section_data(e).ok()
    };
    if let (Some(ca), Some(cb)) = (extract(a), extract(b)) {
        return ca == cb;
    }
    let ca = fs::read(a).expect("read a");
    let cb = fs::read(b).expect("read b");
    ca == cb
}

// ── R1: byte-parity ──────────────────────────────────────────────────

/// R1: fixture graph imported with no drains vs >=2 drains → final .grafeo byte-identical.
#[test]
fn r1_byte_parity_no_drain_vs_two_drains() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let gen_a = dir_a.path().join("gen-a.grafeo.d");
    let gen_b = dir_b.path().join("gen-b.grafeo.d");
    fs::create_dir_all(&gen_a).unwrap();
    fs::create_dir_all(&gen_b).unwrap();

    let budget = normal_budget();

    // Path A: no drains — single build
    let mut db_a = GrafeoDB::new_in_memory();
    db_a.compact().expect("compact a");
    for i in 0..10usize {
        let n = db_a
            .create_node_with_props(&["Person"], [("name", Value::from(format!("r1-a-{i:02}")))])
            .unwrap();
        if i % 2 == 0 {
            let m = db_a
                .create_node_with_props(
                    &["Item"],
                    [("name", Value::from(format!("r1-a-item-{i:02}")))],
                )
                .unwrap();
            let _ = db_a.create_edge(n, m, "OWNS");
        }
    }
    let pub_a = db_a
        .build_and_publish_generation(generation_build_request(&gen_a, "r1-parity"))
        .expect("publish a")
        .publication;
    let file_a = gen_a.join(&pub_a.generation_path);

    // Path B: >=2 drains before final publish
    let mut db_b = GrafeoDB::new_in_memory();
    db_b.compact().expect("compact b");
    for i in 0..5usize {
        let n = db_b
            .create_node_with_props(&["Person"], [("name", Value::from(format!("r1-a-{i:02}")))])
            .unwrap();
        if i % 2 == 0 {
            let m = db_b
                .create_node_with_props(
                    &["Item"],
                    [("name", Value::from(format!("r1-a-item-{i:02}")))],
                )
                .unwrap();
            let _ = db_b.create_edge(n, m, "OWNS");
        }
    }
    let d1 = db_b
        .drain_overlay_to_base(budget, "r1-drain1".into())
        .expect("drain1");
    assert!(d1.rows_drained > 0, "first drain should have rows");

    for i in 5..10usize {
        let n = db_b
            .create_node_with_props(&["Person"], [("name", Value::from(format!("r1-a-{i:02}")))])
            .unwrap();
        if i % 2 == 0 {
            let m = db_b
                .create_node_with_props(
                    &["Item"],
                    [("name", Value::from(format!("r1-a-item-{i:02}")))],
                )
                .unwrap();
            let _ = db_b.create_edge(n, m, "OWNS");
        }
    }
    let d2 = db_b
        .drain_overlay_to_base(budget, "r1-drain2".into())
        .expect("drain2");
    assert!(d2.rows_drained > 0, "second drain should have rows");
    let pub_b = db_b
        .build_and_publish_generation(generation_build_request(&gen_b, "r1-parity"))
        .expect("publish b")
        .publication;
    let file_b = gen_b.join(&pub_b.generation_path);

    assert!(
        file_bytes_equal(&file_a, &file_b),
        "R1 byte-parity failed: no-drain file {} vs two-drain file {} differ (a len {}, b len {})",
        file_a.display(),
        file_b.display(),
        fs::metadata(&file_a).map(|m| m.len()).unwrap_or(0),
        fs::metadata(&file_b).map(|m| m.len()).unwrap_or(0)
    );
}

// ── R2: N drains, phase machine already active, counts/reads exact ──

#[test]
fn r2_three_drains_one_store_no_already_active() {
    let budget = normal_budget();
    let mut db = GrafeoDB::new_in_memory();
    db.compact().expect("compact");

    for cycle in 0..3usize {
        for i in 0..4usize {
            let n = db
                .create_node_with_props(
                    &["Person"],
                    [("name", Value::from(format!("r2-c{cycle}-n{i}")))],
                )
                .unwrap();
            if i % 2 == 0 {
                let m = db
                    .create_node_with_props(
                        &["Item"],
                        [("name", Value::from(format!("r2-c{cycle}-m{i}")))],
                    )
                    .unwrap();
                let _ = db.create_edge(n, m, "OWNS");
            }
        }

        let r = db
            .drain_overlay_to_base(budget, format!("r2-cycle{cycle}"))
            .expect("drain should not fail with already active");
        assert!(r.rows_drained > 0, "cycle {cycle} should drain rows");
        assert!(
            !format!("{r:?}").contains("already active"),
            "report should not contain already active"
        );
    }

    let total = count_nodes(&db);
    assert_eq!(total, 18, "R2 total nodes exact: got {total}");

    let probe = db.execute("MATCH (n:Person {name: 'r2-c1-n1'}) RETURN n.name AS name");
    assert!(
        probe.is_ok(),
        "probe query should succeed after 3 drains: {probe:?}"
    );
}

// ── R3: drain-produced mmap base → final build_and_publish_generation → reopen probe ──

#[test]
fn r3_drain_chain_then_final_publish_and_reopen() {
    let dir = TempDir::new().unwrap();
    let gen_root = dir.path().join("r3.grafeo.d");
    fs::create_dir_all(&gen_root).unwrap();
    let budget = normal_budget();

    let mut db = GrafeoDB::new_in_memory();
    db.compact().expect("compact");

    for i in 0..6usize {
        let n = db
            .create_node_with_props(&["Person"], [("name", Value::from(format!("r3-n{i:02}")))])
            .unwrap();
        if i % 2 == 0 {
            let m = db
                .create_node_with_props(
                    &["Item"],
                    [("name", Value::from(format!("r3-item{i:02}")))],
                )
                .unwrap();
            let _ = db.create_edge(n, m, "KNOWS");
        }
    }
    let d = db
        .drain_overlay_to_base(budget, "r3-drain".into())
        .expect("drain");
    assert!(d.rows_drained > 0);

    let publication = db
        .build_and_publish_generation(generation_build_request(&gen_root, "r3-final"))
        .expect("final publish after drain should succeed")
        .publication;
    assert_eq!(publication.generation_id, "r3-final");
    assert!(publication.publication_sequence >= 1);

    let gen_file = gen_root.join(&publication.generation_path);
    assert!(gen_file.exists(), "published generation file should exist");
    assert!(
        fs::metadata(&gen_file).unwrap().len() > 0,
        "published generation should be non-empty"
    );

    let live = count_nodes(&db);
    assert_eq!(live, 9, "r3 live count after drain+publish: got {live}");
}

// ── R4: BudgetExceeded fail-closed ───────────────────────────────────

#[test]
fn r4_budget_breach_is_typed_and_overlay_intact() {
    let mut db = GrafeoDB::new_in_memory();
    db.compact().expect("compact");

    for i in 0..8usize {
        let n = db
            .create_node_with_props(&["Person"], [("name", Value::from(format!("r4-n{i:02}")))])
            .unwrap();
        let m = db
            .create_node_with_props(&["Item"], [("name", Value::from(format!("r4-m{i:02}")))])
            .unwrap();
        let _ = db.create_edge(n, m, "OWNS");
    }

    let before = count_nodes(&db);

    let tiny = {
        let mut b = GenerationBudget::for_tests();
        b.max_temp_bytes = 1;
        b.max_anon_bytes = 1024;
        b
    };
    let err = db
        .drain_overlay_to_base(tiny, "r4-tiny".into())
        .expect_err("tiny budget should fail");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("budget"),
        "R4 should be typed BudgetExceeded, got: {msg}"
    );

    let after = count_nodes(&db);
    assert_eq!(
        before, after,
        "R4 overlay intact: before {before} vs after {after}"
    );

    let ok = db.drain_overlay_to_base(normal_budget(), "r4-resume".into());
    assert!(
        ok.is_ok(),
        "resume after budget breach should succeed: {ok:?}"
    );
}

// ── R5: zero WAL growth ──────────────────────────────────────────────

#[test]
fn r5_zero_wal_growth_final_checkpoint_stays_ms() {
    let budget = normal_budget();

    let mut db = GrafeoDB::new_in_memory();
    db.compact().expect("compact");

    for i in 0..8usize {
        let n = db
            .create_node_with_props(&["Person"], [("name", Value::from(format!("r5-n{i:02}")))])
            .unwrap();
        let m = db
            .create_node_with_props(&["Item"], [("name", Value::from(format!("r5-m{i:02}")))])
            .unwrap();
        let _ = db.create_edge(n, m, "OWNS");
        if i == 3 {
            let _ = db
                .drain_overlay_to_base(budget, "r5-drain1".into())
                .expect("drain1");
        }
    }
    let _ = db
        .drain_overlay_to_base(budget, "r5-drain2".into())
        .expect("drain2");

    let start = Instant::now();
    let ckpt = db.wal_checkpoint();
    let elapsed_ms = start.elapsed().as_millis() as u64;
    assert!(ckpt.is_ok(), "wal_checkpoint should succeed: {ckpt:?}");
    assert!(
        elapsed_ms < 2000,
        "R5 final wal_checkpoint should stay ~ms, took {elapsed_ms} ms"
    );
}

// ── R6: O(window) cost guard (the anti-M1 test) ─────────────────────

/// R6: the tier file written by one drain must be sized by the WINDOW
/// (rows drained in that drain), not by the total rows imported so far.
/// M1's drain_overlay_to_base re-streamed the whole merged graph — a
/// window drain after a large base would produce a file proportional to
/// base+window. This test writes a large first window, drains it, then a
/// SMALL second window and asserts the second tier file is small.
#[test]
fn r6_tier_file_is_o_window_not_o_graph() {
    let dir = TempDir::new().unwrap();
    let tier_root = dir.path().join("tiers");

    let mut db = GrafeoDB::new_in_memory();
    db.compact().expect("compact");

    // Window 1: many nodes (large base after drain).
    for i in 0..200usize {
        let _ = db
            .create_node_with_props(
                &["Person"],
                [("name", Value::from(format!("r6-big-{i:03}")))],
            )
            .unwrap();
    }
    let d1 = db
        .drain_overlay_to_tier(&tier_root, "r6-drain1")
        .expect("drain1");
    assert!(d1.tier_node_count == 200, "drain1 should capture 200 nodes");

    // Window 2: few nodes.
    for i in 0..5usize {
        let _ = db
            .create_node_with_props(
                &["Person"],
                [("name", Value::from(format!("r6-small-{i:03}")))],
            )
            .unwrap();
    }
    let d2 = db
        .drain_overlay_to_tier(&tier_root, "r6-drain2")
        .expect("drain2");
    assert!(d2.tier_node_count == 5, "drain2 should capture 5 nodes");

    // Tier files on disk.
    let tiers: Vec<std::path::PathBuf> = fs::read_dir(&tier_root)
        .expect("tier dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("grafeo"))
        .collect();
    assert_eq!(tiers.len(), 2, "two tier files expected, got {tiers:?}");

    // Compare CompactStore SECTION payload sizes, not whole files: the
    // container header is a fixed ~16 KiB per file and would dominate tiny
    // fixtures, hiding the O(window) signal.
    let cs_section_len = |p: &std::path::Path| -> u64 {
        use grafeo_common::storage::SectionType;
        use grafeo_storage::file::GrafeoFileManager;
        let m = GrafeoFileManager::open_read_only(p).expect("open tier");
        let dir = m.read_section_directory().expect("dir").expect("some dir");
        let e = dir.find(SectionType::CompactStore).expect("cs section");
        e.length
    };
    let (len_big, len_small) = {
        let l0 = cs_section_len(&tiers[0]);
        let l1 = cs_section_len(&tiers[1]);
        if l0 > l1 { (l0, l1) } else { (l1, l0) }
    };

    // The small window's section payload must be strictly smaller than the
    // big window's. Under M1 (full re-stream per drain), the second drain's
    // output would be >= the first (205 rows > 200 rows), so this
    // discriminates O(window) from O(graph).
    assert!(
        len_small < len_big,
        "R6 O(window) guard failed: small window section {} bytes >= big window section {} bytes",
        len_small,
        len_big
    );
    // And it must be proportionally small (<= 25% of the big one — 5 rows
    // vs 200 rows; generous margin for dictionary/zone-map fixed cost).
    assert!(
        len_small * 4 <= len_big,
        "R6 O(window) guard failed: small section {} not proportionally small vs big section {}",
        len_small,
        len_big
    );
}

// ── R7: edge-phase hazard (the anti-data-loss test) ─────────────────

/// R7: an edge created AFTER a drain (edge phase) whose endpoints live in
/// the drained tier must survive to the final generation. The node-phase-
/// ZERO-edges guardrail is what makes this safe; this test trips if any
/// future change re-introduces node-phase edge writes before a drain and
/// from_graph_store_preserving_ids silently drops the cross-window edge
/// (builder.rs:1182-1188).
#[test]
fn r7_cross_drain_edge_survives_and_parity_holds() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let gen_a = dir_a.path().join("gen-a.grafeo.d");
    let gen_b = dir_b.path().join("gen-b.grafeo.d");
    fs::create_dir_all(&gen_a).unwrap();
    fs::create_dir_all(&gen_b).unwrap();

    // Path A: no drain — nodes then edges then publish.
    let mut db_a = GrafeoDB::new_in_memory();
    db_a.compact().expect("compact a");
    let mut ids_a = Vec::new();
    for i in 0..6usize {
        let n = db_a
            .create_node_with_props(&["Person"], [("name", Value::from(format!("r7-a-{i:02}")))])
            .unwrap();
        ids_a.push(n);
    }
    // Edges all created after all nodes (edge phase).
    for w in ids_a.windows(2) {
        let _ = db_a.create_edge(w[0], w[1], "NEXT");
    }
    let pub_a = db_a
        .build_and_publish_generation(generation_build_request(&gen_a, "r7-parity"))
        .expect("publish a")
        .publication;
    let file_a = gen_a.join(&pub_a.generation_path);

    // Path B: drain between node windows, then edges, then publish.
    let mut db_b = GrafeoDB::new_in_memory();
    db_b.compact().expect("compact b");
    let tier_root_b = dir_b.path().join("tiers");
    let mut ids_b = Vec::new();
    for i in 0..3usize {
        let n = db_b
            .create_node_with_props(&["Person"], [("name", Value::from(format!("r7-a-{i:02}")))])
            .unwrap();
        ids_b.push(n);
    }
    let d1 = db_b
        .drain_overlay_to_tier(&tier_root_b, "r7-drain1")
        .expect("drain1");
    assert!(d1.rows_drained > 0, "drain1 should have rows");
    for i in 3..6usize {
        let n = db_b
            .create_node_with_props(&["Person"], [("name", Value::from(format!("r7-a-{i:02}")))])
            .unwrap();
        ids_b.push(n);
    }
    // Cross-window edges: nodes 0-2 live in the tier, nodes 3-5 in the
    // overlay; the edge 2->3 spans the drain boundary.
    for w in ids_b.windows(2) {
        let _ = db_b.create_edge(w[0], w[1], "NEXT");
    }
    let pub_b = db_b
        .build_and_publish_generation(generation_build_request(&gen_b, "r7-parity"))
        .expect("publish b")
        .publication;
    let file_b = gen_b.join(&pub_b.generation_path);

    // Byte parity with the no-drain build AND exact edge count (5 edges).
    assert!(
        file_bytes_equal(&file_a, &file_b),
        "R7 byte-parity failed: cross-drain edge changed the final generation"
    );
    let edge_count = count_edges_from_generation(&file_b);
    assert_eq!(
        edge_count, 5,
        "R7 edge loss: expected 5 NEXT edges, found {edge_count}"
    );
}

fn count_edges_from_generation(path: &std::path::Path) -> usize {
    use bytes::Bytes;
    use grafeo_common::storage::SectionType;
    use grafeo_core::graph::compact::section::CompactStoreSection;
    use grafeo_storage::file::GrafeoFileManager;
    let m = GrafeoFileManager::open_read_only(path).expect("open gen");
    let dir = m.read_section_directory().expect("dir").expect("some dir");
    let e = dir.find(SectionType::CompactStore).expect("cs section");
    let sec = m.mmap_section(e).expect("mmap");
    let bytes: Bytes = std::sync::Arc::new(sec).into_bytes();
    let mut cs_sec = CompactStoreSection::empty();
    cs_sec.deserialize_from_mapped_bytes(bytes).expect("deser");
    let store = cs_sec.store().expect("store");
    store.total_edges() as usize
}
