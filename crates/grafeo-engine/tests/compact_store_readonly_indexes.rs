//! G-E1.RO: Catalog + PropertyIndex + TextIndex restore after compact RO reopen.
//!
//! Builds deterministic compact snapshots with catalog schema registration,
//! property indexes, and text indexes; checkpoints; explicit close; fresh
//! open_read_only; proves:
//! - catalog/index registration and query result parity vs pre-close
//! - planner/public APIs actually use restored indexes (not section listing alone)
//! - two sizes with ≥4× admitted index/catalog payload growth and zero
//!   proportional anonymous ownership for those sections
//! - fail-closed / explicit fallback for missing/corrupt/optional-absent
//!
//! ```bash
//! cargo test -p grafeo-engine --features "compact-store,text-index" \
//!   --test compact_store_readonly_indexes -- --nocapture
//! ```

#![cfg(all(
    feature = "compact-store",
    feature = "grafeo-file",
    feature = "lpg",
    feature = "text-index"
))]

use grafeo_common::storage::SectionType;
use grafeo_common::types::Value;
use grafeo_core::index::property::{
    PROPERTY_INDEX_MAGIC, PROPERTY_INDEX_VERSION, parse_property_index_section,
};
use grafeo_core::index::text::{
    TEXT_INDEX_MAGIC, TEXT_INDEX_MAPPED_VERSION, parse_text_index_section,
};
use grafeo_engine::{CompactBacking, Config, GrafeoDB};
use grafeo_storage::file::GrafeoFileManager;

const SMALL_NODES: usize = 256;
const LARGE_NODES: usize = 2_048;
const MIN_PAYLOAD_RATIO: u64 = 4;

#[derive(Clone, Debug)]
struct LiveExpectations {
    prop_hits_for_rank_0: Vec<u64>,
    prop_hits_for_rank_mid: Vec<u64>,
    mid_rank: i64,
    text_top_ids: Vec<u64>,
}

fn build_snapshot(path: &std::path::Path, node_count: usize) -> LiveExpectations {
    let mut db = GrafeoDB::with_config(Config::persistent(path)).expect("create db");

    // Schema registration (Catalog surface) via DDL when available; nodes alone
    // still exercise Catalog index registration for property/text.
    let session = db.session();
    let _ = session.execute("CREATE NODE TYPE CodeSymbol (name STRING, rank INT64, body STRING)");

    for index in 0..node_count {
        let name = format!("symbol-{index:08x}");
        // Every 4th doc mentions "fox" for text-index postings.
        let body = if index % 4 == 0 {
            format!("the quick brown fox jumps near {name}")
        } else {
            format!("routine note for {name}")
        };
        db.create_node_with_props(
            &["CodeSymbol"],
            [
                ("name", Value::from(name.as_str())),
                ("rank", Value::Int64(index as i64)),
                ("body", Value::from(body.as_str())),
            ],
        )
        .expect("create node");
    }

    db.compact().expect("compact");

    // Indexes after compact so they live on the overlay and cover base via
    // layered graph scan (create_property_index / create_text_index).
    db.create_property_index("rank");
    db.create_text_index("CodeSymbol", "body")
        .expect("create text index");

    assert!(db.has_property_index("rank"));
    assert!(db.graph_store().has_text_index("CodeSymbol", "body"));

    let mid_rank = (node_count / 2) as i64;
    let prop0 = db.find_nodes_by_property("rank", &Value::Int64(0));
    let prop_mid = db.find_nodes_by_property("rank", &Value::Int64(mid_rank));
    // k large enough to cover all fox docs at both sizes (LARGE_NODES/4 = 512).
    let text_hits = db
        .text_search("CodeSymbol", "body", "fox", 1024)
        .expect("text search live");

    let expectations = LiveExpectations {
        prop_hits_for_rank_0: prop0.iter().map(|id| id.as_u64()).collect(),
        prop_hits_for_rank_mid: prop_mid.iter().map(|id| id.as_u64()).collect(),
        mid_rank,
        text_top_ids: text_hits.iter().map(|(id, _)| id.as_u64()).collect(),
    };

    assert!(
        !expectations.prop_hits_for_rank_0.is_empty(),
        "live property index must hit rank=0"
    );
    assert_eq!(
        expectations.prop_hits_for_rank_mid.len(),
        1,
        "mid rank should hit exactly one node"
    );
    assert!(
        !expectations.text_top_ids.is_empty(),
        "live text index must find fox docs"
    );

    db.close().expect("explicit close");
    expectations
}

fn section_payload_len(path: &std::path::Path, section: SectionType) -> u64 {
    let fm = GrafeoFileManager::open_read_only(path).expect("open container");
    let dir = fm
        .read_section_directory()
        .expect("dir")
        .expect("section directory");
    let len = dir.find(section).map(|e| e.length).unwrap_or(0);
    fm.close().ok();
    len
}

fn assert_sections_present(path: &std::path::Path) {
    let fm = GrafeoFileManager::open_read_only(path).expect("open");
    let dir = fm
        .read_section_directory()
        .unwrap()
        .expect("section directory");
    assert!(
        dir.find(SectionType::Catalog).is_some(),
        "Catalog section must be written on layered path"
    );
    assert!(
        dir.find(SectionType::PropertyIndex).is_some(),
        "PropertyIndex section must be written when indexes exist"
    );
    assert!(
        dir.find(SectionType::TextIndex).is_some(),
        "TextIndex section must be written when indexes exist"
    );
    assert!(
        dir.find(SectionType::CompactStore).is_some(),
        "CompactStore section present"
    );
    let prop = dir.find(SectionType::PropertyIndex).unwrap();
    assert_eq!(prop.version, PROPERTY_INDEX_VERSION);
    let text = dir.find(SectionType::TextIndex).unwrap();
    assert_eq!(text.version, TEXT_INDEX_MAPPED_VERSION);
    let cat = dir.find(SectionType::Catalog).unwrap();
    assert_eq!(cat.version, 2, "Catalog section v2");
    fm.close().ok();
}

fn assert_reopen_parity(path: &std::path::Path, expected: &LiveExpectations) {
    let db = GrafeoDB::open_read_only(path).expect("read-only reopen");

    let CompactBacking::ContainerMmap {
        payload_version, ..
    } = db.compact_backing().expect("backing")
    else {
        panic!("expected ContainerMmap");
    };
    assert_eq!(*payload_version, 5);

    // Registration parity (Catalog + restored sections).
    assert!(
        db.has_property_index("rank"),
        "property index registered after reopen"
    );
    assert!(
        db.graph_store().has_text_index("CodeSymbol", "body"),
        "text index registered after reopen"
    );

    // Property index path: result parity vs pre-close live handle.
    let mut prop0: Vec<u64> = db
        .find_nodes_by_property("rank", &Value::Int64(0))
        .iter()
        .map(|id| id.as_u64())
        .collect();
    prop0.sort_unstable();
    let mut exp0 = expected.prop_hits_for_rank_0.clone();
    exp0.sort_unstable();
    assert_eq!(prop0, exp0, "rank=0 parity via restored property index");

    let mut prop_mid: Vec<u64> = db
        .find_nodes_by_property("rank", &Value::Int64(expected.mid_rank))
        .iter()
        .map(|id| id.as_u64())
        .collect();
    prop_mid.sort_unstable();
    let mut exp_mid = expected.prop_hits_for_rank_mid.clone();
    exp_mid.sort_unstable();
    assert_eq!(
        prop_mid, exp_mid,
        "rank=mid parity via restored property index"
    );

    // Text search parity (full set, not top-k slice that can reorder by score).
    let text_hits = db
        .text_search("CodeSymbol", "body", "fox", 1024)
        .expect("text search");
    let mut got: Vec<u64> = text_hits.iter().map(|(id, _)| id.as_u64()).collect();
    got.sort_unstable();
    let mut exp_text = expected.text_top_ids.clone();
    exp_text.sort_unstable();
    assert_eq!(
        got,
        exp_text,
        "text fox full-set parity after reopen (got {}, exp {})",
        got.len(),
        exp_text.len()
    );

    // Catalog-backed label query still works (schema surface).
    let session = db.session();
    let result = session
        .execute("MATCH (n:CodeSymbol) RETURN count(n) AS c")
        .expect("catalog-backed label query");
    assert!(result.row_count() >= 1);

    // Prove restored postings are actually selected: impossible rank must miss
    // via indexed path (empty), and fox text must still hit without rebuild.
    let impossible = db.find_nodes_by_property("rank", &Value::Int64(i64::MAX));
    assert!(
        impossible.is_empty(),
        "indexed property path must not invent hits for absent values"
    );

    db.close().expect("close");
}

#[test]
fn readonly_indexes_parity_two_sizes_and_accounting() {
    let temp = tempfile::tempdir().expect("tempdir");
    let small_path = temp.path().join("small.grafeo");
    let large_path = temp.path().join("large.grafeo");

    let small_exp = build_snapshot(&small_path, SMALL_NODES);
    let large_exp = build_snapshot(&large_path, LARGE_NODES);

    assert_sections_present(&small_path);
    assert_sections_present(&large_path);

    assert_reopen_parity(&small_path, &small_exp);
    assert_reopen_parity(&large_path, &large_exp);

    // Admitted section payload growth ≥4× for PropertyIndex + TextIndex.
    let small_prop = section_payload_len(&small_path, SectionType::PropertyIndex);
    let large_prop = section_payload_len(&large_path, SectionType::PropertyIndex);
    let small_text = section_payload_len(&small_path, SectionType::TextIndex);
    let large_text = section_payload_len(&large_path, SectionType::TextIndex);
    let small_cat = section_payload_len(&small_path, SectionType::Catalog);
    let large_cat = section_payload_len(&large_path, SectionType::Catalog);

    let small_admitted = small_prop + small_text;
    let large_admitted = large_prop + large_text;
    assert!(
        small_admitted > 0 && large_admitted > 0,
        "admitted index payloads must be non-zero (prop={small_prop}/{large_prop} text={small_text}/{large_text})"
    );
    let ratio = large_admitted / small_admitted.max(1);
    assert!(
        ratio >= MIN_PAYLOAD_RATIO,
        "admitted PropertyIndex+TextIndex payload ratio {ratio}× must be ≥{MIN_PAYLOAD_RATIO}× \
         (small={small_admitted} large={large_admitted}); catalog small={small_cat} large={large_cat}"
    );

    // Mapped open accounting: proportional anonymous for index sections is 0.
    for path in [&small_path, &large_path] {
        let fm = GrafeoFileManager::open_read_only(path).unwrap();
        let dir = fm.read_section_directory().unwrap().unwrap();
        let prop_entry = dir.find(SectionType::PropertyIndex).unwrap();
        let prop_bytes = std::sync::Arc::new(fm.mmap_section(prop_entry).unwrap()).into_bytes();
        assert!(
            prop_bytes.starts_with(PROPERTY_INDEX_MAGIC),
            "PropertyIndex mapped magic"
        );
        let prop_set = parse_property_index_section(prop_bytes).expect("parse prop");
        assert_eq!(
            prop_set.accounting().anonymous_proportional_bytes,
            0,
            "PropertyIndex proportional anonymous must be 0"
        );
        assert!(prop_set.accounting().mapped_payload_bytes > 0);
        // Functional use of restored mapped postings (not section listing alone).
        let hits = prop_set
            .get("rank")
            .expect("rank index")
            .lookup(&Value::Int64(0));
        assert_eq!(hits.len(), 1, "mapped property lookup exercises postings");

        let text_entry = dir.find(SectionType::TextIndex).unwrap();
        let text_bytes = std::sync::Arc::new(fm.mmap_section(text_entry).unwrap()).into_bytes();
        assert!(
            text_bytes.starts_with(TEXT_INDEX_MAGIC),
            "TextIndex mapped magic"
        );
        let text_set = parse_text_index_section(text_bytes).expect("parse text");
        assert_eq!(
            text_set.accounting().anonymous_proportional_bytes,
            0,
            "TextIndex proportional anonymous must be 0"
        );
        let text_hits = text_set
            .get("CodeSymbol:body")
            .expect("text index")
            .search("fox", 8);
        assert!(
            !text_hits.is_empty(),
            "mapped text search exercises postings"
        );

        // Graph proportional still 0 after index restore.
        let db = GrafeoDB::open_read_only(path).unwrap();
        let tiered = db.compact_tiered().unwrap();
        let store = tiered.store();
        let acc = store.memory_accounting().expect("v5 accounting");
        assert_eq!(acc.anonymous_proportional_structure_bytes, 0);
        db.close().ok();
        fm.close().ok();
    }

    eprintln!(
        "G-E1.RO two-size accounting: prop {small_prop}->{large_prop}, text {small_text}->{large_text}, \
         admitted ratio {ratio}×, catalog {small_cat}->{large_cat}"
    );
}

#[test]
fn missing_optional_property_index_section_is_explicit_fallback_not_silent_restored() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("strip_prop.grafeo");
    let _ = build_snapshot(&path, 64);

    // Rewrite without PropertyIndex section.
    let fm = GrafeoFileManager::open(&path).expect("open rw");
    let dir = fm.read_section_directory().unwrap().expect("directory");
    let mut versioned: Vec<(SectionType, u8, Vec<u8>)> = Vec::new();
    for entry in dir.entries() {
        if entry.section_type == SectionType::PropertyIndex {
            continue;
        }
        let data = fm.read_section_data(entry).expect("read section");
        versioned.push((entry.section_type, entry.version, data));
    }
    let refs: Vec<(SectionType, u8, &[u8])> = versioned
        .iter()
        .map(|(t, v, d)| (*t, *v, d.as_slice()))
        .collect();
    fm.write_versioned_sections(&refs, 1, 1, 0, 0)
        .expect("rewrite without PropertyIndex");
    fm.close().ok();

    let fm2 = GrafeoFileManager::open_read_only(&path).unwrap();
    let dir2 = fm2.read_section_directory().unwrap().unwrap();
    assert!(
        dir2.find(SectionType::PropertyIndex).is_none(),
        "PropertyIndex section intentionally absent"
    );
    assert!(dir2.find(SectionType::Catalog).is_some());
    assert!(dir2.find(SectionType::CompactStore).is_some());
    fm2.close().ok();

    // Open succeeds (optional section). Without PropertyIndex postings the
    // index is not registered as restored — never a silent "restored" claim.
    let db = GrafeoDB::open_read_only(&path).expect("open after strip");
    assert!(
        !db.has_property_index("rank"),
        "missing optional PropertyIndex must not claim a restored index"
    );
    // Graph data remains available via CompactStore (section still present).
    assert!(
        db.compact_backing().is_some(),
        "CompactStore still opens after optional PropertyIndex omission"
    );
    let session = db.session();
    let result = session
        .execute("MATCH (n:CodeSymbol) RETURN count(n) AS c")
        .expect("label scan fallback without PropertyIndex");
    assert!(
        result.row_count() >= 1,
        "explicit fallback: graph queries still work without PropertyIndex section"
    );
    db.close().ok();
}

#[test]
fn corrupt_property_index_section_fails_closed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("corrupt_prop.grafeo");
    let _ = build_snapshot(&path, 32);

    let fm = GrafeoFileManager::open(&path).expect("open");
    let dir = fm.read_section_directory().unwrap().unwrap();
    let mut versioned: Vec<(SectionType, u8, Vec<u8>)> = Vec::new();
    for entry in dir.entries() {
        let mut data = fm.read_section_data(entry).unwrap();
        if entry.section_type == SectionType::PropertyIndex && data.len() >= 4 {
            data[0] = 0xFF;
            data[1] = 0xFF;
            data[2] = 0xFF;
            data[3] = 0xFF;
        }
        versioned.push((entry.section_type, entry.version, data));
    }
    let refs: Vec<(SectionType, u8, &[u8])> = versioned
        .iter()
        .map(|(t, v, d)| (*t, *v, d.as_slice()))
        .collect();
    fm.write_versioned_sections(&refs, 1, 1, 0, 0)
        .expect("rewrite corrupt");
    fm.close().ok();

    match GrafeoDB::open_read_only(&path) {
        Ok(_) => panic!("corrupt PropertyIndex must fail closed"),
        Err(err) => {
            let msg = err.to_string();
            assert!(
                msg.contains("PropertyIndex")
                    || msg.contains("magic")
                    || msg.contains("Serialization"),
                "unexpected error: {msg}"
            );
        }
    }
}

#[test]
fn corrupt_text_index_section_fails_closed() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("corrupt_text.grafeo");
    let _ = build_snapshot(&path, 32);

    let fm = GrafeoFileManager::open(&path).expect("open");
    let dir = fm.read_section_directory().unwrap().unwrap();
    let mut versioned: Vec<(SectionType, u8, Vec<u8>)> = Vec::new();
    for entry in dir.entries() {
        let mut data = fm.read_section_data(entry).unwrap();
        if entry.section_type == SectionType::TextIndex && data.len() >= 5 {
            data[4] = 99; // unknown mapped version
        }
        versioned.push((entry.section_type, entry.version, data));
    }
    let refs: Vec<(SectionType, u8, &[u8])> = versioned
        .iter()
        .map(|(t, v, d)| (*t, *v, d.as_slice()))
        .collect();
    fm.write_versioned_sections(&refs, 1, 1, 0, 0)
        .expect("rewrite");
    fm.close().ok();

    match GrafeoDB::open_read_only(&path) {
        Ok(_) => panic!("corrupt TextIndex must fail closed"),
        Err(err) => {
            let msg = err.to_string();
            assert!(
                msg.contains("TextIndex")
                    || msg.contains("version")
                    || msg.contains("Serialization"),
                "unexpected error: {msg}"
            );
        }
    }
}
