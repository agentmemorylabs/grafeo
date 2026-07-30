//! G-E0.1: CompactStore payload v4 large-string persistence regression.
//!
//! Proves a production-shaped `CodeSymbol.documentation_json`-like string
//! (~96 KiB) survives: insert → `compact()` → explicit `close()` → reopen →
//! byte-for-byte property compare, with CompactStore section present and
//! payload header version 4.
//!
//! Explicit `close()` is the success path (returns the final checkpoint
//! error). Do not rely on `Drop`, and do not call `wal_checkpoint()` then
//! `close()` (double checkpoint).
//!
//! Directory-entry version equality is **not** asserted here — that is
//! `G-F0.1`. This test only covers E-0 payload persistence / integrity.
//!
//! ```bash
//! cargo test -p grafeo-engine --features compact-store \
//!   --test compact_store_large_string_persistence -- --nocapture
//! ```

#![cfg(all(feature = "compact-store", feature = "grafeo-file", feature = "lpg"))]

use grafeo_common::storage::SectionType;
use grafeo_common::types::{PropertyKey, Value};
use grafeo_core::graph::GraphStore;
use grafeo_engine::{Config, GrafeoDB};

const LARGE_STRING_LEN: usize = 96 * 1024;

/// Build a deterministic 96 KiB string (production-like documentation blob).
fn documentation_json_body() -> String {
    // Mix of printable ASCII so UTF-8 is trivial and truncation bugs are obvious.
    let pattern = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut body = String::with_capacity(LARGE_STRING_LEN);
    while body.len() < LARGE_STRING_LEN {
        let remaining = LARGE_STRING_LEN - body.len();
        let take = remaining.min(pattern.len());
        body.push_str(std::str::from_utf8(&pattern[..take]).unwrap());
    }
    assert_eq!(body.len(), LARGE_STRING_LEN);
    body
}

/// Read CompactStore section payload from an open persistent DB and return
/// `(payload_bytes, directory_entry_version)`.
///
/// Payload version is the authoritative GCST header byte. Directory version
/// may still be the historical outer-v1 default until `G-F0.1` lands; this
/// helper does **not** require them to match.
fn read_compact_store_payload(db: &GrafeoDB) -> (Vec<u8>, u8) {
    let fm = db
        .file_manager()
        .expect("persistent database must have a file manager");
    let dir = fm
        .read_section_directory()
        .expect("read section directory")
        .expect("section directory present after compact+close");
    let entry = dir
        .find(SectionType::CompactStore)
        .expect("CompactStore section must be present after compact");
    let data = fm
        .read_section_data(entry)
        .expect("read CompactStore section data");
    (data, entry.version)
}

#[test]
fn large_documentation_json_survives_compact_explicit_close_and_reopen() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().join("e0_large_string.grafeo");
    let body = documentation_json_body();

    let node_id = {
        let mut db = GrafeoDB::with_config(Config::persistent(&path)).expect("create db");

        let id = db
            .create_node_with_props(
                &["CodeSymbol"],
                [("documentation_json", Value::from(body.as_str()))],
            )
            .expect("create CodeSymbol with large string property");

        // Compact moves the node into the columnar CompactStore base.
        db.compact().expect("compact()");
        assert!(
            db.layered_store().is_some(),
            "layered CompactStore must be installed after compact()"
        );

        // Prove this persistent regression reaches the production failure
        // path before close serializes the CompactStore: the complete 96 KiB
        // value must be retained as the table-level string zone-map minimum.
        let base = db
            .layered_store()
            .expect("layered store after compact()")
            .base_store_arc();
        let node_table = base
            .node_table("CodeSymbol")
            .expect("CodeSymbol node table");
        let zone_map = node_table
            .zone_map(&PropertyKey::new("documentation_json"))
            .expect("documentation_json table-level zone map");
        let expected = Value::String(body.clone().into());
        assert_eq!(
            zone_map.min.as_ref(),
            Some(&expected),
            "table-level zone-map min must contain the full 96 KiB value before serialization"
        );
        assert_eq!(
            zone_map.max.as_ref(),
            Some(&expected),
            "table-level zone-map max must contain the full 96 KiB value before serialization"
        );

        // Explicit close is the success path — returns final checkpoint Result.
        // Never use drop(db) as the durability proof.
        db.close().expect("explicit close() must return Ok(())");
        id
    };

    // Reopen in a fresh handle (simulates process restart at the API boundary).
    let db = GrafeoDB::with_config(Config::persistent(&path)).expect("reopen db");
    assert!(
        db.layered_store().is_some(),
        "reopen must reconstruct LayeredStore from CompactStore section"
    );

    // Prove CompactStore section is present and payload header is current
    // (v5 mapped layout; still accepts large section-level / dict strings).
    let (payload, _dir_version) = read_compact_store_payload(&db);
    assert!(
        payload.len() >= 6,
        "CompactStore payload too short: {}",
        payload.len()
    );
    assert_eq!(&payload[0..4], b"GCST", "magic must be GCST");
    assert_eq!(
        payload[4], 5,
        "CompactStore payload version must be 5 after G-EM0.2 writer"
    );

    // Retrieve via LayeredStore (GrafeoDB::get_node only sees the overlay
    // LpgStore; base CompactStore properties require the layered path).
    let layered = db
        .layered_store()
        .expect("layered store present after reopen");
    let key = PropertyKey::new("documentation_json");
    let got = layered
        .get_node_property(node_id, &key)
        .expect("documentation_json property present on compact base");

    match got {
        Value::String(s) => {
            assert_eq!(
                s.len(),
                LARGE_STRING_LEN,
                "string length must match (got {})",
                s.len()
            );
            assert_eq!(
                s.as_str(),
                body.as_str(),
                "documentation_json must round-trip byte-for-byte"
            );
        }
        other => panic!("expected Value::String, got {other:?}"),
    }

    // Explicit close again — never leave Drop as the only finalizer.
    db.close()
        .expect("second explicit close() must return Ok(())");
}

#[test]
fn compact_store_payload_v5_is_present_for_modest_strings_too() {
    // Smaller sanity check that the GCST v5 header is written even when no
    // section-level string exceeds the legacy u16 limit (guards against a
    // writer that only bumps version when forced by length overflow).
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().join("em02_modest.grafeo");

    {
        let mut db = GrafeoDB::with_config(Config::persistent(&path)).expect("create");
        db.create_node_with_props(&["CodeSymbol"], [("name", Value::from("sym"))])
            .expect("create");
        db.compact().expect("compact");
        db.close().expect("close");
    }

    let db = GrafeoDB::with_config(Config::persistent(&path)).expect("reopen");
    let (payload, _) = read_compact_store_payload(&db);
    assert_eq!(&payload[0..4], b"GCST");
    assert_eq!(payload[4], 5);
    db.close().expect("close");
}
