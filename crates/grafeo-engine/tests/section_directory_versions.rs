//! G-F0.1: truthful per-section directory versions through engine flush.
//!
//! New checkpoints must record each section's declared `Section::version()`
//! in the container directory. Historical outer-v1 entries with supported
//! payloads must remain readable. Unsupported CompactStore payload versions
//! must fail closed (no silent misparse).

#![cfg(all(feature = "grafeo-file", feature = "lpg"))]

use grafeo_common::storage::{Section, SectionType};
use grafeo_common::types::Value;
use grafeo_engine::{Config, GrafeoDB};
use grafeo_storage::file::GrafeoFileManager;

fn directory_version(path: &std::path::Path, section_type: SectionType) -> u8 {
    let manager = GrafeoFileManager::open_read_only(path).expect("open container");
    let dir = manager
        .read_section_directory()
        .expect("read directory")
        .expect("v2 directory present");
    let entry = dir
        .find(section_type)
        .unwrap_or_else(|| panic!("missing section {section_type:?}"));
    let version = entry.version;
    manager.close().ok();
    version
}

#[test]
fn checkpoint_records_catalog_and_lpg_declared_versions() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("catalog_lpg.grafeo");

    {
        let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
        let session = db.session();
        session
            .execute("INSERT (:Person {name: 'Ada', age: 36})")
            .unwrap();
        db.wal_checkpoint().unwrap();
        db.close().unwrap();
    }

    // CatalogSection and LpgStoreSection both declare version 2 at this pin.
    assert_eq!(directory_version(&path, SectionType::Catalog), 2);
    assert_eq!(directory_version(&path, SectionType::LpgStore), 2);

    // Reopen proves historical-style outer metadata did not break recovery.
    let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
    assert_eq!(db.node_count(), 1);
    db.close().unwrap();
}

#[test]
#[cfg(feature = "compact-store")]
fn compact_checkpoint_records_compact_store_declared_version() {
    use grafeo_core::graph::compact::section::CompactStoreSection;

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("compact_versions.grafeo");

    {
        let mut db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
        db.execute("INSERT (:Doc {title: 't', body: 'hello'})")
            .unwrap();
        db.compact().unwrap();
        assert!(
            db.layered_store().is_some(),
            "layered CompactStore must be installed after compact()"
        );
        db.close().unwrap();
    }

    let declared = CompactStoreSection::empty().version();
    assert_eq!(
        directory_version(&path, SectionType::CompactStore),
        declared,
        "directory version must match CompactStoreSection::version()"
    );
    // Layered compact path also emits overlay LPG.
    assert_eq!(directory_version(&path, SectionType::LpgStore), 2);

    // Reopen through the layered path (node_count alone can under-count base).
    let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
    assert!(
        db.layered_store().is_some(),
        "reopen must reconstruct LayeredStore from CompactStore section"
    );
    let result = db
        .session()
        .execute("MATCH (d:Doc) RETURN count(d)")
        .unwrap();
    assert_eq!(result.rows()[0][0], Value::Int64(1));
    db.close().unwrap();
}

#[test]
#[cfg(feature = "compact-store")]
fn historical_outer_v1_compact_payload_still_opens() {
    // Simulate a pre-F0.1 writer: CompactStore payload is current, but the
    // directory entry still claims version 1.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("outer_v1_compact.grafeo");

    {
        let mut db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
        db.execute("INSERT (:Doc {title: 'keep-me'})").unwrap();
        db.compact().unwrap();
        db.close().unwrap();
    }

    // Rewrite directory with outer version 1 while preserving payload bytes.
    {
        let manager = GrafeoFileManager::open(&path).unwrap();
        let section_dir = manager
            .read_section_directory()
            .unwrap()
            .expect("directory");
        let mut rewritten: Vec<(SectionType, Vec<u8>)> = Vec::new();
        for entry in section_dir.entries() {
            let data = manager.read_section_data(entry).unwrap();
            rewritten.push((entry.section_type, data));
        }
        let refs: Vec<(SectionType, &[u8])> = rewritten
            .iter()
            .map(|(t, d)| (*t, d.as_slice()))
            .collect();
        // write_sections always records directory version 1 (legacy path).
        manager
            .write_sections(&refs, 1, 1, 1, 0)
            .expect("rewrite with outer v1");
        manager.close().unwrap();
    }

    assert_eq!(directory_version(&path, SectionType::CompactStore), 1);

    let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
    assert!(
        db.layered_store().is_some(),
        "outer-v1 CompactStore entry must still reconstruct LayeredStore"
    );
    let result = db
        .session()
        .execute("MATCH (d:Doc) RETURN d.title")
        .unwrap();
    let titles: Vec<String> = result
        .rows()
        .iter()
        .filter_map(|r| match &r[0] {
            Value::String(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(titles, vec!["keep-me".to_string()]);
    db.close().unwrap();
}

#[test]
#[cfg(feature = "compact-store")]
fn compact_store_rejects_unsupported_future_payload_version() {
    // Older reader (this pin understands CompactStore ≤3) must fail closed
    // on a future payload version rather than silently misparsing.
    use grafeo_core::graph::compact::section::CompactStoreSection;

    // Minimal GCST header: magic + version 4 + zero flags, CRC over header.
    let mut payload = Vec::new();
    payload.extend_from_slice(b"GCST");
    payload.push(4); // unsupported future payload version
    payload.push(0); // flags
    let crc = crc32fast::hash(&payload);
    payload.extend_from_slice(&crc.to_le_bytes());

    let mut section = CompactStoreSection::empty();
    let err = section
        .deserialize(&payload)
        .expect_err("future CompactStore payload must fail closed");
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported CompactStore") || msg.contains("version 4"),
        "expected unsupported-version error, got: {msg}"
    );
}

#[test]
#[cfg(feature = "vector-index")]
fn checkpoint_records_vector_store_declared_version() {
    use grafeo_core::index::vector::VectorStoreSection;

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("vector_versions.grafeo");

    {
        let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
        let session = db.session();
        session
            .execute("INSERT (:Doc {emb: [0.1, 0.2, 0.3]})")
            .unwrap();
        db.create_vector_index("Doc", "emb", Some(3), Some("cosine"), None, None, None)
            .unwrap();
        db.wal_checkpoint().unwrap();
        db.close().unwrap();
    }

    let declared = VectorStoreSection::new(Vec::new()).version();
    assert_eq!(
        directory_version(&path, SectionType::VectorStore),
        declared
    );
}

#[test]
#[cfg(feature = "text-index")]
fn checkpoint_records_text_index_declared_version() {
    use grafeo_core::index::text::TextIndexSection;

    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("text_versions.grafeo");

    {
        let db = GrafeoDB::with_config(Config::persistent(&path)).unwrap();
        let session = db.session();
        session
            .execute("INSERT (:Article {title: 'hello world'})")
            .unwrap();
        db.create_text_index("Article", "title").unwrap();
        db.wal_checkpoint().unwrap();
        db.close().unwrap();
    }

    let declared = TextIndexSection::new(Vec::new()).version();
    assert_eq!(directory_version(&path, SectionType::TextIndex), declared);
}

#[test]
fn property_index_directory_version_via_shared_writer() {
    // Property indexes are not emitted as a standalone container section by
    // the current layered/LPG build_sections path; the shared writer still
    // must preserve a declared PropertyIndex directory version when asked.
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("property_index_version.grafeo");

    let manager = GrafeoFileManager::create(&path).unwrap();
    manager
        .write_versioned_sections(
            &[(SectionType::PropertyIndex, 1, b"prop-index-payload".as_slice())],
            1,
            1,
            0,
            0,
        )
        .unwrap();
    let section_dir = manager
        .read_section_directory()
        .unwrap()
        .expect("directory");
    assert_eq!(
        section_dir.find(SectionType::PropertyIndex).unwrap().version,
        1
    );
    manager.close().unwrap();
}
