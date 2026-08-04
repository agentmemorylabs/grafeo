//! H-ADOPT.6 items 1–3 — generation index-section carry + restore proof.
//!
//! RED tests for the section wiring packet:
//!
//! (a) A publication from a DB with schema DDL + vector/text/property
//!     indexes carries all four sections (section directory inspection of
//!     the published container).
//! (b) Reopen restores: `open_generation_root` → the Catalog has the
//!     pre-boundary schema, and vector search returns the pre-boundary
//!     nearest neighbors WITHOUT rebuild (restore path asserted via
//!     resident topology immediately after open, before any query).
//! (c) Repeated reopen determinism with sections.
//! (d) Corrupt VectorStore section bytes → open fails closed (typed error),
//!     never silent fallback-to-rebuild for a corrupt present section.
//! (e) A legacy publication without sections (CompactStore only) still
//!     opens (compat path: fresh Catalog + no shells).

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap",
    feature = "wal",
    feature = "vector-index",
    feature = "text-index"
))]

use std::io::Write;

use grafeo_common::storage::SectionType;
use grafeo_common::types::Value;
use grafeo_engine::{Config, GrafeoDB, VectorTopologyBacking, generation_build_request};
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::file::generation_writer::{
    ExactSectionSource, GenerationContainerHeader, GenerationFileOps, OsGenerationFileOps,
};
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::publication::{PublicationInput, publish_generation};
use grafeo_storage::wal::WalManager;
use tempfile::TempDir;

const LABEL: &str = "Doc";
const PROP: &str = "embedding";
const TITLE: &str = "title";
const DIMS: usize = 8;
const K: usize = 5;

fn seeded_vector(seed: u64, dim: usize) -> Vec<f32> {
    let mut state = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
    let mut raw: Vec<f32> = (0..dim)
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

/// Build a source DB with schema DDL + a vector index with embeddings, plus
/// (when `with_text_and_property`) a text index and a property index.
/// Returns the frozen pre-publish search results for the fixed query seeds.
fn build_source(
    with_text_and_property: bool,
) -> (GrafeoDB, Vec<Vec<(grafeo_common::types::NodeId, f32)>>) {
    let source = GrafeoDB::new_in_memory();
    source
        .session()
        .execute("CREATE NODE TYPE Doc (title STRING)")
        .expect("schema DDL");
    for i in 0u64..64 {
        source
            .create_node_with_props(
                &[LABEL],
                [
                    (TITLE, Value::from(format!("doc-{i}"))),
                    (PROP, Value::Vector(seeded_vector(i, DIMS).into())),
                    (
                        "rank",
                        Value::Int64(i64::try_from(i).expect("rank value fits i64")),
                    ),
                ],
            )
            .expect("create node");
    }
    source
        .create_vector_index(LABEL, PROP, Some(DIMS), Some("cosine"), None, None, None)
        .expect("create vector index");
    if with_text_and_property {
        source
            .create_text_index(LABEL, TITLE)
            .expect("create text index");
        source.create_property_index("rank");
    }
    let queries = [seeded_vector(7, DIMS), seeded_vector(42, DIMS)];
    let mut frozen = Vec::with_capacity(queries.len());
    for q in &queries {
        frozen.push(
            source
                .vector_search(LABEL, PROP, q, K, None, None)
                .expect("search before publish"),
        );
    }
    (source, frozen)
}

/// Publish one generation from `source` into `root`; returns the container path.
fn publish_engine_generation(
    source: &GrafeoDB,
    root: &std::path::Path,
    id: &str,
) -> std::path::PathBuf {
    let publication = source
        .build_and_publish_generation(generation_build_request(root, id))
        .expect("publish generation")
        .publication;
    root.join(&publication.generation_path)
}

fn assert_restored_schema(db: &GrafeoDB) {
    // The pre-boundary schema must be present in the Catalog: re-creating the
    // same node type fails closed instead of silently succeeding.
    let dup = db.session().execute("CREATE NODE TYPE Doc (title STRING)");
    let err = dup.expect_err("restored schema must reject duplicate CREATE NODE TYPE");
    let msg = format!("{err}");
    assert!(
        msg.contains("Doc") || msg.contains("exist") || msg.contains("registered"),
        "duplicate DDL error should name the type, got: {msg}"
    );
}

fn assert_restored_vector_topology(
    db: &GrafeoDB,
    frozen: &[Vec<(grafeo_common::types::NodeId, f32)>],
) {
    // Restore-path proof: shells + resident topology are present immediately
    // after open, before any search runs.
    assert!(
        db.has_vector_index(LABEL, PROP),
        "vector shells must be restored from the Catalog section"
    );
    let diags = db.vector_backing_diagnostics();
    assert_eq!(diags.len(), 1, "one vector index after restore");
    match &diags[0].topology {
        VectorTopologyBacking::Heap { heap_bytes } => {
            assert!(
                *heap_bytes > 0,
                "restored topology must be resident (heap_bytes > 0)"
            );
        }
        VectorTopologyBacking::Mmap { topology_bytes } => {
            assert!(
                *topology_bytes > 0,
                "restored topology must be file-backed (topology_bytes > 0)"
            );
        }
    }

    // Search parity: the pre-boundary neighbors are served from the restored
    // topology — no rebuild produced them.
    for (qi, q) in [seeded_vector(7, DIMS), seeded_vector(42, DIMS)]
        .iter()
        .enumerate()
    {
        let hits = db
            .vector_search(LABEL, PROP, q, K, None, None)
            .expect("search immediately after open");
        assert_eq!(hits, frozen[qi], "pre-boundary neighbor parity query {qi}");
    }
}

/// (a) Publication carries all four index sections when present.
#[test]
fn publication_carries_all_four_index_sections_when_present() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("four-sections.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    let (source, _frozen) = build_source(true);
    let container = publish_engine_generation(&source, &root, "four-g1");
    drop(source);

    let manager = GrafeoFileManager::open_read_only(&container).expect("open container");
    let directory = manager
        .read_section_directory()
        .expect("read section directory")
        .expect("section directory present");
    for section_type in [
        SectionType::CompactStore,
        SectionType::Catalog,
        SectionType::VectorStore,
        SectionType::TextIndex,
        SectionType::PropertyIndex,
    ] {
        assert!(
            directory.find(section_type).is_some(),
            "published container must carry {section_type:?}"
        );
    }
}

/// (b) Reopen restores schema + vector topology; search without rebuild.
#[test]
fn reopen_restores_catalog_schema_and_vector_topology_without_rebuild() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("restore.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    let (source, frozen) = build_source(false);
    let _container = publish_engine_generation(&source, &root, "restore-g1");
    drop(source);

    let db = GrafeoDB::open_generation_root(&root, false).expect("open generation root");
    assert_restored_schema(&db);
    assert_restored_vector_topology(&db, &frozen);
    drop(db);
}

/// (c) Repeated reopen determinism with sections.
#[test]
fn repeated_reopen_is_deterministic_with_sections() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("determinism.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    let (source, frozen) = build_source(false);
    let _container = publish_engine_generation(&source, &root, "determinism-g1");
    drop(source);

    for round in 0..3 {
        let db = GrafeoDB::open_generation_root(&root, false).expect("reopen generation root");
        assert_restored_schema(&db);
        assert_restored_vector_topology(&db, &frozen);
        drop(db);
        assert!(
            GrafeoDB::open_generation_root(&root, false).is_ok(),
            "round {round}: open after drop must succeed"
        );
    }
}

/// (d) Corrupt VectorStore section bytes → open fails closed (typed error).
///
/// The corruption is applied to the container AND the manifest slot's
/// `generation_sha256` is re-derived for the modified bytes, so the open gets
/// past the whole-file recovery SHA gate and fails at the SECTION boundary
/// (VectorStore mmap CRC / decode) — proving the section restore path itself
/// fails closed, never a silent fallback-to-rebuild.
#[test]
fn corrupt_vector_store_section_fails_closed_on_open() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("corrupt.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    let (source, _frozen) = build_source(false);
    let container = publish_engine_generation(&source, &root, "corrupt-g1");
    drop(source);

    // Flip the first occurrence of the GVST VectorStore v2 magic so the
    // section is present but corrupt (CRC mismatch + bad magic).
    let mut bytes = std::fs::read(&container).expect("read container");
    let magic = b"GVST";
    let pos = bytes
        .windows(4)
        .position(|w| w == magic)
        .expect("published container must contain the GVST VectorStore section");
    bytes[pos] = b'X';
    std::fs::write(&container, &bytes).expect("write corrupt container");

    // Re-derive the manifest slot's SHA-256 for the modified file so the open
    // passes recovery and fails at the section boundary.
    let corrupt_sha = OsGenerationFileOps
        .sha256(&container)
        .expect("sha of corrupt container");
    let manifest_path = root.join("manifest.bin");
    let [slot0, slot1] =
        grafeo_storage::generation::manifest::read_both_slots(&manifest_path).expect("read slots");
    let (active_index, mut active) = match (slot0, slot1) {
        (Ok(a), Ok(b)) if b.publication_sequence > a.publication_sequence => (1, b),
        (Ok(a), _) => (0, a),
        (Err(_), Ok(b)) => (1, b),
        (Err(e0), Err(e1)) => panic!("no valid manifest slot: {e0} / {e1}"),
    };
    assert_eq!(
        active.publication_sequence, 1,
        "fixture must publish exactly one generation"
    );
    active.generation_sha256 = corrupt_sha;
    let slot_bytes =
        grafeo_storage::generation::manifest::encode_slot_bytes(&active).expect("encode slot");
    {
        use std::io::{Seek, SeekFrom, Write as _};
        let mut manifest_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&manifest_path)
            .expect("open manifest");
        let offset = u64::try_from(active_index * grafeo_storage::generation::manifest::SLOT_SIZE)
            .expect("slot offset fits u64");
        manifest_file
            .seek(SeekFrom::Start(offset))
            .expect("seek slot");
        manifest_file
            .write_all(&slot_bytes)
            .expect("write patched slot");
        manifest_file.sync_all().expect("sync manifest");
    }

    let err = GrafeoDB::open_generation_root(&root, false)
        .err()
        .expect("corrupt VectorStore section must fail closed");
    let msg = format!("{err}");
    assert!(
        msg.contains("Vector Store")
            || msg.contains("topology")
            || msg.contains("magic")
            || msg.contains("Serialization")
            || msg.contains("bad magic")
            || msg.contains("mmap")
            || msg.contains("CRC")
            || msg.contains("GVST")
            || msg.contains("section"),
        "unexpected error for corrupt VectorStore section: {msg}"
    );
}

/// A raw-bytes [`ExactSectionSource`] for the legacy-container fixture.
struct RawBytesSectionSource {
    section_type: SectionType,
    version: u8,
    bytes: Vec<u8>,
}

impl ExactSectionSource for RawBytesSectionSource {
    fn section_type(&self) -> SectionType {
        self.section_type
    }

    fn directory_version(&self) -> u8 {
        self.version
    }

    fn exact_len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn copy_to(&mut self, sink: &mut dyn Write) -> grafeo_common::utils::error::Result<()> {
        sink.write_all(&self.bytes)
            .map_err(grafeo_common::utils::error::Error::Io)
    }
}

/// (e) A legacy publication WITHOUT sections (CompactStore only) still opens:
/// fresh Catalog, no shells, base data served.
#[test]
fn legacy_publication_without_sections_still_opens() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("legacy.grafeo.d");
    std::fs::create_dir_all(&root).expect("create generation root");

    // First publish a modern generation to obtain a CompactStore section.
    let (source, _frozen) = build_source(false);
    let container = publish_engine_generation(&source, &root, "modern-g1");
    drop(source);

    let manager = GrafeoFileManager::open_read_only(&container).expect("open modern container");
    let directory = manager
        .read_section_directory()
        .expect("read section directory")
        .expect("section directory");
    let compact_entry = directory
        .find(SectionType::CompactStore)
        .expect("CompactStore section present");
    let compact_bytes = manager
        .read_section_data(compact_entry)
        .expect("read CompactStore section bytes");
    let header = manager.active_header();

    // Publish a SECOND generation carrying ONLY the CompactStore section —
    // the exact shape every generation had before H-ADOPT.6 item 2.
    let lock = RootLock::try_acquire(&root).expect("acquire root lock");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&wal_dir).expect("create wal dir");
    let wal = WalManager::open(&wal_dir).expect("open wal");
    let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![Box::new(RawBytesSectionSource {
        section_type: SectionType::CompactStore,
        version: 5,
        bytes: compact_bytes,
    })];
    let gen_header = GenerationContainerHeader {
        epoch: header.epoch,
        transaction_id: header.transaction_id,
        node_count: header.node_count,
        edge_count: header.edge_count,
    };
    publish_generation(
        &lock,
        PublicationInput {
            header: gen_header,
            sections: &mut sections,
            generation_id: "legacy-g2".to_string(),
            parent_generation_id: None,
            parent_publication_sequence: None,
            pre_cut_cursor: None,
        },
        &wal,
        &OsGenerationFileOps,
    )
    .expect("publish legacy CompactStore-only generation");
    drop(lock);

    let db = GrafeoDB::open_generation_root(&root, false).expect("legacy generation root opens");
    let result = db
        .session()
        .execute("MATCH (n:Doc) RETURN count(n)")
        .expect("query legacy base");
    assert_eq!(result.row_count(), 1, "legacy base serves the Doc nodes");
    assert!(
        !db.has_vector_index(LABEL, PROP),
        "legacy open must not fabricate vector shells"
    );
    // Fresh catalog: the schema DDL was never carried.
    let dup = db.session().execute("CREATE NODE TYPE Doc (title STRING)");
    assert!(dup.is_ok(), "fresh catalog accepts the first DDL");
    drop(db);
}

/// Smoke: the writable open path used by the restore tests must also serve
/// normal queries through the layered base (config plumbing sanity).
#[test]
fn restore_open_preserves_config_and_serves_base() {
    let dir = TempDir::new().expect("temp dir");
    let root = dir.path().join("config.grafeo.d");
    let spill = dir.path().join("spill");
    std::fs::create_dir_all(&root).expect("create generation root");

    let (source, _frozen) = build_source(false);
    let _container = publish_engine_generation(&source, &root, "config-g1");
    drop(source);

    let config = Config::persistent(&root)
        .with_memory_limit(64 * 1024 * 1024)
        .with_spill_path(&spill)
        .with_threads(2);
    let db = GrafeoDB::open_generation_root_with_config(config).expect("open with config");
    assert_eq!(db.config().memory_limit, Some(64 * 1024 * 1024));
    let result = db
        .session()
        .execute("MATCH (n:Doc) RETURN count(n)")
        .expect("query generation base");
    assert_eq!(result.row_count(), 1);
    drop(db);
}
