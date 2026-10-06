//! G4 — CompactStore v6 (64-bit column-body geometry) through the real
//! publish → recover → mmap → reopen paths.
//!
//! - `CompactPayloadVersion::V6` generations publish with payload byte,
//!   container section version and manifest slot all at 6, read back every
//!   value, and keep their ids across a v6 rebuild and two reopens.
//! - `CompactPayloadVersion::Auto` (the default) keeps publishing v5 when
//!   every field fits, and rebuilds a v6 base back into v5.
//! - `v6_vector_column_over_4gib_round_trips` (`#[ignore]`, heavy) builds a
//!   600k × 2048 f32 vector column (4,915,200,007-byte body, the build that
//!   failed with `WireWidthOverflow` at v5) under `Auto` and reads it back.
//!
//! ```text
//! cargo test -p grafeo-engine --features generation,generation-streaming,compact-store,lpg,mmap,wal,cypher \
//!   --test compact_store_v6_payload
//! ```

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "compact-store",
    feature = "lpg",
    feature = "mmap",
    feature = "wal",
    feature = "cypher"
))]

use std::path::Path;
use std::sync::Arc;

use grafeo_common::storage::SectionType;
use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};
use grafeo_core::graph::Direction;
use grafeo_core::graph::traits::GraphStore;
use grafeo_engine::{CompactPayloadVersion, Config, GrafeoDB, generation_build_request};
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::recovery::recover;
use tempfile::tempdir;

/// (manifest slot `compact_store_format_version`, container section
/// version, payload header byte) of the root's selected generation.
fn published_versions(root: &Path) -> (u16, u8, u8) {
    let lock = RootLock::try_acquire(root).expect("root lock");
    let selected = recover(&lock).expect("recover");
    drop(lock);
    let manager =
        GrafeoFileManager::open_read_only(&selected.generation_abs_path).expect("open generation");
    let dir = manager.read_section_directory().unwrap().unwrap();
    let entry = dir.find(SectionType::CompactStore).expect("CompactStore");
    let mmap = Arc::new(manager.mmap_section(entry).expect("mmap"));
    let bytes = grafeo_storage::container::MmapSection::into_bytes(mmap);
    (
        selected.slot.compact_store_format_version,
        entry.version,
        bytes[4],
    )
}

fn config(root: &Path, version: CompactPayloadVersion) -> Config {
    Config::persistent(root).with_compact_payload_version(version)
}

fn embedding(i: u64) -> Value {
    Value::Vector(vec![i as f32, 0.25, -(i as f32), 1.0 / (i as f32 + 1.0)].into())
}

/// Seeds `count` `:Doc` nodes chained by `:NEXT` edges; returns their ids.
fn seed(db: &GrafeoDB, count: u64) -> (Vec<NodeId>, Vec<EdgeId>) {
    let mut nodes = Vec::new();
    for i in 0..count {
        nodes.push(
            db.create_node_with_props(
                &["Doc"],
                [
                    ("title", Value::from(format!("doc-{i}"))),
                    ("rank", Value::from(i as i64 - 3)),
                    ("embedding", embedding(i)),
                ],
            )
            .expect("create node"),
        );
    }
    let edges = nodes
        .windows(2)
        .enumerate()
        .map(|(i, w)| db.create_edge_with_props(w[0], w[1], "NEXT", [("w", Value::from(i as i64))]))
        .collect();
    (nodes, edges)
}

/// Every seeded id still resolves to its own values.
fn assert_seeded(db: &GrafeoDB, nodes: &[NodeId], edges: &[EdgeId], what: &str) {
    let store = db.graph_store();
    for (i, &id) in nodes.iter().enumerate() {
        let node = store
            .get_node(id)
            .unwrap_or_else(|| panic!("{what}: node {id:?} lost"));
        let i = i as u64;
        assert_eq!(
            node.properties.get(&PropertyKey::new("title")),
            Some(&Value::from(format!("doc-{i}"))),
            "{what}: node {id:?} title"
        );
        assert_eq!(
            store.get_node_property(id, &PropertyKey::new("embedding")),
            Some(embedding(i)),
            "{what}: node {id:?} embedding"
        );
    }
    for (i, &id) in edges.iter().enumerate() {
        let edge = store
            .get_edge(id)
            .unwrap_or_else(|| panic!("{what}: edge {id:?} lost"));
        assert_eq!(
            (edge.src, edge.dst),
            (nodes[i], nodes[i + 1]),
            "{what}: edge {id:?}"
        );
        assert_eq!(
            edge.properties.get(&PropertyKey::new("w")),
            Some(&Value::from(i as i64)),
            "{what}: edge {id:?} w"
        );
    }
    let out = store.edges_from(nodes[0], Direction::Outgoing);
    assert_eq!(out, vec![(nodes[1], edges[0])], "{what}: adjacency");
}

#[test]
fn auto_keeps_publishing_v5_when_everything_fits() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("auto.grafeo.d");
    std::fs::create_dir_all(&root).unwrap();
    let source = GrafeoDB::new_in_memory();
    seed(&source, 8);
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .expect("publish");
    drop(source);
    assert_eq!(published_versions(&root), (5, 5, 5));
}

/// v6 build → reopen → write → v6 rebuild (epoch handoff from the mapped v6
/// base) → two reopens: every id keeps its values (cf. fork #18, DESIGN §7
/// step 6). Then an `Auto` rebuild of that v6 base writes v5 again.
#[test]
fn v6_generation_root_keeps_ids_across_a_rebuild_and_two_reopens() {
    let dir = tempdir().unwrap();
    let root = dir.path().join("v6.grafeo.d");
    std::fs::create_dir_all(&root).unwrap();

    let source = GrafeoDB::with_config(
        Config::in_memory().with_compact_payload_version(CompactPayloadVersion::V6),
    )
    .expect("source db");
    let (mut nodes, mut edges) = seed(&source, 12);
    source
        .build_and_publish_generation(generation_build_request(&root, "g1"))
        .expect("publish v6 base");
    drop(source);
    assert_eq!(published_versions(&root), (6, 6, 6));

    {
        let db =
            GrafeoDB::open_generation_root_with_config(config(&root, CompactPayloadVersion::V6))
                .expect("open v6 root");
        assert_seeded(&db, &nodes, &edges, "first open");
        let extra = db
            .create_node_with_props(
                &["Doc"],
                [
                    ("title", Value::from("doc-12")),
                    ("rank", Value::from(9i64)),
                    ("embedding", embedding(12)),
                ],
            )
            .expect("overlay node");
        let last = *nodes.last().unwrap();
        edges.push(db.create_edge_with_props(last, extra, "NEXT", [("w", Value::from(11i64))]));
        nodes.push(extra);
        let report = db
            .run_epoch_handoff(generation_build_request(&root, "g2"))
            .expect("v6 epoch handoff");
        db.publish_and_install_handoff(report)
            .expect("publish and install");
        assert_seeded(&db, &nodes, &edges, "after handoff");
        db.close().expect("close");
    }
    assert_eq!(published_versions(&root), (6, 6, 6));

    for reopen in ["reopen 1", "reopen 2"] {
        let db =
            GrafeoDB::open_generation_root_with_config(config(&root, CompactPayloadVersion::V6))
                .expect(reopen);
        assert_seeded(&db, &nodes, &edges, reopen);
        db.close().expect("close");
    }

    // A default (`Auto`) binary rebuilds the small v6 base as v5.
    {
        let db = GrafeoDB::open_generation_root(&root, false).expect("open with Auto");
        assert_seeded(&db, &nodes, &edges, "auto open of v6 base");
        let report = db
            .run_epoch_handoff(generation_build_request(&root, "g3"))
            .expect("auto handoff");
        db.publish_and_install_handoff(report).expect("install");
        db.close().expect("close");
    }
    assert_eq!(published_versions(&root), (5, 5, 5));
    let db = GrafeoDB::open_generation_root(&root, false).expect("reopen v5");
    assert_seeded(&db, &nodes, &edges, "after auto rebuild");
}

mod over_4gib {
    use std::time::Instant;

    use grafeo_core::graph::compact::generation::{
        EdgeRecordSource, GenerationBudget, GenerationEdge, GenerationError, GenerationNode,
        NodeRecordSource,
    };
    use grafeo_core::graph::compact::generation_builder::orchestrator::{
        BoundedBuildConfig, BoundedGenerationBuilder,
    };
    use grafeo_core::graph::compact::mapped::{
        PayloadVersion, SegmentKind, parse_segment_directory, read_block_index_record,
    };
    use grafeo_core::graph::compact::section::CompactStoreSection;
    use grafeo_storage::file::generation_writer::{
        ExactSectionSource, GenerationContainerHeader, OsGenerationFileOps,
        StreamingPayloadSectionSource,
    };
    use grafeo_storage::generation::publication::{PublicationInput, publish_generation};
    use grafeo_storage::generation::run_adapter::DiskRunStore;
    use grafeo_storage::wal::WalManager;

    use super::*;

    /// Samples this process's `RssAnon` / `RssFile` (kB) every 20 ms and
    /// reports each one's peak for a phase, so heap and mapped-file pages are
    /// told apart (a max-RSS figure mixes them).
    struct RssPhase {
        stop: Arc<std::sync::atomic::AtomicBool>,
        handle: std::thread::JoinHandle<(u64, u64)>,
    }

    fn rss_kb(field: &str) -> u64 {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with(field))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|v| v.parse().ok())
            })
            .unwrap_or(0)
    }

    impl RssPhase {
        fn start() -> Self {
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let flag = Arc::clone(&stop);
            let handle = std::thread::spawn(move || {
                let (mut anon, mut file) = (0, 0);
                loop {
                    anon = anon.max(rss_kb("RssAnon:"));
                    file = file.max(rss_kb("RssFile:"));
                    if flag.load(std::sync::atomic::Ordering::Relaxed) {
                        return (anon, file);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            });
            Self { stop, handle }
        }

        fn finish(self, phase: &str) {
            self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let (anon, file) = self.handle.join().unwrap();
            eprintln!("G4 rss {phase}: RssAnon_peak_kb={anon} RssFile_peak_kb={file}");
        }
    }

    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .map_or(default, |v| v.parse().expect(name))
    }

    fn component(row: u64, d: u64) -> f32 {
        ((row.wrapping_mul(2_654_435_761) ^ d) % 10_007) as f32 * 0.001
    }

    fn vector(row: u64, dims: u64) -> Value {
        Value::Vector(
            (0..dims)
                .map(|d| component(row, d))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    /// Streams `rows` `:Emb` nodes (ids 0..rows) without holding them.
    struct SynthNodes {
        next: u64,
        rows: u64,
        dims: u64,
    }

    impl NodeRecordSource for SynthNodes {
        fn next_node(&mut self) -> Result<Option<GenerationNode>, GenerationError> {
            if self.next == self.rows {
                return Ok(None);
            }
            let i = self.next;
            self.next += 1;
            Ok(Some(
                GenerationNode::new(i, "Emb").with_prop("embedding", vector(i, self.dims)),
            ))
        }
    }

    struct NoEdges;

    impl EdgeRecordSource for NoEdges {
        fn next_edge(&mut self) -> Result<Option<GenerationEdge>, GenerationError> {
            Ok(None)
        }
    }

    /// G4 acceptance: a vector column whose body passes 4 GiB builds under
    /// the default `Auto` policy (which must pick v6), publishes, recovers,
    /// maps and reads back. Defaults reproduce the measured v5 failure
    /// (`col_body_len count 4915200007 exceeds wire max 4294967295`).
    /// `GRAFEO_G4_ROWS` / `GRAFEO_G4_DIMS` / `GRAFEO_G4_TMP` override.
    ///
    /// Needs ~3× the body size in free disk under `GRAFEO_G4_TMP` (runs,
    /// spool, generation file) and is meant for a build VM, not a laptop.
    #[test]
    #[ignore = "heavy: writes a >4 GiB generation; run on a build VM"]
    fn v6_vector_column_over_4gib_round_trips() {
        let rows = env_u64("GRAFEO_G4_ROWS", 600_000);
        let dims = env_u64("GRAFEO_G4_DIMS", 2048);
        let base = std::env::var("GRAFEO_G4_TMP").map_or_else(|_| std::env::temp_dir(), Into::into);
        let tmp = tempfile::tempdir_in(base).unwrap();
        let root = tmp.path().join("g4.grafeo.d");
        std::fs::create_dir_all(root.join("wal")).unwrap();

        let budget = GenerationBudget {
            max_temp_bytes: 256 << 30,
            max_mapped_bytes: 256 << 30,
            ..GenerationBudget::acceptance_linux()
        };
        let started = Instant::now();
        let rss = RssPhase::start();
        let mut run_store =
            DiskRunStore::new(tmp.path().join("runs"), budget, "g4").expect("DiskRunStore");
        let mut builder = BoundedGenerationBuilder::new(BoundedBuildConfig {
            budget,
            temp_dir: tmp.path().join("build-tmp"),
            correlation_id: "g4".into(),
            spool_buf_cap: 1 << 20,
            rel_schemas: Vec::new(),
            frozen_epoch: 0,
        });
        let lease = builder
            .build(
                &mut SynthNodes {
                    next: 0,
                    rows,
                    dims,
                },
                &mut NoEdges,
                &mut run_store,
            )
            .expect("bounded build");
        let built = started.elapsed();
        rss.finish("build");
        let rss = RssPhase::start();
        // `[5][dims u16][components u32]` + f32 data, the v5 body size.
        let narrow_body = 1 + 2 + 4 + rows * dims * 4;
        let want = if narrow_body > u64::from(u32::MAX) {
            PayloadVersion::V6
        } else {
            PayloadVersion::V5
        };
        assert_eq!(
            lease.payload_version(),
            want,
            "Auto must pick v6 exactly when the column body passes u32"
        );
        eprintln!("G4 metrics: {:?}", lease.metrics());

        let wal = WalManager::open(root.join("wal")).expect("wal");
        let lock = RootLock::try_acquire(&root).expect("lock");
        let mut sections: Vec<Box<dyn ExactSectionSource>> =
            vec![Box::new(StreamingPayloadSectionSource::new(lease))];
        publish_generation(
            &lock,
            PublicationInput {
                header: GenerationContainerHeader {
                    epoch: 1,
                    transaction_id: 1,
                    node_count: rows,
                    edge_count: 0,
                },
                sections: &mut sections,
                generation_id: "g4".into(),
                parent_generation_id: None,
                parent_publication_sequence: None,
                pre_cut_cursor: None,
            },
            &wal,
            &OsGenerationFileOps,
        )
        .expect("publish");
        drop(sections);
        drop(lock);
        let published = started.elapsed();
        rss.finish("publish");
        let rss = RssPhase::start();

        let lock = RootLock::try_acquire(&root).expect("re-lock");
        let selected = recover(&lock).expect("recover");
        drop(lock);
        assert_eq!(
            selected.slot.compact_store_format_version,
            u16::from(want.byte())
        );
        let file_len = std::fs::metadata(&selected.generation_abs_path)
            .unwrap()
            .len();

        let manager = GrafeoFileManager::open_read_only(&selected.generation_abs_path).unwrap();
        let dir = manager.read_section_directory().unwrap().unwrap();
        let entry = dir.find(SectionType::CompactStore).unwrap();
        assert_eq!(entry.version, want.byte());
        let mmap = Arc::new(manager.mmap_section(entry).expect("mmap"));
        let bytes = grafeo_storage::container::MmapSection::into_bytes(mmap);
        assert_eq!(bytes[4], want.byte());

        let segments = parse_segment_directory(&bytes, bytes.len() - 4).unwrap();
        let bi = segments.require(SegmentKind::ColumnBlockIndex).unwrap();
        let bi_start = usize::try_from(bi.offset).unwrap();
        let rec = read_block_index_record(&bytes[bi_start..], want, 0).unwrap();
        eprintln!("G4 block index record: {rec:?}");
        // The wide v6 vector header (`u64` count) adds 4 bytes; Auto uses it
        // only past u32 components.
        let wide_header = rows * dims > u64::from(u32::MAX);
        assert_eq!(rec.body_len, narrow_body + if wide_header { 4 } else { 0 });

        let mut section = CompactStoreSection::empty();
        section
            .deserialize_from_mapped_bytes(bytes)
            .expect("mapped v6 reopen");
        let store = section.store().expect("store");
        assert_eq!(store.total_nodes(), rows);
        let key = PropertyKey::new("embedding");
        let mut checked = 0;
        for row in [0, 1, rows / 2, rows - 2, rows - 1]
            .into_iter()
            .chain((0..64).map(|k| (k * 7_919_993) % rows))
        {
            assert_eq!(
                store.get_node_property(NodeId::new(row), &key),
                Some(vector(row, dims)),
                "row {row}"
            );
            checked += 1;
        }
        let opened = started.elapsed();
        rss.finish("recover+mmap+deserialize+verify");
        eprintln!(
            "G4 evidence: rows={rows} dims={dims} body_len={} file_len={file_len} \
             build={built:?} publish={published:?} reopen+verify={opened:?} rows_checked={checked}",
            rec.body_len
        );
    }
}
