//! G4 — heavy round trip of a CompactStore column body past 4 GiB.
//!
//! `v6_vector_column_over_4gib_round_trips` (`#[ignore]`) builds a
//! 600k × 2048 f32 vector column (4,915,200,007-byte body, the build that
//! failed with `WireWidthOverflow` at v5) under `Auto`, publishes, recovers,
//! maps and reads it back. Run it on a build VM:
//!
//! ```text
//! GRAFEO_G4_TMP=/data/tmp cargo test -p grafeo-engine \
//!   --features generation,generation-streaming,compact-store,lpg,mmap,wal,cypher \
//!   --test compact_store_v6_payload_large -- --ignored --nocapture
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

use std::sync::Arc;

use grafeo_common::storage::SectionType;
use grafeo_common::types::{NodeId, PropertyKey, Value};
use grafeo_core::graph::traits::GraphStore;
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::recovery::recover;

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
/// Needs ~4× the body size in free disk under `GRAFEO_G4_TMP` (sort runs
/// ≈ 2×, body spool, generation file; measured 19.1 GB for a 4.9 GB body)
/// and is meant for a build VM, not a laptop.
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
