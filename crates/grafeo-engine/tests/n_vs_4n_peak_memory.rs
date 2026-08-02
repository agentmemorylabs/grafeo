//! G-EM0.5b D0.8.10 #7 — N-vs-4N peak memory acceptance (isolated-process).
//!
//! Spawns a fresh child per scale (N and 4N) that runs the production
//! publication + fresh-reopen path under `GenerationBudget::acceptance_linux()`.
//! The parent compares child-reported peak RssAnon samples and requires
//! nonzero truthful ledger counters plus zero leftover job artifacts.

#![cfg(feature = "generation-streaming")]

use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use grafeo_common::storage::SectionType;
use grafeo_common::types::{PropertyKey, Value};
use grafeo_core::graph::compact::generation::{
    EdgeRecordSource, GenerationBudget, GenerationEdge, GenerationError, GenerationNode,
    NodeRecordSource,
};
use grafeo_core::graph::compact::generation_builder::orchestrator::{
    BoundedBuildConfig, BoundedGenerationBuilder,
};
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_storage::file::generation_writer::{
    GenerationContainerHeader, OsGenerationFileOps, StreamingPayloadSectionSource,
};
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::generation::lock::RootLock;
use grafeo_storage::generation::publication::{PublicationInput, publish_generation};
use grafeo_storage::generation::recovery::recover;
use grafeo_storage::generation::run_adapter::DiskRunStore;
use grafeo_storage::generation::RssAnonSampler;
use grafeo_storage::wal::WalManager;
use tempfile::TempDir;

const CHILD_ENV: &str = "GRAFEO_NVS4N_CHILD";

struct SyntheticNodes {
    n: usize,
    cursor: usize,
}

impl NodeRecordSource for SyntheticNodes {
    fn next_node(&mut self) -> Result<Option<GenerationNode>, GenerationError> {
        if self.cursor >= self.n {
            return Ok(None);
        }
        let i = self.cursor;
        self.cursor += 1;
        let mut node = GenerationNode::new(i as u64 + 1, "Person");
        // Cap string cardinality so peak RssAnon measures the sort/spool budget
        // plateau (D0.8.10), not allocator retention of one-string-per-row
        // temporaries. Unbounded per-column dict lookup (no resident HashMap /
        // offset Vec) is proven by `dict_column_lookup` unit tests against
        // large on-disk chunks.
        node.properties.insert(
            PropertyKey::new("name"),
            Value::from(format!("person_{}", i % 4096)),
        );
        node.properties
            .insert(PropertyKey::new("age"), Value::Int64((i % 80) as i64 + 18));
        Ok(Some(node))
    }
}

struct SyntheticEdges {
    count: usize,
    cursor: usize,
    n: usize,
}

impl EdgeRecordSource for SyntheticEdges {
    fn next_edge(&mut self) -> Result<Option<GenerationEdge>, GenerationError> {
        if self.cursor >= self.count {
            return Ok(None);
        }
        let i = self.cursor;
        self.cursor += 1;
        Ok(Some(GenerationEdge::new(
            (self.n + i) as u64 + 1,
            (i * 2) as u64 + 1,
            (i * 2 + 1) as u64 + 1,
            "KNOWS",
        )))
    }
}

#[derive(Debug)]
struct ChildReport {
    label: String,
    n: usize,
    peak_rss_anon_kb: u64,
    payload_len: usize,
    temp_bytes_peak: u64,
    anon_bytes_peak: u64,
    mapped_bytes_peak: u64,
    outer_sha256: String,
    leftover_files: usize,
}

impl ChildReport {
    fn parse(stdout: &str) -> Self {
        let mut fields = std::collections::HashMap::new();
        for line in stdout.lines() {
            if let Some((k, v)) = line.split_once('=') {
                fields.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
        Self {
            label: fields.get("LABEL").cloned().unwrap_or_default(),
            n: fields.get("N").and_then(|v| v.parse().ok()).unwrap_or(0),
            peak_rss_anon_kb: fields
                .get("PEAK_RSS_ANON_KB")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            payload_len: fields
                .get("PAYLOAD_LEN")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            temp_bytes_peak: fields
                .get("TEMP_PEAK")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            anon_bytes_peak: fields
                .get("ANON_PEAK")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            mapped_bytes_peak: fields
                .get("MAPPED_PEAK")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            outer_sha256: fields.get("OUTER_SHA256").cloned().unwrap_or_default(),
            leftover_files: fields
                .get("LEFTOVER_FILES")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        }
    }
}

fn count_files_under(root: &Path) -> usize {
    if !root.exists() {
        return 0;
    }
    let mut count = 0usize;
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            count += if path.is_dir() {
                count_files_under(&path)
            } else {
                1
            };
        }
    }
    count
}

fn peak_sampler(stop: Arc<AtomicBool>, peak: Arc<AtomicU64>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let sampler = RssAnonSampler::current();
        while !stop.load(Ordering::Relaxed) {
            if let Some(s) = sampler.sample() {
                peak.fetch_max(s.rss_anon_kb, Ordering::Relaxed);
            }
            thread::sleep(Duration::from_millis(50));
        }
    })
}

/// Production path: bounded build + DiskRunStore + W0 publication + recovery reopen.
fn run_isolated_scale(n: usize, root: &Path) -> ChildReport {
    let label = if n >= 500_000 { "4N" } else { "N" };
    let budget = GenerationBudget::acceptance_linux();
    let build_tmp = root.join("build-tmp");
    let runs_dir = root.join("build-runs");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&wal_dir).expect("wal dir");

    let warmup = RssAnonSampler::current()
        .sample()
        .map(|s| s.rss_anon_kb)
        .unwrap_or(0);
    let peak = Arc::new(AtomicU64::new(warmup));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = peak_sampler(stop.clone(), peak.clone());

    let config = BoundedBuildConfig {
        budget,
        temp_dir: build_tmp.clone(),
        correlation_id: format!("nvs4n-{label}"),
        spool_buf_cap: 1024 * 1024,
        rel_schemas: Vec::new(),
    };
    let mut run_store =
        DiskRunStore::new(&runs_dir, budget, format!("nvs4n-{label}")).expect("DiskRunStore");
    let mut nodes = SyntheticNodes { n, cursor: 0 };
    let mut edges = SyntheticEdges {
        count: n / 2,
        cursor: 0,
        n,
    };

    let mut builder = BoundedGenerationBuilder::new(config);
    let lease = builder
        .build(&mut nodes, &mut edges, &mut run_store)
        .expect("bounded build");

    let payload_len = usize::try_from(lease.exact_len().expect("exact_len")).expect("usize");
    let metrics = lease.metrics().clone();
    let node_count = lease.total_nodes();
    let edge_count = lease.total_edges();

    let wal = WalManager::open(&wal_dir).expect("wal");
    let lock = RootLock::try_acquire(root).expect("root lock");
    let header = GenerationContainerHeader {
        epoch: 1,
        transaction_id: 1,
        node_count,
        edge_count,
    };
    let section_source = StreamingPayloadSectionSource::new(lease);
    let mut sections: Vec<Box<dyn grafeo_storage::file::generation_writer::ExactSectionSource>> =
        vec![Box::new(section_source)];
    let published = publish_generation(
        &lock,
        PublicationInput {
            header,
            sections: &mut sections,
            generation_id: format!("nvs4n-{label}"),
            parent_generation_id: None,
            parent_publication_sequence: None,
        },
        &wal,
        &OsGenerationFileOps,
    )
    .expect("publish");
    drop(lock);

    let lock = RootLock::try_acquire(root).expect("re-lock");
    let selected = recover(&lock).expect("recover");
    drop(lock);

    let manager = GrafeoFileManager::open_read_only(&selected.generation_abs_path).expect("open");
    let section_dir = manager.read_section_directory().unwrap().unwrap();
    let entry = section_dir.find(SectionType::CompactStore).expect("cs section");
    // Production fresh-reopen: mmap the CompactStore section (zero-copy). Do not
    // read_section_data + copy the whole payload into anonymous RAM — that would
    // make peak RssAnon scale with payload size and invalidate the N-vs-4N gate.
    let mmap = manager.mmap_section(entry).expect("mmap compact-store section");
    assert!(
        mmap.len() > 0,
        "CompactStore section must be mmap-able with nonzero length"
    );
    assert_eq!(mmap.section_type(), SectionType::CompactStore);
    // Production serving retains mapped backing; eager v5 deserialize would
    // materialize graph-proportional ColumnCodecs in anonymous RAM.
    let _mapped = std::sync::Arc::new(mmap);

    // Drop publication sections (owns the payload lease / spool files) and the
    // run store before counting leftovers. Sampling covers the full production path.
    drop(sections);
    drop(_mapped);
    drop(manager);
    drop(run_store);
    stop.store(true, Ordering::Relaxed);
    sampler.join().expect("sampler join");

    let leftover_files = count_files_under(&build_tmp) + count_files_under(&runs_dir);
    let outer_sha256 = hex::encode(published.generation_sha256);

    println!(
        "LABEL={label}\nN={n}\nPEAK_RSS_ANON_KB={}\nPAYLOAD_LEN={payload_len}\n\
         TEMP_PEAK={}\nANON_PEAK={}\nMAPPED_PEAK={}\nOUTER_SHA256={outer_sha256}\n\
         LEFTOVER_FILES={leftover_files}",
        peak.load(Ordering::Relaxed),
        metrics.temp_bytes_peak,
        metrics.anon_bytes_peak,
        metrics.mapped_bytes_peak,
    );

    ChildReport {
        label: label.to_string(),
        n,
        peak_rss_anon_kb: peak.load(Ordering::Relaxed),
        payload_len,
        temp_bytes_peak: metrics.temp_bytes_peak,
        anon_bytes_peak: metrics.anon_bytes_peak,
        mapped_bytes_peak: metrics.mapped_bytes_peak,
        outer_sha256,
        leftover_files,
    }
}

fn spawn_child(n: usize) -> ChildReport {
    let exe = std::env::current_exe().expect("current exe");
    let output = Command::new(exe)
        .arg("--exact")
        .arg("n_vs_4n_peak_memory_plateau")
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .env("GRAFEO_NVS4N_N", n.to_string())
        // Force large sort/spool arenas onto mmap'd regions so freed runs are
        // returned to the OS. Without this, glibc heap retention makes peak
        // RssAnon track total bytes processed even when live anon is bounded.
        .env("MALLOC_MMAP_THRESHOLD_", "65536")
        .env("MALLOC_TRIM_THRESHOLD_", "65536")
        .env("MALLOC_ARENA_MAX", "2")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .output()
        .expect("spawn child");

    assert!(
        output.status.success(),
        "child failed: stdout={}",
        String::from_utf8_lossy(&output.stdout)
    );
    ChildReport::parse(&String::from_utf8_lossy(&output.stdout))
}

#[test]
fn n_vs_4n_peak_memory_plateau() {
    if RssAnonSampler::current().sample().is_none() {
        eprintln!("SKIP: /proc RssAnon not available (non-Linux)");
        return;
    }

    if std::env::var(CHILD_ENV).ok().as_deref() == Some("1") {
        let n: usize = std::env::var("GRAFEO_NVS4N_N")
            .expect("scale")
            .parse()
            .expect("parse N");
        let tmp = TempDir::new().expect("child temp root");
        run_isolated_scale(n, tmp.path());
        return;
    }

    let n = 250_000usize;
    let report_n = spawn_child(n);
    let report_4n = spawn_child(n * 4);

    eprintln!("\n=== N-vs-4N ISOLATED CHILD REPORTS ===");
    eprintln!("N:   {report_n:?}");
    eprintln!("4N:  {report_4n:?}");

    for report in [&report_n, &report_4n] {
        assert_eq!(report.leftover_files, 0, "leftover artifacts: {report:?}");
        assert!(report.temp_bytes_peak > 0, "temp peak must be nonzero: {report:?}");
        assert!(report.anon_bytes_peak > 0, "anon peak must be nonzero: {report:?}");
        assert!(report.mapped_bytes_peak > 0, "mapped peak must be nonzero: {report:?}");
    }

    let payload_ratio = report_4n.payload_len as f64 / report_n.payload_len as f64;
    eprintln!("Payload ratio (4N/N): {payload_ratio:.2}x (expect ~4x)");
    assert!(
        (3.0..5.0).contains(&payload_ratio),
        "payload ratio {payload_ratio:.2} outside [3, 5]"
    );

    let rss_ratio = report_4n.peak_rss_anon_kb as f64 / report_n.peak_rss_anon_kb.max(1) as f64;
    eprintln!(
        "Peak RssAnon ratio (4N/N): {rss_ratio:.2}x (raw N={} kB, 4N={} kB)",
        report_n.peak_rss_anon_kb, report_4n.peak_rss_anon_kb
    );
    assert!(
        rss_ratio < 2.0,
        "Peak RssAnon scaled with input: {} kB -> {} kB ({rss_ratio:.2}x)",
        report_n.peak_rss_anon_kb, report_4n.peak_rss_anon_kb
    );

    let anon_limit_kb = 128 * 1024;
    assert!(
        report_4n.peak_rss_anon_kb < anon_limit_kb,
        "Peak RssAnon {} kB exceeds 128 MiB budget",
        report_4n.peak_rss_anon_kb
    );
}

mod hex {
    pub fn encode(bytes: [u8; 32]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
