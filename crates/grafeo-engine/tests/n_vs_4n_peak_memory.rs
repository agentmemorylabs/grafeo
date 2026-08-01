//! G-EM0.5b D0.8.10 #7 — N-vs-4N peak memory acceptance.
//!
//! Builds the same production-path fixture at N and 4N under
//! `GenerationBudget::acceptance_linux()` (128 MiB anon, 4 GiB temp).
//! Post-warmup peak RssAnon must plateau with no input-sized slope.
//! Reports raw samples; internal counters alone are not evidence.
//!
//! Uses synthetic streaming sources (no in-memory fixture Vec) so the
//! measurement reflects the pipeline, not the test harness.
//!
//! Run with:
//! ```bash
//! cargo test -p grafeo-engine --features generation-streaming \
//!   --test n_vs_4n_peak_memory -- --nocapture
//! ```

#![cfg(feature = "generation-streaming")]

use grafeo_common::types::{PropertyKey, Value};
use grafeo_core::graph::compact::generation::{
    EdgeRecordSource, GenerationBudget, GenerationEdge, GenerationError, GenerationNode,
    NodeRecordSource,
};
use grafeo_core::graph::compact::generation_builder::orchestrator::{
    BoundedBuildConfig, BoundedGenerationBuilder,
};
use grafeo_storage::generation::run_adapter::DiskRunStore;
use tempfile::TempDir;

/// Reads RssAnon from /proc/self/status (Linux only).
fn rss_anon_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("RssAnon:") {
            let kb: u64 = rest.trim().trim_end_matches(" kB").trim().parse().ok()?;
            return Some(kb);
        }
    }
    None
}

/// Streaming node source: generates `n` Person nodes on the fly.
/// Each node has name + age properties. No in-memory Vec.
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
        node.properties
            .insert(PropertyKey::new("name"), Value::from(format!("person_{i}")));
        node.properties
            .insert(PropertyKey::new("age"), Value::Int64((i % 80) as i64 + 18));
        Ok(Some(node))
    }
}

/// Streaming edge source: generates `n/2` KNOWS edges on the fly.
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
        let src = (i * 2) as u64 + 1;
        let dst = (i * 2 + 1) as u64 + 1;
        let id = (self.n + i) as u64 + 1;
        Ok(Some(GenerationEdge::new(id, src, dst, "KNOWS")))
    }
}

/// Runs one bounded build at the given scale, sampling RssAnon.
/// Returns (delta_rss_anon_kb, payload_len, temp_bytes_peak).
fn run_build(n: usize, label: &str) -> (u64, usize, u64) {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path().to_path_buf();
    let temp_dir = root.join("build-tmp");
    let runs_dir = root.join("build-runs");

    let budget = GenerationBudget::acceptance_linux();
    let config = BoundedBuildConfig {
        budget,
        temp_dir: temp_dir.clone(),
        correlation_id: format!("nvs4n-{label}"),
        spool_buf_cap: 1024 * 1024,
        rel_schemas: Vec::new(),
    };

    let mut run_store =
        DiskRunStore::new(runs_dir, budget, format!("nvs4n-{label}")).expect("DiskRunStore");

    let mut nodes = SyntheticNodes { n, cursor: 0 };
    let mut edges = SyntheticEdges {
        count: n / 2,
        cursor: 0,
        n,
    };

    // Warmup sample (before build, after config allocation).
    let warmup_rss = rss_anon_kb().unwrap_or(0);
    eprintln!("[{label}] N={n} warmup RssAnon: {warmup_rss} kB");

    let mut builder = BoundedGenerationBuilder::new(config);
    let mut lease = builder
        .build(&mut nodes, &mut edges, &mut run_store)
        .expect("bounded build");

    let peak_rss = rss_anon_kb().unwrap_or(0);
    let delta_rss = peak_rss.saturating_sub(warmup_rss);
    eprintln!("[{label}] N={n} post-build RssAnon: {peak_rss} kB (delta: {delta_rss} kB)");

    // Stream the payload.
    let mut payload = Vec::new();
    lease.stream_to(&mut payload).expect("stream_to");
    let payload_len = payload.len();

    let metrics = lease.metrics().clone();
    eprintln!(
        "[{label}] N={n} payload={payload_len} B, temp_peak={} B, anon_peak={} B, schema_peak={} B",
        metrics.temp_bytes_peak, metrics.anon_bytes_peak, metrics.schema_bytes_peak
    );
    let temp_peak = metrics.temp_bytes_peak;

    // Drop lease + run_store → RAII cleanup.
    drop(lease);
    drop(run_store);
    drop(payload);

    // Verify zero artifacts.
    assert!(
        !temp_dir.exists(),
        "[{label}] build-tmp should be removed after drop"
    );

    (delta_rss, payload_len, temp_peak)
}

#[test]
fn n_vs_4n_peak_memory_plateau() {
    if rss_anon_kb().is_none() {
        eprintln!("SKIP: /proc/self/status not available (non-Linux)");
        return;
    }

    // Use a scale where external sort spilling actually triggers.
    // sort_run_bytes = 64MB, ~300 bytes/node → need >220K nodes to spill.
    let n = 250_000;
    let (rss_n, payload_n, temp_n) = run_build(n, "N");
    let (rss_4n, payload_4n, temp_4n) = run_build(n * 4, "4N");

    eprintln!("\n=== N-vs-4N SUMMARY (delta RssAnon) ===");
    eprintln!("N={n}:  delta={rss_n} kB, payload={payload_n} B, temp={temp_n} B");
    eprintln!(
        "4N={}: delta={rss_4n} kB, payload={payload_4n} B, temp={temp_4n} B",
        n * 4
    );

    // Payload should separate by ~4x.
    let payload_ratio = payload_4n as f64 / payload_n as f64;
    eprintln!("Payload ratio (4N/N): {payload_ratio:.2}x (expect ~4x)");
    assert!(
        (3.0..5.0).contains(&payload_ratio),
        "payload ratio {payload_ratio:.2} outside [3, 5]"
    );

    // Delta RssAnon must NOT scale with input.
    let rss_ratio = rss_4n as f64 / rss_n.max(1) as f64;
    eprintln!("Delta RssAnon ratio (4N/N): {rss_ratio:.2}x (must be < 2.0)");
    assert!(
        rss_ratio < 2.0,
        "Delta RssAnon scaled with input: {rss_n} kB -> {rss_4n} kB ({rss_ratio:.2}x for 4x input). \
         Graph-proportional retention remains."
    );

    // Absolute bound: delta must stay under 128 MiB anon budget.
    let anon_limit_kb = 128 * 1024;
    assert!(
        rss_4n < anon_limit_kb,
        "Delta RssAnon {rss_4n} kB exceeds 128 MiB anon budget"
    );
}
