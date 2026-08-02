//! Shared test fixtures for the W0-B lifecycle modules: fixture graph →
//! streaming section source, and a helper that builds a fresh writable root
//! (tempdir + WAL) for publication/recovery/snapshot/fault tests.

use std::path::Path;

use grafeo_common::types::Value;
use grafeo_core::graph::compact::generation::{
    GenerationBudget, GenerationEdge, GenerationInput, GenerationNode,
};
use tempfile::TempDir;

use crate::file::generation_writer::{ExactSectionSource, GenerationContainerHeader};
use crate::wal::WalManager;

/// Build a streaming CompactStore section + container header from a small
/// multi-table fixture graph.
///
/// With `generation-streaming`, the eager `generate_compact_store` path is
/// sealed (`pub(crate)`). Fixtures must drive the bounded orchestrator and
/// return a lease-backed section source instead.
pub fn fixture_section() -> (Box<dyn ExactSectionSource>, GenerationContainerHeader) {
    let input = GenerationInput::new()
        .node(GenerationNode::new(1u64, "Person").with_prop("name", "Ada"))
        .node(GenerationNode::new(2u64, "Person").with_prop("name", "Bob"))
        .node(GenerationNode::new(3u64, "Person").with_prop("name", "Carol"))
        .node(GenerationNode::new(100u64, "Project").with_prop("title", "Grafeo"))
        .edge(
            GenerationEdge::new(10u64, 1u64, 2u64, "KNOWS").with_prop("since", Value::Int64(2020)),
        )
        .edge(
            GenerationEdge::new(11u64, 2u64, 3u64, "KNOWS").with_prop("since", Value::Int64(2021)),
        )
        .edge(GenerationEdge::new(12u64, 1u64, 100u64, "WORKS_ON"));

    let budget = GenerationBudget::for_tests();

    #[cfg(feature = "generation-streaming")]
    {
        use crate::file::generation_writer::StreamingPayloadSectionSource;
        use crate::generation::DiskRunStore;
        use grafeo_core::graph::compact::generation_builder::orchestrator::{
            BoundedBuildConfig, BoundedGenerationBuilder,
        };

        let temp = TempDir::new().expect("fixture temp dir");
        let config = BoundedBuildConfig {
            budget,
            temp_dir: temp.path().join("build-tmp"),
            correlation_id: "fixture-section".to_string(),
            spool_buf_cap: usize::try_from(budget.io_buffer_bytes).unwrap_or(1024 * 1024),
            rel_schemas: input.rel_schemas.clone(),
        };
        let mut run_store =
            DiskRunStore::new(temp.path().join("build-runs"), budget, "fixture-section")
                .expect("fixture run store");
        let mut builder = BoundedGenerationBuilder::new(config);
        let lease = builder
            .build(
                &mut input.node_source(),
                &mut input.edge_source(),
                &mut run_store,
            )
            .expect("fixture bounded generation must succeed");
        let node_count = lease.total_nodes();
        let edge_count = lease.total_edges();
        // Keep tempdir alive until copy_to streams the lease (held via section).
        // StreamingPayloadSectionSource owns the lease; spilled bodies live under
        // the lease's temp_dir, which the lease cleans on drop. The DiskRunStore
        // root and build-tmp above are owned by `temp` — keep it by leaking into
        // the section lifetime via forget only if needed. Prefer: attach via
        // lease which already owns its spool dir. Run-store dir under `temp` is
        // cleaned when `temp` drops at end of this block — AFTER lease is moved
        // into the section. Run files are finished before lease return, so
        // dropping the run dir here is safe.
        let section = StreamingPayloadSectionSource::new(lease);
        let header = GenerationContainerHeader {
            epoch: 1,
            transaction_id: 1,
            node_count,
            edge_count,
        };
        // Prevent TempDir cleanup from racing with any deferred spool paths
        // still referenced by the lease (lease owns its own temp_dir; run
        // store is already finished). Explicit drop of temp is fine.
        drop(temp);
        (Box::new(section), header)
    }

    #[cfg(not(feature = "generation-streaming"))]
    {
        use crate::file::generation_writer::CompactStoreSectionSource;
        use grafeo_core::graph::compact::generation::generate_compact_store;

        let generated = generate_compact_store(
            &mut input.node_source(),
            &mut input.edge_source(),
            &input.rel_schemas,
            &budget,
        )
        .expect("fixture generation must succeed");

        let node_count = generated.store.total_nodes();
        let edge_count = generated.store.total_edges();
        let section = CompactStoreSectionSource::new(generated.store, generated.global_strings)
            .expect("section source");
        let header = GenerationContainerHeader {
            epoch: 1,
            transaction_id: 1,
            node_count,
            edge_count,
        };
        (Box::new(section), header)
    }
}

/// A fresh writable root: tempdir + opened WAL. The root path is returned
/// alongside the manager; dropping `TempDir` cleans everything up.
pub struct RootFixture {
    /// Temp dir (drop = cleanup).
    pub dir: TempDir,
    /// Real WAL manager on `root/wal`.
    pub wal: WalManager,
}

/// Counts regular files under `root` (recursive).
pub fn count_files_under(root: &Path) -> usize {
    if !root.exists() {
        return 0;
    }
    let mut count = 0;
    for entry in std::fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            count += count_files_under(&path);
        } else {
            count += 1;
        }
    }
    count
}

impl RootFixture {
    /// The writable root path.
    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    /// WAL directory path.
    pub fn wal_dir(&self) -> std::path::PathBuf {
        self.dir.path().join("wal")
    }
}

/// Create a fresh writable root with a WAL open at `root/wal`.
pub fn new_root() -> RootFixture {
    let dir = TempDir::new().expect("tempdir");
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).expect("wal dir");
    let wal = WalManager::open(&wal_dir).expect("wal open");
    RootFixture { dir, wal }
}

/// Every child-mode env var used by the re-exec process tests across the
/// lifecycle modules. A re-exec'd test binary runs the WHOLE suite; any
/// test that spawns children must skip when ANY child marker is present,
/// otherwise children cascade into spawning grandchildren.
pub(crate) const CHILD_ENV_VARS: [&str; 3] = [
    "GRAFEOLOCK_HELPER",
    "GRAFEOPUB_HELPER",
    "GRAFEOROOTPROC_HELPER",
];

/// True when this process is a re-exec'd child of any process test.
pub(crate) fn in_any_child() -> bool {
    CHILD_ENV_VARS.iter().any(|v| std::env::var(v).is_ok())
}
