//! Shared test fixtures for the W0-B lifecycle modules: fixture graph →
//! streaming section source, and a helper that builds a fresh writable root
//! (tempdir + WAL) for publication/recovery/snapshot/fault tests.

use std::path::Path;

use grafeo_common::types::Value;
use grafeo_core::graph::compact::generation::{
    GenerationBudget, GenerationEdge, GenerationInput, GenerationNode, generate_compact_store,
};
use tempfile::TempDir;

use crate::file::generation_writer::{
    CompactStoreSectionSource, ExactSectionSource, GenerationContainerHeader,
};
use crate::wal::WalManager;

/// Build a streaming CompactStore section + container header from a small
/// multi-table fixture graph.
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

/// A fresh writable root: tempdir + opened WAL. The root path is returned
/// alongside the manager; dropping `TempDir` cleans everything up.
pub struct RootFixture {
    /// Temp dir (drop = cleanup).
    pub dir: TempDir,
    /// Real WAL manager on `root/wal`.
    pub wal: WalManager,
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
