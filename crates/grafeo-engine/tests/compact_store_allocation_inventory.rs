//! G-EM0.R0: fresh-process CompactStore allocation inventory.
//!
//! This is a diagnostic guard for the current v4 reopen path, not an
//! acceptance test for a disk-backed implementation. It creates two persisted
//! compact bases whose CompactStore payloads differ by at least 4x, then opens
//! each one in a fresh test process and records the Linux `smaps_rollup`
//! anonymous-memory delta immediately after `GrafeoDB::with_config` returns.
//!
//! The test deliberately starts its memory sample before the normal container
//! open path. That path currently reads the complete CompactStore section into
//! an owned `Vec<u8>`, copies it at the `Section::deserialize` boundary, and
//! reconstructs proportional graph structures. It is evidence for the v5
//! layout decision, not evidence that the current path is disk-native.
//!
//! ```bash
//! cargo test -p grafeo-engine --features compact-store \
//!   --test compact_store_allocation_inventory -- --nocapture
//! ```

#![cfg(all(feature = "compact-store", feature = "grafeo-file", feature = "lpg"))]

#[cfg(target_os = "linux")]
mod linux {
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    use grafeo_common::storage::SectionType;
    use grafeo_common::types::Value;
    use grafeo_core::graph::GraphStore;
    use grafeo_engine::{Config, GrafeoDB};

    const SMALL_NODE_COUNT: usize = 1_024;
    const LARGE_NODE_COUNT: usize = 8_192;
    const EDGE_FANOUT: usize = 4;
    const CHILD_SNAPSHOT_ENV: &str = "GRAFEO_R0_SNAPSHOT_PATH";
    const CHILD_RESULT_ENV: &str = "GRAFEO_R0_RESULT_PATH";

    #[derive(Debug, PartialEq, Eq)]
    struct OpenInventory {
        node_count: u64,
        edge_count: u64,
        compact_section_bytes: u64,
        estimated_compact_heap_bytes: u64,
        anonymous_before_kib: u64,
        anonymous_after_open_kib: u64,
        anonymous_delta_kib: u64,
    }

    impl OpenInventory {
        fn write_to(&self, path: &Path) {
            let report = format!(
                "node_count={}\nedge_count={}\ncompact_section_bytes={}\nestimated_compact_heap_bytes={}\nanonymous_before_kib={}\nanonymous_after_open_kib={}\nanonymous_delta_kib={}\n",
                self.node_count,
                self.edge_count,
                self.compact_section_bytes,
                self.estimated_compact_heap_bytes,
                self.anonymous_before_kib,
                self.anonymous_after_open_kib,
                self.anonymous_delta_kib,
            );
            fs::write(path, report).expect("write child allocation inventory");
        }

        fn read_from(path: &Path) -> Self {
            let report = fs::read_to_string(path).expect("read child allocation inventory");
            Self {
                node_count: read_field(&report, "node_count"),
                edge_count: read_field(&report, "edge_count"),
                compact_section_bytes: read_field(&report, "compact_section_bytes"),
                estimated_compact_heap_bytes: read_field(&report, "estimated_compact_heap_bytes"),
                anonymous_before_kib: read_field(&report, "anonymous_before_kib"),
                anonymous_after_open_kib: read_field(&report, "anonymous_after_open_kib"),
                anonymous_delta_kib: read_field(&report, "anonymous_delta_kib"),
            }
        }
    }

    fn read_field(report: &str, name: &str) -> u64 {
        report
            .lines()
            .find_map(|line| line.split_once('=').filter(|(key, _)| *key == name))
            .unwrap_or_else(|| panic!("missing {name} in child allocation inventory: {report}"))
            .1
            .parse()
            .unwrap_or_else(|err| panic!("invalid {name} in child allocation inventory: {err}"))
    }

    fn private_anonymous_kib() -> u64 {
        let rollup = fs::read_to_string("/proc/self/smaps_rollup")
            .expect("Linux allocation inventory requires /proc/self/smaps_rollup");
        rollup
            .lines()
            .find_map(|line| {
                let mut fields = line.split_whitespace();
                (fields.next() == Some("Anonymous:"))
                    .then(|| fields.next())
                    .flatten()
                    .and_then(|kib| kib.parse::<u64>().ok())
            })
            .expect("smaps_rollup must contain Anonymous")
    }

    fn build_snapshot(path: &Path, node_count: usize) {
        let mut db = GrafeoDB::with_config(Config::persistent(path)).expect("create persistent db");
        let mut nodes = Vec::with_capacity(node_count);

        for index in 0..node_count {
            let name = format!("symbol-{index:08x}");
            let node = db
                .create_node_with_props(
                    &["CodeSymbol"],
                    [
                        ("name", Value::from(name.as_str())),
                        ("rank", Value::Int64(index as i64)),
                    ],
                )
                .expect("create deterministic node");
            nodes.push(node);
        }

        for (source_index, source) in nodes.iter().copied().enumerate() {
            for fanout in 1..=EDGE_FANOUT {
                let target = nodes[(source_index + fanout) % nodes.len()];
                let _edge_id = db.create_edge(source, target, "REFERENCES");
            }
        }

        db.compact().expect("compact deterministic graph");
        db.close().expect("explicitly close compact snapshot");
    }

    fn compact_section_bytes(db: &GrafeoDB) -> u64 {
        let file_manager = db
            .file_manager()
            .expect("persistent database must retain a file manager");
        let directory = file_manager
            .read_section_directory()
            .expect("read section directory")
            .expect("compact snapshot must have a section directory");
        directory
            .find(SectionType::CompactStore)
            .expect("compact snapshot must contain CompactStore")
            .length
    }

    fn open_inventory(snapshot: &Path) -> OpenInventory {
        let anonymous_before_kib = private_anonymous_kib();
        let db = GrafeoDB::with_config(Config::persistent(snapshot)).expect("fresh-process reopen");
        let anonymous_after_open_kib = private_anonymous_kib();

        let compact_section_bytes = compact_section_bytes(&db);
        let base = db
            .layered_store()
            .expect("reopen must restore the layered CompactStore")
            .base_store_arc();
        let inventory = OpenInventory {
            node_count: base.node_count() as u64,
            edge_count: base.edge_count() as u64,
            compact_section_bytes,
            estimated_compact_heap_bytes: base.memory_bytes() as u64,
            anonymous_before_kib,
            anonymous_after_open_kib,
            anonymous_delta_kib: anonymous_after_open_kib.saturating_sub(anonymous_before_kib),
        };

        db.close().expect("explicitly close fresh-process reopen");
        inventory
    }

    #[test]
    fn r0_child_reopen_inventory() {
        let Some(snapshot) = env::var_os(CHILD_SNAPSHOT_ENV) else {
            return;
        };
        let result = env::var_os(CHILD_RESULT_ENV).expect("child result path must be set");
        let inventory = open_inventory(Path::new(&snapshot));
        inventory.write_to(Path::new(&result));
    }

    fn run_fresh_process_inventory(snapshot: &Path, result: &Path) -> OpenInventory {
        let executable = env::current_exe().expect("locate allocation inventory test binary");
        let child = Command::new(executable)
            .args(["--exact", "linux::r0_child_reopen_inventory", "--nocapture"])
            .env(CHILD_SNAPSHOT_ENV, snapshot)
            .env(CHILD_RESULT_ENV, result)
            .output()
            .expect("run fresh allocation inventory process");

        assert!(
            child.status.success(),
            "fresh allocation inventory child failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr),
        );
        OpenInventory::read_from(result)
    }

    #[test]
    fn allocation_inventory_shows_v4_reopen_scales_with_compact_payload() {
        let temp = tempfile::tempdir().expect("allocation inventory tempdir");
        let small_snapshot = temp.path().join("small.grafeo");
        let large_snapshot = temp.path().join("large.grafeo");
        let small_result = temp.path().join("small.inventory");
        let large_result = temp.path().join("large.inventory");

        build_snapshot(&small_snapshot, SMALL_NODE_COUNT);
        build_snapshot(&large_snapshot, LARGE_NODE_COUNT);

        let small = run_fresh_process_inventory(&small_snapshot, &small_result);
        let large = run_fresh_process_inventory(&large_snapshot, &large_result);

        assert_eq!(small.node_count, SMALL_NODE_COUNT as u64);
        assert_eq!(large.node_count, LARGE_NODE_COUNT as u64);
        assert_eq!(small.edge_count, (SMALL_NODE_COUNT * EDGE_FANOUT) as u64);
        assert_eq!(large.edge_count, (LARGE_NODE_COUNT * EDGE_FANOUT) as u64);
        assert!(
            large.compact_section_bytes >= small.compact_section_bytes * 4,
            "R0 requires a >=4x CompactStore-size comparison; small={small:?}, large={large:?}",
        );
        assert!(
            large.estimated_compact_heap_bytes > small.estimated_compact_heap_bytes,
            "reported retained CompactStore bytes must grow with graph cardinality; small={small:?}, large={large:?}",
        );
        assert!(
            large.anonymous_delta_kib > small.anonymous_delta_kib,
            "fresh-process anonymous reopen delta must expose the current proportional allocation path; small={small:?}, large={large:?}",
        );

        eprintln!("G-EM0.R0 CompactStore allocation inventory");
        eprintln!("small: {small:?}");
        eprintln!("large: {large:?}");
    }
}

#[cfg(not(target_os = "linux"))]
#[test]
fn allocation_inventory_requires_linux_smaps_rollup() {
    eprintln!(
        "G-EM0.R0 allocation inventory uses Linux /proc/self/smaps_rollup; run the accepted measurement on Linux"
    );
}
