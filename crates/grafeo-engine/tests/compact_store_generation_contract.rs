//! Cross-crate production-open contract test (G-EM0.W0-A4).
//!
//! Proves that a `.grafeo` container written via the streaming generation
//! writer opens through the production `GrafeoFileManager::open_read_only`
//! path, deserializes via the public `CompactStoreSection` API, supports
//! direct mmap, and opens cleanly in a fresh child process with bounded RssAnon.

use bytes::Bytes;
use grafeo_common::types::Value;
use grafeo_core::graph::compact::generation::{
    GenerationBudget, GenerationEdge, GenerationInput, GenerationNode, generate_compact_store,
};
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_storage::file::GrafeoFileManager;
use grafeo_storage::file::generation_writer::{
    CompactStoreSectionSource, GenerationContainerHeader, OsGenerationFileOps,
    create_versioned_sections_streaming,
};
use tempfile::TempDir;

/// Build a fixture graph with multiple tables, properties, and edges.
fn fixture_input() -> GenerationInput {
    GenerationInput::new()
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
        .edge(GenerationEdge::new(12u64, 1u64, 100u64, "WORKS_ON"))
}

/// Write a fixture graph to a `.grafeo` container via the streaming writer.
fn write_fixture_container(path: &std::path::Path) {
    let input = fixture_input();
    let budget = GenerationBudget::for_tests();
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &budget,
    )
    .unwrap();

    let node_count = generated.store.total_nodes();
    let edge_count = generated.store.total_edges();

    let section =
        CompactStoreSectionSource::new(generated.store, generated.global_strings).unwrap();

    let header = GenerationContainerHeader {
        epoch: 1,
        transaction_id: 1,
        node_count,
        edge_count,
    };

    let mut sections: Vec<Box<dyn grafeo_storage::file::generation_writer::ExactSectionSource>> =
        vec![Box::new(section)];

    create_versioned_sections_streaming(path, &header, &mut sections, &OsGenerationFileOps)
        .unwrap();
}

#[test]
fn streaming_container_opens_via_production_read_only() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.grafeo");

    write_fixture_container(&path);

    // Production open path.
    let manager = GrafeoFileManager::open_read_only(&path).unwrap();
    assert!(manager.is_read_only());

    // Read section directory.
    let dir_opt = manager.read_section_directory().unwrap();
    let section_dir = dir_opt.expect("v2 container must have a section directory");
    let entry = section_dir
        .find(grafeo_common::storage::SectionType::CompactStore)
        .expect("CompactStore section must exist");
    assert!(entry.length > 0);
    assert!(entry.flags.mmap_able);
}

#[test]
fn streaming_container_deserializes_v5_with_query_parity() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.grafeo");

    let input = fixture_input();
    let budget = GenerationBudget::for_tests();
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &budget,
    )
    .unwrap();
    let original = &generated.store;

    write_fixture_container(&path);

    // Read back through production path.
    let manager = GrafeoFileManager::open_read_only(&path).unwrap();
    let section_dir = manager.read_section_directory().unwrap().unwrap();
    let entry = section_dir
        .find(grafeo_common::storage::SectionType::CompactStore)
        .unwrap();

    // Read section data and deserialize via public CompactStoreSection API.
    let data = manager.read_section_data(entry).unwrap();
    let mut cs_section = CompactStoreSection::empty();
    cs_section
        .deserialize_from_bytes(Bytes::from(data))
        .expect("v5 payload must deserialize through public API");
    let restored = cs_section.store().expect("store must be present");

    // Query parity: node counts.
    assert_eq!(restored.total_nodes(), original.total_nodes());
    assert_eq!(restored.total_edges(), original.total_edges());
    assert_eq!(restored.total_nodes(), 4);
    assert_eq!(restored.total_edges(), 3);

    // Preserves IDs.
    assert!(restored.preserves_ids());

    // Node table parity.
    let person_orig = original.node_table("Person").unwrap();
    let person_rest = restored.node_table("Person").unwrap();
    assert_eq!(person_orig.len(), person_rest.len());
    assert_eq!(person_rest.len(), 3);

    let project_orig = original.node_table("Project").unwrap();
    let project_rest = restored.node_table("Project").unwrap();
    assert_eq!(project_orig.len(), project_rest.len());
    assert_eq!(project_rest.len(), 1);

    // Edge type parity.
    let knows_orig = original.rel_table("KNOWS").unwrap();
    let knows_rest = restored.rel_table("KNOWS").unwrap();
    assert_eq!(knows_orig.num_edges(), knows_rest.num_edges());
    assert_eq!(knows_rest.num_edges(), 2);

    let works_orig = original.rel_table("WORKS_ON").unwrap();
    let works_rest = restored.rel_table("WORKS_ON").unwrap();
    assert_eq!(works_orig.num_edges(), works_rest.num_edges());
    assert_eq!(works_rest.num_edges(), 1);

    // Label/type lookup parity.
    assert!(restored.label_for_table_id(0).is_some());
    assert!(restored.edge_type_for_rel_table_id(0).is_some());
}

#[test]
fn streaming_container_direct_mmap() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.grafeo");

    write_fixture_container(&path);

    let manager = GrafeoFileManager::open_read_only(&path).unwrap();
    let section_dir = manager.read_section_directory().unwrap().unwrap();
    let entry = section_dir
        .find(grafeo_common::storage::SectionType::CompactStore)
        .unwrap();

    // Direct mmap (production path).
    let mmap = manager.mmap_section(entry).unwrap();
    assert!(
        mmap.len() > 0,
        "mapped_bytes must be > 0 for CompactStore section"
    );
    assert_eq!(
        mmap.section_type(),
        grafeo_common::storage::SectionType::CompactStore
    );

    // Deserialize from mmap'd bytes via public API.
    let bytes = Bytes::copy_from_slice(mmap.as_bytes());
    let mut cs_section = CompactStoreSection::empty();
    cs_section
        .deserialize_from_bytes(bytes)
        .expect("mmap'd v5 payload must deserialize");
    let restored = cs_section.store().unwrap();
    assert_eq!(restored.total_nodes(), 4);
    assert_eq!(restored.total_edges(), 3);
}

#[test]
fn pre_existing_target_path_fails_closed() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.grafeo");

    // Create the file first.
    std::fs::write(&path, b"existing").unwrap();

    let input = fixture_input();
    let budget = GenerationBudget::for_tests();
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &budget,
    )
    .unwrap();

    let section =
        CompactStoreSectionSource::new(generated.store, generated.global_strings).unwrap();

    let header = GenerationContainerHeader {
        epoch: 1,
        transaction_id: 1,
        node_count: 0,
        edge_count: 0,
    };

    let mut sections: Vec<Box<dyn grafeo_storage::file::generation_writer::ExactSectionSource>> =
        vec![Box::new(section)];

    let result =
        create_versioned_sections_streaming(&path, &header, &mut sections, &OsGenerationFileOps);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("already exists"),
        "error must mention pre-existing path: {err}"
    );
}

#[test]
fn short_section_source_fails_closed() {
    use grafeo_storage::file::generation_writer::ExactSectionSource;
    use std::io::Write;

    /// A section source that declares 100 bytes but writes only 10.
    struct ShortSource;
    impl ExactSectionSource for ShortSource {
        fn section_type(&self) -> grafeo_common::storage::SectionType {
            grafeo_common::storage::SectionType::CompactStore
        }
        fn directory_version(&self) -> u8 {
            5
        }
        fn exact_len(&self) -> u64 {
            100
        }
        fn copy_to(&mut self, sink: &mut dyn Write) -> grafeo_common::utils::error::Result<()> {
            sink.write_all(&[0u8; 10])
                .map_err(grafeo_common::utils::error::Error::Io)
        }
    }

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("short.grafeo");

    let header = GenerationContainerHeader {
        epoch: 1,
        transaction_id: 1,
        node_count: 0,
        edge_count: 0,
    };

    let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![Box::new(ShortSource)];

    let result =
        create_versioned_sections_streaming(&path, &header, &mut sections, &OsGenerationFileOps);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("declared exact_len 100 but wrote 10"),
        "error must mention length mismatch: {err}"
    );
}

#[test]
fn long_section_source_fails_closed() {
    use grafeo_storage::file::generation_writer::ExactSectionSource;
    use std::io::Write;

    /// A section source that declares 10 bytes but writes 100.
    struct LongSource;
    impl ExactSectionSource for LongSource {
        fn section_type(&self) -> grafeo_common::storage::SectionType {
            grafeo_common::storage::SectionType::CompactStore
        }
        fn directory_version(&self) -> u8 {
            5
        }
        fn exact_len(&self) -> u64 {
            10
        }
        fn copy_to(&mut self, sink: &mut dyn Write) -> grafeo_common::utils::error::Result<()> {
            sink.write_all(&[0u8; 100])
                .map_err(grafeo_common::utils::error::Error::Io)
        }
    }

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("long.grafeo");

    let header = GenerationContainerHeader {
        epoch: 1,
        transaction_id: 1,
        node_count: 0,
        edge_count: 0,
    };

    let mut sections: Vec<Box<dyn ExactSectionSource>> = vec![Box::new(LongSource)];

    let result =
        create_versioned_sections_streaming(&path, &header, &mut sections, &OsGenerationFileOps);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("declared exact_len 10 but wrote 100"),
        "error must mention length mismatch: {err}"
    );
}

fn read_rss_anon_kb() -> u64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("RssAnon:") || line.starts_with("VmHWM:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(val) = parts[1].parse::<u64>() {
                        return val;
                    }
                }
            }
        }
    }
    0
}

fn build_large_fixture_container(path: &std::path::Path) {
    let mut input = GenerationInput::new();
    for i in 0..10_000u64 {
        input = input.node(
            GenerationNode::new(i + 1, "User")
                .with_prop("name", format!("User_{i}"))
                .with_prop("score", Value::Int64(i as i64)),
        );
    }
    for i in 0..50_000u64 {
        let src = (i % 10_000) + 1;
        let dst = ((i * 7 + 3) % 10_000) + 1;
        input = input.edge(
            GenerationEdge::new(i + 1, src, dst, "LINK")
                .with_prop("weight", Value::Int64((i % 100) as i64)),
        );
    }
    let budget = GenerationBudget::for_tests();
    let generated = generate_compact_store(
        &mut input.node_source(),
        &mut input.edge_source(),
        &input.rel_schemas,
        &budget,
    )
    .unwrap();

    let node_count = generated.store.total_nodes();
    let edge_count = generated.store.total_edges();

    let section =
        CompactStoreSectionSource::new(generated.store, generated.global_strings).unwrap();

    let header = GenerationContainerHeader {
        epoch: 1,
        transaction_id: 1,
        node_count,
        edge_count,
    };

    let mut sections: Vec<Box<dyn grafeo_storage::file::generation_writer::ExactSectionSource>> =
        vec![Box::new(section)];

    create_versioned_sections_streaming(path, &header, &mut sections, &OsGenerationFileOps)
        .unwrap();
}

#[test]
fn fresh_child_mmap_and_rss_anon_check() {
    let args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|a| a == "--child-mmap-check") {
        if pos + 1 < args.len() {
            let path_str = &args[pos + 1];
            let path = std::path::Path::new(path_str);
            let manager = GrafeoFileManager::open_read_only(path).expect("open container");
            let section_dir = manager.read_section_directory().unwrap().unwrap();
            let entry = section_dir
                .find(grafeo_common::storage::SectionType::CompactStore)
                .unwrap();
            let mmap = manager.mmap_section(entry).expect("mmap section");
            let mapped_bytes = mmap.len();
            let rss_anon_kb = read_rss_anon_kb();

            println!("MAPPED_BYTES={mapped_bytes} RSS_ANON_KB={rss_anon_kb}");
            std::process::exit(0);
        }
    }

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("large_10k_50k.grafeo");

    build_large_fixture_container(&path);

    let exe = std::env::current_exe().expect("current exe");
    let output = std::process::Command::new(exe)
        .arg("--exact")
        .arg("fresh_child_mmap_and_rss_anon_check")
        .arg("--nocapture")
        .arg("--")
        .arg("--child-mmap-check")
        .arg(path.to_str().unwrap())
        .output()
        .expect("spawn child process");

    assert!(
        output.status.success(),
        "child process must exit 0: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("MAPPED_BYTES="),
        "stdout must contain MAPPED_BYTES: {stdout}"
    );
    assert!(
        stdout.contains("RSS_ANON_KB="),
        "stdout must contain RSS_ANON_KB: {stdout}"
    );

    let mut mapped_bytes = 0u64;
    let mut rss_anon_kb = 0u64;

    for line in stdout.lines() {
        if line.contains("MAPPED_BYTES=") && line.contains("RSS_ANON_KB=") {
            for part in line.split_whitespace() {
                if let Some(val) = part.strip_prefix("MAPPED_BYTES=") {
                    mapped_bytes = val.parse().unwrap_or(0);
                } else if let Some(val) = part.strip_prefix("RSS_ANON_KB=") {
                    rss_anon_kb = val.parse().unwrap_or(0);
                }
            }
        }
    }

    assert!(mapped_bytes > 0, "mapped_bytes must be > 0");
    assert!(
        rss_anon_kb <= 196_608,
        "RSS_ANON_KB must be <= 192 MiB (196608 KB), got {rss_anon_kb}"
    );
}
