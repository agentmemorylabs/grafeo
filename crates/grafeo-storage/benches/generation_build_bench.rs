//! Generation build resource benchmark (G-EM0.W0-A4).
//!
//! Measures the streaming generation container write path with
//! production-shaped cardinalities. Reports phase timings via criterion.
//!
//! NOTE: Same-process RssAnon delta is NOT accepted peak proof per the
//! W0 contract. The fresh-child pattern (W0-B / G-EM0.5d) provides the
//! authoritative measurement.

#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
// reason: criterion_group! expansion does not carry doc comments on the
// generated wrapper functions.
#![allow(missing_docs)]

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use grafeo_common::types::Value;
use grafeo_core::graph::compact::generation::{
    GenerationBudget, GenerationEdge, GenerationInput, GenerationNode, generate_compact_store,
};
use grafeo_storage::file::generation_writer::{
    CompactStoreSectionSource, GenerationContainerHeader, OsGenerationFileOps,
    create_versioned_sections_streaming,
};

/// Build a production-shaped fixture: sparse IDs, multiple tables.
fn production_shaped_input(node_count: u64, edge_count: u64) -> GenerationInput {
    let mut input = GenerationInput::new();

    // Sparse IDs: multiply by 1000 to simulate real-world gaps.
    for i in 0..node_count {
        let id = (i + 1) * 1000;
        let label = match i % 3 {
            0 => "Person",
            1 => "Project",
            _ => "Organization",
        };
        let node = GenerationNode::new(id, label)
            .with_prop("name", format!("entity_{i}"))
            .with_prop("score", Value::Int64(i as i64));
        input = input.node(node);
    }

    for i in 0..edge_count {
        let src_idx = i % node_count;
        let dst_idx = (i + 1) % node_count;
        let src_id = (src_idx + 1) * 1000;
        let dst_id = (dst_idx + 1) * 1000;
        let edge_type = match i % 2 {
            0 => "KNOWS",
            _ => "WORKS_ON",
        };
        let edge = GenerationEdge::new((i + 1) * 100, src_id, dst_id, edge_type)
            .with_prop("weight", Value::Float64(i as f64 * 0.1));
        input = input.edge(edge);
    }

    input
}

fn bench_generation_container_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("generation_build");
    group.sample_size(10);

    // Cardinality 1: 1k nodes, 5k edges.
    let input_1k = production_shaped_input(1_000, 5_000);
    group.bench_function("container_write_1k_nodes_5k_edges", |b| {
        b.iter(|| {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("bench.grafeo");

            let budget = GenerationBudget::for_tests();
            let generated = generate_compact_store(black_box(&input_1k), &budget).unwrap();

            let section =
                CompactStoreSectionSource::new(&generated.store, &generated.global_strings)
                    .unwrap();

            let header = GenerationContainerHeader {
                epoch: 1,
                transaction_id: 1,
                node_count: generated.store.total_nodes(),
                edge_count: generated.store.total_edges(),
            };

            let mut sections: Vec<
                Box<dyn grafeo_storage::file::generation_writer::ExactSectionSource>,
            > = vec![Box::new(section)];

            create_versioned_sections_streaming(
                &path,
                &header,
                &mut sections,
                &OsGenerationFileOps,
            )
            .unwrap();
        });
    });

    // Cardinality 2: 10k nodes, 50k edges.
    let input_10k = production_shaped_input(10_000, 50_000);
    group.bench_function("container_write_10k_nodes_50k_edges", |b| {
        b.iter(|| {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("bench.grafeo");

            let budget = GenerationBudget::for_tests();
            let generated = generate_compact_store(black_box(&input_10k), &budget).unwrap();

            let section =
                CompactStoreSectionSource::new(&generated.store, &generated.global_strings)
                    .unwrap();

            let header = GenerationContainerHeader {
                epoch: 1,
                transaction_id: 1,
                node_count: generated.store.total_nodes(),
                edge_count: generated.store.total_edges(),
            };

            let mut sections: Vec<
                Box<dyn grafeo_storage::file::generation_writer::ExactSectionSource>,
            > = vec![Box::new(section)];

            create_versioned_sections_streaming(
                &path,
                &header,
                &mut sections,
                &OsGenerationFileOps,
            )
            .unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_generation_container_write);
criterion_main!(benches);
