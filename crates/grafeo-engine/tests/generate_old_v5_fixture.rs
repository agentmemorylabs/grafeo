//! One-shot fixture generator for R3-M3. Run with feature OFF:
//! cargo test -p grafeo-engine --no-default-features --features compact-store,lpg,mmap,grafeo-file --test generate_old_v5_fixture -- --ignored
#![cfg(all(feature = "compact-store", feature = "lpg", not(feature = "generation-streaming")))]

use std::sync::Arc;
use grafeo_common::storage::section::Section;
use grafeo_core::graph::compact::builder::CompactStoreBuilder;
use grafeo_core::graph::compact::section::CompactStoreSection;

#[test]
#[ignore]
fn generate_old_v5_fixture() {
    // Simple graph: 2 nodes single-label "Person", 1 edge "KNOWS", all-present props.
    // No companions (single label, no nulls) → old-v5 defaults.
    let store = CompactStoreBuilder::new()
        .node_table("Person", |t| {
            t.column_dict("name", &["Alice", "Bob"]);
            t.column_bitpacked("age", &[30, 25], 8);
            t
        })
        .rel_table("KNOWS", "Person", "Person", |r| {
            r.edges(vec![(0u32, 1u32)]);
            r.backward(true);
            r.column_bitpacked("since", &[2020], 16);
            r
        })
        .build()
        .expect("build");
    let section = CompactStoreSection::new(Arc::new(store));
    let payload = section.serialize().expect("serialize v5");
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/old_v5_single_label.bin");
    std::fs::write(path, &payload).expect("write fixture");
    eprintln!("Wrote {} bytes to {}", payload.len(), path);
}
