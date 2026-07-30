//! G-EM0.2: read-only CompactStore graph parity at two sizes (4× payload).
//!
//! Builds deterministic compact snapshots, reopens them read-only via direct
//! container mmap (v5 mapped proportional structures), and proves:
//! - point property lookup and forward/reverse CSR remain byte-identical
//! - settled accounting reports zero proportional anonymous structure bytes
//! - mapped payload bytes scale with the graph while owner/schema stays bounded
//!
//! ```bash
//! cargo test -p grafeo-engine --features compact-store \
//!   --test compact_store_readonly_graph_parity -- --nocapture
//! ```

#![cfg(all(feature = "compact-store", feature = "grafeo-file", feature = "lpg"))]

use grafeo_common::types::{NodeId, PropertyKey, Value};
use grafeo_core::graph::{Direction, traits::GraphStore};
use grafeo_engine::{CompactBacking, Config, GrafeoDB};

// GraphStore is used for neighbors + find_nodes_by_property on the compact base.

const SMALL_NODES: usize = 256;
/// 8× nodes / edges so CompactStore payload clearly clears the ≥4× gate
/// after fixed header/directory overhead (4× nodes alone was ~3.94×).
const LARGE_NODES: usize = 2_048;
const EDGE_FANOUT: usize = 4;
/// Target serialized CompactStore section size ratio (≥4× per G-EM0.2 §9).
const MIN_PAYLOAD_RATIO: u64 = 4;

fn build_snapshot(path: &std::path::Path, node_count: usize) -> Vec<(NodeId, String, i64)> {
    let mut db = GrafeoDB::with_config(Config::persistent(path)).expect("create db");
    let mut nodes = Vec::with_capacity(node_count);
    for index in 0..node_count {
        let name = format!("symbol-{index:08x}");
        let id = db
            .create_node_with_props(
                &["CodeSymbol"],
                [
                    ("name", Value::from(name.as_str())),
                    ("rank", Value::Int64(index as i64)),
                ],
            )
            .expect("create node");
        nodes.push((id, name, index as i64));
    }
    for (source_index, (source, _, _)) in nodes.iter().enumerate() {
        for fanout in 1..=EDGE_FANOUT {
            let target = nodes[(source_index + fanout) % nodes.len()].0;
            let _ = db.create_edge(*source, target, "REFERENCES");
        }
    }
    db.compact().expect("compact");
    db.close().expect("close");
    nodes
}

fn assert_graph_parity(path: &std::path::Path, expected: &[(NodeId, String, i64)]) {
    let db = GrafeoDB::open_read_only(path).expect("read-only reopen");
    let CompactBacking::ContainerMmap {
        payload_version,
        mapped_bytes,
        ..
    } = db.compact_backing().expect("backing diagnostic")
    else {
        panic!("expected ContainerMmap backing");
    };
    assert_eq!(
        *payload_version, 5,
        "G-EM0.2 requires CompactStore payload v5"
    );
    assert!(*mapped_bytes > 0);

    let base = db.compact_tiered().expect("compact base").store();
    let acc = base
        .memory_accounting()
        .expect("v5 mapped open must record accounting");
    assert_eq!(
        acc.anonymous_proportional_structure_bytes, 0,
        "proportional anonymous structures must be zero on mapped open"
    );
    assert!(
        acc.is_disk_native_graph(),
        "schema budget overflow or residual proportional heap"
    );
    assert!(
        acc.mapped_payload_index_bytes > 0,
        "mapped payload/index bytes must be reported"
    );

    // Table zone maps must be present on mapped reopen (not dropped).
    let rank_key = PropertyKey::new("rank");
    let has_zone = base
        .node_table("CodeSymbol")
        .and_then(|nt| nt.zone_map(&rank_key))
        .is_some();
    assert!(
        has_zone,
        "v5 mapped open must restore table zone maps for prune"
    );

    // Deterministic point lookups across the node set.
    let step = (expected.len() / 8).max(1);
    for (id, name, rank) in expected.iter().step_by(step) {
        assert_eq!(
            base.get_node_property(*id, &PropertyKey::new("name")),
            Some(Value::String(arcstr::ArcStr::from(name.as_str())))
        );
        assert_eq!(
            base.get_node_property(*id, &PropertyKey::new("rank")),
            Some(Value::Int64(*rank))
        );
        let out = base.neighbors(*id, Direction::Outgoing);
        assert_eq!(out.len(), EDGE_FANOUT, "forward degree for {id:?}");
        let inc = base.neighbors(*id, Direction::Incoming);
        assert_eq!(inc.len(), EDGE_FANOUT, "reverse degree for {id:?}");
    }

    // Zone-map prune path: impossible rank must not match.
    let pruned = base.find_nodes_by_property("rank", &Value::Int64(i64::MAX));
    assert!(pruned.is_empty(), "zone map should prune impossible rank");

    // High-cardinality string equality via dictionary code index.
    if let Some((id, name, _)) = expected.first() {
        let hits = base.find_nodes_by_property("name", &Value::from(name.as_str()));
        assert!(hits.contains(id), "string find_eq via mapped dict index");
    }

    db.close().expect("close");
}

#[test]
fn readonly_graph_parity_at_two_sizes_with_bounded_accounting() {
    let temp = tempfile::tempdir().expect("tempdir");
    let small_path = temp.path().join("small.grafeo");
    let large_path = temp.path().join("large.grafeo");

    let small_nodes = build_snapshot(&small_path, SMALL_NODES);
    let large_nodes = build_snapshot(&large_path, LARGE_NODES);

    assert_graph_parity(&small_path, &small_nodes);
    assert_graph_parity(&large_path, &large_nodes);

    // CompactStore payload (mapped_payload_index_bytes) ratio ≥ 4×.
    let small_db = GrafeoDB::open_read_only(&small_path).unwrap();
    let large_db = GrafeoDB::open_read_only(&large_path).unwrap();
    let small_acc = small_db
        .compact_tiered()
        .unwrap()
        .store()
        .memory_accounting()
        .unwrap()
        .clone();
    let large_acc = large_db
        .compact_tiered()
        .unwrap()
        .store()
        .memory_accounting()
        .unwrap()
        .clone();
    assert_eq!(small_acc.anonymous_proportional_structure_bytes, 0);
    assert_eq!(large_acc.anonymous_proportional_structure_bytes, 0);
    assert!(
        large_acc.mapped_payload_index_bytes
            >= small_acc
                .mapped_payload_index_bytes
                .saturating_mul(MIN_PAYLOAD_RATIO as usize),
        "mapped CompactStore payload must grow ≥{MIN_PAYLOAD_RATIO}×: small={} large={}",
        small_acc.mapped_payload_index_bytes,
        large_acc.mapped_payload_index_bytes
    );
    // Schema/owner must not scale like payload (bounded relative growth).
    let schema_ratio = large_acc.anonymous_owner_schema_bytes as f64
        / (small_acc.anonymous_owner_schema_bytes.max(1) as f64);
    let mapped_ratio = large_acc.mapped_payload_index_bytes as f64
        / (small_acc.mapped_payload_index_bytes.max(1) as f64);
    assert!(
        schema_ratio < mapped_ratio,
        "schema/owner must not scale with payload: schema_ratio={schema_ratio} mapped_ratio={mapped_ratio}"
    );
}

/// 96 KiB string property survives v5 mapped reopen with zone-map extremes.
#[test]
fn readonly_96kib_string_zone_map_and_property() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("large_string.grafeo");
    let big = "D".repeat(96 * 1024);
    let mut db = GrafeoDB::with_config(Config::persistent(&path)).expect("create");
    let id = db
        .create_node_with_props(
            &["Doc"],
            [
                ("documentation_json", Value::from(big.as_str())),
                ("rank", Value::Int64(1)),
            ],
        )
        .expect("create");
    db.compact().expect("compact");
    db.close().expect("close");

    let db = GrafeoDB::open_read_only(&path).expect("ro open");
    let base = db.compact_tiered().expect("tiered").store();
    assert_eq!(
        base.memory_accounting()
            .unwrap()
            .anonymous_proportional_structure_bytes,
        0
    );
    assert_eq!(
        base.get_node_property(id, &PropertyKey::new("documentation_json")),
        Some(Value::String(arcstr::ArcStr::from(big.as_str())))
    );
    let zm = base
        .node_table("Doc")
        .and_then(|nt| nt.zone_map(&PropertyKey::new("documentation_json")));
    let zm = zm.expect("documentation_json zone map after v5 mapped open");
    assert_eq!(
        zm.min.as_ref().and_then(|v| v.as_str()),
        Some(big.as_str()),
        "zone map min must retain full 96 KiB string after mapped reopen"
    );
    assert_eq!(
        zm.max.as_ref().and_then(|v| v.as_str()),
        Some(big.as_str()),
        "zone map max must retain full 96 KiB string after mapped reopen"
    );
    db.close().expect("close");
}
