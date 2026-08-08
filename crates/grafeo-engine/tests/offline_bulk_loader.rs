//! Offline bulk-loader integration tests.
//!
//! Gated to match `GrafeoDB::bulk_load_nodes_with_props_unindexed`, which is
//! only available without `temporal`.
#![cfg(not(feature = "temporal"))]

use std::collections::HashMap;

use grafeo_common::types::{PropertyKey, Value};
use grafeo_engine::GrafeoDB;

#[test]
fn bulk_loader_preserves_rows_and_node_ids_without_secondary_indexes() {
    let db = GrafeoDB::new_in_memory();
    let rows = (0..10_000)
        .map(|index| {
            HashMap::from([
                (PropertyKey::new("key"), Value::from(format!("row-{index}"))),
                (PropertyKey::new("ordinal"), Value::from(i64::from(index))),
            ])
        })
        .collect();

    let ids = db
        .bulk_load_nodes_with_props_unindexed("BulkRow", rows)
        .expect("fresh store accepts an offline bulk load");

    assert_eq!(ids.len(), 10_000);
    assert_eq!(db.graph_store().nodes_by_label("BulkRow").len(), 10_000);
    assert_eq!(
        db.graph_store()
            .get_node_property(ids[9_999], &PropertyKey::new("key")),
        Some(Value::from("row-9999"))
    );
}

#[cfg(feature = "vector-index")]
#[test]
fn bulk_loader_rejects_a_store_after_vector_index_creation() {
    let db = GrafeoDB::new_in_memory();
    db.create_vector_index("BulkRow", "embedding", Some(2), None, None, None, None)
        .expect("empty vector index is valid with explicit dimensions");

    let result = db.bulk_load_nodes_with_props_unindexed(
        "BulkRow",
        vec![HashMap::from([(
            PropertyKey::new("key"),
            Value::from("row"),
        )])],
    );

    assert!(result.is_err());
}

#[test]
fn bulk_edge_loader_writes_adjacency_without_online_mutation_path() {
    let db = GrafeoDB::new_in_memory();
    let nodes = db
        .bulk_load_nodes_with_props_unindexed(
            "BulkRow",
            (0..10_000)
                .map(|index| HashMap::from([(PropertyKey::new("key"), Value::from(index as i64))]))
                .collect(),
        )
        .expect("fresh store accepts nodes");
    let edges: Vec<_> = nodes
        .windows(2)
        .map(|pair| (pair[0], pair[1], "NEXT"))
        .collect();

    let ids = db.bulk_load_edges_unindexed(&edges);

    assert_eq!(ids.len(), edges.len());
    assert_eq!(db.graph_store().edge_count(), edges.len());
}

// ---------------------------------------------------------------------------
// bulk_load_edges_with_props_unindexed
// ---------------------------------------------------------------------------

#[test]
fn bulk_edge_props_roundtrip_adjacency_and_counts() {
    let db = GrafeoDB::new_in_memory();
    let node_ids = db
        .bulk_load_nodes_with_props_unindexed(
            "N",
            (0..1_000)
                .map(|i| HashMap::from([(PropertyKey::new("idx"), Value::from(i as i64))]))
                .collect(),
        )
        .expect("nodes");

    // Chain: 0→1, 1→2, …, 999→0 (cycle so every node has out-degree 1).
    let edge_specs: Vec<_> = (0..1_000)
        .map(|i| {
            let src = node_ids[i];
            let dst = node_ids[(i + 1) % 1_000];
            let props = HashMap::from([
                (
                    PropertyKey::new("relationship_id"),
                    Value::from(format!("rel-{i}")),
                ),
                (PropertyKey::new("evidence_count"), Value::from(i as i64)),
                (PropertyKey::new("provenance"), Value::from("bulk")),
            ]);
            (src, dst, "NEXT", props)
        })
        .collect();

    let edge_ids = db
        .bulk_load_edges_with_props_unindexed(&edge_specs)
        .expect("fresh store accepts property-carrying edges");

    assert_eq!(edge_ids.len(), 1_000);
    assert_eq!(db.graph_store().edge_count(), 1_000);

    // Forward adjacency: node 0 has out-degree 1 (edge to node 1).
    assert_eq!(db.graph_store().out_degree(node_ids[0]), 1);
    assert_eq!(db.graph_store().out_degree(node_ids[999]), 1);

    // Backward adjacency: node 0 has in-degree 1 (edge from node 999).
    assert_eq!(db.graph_store().in_degree(node_ids[0]), 1);
    assert_eq!(db.graph_store().in_degree(node_ids[500]), 1);

    // Properties round-trip per key (not PropertyMap equality).
    for (i, &eid) in edge_ids.iter().enumerate() {
        let edge = db.get_edge(eid).expect("edge exists");
        assert_eq!(
            edge.properties.get(&PropertyKey::new("relationship_id")),
            Some(&Value::from(format!("rel-{i}"))),
        );
        assert_eq!(
            edge.properties.get(&PropertyKey::new("evidence_count")),
            Some(&Value::from(i as i64)),
        );
        assert_eq!(
            edge.properties.get(&PropertyKey::new("provenance")),
            Some(&Value::from("bulk")),
        );
    }
}

#[test]
fn bulk_edge_props_multiple_types_pre_resolved() {
    let db = GrafeoDB::new_in_memory();
    let node_ids = db
        .bulk_load_nodes_with_props_unindexed(
            "N",
            (0..100)
                .map(|i| HashMap::from([(PropertyKey::new("i"), Value::from(i as i64))]))
                .collect(),
        )
        .expect("nodes");

    // Mix multiple edge types to exercise the registry pre-resolution path.
    let edge_specs: Vec<_> = (0..100)
        .map(|i| {
            let src = node_ids[i];
            let dst = node_ids[(i + 1) % 100];
            let edge_type = match i % 3 {
                0 => "KNOWS",
                1 => "LIKES",
                _ => "FOLLOWS",
            };
            let props = HashMap::from([(PropertyKey::new("w"), Value::from(i as i64))]);
            (src, dst, edge_type, props)
        })
        .collect();

    let edge_ids = db
        .bulk_load_edges_with_props_unindexed(&edge_specs)
        .expect("multi-type load succeeds");

    assert_eq!(edge_ids.len(), 100);
    assert_eq!(db.graph_store().edge_count(), 100);

    // Verify per-type properties.
    for (i, &eid) in edge_ids.iter().enumerate() {
        let edge = db.get_edge(eid).expect("edge exists");
        let expected_type = match i % 3 {
            0 => "KNOWS",
            1 => "LIKES",
            _ => "FOLLOWS",
        };
        assert_eq!(edge.edge_type.as_str(), expected_type);
        assert_eq!(
            edge.properties.get(&PropertyKey::new("w")),
            Some(&Value::from(i as i64)),
        );
    }
}

#[test]
fn bulk_edge_props_rejects_property_index() {
    let db = GrafeoDB::new_in_memory();
    let nodes = db
        .bulk_load_nodes_with_props_unindexed(
            "N",
            (0..2)
                .map(|i| HashMap::from([(PropertyKey::new("i"), Value::from(i as i64))]))
                .collect(),
        )
        .expect("nodes");
    db.create_property_index("w");

    let result = db.bulk_load_edges_with_props_unindexed(&[(
        nodes[0],
        nodes[1],
        "T",
        HashMap::from([(PropertyKey::new("w"), Value::from(1i64))]),
    )]);

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("bulk edge load requires no property indexes")
    );
}

#[cfg(feature = "vector-index")]
#[test]
fn bulk_edge_props_rejects_vector_index() {
    let db = GrafeoDB::new_in_memory();
    let nodes = db
        .bulk_load_nodes_with_props_unindexed(
            "N",
            (0..2)
                .map(|i| HashMap::from([(PropertyKey::new("i"), Value::from(i as i64))]))
                .collect(),
        )
        .expect("nodes");
    db.create_vector_index("N", "embedding", Some(2), None, None, None, None)
        .expect("vector index creation");

    let result = db.bulk_load_edges_with_props_unindexed(&[(
        nodes[0],
        nodes[1],
        "T",
        HashMap::from([(PropertyKey::new("w"), Value::from(1i64))]),
    )]);

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("bulk edge load requires no vector indexes")
    );
}

#[cfg(feature = "text-index")]
#[test]
fn bulk_edge_props_rejects_text_index() {
    let db = GrafeoDB::new_in_memory();
    let nodes = db
        .bulk_load_nodes_with_props_unindexed(
            "N",
            (0..2)
                .map(|i| HashMap::from([(PropertyKey::new("i"), Value::from(i as i64))]))
                .collect(),
        )
        .expect("nodes");
    db.create_text_index("N", "body")
        .expect("text index creation");

    let result = db.bulk_load_edges_with_props_unindexed(&[(
        nodes[0],
        nodes[1],
        "T",
        HashMap::from([(PropertyKey::new("w"), Value::from(1i64))]),
    )]);

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("bulk edge load requires no text indexes")
    );
}

#[test]
fn bulk_edge_props_empty_input_returns_empty() {
    let db = GrafeoDB::new_in_memory();
    let ids = db
        .bulk_load_edges_with_props_unindexed(&[])
        .expect("empty is ok");
    assert!(ids.is_empty());
}

#[test]
fn bulk_edge_props_empty_properties_map_is_allowed() {
    let db = GrafeoDB::new_in_memory();
    let nodes = db
        .bulk_load_nodes_with_props_unindexed(
            "N",
            (0..2)
                .map(|i| HashMap::from([(PropertyKey::new("i"), Value::from(i as i64))]))
                .collect(),
        )
        .expect("nodes");

    let ids = db
        .bulk_load_edges_with_props_unindexed(&[(nodes[0], nodes[1], "T", HashMap::new())])
        .expect("empty props ok");

    assert_eq!(ids.len(), 1);
    assert_eq!(db.graph_store().edge_count(), 1);

    let edge = db.get_edge(ids[0]).expect("edge exists");
    assert!(edge.properties.is_empty());
}

#[cfg(feature = "wal")]
#[test]
fn bulk_edge_props_writes_zero_wal_records() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let db_path = dir.path().join("wal_test");
    let db = GrafeoDB::open(&db_path).expect("open db");

    let nodes = db
        .bulk_load_nodes_with_props_unindexed(
            "N",
            (0..10)
                .map(|i| HashMap::from([(PropertyKey::new("i"), Value::from(i as i64))]))
                .collect(),
        )
        .expect("nodes");

    let wal_before = db.wal_status().record_count;

    let edge_specs: Vec<_> = (0..10)
        .map(|i| {
            let src = nodes[i];
            let dst = nodes[(i + 1) % 10];
            let props = HashMap::from([(PropertyKey::new("w"), Value::from(i as i64))]);
            (src, dst, "NEXT", props)
        })
        .collect();

    db.bulk_load_edges_with_props_unindexed(&edge_specs)
        .expect("bulk edges");

    let wal_after = db.wal_status().record_count;
    assert_eq!(
        wal_after - wal_before,
        0,
        "bulk edge loader must not generate WAL records"
    );
}
