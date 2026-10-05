//! Database-level counts on a layered (compacted or generation-root) database.
//!
//! After `compact()`, and on every reopen of a compacted single file or a
//! generation root, the database runs on a `LayeredStore`: an immutable
//! `CompactStore` base plus an `LpgStore` overlay that holds later writes.
//! Queries read the layered view, but `GrafeoDB::node_count()`,
//! `edge_count()`, `info()` and `detailed_stats()` read the overlay alone, so
//! a reopened database with no writes reported 0 nodes. The node and edge
//! counts in the checkpoint header were taken from the overlay too.
//!
//! Every check here compares the database-level counts with
//! `MATCH (n) RETURN count(n)` and with the layered store, in the session
//! that made the write and again after a reopen.
//!
//! ```bash
//! cargo test -p grafeo-engine --features compact-store,cypher --test layered_db_counts
//! cargo test -p grafeo-engine --features compact-store,cypher,generation-streaming --test layered_db_counts
//! ```

#![cfg(all(
    feature = "compact-store",
    feature = "lpg",
    feature = "gql",
    feature = "grafeo-file",
    feature = "wal"
))]

use std::path::Path;

use grafeo_common::types::Value;
use grafeo_core::graph::traits::GraphStore;
use grafeo_engine::{Config, GrafeoDB};
use tempfile::tempdir;

/// The writes each scenario makes on top of the base graph
/// `(a:P)-[:K]->(b:P)-[:K]->(c:Q)`, and the counts that must follow.
struct Scenario {
    name: &'static str,
    writes: &'static [&'static str],
    nodes: usize,
    edges: usize,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "no writes",
        writes: &[],
        nodes: 3,
        edges: 2,
    },
    Scenario {
        name: "overlay creates",
        writes: &[
            "INSERT (:R {name: 'd'})",
            "MATCH (a:P {name: 'a'}), (d:R {name: 'd'}) INSERT (a)-[:L]->(d)",
        ],
        nodes: 4,
        edges: 3,
    },
    Scenario {
        name: "delete a base node",
        writes: &["MATCH (n:Q {name: 'c'}) DETACH DELETE n"],
        nodes: 2,
        edges: 1,
    },
    Scenario {
        name: "copy-up",
        writes: &["MATCH (n:Q {name: 'c'}) SET n.n = 100"],
        nodes: 3,
        edges: 2,
    },
    Scenario {
        name: "copy-up then delete",
        writes: &[
            "MATCH (n:Q {name: 'c'}) SET n.n = 100",
            "MATCH (n:Q {name: 'c'}) DETACH DELETE n",
        ],
        nodes: 2,
        edges: 1,
    },
];

fn count(db: &GrafeoDB, query: &str) -> usize {
    let result = db
        .execute(query)
        .unwrap_or_else(|e| panic!("`{query}` failed: {e}"));
    match &result.rows()[0][0] {
        Value::Int64(v) => usize::try_from(*v).expect("non-negative count"),
        other => panic!("expected integer count from `{query}`, got {other:?}"),
    }
}

fn run_writes(db: &GrafeoDB, writes: &[&str]) {
    for query in writes {
        db.execute(query)
            .unwrap_or_else(|e| panic!("`{query}` failed: {e}"));
    }
}

/// Every database-level count agrees with the query view, the layered store
/// and the expected totals.
fn assert_counts(db: &GrafeoDB, nodes: usize, edges: usize, ctx: &str) {
    assert_eq!(
        count(db, "MATCH (n) RETURN count(n)"),
        nodes,
        "{ctx}: MATCH (n)"
    );
    assert_eq!(
        count(db, "MATCH ()-[r]->() RETURN count(r)"),
        edges,
        "{ctx}: MATCH ()-[r]->()"
    );

    let layered = db.layered_store().expect("layered store is installed");
    assert_eq!(layered.node_count(), nodes, "{ctx}: layered node_count");
    assert_eq!(layered.edge_count(), edges, "{ctx}: layered edge_count");

    assert_eq!(db.node_count(), nodes, "{ctx}: GrafeoDB::node_count");
    assert_eq!(db.edge_count(), edges, "{ctx}: GrafeoDB::edge_count");

    let info = db.info();
    assert_eq!(info.node_count, nodes, "{ctx}: info().node_count");
    assert_eq!(info.edge_count, edges, "{ctx}: info().edge_count");

    let stats = db.detailed_stats();
    assert_eq!(
        stats.node_count, nodes,
        "{ctx}: detailed_stats().node_count"
    );
    assert_eq!(
        stats.edge_count, edges,
        "{ctx}: detailed_stats().edge_count"
    );
    // Base labels P and Q and base edge type K are always registered, even
    // when the overlay holds nothing.
    assert!(
        stats.label_count >= 2,
        "{ctx}: detailed_stats().label_count = {}",
        stats.label_count
    );
    assert!(
        stats.edge_type_count >= 1,
        "{ctx}: detailed_stats().edge_type_count = {}",
        stats.edge_type_count
    );
    assert_eq!(
        db.label_count(),
        stats.label_count,
        "{ctx}: label_count agrees with detailed_stats"
    );
    assert_eq!(
        db.edge_type_count(),
        stats.edge_type_count,
        "{ctx}: edge_type_count agrees with detailed_stats"
    );
}

// ── Compacted single file ─────────────────────────────────────────────

fn open_file(path: &Path) -> GrafeoDB {
    GrafeoDB::with_config(Config::persistent(path)).expect("open single-file db")
}

fn build_base(db: &GrafeoDB) {
    let a = db
        .create_node_with_props(&["P"], [("name", Value::from("a"))])
        .expect("a");
    let b = db
        .create_node_with_props(&["P"], [("name", Value::from("b"))])
        .expect("b");
    let c = db
        .create_node_with_props(
            &["Q"],
            [("name", Value::from("c")), ("n", Value::from(3i64))],
        )
        .expect("c");
    db.create_edge(a, b, "K");
    db.create_edge(b, c, "K");
}

/// The node and edge counts written to the active checkpoint header.
fn header_counts(path: &Path) -> (u64, u64) {
    let fm = grafeo_storage::file::GrafeoFileManager::open_read_only(path)
        .expect("open container read-only");
    let header = fm.active_header();
    (header.node_count, header.edge_count)
}

fn single_file(scenario: &Scenario) {
    let ctx = scenario.name;
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("counts.grafeo");
    {
        let mut db = open_file(&path);
        build_base(&db);
        db.compact().expect("compact");
        assert_counts(&db, 3, 2, &format!("{ctx}: after compact"));
        run_writes(&db, scenario.writes);
        assert_counts(
            &db,
            scenario.nodes,
            scenario.edges,
            &format!("{ctx}: after writes, before reopen"),
        );
        db.close().expect("close");
    }
    assert_eq!(
        header_counts(&path),
        (scenario.nodes as u64, scenario.edges as u64),
        "{ctx}: checkpoint header counts"
    );
    for reopen in 1..=2 {
        let db = open_file(&path);
        assert_counts(
            &db,
            scenario.nodes,
            scenario.edges,
            &format!("{ctx}: reopen #{reopen}"),
        );
        db.close().expect("close");
    }
}

#[test]
fn single_file_counts_without_writes() {
    single_file(&SCENARIOS[0]);
}

#[test]
fn single_file_counts_after_overlay_creates() {
    single_file(&SCENARIOS[1]);
}

#[test]
fn single_file_counts_after_base_delete() {
    single_file(&SCENARIOS[2]);
}

#[test]
fn single_file_counts_after_copy_up() {
    single_file(&SCENARIOS[3]);
}

#[test]
fn single_file_counts_after_copy_up_then_delete() {
    single_file(&SCENARIOS[4]);
}

/// Writes made in a reopened session (on the mapped base) are counted too.
///
/// Leaves out "overlay creates": on a reopened compacted file whose overlay
/// is empty, the overlay's id allocator is not seeded past the base, so a
/// new node takes a base node's id and hides it. That is a separate bug from
/// the counts and is not fixed here.
#[test]
fn single_file_counts_after_writes_in_a_reopened_session() {
    for scenario in SCENARIOS
        .iter()
        .filter(|scenario| scenario.name != "overlay creates")
    {
        let ctx = scenario.name;
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("counts-reopened.grafeo");
        {
            let mut db = open_file(&path);
            build_base(&db);
            db.compact().expect("compact");
            db.close().expect("close");
        }
        {
            let db = open_file(&path);
            assert_counts(&db, 3, 2, &format!("{ctx}: first reopen"));
            run_writes(&db, scenario.writes);
            assert_counts(
                &db,
                scenario.nodes,
                scenario.edges,
                &format!("{ctx}: writes after reopen"),
            );
            db.close().expect("close");
        }
        assert_eq!(
            header_counts(&path),
            (scenario.nodes as u64, scenario.edges as u64),
            "{ctx}: checkpoint header counts after reopened writes"
        );
        let db = open_file(&path);
        assert_counts(
            &db,
            scenario.nodes,
            scenario.edges,
            &format!("{ctx}: second reopen"),
        );
        db.close().expect("close");
    }
}

// ── Generation root ───────────────────────────────────────────────────

#[cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "mmap"
))]
mod generation_root {
    use super::*;
    use grafeo_engine::generation_build_request;

    fn publish_base(root: &Path) {
        std::fs::create_dir_all(root).expect("create generation root");
        let source = GrafeoDB::new_in_memory();
        build_base(&source);
        source
            .build_and_publish_generation(generation_build_request(root, "counts-g1"))
            .expect("publish base generation");
    }

    fn open_root(root: &Path) -> GrafeoDB {
        GrafeoDB::open_generation_root(root, false).expect("open generation root writable")
    }

    fn generation_root(scenario: &Scenario) {
        let ctx = scenario.name;
        let dir = tempdir().expect("temp dir");
        let root = dir.path().join("counts.grafeo.d");
        publish_base(&root);
        {
            let db = open_root(&root);
            assert_counts(&db, 3, 2, &format!("{ctx}: first open"));
            run_writes(&db, scenario.writes);
            assert_counts(
                &db,
                scenario.nodes,
                scenario.edges,
                &format!("{ctx}: after writes, before reopen"),
            );
            db.close().expect("close");
        }
        for reopen in 1..=2 {
            let db = open_root(&root);
            assert_counts(
                &db,
                scenario.nodes,
                scenario.edges,
                &format!("{ctx}: reopen #{reopen}"),
            );
            db.close().expect("close");
        }
    }

    #[test]
    fn generation_root_counts_without_writes() {
        generation_root(&SCENARIOS[0]);
    }

    #[test]
    fn generation_root_counts_after_overlay_creates() {
        generation_root(&SCENARIOS[1]);
    }

    #[test]
    fn generation_root_counts_after_base_delete() {
        generation_root(&SCENARIOS[2]);
    }

    #[test]
    fn generation_root_counts_after_copy_up() {
        generation_root(&SCENARIOS[3]);
    }

    #[test]
    fn generation_root_counts_after_copy_up_then_delete() {
        generation_root(&SCENARIOS[4]);
    }
}
