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
    /// Distinct label names in the schema (base and overlay). A label stays
    /// in the base schema after its last node is deleted.
    labels: usize,
    /// Distinct edge type names in the schema.
    edge_types: usize,
    /// Distinct property key names of nodes and edges together. `None` when
    /// the only element carrying a key was deleted: whether the name stays
    /// in the overlay's schema then differs before and after a reopen.
    property_keys: Option<usize>,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "no writes",
        writes: &[],
        nodes: 3,
        edges: 2,
        labels: 2,
        edge_types: 1,
        property_keys: Some(2),
    },
    Scenario {
        name: "overlay creates",
        writes: &[
            "INSERT (:R {name: 'd'})",
            "MATCH (a:P {name: 'a'}), (d:R {name: 'd'}) INSERT (a)-[:L]->(d)",
        ],
        nodes: 4,
        edges: 3,
        labels: 3,
        edge_types: 2,
        property_keys: Some(2),
    },
    Scenario {
        name: "delete a base node",
        writes: &["MATCH (n:Q {name: 'c'}) DETACH DELETE n"],
        nodes: 2,
        edges: 1,
        labels: 2,
        edge_types: 1,
        property_keys: Some(2),
    },
    Scenario {
        name: "copy-up",
        writes: &["MATCH (n:Q {name: 'c'}) SET n.n = 100"],
        nodes: 3,
        edges: 2,
        labels: 2,
        edge_types: 1,
        property_keys: Some(2),
    },
    Scenario {
        name: "copy-up then delete",
        writes: &[
            "MATCH (n:Q {name: 'c'}) SET n.n = 100",
            "MATCH (n:Q {name: 'c'}) DETACH DELETE n",
        ],
        nodes: 2,
        edges: 1,
        labels: 2,
        edge_types: 1,
        property_keys: Some(2),
    },
    // A SET on a base edge copies the edge and both endpoints up; the DETACH
    // DELETE of its source then tombstones the base edge while its id is
    // still marked dirty. The base edge must be excluded once, not twice.
    Scenario {
        name: "edge copy-up then detach delete of its source",
        writes: &[
            "MATCH (:P {name: 'a'})-[r:K]->(:P {name: 'b'}) SET r.w = 1",
            "MATCH (n:P {name: 'a'}) DETACH DELETE n",
        ],
        nodes: 2,
        edges: 1,
        labels: 2,
        edge_types: 1,
        property_keys: None,
    },
    // A SET on a base edge copies it up; deleting it then removes the
    // overlay copy and must hide the base row.
    Scenario {
        name: "edge copy-up then delete",
        writes: &[
            "MATCH (:P {name: 'a'})-[r:K]->(:P {name: 'b'}) SET r.w = 1",
            "MATCH ()-[r:K]->() WHERE r.w = 1 DELETE r",
        ],
        nodes: 3,
        edges: 1,
        labels: 2,
        edge_types: 1,
        property_keys: None,
    },
    Scenario {
        name: "delete a base edge",
        writes: &["MATCH (:P {name: 'a'})-[r:K]->(:P {name: 'b'}) DELETE r"],
        nodes: 3,
        edges: 1,
        labels: 2,
        edge_types: 1,
        property_keys: Some(2),
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
}

/// Both the element counts and the schema counts a scenario expects.
fn assert_scenario(db: &GrafeoDB, scenario: &Scenario, ctx: &str) {
    assert_counts(db, scenario.nodes, scenario.edges, ctx);
    assert_schema_counts(db, scenario, ctx);
}

/// The schema-level counts: distinct label, edge type and property key names
/// across the base and the overlay.
fn assert_schema_counts(db: &GrafeoDB, scenario: &Scenario, ctx: &str) {
    let stats = db.detailed_stats();
    for (what, from_db, from_stats, expected) in [
        (
            "label_count",
            db.label_count(),
            stats.label_count,
            scenario.labels,
        ),
        (
            "edge_type_count",
            db.edge_type_count(),
            stats.edge_type_count,
            scenario.edge_types,
        ),
    ] {
        assert_eq!(from_db, expected, "{ctx}: {what}");
        assert_eq!(from_stats, expected, "{ctx}: detailed_stats().{what}");
    }
    assert_eq!(
        db.property_key_count(),
        stats.property_key_count,
        "{ctx}: property_key_count agrees with detailed_stats"
    );
    if let Some(expected) = scenario.property_keys {
        assert_eq!(
            db.property_key_count(),
            expected,
            "{ctx}: property_key_count"
        );
    }
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
        assert_scenario(
            &db,
            scenario,
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
        assert_scenario(&db, scenario, &format!("{ctx}: reopen #{reopen}"));
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

#[test]
fn single_file_counts_after_edge_copy_up_then_source_delete() {
    single_file(&SCENARIOS[5]);
}

#[test]
fn single_file_counts_after_edge_copy_up_then_delete() {
    single_file(&SCENARIOS[6]);
}

#[test]
fn single_file_counts_after_base_edge_delete() {
    single_file(&SCENARIOS[7]);
}

/// Writes made in a reopened session (on the mapped base) are counted too.
#[test]
fn single_file_counts_after_writes_in_a_reopened_session() {
    for scenario in SCENARIOS {
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
            assert_scenario(&db, scenario, &format!("{ctx}: writes after reopen"));
            db.close().expect("close");
        }
        assert_eq!(
            header_counts(&path),
            (scenario.nodes as u64, scenario.edges as u64),
            "{ctx}: checkpoint header counts after reopened writes"
        );
        let db = open_file(&path);
        assert_scenario(&db, scenario, &format!("{ctx}: second reopen"));
        db.close().expect("close");
    }
}

/// `GraphStoreMut::delete_node_edges` tombstones every base edge of the node,
/// including one that a property write already copied up. The layered edge
/// count then subtracted that edge twice: once as deleted from the base and
/// once as promoted (with a single-edge base, `1 - 1 - 1 + 0` underflows).
#[test]
fn single_file_counts_after_delete_node_edges_of_a_copied_up_edge() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("delete-node-edges.grafeo");
    {
        let mut db = open_file(&path);
        build_base(&db);
        db.compact().expect("compact");
        let row = db
            .execute("MATCH (a:P {name: 'a'})-[r:K]->() RETURN id(a), id(r)")
            .expect("ids")
            .rows()[0]
            .clone();
        let (Value::Int64(a), Value::Int64(r)) = (&row[0], &row[1]) else {
            panic!("expected integer ids, got {row:?}");
        };
        let a = grafeo_common::types::NodeId::new(u64::try_from(*a).expect("id"));
        let r = grafeo_common::types::EdgeId::new(u64::try_from(*r).expect("id"));
        let store = db.graph_store_mut().expect("layered write store");
        store.set_edge_property(r, "w", Value::from(1i64));
        store.delete_node_edges(a);
        assert_counts(&db, 3, 1, "after delete_node_edges");
        db.close().expect("close");
    }
    assert_eq!(header_counts(&path), (3, 1), "checkpoint header counts");
    for reopen in 1..=2 {
        let db = open_file(&path);
        assert_counts(&db, 3, 1, &format!("reopen #{reopen}"));
        db.close().expect("close");
    }
}

/// On reopen of a compacted single file the overlay's id allocator must sit
/// above the base's ids. It was restored from the overlay's own rows only, so
/// a new node took a base node's id and, on the next reopen, hid that base
/// node as if it were a copy-up.
#[test]
fn single_file_new_nodes_after_reopen_do_not_reuse_base_ids() {
    for (ctx, before_reopen) in [
        ("empty overlay", &[][..]),
        (
            "overlay holds a copy-up of the lowest base node",
            &["MATCH (n:P {name: 'a'}) SET n.touched = true"][..],
        ),
    ] {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("ids.grafeo");
        {
            let mut db = open_file(&path);
            build_base(&db);
            db.compact().expect("compact");
            run_writes(&db, before_reopen);
            db.close().expect("close");
        }
        {
            let db = open_file(&path);
            let node = db.create_node(&["R"]).expect("create node");
            let edge = db.create_edge(node, node, "SELF");
            let base_ids = db
                .execute("MATCH (n) WHERE NOT n:R RETURN id(n)")
                .expect("base ids");
            for row in base_ids.rows() {
                assert_ne!(
                    row[0],
                    Value::Int64(i64::try_from(node.as_u64()).expect("id")),
                    "{ctx}: new node took a base node's id"
                );
            }
            let base_edge_ids = db
                .execute("MATCH ()-[r:K]->() RETURN id(r)")
                .expect("base edge ids");
            for row in base_edge_ids.rows() {
                assert_ne!(
                    row[0],
                    Value::Int64(i64::try_from(edge.as_u64()).expect("id")),
                    "{ctx}: new edge took a base edge's id"
                );
            }
            assert_counts(&db, 4, 3, &format!("{ctx}: after create"));
            db.close().expect("close");
        }
        for reopen in 1..=2 {
            let db = open_file(&path);
            assert_counts(&db, 4, 3, &format!("{ctx}: reopen #{reopen}"));
            assert_eq!(
                count(&db, "MATCH (n:P) RETURN count(n)"),
                2,
                "{ctx}: reopen #{reopen}: base P nodes"
            );
            db.close().expect("close");
        }
    }
}

// ── Periodic checkpoint timer ─────────────────────────────────────────

/// Copies every file of `from` (the container and its sidecar WAL) into
/// `to`, as a crash at this instant would leave them.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create copy dir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("dir entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}

const TIMER_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);

/// Lets several timer checkpoints run.
fn let_timer_run() {
    std::thread::sleep(TIMER_INTERVAL * 15);
}

/// After a timer checkpoint the files on disk still hold the base.
fn assert_crash_copy_keeps_base(dir: &Path, file_name: &str, nodes: usize, ctx: &str) {
    let copy = tempdir().expect("copy dir");
    copy_dir(dir, copy.path());
    let db = open_file(&copy.path().join(file_name));
    assert_eq!(
        count(&db, "MATCH (n) RETURN count(n)"),
        nodes,
        "{ctx}: MATCH (n) on a crash copy taken after timer checkpoints"
    );
    assert_eq!(
        db.node_count(),
        nodes,
        "{ctx}: node_count on the crash copy"
    );
}

/// The periodic checkpoint timer snapshots the LpgStore alone. On a reopened
/// compacted file that is the overlay, and a timer checkpoint replaced the
/// whole container with an image that has no CompactStore section.
#[test]
fn checkpoint_timer_keeps_the_base_of_a_reopened_compacted_file() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("timer.grafeo");
    {
        let mut db = open_file(&path);
        build_base(&db);
        db.compact().expect("compact");
        db.close().expect("close");
    }
    let db =
        GrafeoDB::with_config(Config::persistent(&path).with_checkpoint_interval(TIMER_INTERVAL))
            .expect("reopen with checkpoint timer");
    assert_counts(&db, 3, 2, "reopened with timer");
    let_timer_run();
    assert_crash_copy_keeps_base(dir.path(), "timer.grafeo", 3, "reopened with timer");
    db.close().expect("close");
}

/// Guard for `compact()` while the timer is already running: the timer holds
/// the pre-compact LpgStore, and a timer checkpoint must never replace the
/// layered checkpoint (base + overlay, including writes made after
/// `compact()`).
#[test]
fn checkpoint_timer_keeps_writes_after_compact_in_session() {
    let dir = tempdir().expect("temp dir");
    let path = dir.path().join("timer-compact.grafeo");
    let mut db =
        GrafeoDB::with_config(Config::persistent(&path).with_checkpoint_interval(TIMER_INTERVAL))
            .expect("open with checkpoint timer");
    build_base(&db);
    db.compact().expect("compact");
    run_writes(&db, &["INSERT (:R {name: 'd'})"]);
    db.wal_checkpoint().expect("layered checkpoint");
    assert_crash_copy_keeps_base(
        dir.path(),
        "timer-compact.grafeo",
        4,
        "checkpoint after compact",
    );
    let_timer_run();
    assert_crash_copy_keeps_base(dir.path(), "timer-compact.grafeo", 4, "compact under timer");
    db.close().expect("close");
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
            assert_scenario(
                &db,
                scenario,
                &format!("{ctx}: after writes, before reopen"),
            );
            db.close().expect("close");
        }
        for reopen in 1..=2 {
            let db = open_root(&root);
            assert_scenario(&db, scenario, &format!("{ctx}: reopen #{reopen}"));
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

    #[test]
    fn generation_root_counts_after_edge_copy_up_then_source_delete() {
        generation_root(&SCENARIOS[5]);
    }

    #[test]
    fn generation_root_counts_after_edge_copy_up_then_delete() {
        generation_root(&SCENARIOS[6]);
    }

    #[test]
    fn generation_root_counts_after_base_edge_delete() {
        generation_root(&SCENARIOS[7]);
    }

    /// An in-process epoch handoff swaps in a base that absorbed the overlay
    /// rows while those rows stay in the overlay, and keeps tombstones whose
    /// rows the new base no longer has. Neither may change the counts.
    #[test]
    fn counts_after_an_epoch_handoff_install() {
        let dir = tempdir().expect("temp dir");
        let root = dir.path().join("handoff.grafeo.d");
        publish_base(&root);
        let db = open_root(&root);
        run_writes(
            &db,
            &[
                "INSERT (:R {name: 'd'})",
                "MATCH (a:P {name: 'a'}), (d:R {name: 'd'}) INSERT (a)-[:L]->(d)",
                "MATCH (n:Q {name: 'c'}) DETACH DELETE n",
            ],
        );
        assert_counts(&db, 3, 2, "before handoff");
        let report = db
            .run_epoch_handoff(generation_build_request(&root, "counts-g2"))
            .expect("run epoch handoff");
        db.publish_and_install_handoff(report)
            .expect("publish and install handoff");
        assert_eq!(
            db.layered_store()
                .expect("layered")
                .base_store_arc()
                .total_nodes(),
            3,
            "the new base absorbed the overlay"
        );
        assert_counts(&db, 3, 2, "after handoff install");
        db.close().expect("close");
        drop(db);

        let db = open_root(&root);
        assert_counts(&db, 3, 2, "reopen after handoff");
        db.close().expect("close");
    }

    /// A base whose nodes carry more than one label stores the extra labels
    /// in the label-membership segment. `label_count` must see them.
    #[test]
    fn generation_root_label_count_includes_extra_labels() {
        let dir = tempdir().expect("temp dir");
        let root = dir.path().join("multi-label.grafeo.d");
        std::fs::create_dir_all(&root).expect("create generation root");
        {
            let source = GrafeoDB::new_in_memory();
            let a = source
                .create_node_with_props(&["P", "Extra"], [("name", Value::from("a"))])
                .expect("a");
            let b = source
                .create_node_with_props(&["Q", "Other"], [("name", Value::from("b"))])
                .expect("b");
            source.create_edge(a, b, "K");
            source
                .build_and_publish_generation(generation_build_request(&root, "multi-g1"))
                .expect("publish base generation");
        }
        for reopen in 1..=2 {
            let db = open_root(&root);
            assert_eq!(
                count(&db, "MATCH (n:Extra) RETURN count(n)"),
                1,
                "reopen #{reopen}: MATCH (n:Extra)"
            );
            let mut labels = db.layered_store().expect("layered").all_labels();
            labels.sort();
            assert_eq!(
                labels,
                ["Extra", "Other", "P", "Q"],
                "reopen #{reopen}: all_labels"
            );
            assert_eq!(db.label_count(), 4, "reopen #{reopen}: label_count");
            db.close().expect("close");
        }
    }

    /// While a mid-build drain has tiers installed, queries read the tier
    /// chain; the database-level counts must read the same view.
    #[test]
    fn counts_match_queries_after_a_mid_build_drain() {
        let dir = tempdir().expect("temp dir");
        let mut db = GrafeoDB::new_in_memory();
        db.compact().expect("compact");
        let n = db
            .create_node_with_props(&["Person"], [("name", Value::from("drained"))])
            .expect("create node");
        let m = db.create_node(&["Item"]).expect("create item");
        db.create_edge(n, m, "OWNS");
        db.drain_overlay_to_tier(&dir.path().join("tiers"), "counts-drain")
            .expect("drain");
        db.create_node(&["Person"]).expect("create node");

        let nodes = count(&db, "MATCH (n) RETURN count(n)");
        let edges = count(&db, "MATCH ()-[r]->() RETURN count(r)");
        assert_eq!(nodes, 3, "MATCH (n) after drain");
        assert_eq!(db.node_count(), nodes, "node_count after drain");
        assert_eq!(db.edge_count(), edges, "edge_count after drain");
        assert_eq!(db.info().node_count, nodes, "info().node_count after drain");
    }
}
