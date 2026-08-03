//! H-ADOPT.2 — production generation-root open proof.
//!
//! Builds and publishes a real W generation, then opens it through
//! `GrafeoDB::open_generation_root_with_config` and serves a normal session
//! query from the mmap-backed CompactStore base.

#![cfg(all(
    feature = "generation",
    feature = "generation-streaming",
    feature = "lpg",
    feature = "compact-store",
    feature = "mmap"
))]

use grafeo_common::types::Value;
use grafeo_engine::{Config, GrafeoDB, generation_build_request};
use tempfile::tempdir;

#[test]
fn generation_root_open_serves_base_and_preserves_config() {
    let dir = tempdir().expect("temp dir");
    let root = dir.path().join("runtime-open.grafeo.d");
    let spill = dir.path().join("bounded-spill");
    std::fs::create_dir_all(&root).expect("create generation root");

    let source = GrafeoDB::new_in_memory();
    source
        .create_node_with_props(&["Person"], [("name", Value::from("Ada"))])
        .expect("create Ada");
    source
        .create_node_with_props(&["Person"], [("name", Value::from("Grace"))])
        .expect("create Grace");
    source
        .build_and_publish_generation(generation_build_request(&root, "runtime-open-g1"))
        .expect("publish generation");
    drop(source);

    let config = Config::persistent(&root)
        .with_memory_limit(64 * 1024 * 1024)
        .with_spill_path(&spill)
        .with_threads(2);
    let db = GrafeoDB::open_generation_root_with_config(config)
        .expect("open generation root through production constructor");

    assert_eq!(db.config().memory_limit, Some(64 * 1024 * 1024));
    assert_eq!(db.config().spill_path.as_deref(), Some(spill.as_path()));
    assert_eq!(db.config().threads, 2);

    let result = db
        .session()
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("query generation base");
    let mut names: Vec<String> = result
        .rows()
        .iter()
        .map(|row| match &row[0] {
            Value::String(value) => value.as_str().to_string(),
            other => panic!("expected string name, got {other:?}"),
        })
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["Ada", "Grace"]);

    // The live DB must retain the process root lock + selected base lease.
    assert!(GrafeoDB::open_generation_root(&root, false).is_err());
    drop(db);

    // Releasing the database releases ownership; a clean reopen serves the
    // same immutable base (post-boundary WAL replay is H-ADOPT.3).
    let reopened = GrafeoDB::open_generation_root(&root, false).expect("reopen after drop");
    let result = reopened
        .session()
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("query reopened generation base");
    assert_eq!(result.row_count(), 2);
}
