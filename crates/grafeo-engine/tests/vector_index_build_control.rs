//! Integration tests for the G-OBS.1 index-build control seam: cancellation
//! and progress callbacks on the `*_with_control` index-build entry points.
//!
//! Contracts under test:
//! - A cancelled build returns `Error::Cancelled` and never registers a
//!   (partial) index.
//! - `progress` callbacks are monotonic in `done`, never exceed `total`, and
//!   end with `done == total`.
//! - `control = None` preserves the historical unmonitored behavior exactly.

#![cfg(feature = "vector-index")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use grafeo_common::types::Value;
use grafeo_common::utils::error::{Error, ErrorCode};
use grafeo_engine::GrafeoDB;
use grafeo_engine::database::index_build_control::{IndexBuildControl, IndexBuildProgress};

/// Helper: create a 3D vector value.
fn vec3(x: f32, y: f32, z: f32) -> Value {
    Value::Vector(vec![x, y, z].into())
}

/// Helper: create `count` nodes of label `"Doc"` each carrying a distinct 3-D
/// `"emb"` vector.
fn build_vector_graph(db: &GrafeoDB, count: usize) {
    for i in 0..count {
        let id = db.create_node(&["Doc"]).unwrap();
        let v = vec3(i as f32 * 0.1, 1.0 - i as f32 * 0.1, 0.5);
        db.set_node_property(id, "emb", v).unwrap();
    }
}

/// Helper: create `count` nodes of label `"Doc"` each carrying `"content"` text.
#[cfg(feature = "text-index")]
fn build_text_graph(db: &GrafeoDB, count: usize) {
    for i in 0..count {
        db.create_node_with_props(
            &["Doc"],
            [(
                "content",
                Value::String(format!("document number {i} about grafeo indexes").into()),
            )],
        )
        .unwrap();
    }
}

#[test]
fn pre_cancelled_build_returns_cancelled_and_registers_nothing() {
    let db = GrafeoDB::new_in_memory();
    build_vector_graph(&db, 20);

    let control = IndexBuildControl::new().with_cancel_check(Arc::new(|| true));
    let err = db
        .create_vector_index_with_control(
            "Doc",
            "emb",
            Some(3),
            Some("cosine"),
            None,
            None,
            None,
            Some(&control),
        )
        .unwrap_err();

    assert!(matches!(err, Error::Cancelled { .. }), "got: {err}");
    assert_eq!(err.error_code(), ErrorCode::Cancelled);
    let msg = err.to_string();
    assert!(
        msg.contains("create_vector_index dim scan :Doc(emb)"),
        "display should carry the cancelled operation, got: {msg}"
    );
    assert!(!db.has_vector_index("Doc", "emb"));
}

#[test]
fn mid_build_cancel_stops_before_registration() {
    let db = GrafeoDB::new_in_memory();
    build_vector_graph(&db, 20);

    // First poll (dim-validation scan, iteration 0) returns false; every later
    // poll returns true, so the insert loop cancels at its first cadence point
    // — i.e. mid-build, after the scan has fully validated.
    let polls = Arc::new(AtomicUsize::new(0));
    let polls_clone = Arc::clone(&polls);
    let control = IndexBuildControl::new().with_cancel_check(Arc::new(move || {
        polls_clone.fetch_add(1, Ordering::SeqCst) > 0
    }));

    let err = db
        .create_vector_index_with_control(
            "Doc",
            "emb",
            Some(3),
            Some("cosine"),
            None,
            None,
            None,
            Some(&control),
        )
        .unwrap_err();

    assert!(
        matches!(
            err,
            Error::Cancelled { ref operation }
                if operation.contains("create_vector_index build :Doc(emb)")
        ),
        "expected a build-phase cancellation, got: {err}"
    );
    assert_eq!(err.error_code(), ErrorCode::Cancelled);
    assert!(polls.load(Ordering::SeqCst) >= 2, "cancel_check was polled");
    assert!(!db.has_vector_index("Doc", "emb"));
}

#[test]
fn progress_is_monotonic_and_completes() {
    let db = GrafeoDB::new_in_memory();
    let count = 20;
    build_vector_graph(&db, count);

    let events: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let events_clone = Arc::clone(&events);
    let progress: IndexBuildProgress = Arc::new(move |done, total| {
        events_clone.lock().unwrap().push((done, total));
    });
    let control = IndexBuildControl::new().with_progress(progress);

    db.create_vector_index_with_control(
        "Doc",
        "emb",
        Some(3),
        Some("cosine"),
        None,
        None,
        None,
        Some(&control),
    )
    .expect("build with progress callback should succeed");
    assert!(db.has_vector_index("Doc", "emb"));

    let events = events.lock().unwrap();
    assert!(!events.is_empty(), "progress callback never fired");
    for (i, &(done, total)) in events.iter().enumerate() {
        assert!(done <= total, "event {i}: done {done} > total {total}");
        assert_eq!(total, count as u64, "event {i}: wrong total");
        if i > 0 {
            assert!(
                done >= events[i - 1].0,
                "event {i}: done regressed from {} to {done}",
                events[i - 1].0
            );
        }
    }
    let (done, total) = *events.last().unwrap();
    assert_eq!(done, count as u64, "final done must equal inserted count");
    assert_eq!(total, count as u64, "final total must equal inserted count");
}

#[cfg(feature = "text-index")]
#[test]
fn text_index_cancel_parity() {
    let db = GrafeoDB::new_in_memory();
    build_text_graph(&db, 20);

    let control = IndexBuildControl::new().with_cancel_check(Arc::new(|| true));
    let err = db
        .create_text_index_with_control("Doc", "content", Some(&control))
        .unwrap_err();

    assert!(matches!(err, Error::Cancelled { .. }), "got: {err}");
    assert_eq!(err.error_code(), ErrorCode::Cancelled);
    let msg = err.to_string();
    assert!(
        msg.contains("create_text_index build :Doc(content)"),
        "display should carry the cancelled operation, got: {msg}"
    );
    // Nothing was registered: there is no text index to drop.
    assert!(!db.drop_text_index("Doc", "content"));
}

#[test]
fn none_control_matches_legacy_behavior() {
    let db = GrafeoDB::new_in_memory();
    build_vector_graph(&db, 20);

    // New seam with `None` control...
    db.create_vector_index_with_control(
        "Doc",
        "emb",
        Some(3),
        Some("cosine"),
        None,
        None,
        None,
        None,
    )
    .expect("build with None control succeeds exactly like the legacy path");
    assert!(db.has_vector_index("Doc", "emb"));

    // ...and the legacy entry point both register a fully searchable index.
    let results = db
        .vector_search("Doc", "emb", &[1.0, 0.0, 0.0], 3, None, None)
        .expect("search");
    assert_eq!(results.len(), 3, "should find the 3 closest vectors");
    let ids: Vec<u64> = results.iter().map(|(id, _)| id.as_u64()).collect();
    assert!(!ids.is_empty());

    let db2 = GrafeoDB::new_in_memory();
    build_vector_graph(&db2, 20);
    db2.create_vector_index("Doc", "emb", Some(3), Some("cosine"), None, None, None)
        .expect("legacy create_vector_index still works");
    assert!(db2.has_vector_index("Doc", "emb"));
    let results = db2
        .vector_search("Doc", "emb", &[1.0, 0.0, 0.0], 3, None, None)
        .expect("search on legacy-built index");
    assert_eq!(results.len(), 3);
}
