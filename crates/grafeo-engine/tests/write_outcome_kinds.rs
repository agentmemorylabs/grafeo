//! `Error::write_outcome` on the engine's real errors: each outcome a caller
//! must not treat as an ordinary failure is classified from the error the
//! engine actually returns, so a reworded message that drops its marker fails
//! here. (`RollbackIncomplete` is asserted in `layered_rollback_base_mutations`.)

#![cfg(all(
    feature = "wal",
    feature = "lpg",
    feature = "gql",
    feature = "grafeo-file"
))]

use grafeo_common::types::Value;
use grafeo_common::utils::WriteOutcome;
use grafeo_engine::{Config, GrafeoDB};

const CAP: usize = 4 * 1024;

fn open_capped(dir: &std::path::Path) -> GrafeoDB {
    GrafeoDB::with_config(
        Config::persistent(dir.join("kinds.grafeo")).with_wal_transaction_buffer_cap(CAP),
    )
    .expect("open")
}

fn big() -> Value {
    Value::from("x".repeat(2 * CAP))
}

#[test]
fn over_the_cap_in_a_transaction_is_admission_retryable() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_capped(dir.path());
    let mut session = db.session();
    session.begin_transaction().unwrap();
    let err = session
        .create_node_with_props(&["Big"], [("p", big())])
        .expect_err("over the cap");
    assert_eq!(
        err.write_outcome(),
        Some(WriteOutcome::AdmissionRetryable),
        "{err}"
    );
    session.rollback().unwrap();
    db.session()
        .execute("INSERT (:Small)")
        .expect("not poisoned");
}

#[test]
fn poisoned_database_reports_each_kind() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_capped(dir.path());

    // Outside a transaction the write is applied before its records are
    // refused: durability unconfirmed, and the WAL is poisoned.
    let err = db
        .session()
        .create_node_with_props(&["Big"], [("p", big())])
        .expect_err("over the cap");
    assert_eq!(
        err.write_outcome(),
        Some(WriteOutcome::DurabilityUnconfirmed),
        "{err}"
    );

    // Every later write, snapshot and the close report the poison.
    let err = db
        .session()
        .execute("INSERT (:After)")
        .expect_err("refused");
    assert_eq!(
        err.write_outcome(),
        Some(WriteOutcome::WalPoisoned),
        "{err}"
    );
    let err = db
        .save(dir.path().join("copy.grafeo"))
        .expect_err("save refused");
    assert_eq!(
        err.write_outcome(),
        Some(WriteOutcome::WalPoisoned),
        "{err}"
    );
    let err = db.export_snapshot().expect_err("export refused");
    assert_eq!(
        err.write_outcome(),
        Some(WriteOutcome::WalPoisoned),
        "{err}"
    );
    let err = db.close().expect_err("close without a checkpoint");
    assert_eq!(
        err.write_outcome(),
        Some(WriteOutcome::WalPoisoned),
        "{err}"
    );
    assert!(WriteOutcome::WalPoisoned.requires_reopen());
}
