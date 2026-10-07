//! Typed write outcomes that a caller must not treat as an ordinary failure.
//!
//! The engine reports these outcomes through existing error variants
//! (`Internal`, `Transaction(InvalidState)`, `AdmissionRetryable`) with fixed
//! wording. Callers used to classify them by matching that wording, which
//! drifts when a message changes. [`Error::write_outcome`] classifies them
//! here instead. Every engine message for one of these outcomes interpolates
//! the marker constants below, so the wording and the classification change
//! together; `write_outcome_kinds` pins each public path.
//!
//! The messages are byte-identical to before, except `close()` over a
//! poisoned WAL: it said "a write reported as durability unconfirmed …",
//! which put the durability marker in a close error. It now says "a write
//! whose durability was reported unconfirmed …" and classifies as
//! [`WriteOutcome::WalPoisoned`]. A caller that matched the old substring
//! (AMH's `classify_engine_error`) no longer classifies that close error;
//! classify it with [`Error::write_outcome`] instead.

use super::error::{Error, TransactionError};

/// Present in every "applied in memory, not confirmed in the WAL" message: an
/// explicit commit whose WAL group failed, or a write outside a transaction
/// whose group failed or whose records were refused.
pub const DURABILITY_UNCONFIRMED: &str = "durability unconfirmed";

/// A refused write or append: "… refuses writes until the database is
/// reopened". Also in the durability-unconfirmed messages, which win.
pub const UNTIL_REOPENED: &str = "until the database is reopened";

/// A refused snapshot or checkpoint of a poisoned database.
pub const WAL_IS_POISONED: &str = "the WAL is poisoned";

/// A close over a poisoned WAL (closed without a checkpoint).
pub const WAL_WAS_POISONED: &str = "the WAL was poisoned";

/// Every "the WAL is poisoned, reopen the database" marker.
pub const WAL_POISONED_MARKERS: [&str; 3] = [UNTIL_REOPENED, WAL_IS_POISONED, WAL_WAS_POISONED];

/// A write or root operation refused because the database was closed: its
/// WAL is sealed and, for a generation root, the root lock was released.
pub const DATABASE_CLOSED: &str = "the database is closed";

/// Prefix of a rollback that could not restore every change.
pub const ROLLBACK_INCOMPLETE: &str = "rollback incomplete:";

/// A write outcome that is not an ordinary failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteOutcome {
    /// Applied in memory but not confirmed in the WAL: it may or may not be
    /// durable. The WAL is poisoned. Never retry blindly.
    DurabilityUnconfirmed,
    /// The WAL refuses writes (or a snapshot of memory) until the database
    /// is reopened.
    WalPoisoned,
    /// A rollback could not undo every change: the live graph may not match
    /// its state before the transaction.
    RollbackIncomplete,
    /// Refused before anything was applied (rolled back cleanly); retryable.
    AdmissionRetryable,
    /// Refused before anything was applied because the database was closed.
    /// Write through a database opened again.
    DatabaseClosed,
}

impl WriteOutcome {
    /// Whether the database must be dropped and reopened after this outcome.
    #[must_use]
    pub const fn requires_reopen(self) -> bool {
        !matches!(self, Self::AdmissionRetryable)
    }
}

impl Error {
    /// This error's [`WriteOutcome`], or `None` for an ordinary failure.
    /// "Durability unconfirmed" wins over "WAL poisoned", since those
    /// messages also say that the WAL refuses further writes.
    #[must_use]
    pub fn write_outcome(&self) -> Option<WriteOutcome> {
        match self {
            Error::AdmissionRetryable(_) => Some(WriteOutcome::AdmissionRetryable),
            Error::Internal(message) if message.contains(DATABASE_CLOSED) => {
                Some(WriteOutcome::DatabaseClosed)
            }
            Error::Internal(message) if message.contains(DURABILITY_UNCONFIRMED) => {
                Some(WriteOutcome::DurabilityUnconfirmed)
            }
            Error::Internal(message)
                if WAL_POISONED_MARKERS
                    .iter()
                    .any(|marker| message.contains(marker)) =>
            {
                Some(WriteOutcome::WalPoisoned)
            }
            Error::Transaction(TransactionError::InvalidState(message))
                if message.starts_with(ROLLBACK_INCOMPLETE) =>
            {
                Some(WriteOutcome::RollbackIncomplete)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn internal(message: &str) -> Error {
        Error::Internal(message.to_string())
    }

    #[test]
    fn classifies_each_outcome() {
        assert_eq!(
            internal(&format!(
                "write applied in memory; {DURABILITY_UNCONFIRMED}: x; the WAL refuses \
                 further writes until the database is reopened"
            ))
            .write_outcome(),
            Some(WriteOutcome::DurabilityUnconfirmed)
        );
        for marker in WAL_POISONED_MARKERS {
            assert_eq!(
                internal(&format!("refused: {marker} (disk full)")).write_outcome(),
                Some(WriteOutcome::WalPoisoned),
                "{marker}"
            );
        }
        assert_eq!(
            Error::Transaction(TransactionError::InvalidState(format!(
                "{ROLLBACK_INCOMPLETE} 2 change(s)"
            )))
            .write_outcome(),
            Some(WriteOutcome::RollbackIncomplete)
        );
        assert_eq!(
            Error::AdmissionRetryable("cap".into()).write_outcome(),
            Some(WriteOutcome::AdmissionRetryable)
        );
        assert_eq!(
            internal(&format!("WAL refuses appends: {DATABASE_CLOSED}")).write_outcome(),
            Some(WriteOutcome::DatabaseClosed)
        );
        assert!(WriteOutcome::DatabaseClosed.requires_reopen());
    }

    #[test]
    fn ordinary_failures_are_not_classified() {
        assert_eq!(internal("boom").write_outcome(), None);
        assert_eq!(
            Error::Transaction(TransactionError::InvalidState(
                "No active transaction".into()
            ))
            .write_outcome(),
            None
        );
        assert!(!WriteOutcome::AdmissionRetryable.requires_reopen());
        assert!(WriteOutcome::DurabilityUnconfirmed.requires_reopen());
    }
}
