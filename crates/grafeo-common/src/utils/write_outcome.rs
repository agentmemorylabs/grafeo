//! Typed write outcomes that a caller must not treat as an ordinary failure.
//!
//! The engine reports these outcomes through existing error variants
//! (`Internal`, `Transaction(InvalidState)`, `AdmissionRetryable`) with fixed
//! wording. Callers used to classify them by matching that wording, which
//! drifts when a message changes. [`Error::write_outcome`] classifies them
//! here instead, from the same marker constants the engine builds its
//! messages with, so the wording and the classification change together.
//! The messages themselves are unchanged.

use super::error::{Error, TransactionError};

/// Present in every "applied in memory, not confirmed in the WAL" message: an
/// explicit commit whose WAL group failed, or a write outside a transaction
/// whose group failed or whose records were refused.
pub const DURABILITY_UNCONFIRMED: &str = "durability unconfirmed";

/// Present in every "the WAL is poisoned, reopen the database" refusal of a
/// write, an append or a snapshot.
pub const WAL_POISONED_MARKERS: [&str; 3] = [
    "until the database is reopened",
    "the WAL is poisoned",
    "the WAL was poisoned",
];

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
