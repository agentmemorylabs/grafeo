//! Enforcing whole-job anonymous-memory ledger (G-EM0.5b R2).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Job-level concurrent anonymous-memory ledger shared across every sink in
/// one generation job (G-EM0.5b R2).
///
/// A single sink's `ExternalSortMetrics::anon_bytes_peak` only captures that
/// sink's own high-water mark. Several sinks are live at once (the node pass
/// holds row + id + membership arenas simultaneously), so the truthful
/// whole-job anonymous footprint is the **sum of concurrently live sinks**,
/// not the max of their individual peaks. This shared counter is charged by
/// each sink on every arena growth and released on flush/cleanup; `peak`
/// records the maximum concurrent total observed across the whole job.
///
/// # Enforcement (R2)
///
/// `reserve()` is **fallible**: it performs a checked, overflow-safe CAS
/// admission against `max_anon_bytes` and returns an [`AnonReservation`]
/// RAII guard. The guard releases the charge on drop. No allocation may
/// precede its reservation — callers must reserve *before* `Vec::reserve`,
/// heap growth, or buffer allocation.
#[derive(Debug)]
pub struct JobAnonLedger {
    current: AtomicU64,
    peak: AtomicU64,
    limit: u64,
}

/// Error returned when a reservation cannot be admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnonLedgerError {
    /// `current + bytes` would exceed the configured `max_anon_bytes`.
    BudgetExceeded {
        /// Total that would have been charged (current + requested).
        requested: u64,
        /// Configured hard limit.
        limit: u64,
    },
    /// `current + bytes` overflows `u64`.
    Overflow,
}

impl std::fmt::Display for AnonLedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BudgetExceeded { requested, limit } => {
                write!(
                    f,
                    "anon budget exceeded: requested {requested}, limit {limit}"
                )
            }
            Self::Overflow => write!(f, "u64 overflow in anon accounting"),
        }
    }
}

impl std::error::Error for AnonLedgerError {}

/// RAII reservation guard. Releases the charged bytes on drop.
///
/// Obtain via [`JobAnonLedger::reserve`]. To release early, drop explicitly.
/// To transfer ownership without releasing, use [`std::mem::forget`].
#[derive(Debug)]
pub struct AnonReservation {
    ledger: std::sync::Arc<JobAnonLedger>,
    bytes: u64,
}

impl Drop for AnonReservation {
    fn drop(&mut self) {
        self.ledger.release_raw(self.bytes);
    }
}

impl AnonReservation {
    /// Bytes held by this reservation.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Grow this reservation by `additional` bytes.
    ///
    /// Atomically admits the extra charge against the job limit via CAS.
    /// The additional bytes are released together with the original charge
    /// on drop. Call **before** the allocation that consumes the bytes.
    ///
    /// # Errors
    /// Returns [`AnonLedgerError::BudgetExceeded`] if the grown total would
    /// cross the limit, or [`AnonLedgerError::Overflow`] on `u64` overflow.
    pub fn grow(&mut self, additional: u64) -> Result<(), AnonLedgerError> {
        if additional == 0 {
            return Ok(());
        }
        loop {
            let cur = self.ledger.current.load(Ordering::Acquire);
            let next = cur
                .checked_add(additional)
                .ok_or(AnonLedgerError::Overflow)?;
            if next > self.ledger.limit {
                return Err(AnonLedgerError::BudgetExceeded {
                    requested: next,
                    limit: self.ledger.limit,
                });
            }
            match self.ledger.current.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.ledger.bump_peak(next);
                    self.bytes += additional;
                    return Ok(());
                }
                Err(_) => continue,
            }
        }
    }
}

/// Point-in-time snapshot of the job ledger for reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobAnonSnapshot {
    /// Currently charged bytes across all live reservations.
    pub current: u64,
    /// Peak concurrent charge observed.
    pub peak: u64,
    /// Configured hard limit.
    pub limit: u64,
}

impl JobAnonLedger {
    /// Create a ledger with the given hard limit (`max_anon_bytes`).
    #[must_use]
    pub fn new(max_anon_bytes: u64) -> Self {
        Self {
            current: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            limit: max_anon_bytes,
        }
    }

    /// Fallible, overflow-safe whole-job admission.
    ///
    /// Atomically checks `current + bytes <= limit` via a CAS loop, then
    /// charges and returns an RAII guard. The guard releases on drop.
    ///
    /// # Errors
    /// Returns [`AnonLedgerError::BudgetExceeded`] if the reservation would
    /// cross the limit, or [`AnonLedgerError::Overflow`] on `u64` overflow.
    pub fn reserve(
        self: &std::sync::Arc<Self>,
        bytes: u64,
    ) -> Result<AnonReservation, AnonLedgerError> {
        loop {
            let cur = self.current.load(Ordering::Acquire);
            let next = cur.checked_add(bytes).ok_or(AnonLedgerError::Overflow)?;
            if next > self.limit {
                return Err(AnonLedgerError::BudgetExceeded {
                    requested: next,
                    limit: self.limit,
                });
            }
            match self
                .current
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    self.bump_peak(next);
                    return Ok(AnonReservation {
                        ledger: std::sync::Arc::clone(self),
                        bytes,
                    });
                }
                Err(_) => continue,
            }
        }
    }

    /// Current concurrent charge (for diagnostics / snapshots).
    #[must_use]
    pub fn current(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    /// Peak concurrent anonymous bytes observed across the whole job.
    #[must_use]
    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Acquire)
    }

    /// Configured hard limit.
    #[must_use]
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Point-in-time snapshot for reconciliation.
    #[must_use]
    pub fn snapshot(&self) -> JobAnonSnapshot {
        JobAnonSnapshot {
            current: self.current(),
            peak: self.peak(),
            limit: self.limit,
        }
    }

    /// Atomic saturating release (called by [`AnonReservation::drop`]).
    ///
    /// Uses a CAS loop to avoid the stale-load underflow that the prior
    /// `fetch_sub(bytes.min(load()))` pattern allowed under concurrency.
    fn release_raw(&self, bytes: u64) {
        loop {
            let cur = self.current.load(Ordering::Acquire);
            let next = cur.saturating_sub(bytes);
            match self
                .current
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(_) => continue,
            }
        }
    }

    fn bump_peak(&self, candidate: u64) {
        let mut cur = self.peak.load(Ordering::Acquire);
        while candidate > cur {
            match self.peak.compare_exchange_weak(
                cur,
                candidate,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn reserve_and_drop_releases() {
        let ledger = Arc::new(JobAnonLedger::new(1024));
        let guard = ledger.reserve(100).expect("reserve 100 of 1024");
        assert_eq!(guard.bytes(), 100);
        assert_eq!(ledger.current(), 100);
        drop(guard);
        assert_eq!(ledger.current(), 0);
    }

    #[test]
    fn one_byte_over_budget_fails_before_allocation() {
        let ledger = Arc::new(JobAnonLedger::new(100));
        let _guard = ledger.reserve(100).expect("reserve exactly the limit");
        let err = ledger
            .reserve(1)
            .expect_err("one byte over budget must be rejected");
        assert_eq!(
            err,
            AnonLedgerError::BudgetExceeded {
                requested: 101,
                limit: 100
            }
        );
        // The failed reserve must NOT have charged the ledger.
        assert_eq!(ledger.current(), 100);
    }

    #[test]
    fn grow_over_budget_fails() {
        let ledger = Arc::new(JobAnonLedger::new(100));
        let mut guard = ledger.reserve(50).expect("reserve 50 of 100");
        let err = guard
            .grow(51)
            .expect_err("growing past the limit must be rejected");
        assert_eq!(
            err,
            AnonLedgerError::BudgetExceeded {
                requested: 101,
                limit: 100
            }
        );
        assert_eq!(ledger.current(), 50, "failed grow must not charge");
        guard.grow(50).expect("grow up to exactly the limit");
        assert_eq!(ledger.current(), 100);
        assert_eq!(guard.bytes(), 100);
    }

    #[test]
    fn concurrent_reserve_release_race() {
        let ledger = Arc::new(JobAnonLedger::new(u64::MAX));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let ledger = Arc::clone(&ledger);
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        let guard = ledger.reserve(1).expect("reserve(1) under u64::MAX limit");
                        drop(guard);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("worker thread panicked");
        }
        assert_eq!(ledger.current(), 0, "all charges must be released");
        assert!(ledger.peak() > 0, "peak must record concurrent activity");
    }

    #[test]
    fn snapshot_reports_truth() {
        let ledger = Arc::new(JobAnonLedger::new(1000));
        let guard = ledger.reserve(42).expect("reserve 42 of 1000");
        let snap = ledger.snapshot();
        assert_eq!(snap.current, 42);
        assert_eq!(snap.peak, 42);
        assert_eq!(snap.limit, 1000);
        drop(guard);
        let snap = ledger.snapshot();
        assert_eq!(snap.current, 0);
        assert_eq!(snap.peak, 42, "peak is sticky");
        assert_eq!(snap.limit, 1000);
    }

    #[test]
    fn zero_limit_rejects_everything() {
        let ledger = Arc::new(JobAnonLedger::new(0));
        let guard = ledger
            .reserve(0)
            .expect("zero-byte reservation is always admissible");
        assert_eq!(guard.bytes(), 0);
        assert_eq!(ledger.current(), 0);
        let err = ledger
            .reserve(1)
            .expect_err("any nonzero reservation must fail at limit 0");
        assert_eq!(
            err,
            AnonLedgerError::BudgetExceeded {
                requested: 1,
                limit: 0
            }
        );
        assert_eq!(ledger.current(), 0);
    }
}
