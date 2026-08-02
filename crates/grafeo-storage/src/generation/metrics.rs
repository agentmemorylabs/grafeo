//! Phase metrics and Linux RssAnon sampler (G-EM0.W0-A1, W0 contract §8).
//!
//! The build must report truthful resource usage. This module provides:
//! - [`ExternalSortMetrics`] — temp-disk counters, run counts, merge passes.
//! - [`RssAnonSampler`] — Linux `/proc/<pid>/status` RssAnon reader.
//!
//! Per the skill `grafeo-generation-writable-contract`: same-process
//! before/after delta is **not** accepted peak proof. The sampler is a
//! building block for the fresh-child pattern; W0-A1 exposes the primitive,
//! W0-B / the benchmark composes it.

use std::fs;
use std::path::Path;

// R2: enforcing ledger types live in grafeo-core (dependency direction).
pub use grafeo_core::graph::compact::generation::ledger::{
    AnonLedgerError, AnonReservation, JobAnonLedger, JobAnonSnapshot,
};

/// External-sort metrics: truthful temp-disk accounting + pass counters.
#[derive(Debug, Clone, Default)]
pub struct ExternalSortMetrics {
    /// Peak temporary disk charge observed.
    pub temp_bytes_peak: u64,
    /// Current temporary disk charge (live files).
    pub temp_bytes_current: u64,
    /// Peak anonymous (in-memory sort arena) charge observed.
    pub anon_bytes_peak: u64,
    /// Current anonymous charge (live arena bytes).
    pub anon_bytes_current: u64,
    /// Total sorted runs formed.
    pub run_count: u64,
    /// Maximum simultaneously open run readers.
    pub max_open_runs: u64,
    /// Recursive merge passes executed.
    pub merge_passes: u64,
    /// Total records processed.
    pub record_count: u64,
}

impl ExternalSortMetrics {
    /// Reserve `bytes` against the temp-disk counter. Updates peak. Fails
    /// before crossing `limit`.
    ///
    /// # Errors
    /// Returns a budget-exceeded error string if `current + bytes > limit`.
    pub fn reserve_temp(&mut self, bytes: u64, limit: u64) -> Result<(), ExternalSortMetricsError> {
        let next = self
            .temp_bytes_current
            .checked_add(bytes)
            .ok_or(ExternalSortMetricsError::Overflow)?;
        if next > limit {
            return Err(ExternalSortMetricsError::BudgetExceeded {
                requested: next,
                limit,
            });
        }
        self.temp_bytes_current = next;
        self.temp_bytes_peak = self.temp_bytes_peak.max(next);
        Ok(())
    }

    /// Release `bytes` from the temp-disk counter (saturating).
    pub fn release_temp(&mut self, bytes: u64) {
        self.temp_bytes_current = self.temp_bytes_current.saturating_sub(bytes);
    }

    /// Reserve `bytes` against the anonymous (in-memory arena) counter.
    /// Updates peak. Fails before crossing `limit`.
    ///
    /// # Errors
    /// Returns a budget-exceeded error if `current + bytes > limit`.
    pub fn reserve_anon(&mut self, bytes: u64, limit: u64) -> Result<(), ExternalSortMetricsError> {
        let next = self
            .anon_bytes_current
            .checked_add(bytes)
            .ok_or(ExternalSortMetricsError::Overflow)?;
        if next > limit {
            return Err(ExternalSortMetricsError::BudgetExceeded {
                requested: next,
                limit,
            });
        }
        self.anon_bytes_current = next;
        self.anon_bytes_peak = self.anon_bytes_peak.max(next);
        Ok(())
    }

    /// Release `bytes` from the anonymous counter (saturating).
    pub fn release_anon(&mut self, bytes: u64) {
        self.anon_bytes_current = self.anon_bytes_current.saturating_sub(bytes);
    }

    /// Update `max_open_runs` if `count` exceeds the current value.
    pub fn observe_open_runs(&mut self, count: usize) {
        self.max_open_runs = self.max_open_runs.max(count as u64);
    }
}

/// Metrics error for budget reservation failures and I/O errors.
#[derive(Debug)]
pub enum ExternalSortMetricsError {
    /// Requested charge would exceed the configured limit.
    BudgetExceeded {
        /// Total that would have been charged.
        requested: u64,
        /// Configured limit.
        limit: u64,
    },
    /// u64 overflow during checked add.
    Overflow,
    /// I/O error during run/merge file operations.
    Io(String),
}

impl std::fmt::Display for ExternalSortMetricsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BudgetExceeded { requested, limit } => {
                write!(
                    f,
                    "temp budget exceeded: requested {requested}, limit {limit}"
                )
            }
            Self::Overflow => write!(f, "u64 overflow in temp accounting"),
            Self::Io(msg) => write!(f, "external sort i/o: {msg}"),
        }
    }
}

impl std::error::Error for ExternalSortMetricsError {}

impl ExternalSortMetricsError {
    /// Convenience: wrap an `io::Error` into the metrics error enum.
    pub fn from_io(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

/// One RssAnon sample (in KiB) from `/proc/<pid>/status`.
#[derive(Debug, Clone, Copy)]
pub struct RssAnonSample {
    /// RssAnon in KiB.
    pub rss_anon_kb: u64,
}

/// Sampler for reading `/proc/<pid>/status` RssAnon on Linux.
pub struct RssAnonSampler {
    pid: u32,
}

impl RssAnonSampler {
    /// Create a sampler for the current process.
    #[must_use]
    pub fn current() -> Self {
        Self {
            pid: std::process::id(),
        }
    }

    /// Create a sampler for a specific child PID.
    #[must_use]
    pub fn for_pid(pid: u32) -> Self {
        Self { pid }
    }

    /// Read the current RssAnon value.
    ///
    /// Returns `None` on non-Linux platforms or if `/proc` is unavailable.
    #[must_use]
    pub fn sample(&self) -> Option<RssAnonSample> {
        read_rss_anon_kb(self.pid).map(|kb| RssAnonSample { rss_anon_kb: kb })
    }
}

/// Read `RssAnon:` from `/proc/<pid>/status`. Returns `None` on any error
/// or non-Linux platform.
fn read_rss_anon_kb(pid: u32) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let path = Path::new("/proc").join(pid.to_string()).join("status");
        let content = fs::read_to_string(&path).ok()?;
        for line in content.lines() {
            if let Some(rest) = line.strip_prefix("RssAnon:") {
                let num_part = rest.split_whitespace().next()?;
                return num_part.parse::<u64>().ok();
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = pid;
        None
    }
}
