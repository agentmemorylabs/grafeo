//! Overlay retained-capacity accounting, admission, and backpressure (G-EM0.5a).
//!
//! The mutable overlay of a writable (`W`-mode) database must stay bounded:
//! anonymous memory may grow with the configured overlay budget but never with
//! total durable database size. This module provides the single authority that
//! accounts *retained* capacity across every overlay category, enforces
//! calibrated soft/hard limits, and admits or backpressures writers.
//!
//! # Model
//!
//! Retained bytes are charged per [`RetainedCategory`]. Admission is gated on
//! the aggregate of all categories:
//!
//! - Below the soft limit: writes are admitted with [`PressureLevel::Normal`].
//! - At/above the soft limit: writes are still admitted, but the *first*
//!   upward crossing of the soft limit edge-triggers exactly one
//!   [`PressureLevel::Soft`] build request (one coordinator asks for a
//!   generation build). The request stays latched until pressure drops below
//!   the soft limit (a successful build drains the overlay) and is re-armed.
//! - At the hard limit: writers either block boundedly (FIFO-fair, with
//!   timeout/cancellation/shutdown) via [`OverlayAdmissionController::reserve`],
//!   or receive a typed retryable rejection via
//!   [`OverlayAdmissionController::try_reserve`]. The controller never admits a
//!   mutation that would push retained bytes above the hard limit, so
//!   unbounded enqueue is impossible by construction.
//!
//! Durability ordering is the caller's contract: an admitted write must be
//! durable in the WAL *before* the caller acknowledges it to its client. The
//! controller only gates admission; it does not itself write the WAL.
//!
//! # Fail-closed accounting
//!
//! Any arithmetic overflow or release-underflow is counted in
//! [`OverlayAccountingSnapshot::accounting_errors`] and saturates rather than
//! wrapping. Accounting drift is therefore observable and never silently
//! corrupts the limit comparison.
//!
//! # Layout
//!
//! This file holds the data model (categories, config, pressure/outcome enums,
//! and the observable snapshot). The admission state machine lives in the
//! [`controller`] submodule and is re-exported here so the public API stays
//! `overlay_budget::OverlayAdmissionController`.

use std::time::Duration;

mod controller;

pub use controller::OverlayAdmissionController;

/// Default soft limit: 32 MiB (planning value, packet §2).
pub const DEFAULT_SOFT_LIMIT_BYTES: u64 = 32 * 1024 * 1024;
/// Default hard limit: 64 MiB (planning value, packet §2).
pub const DEFAULT_HARD_LIMIT_BYTES: u64 = 64 * 1024 * 1024;
/// Default bounded-block timeout for writers waiting at hard pressure.
pub const DEFAULT_MAX_BLOCK_DURATION: Duration = Duration::from_secs(5);

/// Categories of retained overlay capacity that count toward pressure.
///
/// The packet requires accounting for retained capacity — not only logical
/// payload — across inserted/updated data, bookkeeping sets, next-epoch state,
/// queued work, and WAL buffers. Each variant maps to one slot in the
/// controller's fixed-size counter array.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum RetainedCategory {
    /// Inserted/updated node, edge, and property payloads held in the overlay.
    MutationPayload = 0,
    /// Dirty-id sets tracking overlay-created/modified base entities.
    DirtySets = 1,
    /// Base-deletion sets (deleted-from-base node/edge id logs).
    DeletionSets = 2,
    /// Next-epoch state being assembled before a generation build freezes it.
    NextEpoch = 3,
    /// Queued mutation work awaiting admission or flush.
    QueuedWork = 4,
    /// WAL buffers not yet flushed/truncated.
    WalBuffers = 5,
}

impl RetainedCategory {
    /// All categories, in stable index order.
    pub const ALL: [RetainedCategory; Self::COUNT] = [
        Self::MutationPayload,
        Self::DirtySets,
        Self::DeletionSets,
        Self::NextEpoch,
        Self::QueuedWork,
        Self::WalBuffers,
    ];

    /// Number of categories.
    pub const COUNT: usize = 6;

    /// Stable index into the counter array.
    #[must_use]
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Calibrated overlay admission limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverlayBudgetConfig {
    /// Soft limit in bytes; upward crossing edge-triggers one build request.
    pub soft_limit_bytes: u64,
    /// Hard limit in bytes; never exceeded by an admitted mutation.
    pub hard_limit_bytes: u64,
    /// Maximum time a writer blocks at hard pressure before a retryable timeout.
    pub max_block_duration: Duration,
}

impl Default for OverlayBudgetConfig {
    fn default() -> Self {
        Self {
            soft_limit_bytes: DEFAULT_SOFT_LIMIT_BYTES,
            hard_limit_bytes: DEFAULT_HARD_LIMIT_BYTES,
            max_block_duration: DEFAULT_MAX_BLOCK_DURATION,
        }
    }
}

impl OverlayBudgetConfig {
    /// Small-budget configuration for focused tests.
    #[must_use]
    pub const fn for_tests() -> Self {
        Self {
            soft_limit_bytes: 4 * 1024,
            hard_limit_bytes: 8 * 1024,
            max_block_duration: Duration::from_millis(250),
        }
    }

    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns a message when the hard limit is zero or the soft limit exceeds
    /// the hard limit.
    pub fn validate(&self) -> Result<(), String> {
        if self.hard_limit_bytes == 0 {
            return Err("hard_limit_bytes must be non-zero".to_string());
        }
        if self.soft_limit_bytes > self.hard_limit_bytes {
            return Err(format!(
                "soft_limit_bytes ({}) must not exceed hard_limit_bytes ({})",
                self.soft_limit_bytes, self.hard_limit_bytes
            ));
        }
        Ok(())
    }
}

/// Pressure level derived from aggregate retained bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureLevel {
    /// Below the soft limit.
    Normal,
    /// At/above the soft limit, below the hard limit.
    Soft,
    /// At/above the hard limit.
    Hard,
}

/// Outcome of an admission attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionOutcome {
    /// The mutation was admitted.
    Admitted {
        /// Pressure level after admission.
        pressure: PressureLevel,
        /// `true` only on the single admit that edge-triggered the build
        /// request for this soft-pressure episode (the one coordinator).
        requested_build: bool,
    },
    /// The mutation was not admitted but may succeed if retried later.
    Retryable {
        /// Why it can be retried.
        reason: RetryReason,
    },
    /// The mutation was rejected and will not succeed without a change in
    /// request shape or controller state.
    Rejected {
        /// Why it was rejected.
        reason: RejectReason,
    },
}

/// Retryable rejection reasons (typed, packet §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryReason {
    /// Hard pressure: capacity unavailable now; a later attempt after a build
    /// or release may succeed.
    HardPressure,
    /// The bounded block timed out at hard pressure.
    Timeout,
}

/// Non-retryable rejection reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// The single request exceeds the hard limit; it can never be admitted.
    Oversized,
    /// The controller is shutting down; no further writes are admitted.
    Shutdown,
    /// The controller was cancelled; blocked and future writers are rejected.
    Cancelled,
}

/// Per-category retained-byte accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CategoryAccounting {
    /// Current retained bytes.
    pub current_bytes: u64,
    /// High-water mark of retained bytes since construction/reset.
    pub high_water_bytes: u64,
}

/// Observable admission/accounting state (packet §5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverlayAccountingSnapshot {
    /// Per-category current and high-water retained bytes.
    pub categories: [CategoryAccounting; RetainedCategory::COUNT],
    /// Aggregate current retained bytes across all categories.
    pub total_bytes: u64,
    /// Aggregate high-water mark.
    pub total_high_water_bytes: u64,
    /// Configured soft limit.
    pub soft_limit_bytes: u64,
    /// Configured hard limit.
    pub hard_limit_bytes: u64,
    /// Current pressure level.
    pub pressure: PressureLevel,
    /// Whether a build request is currently latched (one coordinator).
    pub build_requested: bool,
    /// Active overlay epoch reported by the engine.
    pub active_epoch: u64,
    /// Count of admitted mutations.
    pub admitted_count: u64,
    /// Count of retryable rejections.
    pub retryable_count: u64,
    /// Count of non-retryable rejections.
    pub rejected_count: u64,
    /// Count of bounded-block timeouts.
    pub timed_out_count: u64,
    /// Count of accounting overflow/underflow events (should stay 0).
    pub accounting_errors: u64,
    /// Number of writers currently blocked at hard pressure.
    pub blocked_writers: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_zero_hard_and_inverted_limits() {
        assert!(
            OverlayBudgetConfig {
                soft_limit_bytes: 10,
                hard_limit_bytes: 0,
                max_block_duration: Duration::from_millis(1),
            }
            .validate()
            .is_err()
        );
        assert!(
            OverlayBudgetConfig {
                soft_limit_bytes: 20,
                hard_limit_bytes: 10,
                max_block_duration: Duration::from_millis(1),
            }
            .validate()
            .is_err()
        );
        assert!(OverlayBudgetConfig::default().validate().is_ok());
    }

    #[test]
    fn category_indexes_are_stable_and_distinct() {
        let mut seen = [false; RetainedCategory::COUNT];
        for cat in RetainedCategory::ALL {
            let idx = cat.index();
            assert!(idx < RetainedCategory::COUNT);
            assert!(!seen[idx], "duplicate index {idx}");
            seen[idx] = true;
        }
        assert!(seen.iter().all(|&s| s), "every index must be covered");
    }
}
