//! Optional control surface for long-running index builds (G-OBS.1).
//!
//! The engine's index-build loops are historically unmonitored: a long
//! vector-index build cannot be stopped and reports no progress. This module
//! provides a pollable cancellation check plus a progress callback that the
//! `*_with_control` index-build entry points accept. `None` (or a control with
//! empty fields) preserves the historical unmonitored behavior exactly.

use std::sync::Arc;

/// Pollable cancellation check: returns `true` when the build should stop.
pub type IndexBuildCancelCheck = Arc<dyn Fn() -> bool + Send + Sync>;

/// Progress callback: `(done, total)` row counts, monotonic `done`.
pub type IndexBuildProgress = Arc<dyn Fn(u64, u64) + Send + Sync>;

/// Optional control surface for long-running index builds.
///
/// Passed into the `*_with_control` index-build entry points. `None` (or empty
/// fields) preserves the historical unmonitored behavior exactly.
///
/// Cadence contract: `cancel_check` is polled every 256 loop iterations and
/// `progress` fires every 1024 iterations plus once at completion
/// (`done == total`). On cancel the build returns `Error::Cancelled` before
/// the index is registered, so a cancelled build never leaves a partial index.
#[derive(Default, Clone)]
pub struct IndexBuildControl {
    /// Polled at build cadence; `true` means "stop the build".
    pub cancel_check: Option<IndexBuildCancelCheck>,
    /// Receives `(done, total)` at progress cadence and on completion.
    pub progress: Option<IndexBuildProgress>,
}

impl IndexBuildControl {
    /// Creates an empty control surface (no cancellation, no progress).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the cancellation check.
    #[must_use]
    pub fn with_cancel_check(mut self, check: IndexBuildCancelCheck) -> Self {
        self.cancel_check = Some(check);
        self
    }

    /// Sets the progress callback.
    #[must_use]
    pub fn with_progress(mut self, progress: IndexBuildProgress) -> Self {
        self.progress = Some(progress);
        self
    }

    /// Poll cadence counter helper: call with the 0-based iteration counter;
    /// returns `true` when cancelled (cancel every 256, progress every 1024
    /// handled via `maybe_progress`).
    #[must_use]
    pub fn maybe_cancel(&self, iteration: u64) -> bool {
        iteration.is_multiple_of(256) && self.cancel_check.as_ref().is_some_and(|c| c())
    }

    /// Fire progress at cadence (every 1024); `done` is count completed so far.
    pub fn maybe_progress(&self, iteration: u64, done: u64, total: u64) {
        if iteration.is_multiple_of(1024)
            && let Some(progress) = &self.progress
        {
            progress(done, total);
        }
    }

    /// Unconditional final progress event (`done == total`).
    pub fn finish_progress(&self, total: u64) {
        if let Some(progress) = &self.progress {
            progress(total, total);
        }
    }
}
