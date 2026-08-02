//! Overlay admission state machine (G-EM0.5a).
//!
//! Implements the bounded, FIFO-fair admission controller that gates overlay
//! mutations against the calibrated soft/hard limits defined in the parent
//! [`overlay_budget`](super) module. See the parent module docs for the
//! admission model; this file owns the concurrency machinery (blocked-writer
//! queue, edge-triggered build request, shutdown/cancel) and the observable
//! snapshot.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::{
    AdmissionOutcome, CategoryAccounting, OverlayAccountingSnapshot, OverlayBudgetConfig,
    PressureLevel, RejectReason, RetainedCategory, RetryReason,
};

/// A blocked writer's wake signal (FIFO fairness).
#[derive(Default)]
struct WaiterSignal {
    flag: std::sync::Mutex<bool>,
    cv: std::sync::Condvar,
}

impl WaiterSignal {
    fn notify(&self) {
        let mut flag = self.flag.lock().unwrap_or_else(|e| e.into_inner());
        *flag = true;
        self.cv.notify_one();
    }

    /// Waits up to `timeout` for a notification. Returns `true` if notified.
    fn wait(&self, timeout: Duration) -> bool {
        let mut flag = self.flag.lock().unwrap_or_else(|e| e.into_inner());
        if *flag {
            *flag = false;
            return true;
        }
        let (guard, result) = self
            .cv
            .wait_timeout(flag, timeout)
            .unwrap_or_else(|e| e.into_inner());
        flag = guard;
        let notified = *flag || !result.timed_out();
        *flag = false;
        notified
    }
}

struct Inner {
    category_bytes: [u64; RetainedCategory::COUNT],
    category_high_water: [u64; RetainedCategory::COUNT],
    total_bytes: u64,
    total_high_water: u64,
    accounting_errors: u64,
    admitted_count: u64,
    retryable_count: u64,
    rejected_count: u64,
    timed_out_count: u64,
    build_requested: bool,
    shutdown: bool,
    cancelled: bool,
    next_ticket: u64,
    /// FIFO queue of (ticket, signal, requested bytes) for blocked writers.
    waiters: VecDeque<(u64, Arc<WaiterSignal>, u64)>,
}

impl Inner {
    fn new() -> Self {
        Self {
            category_bytes: [0; RetainedCategory::COUNT],
            category_high_water: [0; RetainedCategory::COUNT],
            total_bytes: 0,
            total_high_water: 0,
            accounting_errors: 0,
            admitted_count: 0,
            retryable_count: 0,
            rejected_count: 0,
            timed_out_count: 0,
            build_requested: false,
            shutdown: false,
            cancelled: false,
            next_ticket: 0,
            waiters: VecDeque::new(),
        }
    }

    fn pressure(&self, soft: u64, hard: u64) -> PressureLevel {
        if self.total_bytes >= hard {
            PressureLevel::Hard
        } else if self.total_bytes >= soft {
            PressureLevel::Soft
        } else {
            PressureLevel::Normal
        }
    }

    /// Charges `bytes` to `cat`, updating totals/high-water. Returns `true` if
    /// this charge edge-triggered a fresh build request.
    fn charge(&mut self, cat: RetainedCategory, bytes: u64, soft: u64) -> bool {
        let idx = cat.index();
        let prev_total = self.total_bytes;
        self.category_bytes[idx] = self.category_bytes[idx].saturating_add(bytes);
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        if self.category_bytes[idx] > self.category_high_water[idx] {
            self.category_high_water[idx] = self.category_bytes[idx];
        }
        if self.total_bytes > self.total_high_water {
            self.total_high_water = self.total_bytes;
        }
        // Edge-trigger: first upward crossing of soft for this episode.
        let crossed = prev_total < soft && self.total_bytes >= soft && !self.build_requested;
        if crossed {
            self.build_requested = true;
        }
        crossed
    }

    fn release(&mut self, cat: RetainedCategory, bytes: u64, soft: u64) {
        let idx = cat.index();
        if bytes > self.category_bytes[idx] {
            self.accounting_errors = self.accounting_errors.saturating_add(1);
        }
        self.category_bytes[idx] = self.category_bytes[idx].saturating_sub(bytes);
        self.total_bytes = self.total_bytes.saturating_sub(bytes);
        // Re-arm the build request once pressure drops below soft.
        if self.total_bytes < soft {
            self.build_requested = false;
        }
    }
}

/// Bounded overlay admission controller.
///
/// Thread-safe; intended to be held in an `Arc` and shared across writer
/// threads and the buffer-manager pressure path. See the parent module docs
/// for the admission model.
pub struct OverlayAdmissionController {
    config: OverlayBudgetConfig,
    inner: Mutex<Inner>,
    active_epoch: AtomicU64,
}

impl OverlayAdmissionController {
    /// Creates a controller with validated configuration.
    ///
    /// # Errors
    ///
    /// Returns the configuration validation error if limits are invalid.
    pub fn new(config: OverlayBudgetConfig) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(Inner::new()),
            active_epoch: AtomicU64::new(0),
        })
    }

    /// The active configuration.
    #[must_use]
    pub const fn config(&self) -> &OverlayBudgetConfig {
        &self.config
    }

    /// Reports the active overlay epoch (set by the engine).
    #[must_use]
    pub fn active_epoch(&self) -> u64 {
        self.active_epoch.load(Ordering::Acquire)
    }

    /// Sets the active overlay epoch (engine seam).
    pub fn set_active_epoch(&self, epoch: u64) {
        self.active_epoch.store(epoch, Ordering::Release);
    }

    /// Current pressure level.
    #[must_use]
    pub fn pressure(&self) -> PressureLevel {
        let inner = self.inner.lock();
        inner.pressure(self.config.soft_limit_bytes, self.config.hard_limit_bytes)
    }

    /// Non-blocking admission (packet §2: "return a typed retryable error").
    ///
    /// Admits when capacity is available; at hard pressure returns
    /// [`AdmissionOutcome::Retryable`] immediately rather than blocking.
    pub fn try_reserve(&self, cat: RetainedCategory, bytes: u64) -> AdmissionOutcome {
        let mut inner = self.inner.lock();
        self.admit_if_ready(&mut inner, cat, bytes, true)
    }

    /// Bounded-blocking admission (packet §2: "block boundedly").
    ///
    /// FIFO-fair: writers blocked at hard pressure are admitted in arrival
    /// order. Honors the configured block timeout, cancellation, and shutdown.
    pub fn reserve(&self, cat: RetainedCategory, bytes: u64) -> AdmissionOutcome {
        // Fast path: non-blocking attempt first.
        {
            let mut inner = self.inner.lock();
            let outcome = self.admit_if_ready(&mut inner, cat, bytes, false);
            if !matches!(
                outcome,
                AdmissionOutcome::Retryable {
                    reason: RetryReason::HardPressure
                }
            ) {
                return outcome;
            }
        }
        // Slow path: register a FIFO waiter and block boundedly.
        let deadline = Instant::now() + self.config.max_block_duration;
        let signal = Arc::new(WaiterSignal::default());
        let ticket = {
            let mut inner = self.inner.lock();
            // Re-check under lock in case capacity freed between the two locks.
            let outcome = self.admit_if_ready(&mut inner, cat, bytes, false);
            if !matches!(
                outcome,
                AdmissionOutcome::Retryable {
                    reason: RetryReason::HardPressure
                }
            ) {
                return outcome;
            }
            let ticket = inner.next_ticket;
            inner.next_ticket = inner.next_ticket.saturating_add(1);
            inner
                .waiters
                .push_back((ticket, Arc::clone(&signal), bytes));
            ticket
        };

        loop {
            let now = Instant::now();
            if now >= deadline {
                return self.abandon_wait(ticket, true);
            }
            let waited = signal.wait(deadline - now);
            let mut inner = self.inner.lock();
            if inner.shutdown {
                self.remove_waiter_locked(&mut inner, ticket);
                inner.rejected_count = inner.rejected_count.saturating_add(1);
                return AdmissionOutcome::Rejected {
                    reason: RejectReason::Shutdown,
                };
            }
            if inner.cancelled {
                self.remove_waiter_locked(&mut inner, ticket);
                inner.rejected_count = inner.rejected_count.saturating_add(1);
                return AdmissionOutcome::Rejected {
                    reason: RejectReason::Cancelled,
                };
            }
            // Only the front waiter may be admitted (strict FIFO fairness).
            let at_front = inner.waiters.front().is_some_and(|(t, _, _)| *t == ticket);
            if at_front {
                let outcome = self.admit_if_ready(&mut inner, cat, bytes, false);
                match outcome {
                    AdmissionOutcome::Admitted { .. } => {
                        // admit_if_ready popped us via remove_waiter_locked below.
                        self.remove_waiter_locked(&mut inner, ticket);
                        self.wake_front_locked(&inner);
                        return outcome;
                    }
                    AdmissionOutcome::Rejected { reason } => {
                        self.remove_waiter_locked(&mut inner, ticket);
                        self.wake_front_locked(&inner);
                        return AdmissionOutcome::Rejected { reason };
                    }
                    AdmissionOutcome::Retryable { .. } => {
                        // Capacity still insufficient; keep waiting.
                    }
                }
            }
            drop(inner);
            if !waited {
                // Spurious/timeout edge: loop re-checks the deadline.
                continue;
            }
        }
    }

    /// Releases previously reserved retained bytes (packet §1 accounting).
    pub fn release(&self, cat: RetainedCategory, bytes: u64) {
        let mut inner = self.inner.lock();
        inner.release(cat, bytes, self.config.soft_limit_bytes);
        self.wake_front_locked(&inner);
    }

    /// Moves `bytes` of retained capacity from one category to another without
    /// changing the aggregate total (G-EM0.5c MAJOR-4).
    ///
    /// Used to re-attribute the next-epoch working set back to the mutable
    /// payload category when a generation handoff retires: the surviving N+1
    /// entities stay on the live overlay and become the next cycle's
    /// `MutationPayload`, so their charges migrate `NextEpoch -> MutationPayload`
    /// instead of vanishing (leak) or double-counting (ratchet). Because the
    /// aggregate total is unchanged, no admission check applies and no blocked
    /// writer can be admitted by this call.
    ///
    /// Fail-closed: if `from` holds fewer than `bytes`, only the available
    /// amount is moved and an accounting error is recorded (mirrors
    /// [`Inner::release`] underflow accounting).
    pub fn transfer_retained(&self, from: RetainedCategory, to: RetainedCategory, bytes: u64) {
        let mut inner = self.inner.lock();
        let f = from.index();
        let t = to.index();
        if f == t || bytes == 0 {
            return;
        }
        let available = inner.category_bytes[f].min(bytes);
        if available < bytes {
            inner.accounting_errors = inner.accounting_errors.saturating_add(1);
        }
        inner.category_bytes[f] = inner.category_bytes[f].saturating_sub(available);
        inner.category_bytes[t] = inner.category_bytes[t].saturating_add(available);
        if inner.category_bytes[t] > inner.category_high_water[t] {
            inner.category_high_water[t] = inner.category_bytes[t];
        }
        // total_bytes is unchanged by design; the build-request latch is
        // therefore untouched (capacity neither freed nor consumed).
    }

    /// Releases all retained bytes across every category to zero.
    ///
    /// Called by the engine after a generation build drains the overlay: the
    /// frozen epoch's mutations, dirty/deletion sets, next-epoch state, queued
    /// work, and WAL buffers are no longer retained. Re-arms the one-coordinator
    /// build request (total drops below soft) and wakes any blocked writers.
    pub fn drain_all_retained(&self) {
        let mut inner = self.inner.lock();
        for cat in RetainedCategory::ALL {
            let idx = cat.index();
            let current = inner.category_bytes[idx];
            if current > 0 {
                inner.release(cat, current, self.config.soft_limit_bytes);
            }
        }
        self.wake_front_locked(&inner);
    }

    /// Records that the requested generation build completed (engine seam).
    ///
    /// Re-arms the one-coordinator build request so the next upward soft
    /// crossing triggers a fresh request. A successful build normally drains
    /// the overlay below soft (which also re-arms via [`Inner::release`]).
    pub fn complete_generation_build(&self) {
        let mut inner = self.inner.lock();
        if inner.total_bytes < self.config.soft_limit_bytes {
            inner.build_requested = false;
        }
    }

    /// Requests a graceful shutdown: blocked writers are rejected and no
    /// further writes are admitted.
    pub fn request_shutdown(&self) {
        let mut inner = self.inner.lock();
        inner.shutdown = true;
        self.wake_all_locked(&inner);
    }

    /// Cancels admission: equivalent to shutdown for blocked/future writers
    /// but reported as [`RejectReason::Cancelled`].
    pub fn cancel(&self) {
        let mut inner = self.inner.lock();
        inner.cancelled = true;
        self.wake_all_locked(&inner);
    }

    /// Takes an observable snapshot (packet §5).
    #[must_use]
    pub fn snapshot(&self) -> OverlayAccountingSnapshot {
        let inner = self.inner.lock();
        let mut categories = [CategoryAccounting::default(); RetainedCategory::COUNT];
        for (i, cat) in RetainedCategory::ALL.iter().enumerate() {
            let idx = cat.index();
            categories[i] = CategoryAccounting {
                current_bytes: inner.category_bytes[idx],
                high_water_bytes: inner.category_high_water[idx],
            };
        }
        OverlayAccountingSnapshot {
            categories,
            total_bytes: inner.total_bytes,
            total_high_water_bytes: inner.total_high_water,
            soft_limit_bytes: self.config.soft_limit_bytes,
            hard_limit_bytes: self.config.hard_limit_bytes,
            pressure: inner.pressure(self.config.soft_limit_bytes, self.config.hard_limit_bytes),
            build_requested: inner.build_requested,
            active_epoch: self.active_epoch(),
            admitted_count: inner.admitted_count,
            retryable_count: inner.retryable_count,
            rejected_count: inner.rejected_count,
            timed_out_count: inner.timed_out_count,
            accounting_errors: inner.accounting_errors,
            blocked_writers: inner.waiters.len(),
        }
    }

    /// Admits if state allows; otherwise returns the typed non-admitted outcome.
    ///
    /// When `non_blocking` is true, hard pressure yields an immediate
    /// [`RetryReason::HardPressure`]. When false, the same outcome is returned
    /// so the caller can decide to block. Oversized/shutdown/cancelled are
    /// always terminal rejections.
    fn admit_if_ready(
        &self,
        inner: &mut Inner,
        cat: RetainedCategory,
        bytes: u64,
        _non_blocking: bool,
    ) -> AdmissionOutcome {
        if inner.shutdown {
            inner.rejected_count = inner.rejected_count.saturating_add(1);
            return AdmissionOutcome::Rejected {
                reason: RejectReason::Shutdown,
            };
        }
        if inner.cancelled {
            inner.rejected_count = inner.rejected_count.saturating_add(1);
            return AdmissionOutcome::Rejected {
                reason: RejectReason::Cancelled,
            };
        }
        let hard = self.config.hard_limit_bytes;
        if bytes > hard {
            inner.rejected_count = inner.rejected_count.saturating_add(1);
            return AdmissionOutcome::Rejected {
                reason: RejectReason::Oversized,
            };
        }
        let would_exceed = inner
            .total_bytes
            .checked_add(bytes)
            .is_none_or(|next| next > hard);
        if would_exceed {
            inner.retryable_count = inner.retryable_count.saturating_add(1);
            return AdmissionOutcome::Retryable {
                reason: RetryReason::HardPressure,
            };
        }
        let crossed = inner.charge(cat, bytes, self.config.soft_limit_bytes);
        inner.admitted_count = inner.admitted_count.saturating_add(1);
        AdmissionOutcome::Admitted {
            pressure: inner.pressure(self.config.soft_limit_bytes, hard),
            requested_build: crossed,
        }
    }

    fn remove_waiter_locked(&self, inner: &mut Inner, ticket: u64) {
        if let Some(pos) = inner.waiters.iter().position(|(t, _, _)| *t == ticket) {
            inner.waiters.remove(pos);
        }
    }

    fn abandon_wait(&self, ticket: u64, timed_out: bool) -> AdmissionOutcome {
        let mut inner = self.inner.lock();
        self.remove_waiter_locked(&mut inner, ticket);
        if timed_out {
            inner.timed_out_count = inner.timed_out_count.saturating_add(1);
            inner.retryable_count = inner.retryable_count.saturating_add(1);
        }
        self.wake_front_locked(&inner);
        AdmissionOutcome::Retryable {
            reason: RetryReason::Timeout,
        }
    }

    fn wake_front_locked(&self, inner: &Inner) {
        if let Some((_, signal, _)) = inner.waiters.front() {
            signal.notify();
        }
    }

    fn wake_all_locked(&self, inner: &Inner) {
        for (_, signal, _) in &inner.waiters {
            signal.notify();
        }
    }
}

impl std::fmt::Debug for OverlayAdmissionController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OverlayAdmissionController")
            .field("config", &self.config)
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctl(soft: u64, hard: u64) -> OverlayAdmissionController {
        OverlayAdmissionController::new(OverlayBudgetConfig {
            soft_limit_bytes: soft,
            hard_limit_bytes: hard,
            max_block_duration: Duration::from_millis(100),
        })
        .expect("valid config")
    }

    #[test]
    fn admits_below_soft_with_normal_pressure() {
        let c = ctl(100, 200);
        match c.try_reserve(RetainedCategory::MutationPayload, 50) {
            AdmissionOutcome::Admitted {
                pressure,
                requested_build,
            } => {
                assert_eq!(pressure, PressureLevel::Normal);
                assert!(!requested_build);
            }
            other => panic!("expected admit, got {other:?}"),
        }
        assert_eq!(c.snapshot().total_bytes, 50);
    }

    #[test]
    fn soft_crossing_edge_triggers_exactly_one_build_request() {
        let c = ctl(100, 1000);
        // First admit crossing soft: requests build.
        match c.try_reserve(RetainedCategory::MutationPayload, 120) {
            AdmissionOutcome::Admitted {
                pressure,
                requested_build,
            } => {
                assert_eq!(pressure, PressureLevel::Soft);
                assert!(requested_build, "first crossing must request build");
            }
            other => panic!("expected admit, got {other:?}"),
        }
        // Second admit still soft but must NOT re-request.
        match c.try_reserve(RetainedCategory::MutationPayload, 10) {
            AdmissionOutcome::Admitted {
                pressure,
                requested_build,
            } => {
                assert_eq!(pressure, PressureLevel::Soft);
                assert!(!requested_build, "only one coordinator requests");
            }
            other => panic!("expected admit, got {other:?}"),
        }
        assert!(c.snapshot().build_requested);
    }

    #[test]
    fn hard_pressure_try_reserve_is_retryable() {
        let c = ctl(50, 100);
        assert!(matches!(
            c.try_reserve(RetainedCategory::WalBuffers, 90),
            AdmissionOutcome::Admitted { .. }
        ));
        // Now at 90/100; another 20 would exceed hard.
        match c.try_reserve(RetainedCategory::WalBuffers, 20) {
            AdmissionOutcome::Retryable {
                reason: RetryReason::HardPressure,
            } => {}
            other => panic!("expected retryable hard pressure, got {other:?}"),
        }
    }

    #[test]
    fn oversized_request_is_rejected_not_retryable() {
        let c = ctl(50, 100);
        match c.try_reserve(RetainedCategory::NextEpoch, 101) {
            AdmissionOutcome::Rejected {
                reason: RejectReason::Oversized,
            } => {}
            other => panic!("expected oversized rejection, got {other:?}"),
        }
    }

    #[test]
    fn release_rearms_build_request_below_soft() {
        let c = ctl(100, 1000);
        let _ = c.try_reserve(RetainedCategory::MutationPayload, 150);
        assert!(c.snapshot().build_requested);
        c.release(RetainedCategory::MutationPayload, 100); // now 50 < 100
        assert!(!c.snapshot().build_requested, "re-armed below soft");
        // Next upward crossing requests again.
        match c.try_reserve(RetainedCategory::MutationPayload, 60) {
            AdmissionOutcome::Admitted {
                requested_build, ..
            } => assert!(requested_build, "re-request after re-arm"),
            other => panic!("expected admit, got {other:?}"),
        }
    }

    #[test]
    fn release_underflow_counts_accounting_error_and_saturates() {
        let c = ctl(100, 200);
        c.release(RetainedCategory::DirtySets, 10); // nothing reserved
        let snap = c.snapshot();
        assert_eq!(snap.accounting_errors, 1);
        assert_eq!(snap.total_bytes, 0);
    }

    #[test]
    fn shutdown_rejects_new_and_blocked_writers() {
        let c = Arc::new(ctl(50, 100));
        let _ = c.try_reserve(RetainedCategory::MutationPayload, 100);
        let c2 = Arc::clone(&c);
        let handle = std::thread::spawn(move || c2.reserve(RetainedCategory::MutationPayload, 50));
        // Give the writer time to block.
        std::thread::sleep(Duration::from_millis(20));
        c.request_shutdown();
        let outcome = handle.join().expect("writer thread");
        assert!(matches!(
            outcome,
            AdmissionOutcome::Rejected {
                reason: RejectReason::Shutdown
            }
        ));
        // Subsequent reserve also rejected.
        assert!(matches!(
            c.reserve(RetainedCategory::MutationPayload, 1),
            AdmissionOutcome::Rejected {
                reason: RejectReason::Shutdown
            }
        ));
    }

    #[test]
    fn blocked_writer_admitted_fifo_after_release() {
        let c = Arc::new(ctl(50, 100));
        let _ = c.try_reserve(RetainedCategory::MutationPayload, 100); // fill to hard
        let c2 = Arc::clone(&c);
        let handle = std::thread::spawn(move || c2.reserve(RetainedCategory::MutationPayload, 40));
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(c.snapshot().blocked_writers, 1);
        c.release(RetainedCategory::MutationPayload, 60); // room for 40 now
        let outcome = handle.join().expect("writer thread");
        assert!(matches!(outcome, AdmissionOutcome::Admitted { .. }));
        assert_eq!(c.snapshot().total_bytes, 80);
    }

    #[test]
    fn blocked_writer_times_out_retryably() {
        let c = Arc::new(ctl(50, 100));
        let _ = c.try_reserve(RetainedCategory::MutationPayload, 100);
        let c2 = Arc::clone(&c);
        let outcome = std::thread::spawn(move || c2.reserve(RetainedCategory::MutationPayload, 50))
            .join()
            .expect("writer thread");
        assert!(matches!(
            outcome,
            AdmissionOutcome::Retryable {
                reason: RetryReason::Timeout
            }
        ));
        assert_eq!(c.snapshot().timed_out_count, 1);
    }
}
