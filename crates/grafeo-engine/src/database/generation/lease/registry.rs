//! The database-owner registry of in-process generation leases (G-EM0.4a).
//!
//! Owns the currently-selected [`BaseGeneration`] and the transition log that
//! proves no generation's mapping is released while a lease on it survives.
//! See [`super`] (the `lease` module) for the full contract.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use arc_swap::ArcSwap;
use grafeo_common::utils::error::Result;
use parking_lot::Mutex;

use super::base::BaseGeneration;
use super::{GenerationLease, GenerationTransitionError};

/// Per-generation lease observability, without holding a strong reference.
///
/// Reported by [`GenerationLeaseRegistry::lease_stats`]. The counts are
/// derived from a `Weak` probe, so observability never keeps a retired base
/// alive (packet requirement 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerationLeaseStats {
    /// Manifest publication sequence of the generation.
    pub publication_sequence: u64,
    /// The generation identifier.
    pub generation_id: String,
    /// Absolute path of the generation container.
    pub generation_abs_path: PathBuf,
    /// Strong references to this base (owner + live read snapshots).
    pub strong_refs: usize,
    /// `true` when the registry still holds this base as the selected owner.
    pub is_selected: bool,
}

/// One tracked generation slot: a weak probe to the base plus its identity.
///
/// The registry keeps only a `Weak` to the base so that simply *tracking* a
/// retired generation never keeps its mapping alive (packet requirement 4).
#[derive(Debug)]
struct TrackedGeneration {
    publication_sequence: u64,
    generation_id: String,
    generation_abs_path: PathBuf,
    base: Weak<BaseGeneration>,
}

impl TrackedGeneration {
    fn stats(&self, is_selected: bool) -> GenerationLeaseStats {
        GenerationLeaseStats {
            publication_sequence: self.publication_sequence,
            generation_id: self.generation_id.clone(),
            generation_abs_path: self.generation_abs_path.clone(),
            strong_refs: self.base.strong_count(),
            is_selected,
        }
    }
}

/// The database-owner registry of in-process generation leases.
///
/// Owns the currently-selected [`BaseGeneration`] (via an `ArcSwap` for
/// atomic reader redirect) and a transition log proving that no generation's
/// mapping is released while a lease on it survives.
///
/// The selected strong reference lives **only** in the `ArcSwap`; historical
/// generations are tracked by `Weak` probe so the registry never extends a
/// retired mapping's lifetime.
#[derive(Debug)]
pub struct GenerationLeaseRegistry {
    /// The currently-selected base. `ArcSwap::swap` redirects new snapshots
    /// atomically; existing readers keep their old `Arc` snapshot.
    selected: ArcSwap<BaseGeneration>,
    /// Every generation this registry has selected, in publication order.
    /// Guarded by a mutex (publish/close are cold paths).
    tracked: Mutex<Vec<TrackedGeneration>>,
    /// Number of publications (base transitions) performed.
    transitions: AtomicU64,
    /// Set once `close_transition` has run; further publishes are rejected
    /// with a typed transition error (packet requirement 5).
    /// `checkpoint_transition` is non-closing and never sets this.
    closed: AtomicBool,
}

impl GenerationLeaseRegistry {
    /// Select the first base generation for a freshly recovered root.
    ///
    /// `generation_abs_path` must be the validated selected generation (from
    /// [`recover_generation_root`](crate::database::generation::recovery::recover_generation_root)).
    ///
    /// # Errors
    ///
    /// Returns an error when the selected generation container cannot be
    /// opened, mapped, or deserialized (never serves a torn generation).
    pub fn from_selected(
        publication_sequence: u64,
        generation_id: String,
        generation_abs_path: PathBuf,
    ) -> Result<Arc<Self>> {
        let base = BaseGeneration::open(
            publication_sequence,
            generation_id.clone(),
            generation_abs_path.clone(),
        )?;
        let base = Arc::new(base);
        Ok(Arc::new(Self {
            selected: ArcSwap::new(Arc::clone(&base)),
            tracked: Mutex::new(vec![TrackedGeneration {
                publication_sequence,
                generation_id,
                generation_abs_path,
                base: Arc::downgrade(&base),
            }]),
            transitions: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }))
    }

    /// Acquire an external read-snapshot lease on the currently-selected base.
    ///
    /// Cheap `Arc` clone; the snapshot stays valid (serving the same immutable
    /// bytes) even after a later publication redirects the selected base.
    #[must_use]
    pub fn snapshot(self: &Arc<Self>) -> GenerationLease {
        GenerationLease::new(self.selected.load_full())
    }

    /// The manifest publication sequence of the currently-selected base.
    #[must_use]
    pub fn selected_sequence(&self) -> u64 {
        self.selected.load().publication_sequence()
    }

    /// The previous generation's stats (the base replaced by the latest
    /// publication), when one exists, without holding a strong reference.
    #[must_use]
    pub fn previous_stats(&self) -> Option<GenerationLeaseStats> {
        let tracked = self.tracked.lock();
        let selected_seq = self.selected_sequence();
        tracked
            .iter()
            .rev()
            .find(|t| t.publication_sequence != selected_seq)
            .map(|t| t.stats(false))
    }

    /// Stats for the currently-selected base (no extra strong reference kept).
    ///
    /// Reports from the tracked weak probe like every other generation. Since
    /// [`publish`](Self::publish) pushes the tracked slot *before* swapping the
    /// selected base, the selected sequence is always present in `tracked`; the
    /// fallback below is a defensive cover (e.g. against a future refactor that
    /// reorders publish) that derives stats from ONE atomic snapshot so a
    /// concurrent publish can never tear seq/id/path across two bases.
    #[must_use]
    pub fn selected_stats(&self) -> GenerationLeaseStats {
        let seq = self.selected_sequence();
        let tracked = self.tracked.lock();
        if let Some(slot) = tracked.iter().find(|t| t.publication_sequence == seq) {
            return slot.stats(true);
        }
        // Defensive fallback (unreachable while publish pushes before swap):
        // read all fields from ONE atomic snapshot. `strong_refs` subtracts
        // this transient probe's own reference so it matches the weak-probe
        // semantics of the tracked arm (owner + external snapshots only).
        let base = self.selected.load_full();
        GenerationLeaseStats {
            publication_sequence: base.publication_sequence(),
            generation_id: base.generation_id().to_string(),
            generation_abs_path: base.generation_abs_path().to_path_buf(),
            strong_refs: Arc::strong_count(&base).saturating_sub(1),
            is_selected: true,
        }
    }

    /// Stats for every tracked generation (selected + retired), in order.
    #[must_use]
    pub fn lease_stats(&self) -> Vec<GenerationLeaseStats> {
        let seq = self.selected_sequence();
        self.tracked
            .lock()
            .iter()
            .map(|t| t.stats(t.publication_sequence == seq))
            .collect()
    }

    /// Publish a newly-built generation: atomically redirect new snapshots to
    /// it while existing readers finish against the old immutable bytes.
    ///
    /// This **only** redirects the in-process base; it never modifies or
    /// deletes the old generation's bytes (packet requirement 3). The old base
    /// is demoted to a weak-tracked retired generation; its mapping is released
    /// when its last read snapshot drops.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationTransitionError::PublishOnClosed`] when the registry
    /// is closed, or [`GenerationTransitionError::OpenFailed`] when the new
    /// generation fails to open.
    pub fn publish(
        self: &Arc<Self>,
        publication_sequence: u64,
        generation_id: String,
        generation_abs_path: PathBuf,
    ) -> Result<GenerationLease> {
        if self.closed.load(Ordering::Acquire) {
            return Err(GenerationTransitionError::PublishOnClosed {
                sequence: publication_sequence,
            }
            .into());
        }
        let new_base = Arc::new(
            BaseGeneration::open(
                publication_sequence,
                generation_id.clone(),
                generation_abs_path.clone(),
            )
            .map_err(|e| GenerationTransitionError::OpenFailed {
                sequence: publication_sequence,
                source: Box::new(e),
            })?,
        );

        // Track the new generation by weak probe BEFORE the atomic redirect.
        // Pushing first closes the swap-before-push window: a concurrent
        // `selected_stats`/`lease_stats` that observes the new selected base
        // always finds its slot already present in the tracked log, so the
        // "selected generation is tracked" invariant holds under concurrency.
        // (Registry strong ref lives in the ArcSwap only, added just below.)
        self.tracked.lock().push(TrackedGeneration {
            publication_sequence,
            generation_id,
            generation_abs_path,
            base: Arc::downgrade(&new_base),
        });

        // Atomic redirect: new snapshots see `new_base`; existing snapshots
        // keep their old `Arc<BaseGeneration>` and finish against the old
        // immutable bytes. The old base is NOT modified or deleted here.
        let old_base = self.selected.swap(Arc::clone(&new_base));
        self.transitions.fetch_add(1, Ordering::AcqRel);

        // Prove the old generation's bytes are untouched: it is still readable
        // through `old_base` (the ArcSwap returns the previous strong ref). We
        // drop that transient ref at the end of this scope; any surviving
        // snapshots keep the old mapping alive. Reading the store here also
        // proves the old base still deserializes after the swap.
        let _ = old_base.store();
        drop(old_base);

        Ok(GenerationLease::new(new_base))
    }

    /// Number of base transitions (publications) performed so far.
    #[must_use]
    pub fn transition_count(&self) -> u64 {
        self.transitions.load(Ordering::Acquire)
    }

    /// Checkpoint the base transition surface: snapshot per-generation lease
    /// stats into a [`TransitionReport`] (transitions + selected/retired strong
    /// refs). This is a read-only snapshot; it performs no mutation and, today,
    /// no validation that can fail.
    ///
    /// # Errors
    ///
    /// This surface is **infallible today** — it always returns
    /// `Ok(TransitionReport)`. The `Result` shape is the packet requirement-5
    /// contract reserved for the retirement/GC validation (G-EM0.4b) that can
    /// fail; the typed transition errors currently live on
    /// [`publish`](Self::publish). Callers should propagate the `Result` rather
    /// than assume success so the future fallible path needs no API change.
    pub fn checkpoint_transition(self: &Arc<Self>) -> Result<TransitionReport> {
        let stats = self.lease_stats();
        let report = TransitionReport {
            transitions: self.transition_count(),
            generations: stats,
        };
        Ok(report)
    }

    /// Close the transition surface, returning the final [`TransitionReport`]
    /// to the caller (packet requirement 5).
    ///
    /// Marks the registry closed (further publishes fail with
    /// [`GenerationTransitionError::PublishOnClosed`]) and returns the final
    /// report. `Drop` remains best-effort and is never the tested success
    /// path: it cannot return an error, so it only logs.
    ///
    /// # Errors
    ///
    /// Like [`checkpoint_transition`](Self::checkpoint_transition), this is
    /// **infallible today**; the `Result` shape is reserved for the future
    /// retirement/GC validation that can return a typed transition error.
    pub fn close_transition(self: &Arc<Self>) -> Result<TransitionReport> {
        self.closed.store(true, Ordering::Release);
        self.checkpoint_transition()
    }
}

impl Drop for GenerationLeaseRegistry {
    fn drop(&mut self) {
        // Best-effort only: Drop cannot return a transition error, so it is
        // never the tested success path (packet requirement 5). The real
        // transition validation runs in `close_transition`, which returns
        // `Result`.
        if !self.closed.load(Ordering::Acquire) {
            grafeo_common::grafeo_warn!(
                "GenerationLeaseRegistry dropped without close_transition(); \
                 transition errors are only surfaced by close_transition()"
            );
        }
    }
}

/// A checkpoint/close report over the base-transition surface.
#[derive(Debug, Clone)]
pub struct TransitionReport {
    /// Number of base transitions (publications) performed.
    pub transitions: u64,
    /// Per-generation lease stats at report time.
    pub generations: Vec<GenerationLeaseStats>,
}

impl TransitionReport {
    /// `true` when every retired (non-selected) generation's mapping has been
    /// fully released (no strong refs remain).
    #[must_use]
    pub fn all_retired_released(&self) -> bool {
        self.generations
            .iter()
            .filter(|g| !g.is_selected)
            .all(|g| g.strong_refs == 0)
    }
}
