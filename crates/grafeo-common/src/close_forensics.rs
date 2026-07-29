//! Diagnostics-only close/lifecycle forensics (feature `close-forensics`).
//!
//! # Non-owning invariant
//! Never store `Arc` to databases, indexes, buffer managers, worker groups, or
//! closures capturing those. Records are plain integers only.
//!
//! # Drop safety
//! [`emit`] never panics, never blocks on a contended lock (`try_lock` only),
//! never walks memory graphs, and never performs file I/O. A full or busy sink
//! drops the event and increments a counter.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

/// Component type codes (stable for forensics reports).
pub mod component_type {
    pub const GRAFEO_DB: u8 = 1;
    pub const BUFFER_MANAGER: u8 = 2;
    pub const HNSW_INDEX: u8 = 3;
    pub const CHECKPOINT_TIMER: u8 = 4;
    pub const QUANTIZED_HNSW: u8 = 5;
}

/// Event type codes.
pub mod event_type {
    pub const DROP: u8 = 1;
    pub const WORKER_STARTED: u8 = 2;
    pub const WORKER_SHUTDOWN_REQUESTED: u8 = 3;
    pub const WORKER_JOINED: u8 = 4;
    pub const INSTANCE_BOUND: u8 = 5;
    /// Drop body entered; free work follows inside timed phases.
    pub const DROP_ENTER: u8 = 6;
    /// Sub-phase inside Drop (`worker_scope` carries [`drop_phase`] code).
    pub const DROP_PHASE: u8 = 7;
}

/// Phase tags for [`event_type::DROP_PHASE`] (stored in `worker_scope`).
pub mod drop_phase {
    pub const TOPOLOGY: u8 = 1;
    pub const ENTRY_POINT: u8 = 2;
    pub const MAX_LEVEL: u8 = 3;
    pub const RNG: u8 = 4;
    pub const CONFIG: u8 = 5;
    pub const CONSUMERS: u8 = 6;
    pub const FORCE_RAM: u8 = 7;
    pub const CLOSE_FN: u8 = 8;
    pub const FIELDS: u8 = 9;
}

/// Worker scope codes.
pub mod worker_scope {
    pub const DATABASE_INSTANCE: u8 = 1;
    pub const PROCESS_GLOBAL: u8 = 2;
    pub const SHARED_ENGINE: u8 = 3;
    pub const UNKNOWN: u8 = 4;
}

/// db_ref_kind: reported metadata only (no Arc retained).
pub mod db_ref_kind {
    pub const NONE: u8 = 0;
    pub const STRONG: u8 = 1;
    pub const WEAK: u8 = 2;
    pub const UNKNOWN: u8 = 3;
}

/// Fixed-size lifecycle record.
#[derive(Debug, Clone, Copy, Default)]
pub struct TinyLifecycleEvent {
    pub event_sequence: u64,
    pub monotonic_elapsed_ns: u64,
    pub instance_id: u64,
    pub component_id: u64,
    pub component_type: u8,
    pub event_type: u8,
    pub worker_scope: u8,
    pub db_ref_kind: u8,
    pub thread_id_hash: u64,
}

const RING_CAP: usize = 4096;

struct RingState {
    slots: Vec<TinyLifecycleEvent>,
    write: usize,
}

static SEQ: AtomicU64 = AtomicU64::new(1);
static COMPONENT_IDS: AtomicU64 = AtomicU64::new(1);
static ACTIVE_INSTANCE_ID: AtomicU64 = AtomicU64::new(0);
static WRITTEN: AtomicU64 = AtomicU64::new(0);
static DROPPED: AtomicU64 = AtomicU64::new(0);
static START: OnceLock<Instant> = OnceLock::new();
static RING: OnceLock<Mutex<RingState>> = OnceLock::new();

fn ring() -> &'static Mutex<RingState> {
    RING.get_or_init(|| {
        Mutex::new(RingState {
            slots: vec![TinyLifecycleEvent::default(); RING_CAP],
            write: 0,
        })
    })
}

fn elapsed_ns() -> u64 {
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

thread_local! {
    static THREAD_DIAG_ID: Cell<u64> = const { Cell::new(0) };
}
static THREAD_DIAG_IDS: AtomicU64 = AtomicU64::new(1);

fn thread_id_hash() -> u64 {
    THREAD_DIAG_ID.with(|cell| {
        let mut id = cell.get();
        if id == 0 {
            id = THREAD_DIAG_IDS.fetch_add(1, Ordering::Relaxed);
            cell.set(id);
        }
        id
    })
}

/// Pre-initialize the ring (call from probe before open so first Drop never allocates the Vec).
pub fn init_sink() {
    let _ = START.get_or_init(Instant::now);
    let _ = ring();
}

/// Bind the process-local active instance id for subsequent component Drop.
pub fn set_active_instance_id(instance_id: u64) {
    ACTIVE_INSTANCE_ID.store(instance_id, Ordering::Release);
}

pub fn active_instance_id() -> u64 {
    ACTIVE_INSTANCE_ID.load(Ordering::Acquire)
}

/// Allocate a new component id (monotonic).
pub fn next_component_id() -> u64 {
    COMPONENT_IDS.fetch_add(1, Ordering::Relaxed)
}

/// Emit a tiny lifecycle event. Never panics. Never holds diagnosed Arcs.
pub fn emit(
    instance_id: u64,
    component_id: u64,
    component_type: u8,
    event_type: u8,
    worker_scope: u8,
    db_ref_kind: u8,
) {
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let event = TinyLifecycleEvent {
        event_sequence: seq,
        monotonic_elapsed_ns: elapsed_ns(),
        instance_id,
        component_id,
        component_type,
        event_type,
        worker_scope,
        db_ref_kind,
        thread_id_hash: thread_id_hash(),
    };
    let Ok(mut guard) = ring().try_lock() else {
        DROPPED.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let idx = guard.write % RING_CAP;
    guard.slots[idx] = event;
    guard.write = guard.write.wrapping_add(1);
    WRITTEN.fetch_add(1, Ordering::Relaxed);
}

/// Copy up to `out.len()` most recent events (best-effort).
pub fn drain_snapshot(out: &mut [TinyLifecycleEvent]) -> usize {
    let Ok(guard) = ring().try_lock() else {
        return 0;
    };
    let written = WRITTEN.load(Ordering::Relaxed) as usize;
    if written == 0 || out.is_empty() {
        return 0;
    }
    let n = out.len().min(RING_CAP).min(written);
    let start = guard.write.wrapping_sub(n);
    for (i, slot) in out.iter_mut().enumerate().take(n) {
        let idx = start.wrapping_add(i) % RING_CAP;
        *slot = guard.slots[idx];
    }
    n
}

pub fn stats() -> (u64, u64) {
    (WRITTEN.load(Ordering::Relaxed), DROPPED.load(Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emit_and_drain_roundtrip() {
        let id = next_component_id();
        set_active_instance_id(42);
        emit(
            42,
            id,
            component_type::GRAFEO_DB,
            event_type::DROP,
            worker_scope::DATABASE_INSTANCE,
            db_ref_kind::NONE,
        );
        let mut buf = [TinyLifecycleEvent::default(); 16];
        let n = drain_snapshot(&mut buf);
        assert!(n >= 1);
        assert!(buf[..n]
            .iter()
            .any(|e| e.instance_id == 42 && e.component_id == id));
    }

    #[test]
    fn emit_never_panics_on_flood() {
        set_active_instance_id(7);
        for _ in 0..RING_CAP * 2 {
            emit(
                7,
                next_component_id(),
                component_type::BUFFER_MANAGER,
                event_type::DROP,
                0,
                db_ref_kind::NONE,
            );
        }
    }
}
