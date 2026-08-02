//! Concurrent overlay epoch and WAL handoff (G-EM0.5c).
//!
//! # Contract
//!
//! 1. **Freeze** epoch N at exact WAL boundary B. New writes either enter
//!    bounded epoch N+1 (charged to `RetainedCategory::NextEpoch`) or block
//!    under G-EM0.5a admission.
//! 2. **Build** generation G(N) from the frozen input (materialized overlay
//!    payloads + mapped base) and **publish** it with boundary B through the
//!    G-EM0.3b path (`pre_cut_cursor`).
//! 3. **Retire** only the represented prefix of epoch N (overlay entities not
//!    re-mutated after freeze + frozen retained bytes). Epoch N+1 remains
//!    applied exactly once.
//!
//! # Linearization points
//!
//! | Phase | Linearization |
//! |-------|---------------|
//! | `FreezeCaptured` | WAL cut B durable; freeze id/payload snapshot taken; next epoch open |
//! | `Building` | G(N) streams only frozen input; N+1 writers concurrent |
//! | `Published` | Manifest sync (3b commit); selected generation = G(N) |
//! | `EpochRetired` | Frozen overlay prefix drained; WAL truncated at B (post-commit) |
//! | `Cancelled` / `Failed` | No WAL advance, no overlay retire, freeze slot cleared |
//!
//! Checkpoint/close while handoff is active return a typed error rather than
//! racing freeze/publication. Drop is best-effort cancel only.
//!
//! # Fault injection
//!
//! Under `debug_assertions`, `GRAFEO_5C_ABORT` may be set to one of:
//! `after_freeze`, `after_build`, `after_publication`, `after_retire` — the
//! process aborts at that boundary so a fresh-process parent can prove recovery.

mod handoff;
mod records;
mod types;

pub use types::{
    EpochHandoffCoordinator, EpochHandoffPhase, EpochHandoffReport, FrozenEpochHandle,
};
