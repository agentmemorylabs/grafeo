//! Engine-side manifest publication and WAL boundary surface (G-EM0.3b).
//!
//! This module groups the G-EM0.3b publication contract:
//!
//! - [`manifest`]: a read-only typed view over the W0 dual-slot manifest,
//!   exposing the selected/previous generation, publication sequence, durable
//!   WAL boundary, and overlay epoch.
//! - [`publication`]: the ordered publication phases, phase-tagged errors,
//!   and the extended [`publication::PublishedGeneration`] descriptor that
//!   retains the WAL boundary and overlay epoch a published generation
//!   represents.
//!
//! Neither module re-implements the W0 manifest schema, the 11-step
//! publication ordering, or WAL-cursor mechanics — those stay in
//! `grafeo-storage` (W0). This module only re-exposes them at the engine
//! boundary and adds the observability G-EM0.3a did not provide.

pub mod manifest;
pub mod publication;

pub use manifest::{
    ManifestSelection, ManifestState, ManifestStateError, WalBoundary, read_manifest_state,
};
pub use publication::{PublicationPhase, PublicationPhaseError, PublishedGeneration};
