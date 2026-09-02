//! Engine-side manifest publication state surface (G-EM0.3b).
//!
//! This module is a thin, read-only typed view over the W0-owned dual-slot
//! manifest ([`grafeo_storage::generation::manifest`]). It does **not**
//! re-encode, decode, or re-order the manifest — it translates the accepted
//! W0 schema into the engine's publication-facing types so callers can observe
//! the selected/previous generation, publication sequence, durable WAL
//! boundary, and overlay epoch without touching storage internals.
//!
//! The manifest schema, slot layout, and selection rule (highest valid
//! publication sequence) are owned by W0 (`grafeo-storage`). This module only
//! re-exposes them. Any change to the on-disk schema belongs in W0, not here.

use std::path::Path;

use grafeo_storage::generation::manifest::{self, ManifestSlot};
use grafeo_storage::generation::wal_cursor::WalReplayCursor;

/// The precise durable WAL boundary recorded in a manifest slot.
///
/// This is the replay cursor the generation represents: after the manifest
/// slot is durable (the commit point), WAL files strictly older than this
/// boundary may be truncated. Before the commit point the boundary must not
/// be used to advance/truncate the WAL or reset overlay state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalBoundary {
    /// WAL log file sequence of the replay cursor.
    pub log_sequence: u64,
    /// Byte offset of the first valid frame in that file.
    pub byte_offset: u64,
    /// Overlay epoch captured at the boundary (0 when none).
    pub overlay_epoch: u64,
    /// Last committed transaction ID at the boundary (0 when none).
    pub transaction_id: u64,
}

impl WalBoundary {
    /// Translate the W0 storage replay cursor into the engine boundary type.
    #[must_use]
    pub fn from_cursor(cursor: &WalReplayCursor) -> Self {
        Self {
            log_sequence: cursor.log_sequence,
            byte_offset: cursor.byte_offset,
            overlay_epoch: cursor.epoch,
            transaction_id: cursor.transaction_id,
        }
    }

    /// View this boundary back as the W0 storage cursor (for validation).
    #[must_use]
    pub fn to_cursor(self) -> WalReplayCursor {
        WalReplayCursor {
            log_sequence: self.log_sequence,
            byte_offset: self.byte_offset,
            epoch: self.overlay_epoch,
            transaction_id: self.transaction_id,
        }
    }
}

/// A validated generation selection read from the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSelection {
    /// Manifest slot index that was selected (0 or 1).
    pub slot_index: usize,
    /// Monotonic publication sequence of the selected generation.
    pub publication_sequence: u64,
    /// Publication sequence of the parent/previous generation (0 = genesis).
    pub parent_publication_sequence: u64,
    /// Generation identifier recorded in the slot.
    pub generation_id: String,
    /// Parent generation identifier (empty for genesis).
    pub parent_generation_id: String,
    /// Root-relative generation path (`generations/g-…grafeo`).
    pub generation_path: String,
    /// The precise durable WAL boundary this generation represents.
    pub wal_boundary: WalBoundary,
    /// Overlay epoch represented by the selected generation.
    pub overlay_epoch: u64,
}

impl ManifestSelection {
    /// Build a selection view from a validated W0 slot + its index.
    #[must_use]
    pub fn from_slot(slot_index: usize, slot: &ManifestSlot) -> Self {
        let cursor = WalReplayCursor {
            log_sequence: slot.wal_log_sequence,
            byte_offset: slot.wal_byte_offset,
            epoch: slot.overlay_epoch,
            transaction_id: slot.transaction_id,
        };
        Self {
            slot_index,
            publication_sequence: slot.publication_sequence,
            parent_publication_sequence: slot.parent_publication_sequence,
            generation_id: slot.generation_id.clone(),
            parent_generation_id: slot.parent_generation_id.clone(),
            generation_path: slot.generation_path.clone(),
            wal_boundary: WalBoundary::from_cursor(&cursor),
            overlay_epoch: slot.overlay_epoch,
        }
    }
}

/// Read-only view over both manifest slots: the selected generation plus the
/// retained previous generation (when one exists and is still valid).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestState {
    /// The selected (highest valid sequence) generation.
    pub selected: ManifestSelection,
    /// The retained previous generation, if a second valid slot exists.
    pub previous: Option<ManifestSelection>,
}

/// Errors reading manifest publication state.
#[derive(Debug, thiserror::Error)]
pub enum ManifestStateError {
    /// No valid generation slot exists yet (fresh / genesis root).
    #[error("no valid generation slot (genesis root)")]
    Genesis,
    /// Manifest decode/validation failed.
    #[error("manifest: {0}")]
    Manifest(String),
    /// Underlying I/O error.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// Read the full manifest publication state from a generation root.
///
/// Returns the selected generation plus the retained previous generation when
/// both slots decode. Selection follows the W0 rule: highest valid
/// publication sequence. A genesis (all-zero) root yields
/// [`ManifestStateError::Genesis`].
///
/// # Errors
///
/// Returns [`ManifestStateError::Genesis`] when no slot is valid,
/// [`ManifestStateError::Manifest`] when slots are corrupt, and
/// [`ManifestStateError::Io`] for read failures.
pub fn read_manifest_state(root: &Path) -> Result<ManifestState, ManifestStateError> {
    let manifest_path = root.join("manifest.bin");
    let bytes = std::fs::read(&manifest_path)?;
    if bytes.iter().all(|b| *b == 0) {
        return Err(ManifestStateError::Genesis);
    }
    let [slot0, slot1] =
        manifest::read_both_slots(&manifest_path).map_err(ManifestStateError::Io)?;
    // Selection rule mirrors W0 `read_manifest` / `inactive_slot_index`
    // (grafeo-storage/src/generation/manifest.rs: highest valid publication
    // sequence wins; ties keep the lower slot index). If W0 changes that rule,
    // this view must change with it — the engine does not define its own rule.
    match (slot0, slot1) {
        (Ok(a), Ok(b)) => {
            let (sel_index, sel, prev_index, prev) =
                if b.publication_sequence > a.publication_sequence {
                    (1, b, 0, a)
                } else {
                    (0, a, 1, b)
                };
            Ok(ManifestState {
                selected: ManifestSelection::from_slot(sel_index, &sel),
                previous: Some(ManifestSelection::from_slot(prev_index, &prev)),
            })
        }
        (Ok(a), Err(_)) => Ok(ManifestState {
            selected: ManifestSelection::from_slot(0, &a),
            previous: None,
        }),
        (Err(_), Ok(b)) => Ok(ManifestState {
            selected: ManifestSelection::from_slot(1, &b),
            previous: None,
        }),
        (Err(c0), Err(c1)) => Err(ManifestStateError::Manifest(format!(
            "both slots invalid: slot0={c0}, slot1={c1}"
        ))),
    }
}
