//! Restart recovery and generation selection (G-EM0.W0-B, Module 5).
//!
//! [`recover`] reads the dual-slot manifest, validates each slot
//! independently (structure + generation file + real WAL replayability),
//! and selects the highest-sequence fully-valid slot. No mtime, filename,
//! PID, or partial decode is authority; unreferenced generation files are
//! never promoted.

use std::path::{Path, PathBuf};

use grafeo_common::storage::SectionType;

use crate::file::GrafeoFileManager;
use crate::file::generation_writer::{GenerationFileOps, OsGenerationFileOps};

use super::lock::RootLock;
use super::manifest::{self, ManifestError, ManifestSlot};
use super::wal_cursor::{WalCursorError, WalReplayCursor, validate_replayable};

/// A fully validated generation selected by recovery.
#[derive(Debug, Clone)]
pub struct SelectedGeneration {
    /// Manifest slot index (0 or 1).
    pub slot_index: usize,
    /// The validated manifest slot.
    pub slot: ManifestSlot,
    /// Absolute path of the immutable generation file.
    pub generation_abs_path: PathBuf,
    /// WAL replay cursor recorded in the slot.
    pub wal_cursor: WalReplayCursor,
}

/// Errors from recovery.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    /// No slot passed all three validation stages.
    #[error("no valid generation: {0}")]
    NoValidGeneration(String),
    /// Manifest read/decode failure.
    #[error("manifest: {0}")]
    Manifest(#[from] ManifestError),
    /// Generation file validation failure.
    #[error("generation validation: {0}")]
    GenerationValidation(String),
    /// WAL replayability failure.
    #[error("WAL replayability: {0}")]
    WalReplay(#[from] WalCursorError),
    /// Underlying I/O error.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// Run full recovery: read the manifest, validate slots (structure +
/// generation file + WAL replayability), and select the highest-sequence
/// fully-valid slot.
///
/// Selection rules:
///
/// - Try the highest-sequence slot first.
/// - If it fails generation or WAL validation, try the other slot.
/// - If neither validates, return [`RecoveryError::NoValidGeneration`] with
///   BOTH causes.
/// - Never select by mtime, filename, PID, or partial decode.
/// - Never auto-promote an unreferenced generation file.
///
/// # Errors
///
/// Returns [`RecoveryError`] for every failure branch.
pub fn recover(lock: &RootLock) -> std::result::Result<SelectedGeneration, RecoveryError> {
    let root = lock.canonical_root();
    let manifest_path = root.join("manifest.bin");
    let wal_dir = root.join("wal");

    let [slot0, slot1] = manifest::read_both_slots(&manifest_path)?;

    // Order candidates by descending publication sequence.
    let mut candidates: Vec<(usize, ManifestSlot)> = Vec::new();
    let mut causes: Vec<(usize, String)> = Vec::new();
    for (index, slot) in [(0usize, slot0), (1, slot1)] {
        match slot {
            Ok(s) => candidates.push((index, s)),
            Err(cause) => causes.push((index, format!("manifest slot {index}: {cause}"))),
        }
    }
    candidates.sort_by_key(|(_, slot)| std::cmp::Reverse(slot.publication_sequence));

    let mut candidate_causes: Vec<String> = Vec::new();
    for (index, slot) in candidates {
        let gen_cause = match validate_generation_file(root, &slot) {
            Ok(()) => None,
            Err(cause) => Some(format!("slot {index} generation: {cause}")),
        };
        let wal_cause = match validate_replayable(&wal_dir, &cursor_from_slot(&slot)) {
            Ok(()) => None,
            Err(cause) => Some(format!("slot {index} WAL replay: {cause}")),
        };
        match (gen_cause, wal_cause) {
            (None, None) => {
                let generation_abs_path = root.join(&slot.generation_path);
                let wal_cursor = cursor_from_slot(&slot);
                return Ok(SelectedGeneration {
                    slot_index: index,
                    slot,
                    generation_abs_path,
                    wal_cursor,
                });
            }
            (g, w) => {
                candidate_causes.extend(g);
                candidate_causes.extend(w);
            }
        }
    }

    let mut all_causes = Vec::new();
    for (index, cause) in causes {
        all_causes.push(format!("slot {index}: {cause}"));
    }
    all_causes.extend(candidate_causes);
    Err(RecoveryError::NoValidGeneration(all_causes.join("; ")))
}

/// Build the replay cursor recorded in a manifest slot.
#[must_use]
pub fn cursor_from_slot(slot: &ManifestSlot) -> WalReplayCursor {
    WalReplayCursor {
        log_sequence: slot.wal_log_sequence,
        byte_offset: slot.wal_byte_offset,
        epoch: slot.overlay_epoch,
        transaction_id: slot.transaction_id,
    }
}

/// Validate a manifest slot's generation file: path safety, existence,
/// length, SHA-256, and production `open_read_only` + CompactStore section.
/// Shared by recovery and snapshot publication.
///
/// # Errors
///
/// Returns a human-readable cause string.
pub(crate) fn validate_generation_file(
    root: &Path,
    slot: &ManifestSlot,
) -> std::result::Result<(), String> {
    let abs = root.join(&slot.generation_path);
    if !abs.starts_with(root) {
        return Err(format!(
            "generation path escapes root: {}",
            slot.generation_path
        ));
    }

    let ops = OsGenerationFileOps;
    if !ops.path_exists(&abs) {
        return Err(format!("generation file missing: {}", abs.display()));
    }
    let len = ops
        .file_len(&abs)
        .map_err(|e| format!("stat {}: {e}", abs.display()))?;
    if len != slot.generation_length {
        return Err(format!(
            "length mismatch: stored={}, actual={len}",
            slot.generation_length
        ));
    }
    let sha = ops
        .sha256(&abs)
        .map_err(|e| format!("sha256 {}: {e}", abs.display()))?;
    if sha != slot.generation_sha256 {
        return Err("SHA-256 mismatch".to_string());
    }

    let manager = GrafeoFileManager::open_read_only(&abs)
        .map_err(|e| format!("open_read_only {}: {e}", abs.display()))?;
    let dir = manager
        .read_section_directory()
        .map_err(|e| format!("section directory: {e}"))?
        .ok_or_else(|| "no section directory".to_string())?;
    let entry = dir
        .find(SectionType::CompactStore)
        .ok_or_else(|| "CompactStore section missing".to_string())?;
    if entry.length == 0 {
        return Err("CompactStore section has zero length".to_string());
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/recovery_tests.rs"]
mod tests;
