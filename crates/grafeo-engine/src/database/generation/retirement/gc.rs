//! Live-root generation garbage collection (G-EM0.4b, packet requirement 3).
//!
//! Live-root GC may delete a generation **only** when it is not selected,
//! previous-recovery retained, backup-pinned, in-process leased, or
//! referenced by a valid manifest/WAL recovery state. It never tracks
//! external readers, because external readers never receive live-root
//! generation paths (packet requirement 2: they get independent immutable
//! snapshots outside the live root).
//!
//! Safety rails:
//!
//! - **Fresh slot authority at both phases.** Planning re-reads the manifest
//!   and collection re-reads it again immediately before deleting; if the
//!   selection moved between plan and collect, collection fails closed with
//!   [`RetirementError::SelectionChanged`] and deletes nothing.
//! - **Precedence.** Selected > previous-recovery retained > backup-pinned >
//!   in-process leased > eligible. A generation protected by any class is
//!   never collected.
//! - **Windows mapped-file rail by construction.** An eligible generation
//!   has no in-process lease (no mapping) and external readers never hold
//!   live-root paths, so no live mapping of an eligible file exists on any
//!   platform; `ERROR_USER_MAPPED_FILE` is structurally unreachable. A
//!   delete that still fails (operator interference, antivirus) surfaces as
//!   a typed I/O error with earlier deletions reported, never as a silent
//!   partial success.

use std::path::Path;

use grafeo_storage::file::generation_writer::{GenerationFileOps, OsGenerationFileOps};

use super::super::manifest::{ManifestState, ManifestStateError, read_manifest_state};
use super::{
    BackupPin, ClassifiedGeneration, RetentionClass, RetirementAuthority, RetirementError,
};

/// A retirement plan: the artifacts eligible for deletion and the artifacts
/// examined but protected, each with its reason (packet requirements 3 + 5).
#[derive(Debug, Clone)]
pub struct RetirementPlan {
    /// Artifacts eligible for deletion right now.
    pub eligible: Vec<ClassifiedGeneration>,
    /// Artifacts examined but protected (selected, previous, pinned,
    /// leased).
    pub protected: Vec<ClassifiedGeneration>,
    /// The manifest-selected path at plan time (`None` on a genesis root).
    pub planned_selected: Option<String>,
}

/// Compute a retirement plan for an owned root.
///
/// The manifest is read fresh (never a caller-cached selection), pins come
/// from the authority's registry, and in-process leases come from the lease
/// registry's weak-probe statistics. A genesis (all-zero) manifest yields a
/// plan with no slot protection; a corrupt manifest fails closed.
///
/// # Errors
///
/// Returns [`RetirementError::ManifestState`] when the manifest is corrupt
/// (never plan a deletion over an unknown selection state), and
/// [`RetirementError::Io`] when the root cannot be listed.
pub fn plan_retirement(auth: &RetirementAuthority) -> Result<RetirementPlan, RetirementError> {
    let root = auth.root();
    let state = read_selection(root)?;
    let pins = auth.active_pins();
    let leased = auth.leased_generations();
    classify_root(root, &state, &pins, &leased)
}

/// Execute a retirement plan, deleting only the eligible artifacts.
///
/// The manifest state, pins, and leases are re-evaluated **immediately
/// before any deletion**; if any planned-eligible artifact has become
/// protected (a publication changed the slots, a backup pinned it, a lease
/// appeared), the collection fails closed and deletes nothing.
///
/// After all deletions, the generations directory and the root directory
/// are fsynced and the retired entries (with reasons) are recorded in the
/// authority's retired log for observability.
///
/// # Errors
///
/// Returns [`RetirementError::SelectionChanged`] when the plan is stale
/// (nothing deleted), [`RetirementError::ManifestState`] when the manifest
/// re-read fails (nothing deleted), and [`RetirementError::Io`] when a
/// deletion fails (earlier deletions are already durable and reported via
/// the retired log).
pub fn collect_retirement(
    auth: &RetirementAuthority,
    plan: &RetirementPlan,
) -> Result<Vec<ClassifiedGeneration>, RetirementError> {
    let root = auth.root();

    // Re-evaluate protection with completely fresh state (TOCTOU rail).
    let fresh = plan_retirement(auth)?;
    let still_eligible: Vec<&str> = fresh.eligible.iter().map(|c| c.path.as_str()).collect();
    for entry in &plan.eligible {
        if !still_eligible.contains(&entry.path.as_str()) {
            return Err(RetirementError::SelectionChanged(format!(
                "{} is no longer eligible; collection aborted, nothing deleted",
                entry.path
            )));
        }
    }

    let ops = OsGenerationFileOps;
    let mut retired = Vec::with_capacity(plan.eligible.len());
    for entry in &plan.eligible {
        let abs = root.join(&entry.path);
        if entry.path.starts_with(".unpublished-") {
            // Unpublished build artifacts may be files or directories; no
            // reader holds an FD (the build crashed pre-commit).
            if abs.is_dir() {
                std::fs::remove_dir_all(&abs)?;
            } else if abs.exists() {
                std::fs::remove_file(&abs)?;
            }
        } else if abs.exists() {
            std::fs::remove_file(&abs)?;
        }
        retired.push(ClassifiedGeneration {
            path: entry.path.clone(),
            generation_id: entry.generation_id.clone(),
            class: RetentionClass::Retired,
            reason: format!("retired: {}", entry.reason),
        });
    }

    // Durability: fsync the touched directories so the deletions survive a
    // crash (a resurrected generation would be classified unreferenced on
    // recovery, never promoted — but the fsync keeps the GC itself durable).
    if !retired.is_empty() {
        let generations_dir = root.join("generations");
        if generations_dir.is_dir() {
            ops.sync_dir(&generations_dir)?;
        }
        ops.sync_dir(root)?;
    }

    auth.record_retired(retired.clone());
    Ok(retired)
}

/// Read the current slot selection, treating a genesis (all-zero) manifest
/// as "no selection" rather than an error.
fn read_selection(root: &Path) -> Result<Option<ManifestState>, RetirementError> {
    match read_manifest_state(root) {
        Ok(state) => Ok(Some(state)),
        Err(ManifestStateError::Genesis) => Ok(None),
        Err(e) => Err(RetirementError::ManifestState(e)),
    }
}

/// Classify every artifact under the root against the selection, pins, and
/// leases. Strict precedence: selected > previous > pinned > leased >
/// eligible.
fn classify_root(
    root: &Path,
    state: &Option<ManifestState>,
    pins: &[BackupPin],
    leased: &[(String, u64, usize)],
) -> Result<RetirementPlan, RetirementError> {
    let selected = state.as_ref().map(|s| {
        (
            s.selected.generation_path.clone(),
            s.selected.generation_id.clone(),
        )
    });
    let previous = state
        .as_ref()
        .and_then(|s| s.previous.as_ref())
        .map(|p| (p.generation_path.clone(), p.generation_id.clone()));

    let mut eligible = Vec::new();
    let mut protected = Vec::new();

    // Unpublished build artifacts at the root level (pre-commit crash
    // leftovers). Never slot-referenced, always collectable.
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(".unpublished-") {
            eligible.push(ClassifiedGeneration {
                path: name,
                generation_id: None,
                class: RetentionClass::Eligible,
                reason: "unpublished build artifact from a pre-commit crash; \
                         referenced by no manifest slot"
                    .to_string(),
            });
        }
    }

    let generations_dir = root.join("generations");
    if generations_dir.is_dir() {
        for entry in std::fs::read_dir(&generations_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".grafeo") {
                continue;
            }
            let rel = format!("generations/{name}");
            protected_or_eligible(
                &rel,
                &selected,
                &previous,
                pins,
                leased,
                &mut protected,
                &mut eligible,
            );
        }
    }

    Ok(RetirementPlan {
        eligible,
        protected,
        planned_selected: selected.map(|(path, _)| path),
    })
}

/// Classify one generation file under `generations/` (strict precedence).
fn protected_or_eligible(
    rel: &str,
    selected: &Option<(String, String)>,
    previous: &Option<(String, String)>,
    pins: &[BackupPin],
    leased: &[(String, u64, usize)],
    protected: &mut Vec<ClassifiedGeneration>,
    eligible: &mut Vec<ClassifiedGeneration>,
) {
    if let Some((path, id)) = selected
        && rel == path
    {
        protected.push(ClassifiedGeneration {
            path: rel.to_string(),
            generation_id: Some(id.clone()),
            class: RetentionClass::Selected,
            reason: "selected by a valid manifest slot (the live base)".to_string(),
        });
        return;
    }
    if let Some((path, id)) = previous
        && rel == path
    {
        protected.push(ClassifiedGeneration {
            path: rel.to_string(),
            generation_id: Some(id.clone()),
            class: RetentionClass::PreviousRecoveryRetained,
            reason: "referenced by the retained previous manifest slot \
                     (explicit recovery fallback)"
                .to_string(),
        });
        return;
    }
    if let Some(pin) = pins.iter().find(|p| p.generation_path == rel) {
        protected.push(ClassifiedGeneration {
            path: rel.to_string(),
            generation_id: None,
            class: RetentionClass::BackupPinned,
            reason: format!(
                "pinned by an in-flight backup of publication sequence {}",
                pin.publication_sequence
            ),
        });
        return;
    }
    if let Some((_, seq, strong)) = leased.iter().find(|(path, _, _)| path == rel) {
        protected.push(ClassifiedGeneration {
            path: rel.to_string(),
            generation_id: None,
            class: RetentionClass::InProcessLeased,
            reason: format!(
                "held by {strong} live in-process strong reference(s) \
                 (lease on publication sequence {seq})"
            ),
        });
        return;
    }
    eligible.push(ClassifiedGeneration {
        path: rel.to_string(),
        generation_id: None,
        class: RetentionClass::Eligible,
        reason: "referenced by no valid manifest slot, backup pin, or \
                 in-process lease"
            .to_string(),
    });
}
