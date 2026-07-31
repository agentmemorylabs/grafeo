//! External immutable snapshot publication (G-EM0.W0-B, Module 6).
//!
//! [`publish_snapshot`] copies a [`SelectedGeneration`] (the capability
//! token returned by locked recovery) to a destination OUTSIDE the live
//! root, using bounded streaming copy, sync, fresh-reopen validation, and
//! atomic rename. Hardlinks are never used: the snapshot must have an
//! independent lifetime from live-root GC.

use std::path::{Path, PathBuf};

use grafeo_common::storage::SectionType;

use crate::file::GrafeoFileManager;
use crate::file::generation_writer::GenerationFileOps;

use super::lock::RootLock;
use super::publication::correlation_id;
use super::recovery::{SelectedGeneration, validate_generation_file};

/// Provenance recorded in a published snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotProvenance {
    /// Generation ID of the source slot.
    pub source_generation_id: String,
    /// Publication sequence of the source slot.
    pub source_publication_sequence: u64,
    /// SHA-256 of the source generation file.
    pub source_sha256: [u8; 32],
    /// CompactStore format version of the source slot.
    pub format_version: u16,
}

/// Errors from snapshot publication.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// The destination directory is inside the live root.
    #[error("destination inside live root")]
    DestinationInsideRoot,
    /// The snapshot name is not a single safe path component.
    #[error("destination path is not a single safe component")]
    UnsafeName,
    /// Source generation revalidation failed.
    #[error("source generation revalidation failed: {0}")]
    SourceInvalid(String),
    /// Underlying I/O error.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// Copy buffer size for snapshot streaming (bounded memory).
const COPY_BUFFER_BYTES: usize = 4 * 1024 * 1024;

/// Publish an immutable snapshot of the selected generation to a
/// destination directory OUTSIDE the live root.
///
/// Steps:
/// 1. Revalidate the source (path, length, hash, `open_read_only`).
/// 2. Canonicalize the destination parent and prove it is outside the live
///    root.
/// 3. Validate the snapshot name is one safe path component.
/// 4. Bounded streaming copy to a unique temp path in the destination dir.
/// 5. Sync the temp file.
/// 6. Fresh-reopen validate the temp file.
/// 7. Atomic rename to the final snapshot name.
/// 8. Fsync the destination parent directory.
///
/// # Errors
///
/// Returns [`SnapshotError`] for every failure branch.
pub fn publish_snapshot(
    lock: &RootLock,
    selected: &SelectedGeneration,
    destination_dir: &Path,
    snapshot_name: &str,
    file_ops: &dyn GenerationFileOps,
    #[cfg(test)] fault_hook: Option<&dyn Fn(&str)>,
) -> std::result::Result<SnapshotProvenance, SnapshotError> {
    #[cfg(test)]
    let hook = |name: &str| {
        if let Some(h) = fault_hook {
            h(name);
        }
    };
    #[cfg(not(test))]
    let hook = |_name: &str| {};

    // Step 1: revalidate the source capability.
    let root = lock.canonical_root();
    validate_generation_file(root, &selected.slot).map_err(SnapshotError::SourceInvalid)?;

    // Step 2: canonicalize destination, prove outside the live root.
    let canonical_dest = std::fs::canonicalize(destination_dir)?;
    if canonical_dest.starts_with(root) {
        return Err(SnapshotError::DestinationInsideRoot);
    }

    // Step 3: snapshot name must be one safe component.
    if !is_safe_component(snapshot_name) {
        return Err(SnapshotError::UnsafeName);
    }

    // Step 4-5: bounded streaming copy to a unique temp path, then sync.
    let temp_name = format!(".tmp-{}-{snapshot_name}", correlation_id());
    let temp_path: PathBuf = canonical_dest.join(&temp_name);
    hook("before_snapshot_copy");
    let copied =
        file_ops.copy_bounded(&selected.generation_abs_path, &temp_path, COPY_BUFFER_BYTES)?;
    if copied != selected.slot.generation_length {
        // Fail closed: the source changed under us.
        let _ = std::fs::remove_file(&temp_path);
        return Err(SnapshotError::SourceInvalid(format!(
            "copied {copied} bytes, expected {}",
            selected.slot.generation_length
        )));
    }
    file_ops.sync_path(&temp_path)?;
    hook("after_snapshot_sync");

    // Step 6: fresh-reopen validate the temp file.
    validate_container(&temp_path)
        .map_err(|e| SnapshotError::SourceInvalid(format!("snapshot reopen: {e}")))?;

    // Step 7-8: atomic rename + destination parent fsync.
    let final_path = canonical_dest.join(snapshot_name);
    file_ops.rename(&temp_path, &final_path)?;
    hook("after_snapshot_rename");
    file_ops.sync_dir(&canonical_dest)?;

    Ok(SnapshotProvenance {
        source_generation_id: selected.slot.generation_id.clone(),
        source_publication_sequence: selected.slot.publication_sequence,
        source_sha256: selected.slot.generation_sha256,
        format_version: selected.slot.compact_store_format_version,
    })
}

/// Map a common storage error into a snapshot error. I/O errors keep their
/// [`SnapshotError::Io`] identity; everything else is a source-invalid
/// failure (the closest variant in the locked error surface).
impl From<grafeo_common::utils::error::Error> for SnapshotError {
    fn from(e: grafeo_common::utils::error::Error) -> Self {
        match e {
            grafeo_common::utils::error::Error::Io(io) => SnapshotError::Io(io),
            other => SnapshotError::SourceInvalid(other.to_string()),
        }
    }
}

/// True when `name` is a single safe path component: non-empty, no path
/// separators, no `.`/`..`, no NUL.
fn is_safe_component(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains('\0')
}

/// Fresh-open a container and prove the CompactStore section is present
/// with nonzero length.
fn validate_container(path: &Path) -> std::result::Result<(), String> {
    let manager =
        GrafeoFileManager::open_read_only(path).map_err(|e| format!("open_read_only: {e}"))?;
    let dir = manager
        .read_section_directory()
        .map_err(|e| format!("read_section_directory: {e}"))?
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
#[path = "tests/snapshot_tests.rs"]
mod tests;
