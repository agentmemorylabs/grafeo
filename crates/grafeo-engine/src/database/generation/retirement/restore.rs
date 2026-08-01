//! Generation-root restore (G-EM0.4b, packet requirement 4).
//!
//! Restore validates and publishes a backup into a **new exclusively locked
//! root**: it never overwrites an existing root (and therefore never
//! overwrites a mapped generation), re-validates every declared backup file
//! (length + SHA-256) before any byte is written, reconstructs the W0
//! manifest slot, and runs the full W0 triple-validation recovery before
//! handing the owned root back.

use std::path::Path;

use grafeo_storage::file::generation_writer::{GenerationFileOps, OsGenerationFileOps};
use grafeo_storage::generation::manifest::{self, ManifestSlot};
use grafeo_storage::generation::recovery::recover;

use super::super::manifest::WalBoundary;
use super::super::ownership::{OpenMode, RootOwnership};
use super::super::recovery::{OrphanClassification, RootRecovery};
use super::RetirementError;
use super::backup::{
    BACKUP_FORMAT_VERSION, BACKUP_MANIFEST_NAME, COPY_BUFFER_BYTES, GenerationBackupManifest,
    is_safe_component, validate_declared_file,
};

/// Restore a generation-root backup into a **new exclusively locked root**.
///
/// The new root must be empty (or not yet exist); restore never overwrites
/// an existing root — and therefore never overwrites a mapped generation.
/// Every declared backup file is re-validated (length + SHA-256) before any
/// byte is written, the W0 manifest slot is reconstructed, and the restored
/// root must pass the full W0 triple-validation recovery before the owned
/// root is returned.
///
/// # Errors
///
/// Returns [`RetirementError::NonEmptyRoot`] when the target root is not
/// empty, [`RetirementError::Lock`] when the new root is already owned,
/// [`RetirementError::BackupManifest`] / [`RetirementError::ValidationFailed`]
/// when the backup is unreadable or fails hash validation, and
/// [`RetirementError::Recovery`] when the restored root fails recovery.
pub fn restore_generation_root(
    backup_dir: &Path,
    new_root: &Path,
) -> Result<RootOwnership, RetirementError> {
    // Read and validate the backup manifest + every declared file.
    let manifest_path = backup_dir.join(BACKUP_MANIFEST_NAME);
    let manifest_data = std::fs::read(&manifest_path)?;
    let (record, _): (GenerationBackupManifest, usize) =
        bincode::serde::decode_from_slice(&manifest_data, bincode::config::standard())
            .map_err(|e| RetirementError::BackupManifest(format!("decode: {e}")))?;
    if record.version != BACKUP_FORMAT_VERSION {
        return Err(RetirementError::BackupManifest(format!(
            "unsupported backup version {} (max {BACKUP_FORMAT_VERSION})",
            record.version
        )));
    }

    let ops = OsGenerationFileOps;
    let generation_src = backup_dir.join(
        Path::new(&record.generation_path)
            .file_name()
            .ok_or_else(|| {
                RetirementError::BackupManifest(format!(
                    "generation path {} has no file name",
                    record.generation_path
                ))
            })?,
    );
    validate_declared_file(
        ops,
        &generation_src,
        record.generation_length,
        &record.generation_sha256,
    )?;
    for wal in &record.wal_files {
        if !is_safe_component(&wal.name) {
            return Err(RetirementError::BackupManifest(format!(
                "unsafe WAL file name {:?} in backup",
                wal.name
            )));
        }
        validate_declared_file(
            ops,
            &backup_dir.join("wal").join(&wal.name),
            wal.length,
            &wal.sha256,
        )?;
    }

    // The new root must be empty: restore never overwrites an existing root
    // (and therefore never overwrites a mapped generation).
    if new_root.exists() {
        if !new_root.is_dir() || std::fs::read_dir(new_root)?.next().is_some() {
            return Err(RetirementError::NonEmptyRoot);
        }
    } else {
        std::fs::create_dir_all(new_root)?;
    }
    let canonical_new = std::fs::canonicalize(new_root)?;
    let lock = grafeo_storage::generation::lock::RootLock::try_acquire(&canonical_new)?;

    // Publish the restored bytes under the exclusively locked new root.
    let generations_dir = canonical_new.join("generations");
    let wal_dir = canonical_new.join("wal");
    std::fs::create_dir_all(&generations_dir)?;
    std::fs::create_dir_all(&wal_dir)?;

    let generation_name = generation_src
        .file_name()
        .ok_or_else(|| {
            RetirementError::ValidationFailed("backup generation has no file name".to_string())
        })?
        .to_string_lossy()
        .into_owned();
    let restored_generation = generations_dir.join(&generation_name);
    let copied = ops.copy_bounded(&generation_src, &restored_generation, COPY_BUFFER_BYTES)?;
    if copied != record.generation_length {
        return Err(RetirementError::ValidationFailed(format!(
            "restored {copied} generation bytes, expected {}",
            record.generation_length
        )));
    }
    ops.sync_path(&restored_generation)?;
    if ops.sha256(&restored_generation)? != record.generation_sha256 {
        return Err(RetirementError::ValidationFailed(
            "restored generation hash mismatch".to_string(),
        ));
    }
    for wal in &record.wal_files {
        let dst = wal_dir.join(&wal.name);
        let len = ops.copy_bounded(
            &backup_dir.join("wal").join(&wal.name),
            &dst,
            COPY_BUFFER_BYTES,
        )?;
        if len != wal.length {
            return Err(RetirementError::ValidationFailed(format!(
                "restored {len} WAL bytes for {}, expected {}",
                wal.name, wal.length
            )));
        }
        ops.sync_path(&dst)?;
    }
    ops.sync_dir(&generations_dir)?;
    ops.sync_dir(&wal_dir)?;

    // Reconstruct the W0 manifest: slot 0 = the pinned slot, slot 1 empty.
    let restored_path = format!("generations/{generation_name}");
    let slot = ManifestSlot {
        publication_sequence: record.publication_sequence,
        parent_publication_sequence: record.parent_publication_sequence,
        overlay_epoch: record.overlay_epoch,
        transaction_id: record.transaction_id,
        wal_log_sequence: record.wal_log_sequence,
        wal_byte_offset: record.wal_byte_offset,
        generation_length: record.generation_length,
        node_count: record.node_count,
        edge_count: record.edge_count,
        outer_container_format_version: record.outer_container_format_version,
        compact_store_format_version: record.compact_store_format_version,
        generation_sha256: record.generation_sha256,
        generation_id: record.generation_id.clone(),
        parent_generation_id: record.parent_generation_id.clone(),
        generation_path: restored_path.clone(),
    };
    let manifest_out = canonical_new.join("manifest.bin");
    manifest::create_manifest(&manifest_out)?;
    {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&manifest_out)?;
        manifest::write_slot(&mut file, 0, &slot)?;
        file.sync_all()?;
    }
    ops.sync_dir(&canonical_new)?;

    // Validate: the restored root must pass the full W0 triple-validation
    // recovery and select the restored generation.
    let selected = recover(&lock)?;
    if selected.slot.publication_sequence != record.publication_sequence
        || selected.slot.generation_sha256 != record.generation_sha256
    {
        return Err(RetirementError::ValidationFailed(format!(
            "restored root recovered sequence {} (expected {})",
            selected.slot.publication_sequence, record.publication_sequence
        )));
    }

    let recovery = RootRecovery {
        lock,
        wal_boundary: WalBoundary::from_cursor(&selected.wal_cursor),
        previous_generation_path: None,
        orphans: vec![OrphanClassification::SelectedGeneration {
            path: restored_path,
        }],
        selected,
    };
    Ok(RootOwnership::from_recovery(recovery, OpenMode::Writable))
}
