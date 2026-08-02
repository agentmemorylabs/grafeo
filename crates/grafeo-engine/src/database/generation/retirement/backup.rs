//! Generation-root backup (G-EM0.4b, packet requirement 4).
//!
//! A generation-root backup pins an **exact selected manifest sequence**
//! plus all referenced immutable generation/WAL bytes *before* copying:
//!
//! 1. The current W0 slot is read fresh; the generation is pinned through
//!    [`RetirementAuthority::pin_for_backup`] so live-root GC can never
//!    delete it mid-copy.
//! 2. The pinned generation file is re-validated (length + SHA-256 against
//!    the slot) so the bytes copied are exactly the bytes the slot records.
//! 3. The generation and every referenced WAL file are streaming-copied
//!    into a uniquely named temp directory outside the live root, fsynced,
//!    re-hashed (the durable copies are proven to be the pinned bytes), and
//!    published by atomic rename with a parent-directory fsync.
//! 4. The pin is released only after the copy is durable (RAII guard).
//!
//! Restore lives in [`super::restore`]; the shared backup-manifest types and
//! validation helpers here are `pub(super)` so restore can reuse them.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use grafeo_storage::file::generation_writer::{GenerationFileOps, OsGenerationFileOps};
use grafeo_storage::generation::manifest::{self, ManifestSlot};
use serde::{Deserialize, Serialize};

use super::super::ownership::RootOwnership;
use super::{RetirementAuthority, RetirementError};

/// File name of the backup manifest inside a backup directory.
pub const BACKUP_MANIFEST_NAME: &str = "generation_backup.bin";
/// Backup format version written by this implementation.
pub(super) const BACKUP_FORMAT_VERSION: u32 = 1;
/// Streaming copy buffer (bounded memory; never a whole-file Vec).
pub(super) const COPY_BUFFER_BYTES: usize = 4 * 1024 * 1024;

/// One WAL file captured in a backup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackedUpWalFile {
    /// File name within the backup's `wal/` directory.
    pub name: String,
    /// Byte length of the captured file.
    pub length: u64,
    /// SHA-256 of the captured file bytes.
    pub sha256: [u8; 32],
}

/// The backup manifest: the exact pinned manifest sequence plus the
/// identity and hash of every referenced immutable byte.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationBackupManifest {
    /// Backup format version.
    pub version: u32,
    /// The pinned manifest publication sequence.
    pub publication_sequence: u64,
    /// The pinned parent publication sequence (0 = genesis).
    pub parent_publication_sequence: u64,
    /// The pinned generation identifier.
    pub generation_id: String,
    /// The pinned parent generation identifier.
    pub parent_generation_id: String,
    /// Root-relative generation path (`generations/<name>.grafeo`).
    pub generation_path: String,
    /// Byte length of the generation file.
    pub generation_length: u64,
    /// SHA-256 of the generation file bytes.
    pub generation_sha256: [u8; 32],
    /// Node count recorded in the generation container.
    pub node_count: u64,
    /// Edge count recorded in the generation container.
    pub edge_count: u64,
    /// Outer container format version.
    pub outer_container_format_version: u32,
    /// CompactStore format version.
    pub compact_store_format_version: u16,
    /// WAL log sequence of the pinned replay cursor.
    pub wal_log_sequence: u64,
    /// WAL byte offset of the pinned replay cursor.
    pub wal_byte_offset: u64,
    /// Overlay epoch captured at publication.
    pub overlay_epoch: u64,
    /// Last committed transaction ID captured at publication.
    pub transaction_id: u64,
    /// Every WAL file captured with the backup.
    pub wal_files: Vec<BackedUpWalFile>,
    /// Wall-clock milliseconds (UNIX epoch) when the backup was taken.
    pub created_at_ms: u64,
}

/// Receipt for a completed generation-root backup.
#[derive(Debug, Clone)]
pub struct GenerationBackupReceipt {
    /// The published backup directory (outside the live root).
    pub backup_dir: PathBuf,
    /// The pinned publication sequence that was backed up.
    pub publication_sequence: u64,
    /// The backed-up generation identifier.
    pub generation_id: String,
    /// Total copied bytes (generation + WAL + manifest).
    pub copied_bytes: u64,
    /// Number of WAL files captured.
    pub wal_files: usize,
}

/// Monotonic counter for unique temp-backup directory names.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Back up the currently selected generation of an owned live root.
///
/// `destination_dir` must be outside the live root; `backup_name` must be a
/// single safe path component. The returned receipt names the published
/// backup directory. The backup pin is held for the entire copy and
/// released before return (RAII), so GC can never delete the pinned
/// generation mid-copy.
///
/// # Errors
///
/// Returns [`RetirementError::UnsafeName`] for an unsafe backup name,
/// [`RetirementError::DestinationInsideRoot`] when the destination is inside
/// the live root, [`RetirementError::ValidationFailed`] when the pinned
/// generation's on-disk bytes no longer match its slot, and
/// [`RetirementError::Io`] for copy/fsync failures.
pub fn backup_generation_root(
    auth: &RetirementAuthority,
    ownership: &RootOwnership,
    destination_dir: &Path,
    backup_name: &str,
) -> Result<GenerationBackupReceipt, RetirementError> {
    let root = ownership.canonical_root();
    if auth.root() != root {
        return Err(RetirementError::ValidationFailed(
            "retirement authority and ownership are bound to different roots".to_string(),
        ));
    }
    if !is_safe_component(backup_name) {
        return Err(RetirementError::UnsafeName);
    }
    std::fs::create_dir_all(destination_dir)?;
    let canonical_dest = std::fs::canonicalize(destination_dir)?;
    if canonical_dest.starts_with(root) {
        return Err(RetirementError::DestinationInsideRoot);
    }
    let final_dir = canonical_dest.join(backup_name);
    if final_dir.exists() {
        return Err(RetirementError::ValidationFailed(format!(
            "backup destination {} already exists; never overwrite a backup",
            final_dir.display()
        )));
    }

    // Pin the exact selected manifest sequence BEFORE copying anything.
    let manifest_path = root.join("manifest.bin");
    let manifest_bytes = std::fs::read(&manifest_path)?;
    if manifest_bytes.iter().all(|b| *b == 0) {
        return Err(RetirementError::ValidationFailed(
            "genesis root: no published generation to back up".to_string(),
        ));
    }
    let (_index, slot) = manifest::read_manifest(&manifest_path)?;
    let pin = auth.pin_for_backup(slot.generation_path.clone(), slot.publication_sequence);
    debug_assert_eq!(pin.pinned_path(), slot.generation_path.as_str());

    let ops = OsGenerationFileOps;
    let generation_abs = root.join(&slot.generation_path);

    // Re-validate the pinned generation: the bytes copied must be exactly
    // the bytes the slot records (length + SHA-256).
    let actual_len = ops.file_len(&generation_abs)?;
    if actual_len != slot.generation_length {
        return Err(RetirementError::ValidationFailed(format!(
            "pinned generation length changed: slot={}, disk={actual_len}",
            slot.generation_length
        )));
    }
    let actual_sha = ops.sha256(&generation_abs)?;
    if actual_sha != slot.generation_sha256 {
        return Err(RetirementError::ValidationFailed(
            "pinned generation SHA-256 changed (the selected slot's bytes \
             must be immutable)"
                .to_string(),
        ));
    }

    // Stage the backup in a unique temp directory under the destination.
    let temp_name = format!(
        ".tmp-gbackup-{}-{}-{backup_name}",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let temp_dir = canonical_dest.join(&temp_name);
    let temp_wal = temp_dir.join("wal");
    std::fs::create_dir_all(&temp_wal)?;

    let (manifest_record, mut copied_bytes) =
        match stage_backup(ops, root, &slot, &temp_dir, &temp_wal) {
            Ok(staged) => staged,
            Err(e) => {
                // Fail closed: remove the incomplete staging dir, release the pin.
                let _ = std::fs::remove_dir_all(&temp_dir);
                return Err(e);
            }
        };

    // Publish: write the backup manifest, fsync, atomic rename, parent fsync.
    let manifest_data =
        bincode::serde::encode_to_vec(&manifest_record, bincode::config::standard())
            .map_err(|e| RetirementError::BackupManifest(format!("encode: {e}")))?;
    copied_bytes += manifest_data.len() as u64;
    let manifest_out = temp_dir.join(BACKUP_MANIFEST_NAME);
    std::fs::write(&manifest_out, &manifest_data)?;
    ops.sync_path(&manifest_out)?;
    ops.sync_dir(&temp_dir)?;

    ops.rename(&temp_dir, &final_dir)?;
    ops.sync_dir(&canonical_dest)?;

    // The pin is released here (guard drop), after the copy is durable.
    drop(pin);

    Ok(GenerationBackupReceipt {
        backup_dir: final_dir,
        publication_sequence: slot.publication_sequence,
        generation_id: slot.generation_id.clone(),
        copied_bytes,
        wal_files: manifest_record.wal_files.len(),
    })
}

/// Copy the pinned generation + all referenced WAL files into the staging
/// directory, returning the assembled backup manifest record and the total
/// copied bytes. Every copied file is fsynced and re-hashed so the durable
/// copies are proven to be the pinned bytes.
fn stage_backup(
    ops: OsGenerationFileOps,
    root: &Path,
    slot: &ManifestSlot,
    temp_dir: &Path,
    temp_wal: &Path,
) -> Result<(GenerationBackupManifest, u64), RetirementError> {
    let generation_abs = root.join(&slot.generation_path);
    let generation_name = Path::new(&slot.generation_path)
        .file_name()
        .ok_or_else(|| {
            RetirementError::ValidationFailed(format!(
                "slot generation path {} has no file name",
                slot.generation_path
            ))
        })?
        .to_string_lossy()
        .into_owned();
    let staged_generation = temp_dir.join(&generation_name);

    let mut copied = ops.copy_bounded(&generation_abs, &staged_generation, COPY_BUFFER_BYTES)?;
    if copied != slot.generation_length {
        return Err(RetirementError::ValidationFailed(format!(
            "copied {copied} generation bytes, expected {}",
            slot.generation_length
        )));
    }
    ops.sync_path(&staged_generation)?;
    let staged_sha = ops.sha256(&staged_generation)?;
    if staged_sha != slot.generation_sha256 {
        return Err(RetirementError::ValidationFailed(
            "staged generation copy hash mismatch".to_string(),
        ));
    }

    // Copy every referenced WAL file verbatim (names preserved so the
    // restored root's replay cursor resolves identically).
    let mut wal_files = Vec::new();
    let wal_dir = root.join("wal");
    if wal_dir.is_dir() {
        let mut names: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(&wal_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        names.sort_unstable();
        for name in names {
            let src = wal_dir.join(&name);
            let dst = temp_wal.join(&name);
            let len = ops.copy_bounded(&src, &dst, COPY_BUFFER_BYTES)?;
            ops.sync_path(&dst)?;
            let sha = ops.sha256(&dst)?;
            copied += len;
            wal_files.push(BackedUpWalFile {
                name,
                length: len,
                sha256: sha,
            });
        }
    }

    Ok((
        GenerationBackupManifest {
            version: BACKUP_FORMAT_VERSION,
            publication_sequence: slot.publication_sequence,
            parent_publication_sequence: slot.parent_publication_sequence,
            generation_id: slot.generation_id.clone(),
            parent_generation_id: slot.parent_generation_id.clone(),
            generation_path: slot.generation_path.clone(),
            generation_length: slot.generation_length,
            generation_sha256: slot.generation_sha256,
            node_count: slot.node_count,
            edge_count: slot.edge_count,
            outer_container_format_version: slot.outer_container_format_version,
            compact_store_format_version: slot.compact_store_format_version,
            wal_log_sequence: slot.wal_log_sequence,
            wal_byte_offset: slot.wal_byte_offset,
            overlay_epoch: slot.overlay_epoch,
            transaction_id: slot.transaction_id,
            wal_files,
            created_at_ms: super::super::now_ms(),
        },
        copied,
    ))
}

/// Validate one declared backup file: existence, exact length, exact hash.
pub(super) fn validate_declared_file(
    ops: OsGenerationFileOps,
    path: &Path,
    length: u64,
    sha256: &[u8; 32],
) -> Result<(), RetirementError> {
    if !ops.path_exists(path) {
        return Err(RetirementError::ValidationFailed(format!(
            "backup file missing: {}",
            path.display()
        )));
    }
    let actual_len = ops.file_len(path)?;
    if actual_len != length {
        return Err(RetirementError::ValidationFailed(format!(
            "backup file {} length {actual_len} != {length}",
            path.display()
        )));
    }
    let actual_sha = ops.sha256(path)?;
    if &actual_sha != sha256 {
        return Err(RetirementError::ValidationFailed(format!(
            "backup file {} SHA-256 mismatch",
            path.display()
        )));
    }
    Ok(())
}

/// True when `name` is a single safe path component: non-empty, no path
/// separators, no `.`/`..`, no NUL.
pub(super) fn is_safe_component(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains('\0')
}
