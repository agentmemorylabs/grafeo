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
use grafeo_storage::generation::wal_cursor::{WalReplayCursor, validate_replayable};
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
    backup_root_inner(auth, ownership, destination_dir, backup_name, None)
}

/// Back up a root that a live, writable `GrafeoDB` is serving.
///
/// Same pin / validate / stage / publish sequence as
/// [`backup_generation_root`], except that the WAL is captured as a
/// **consistent cut** instead of whole files: `wal_cut` flushes and fsyncs
/// the live WAL under its own append lock and reports the active file's
/// sequence and length. Every lower sequence is final at that moment. Files
/// from the manifest boundary through the active sequence are copied, the
/// active one only up to the reported length, so bytes appended (or files
/// created by a rotation) while the copy runs are not part of the backup.
/// The staged WAL is replay-validated before publication. The caller must
/// already exclude publication (epoch handoff) for the whole call; see
/// `GrafeoDB::backup_generation_root`.
#[cfg(all(
    feature = "wal",
    feature = "lpg",
    feature = "generation",
    feature = "compact-store",
    feature = "generation-streaming",
    feature = "mmap"
))]
pub(crate) fn backup_live_generation_root(
    auth: &RetirementAuthority,
    ownership: &RootOwnership,
    destination_dir: &Path,
    backup_name: &str,
    wal_cut: &LiveWalCutFn<'_>,
) -> Result<GenerationBackupReceipt, RetirementError> {
    backup_root_inner(auth, ownership, destination_dir, backup_name, Some(wal_cut))
}

/// Takes the live WAL cut: flushes and fsyncs the WAL under its append lock
/// and returns `(active sequence, active file length)`, or `None` when the
/// database has no WAL of its own (nothing is appended, so the files on disk
/// are final).
pub(crate) type LiveWalCutFn<'a> = dyn Fn() -> Result<Option<(u64, u64)>, RetirementError> + 'a;

/// Shared body. `live_wal_cut` is `Some` for a live root (consistent-cut WAL
/// capture) and `None` for an offline root (every WAL file copied whole).
fn backup_root_inner(
    auth: &RetirementAuthority,
    ownership: &RootOwnership,
    destination_dir: &Path,
    backup_name: &str,
    live_wal_cut: Option<&LiveWalCutFn<'_>>,
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
    sweep_dead_staging_dirs(&canonical_dest);
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

    // The consistency point of a live backup: the WAL is flushed and fsynced
    // under its own append lock, which also yields the active file's length.
    // Everything acknowledged before this point is inside the cut; the files
    // are append-only, so a prefix ending at an append-group boundary is a
    // valid log.
    let wal_cut = match live_wal_cut {
        Some(cut) => WalSelection::Cut(capture_wal_cut(root, slot.wal_log_sequence, cut()?)?),
        None => WalSelection::All,
    };

    // Stage the backup in a unique temp directory under the destination.
    let temp_name = format!(
        ".tmp-gbackup-{}-{}-{backup_name}",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let temp_dir = canonical_dest.join(&temp_name);
    let temp_wal = temp_dir.join("wal");

    // Stage, validate and publish. Any failure removes the staging dir, so a
    // failed (say, disk-full) backup never leaks a partial copy; the pin is
    // released when `pin` drops.
    let published = (|| -> Result<(GenerationBackupManifest, u64), RetirementError> {
        std::fs::create_dir_all(&temp_wal)?;
        let (manifest_record, mut copied_bytes) =
            stage_backup(ops, root, &slot, &wal_cut, &temp_dir, &temp_wal)?;

        // A live cut must replay from the manifest boundary exactly as
        // restore + open will; refuse here rather than at restore time.
        if matches!(wal_cut, WalSelection::Cut(_)) {
            let cursor = WalReplayCursor {
                log_sequence: slot.wal_log_sequence,
                byte_offset: slot.wal_byte_offset,
                epoch: slot.overlay_epoch,
                transaction_id: slot.transaction_id,
            };
            validate_replayable(&temp_wal, &cursor).map_err(|e| {
                RetirementError::ValidationFailed(format!(
                    "staged WAL cut does not replay from the manifest boundary: {e}"
                ))
            })?;
        }

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
        Ok((manifest_record, copied_bytes))
    })();
    let (manifest_record, copied_bytes) = match published {
        Ok(done) => done,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&temp_dir);
            return Err(e);
        }
    };

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
    wal_cut: &WalSelection,
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

    // Copy the WAL (names preserved so the restored root's replay cursor
    // resolves identically).
    let mut wal_files = Vec::new();
    let wal_dir = root.join("wal");
    match wal_cut {
        WalSelection::All => {
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
        }
        WalSelection::Cut(files) => {
            for (name, cut_len) in files {
                let dst = temp_wal.join(name);
                copy_prefix(&wal_dir.join(name), &dst, *cut_len)?;
                ops.sync_path(&dst)?;
                let sha = ops.sha256(&dst)?;
                copied += cut_len;
                wal_files.push(BackedUpWalFile {
                    name: name.clone(),
                    length: *cut_len,
                    sha256: sha,
                });
            }
        }
    }

    // The WAL files' directory entries must be durable before the backup is
    // published, or a power loss could leave a published backup without them.
    ops.sync_dir(temp_wal)?;

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

/// Which WAL bytes a backup captures.
enum WalSelection {
    /// Offline root: every file in `wal/`, whole.
    All,
    /// Live root: `(file name, byte length)` recorded at the cut.
    Cut(Vec<(String, u64)>),
}

/// Choose the WAL files of a live cut and their byte lengths.
///
/// With `active = Some((sequence, length))` (from the WAL's own cut under its
/// append lock) the cut is the files `from_sequence..=sequence`: every lower
/// file is final and taken whole, the active one only up to `length`. A file
/// above `sequence` (created by a rotation after the cut) is ignored. With
/// `None` (no live WAL in this process) every file from `from_sequence` on is
/// final and taken whole. The sequences must be contiguous.
fn capture_wal_cut(
    root: &Path,
    from_sequence: u64,
    active: Option<(u64, u64)>,
) -> Result<Vec<(String, u64)>, RetirementError> {
    let wal_dir = root.join("wal");
    let mut listed: Vec<(u64, String)> = Vec::new();
    for entry in std::fs::read_dir(&wal_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let seq = name
            .strip_prefix("wal_")
            .and_then(|rest| rest.strip_suffix(".log"))
            .and_then(|n| n.parse::<u64>().ok());
        if let Some(seq) = seq
            && seq >= from_sequence
            && active.is_none_or(|(active_seq, _)| seq <= active_seq)
            && entry.file_type()?.is_file()
        {
            listed.push((seq, name));
        }
    }
    listed.sort_unstable();
    for (index, (seq, _)) in listed.iter().enumerate() {
        let expected = from_sequence + index as u64;
        if *seq != expected {
            return Err(RetirementError::ValidationFailed(format!(
                "WAL sequence {expected} is missing (found {seq}); the log from the \
                 manifest boundary (sequence {from_sequence}) is not contiguous"
            )));
        }
    }
    if let Some((active_seq, _)) = active
        && listed.last().map(|(seq, _)| *seq) != Some(active_seq)
    {
        return Err(RetirementError::ValidationFailed(format!(
            "active WAL file (sequence {active_seq}) is missing from {}",
            wal_dir.display()
        )));
    }
    if listed.is_empty() {
        return Err(RetirementError::ValidationFailed(format!(
            "WAL file for the manifest boundary (sequence {from_sequence}) is missing"
        )));
    }
    listed
        .into_iter()
        .map(|(seq, name)| {
            let len = match active {
                Some((active_seq, active_len)) if seq == active_seq => active_len,
                _ => std::fs::metadata(wal_dir.join(&name))?.len(),
            };
            Ok((name, len))
        })
        .collect()
}

/// Remove `.tmp-gbackup-<pid>-*` staging directories left by a backup whose
/// process died (Linux `/proc` check; elsewhere nothing is swept).
fn sweep_dead_staging_dirs(dest: &Path) {
    if !Path::new("/proc/self").exists() {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dest) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(rest) = name.strip_prefix(".tmp-gbackup-") else {
            continue;
        };
        let Some(pid) = rest.split('-').next().and_then(|p| p.parse::<u32>().ok()) else {
            continue;
        };
        if !Path::new("/proc").join(pid.to_string()).exists() {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

/// Copy exactly the first `len` bytes of `src` to a new file `dst`
/// (streaming, bounded memory). Fails if `src` is shorter than `len`.
fn copy_prefix(src: &Path, dst: &Path, len: u64) -> Result<(), RetirementError> {
    use std::io::{Read, Write};
    let mut input = std::fs::File::open(src)?.take(len);
    let mut output = std::fs::File::create(dst)?;
    let copied = std::io::copy(&mut input, &mut output)?;
    if copied != len {
        return Err(RetirementError::ValidationFailed(format!(
            "WAL file {} shrank during backup: copied {copied} of {len} bytes",
            src.display()
        )));
    }
    output.flush()?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn wal_dir_with(files: &[(u64, usize)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("temp dir");
        let wal = dir.path().join("wal");
        std::fs::create_dir_all(&wal).expect("wal dir");
        for (seq, len) in files {
            std::fs::write(wal.join(format!("wal_{seq:08}.log")), vec![7u8; *len])
                .expect("write wal file");
        }
        dir
    }

    #[test]
    fn live_cut_ignores_files_created_after_the_cut_and_uses_the_active_length() {
        // wal_5 is active with 10 bytes at the cut; wal_6 appeared from a
        // rotation just after it and must not be part of the backup; wal_5 on
        // disk is already longer than the cut.
        let dir = wal_dir_with(&[(4, 100), (5, 40), (6, 3)]);
        let cut = capture_wal_cut(dir.path(), 4, Some((5, 10))).expect("cut");
        assert_eq!(
            cut,
            vec![
                ("wal_00000004.log".to_string(), 100),
                ("wal_00000005.log".to_string(), 10)
            ]
        );
    }

    #[test]
    fn cut_refuses_a_sequence_gap_or_a_missing_active_file() {
        let gap = wal_dir_with(&[(4, 1), (6, 1)]);
        assert!(capture_wal_cut(gap.path(), 4, Some((6, 1))).is_err());
        let missing_active = wal_dir_with(&[(4, 1)]);
        assert!(capture_wal_cut(missing_active.path(), 4, Some((5, 1))).is_err());
        let missing_boundary = wal_dir_with(&[(5, 1)]);
        assert!(capture_wal_cut(missing_boundary.path(), 4, None).is_err());
    }

    #[test]
    fn cut_without_a_live_wal_takes_every_file_whole() {
        let dir = wal_dir_with(&[(4, 9), (5, 11)]);
        let cut = capture_wal_cut(dir.path(), 4, None).expect("cut");
        assert_eq!(cut.iter().map(|(_, l)| *l).collect::<Vec<_>>(), vec![9, 11]);
    }
}
