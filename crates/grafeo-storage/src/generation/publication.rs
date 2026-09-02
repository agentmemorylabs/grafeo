//! Generation publication ordering (G-EM0.W0-B, Module 4).
//!
//! [`publish_generation`] executes the locked W0 §11 publication sequence:
//! build under a correlation-scoped unpublished directory, stream + sync +
//! fresh-reopen validate, atomic rename to an immutable
//! `generations/g-<seq>-<sha16>.grafeo`, fsync the generations directory,
//! then write + sync the inactive manifest slot — **the manifest sync is the
//! commit point**. Only after the commit point may WAL truncation and
//! unpublished-path cleanup run (cleanup is optimization, not safety).
//!
//! All steps require a held [`RootLock`] and go through the
//! [`GenerationFileOps`] seam so the deterministic fault model
//! ([`super::faults`]) can prove exact durability ordering.

use std::io::{Seek, SeekFrom, Write};

use grafeo_common::grafeo_warn;
use grafeo_common::storage::SectionType;
use grafeo_common::utils::error::Error;

use crate::file::GrafeoFileManager;
use crate::file::generation_writer::{
    ExactSectionSource, GenerationContainerHeader, GenerationFileOps,
    create_versioned_sections_streaming,
};
use crate::wal::WalManager;

use super::lock::RootLock;
use super::manifest::{self, ManifestSlot};
use super::wal_cursor::{
    GenerationCut, WalCursorError, WalReplayCursor, cut_generation_boundary,
    earliest_retained_cursor, truncate_before, validate_replayable,
};

/// Input to a generation publication.
pub struct PublicationInput<'a> {
    /// Container header (epoch, transaction, counts).
    pub header: GenerationContainerHeader,
    /// Streaming section sources for the `.grafeo` container.
    pub sections: &'a mut [Box<dyn ExactSectionSource>],
    /// Generation identifier.
    pub generation_id: String,
    /// Parent generation identifier (None = derive from previous slot).
    pub parent_generation_id: Option<String>,
    /// Parent publication sequence (None = derive from previous slot).
    pub parent_publication_sequence: Option<u64>,
    /// Optional pre-cut WAL boundary from an earlier freeze (G-EM0.5c).
    ///
    /// When `Some`, publication **skips** the internal `cut_generation_boundary`
    /// step and records this cursor in the manifest slot. The freeze path owns
    /// the cut so concurrent next-epoch writes can land after boundary B while
    /// generation G(N) is still building. When `None` (default for 3a/3b/3c),
    /// publication cuts the WAL at step 0 exactly as before.
    pub pre_cut_cursor: Option<WalReplayCursor>,
}

/// Result of a successful publication.
#[derive(Debug)]
pub struct PublicationResult {
    /// Monotonic publication sequence (max slot + 1).
    pub publication_sequence: u64,
    /// Root-relative generation path.
    pub generation_path: String,
    /// SHA-256 over the complete generation file bytes.
    pub generation_sha256: [u8; 32],
    /// Byte length of the generation file.
    pub generation_length: u64,
    /// WAL replay cursor recorded in the new slot.
    pub wal_cursor: WalReplayCursor,
}

/// Errors from publication.
#[derive(Debug, thiserror::Error)]
pub enum PublicationError {
    /// The root lock is not held (unreachable by API shape; reserved).
    #[error("root lock not held")]
    NoLock,
    /// The immutable generation target already exists.
    #[error("generation target already exists: {0}")]
    TargetExists(String),
    /// Fresh-reopen validation failed.
    #[error("reopen validation failed: {0}")]
    ValidationFailed(String),
    /// Manifest read/write failed.
    #[error("manifest write failed: {0}")]
    ManifestWrite(String),
    /// WAL cut or truncation failed.
    #[error("WAL cut failed: {0}")]
    WalCut(#[from] WalCursorError),
    /// Underlying I/O error.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// Map a common storage error into a publication error. I/O errors keep
/// their [`PublicationError::Io`] identity; every other failure becomes a
/// [`PublicationError::ManifestWrite`] (the closest general "operation
/// failed" variant in the locked error surface).
impl From<Error> for PublicationError {
    fn from(e: Error) -> Self {
        match e {
            Error::Io(io) => PublicationError::Io(io),
            other => PublicationError::ManifestWrite(other.to_string()),
        }
    }
}

/// Execute the full publication sequence. Requires a held `RootLock`.
///
/// See the module docs and W0 §11 for the exact step order. The
/// `fault_hook` parameter exists only in test builds and is called at each
/// named publication boundary for deterministic power-loss injection.
///
/// # Errors
///
/// Returns [`PublicationError`] for every failure branch.
pub fn publish_generation(
    lock: &RootLock,
    input: PublicationInput<'_>,
    wal: &WalManager,
    file_ops: &dyn GenerationFileOps,
    #[cfg(test)] fault_hook: Option<&dyn Fn(&str)>,
) -> std::result::Result<PublicationResult, PublicationError> {
    #[cfg(test)]
    let hook = |name: &str| {
        if let Some(h) = fault_hook {
            h(name);
        }
    };
    #[cfg(not(test))]
    let hook = |_name: &str| {};

    // Step 0: cut the WAL generation boundary (sync + rotate), unless the
    // caller already froze boundary B at epoch-handoff freeze time (G-EM0.5c).
    // A pre-cut cursor must remain replayable against the live WAL directory.
    let cut = if let Some(cursor) = input.pre_cut_cursor {
        validate_replayable(wal.dir(), &cursor).map_err(PublicationError::WalCut)?;
        let retained_log_files = wal
            .log_files()
            .map_err(PublicationError::from)?
            .into_iter()
            .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
            .collect();
        GenerationCut {
            cursor,
            retained_log_files,
        }
    } else {
        cut_generation_boundary(wal)?
    };

    let root = lock.canonical_root();
    let generations_dir = root.join("generations");
    let wal_dir = root.join("wal");
    let manifest_path = root.join("manifest.bin");

    // Step 1-2: derive paths; for a new root create the layout and sync.
    let genesis = !file_ops.path_exists(&manifest_path);
    if genesis {
        file_ops.create_dir_all(&generations_dir)?;
        file_ops.create_dir_all(&wal_dir)?;
        let mut manifest_file = file_ops.create_new(&manifest_path)?;
        manifest_file.write_all(&[0u8; manifest::MANIFEST_SIZE])?;
        drop(manifest_file);
        file_ops.sync_path(&manifest_path)?;
        file_ops.sync_dir(&generations_dir)?;
        file_ops.sync_dir(&wal_dir)?;
        file_ops.sync_dir(root)?;
    }

    // Read the current manifest state for sequence/parent derivation.
    let (previous_slot, next_seq, inactive_index) =
        read_publication_state(&manifest_path, genesis, file_ops)?;

    // Step 3: correlation-scoped unpublished build directory.
    let correlation = correlation_id();
    let unpublished_dir = root.join(format!(".unpublished-{correlation}-g-{next_seq:020}"));
    file_ops.create_dir_all(&unpublished_dir)?;
    hook("after_unpublished_dir");

    let partial_path = unpublished_dir.join("generation.grafeo.partial");

    // Step 4: stream the complete container to the partial file.
    create_versioned_sections_streaming(&partial_path, &input.header, input.sections, file_ops)
        .map_err(|e| PublicationError::ManifestWrite(format!("streaming: {e}")))?;
    hook("after_streaming");

    // Step 5: sync the generation file.
    hook("before_gen_sync");
    file_ops.sync_path(&partial_path)?;
    hook("after_gen_sync");

    // Step 6: production fresh-reopen validation (open + section dir +
    // CompactStore entry + mmap with nonzero length).
    validate_container(&partial_path).map_err(PublicationError::ValidationFailed)?;
    hook("after_reopen");

    let generation_length = file_ops.file_len(&partial_path)?;
    let generation_sha256 = file_ops.sha256(&partial_path)?;
    let sha16 = hex16(&generation_sha256);

    // Step 7: atomic rename to the immutable final path.
    let final_name = format!("g-{next_seq:020}-{sha16}.grafeo");
    let final_path = generations_dir.join(&final_name);
    if file_ops.path_exists(&final_path) {
        return Err(PublicationError::TargetExists(final_name));
    }
    file_ops.rename(&partial_path, &final_path)?;
    hook("after_rename");

    // Step 8: sync the final file + the generations directory.
    file_ops.sync_path(&final_path)?;
    file_ops.sync_dir(&generations_dir)?;
    hook("after_gen_dir_sync");

    // Step 9: encode the next slot into the inactive region, write the
    // ENTIRE 4096 bytes, then sync the manifest — the commit point.
    let generation_path = format!("generations/{final_name}");
    let parent = previous_slot.as_ref();
    let slot = ManifestSlot {
        publication_sequence: next_seq,
        parent_publication_sequence: input
            .parent_publication_sequence
            .or(parent.map(|p| p.publication_sequence))
            .unwrap_or(0),
        overlay_epoch: input.header.epoch,
        transaction_id: input.header.transaction_id,
        wal_log_sequence: cut.cursor.log_sequence,
        wal_byte_offset: cut.cursor.byte_offset,
        generation_length,
        node_count: input.header.node_count,
        edge_count: input.header.edge_count,
        outer_container_format_version: crate::file::format::FORMAT_VERSION,
        compact_store_format_version: manifest::COMPACT_STORE_FORMAT_VERSION,
        generation_sha256,
        generation_id: input.generation_id.clone(),
        parent_generation_id: input
            .parent_generation_id
            .clone()
            .or_else(|| parent.map(|p| p.generation_id.clone()))
            .unwrap_or_default(),
        generation_path: generation_path.clone(),
    };

    let slot_bytes = manifest::encode_slot_bytes(&slot).map_err(PublicationError::ManifestWrite)?;
    {
        let mut manifest_file = file_ops.open_existing(&manifest_path)?;
        // reason: inactive_index is 0 or 1, so the byte offset always fits u64
        #[allow(clippy::cast_possible_truncation)]
        let slot_offset = (inactive_index * manifest::SLOT_SIZE) as u64;
        manifest_file.seek(SeekFrom::Start(slot_offset))?;
        hook("before_slot_write");
        manifest_file.write_all(&slot_bytes[..2048])?;
        hook("during_slot_write");
        manifest_file.write_all(&slot_bytes[2048..])?;
        hook("after_slot_write");
        drop(manifest_file);
    }
    hook("before_manifest_sync");
    file_ops.sync_path(&manifest_path)?;
    hook("after_manifest_sync");
    // ── COMMIT POINT PASSED ──────────────────────────────────────────

    // Step 10: WAL truncation — only now. The floor preserves the replay
    // range of BOTH the new slot and the still-retained previous slot.
    let previous_cursor = parent.map(|p| WalReplayCursor {
        log_sequence: p.wal_log_sequence,
        byte_offset: p.wal_byte_offset,
        epoch: p.overlay_epoch,
        transaction_id: p.transaction_id,
    });
    let floor = earliest_retained_cursor(&cut.cursor, previous_cursor.as_ref());
    hook("during_wal_cleanup");
    let _deleted = truncate_before(&wal_dir, &floor)?;
    hook("after_wal_cleanup");

    // Step 11: cleanup unpublished paths; unselected immutable generations
    // are classified but NOT deleted (GC is a later packet).
    cleanup_unpublished(&unpublished_dir);

    Ok(PublicationResult {
        publication_sequence: next_seq,
        generation_path,
        generation_sha256,
        generation_length,
        wal_cursor: cut.cursor,
    })
}

/// Read the manifest and derive the publication state: the previous valid
/// slot (for parent fields + retention floor), the next sequence, and the
/// inactive slot index. A missing or pristine (all-zero) manifest is
/// genesis; any other both-invalid state fails closed.
fn read_publication_state(
    manifest_path: &std::path::Path,
    genesis: bool,
    file_ops: &dyn GenerationFileOps,
) -> std::result::Result<(Option<ManifestSlot>, u64, usize), PublicationError> {
    if genesis {
        return Ok((None, 1, 0));
    }
    let bytes = file_ops
        .read_to_end(manifest_path)
        .map_err(|e| PublicationError::ManifestWrite(e.to_string()))?;
    if bytes.iter().all(|b| *b == 0) {
        return Ok((None, 1, 0));
    }
    let [slot0, slot1] = manifest::read_both_slots(manifest_path)
        .map_err(|e| PublicationError::ManifestWrite(e.to_string()))?;
    match (slot0, slot1) {
        (Ok(a), Ok(b)) => {
            let (active, inactive) = if b.publication_sequence > a.publication_sequence {
                (b, 0)
            } else {
                (a, 1)
            };
            let next = active.publication_sequence + 1;
            Ok((Some(active), next, inactive))
        }
        (Ok(a), Err(_)) => {
            let next = a.publication_sequence + 1;
            Ok((Some(a), next, 1))
        }
        (Err(_), Ok(b)) => {
            let next = b.publication_sequence + 1;
            Ok((Some(b), next, 0))
        }
        (Err(c0), Err(c1)) => Err(PublicationError::ManifestWrite(format!(
            "both slots invalid: slot0={c0}, slot1={c1}"
        ))),
    }
}

/// Fresh-open a container through the production reader and prove the
/// CompactStore section is present and mmap-able with nonzero length.
///
/// H-ADOPT.6 decision 7: when a Catalog section is present, validate it too
/// (readable + nonzero). The four index sections stay `required:false` in the
/// SectionType metadata — they remain optional but are CRC-validated by the
/// writer and by [`GrafeoFileManager::read_section_data`] / `mmap_section`
/// at open.
fn validate_container(path: &std::path::Path) -> std::result::Result<(), String> {
    let manager =
        GrafeoFileManager::open_read_only(path).map_err(|e| format!("open_read_only: {e}"))?;
    let dir = manager
        .read_section_directory()
        .map_err(|e| format!("read_section_directory: {e}"))?
        .ok_or_else(|| "no section directory".to_string())?;
    let entry = dir
        .find(SectionType::CompactStore)
        .ok_or_else(|| "CompactStore section missing".to_string())?;
    let mapped = manager
        .mmap_section(entry)
        .map_err(|e| format!("mmap_section: {e}"))?;
    if mapped.is_empty() {
        return Err("CompactStore section mapped with zero length".to_string());
    }
    if let Some(catalog_entry) = dir.find(SectionType::Catalog) {
        let data = manager
            .read_section_data(catalog_entry)
            .map_err(|e| format!("catalog section read: {e}"))?;
        if data.is_empty() {
            return Err("Catalog section present but zero length".to_string());
        }
    }
    Ok(())
}

/// 32 lowercase hex chars from a CSPRNG.
pub(crate) fn correlation_id() -> String {
    use std::fmt::Write as _;
    let bytes: [u8; 16] = rand::random();
    let mut out = String::with_capacity(32);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// First 16 hex chars of a SHA-256 digest.
fn hex16(sha: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(16);
    for b in sha.iter().take(8) {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Remove the unpublished build directory after the commit point. Cleanup
/// is best-effort; failure is logged as an error but never fails
/// publication (it is an optimization, not a safety condition).
fn cleanup_unpublished(unpublished_dir: &std::path::Path) {
    if let Err(e) = std::fs::remove_dir_all(unpublished_dir) {
        grafeo_warn!(
            "unpublished dir cleanup failed: {} ({})",
            e,
            unpublished_dir.display()
        );
    }
}
