//! Real WAL replay cursors and generation-boundary cuts (G-EM0.W0-B, Module 3).
//!
//! A [`WalReplayCursor`] is a durable replay boundary recorded in a manifest
//! slot: `(log_sequence, byte_offset)` into the real production WAL. The
//! boundary is created by [`cut_generation_boundary`] (sync + rotate), proven
//! replayable by [`validate_replayable`] against real WAL files and frame
//! parsing, and honored by [`truncate_before`] for whole-file retention.
//!
//! Production WAL internals (`wal/log.rs`, `wal/recovery.rs`) are read-only
//! authorities; this module calls their public APIs and parses the documented
//! frame format `[len: u32 LE][payload][crc32: u32 LE]`.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::wal::{WalEntry, WalManager, WalRecord};

/// Durable WAL replay boundary recorded in a manifest slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalReplayCursor {
    /// WAL log file sequence.
    pub log_sequence: u64,
    /// Byte offset of the first valid frame in that file.
    pub byte_offset: u64,
    /// Overlay epoch at the boundary (from checkpoint metadata, 0 when none).
    pub epoch: u64,
    /// Last committed transaction ID at the boundary (0 when none).
    pub transaction_id: u64,
}

/// Result of a generation boundary cut.
#[derive(Debug, Clone)]
pub struct GenerationCut {
    /// Cursor at the first valid frame position of the new sequence.
    pub cursor: WalReplayCursor,
    /// WAL log files retained after the cut, in sequence order.
    pub retained_log_files: Vec<PathBuf>,
}

/// Errors from WAL cursor operations.
#[derive(Debug, thiserror::Error)]
pub enum WalCursorError {
    /// The cursor's log file does not exist.
    #[error("cursor file missing: sequence={0}")]
    MissingFile(u64),
    /// A sequence between the cursor and the newest file is missing.
    #[error("sequence gap: expected={expected}, found={found}")]
    SequenceGap {
        /// Expected next sequence.
        expected: u64,
        /// Sequence actually found.
        found: u64,
    },
    /// The byte offset is not at a frame boundary (or points past EOF).
    #[error("frame not aligned at offset {0}")]
    FrameMisaligned(u64),
    /// A frame's checksum is invalid or its payload is not a valid record.
    #[error("frame checksum invalid at sequence={seq}, offset={offset}")]
    FrameChecksum {
        /// WAL file sequence.
        seq: u64,
        /// Byte offset of the failing frame.
        offset: u64,
    },
    /// A non-active WAL file ends mid-transaction or with a torn frame
    /// (a rotated file must end at a committed boundary).
    #[error("incomplete committed transaction at sequence={0}")]
    IncompleteTransaction(u64),
    /// Underlying I/O error.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

impl From<grafeo_common::utils::error::Error> for WalCursorError {
    fn from(e: grafeo_common::utils::error::Error) -> Self {
        match e {
            grafeo_common::utils::error::Error::Io(io) => WalCursorError::Io(io),
            other => WalCursorError::Io(std::io::Error::other(other.to_string())),
        }
    }
}

/// Parse a WAL sequence number from a `wal_XXXXXXXX.log` file name.
fn sequence_from_path(path: &Path) -> Option<u64> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.strip_prefix("wal_"))
        .and_then(|s| s.parse().ok())
}

/// List `wal_*.log` files in a directory, sorted by sequence.
fn wal_files(wal_dir: &Path) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let mut files = Vec::new();
    if !wal_dir.exists() {
        return Ok(files);
    }
    for entry in std::fs::read_dir(wal_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "log")
            && let Some(seq) = sequence_from_path(&path)
        {
            files.push((seq, path));
        }
    }
    files.sort_by_key(|(seq, _)| *seq);
    Ok(files)
}

/// Cut a generation boundary: sync the WAL, rotate to a new file, and
/// return a cursor at offset 0 of the new sequence. New writes resume in
/// that sequence.
///
/// # Errors
///
/// Returns [`WalCursorError::Io`] when sync/rotate fails.
pub fn cut_generation_boundary(wal: &WalManager) -> Result<GenerationCut, WalCursorError> {
    wal.sync()?;
    wal.rotate()?;
    let log_sequence = wal.current_sequence();

    let (epoch, transaction_id) = match wal.read_checkpoint_metadata()? {
        Some(meta) => (meta.epoch.0, meta.transaction_id.0),
        None => (0, 0),
    };

    let cursor = WalReplayCursor {
        log_sequence,
        byte_offset: 0,
        epoch,
        transaction_id,
    };

    // Make the new log file's directory entry durable so the cursor's file
    // cannot vanish in a power loss before the manifest references it.
    let dir = wal.dir();
    let dir_file = File::open(dir)?;
    dir_file.sync_all()?;

    let retained_log_files = wal
        .log_files()?
        .into_iter()
        .filter(|p| p.extension().is_some_and(|ext| ext == "log"))
        .collect();

    Ok(GenerationCut {
        cursor,
        retained_log_files,
    })
}

/// Validate that a recorded cursor is replayable: the cursor file exists,
/// sequences are contiguous through the newest file, `byte_offset` is
/// frame-aligned, every complete frame checksum-validates, and rotated
/// (non-active) files end at committed boundaries.
///
/// # Errors
///
/// Returns [`WalCursorError`] for every validation failure.
pub fn validate_replayable(wal_dir: &Path, cursor: &WalReplayCursor) -> Result<(), WalCursorError> {
    let files = wal_files(wal_dir)?;
    let max_seq = files.last().map_or(0, |(seq, _)| *seq);

    if !files.iter().any(|(seq, _)| *seq == cursor.log_sequence) {
        return Err(WalCursorError::MissingFile(cursor.log_sequence));
    }
    for seq in cursor.log_sequence + 1..=max_seq {
        if !files.iter().any(|(s, _)| *s == seq) {
            return Err(WalCursorError::SequenceGap {
                expected: seq,
                found: max_seq,
            });
        }
    }

    let cursor_path = files
        .iter()
        .find(|(seq, _)| *seq == cursor.log_sequence)
        .map(|(_, p)| p.clone())
        .ok_or(WalCursorError::MissingFile(cursor.log_sequence))?;

    let mut file = File::open(&cursor_path)?;
    let file_len = file.metadata()?.len();
    if cursor.byte_offset > file_len {
        return Err(WalCursorError::FrameMisaligned(cursor.byte_offset));
    }
    file.seek(SeekFrom::Start(cursor.byte_offset))?;

    // Parse frames from the cursor file through every later file. The
    // newest file may have a torn tail (crash during write); any earlier
    // file must end exactly at a committed boundary.
    let mut offset = cursor.byte_offset;
    let mut tx_open = false;
    for (seq, path) in files.iter().filter(|(seq, _)| *seq >= cursor.log_sequence) {
        if *seq != cursor.log_sequence {
            file = File::open(path)?;
            offset = 0;
        }
        let is_active = *seq == max_seq;
        let next = parse_frames(&mut file, *seq, &mut offset, &mut tx_open, is_active)?;
        if !next && !is_active {
            // Reached EOF: rotated file must end at a committed boundary.
            if tx_open {
                return Err(WalCursorError::IncompleteTransaction(*seq));
            }
        }
        if !next {
            break;
        }
    }

    Ok(())
}

/// Parse frames from the current position. Returns false when EOF (or a
/// tolerated torn tail in the active file) stops parsing.
fn parse_frames(
    file: &mut File,
    seq: u64,
    offset: &mut u64,
    tx_open: &mut bool,
    is_active: bool,
) -> Result<bool, WalCursorError> {
    loop {
        let mut len_buf = [0u8; 4];
        match file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Clean EOF at a frame boundary.
                return Ok(false);
            }
            Err(_) => {
                // Partial length prefix: torn tail.
                if is_active {
                    return Ok(false);
                }
                return Err(WalCursorError::IncompleteTransaction(seq));
            }
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut data = vec![0u8; len];
        let mut crc_buf = [0u8; 4];
        if file.read_exact(&mut data).is_err() || file.read_exact(&mut crc_buf).is_err() {
            if is_active {
                return Ok(false);
            }
            return Err(WalCursorError::IncompleteTransaction(seq));
        }
        let stored = u32::from_le_bytes(crc_buf);
        let computed = crc32fast::hash(&data);
        if stored != computed {
            return Err(WalCursorError::FrameChecksum {
                seq,
                offset: *offset,
            });
        }
        match bincode::serde::decode_from_slice::<WalRecord, _>(&data, bincode::config::standard())
        {
            Ok((record, _)) => {
                let completes_tx = record.is_commit()
                    || record.is_abort()
                    || record.is_checkpoint()
                    || record.is_metadata();
                *tx_open = !completes_tx;
            }
            Err(_) => {
                return Err(WalCursorError::FrameChecksum {
                    seq,
                    offset: *offset,
                });
            }
        }
        // reason: frame length is bounded by the 4-byte prefix, fits u64
        #[allow(clippy::cast_possible_truncation)]
        {
            *offset += 4 + len as u64 + 4;
        }
    }
}

/// Return the earliest cursor that must be retained given two valid slots:
/// the older log sequence wins (ties broken by byte offset).
#[must_use]
pub fn earliest_retained_cursor(
    selected: &WalReplayCursor,
    previous: Option<&WalReplayCursor>,
) -> WalReplayCursor {
    match previous {
        None => *selected,
        Some(prev) => {
            if prev.log_sequence < selected.log_sequence
                || (prev.log_sequence == selected.log_sequence
                    && prev.byte_offset <= selected.byte_offset)
            {
                *prev
            } else {
                *selected
            }
        }
    }
}

/// Delete whole WAL log files with sequence strictly older than the
/// retention floor's log sequence. Never deletes the floor's own file.
///
/// # Errors
///
/// Returns [`WalCursorError::Io`] when a deletion fails.
pub fn truncate_before(
    wal_dir: &Path,
    retention_floor: &WalReplayCursor,
) -> Result<Vec<PathBuf>, WalCursorError> {
    let files = wal_files(wal_dir)?;
    let mut deleted = Vec::new();
    for (seq, path) in files {
        if seq < retention_floor.log_sequence {
            std::fs::remove_file(&path)?;
            deleted.push(path);
        }
    }
    Ok(deleted)
}

#[cfg(test)]
#[path = "tests/wal_cursor_tests.rs"]
mod tests;
