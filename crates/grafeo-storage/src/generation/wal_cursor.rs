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

use crate::generation::records::MAX_RECORD_BODY_BYTES;
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
    /// A frame's declared length exceeds the hard cap
    /// ([`MAX_RECORD_BODY_BYTES`](super::records::MAX_RECORD_BODY_BYTES)).
    /// Detected before the frame body is allocated (H-ADOPT.3 §A-cap).
    #[error("frame oversized at sequence={seq}, offset={offset}: declared={declared}")]
    FrameOversized {
        /// WAL file sequence.
        seq: u64,
        /// Byte offset of the frame's length prefix.
        offset: u64,
        /// Declared body length from the 4-byte prefix.
        declared: u32,
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
    // One reusable frame-body buffer: O(one frame <= cap) memory (H-ADOPT.3).
    let mut body_buf = Vec::new();
    for (seq, path) in files.iter().filter(|(seq, _)| *seq >= cursor.log_sequence) {
        if *seq != cursor.log_sequence {
            file = File::open(path)?;
            offset = 0;
        }
        let is_active = *seq == max_seq;
        while parse_one_frame(
            &mut file,
            *seq,
            &mut offset,
            &mut tx_open,
            is_active,
            &mut body_buf,
        )?
        .is_some()
        {}
        // Reached EOF (or a tolerated torn tail): a rotated file must end at
        // a committed boundary.
        if !is_active && tx_open {
            return Err(WalCursorError::IncompleteTransaction(*seq));
        }
    }

    Ok(())
}

/// One decoded WAL frame with its durable position (H-ADOPT.3 Phase A).
#[derive(Debug, Clone)]
pub struct ReplayFrame {
    /// The decoded record.
    pub record: WalRecord,
    /// WAL log file sequence containing the frame.
    pub log_sequence: u64,
    /// Byte offset of this frame's length prefix within that file.
    pub byte_offset: u64,
}

/// Streaming replay iterator from a boundary cursor (H-ADOPT.3 Phase A).
///
/// Yields one decoded record at a time with its `(seq, offset)` position,
/// walking contiguous sequences from the cursor file to the newest file
/// (the same gap/missing-file/misalignment checks as [`validate_replayable`]).
/// Anonymous memory is O(one frame ≤
/// [`MAX_RECORD_BODY_BYTES`](super::records::MAX_RECORD_BODY_BYTES)): the
/// iterator owns a single reusable frame-body buffer and never accumulates
/// records.
///
/// Ends cleanly at EOF of the active (max-sequence) file, tolerating a torn
/// tail there; every other anomaly yields the same [`WalCursorError`] variant
/// [`validate_replayable`] reports (checksum/decode failures are never
/// downgraded to torn tail). The stream is fused: after clean termination or
/// after yielding an error, [`Iterator::next`] returns `None`.
#[derive(Debug)]
pub struct WalReplayStream {
    /// Files from the cursor file through the newest, in sequence order.
    files: Vec<(u64, PathBuf)>,
    /// Index of the file currently being read.
    file_index: usize,
    /// Handle of the current file.
    file: File,
    /// Sequence of the current file.
    seq: u64,
    /// Sequence of the newest (active) file.
    max_seq: u64,
    /// Offset of the next byte to read in the current file.
    offset: u64,
    /// Single-open-transaction state (serial-transaction invariant).
    tx_open: bool,
    /// Reusable frame-body buffer: O(one frame) anonymous memory.
    body_buf: Vec<u8>,
    /// `(seq, byte_offset)` where the stream stopped after clean termination.
    stopped: Option<(u64, u64)>,
    /// Set once an error is yielded; the stream is fused thereafter.
    errored: bool,
}

impl WalReplayStream {
    /// `(seq, byte_offset)` where the stream stopped after **clean**
    /// termination; `None` before termination or after an error. Points at
    /// the first partial/absent frame of a torn tail (or the active file's
    /// EOF when the tail is frame-aligned).
    #[must_use]
    pub fn stopped_at(&self) -> Option<(u64, u64)> {
        self.stopped
    }
}

impl Iterator for WalReplayStream {
    type Item = Result<ReplayFrame, WalCursorError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Fused after clean termination or after an error.
        if self.errored || self.stopped.is_some() {
            return None;
        }
        loop {
            let is_active = self.seq == self.max_seq;
            let frame_start = self.offset;
            match parse_one_frame(
                &mut self.file,
                self.seq,
                &mut self.offset,
                &mut self.tx_open,
                is_active,
                &mut self.body_buf,
            ) {
                Ok(Some(record)) => {
                    return Some(Ok(ReplayFrame {
                        record,
                        log_sequence: self.seq,
                        byte_offset: frame_start,
                    }));
                }
                Ok(None) => {
                    if is_active {
                        // EOF of the active file (a torn tail, if any, was
                        // excluded by parse_one_frame): clean termination at
                        // the first partial/absent frame position.
                        self.stopped = Some((self.seq, self.offset));
                        return None;
                    }
                    // Rotated file: it must end at a committed boundary.
                    if self.tx_open {
                        self.errored = true;
                        return Some(Err(WalCursorError::IncompleteTransaction(self.seq)));
                    }
                    // Continue into the next sequence file (existence and
                    // contiguity were checked in replay_stream_from).
                    self.file_index += 1;
                    let (next_seq, next_path) = &self.files[self.file_index];
                    match File::open(next_path) {
                        Ok(next_file) => {
                            self.file = next_file;
                            self.seq = *next_seq;
                            self.offset = 0;
                        }
                        Err(e) => {
                            self.errored = true;
                            return Some(Err(WalCursorError::Io(e)));
                        }
                    }
                }
                Err(e) => {
                    self.errored = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

/// Open a streaming replay iterator over `wal_dir` starting at `cursor`
/// (H-ADOPT.3 Phase A).
///
/// Performs the same selection-time checks as [`validate_replayable`]
/// (cursor file present, contiguous sequences to the newest file, offset not
/// past EOF) and then yields one [`ReplayFrame`] per decoded frame.
///
/// # Errors
///
/// Returns [`WalCursorError::MissingFile`], [`WalCursorError::SequenceGap`],
/// [`WalCursorError::FrameMisaligned`], or [`WalCursorError::Io`] for the
/// construction-time checks.
pub fn replay_stream_from(
    wal_dir: &Path,
    cursor: &WalReplayCursor,
) -> Result<WalReplayStream, WalCursorError> {
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

    let cursor_index = files
        .iter()
        .position(|(seq, _)| *seq == cursor.log_sequence)
        .ok_or(WalCursorError::MissingFile(cursor.log_sequence))?;
    let mut file = File::open(&files[cursor_index].1)?;
    let file_len = file.metadata()?.len();
    if cursor.byte_offset > file_len {
        return Err(WalCursorError::FrameMisaligned(cursor.byte_offset));
    }
    file.seek(SeekFrom::Start(cursor.byte_offset))?;

    Ok(WalReplayStream {
        files: files[cursor_index..].to_vec(),
        file_index: 0,
        file,
        seq: cursor.log_sequence,
        max_seq,
        offset: cursor.byte_offset,
        tx_open: false,
        body_buf: Vec::new(),
        stopped: None,
        errored: false,
    })
}

/// Read one 4-byte length prefix, distinguishing clean EOF at a frame
/// boundary (0 bytes available → `Ok(None)`) from a partial prefix (1–3
/// bytes → `Err` with [`std::io::ErrorKind::UnexpectedEof`]).
fn read_len_prefix(file: &mut File) -> std::io::Result<Option<u32>> {
    let mut buf = [0u8; 4];
    let mut filled = 0usize;
    loop {
        match file.read(&mut buf[filled..]) {
            Ok(0) => {
                if filled == 0 {
                    return Ok(None);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "partial WAL frame length prefix",
                ));
            }
            Ok(n) => {
                filled += n;
                if filled == 4 {
                    return Ok(Some(u32::from_le_bytes(buf)));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

/// Parse one frame from the current file position.
///
/// Returns `Ok(Some(record))` for each complete, checksum-valid frame;
/// `Ok(None)` at clean EOF or a tolerated torn tail in the active file.
/// Partial reads in non-active files are [`WalCursorError::IncompleteTransaction`];
/// a declared body length above
/// [`MAX_RECORD_BODY_BYTES`](super::records::MAX_RECORD_BODY_BYTES) is
/// [`WalCursorError::FrameOversized`] **before** the body is allocated
/// (H-ADOPT.3 §A-cap). On success `*offset` advances past the frame; on
/// `Ok(None)`/`Err` it is left at the frame's length-prefix offset.
/// `body_buf` is reused across frames (O(one frame) memory).
fn parse_one_frame(
    file: &mut File,
    seq: u64,
    offset: &mut u64,
    tx_open: &mut bool,
    is_active: bool,
    body_buf: &mut Vec<u8>,
) -> Result<Option<WalRecord>, WalCursorError> {
    let frame_start = *offset;
    let declared = match read_len_prefix(file) {
        Ok(Some(len)) => len,
        Ok(None) => {
            // Clean EOF at a frame boundary.
            return Ok(None);
        }
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            // Partial length prefix: torn tail.
            if is_active {
                return Ok(None);
            }
            return Err(WalCursorError::IncompleteTransaction(seq));
        }
        Err(e) => return Err(WalCursorError::Io(e)),
    };

    // §A-cap: reject oversized frames before allocating the body. A corrupt
    // ~4 GiB prefix must fail here, not OOM in the allocation below.
    if declared > MAX_RECORD_BODY_BYTES {
        return Err(WalCursorError::FrameOversized {
            seq,
            offset: frame_start,
            declared,
        });
    }

    // reason: declared is capped at 16 MiB (u32); fits usize on all targets
    #[allow(clippy::cast_possible_truncation)]
    let body_len = declared as usize;
    body_buf.resize(body_len, 0);
    let mut crc_buf = [0u8; 4];
    if file.read_exact(body_buf).is_err() || file.read_exact(&mut crc_buf).is_err() {
        if is_active {
            return Ok(None);
        }
        return Err(WalCursorError::IncompleteTransaction(seq));
    }
    let stored = u32::from_le_bytes(crc_buf);
    let computed = crc32fast::hash(body_buf);
    if stored != computed {
        return Err(WalCursorError::FrameChecksum {
            seq,
            offset: frame_start,
        });
    }
    let Ok((record, _)) =
        bincode::serde::decode_from_slice::<WalRecord, _>(body_buf, bincode::config::standard())
    else {
        return Err(WalCursorError::FrameChecksum {
            seq,
            offset: frame_start,
        });
    };
    let completes_tx =
        record.is_commit() || record.is_abort() || record.is_checkpoint() || record.is_metadata();
    *tx_open = !completes_tx;
    // reason: frame size is bounded by the u32 prefix; u64 cannot wrap here
    *offset = frame_start + 4 + u64::from(declared) + 4;
    Ok(Some(record))
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

#[cfg(test)]
#[path = "tests/wal_replay_stream_tests.rs"]
mod replay_stream_tests;
