//! Streaming global dictionary pass (G-EM0.5b D0.8.3).
//!
//! Replaces the heap-eager `Vec<String>` / `FxHashMap<String, u32>` global
//! dictionary with an external-merge pipeline that never retains a
//! database-proportional string map in anonymous memory.
//!
//! ## Input: unambiguous occurrence records
//!
//! Each string use is a [`SortRecord`] whose **key** is the exact string
//! bytes and whose **payload** is the framed `(use_kind, owner_key)`:
//!
//! ```text
//! key:     string bytes (UTF-8; byte-lexicographic sort key)
//! payload: use_kind u8 || owner_key_len u16 LE || owner_key
//! ```
//!
//! The string is never concatenated with a suffix to infer the split —
//! prefixes and embedded NUL bytes make that representation ambiguous
//! (packet D0.8.3). The string is the whole sort key; the use framing rides
//! in the opaque payload.
//!
//! ## Pass: adjacent dedup → deterministic lexicographic codes
//!
//! Occurrences are external-sorted by string bytes. One streaming merge:
//! - assigns each newly-seen string the next `u32` code (UTF-8
//!   byte-lexicographic order, matching `serialize_v5` Lexicographic);
//! - streams `StringOffsets` (with trailing sentinel), `StringBytes`, and one
//!   `DictionaryCodeIndex` record per code into their sinks;
//! - re-emits each occurrence as a **remap record** with key
//!   `use_kind || owner_key` and payload `str_len u32 LE || string || code
//!   u32 LE`, for pass-2 column/metadata resolution.
//!
//! For `DictValue` occurrences the owner_key is the column identity
//! (`table_id u16 BE || prop_key`), so the remap stream is grouped by column
//! and the column-body pass can resolve each column's strings from a
//! per-column chunk without any global string map.
//!
//! No `Vec<String>`, no complete merged-key vector, no graph-sized string map.

use crate::graph::compact::generation::budget::{GenerationBudget, GenerationMetrics};
use crate::graph::compact::generation::emit::descriptor::SegmentDescriptor;
use crate::graph::compact::generation::emit::sink::SegmentSink;
use crate::graph::compact::generation::error::GenerationError;
use crate::graph::compact::generation::runs::{
    CancelToken, ExternalRunMerger, RunSetLease, SortRecord,
};
use crate::graph::compact::mapped::{CODE_INDEX_RECORD_LEN, SegmentKind};
/// String occurrence use kinds (mirrors emit/dictionary.rs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum StringUseKind {
    /// Node table label.
    Label = 0,
    /// Property/column key.
    PropertyKey = 1,
    /// Edge type.
    EdgeType = 2,
    /// Dictionary-encoded column value.
    DictValue = 3,
    /// Zone-map string bound.
    ZoneString = 4,
}

impl StringUseKind {
    fn from_u8(v: u8) -> Result<Self, GenerationError> {
        Ok(match v {
            0 => Self::Label,
            1 => Self::PropertyKey,
            2 => Self::EdgeType,
            3 => Self::DictValue,
            4 => Self::ZoneString,
            other => return Err(GenerationError::Codec(format!("bad use_kind {other}"))),
        })
    }
}

/// Builds one occurrence [`SortRecord`]: key = string bytes, payload = framed
/// `(use_kind, owner_key)`.
///
/// # Errors
///
/// Returns [`GenerationError::WireWidthOverflow`] when `owner_key` exceeds `u16::MAX`.
pub fn occurrence_record(
    string: &[u8],
    use_kind: StringUseKind,
    owner_key: &[u8],
) -> Result<SortRecord, GenerationError> {
    let mut payload = Vec::with_capacity(3 + owner_key.len());
    payload.push(use_kind as u8);
    payload.extend_from_slice(
        &u16::try_from(owner_key.len())
            .map_err(|_| GenerationError::WireWidthOverflow {
                what: "occurrence_owner_key_len",
                count: owner_key.len() as u64,
                max: u64::from(u16::MAX),
            })?
            .to_le_bytes(),
    );
    payload.extend_from_slice(owner_key);
    Ok(SortRecord::new(string.to_vec(), payload))
}

/// Decodes the framed `(use_kind, owner_key)` from an occurrence payload.
fn decode_payload(payload: &[u8]) -> Result<(StringUseKind, &[u8]), GenerationError> {
    if payload.len() < 3 {
        return Err(GenerationError::Codec(
            "occurrence payload too short for (use_kind, owner_key_len)".into(),
        ));
    }
    let use_kind = StringUseKind::from_u8(payload[0])?;
    let olen = u16::from_le_bytes([payload[1], payload[2]]) as usize;
    if payload.len() < 3 + olen {
        return Err(GenerationError::Codec(
            "occurrence payload owner_key truncated".into(),
        ));
    }
    Ok((use_kind, &payload[3..3 + olen]))
}

/// Result of the streaming global dictionary pass.
pub struct StreamingDictionaryResult {
    /// Number of unique strings (== next code).
    pub unique_count: u64,
    /// Lease owning the remap runs (`(use_kind, owner_key, string)→code`).
    pub remap_lease: RunSetLease,
}

/// Drives the external-sort global dictionary pass.
pub struct StreamingDictionary<'a> {
    budget: &'a GenerationBudget,
    metrics: &'a mut GenerationMetrics,
}

impl<'a> StreamingDictionary<'a> {
    /// Creates a driver over the shared job budget/metrics.
    #[must_use]
    pub fn new(budget: &'a GenerationBudget, metrics: &'a mut GenerationMetrics) -> Self {
        Self { budget, metrics }
    }

    /// Runs the dictionary pass over the occurrence runs.
    ///
    /// `occ_lease` owns the external-sorted occurrence runs. `merger` merges
    /// them. Segment bytes are streamed into the three sinks. Occurrences are
    /// re-emitted to `remap_sink` as `(use_kind || owner_key) → (string, code)`
    /// records (the caller finishes that sink and owns the remap lease in the
    /// returned result). Returns the number of unique strings (== next code).
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError`] on overflow, I/O, or malformed records.
    pub fn run(
        &mut self,
        occ_lease: &RunSetLease,
        merger: &mut dyn ExternalRunMerger,
        offsets_sink: &mut dyn SegmentSink,
        bytes_sink: &mut dyn SegmentSink,
        code_index_sink: &mut dyn SegmentSink,
        remap_sink: &mut dyn crate::graph::compact::generation::runs::ExternalRunSink,
        cancel: Option<&CancelToken>,
    ) -> Result<u64, GenerationError> {
        let mut byte_pos: u64 = 0;
        let mut prev_string: Option<Vec<u8>> = None;

        // Reusable emit closure invoked per merged occurrence.
        let mut emit = |rec: &SortRecord, dict: &mut DictState| -> Result<(), GenerationError> {
            let string = &rec.key;
            let (use_kind, owner_key) = decode_payload(&rec.payload)?;

            // Assign a code on first sight of a new string.
            let code32 = if prev_string.as_deref() == Some(string.as_slice()) {
                u32::try_from(dict.current_code).map_err(|_| {
                    GenerationError::WireWidthOverflow {
                        what: "global_string_dictionary",
                        count: dict.current_code,
                        max: u64::from(u32::MAX),
                    }
                })?
            } else {
                // New unique string: assign next code, stream segments.
                let code = dict.next_code;
                let code32 =
                    u32::try_from(code).map_err(|_| GenerationError::WireWidthOverflow {
                        what: "global_string_dictionary",
                        count: code,
                        max: u64::from(u32::MAX),
                    })?;
                let slen = u32::try_from(string.len()).map_err(|_| {
                    GenerationError::WireWidthOverflow {
                        what: "global_dict_string_len",
                        count: string.len() as u64,
                        max: u64::from(u32::MAX),
                    }
                })?;
                offsets_sink.write(&byte_pos.to_le_bytes())?;
                bytes_sink.write(string)?;
                // DictionaryCodeIndex record: (offset u64, len u32, code u32).
                let mut ci = [0u8; CODE_INDEX_RECORD_LEN];
                ci[0..8].copy_from_slice(&byte_pos.to_le_bytes());
                ci[8..12].copy_from_slice(&slen.to_le_bytes());
                ci[12..16].copy_from_slice(&code32.to_le_bytes());
                code_index_sink.write(&ci)?;
                byte_pos += string.len() as u64;
                dict.next_code += 1;
                dict.current_code = u64::from(code32);
                prev_string = Some(string.clone());
                code32
            };

            // Re-emit remap record: key = use_kind || owner_key (the
            // DictValue owner_key carries the column identity, so the
            // remap stream is grouped by column), payload =
            // str_len u32 LE || string || code u32 LE. Sorted later by the
            // caller's remap sink.
            let slen =
                u32::try_from(string.len()).map_err(|_| GenerationError::WireWidthOverflow {
                    what: "remap_string_len",
                    count: string.len() as u64,
                    max: u64::from(u32::MAX),
                })?;
            let mut rkey = Vec::with_capacity(1 + owner_key.len());
            rkey.push(use_kind as u8);
            rkey.extend_from_slice(owner_key);
            let mut rpayload = Vec::with_capacity(4 + string.len() + 4);
            rpayload.extend_from_slice(&slen.to_le_bytes());
            rpayload.extend_from_slice(string);
            rpayload.extend_from_slice(&code32.to_le_bytes());
            remap_sink.push(SortRecord::new(rkey, rpayload))?;
            Ok(())
        };

        let mut state = DictState {
            next_code: 0,
            current_code: 0,
        };
        merger.merge_all(
            &occ_lease.handles,
            self.budget,
            self.metrics,
            cancel,
            &mut |rec| emit(rec, &mut state),
        )?;
        let next_code = state.next_code;

        // StringOffsets trailing sentinel.
        offsets_sink.write(&byte_pos.to_le_bytes())?;
        self.metrics.global_string_count = next_code;
        Ok(next_code)
    }
}

struct DictState {
    next_code: u64,
    current_code: u64,
}

/// Finalizes the three dictionary segment sinks into descriptors.
///
/// # Errors
///
/// Propagates sink finish errors.
pub fn finish_dictionary_sinks(
    offsets_sink: Box<dyn SegmentSink>,
    bytes_sink: Box<dyn SegmentSink>,
    code_index_sink: Box<dyn SegmentSink>,
) -> Result<(SegmentDescriptor, SegmentDescriptor, SegmentDescriptor), GenerationError> {
    Ok((
        offsets_sink.finish()?,
        bytes_sink.finish()?,
        code_index_sink.finish()?,
    ))
}

/// Kind codes for the dictionary segments (re-exported for the builder).
#[must_use]
pub const fn dictionary_segment_kinds() -> (SegmentKind, SegmentKind, SegmentKind) {
    (
        SegmentKind::StringOffsets,
        SegmentKind::StringBytes,
        SegmentKind::DictionaryCodeIndex,
    )
}
