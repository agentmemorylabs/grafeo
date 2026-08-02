//! Bounded V5SegmentSource trait and payload assembly helper (G-EM0.W0-A3).

use super::error::GenerationError;
use super::strings::GlobalStringDictionary;
use super::v5_emitter::{SegmentPlanEntry, build_segment_plan, emit_single_segment};
use crate::graph::compact::CompactStore;
use crate::graph::compact::mapped::{DIRECTORY_ENTRY_LEN, HEADER_LEN, SegmentKind, layout_flags};
use crate::graph::compact::section_v5::align_up;
use grafeo_common::utils::hash::FxHashMap;

#[cfg(test)]
const MAGIC: [u8; 4] = *b"GCST";

/// Individual CompactStore v5 segment payload and metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V5Segment {
    /// Segment kind code.
    pub kind: SegmentKind,
    /// Per-kind encoding version (usually 1).
    pub encoding_version: u16,
    /// Flags (bit 0 = required).
    pub flags: u16,
    /// Required alignment in bytes (1, 4, 8, etc.).
    pub alignment: u16,
    /// Fixed element width, or 0 for variable length.
    pub element_width: u32,
    /// Bounded segment byte payload (one segment, not whole payload).
    pub bytes: Vec<u8>,
}

/// Bounded stream of v5 payload segments emitted in ascending `SegmentKind` order.
pub trait V5SegmentSource {
    /// Returns the next segment or None when complete.
    ///
    /// # Errors
    ///
    /// Returns `GenerationError` on codec or width failures.
    fn next_segment(&mut self) -> Result<Option<V5Segment>, GenerationError>;

    /// Total number of segments to be emitted (for directory pre-allocation).
    fn segment_count(&self) -> usize;
}

/// Concrete lazy `V5SegmentSource` backed by a heap-built `CompactStore` and `GlobalStringDictionary`.
pub struct CompactV5SegmentSource<'a> {
    store: &'a CompactStore,
    global_strings: &'a GlobalStringDictionary,
    string_index: FxHashMap<String, u32>,
    segment_plan: Vec<SegmentPlanEntry>,
    cursor: usize,
}

impl<'a> CompactV5SegmentSource<'a> {
    /// Constructs a lazy `CompactV5SegmentSource` by computing segment plan metadata.
    ///
    /// # Errors
    ///
    /// Returns `GenerationError` if segment plan generation fails.
    pub fn new(
        store: &'a CompactStore,
        global_strings: &'a GlobalStringDictionary,
    ) -> Result<Self, GenerationError> {
        let (string_index, segment_plan) = build_segment_plan(store, global_strings)?;
        Ok(Self {
            store,
            global_strings,
            string_index,
            segment_plan,
            cursor: 0,
        })
    }

    /// Accesses the precalculated segment plan entries.
    #[must_use]
    pub fn segment_plan(&self) -> &[SegmentPlanEntry] {
        &self.segment_plan
    }

    /// Calculates total byte length of the complete v5 container payload.
    #[must_use]
    pub fn payload_len(&self) -> u64 {
        // reason: segment count fits u64 on all supported platforms
        #[allow(clippy::cast_possible_truncation)]
        let segment_count = self.segment_plan.len() as u64;
        // reason: DIRECTORY_ENTRY_LEN is 48
        #[allow(clippy::cast_possible_truncation)]
        let directory_length = segment_count * (DIRECTORY_ENTRY_LEN as u64);
        // reason: HEADER_LEN is 64
        #[allow(clippy::cast_possible_truncation)]
        let data_offset = align_up((HEADER_LEN as u64) + directory_length, 8);
        let mut cursor = data_offset;
        for plan in &self.segment_plan {
            cursor = align_up(cursor, u64::from(plan.alignment)) + plan.length;
        }
        cursor + 4 // trailing CRC-32
    }
}

impl V5SegmentSource for CompactV5SegmentSource<'_> {
    fn next_segment(&mut self) -> Result<Option<V5Segment>, GenerationError> {
        if self.cursor >= self.segment_plan.len() {
            Ok(None)
        } else {
            let plan = &self.segment_plan[self.cursor];
            let seg =
                emit_single_segment(self.store, self.global_strings, &self.string_index, plan)?;
            self.cursor += 1;
            Ok(Some(seg))
        }
    }

    fn segment_count(&self) -> usize {
        self.segment_plan.len()
    }
}

/// Assemble a full v5 payload byte vector from a `V5SegmentSource`.
///
/// # Errors
///
/// Returns `GenerationError` on segment streaming or geometry overflow.
#[cfg(test)]
pub fn assemble_v5_payload_from_source<S: V5SegmentSource>(
    source: &mut S,
    total_nodes: u64,
    total_edges: u64,
    preserves_ids: bool,
) -> Result<Vec<u8>, GenerationError> {
    use crate::graph::compact::mapped::FORMAT_VERSION_V5;
    let mut segments = Vec::with_capacity(source.segment_count());
    while let Some(seg) = source.next_segment()? {
        segments.push(seg);
    }

    if segments.len() != source.segment_count() {
        return Err(GenerationError::Codec(format!(
            "segment count mismatch: expected {}, yielded {}",
            source.segment_count(),
            segments.len()
        )));
    }

    // Verify ascending order
    for window in segments.windows(2) {
        if window[0].kind.as_u16() >= window[1].kind.as_u16() {
            return Err(GenerationError::Codec(format!(
                "segments out of order: {:?} ({}) >= {:?} ({})",
                window[0].kind,
                window[0].kind.as_u16(),
                window[1].kind,
                window[1].kind.as_u16()
            )));
        }
    }

    let segment_count = u16::try_from(segments.len())
        .map_err(|_| GenerationError::Codec(format!("too many segments: {}", segments.len())))?;
    // reason: DIRECTORY_ENTRY_LEN is 48
    #[allow(clippy::cast_possible_truncation)]
    let directory_length = u64::from(segment_count) * (DIRECTORY_ENTRY_LEN as u64);
    // reason: HEADER_LEN is 64
    #[allow(clippy::cast_possible_truncation)]
    let data_offset = align_up((HEADER_LEN as u64) + directory_length, 8);

    // reason: directory_length fits usize for test payloads
    #[allow(clippy::cast_possible_truncation)]
    let dir_cap = directory_length as usize;
    let mut dir_bytes = Vec::with_capacity(dir_cap);
    let mut data_bytes = Vec::new();
    let mut cursor = data_offset;
    let mut entries_meta = Vec::new();

    for seg in &segments {
        let align = u64::from(seg.alignment);
        let padded_off = align_up(cursor, align);
        // reason: pad fits usize
        #[allow(clippy::cast_possible_truncation)]
        let pad = (padded_off - cursor) as usize;
        data_bytes.resize(data_bytes.len() + pad, 0);
        let offset = padded_off;
        // reason: seg.bytes.len() fits u64
        #[allow(clippy::cast_possible_truncation)]
        let length = seg.bytes.len() as u64;
        let crc = crc32fast::hash(&seg.bytes);
        let element_count = if seg.element_width > 0 {
            // reason: element count fits u32
            #[allow(clippy::cast_possible_truncation)]
            let count = (length / u64::from(seg.element_width)) as u32;
            count
        } else {
            0
        };
        entries_meta.push((seg.kind, offset, length, crc, element_count));
        data_bytes.extend_from_slice(&seg.bytes);
        cursor = offset + length;
    }

    for (seg, meta) in segments.iter().zip(entries_meta.iter()) {
        let (kind, offset, length, crc, element_count) = *meta;
        write_u16(&mut dir_bytes, kind.as_u16());
        write_u16(&mut dir_bytes, seg.encoding_version);
        write_u16(&mut dir_bytes, seg.flags);
        write_u16(&mut dir_bytes, seg.alignment);
        write_u64(&mut dir_bytes, offset);
        write_u64(&mut dir_bytes, length);
        write_u32(&mut dir_bytes, seg.element_width);
        write_u32(&mut dir_bytes, element_count);
        write_u32(&mut dir_bytes, crc);
        write_u32(&mut dir_bytes, 0); // reserved_a
        write_u64(&mut dir_bytes, 0); // reserved_b
    }

    let directory_crc = crc32fast::hash(&dir_bytes);
    let flags: u8 = u8::from(preserves_ids);
    let header_layout_flags = layout_flags::from_companion_segments(
        segments
            .iter()
            .any(|s| s.kind == SegmentKind::NodeLabelMembership),
        segments
            .iter()
            .any(|s| s.kind == SegmentKind::ColumnRowPresence),
        segments.iter().any(|s| s.kind == SegmentKind::ColumnRowNull),
    );

    // reason: data_offset + data_bytes.len() fits usize
    #[allow(clippy::cast_possible_truncation)]
    let out_cap = (data_offset as usize) + data_bytes.len() + 4;
    let mut out = Vec::with_capacity(out_cap);
    out.extend_from_slice(&MAGIC);
    out.push(FORMAT_VERSION_V5);
    out.push(flags);
    // reason: HEADER_LEN is 64
    #[allow(clippy::cast_possible_truncation)]
    write_u16(&mut out, HEADER_LEN as u16);
    write_u16(&mut out, segment_count);
    // reason: DIRECTORY_ENTRY_LEN is 48
    #[allow(clippy::cast_possible_truncation)]
    write_u16(&mut out, DIRECTORY_ENTRY_LEN as u16);
    write_u32(&mut out, header_layout_flags);
    // reason: HEADER_LEN is 64
    #[allow(clippy::cast_possible_truncation)]
    write_u64(&mut out, HEADER_LEN as u64); // directory_offset
    write_u64(&mut out, directory_length);
    write_u64(&mut out, data_offset);
    write_u64(&mut out, total_nodes);
    write_u64(&mut out, total_edges);
    write_u32(&mut out, directory_crc);
    write_u32(&mut out, 0); // reserved
    debug_assert_eq!(out.len(), HEADER_LEN);

    out.extend_from_slice(&dir_bytes);
    // reason: data_offset fits usize
    #[allow(clippy::cast_possible_truncation)]
    let target_data_off = data_offset as usize;
    while out.len() < target_data_off {
        out.push(0);
    }
    out.extend_from_slice(&data_bytes);
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());

    Ok(out)
}

#[cfg(test)]
fn write_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
#[cfg(test)]
fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
#[cfg(test)]
fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
