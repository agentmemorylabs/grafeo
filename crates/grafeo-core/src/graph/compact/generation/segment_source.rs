//! Bounded V5SegmentSource trait and payload assembly helper (G-EM0.W0-A3).

use super::error::GenerationError;
use super::strings::GlobalStringDictionary;
use super::v5_emitter::emit_v5_segments;
use crate::graph::compact::CompactStore;
use crate::graph::compact::mapped::{
    DIRECTORY_ENTRY_LEN, FORMAT_VERSION_V5, HEADER_LEN, SegmentKind,
};
use crate::graph::compact::section_v5::align_up;

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

/// Concrete `V5SegmentSource` backed by a heap-built `CompactStore` and `GlobalStringDictionary`.
pub struct CompactV5SegmentSource {
    segments: Vec<V5Segment>,
    cursor: usize,
}

impl CompactV5SegmentSource {
    /// Constructs a `CompactV5SegmentSource` by generating all segments in ascending kind order.
    ///
    /// # Errors
    ///
    /// Returns `GenerationError` if segment codec generation fails.
    pub fn new(
        store: &CompactStore,
        global_strings: &GlobalStringDictionary,
    ) -> Result<Self, GenerationError> {
        let segments = emit_v5_segments(store, global_strings)?;
        Ok(Self {
            segments,
            cursor: 0,
        })
    }
}

impl V5SegmentSource for CompactV5SegmentSource {
    fn next_segment(&mut self) -> Result<Option<V5Segment>, GenerationError> {
        if self.cursor >= self.segments.len() {
            Ok(None)
        } else {
            let seg = self.segments[self.cursor].clone();
            self.cursor += 1;
            Ok(Some(seg))
        }
    }

    fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

/// Assemble a full v5 payload byte vector from a `V5SegmentSource`.
///
/// # Errors
///
/// Returns `GenerationError` on segment streaming or geometry overflow.
pub fn assemble_v5_payload_from_source<S: V5SegmentSource>(
    source: &mut S,
    total_nodes: u64,
    total_edges: u64,
    preserves_ids: bool,
) -> Result<Vec<u8>, GenerationError> {
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
    let directory_length = u64::from(segment_count) * DIRECTORY_ENTRY_LEN as u64;
    let data_offset = align_up(HEADER_LEN as u64 + directory_length, 8);

    let mut dir_bytes = Vec::with_capacity(directory_length as usize);
    let mut data_bytes = Vec::new();
    let mut cursor = data_offset;
    let mut entries_meta = Vec::new();

    for seg in &segments {
        let align = u64::from(seg.alignment);
        let padded_off = align_up(cursor, align);
        let pad = (padded_off - cursor) as usize;
        data_bytes.resize(data_bytes.len() + pad, 0);
        let offset = padded_off;
        let length = seg.bytes.len() as u64;
        let crc = crc32fast::hash(&seg.bytes);
        let element_count = if seg.element_width > 0 {
            (length / u64::from(seg.element_width)) as u32
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

    let mut out = Vec::with_capacity((data_offset as usize) + data_bytes.len() + 4);
    out.extend_from_slice(&MAGIC);
    out.push(FORMAT_VERSION_V5);
    out.push(flags);
    write_u16(&mut out, HEADER_LEN as u16);
    write_u16(&mut out, segment_count);
    write_u16(&mut out, DIRECTORY_ENTRY_LEN as u16);
    write_u32(&mut out, 0); // layout_flags
    write_u64(&mut out, HEADER_LEN as u64); // directory_offset
    write_u64(&mut out, directory_length);
    write_u64(&mut out, data_offset);
    write_u64(&mut out, total_nodes);
    write_u64(&mut out, total_edges);
    write_u32(&mut out, directory_crc);
    write_u32(&mut out, 0); // reserved
    debug_assert_eq!(out.len(), HEADER_LEN);

    out.extend_from_slice(&dir_bytes);
    while out.len() < data_offset as usize {
        out.push(0);
    }
    out.extend_from_slice(&data_bytes);
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());

    Ok(out)
}

fn write_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
