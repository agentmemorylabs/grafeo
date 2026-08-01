//! V5 payload assembler for bounded emission (G-EM0.5b Phase 0).
//!
//! Consumes ordered [`SegmentDescriptor`]s and emits the 64-byte header +
//! 48-byte directory entries + streams each body (from memory or spool) +
//! trailing CRC-32, **byte-identical** to the eager `serialize_v5` layout.
//!
//! Directory geometry is computed from descriptors only after every segment's
//! exact length/crc is known (true multi-pass, packet §6).

// All casts in this module convert wire-width fields (u16/u32/u64) that are
// bounded by the v5 format by construction.
#![allow(clippy::cast_possible_truncation)]

use super::descriptor::SegmentDescriptor;
use crate::graph::compact::generation::error::GenerationError;
use crate::graph::compact::mapped::{DIRECTORY_ENTRY_LEN, FORMAT_VERSION_V5, HEADER_LEN};
use crate::graph::compact::section_v5::align_up;

const MAGIC: [u8; 4] = *b"GCST";

/// Assembles a complete v5 payload from ordered segment descriptors.
///
/// The output is byte-identical to `serialize_v5_with_string_order` when
/// given the same segments in the same order.
pub struct V5PayloadAssembler {
    total_nodes: u64,
    total_edges: u64,
    preserves_ids: bool,
}

impl V5PayloadAssembler {
    /// Creates an assembler for one generation build.
    #[must_use]
    pub fn new(total_nodes: u64, total_edges: u64, preserves_ids: bool) -> Self {
        Self {
            total_nodes,
            total_edges,
            preserves_ids,
        }
    }

    /// Assembles the complete v5 payload byte vector.
    ///
    /// Descriptors must be pre-sorted by `kind.as_u16()` ascending.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::WireWidthOverflow`] when segment count
    /// exceeds `u16::MAX`, or [`GenerationError::Io`] when a spilled body
    /// cannot be read.
    pub fn assemble(&self, descriptors: &[SegmentDescriptor]) -> Result<Vec<u8>, GenerationError> {
        let segment_count =
            u16::try_from(descriptors.len()).map_err(|_| GenerationError::WireWidthOverflow {
                what: "segment_count",
                count: descriptors.len() as u64,
                max: u64::from(u16::MAX),
            })?;

        #[allow(clippy::cast_possible_truncation)]
        let directory_length = u64::from(segment_count) * (DIRECTORY_ENTRY_LEN as u64);
        #[allow(clippy::cast_possible_truncation)]
        let data_offset = align_up((HEADER_LEN as u64) + directory_length, 8);

        // ── Pass 1: compute offsets from descriptor metadata ──────────
        let mut cursor = data_offset;
        let mut entries: Vec<(u64, u64)> = Vec::with_capacity(descriptors.len()); // (offset, length)
        for desc in descriptors {
            let align = u64::from(desc.alignment);
            let padded_off = align_up(cursor, align);
            entries.push((padded_off, desc.length));
            cursor = padded_off + desc.length;
        }

        // ── Directory bytes ───────────────────────────────────────────
        let mut dir_bytes = Vec::with_capacity(directory_length as usize);
        for (desc, &(offset, length)) in descriptors.iter().zip(entries.iter()) {
            write_u16(&mut dir_bytes, desc.kind.as_u16());
            write_u16(&mut dir_bytes, desc.encoding_version);
            write_u16(&mut dir_bytes, desc.flags);
            write_u16(&mut dir_bytes, desc.alignment);
            write_u64(&mut dir_bytes, offset);
            write_u64(&mut dir_bytes, length);
            write_u32(&mut dir_bytes, desc.element_width);
            write_u32(&mut dir_bytes, desc.element_count);
            write_u32(&mut dir_bytes, desc.crc);
            write_u32(&mut dir_bytes, 0); // reserved_a
            write_u64(&mut dir_bytes, 0); // reserved_b
        }
        let directory_crc = crc32fast::hash(&dir_bytes);

        // ── Header ────────────────────────────────────────────────────
        let flags: u8 = u8::from(self.preserves_ids);
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC);
        out.push(FORMAT_VERSION_V5);
        out.push(flags);
        #[allow(clippy::cast_possible_truncation)]
        write_u16(&mut out, HEADER_LEN as u16);
        write_u16(&mut out, segment_count);
        #[allow(clippy::cast_possible_truncation)]
        write_u16(&mut out, DIRECTORY_ENTRY_LEN as u16);
        write_u32(&mut out, 0); // layout_flags
        #[allow(clippy::cast_possible_truncation)]
        write_u64(&mut out, HEADER_LEN as u64); // directory_offset
        write_u64(&mut out, directory_length);
        write_u64(&mut out, data_offset);
        write_u64(&mut out, self.total_nodes);
        write_u64(&mut out, self.total_edges);
        write_u32(&mut out, directory_crc);
        write_u32(&mut out, 0); // reserved
        debug_assert_eq!(out.len(), HEADER_LEN);

        // ── Directory + padding ───────────────────────────────────────
        out.extend_from_slice(&dir_bytes);
        #[allow(clippy::cast_possible_truncation)]
        let target = data_offset as usize;
        while out.len() < target {
            out.push(0);
        }

        // ── Segment bodies (streamed from resident or spool) ──────────
        for (desc, &(offset, _length)) in descriptors.iter().zip(entries.iter()) {
            // Alignment padding between segments.
            #[allow(clippy::cast_possible_truncation)]
            let pad = (offset as usize).saturating_sub(out.len());
            out.resize(out.len() + pad, 0);
            desc.body.stream(&mut |chunk| {
                out.extend_from_slice(chunk);
                Ok(())
            })?;
        }

        // ── Trailing CRC ──────────────────────────────────────────────
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        Ok(out)
    }
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
