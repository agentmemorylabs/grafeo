//! V5/V6 payload assembler for bounded emission (G-EM0.5b Phase 0, G4).
//!
//! Consumes ordered [`SegmentDescriptor`]s and emits the 64-byte header +
//! 48-byte directory entries + streams each body (from memory or spool) +
//! trailing CRC-32. A v5 payload is **byte-identical** to the eager
//! `serialize_v5` layout; a v6 payload differs only in the header version
//! byte and the directory entry layout (`mapped::payload_version`).
//!
//! Directory geometry is computed from descriptors only after every segment's
//! exact length/crc is known (true multi-pass, packet §6).

// All casts in this module convert wire-width fields (u16/u32/u64) that are
// bounded by the v5 format by construction.
#![allow(clippy::cast_possible_truncation)]

use super::descriptor::SegmentDescriptor;
use crate::graph::compact::generation::error::GenerationError;
use crate::graph::compact::mapped::{
    DIRECTORY_ENTRY_LEN, DirectoryEntryFields, HEADER_LEN, PayloadVersion, SegmentKind,
    layout_flags, write_directory_entry,
};
use crate::graph::compact::section_v5::align_up;

const MAGIC: [u8; 4] = *b"GCST";

/// Assembles a complete v5 payload from ordered segment descriptors.
///
/// The output is byte-identical to `serialize_v5_with_string_order` when
/// given the same segments in the same order.
#[derive(Debug, Clone, Copy)]
pub struct V5PayloadAssembler {
    total_nodes: u64,
    total_edges: u64,
    preserves_ids: bool,
    layout_flags: u32,
    version: PayloadVersion,
}

impl V5PayloadAssembler {
    /// Creates an assembler for one generation build.
    #[must_use]
    pub fn new(total_nodes: u64, total_edges: u64, preserves_ids: bool) -> Self {
        Self {
            total_nodes,
            total_edges,
            preserves_ids,
            layout_flags: 0,
            version: PayloadVersion::V5,
        }
    }

    /// Sets the payload version written (default v5).
    ///
    /// The caller must have encoded the `ColumnBlockIndex` segment in the
    /// same version's layout; [`Self::stream_to`] checks its record width.
    #[must_use]
    pub fn with_payload_version(mut self, version: PayloadVersion) -> Self {
        self.version = version;
        self
    }

    /// The payload version this assembler writes.
    #[must_use]
    pub fn payload_version(&self) -> PayloadVersion {
        self.version
    }

    /// Sets the v5 header layout flags (D0.8.0 extended-payload contract).
    #[must_use]
    pub fn with_layout_flags(mut self, layout_flags: u32) -> Self {
        self.layout_flags = layout_flags;
        self
    }

    /// Computes layout flags from companion segment kinds in `descriptors`.
    #[must_use]
    pub fn layout_flags_from_descriptors(descriptors: &[SegmentDescriptor]) -> u32 {
        let mut has_membership = false;
        let mut has_presence = false;
        let mut has_null = false;
        for desc in descriptors {
            match desc.kind {
                SegmentKind::NodeLabelMembership => has_membership = true,
                SegmentKind::ColumnRowPresence => has_presence = true,
                SegmentKind::ColumnRowNull => has_null = true,
                _ => {}
            }
        }
        layout_flags::from_companion_segments(has_membership, has_presence, has_null)
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
        let len = self.payload_len(descriptors)?;
        let cap = usize::try_from(len).map_err(|_| GenerationError::WireWidthOverflow {
            what: "payload_len",
            count: len,
            max: u64::MAX,
        })?;
        let mut out = Vec::with_capacity(cap);
        self.stream_to(descriptors, &mut out)?;
        Ok(out)
    }

    /// Computes the exact total payload byte length without materializing it.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::WireWidthOverflow`] when segment count
    /// exceeds `u16::MAX`, or [`GenerationError::Io`] when a spilled body
    /// length cannot be stat'd.
    pub fn payload_len(&self, descriptors: &[SegmentDescriptor]) -> Result<u64, GenerationError> {
        if descriptors.len() > usize::from(u16::MAX) {
            return Err(GenerationError::WireWidthOverflow {
                what: "segment_count",
                count: descriptors.len() as u64,
                max: u64::from(u16::MAX),
            });
        }
        let directory_length = descriptors.len() as u64 * (DIRECTORY_ENTRY_LEN as u64);
        let data_offset = align_up((HEADER_LEN as u64) + directory_length, 8);
        let mut cursor = data_offset;
        for desc in descriptors {
            let align = u64::from(desc.alignment);
            let padded_off = align_up(cursor, align);
            cursor = padded_off + desc.length;
        }
        Ok(cursor + 4) // trailing CRC-32
    }

    /// Streams the assembled payload to `sink` in bounded chunks.
    ///
    /// Segment bodies are read through [`SegmentBody::stream`], so spilled
    /// bodies never become fully resident. The output is byte-identical to
    /// [`Self::assemble`].
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::WireWidthOverflow`] when segment count
    /// exceeds `u16::MAX`, or [`GenerationError::Io`] on body read / sink
    /// write failure.
    pub fn stream_to(
        &self,
        descriptors: &[SegmentDescriptor],
        sink: &mut dyn std::io::Write,
    ) -> Result<(), GenerationError> {
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
        let mut entries: Vec<(u64, u64)> = Vec::with_capacity(descriptors.len());
        for desc in descriptors {
            let align = u64::from(desc.alignment);
            let padded_off = align_up(cursor, align);
            entries.push((padded_off, desc.length));
            cursor = padded_off + desc.length;
        }

        // Everything written flows through a CRC hasher so the trailing CRC
        // covers header + directory + padding + bodies exactly as `assemble`.
        let mut hasher = crc32fast::Hasher::new();
        let mut write = |buf: &[u8]| -> Result<(), GenerationError> {
            hasher.update(buf);
            sink.write_all(buf)
                .map_err(|e| GenerationError::Io(format!("assemble stream: {e}")))
        };

        // ── Directory bytes ───────────────────────────────────────────
        let mut dir_bytes = Vec::with_capacity(directory_length as usize);
        for (desc, &(offset, length)) in descriptors.iter().zip(entries.iter()) {
            if desc.kind == SegmentKind::ColumnBlockIndex
                && desc.element_width as usize != self.version.block_index_record_len()
            {
                return Err(GenerationError::Codec(format!(
                    "ColumnBlockIndex record width {} does not match payload {:?} ({})",
                    desc.element_width,
                    self.version,
                    self.version.block_index_record_len()
                )));
            }
            write_directory_entry(
                &mut dir_bytes,
                self.version,
                &DirectoryEntryFields {
                    kind: desc.kind.as_u16(),
                    encoding_version: desc.encoding_version,
                    flags: desc.flags,
                    alignment: desc.alignment,
                    offset,
                    length,
                    element_width: desc.element_width,
                    element_count: desc.element_count,
                    crc32: desc.crc,
                },
            )?;
        }
        let directory_crc = crc32fast::hash(&dir_bytes);

        // ── Header ────────────────────────────────────────────────────
        let flags: u8 = u8::from(self.preserves_ids);
        let mut header = Vec::with_capacity(HEADER_LEN);
        header.extend_from_slice(&MAGIC);
        header.push(self.version.byte());
        header.push(flags);
        #[allow(clippy::cast_possible_truncation)]
        write_u16(&mut header, HEADER_LEN as u16);
        write_u16(&mut header, segment_count);
        #[allow(clippy::cast_possible_truncation)]
        write_u16(&mut header, DIRECTORY_ENTRY_LEN as u16);
        write_u32(&mut header, self.layout_flags);
        #[allow(clippy::cast_possible_truncation)]
        write_u64(&mut header, HEADER_LEN as u64); // directory_offset
        write_u64(&mut header, directory_length);
        write_u64(&mut header, data_offset);
        write_u64(&mut header, self.total_nodes);
        write_u64(&mut header, self.total_edges);
        write_u32(&mut header, directory_crc);
        write_u32(&mut header, 0); // reserved
        debug_assert_eq!(header.len(), HEADER_LEN);
        write(&header)?;

        // ── Directory + padding ───────────────────────────────────────
        write(&dir_bytes)?;
        #[allow(clippy::cast_possible_truncation)]
        let target = data_offset as usize;
        let written = HEADER_LEN + dir_bytes.len();
        if written < target {
            let zeros = vec![0u8; target - written];
            write(&zeros)?;
        }

        // ── Segment bodies (streamed from resident or spool) ──────────
        let mut out_len = target;
        for (desc, &(offset, _length)) in descriptors.iter().zip(entries.iter()) {
            #[allow(clippy::cast_possible_truncation)]
            let pad = (offset as usize).saturating_sub(out_len);
            if pad > 0 {
                let zeros = vec![0u8; pad];
                write(&zeros)?;
                out_len += pad;
            }
            desc.body.stream(&mut |chunk| {
                write(chunk)?;
                out_len += chunk.len();
                Ok(())
            })?;
        }

        // ── Trailing CRC (not covered by the hasher) ──────────────────
        let crc = hasher.finalize();
        sink.write_all(&crc.to_le_bytes())
            .map_err(|e| GenerationError::Io(format!("assemble trailer: {e}")))?;
        Ok(())
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
