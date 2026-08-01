//! Bounded emission stage: metadata + directories (G-EM0.5b Phase 2b).
//!
//! Drives the v5 Metadata, NodeTableDirectory, RelTableDirectory,
//! ColumnDirectory, ColumnBlockIndex, and ColumnBodies segments from the
//! bounded pass outputs (node schema, rel keys, column geometries) into
//! spool sinks. Layout is byte-exact with `emit_canonical_descriptors`
//! (the D0.1 parity anchor); only the *source* of the values differs
//! (bounded pass outputs, not a heap `CompactStore`).

/// Local LE writers (byte-exact with section_v5 helpers).
/// Writes a little-endian `u16`.
pub fn w16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
/// Writes a little-endian `u32`.
pub fn w32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
/// Writes a little-endian `u64`.
pub fn w64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Codec discriminant codes, mirroring `codec_disc` (and `value_type_code`,
/// which is identical). Derived from the column geometry family (D0.8.6),
/// matching what the eager path derives from the `ColumnCodec`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecKind {
    /// Bit-packed unsigned ints.
    BitPacked,
    /// Dictionary-encoded strings.
    Dict,
    /// Boolean bitmap.
    Bitmap,
    /// Native signed i64.
    RawI64,
    /// Native f64.
    Float64,
    /// Float32 vectors.
    Float32Vector,
}

impl CodecKind {
    /// Segment `disc` code (== `value_type` code; they are the same function).
    #[must_use]
    pub fn disc(self) -> u16 {
        match self {
            Self::BitPacked => 0,
            Self::Dict => 1,
            Self::Bitmap => 2,
            Self::Float64 => 4,
            Self::Float32Vector => 5,
            Self::RawI64 => 6,
        }
    }

    /// Segment `value_type` code (identical to `disc`, per production
    /// `value_type_code`).
    #[must_use]
    pub fn value_type(self) -> u16 {
        self.disc()
    }
}
