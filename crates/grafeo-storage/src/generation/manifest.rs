//! Dual-slot generation manifest (G-EM0.W0-B, Module 2).
//!
//! One 8192-byte `manifest.bin` with two independent 4096-byte slots at
//! offsets 0 and 4096. There is NO selector byte: selection is by
//! highest publication sequence among fully-valid slots.
//!
//! Slot layout (all little-endian, W0 §11):
//!
//! | Offset | Size | Field |
//! |--------|------|-------|
//! | 0..4 | 4 | magic `GMNF` |
//! | 4..6 | 2 | schema_version = 1 |
//! | 6..8 | 2 | flags = 0 |
//! | 8..80 | 72 | publication/parent/epoch/txn/WAL/generation counters |
//! | 80..94 | 14 | format versions + string lengths + reserved |
//! | 94..126 | 32 | generation SHA-256 |
//! | 126..254 | 128 | generation_id, zero-padded UTF-8 |
//! | 254..382 | 128 | parent_generation_id, zero-padded UTF-8 |
//! | 382..894 | 512 | root-relative generation_path, zero-padded UTF-8 |
//! | 894..4092 | 3198 | reserved zero (v1) |
//! | 4092..4096 | 4 | CRC32 over bytes 0..4092 |

use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

/// Size of one manifest slot in bytes.
pub const SLOT_SIZE: usize = 4096;
/// Size of the whole manifest file (two slots).
pub const MANIFEST_SIZE: usize = 2 * SLOT_SIZE;
/// Slot magic bytes.
pub const SLOT_MAGIC: &[u8; 4] = b"GMNF";
/// Schema version written by this module.
pub const SCHEMA_VERSION: u16 = 1;
/// CompactStore format version recorded in slots.
pub const COMPACT_STORE_FORMAT_VERSION: u16 = 5;

const GEN_ID_CAP: usize = 128;
const PARENT_GEN_ID_CAP: usize = 128;
const GEN_PATH_CAP: usize = 512;

/// One decoded manifest slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSlot {
    /// Monotonic publication sequence.
    pub publication_sequence: u64,
    /// Sequence of the parent generation (0 for genesis).
    pub parent_publication_sequence: u64,
    /// Overlay epoch captured at publication.
    pub overlay_epoch: u64,
    /// Last committed transaction ID captured at publication.
    pub transaction_id: u64,
    /// WAL log sequence of the replay cursor.
    pub wal_log_sequence: u64,
    /// WAL byte offset of the replay cursor.
    pub wal_byte_offset: u64,
    /// Byte length of the immutable generation file.
    pub generation_length: u64,
    /// Node count recorded in the generation container.
    pub node_count: u64,
    /// Edge count recorded in the generation container.
    pub edge_count: u64,
    /// Outer container format version.
    pub outer_container_format_version: u32,
    /// CompactStore format version (5).
    pub compact_store_format_version: u16,
    /// SHA-256 of the generation file bytes.
    pub generation_sha256: [u8; 32],
    /// Generation identifier (UTF-8).
    pub generation_id: String,
    /// Parent generation identifier (empty for genesis).
    pub parent_generation_id: String,
    /// Root-relative generation path (e.g. `generations/g-....grafeo`).
    pub generation_path: String,
}

/// Errors from manifest operations.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// Slot magic mismatch.
    #[error("slot {slot}: bad magic")]
    BadMagic {
        /// Slot index (0 or 1).
        slot: usize,
    },
    /// Unknown schema version.
    #[error("slot {slot}: unknown schema version {version}")]
    UnknownVersion {
        /// Slot index (0 or 1).
        slot: usize,
        /// Version read from the slot.
        version: u16,
    },
    /// CRC mismatch.
    #[error("slot {slot}: CRC mismatch (stored={stored:#x}, computed={computed:#x})")]
    CrcMismatch {
        /// Slot index (0 or 1).
        slot: usize,
        /// CRC stored in the slot.
        stored: u32,
        /// CRC computed over bytes 0..4092.
        computed: u32,
    },
    /// Non-canonical encoding (padding, lengths, UTF-8, flags, versions).
    #[error("slot {slot}: non-canonical encoding ({detail})")]
    NonCanonical {
        /// Slot index (0 or 1).
        slot: usize,
        /// Human-readable cause.
        detail: String,
    },
    /// Generation path is absolute or contains traversal.
    #[error("slot {slot}: generation path is absolute or contains traversal")]
    BadPath {
        /// Slot index (0 or 1).
        slot: usize,
    },
    /// Both slots are invalid.
    #[error("both slots invalid: slot0={slot0_cause}, slot1={slot1_cause}")]
    BothInvalid {
        /// Cause for slot 0.
        slot0_cause: String,
        /// Cause for slot 1.
        slot1_cause: String,
    },
    /// Underlying I/O error.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// Per-slot decode result: `Ok(slot)` or `Err(cause string)`.
type SlotResult = std::result::Result<ManifestSlot, String>;

/// Decode and validate one slot region.
fn decode_slot(bytes: &[u8], slot: usize) -> SlotResult {
    let cause = |detail: String| format!("slot {slot}: {detail}");

    if bytes.len() != SLOT_SIZE {
        return Err(cause(format!(
            "region length {} != {SLOT_SIZE}",
            bytes.len()
        )));
    }
    if &bytes[0..4] != SLOT_MAGIC {
        return Err(cause("bad magic".to_string()));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != SCHEMA_VERSION {
        return Err(format!("unknown schema version {version}"));
    }
    let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
    if flags != 0 {
        return Err(cause(format!("flags {flags} != 0")));
    }
    let stored_crc = u32::from_le_bytes(bytes[4092..4096].try_into().expect("4 bytes"));
    let computed_crc = crc32fast::hash(&bytes[0..4092]);
    if stored_crc != computed_crc {
        return Err(cause(format!(
            "CRC mismatch (stored={stored_crc:#x}, computed={computed_crc:#x})"
        )));
    }
    for (i, b) in bytes[894..4092].iter().enumerate() {
        if *b != 0 {
            return Err(cause(format!(
                "reserved byte at offset {} nonzero",
                894 + i
            )));
        }
    }

    let gen_id_len = u16::from_le_bytes([bytes[86], bytes[87]]) as usize;
    let parent_len = u16::from_le_bytes([bytes[88], bytes[89]]) as usize;
    let path_len = u16::from_le_bytes([bytes[90], bytes[91]]) as usize;
    let reserved16 = u16::from_le_bytes([bytes[92], bytes[93]]);
    if reserved16 != 0 {
        return Err(cause(format!("reserved u16 {reserved16} != 0")));
    }
    if gen_id_len > GEN_ID_CAP {
        return Err(cause(format!(
            "generation_id_len {gen_id_len} exceeds {GEN_ID_CAP}"
        )));
    }
    if parent_len > PARENT_GEN_ID_CAP {
        return Err(cause(format!(
            "parent_generation_id_len {parent_len} exceeds {PARENT_GEN_ID_CAP}"
        )));
    }
    if path_len > GEN_PATH_CAP {
        return Err(cause(format!(
            "generation_path_len {path_len} exceeds {GEN_PATH_CAP}"
        )));
    }

    let id_region = &bytes[126..254];
    check_zero_padding(id_region, gen_id_len, "generation_id").map_err(&cause)?;
    let parent_region = &bytes[254..382];
    check_zero_padding(parent_region, parent_len, "parent_generation_id").map_err(&cause)?;
    let path_region = &bytes[382..894];
    check_zero_padding(path_region, path_len, "generation_path").map_err(&cause)?;

    let generation_id = decode_utf8(&id_region[..gen_id_len], "generation_id").map_err(&cause)?;
    let parent_generation_id =
        decode_utf8(&parent_region[..parent_len], "parent_generation_id").map_err(&cause)?;
    let generation_path =
        decode_utf8(&path_region[..path_len], "generation_path").map_err(&cause)?;

    if is_absolute_or_traversal(&generation_path) {
        return Err(cause(format!("generation path {generation_path:?}")));
    }

    let mut sha = [0u8; 32];
    sha.copy_from_slice(&bytes[94..126]);

    Ok(ManifestSlot {
        publication_sequence: u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes")),
        parent_publication_sequence: u64::from_le_bytes(bytes[16..24].try_into().expect("8 bytes")),
        overlay_epoch: u64::from_le_bytes(bytes[24..32].try_into().expect("8 bytes")),
        transaction_id: u64::from_le_bytes(bytes[32..40].try_into().expect("8 bytes")),
        wal_log_sequence: u64::from_le_bytes(bytes[40..48].try_into().expect("8 bytes")),
        wal_byte_offset: u64::from_le_bytes(bytes[48..56].try_into().expect("8 bytes")),
        generation_length: u64::from_le_bytes(bytes[56..64].try_into().expect("8 bytes")),
        node_count: u64::from_le_bytes(bytes[64..72].try_into().expect("8 bytes")),
        edge_count: u64::from_le_bytes(bytes[72..80].try_into().expect("8 bytes")),
        outer_container_format_version: u32::from_le_bytes(
            bytes[80..84].try_into().expect("4 bytes"),
        ),
        compact_store_format_version: u16::from_le_bytes([bytes[84], bytes[85]]),
        generation_sha256: sha,
        generation_id,
        parent_generation_id,
        generation_path,
    })
}

fn check_zero_padding(
    region: &[u8],
    declared_len: usize,
    name: &str,
) -> std::result::Result<(), String> {
    for (i, b) in region[declared_len..].iter().enumerate() {
        if *b != 0 {
            return Err(format!(
                "{name} padding byte at region offset {} nonzero",
                declared_len + i
            ));
        }
    }
    Ok(())
}

fn decode_utf8(bytes: &[u8], name: &str) -> std::result::Result<String, String> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|e| format!("{name} not valid UTF-8: {e}"))
}

/// True when the path is absolute or contains `..` traversal.
fn is_absolute_or_traversal(path: &str) -> bool {
    if path.is_empty() || path.starts_with('/') {
        return true;
    }
    path.split('/').any(|comp| comp == ".." || comp == ".")
}

/// Encode a slot into its canonical 4096-byte representation.
fn encode_slot(slot: &ManifestSlot) -> std::result::Result<[u8; SLOT_SIZE], String> {
    let mut buf = [0u8; SLOT_SIZE];
    buf[0..4].copy_from_slice(SLOT_MAGIC);
    buf[4..6].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
    buf[6..8].copy_from_slice(&0u16.to_le_bytes());
    buf[8..16].copy_from_slice(&slot.publication_sequence.to_le_bytes());
    buf[16..24].copy_from_slice(&slot.parent_publication_sequence.to_le_bytes());
    buf[24..32].copy_from_slice(&slot.overlay_epoch.to_le_bytes());
    buf[32..40].copy_from_slice(&slot.transaction_id.to_le_bytes());
    buf[40..48].copy_from_slice(&slot.wal_log_sequence.to_le_bytes());
    buf[48..56].copy_from_slice(&slot.wal_byte_offset.to_le_bytes());
    buf[56..64].copy_from_slice(&slot.generation_length.to_le_bytes());
    buf[64..72].copy_from_slice(&slot.node_count.to_le_bytes());
    buf[72..80].copy_from_slice(&slot.edge_count.to_le_bytes());
    buf[80..84].copy_from_slice(&slot.outer_container_format_version.to_le_bytes());
    buf[84..86].copy_from_slice(&slot.compact_store_format_version.to_le_bytes());

    let gen_id = slot.generation_id.as_bytes();
    if gen_id.len() > GEN_ID_CAP {
        return Err(format!(
            "generation_id length {} exceeds {GEN_ID_CAP}",
            gen_id.len()
        ));
    }
    let parent = slot.parent_generation_id.as_bytes();
    if parent.len() > PARENT_GEN_ID_CAP {
        return Err(format!(
            "parent_generation_id length {} exceeds {PARENT_GEN_ID_CAP}",
            parent.len()
        ));
    }
    let path = slot.generation_path.as_bytes();
    if path.len() > GEN_PATH_CAP {
        return Err(format!(
            "generation_path length {} exceeds {GEN_PATH_CAP}",
            path.len()
        ));
    }
    if is_absolute_or_traversal(&slot.generation_path) {
        return Err(format!("generation path {:?}", slot.generation_path));
    }

    let gen_id_len = u16::try_from(gen_id.len()).expect("capped at 128");
    let parent_len = u16::try_from(parent.len()).expect("capped at 128");
    let path_len = u16::try_from(path.len()).expect("capped at 512");
    buf[86..88].copy_from_slice(&gen_id_len.to_le_bytes());
    buf[88..90].copy_from_slice(&parent_len.to_le_bytes());
    buf[90..92].copy_from_slice(&path_len.to_le_bytes());
    buf[92..94].copy_from_slice(&0u16.to_le_bytes());

    buf[94..126].copy_from_slice(&slot.generation_sha256);
    buf[126..126 + gen_id.len()].copy_from_slice(gen_id);
    buf[254..254 + parent.len()].copy_from_slice(parent);
    buf[382..382 + path.len()].copy_from_slice(path);

    let crc = crc32fast::hash(&buf[0..4092]);
    buf[4092..4096].copy_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

/// Decode both slots independently, preserving per-slot causes.
///
/// # Errors
///
/// Returns [`ManifestError::Io`] when the file cannot be read.
pub fn read_both_slots(path: &Path) -> std::io::Result<[SlotResult; 2]> {
    let bytes = std::fs::read(path)?;
    if bytes.len() < MANIFEST_SIZE {
        let cause = format!("file length {} < {MANIFEST_SIZE}", bytes.len());
        return Ok([Err(cause.clone()), Err(cause)]);
    }
    let slot0 = decode_slot(&bytes[0..SLOT_SIZE], 0);
    let slot1 = decode_slot(&bytes[SLOT_SIZE..2 * SLOT_SIZE], 1);
    Ok([slot0, slot1])
}

/// Read and validate both slots. Returns the highest-sequence valid slot
/// index (0 or 1) and its decoded content, or `BothInvalid`.
///
/// # Errors
///
/// Returns [`ManifestError::Io`] for I/O failures and
/// [`ManifestError::BothInvalid`] when neither slot validates.
pub fn read_manifest(path: &Path) -> std::result::Result<(usize, ManifestSlot), ManifestError> {
    let [slot0, slot1] = read_both_slots(path)?;
    match (slot0, slot1) {
        (Ok(a), Ok(b)) => {
            if b.publication_sequence > a.publication_sequence {
                Ok((1, b))
            } else {
                Ok((0, a))
            }
        }
        (Ok(a), Err(_cause1)) => Ok((0, a)),
        (Err(_cause0), Ok(b)) => Ok((1, b)),
        (Err(cause0), Err(cause1)) => Err(ManifestError::BothInvalid {
            slot0_cause: cause0,
            slot1_cause: cause1,
        }),
    }
}

/// Encode and write one slot (full 4096 bytes) at the given offset.
/// Does NOT sync — the caller is responsible for sync ordering.
///
/// # Errors
///
/// Returns [`ManifestError::NonCanonical`] when the slot cannot be encoded
/// and [`ManifestError::Io`] for write failures.
pub fn write_slot(
    file: &mut File,
    slot_index: usize,
    slot: &ManifestSlot,
) -> std::result::Result<(), ManifestError> {
    let encoded = encode_slot(slot).map_err(|detail| ManifestError::NonCanonical {
        slot: slot_index,
        detail,
    })?;
    // reason: slot_index is 0 or 1, so the byte offset always fits u64
    #[allow(clippy::cast_possible_truncation)]
    let offset = (slot_index * SLOT_SIZE) as u64;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&encoded)?;
    Ok(())
}

/// Encode one slot into its canonical 4096-byte representation (in memory).
///
/// # Errors
///
/// Returns a cause string when the slot cannot be encoded (oversized
/// strings or a non-relative generation path).
pub fn encode_slot_bytes(slot: &ManifestSlot) -> std::result::Result<[u8; SLOT_SIZE], String> {
    encode_slot(slot)
}

/// Create a fresh 8192-byte manifest with two empty (all-zero, invalid)
/// slots. Fails if the path already exists. Does NOT sync.
///
/// # Errors
///
/// Returns [`ManifestError::Io`] for I/O failures.
pub fn create_manifest(path: &Path) -> std::result::Result<(), ManifestError> {
    let mut file = File::create_new(path)?;
    file.write_all(&[0u8; MANIFEST_SIZE])?;
    Ok(())
}

/// Determine which slot index is currently inactive (for publication
/// writes). The inactive slot is the valid slot with the lower sequence,
/// or the only other slot when exactly one is valid. When both slots are
/// invalid (fresh manifest), returns 0 for genesis publication.
///
/// # Errors
///
/// Returns [`ManifestError::Io`] for I/O failures.
pub fn inactive_slot_index(path: &Path) -> std::result::Result<usize, ManifestError> {
    let [slot0, slot1] = read_both_slots(path)?;
    match (slot0, slot1) {
        (Ok(a), Ok(b)) => {
            if b.publication_sequence > a.publication_sequence {
                Ok(0)
            } else {
                Ok(1)
            }
        }
        (Ok(_), Err(_)) => Ok(1),
        (Err(_), Ok(_)) => Ok(0),
        (Err(_), Err(_)) => Ok(0),
    }
}

#[cfg(test)]
#[path = "tests/manifest_tests.rs"]
mod tests;
