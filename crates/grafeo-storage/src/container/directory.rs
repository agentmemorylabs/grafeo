//! Section directory: the index of sections within a `.grafeo` container.
//!
//! The directory occupies a single 4 KiB page at offset 0x3000 in the file.
//! It contains a count followed by fixed-size 32-byte entries, one per section.

use grafeo_common::storage::section::{SectionDirectoryEntry, SectionFlags, SectionType};
use grafeo_common::utils::error::{Error, Result};

/// Maximum number of sections in a directory (limited by 4 KiB page).
/// (4096 - 8 byte header) / 32 bytes per entry = 127 entries.
pub const MAX_SECTIONS: usize = 127;

/// Size of the section directory page in bytes.
pub const DIRECTORY_PAGE_SIZE: usize = 4096;

/// Byte offset of the section directory within the `.grafeo` file.
pub const DIRECTORY_OFFSET: u64 = 3 * 4096; // After FileHeader + 2 DbHeaders

/// Byte offset where section data begins (after all headers + directory).
pub const SECTION_DATA_OFFSET: u64 = 4 * 4096;

/// In-memory representation of the section directory.
#[derive(Debug, Clone)]
pub struct SectionDirectory {
    entries: Vec<SectionDirectoryEntry>,
}

impl SectionDirectory {
    /// Create an empty directory.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Number of sections in the directory.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the directory has no sections.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// All entries in the directory.
    #[must_use]
    pub fn entries(&self) -> &[SectionDirectoryEntry] {
        &self.entries
    }

    /// Find an entry by section type.
    #[must_use]
    pub fn find(&self, section_type: SectionType) -> Option<&SectionDirectoryEntry> {
        self.entries.iter().find(|e| e.section_type == section_type)
    }

    /// Add or replace an entry for the given section type.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory is full.
    pub fn upsert(&mut self, entry: SectionDirectoryEntry) -> Result<()> {
        if let Some(existing) = self
            .entries
            .iter_mut()
            .find(|e| e.section_type == entry.section_type)
        {
            *existing = entry;
        } else {
            if self.entries.len() >= MAX_SECTIONS {
                return Err(Error::Internal(format!(
                    "section directory is full ({MAX_SECTIONS} entries)"
                )));
            }
            self.entries.push(entry);
        }
        Ok(())
    }

    /// Remove an entry by section type. Returns true if found.
    pub fn remove(&mut self, section_type: SectionType) -> bool {
        let before = self.entries.len();
        self.entries.retain(|e| e.section_type != section_type);
        self.entries.len() < before
    }

    /// Serialize the directory to a fixed 4 KiB page.
    ///
    /// Layout:
    /// - Bytes 0-3: entry count (u32 LE)
    /// - Bytes 4-7: reserved (zero)
    /// - Bytes 8+: entries (32 bytes each)
    /// - Remaining: zero-padded to 4096 bytes
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; DIRECTORY_PAGE_SIZE];

        // Header: entry count
        // reason: MAX_SECTIONS is 127, so entries.len() always fits in u32
        #[allow(clippy::cast_possible_truncation)]
        let count = self.entries.len() as u32;
        buf[0..4].copy_from_slice(&count.to_le_bytes());
        // Bytes 4-7: reserved

        // Entries
        for (i, entry) in self.entries.iter().enumerate() {
            let offset = 8 + i * SectionDirectoryEntry::SIZE;
            write_entry(
                &mut buf[offset..offset + SectionDirectoryEntry::SIZE],
                entry,
            );
        }

        buf
    }

    /// Deserialize a directory from a 4 KiB page.
    ///
    /// # Errors
    ///
    /// Returns an error if the page is too short or contains invalid data.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            return Err(Error::Serialization(
                "section directory too short".to_string(),
            ));
        }

        let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if count > MAX_SECTIONS {
            return Err(Error::Serialization(format!(
                "section directory has {count} entries, max is {MAX_SECTIONS}"
            )));
        }

        let required = 8 + count * SectionDirectoryEntry::SIZE;
        if data.len() < required {
            return Err(Error::Serialization(format!(
                "section directory data too short: need {required} bytes, got {}",
                data.len()
            )));
        }

        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            let offset = 8 + i * SectionDirectoryEntry::SIZE;
            // Unknown optional section types are skipped so older binaries can
            // open files that carry newer optional indexes. Unknown required
            // types fail closed (see `read_entry`).
            if let Some(entry) = read_entry(&data[offset..offset + SectionDirectoryEntry::SIZE])? {
                entries.push(entry);
            }
        }

        Ok(Self { entries })
    }

    /// Compute CRC-32 of the serialized directory (used in DbHeader).
    #[must_use]
    pub fn checksum(&self) -> u32 {
        let bytes = self.to_bytes();
        crc32fast::hash(&bytes)
    }
}

impl Default for SectionDirectory {
    fn default() -> Self {
        Self::new()
    }
}

// ── Binary serialization for directory entries ──────────────────────

fn write_entry(buf: &mut [u8], entry: &SectionDirectoryEntry) {
    buf[0..4].copy_from_slice(&(entry.section_type as u32).to_le_bytes());
    buf[4] = entry.version;
    buf[5] = entry.flags.to_byte();
    buf[6..8].copy_from_slice(&[0, 0]); // reserved
    buf[8..16].copy_from_slice(&entry.offset.to_le_bytes());
    buf[16..24].copy_from_slice(&entry.length.to_le_bytes());
    buf[24..28].copy_from_slice(&entry.checksum.to_le_bytes());
    buf[28..32].copy_from_slice(&[0, 0, 0, 0]); // reserved
}

/// Parse one directory entry.
///
/// Returns `Ok(None)` when the type id is unknown and the entry is optional
/// (`flags.required == false`), so older readers can skip newer optional
/// sections. Returns an error when the type is unknown and required.
fn read_entry(buf: &[u8]) -> Result<Option<SectionDirectoryEntry>> {
    let type_val = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let flags = SectionFlags::from_byte(buf[5]);
    let section_type = match type_val {
        1 => SectionType::Catalog,
        2 => SectionType::LpgStore,
        3 => SectionType::RdfStore,
        4 => SectionType::CompactStore,
        5 => SectionType::OverlayDeletions,
        10 => SectionType::VectorStore,
        11 => SectionType::TextIndex,
        12 => SectionType::RdfRing,
        20 => SectionType::PropertyIndex,
        other => {
            if flags.required {
                return Err(Error::Serialization(format!(
                    "unknown required section type: {other}"
                )));
            }
            // Optional unknown: skip without failing open.
            return Ok(None);
        }
    };

    Ok(Some(SectionDirectoryEntry {
        section_type,
        version: buf[4],
        flags,
        offset: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        length: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
        checksum: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
    }))
}

#[cfg(test)]
// reason: test values are small known constants
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;

    #[test]
    fn empty_directory_round_trip() {
        let dir = SectionDirectory::new();
        let bytes = dir.to_bytes();
        assert_eq!(bytes.len(), DIRECTORY_PAGE_SIZE);

        let dir2 = SectionDirectory::from_bytes(&bytes).unwrap();
        assert!(dir2.is_empty());
    }

    #[test]
    fn single_entry_round_trip() {
        let mut dir = SectionDirectory::new();
        dir.upsert(SectionDirectoryEntry {
            section_type: SectionType::LpgStore,
            version: 1,
            flags: SectionFlags {
                required: true,
                mmap_able: false,
            },
            offset: SECTION_DATA_OFFSET,
            length: 1024,
            checksum: 0xDEADBEEF,
        })
        .unwrap();

        let bytes = dir.to_bytes();
        let dir2 = SectionDirectory::from_bytes(&bytes).unwrap();

        assert_eq!(dir2.len(), 1);
        let entry = dir2.find(SectionType::LpgStore).unwrap();
        assert_eq!(entry.version, 1);
        assert!(entry.flags.required);
        assert!(!entry.flags.mmap_able);
        assert_eq!(entry.offset, SECTION_DATA_OFFSET);
        assert_eq!(entry.length, 1024);
        assert_eq!(entry.checksum, 0xDEADBEEF);
    }

    #[test]
    fn multiple_entries_round_trip() {
        let mut dir = SectionDirectory::new();
        for (i, st) in [
            SectionType::Catalog,
            SectionType::LpgStore,
            SectionType::VectorStore,
        ]
        .iter()
        .enumerate()
        {
            dir.upsert(SectionDirectoryEntry {
                section_type: *st,
                version: 1,
                flags: st.default_flags(),
                offset: SECTION_DATA_OFFSET + (i as u64) * 4096,
                length: 4096,
                checksum: i as u32,
            })
            .unwrap();
        }

        let bytes = dir.to_bytes();
        let dir2 = SectionDirectory::from_bytes(&bytes).unwrap();
        assert_eq!(dir2.len(), 3);

        assert!(dir2.find(SectionType::Catalog).is_some());
        assert!(dir2.find(SectionType::LpgStore).is_some());
        assert!(dir2.find(SectionType::VectorStore).is_some());
        assert!(dir2.find(SectionType::RdfStore).is_none());
    }

    #[test]
    fn upsert_replaces_existing() {
        let mut dir = SectionDirectory::new();
        dir.upsert(SectionDirectoryEntry {
            section_type: SectionType::LpgStore,
            version: 1,
            flags: SectionType::LpgStore.default_flags(),
            offset: SECTION_DATA_OFFSET,
            length: 1024,
            checksum: 100,
        })
        .unwrap();

        dir.upsert(SectionDirectoryEntry {
            section_type: SectionType::LpgStore,
            version: 2,
            flags: SectionType::LpgStore.default_flags(),
            offset: SECTION_DATA_OFFSET + 4096,
            length: 2048,
            checksum: 200,
        })
        .unwrap();

        assert_eq!(dir.len(), 1);
        let entry = dir.find(SectionType::LpgStore).unwrap();
        assert_eq!(entry.version, 2);
        assert_eq!(entry.length, 2048);
        assert_eq!(entry.checksum, 200);
    }

    #[test]
    fn remove_section() {
        let mut dir = SectionDirectory::new();
        dir.upsert(SectionDirectoryEntry {
            section_type: SectionType::Catalog,
            version: 1,
            flags: SectionType::Catalog.default_flags(),
            offset: SECTION_DATA_OFFSET,
            length: 512,
            checksum: 0,
        })
        .unwrap();

        assert!(dir.remove(SectionType::Catalog));
        assert!(dir.is_empty());
        assert!(!dir.remove(SectionType::Catalog));
    }

    #[test]
    fn directory_checksum_deterministic() {
        let mut dir = SectionDirectory::new();
        dir.upsert(SectionDirectoryEntry {
            section_type: SectionType::Catalog,
            version: 1,
            flags: SectionType::Catalog.default_flags(),
            offset: SECTION_DATA_OFFSET,
            length: 512,
            checksum: 42,
        })
        .unwrap();

        let c1 = dir.checksum();
        let c2 = dir.checksum();
        assert_eq!(c1, c2);
        assert_ne!(c1, 0);
    }

    #[test]
    fn page_is_4kib() {
        let dir = SectionDirectory::new();
        assert_eq!(dir.to_bytes().len(), 4096);
    }

    #[test]
    fn directory_full_at_max_sections() {
        let mut dir = SectionDirectory::new();
        // Fill with all known section types first
        let known_types = [
            SectionType::Catalog,
            SectionType::LpgStore,
            SectionType::RdfStore,
            SectionType::VectorStore,
            SectionType::TextIndex,
            SectionType::RdfRing,
            SectionType::PropertyIndex,
        ];
        for (i, st) in known_types.iter().enumerate() {
            dir.upsert(SectionDirectoryEntry {
                section_type: *st,
                version: 1,
                flags: st.default_flags(),
                offset: SECTION_DATA_OFFSET + (i as u64) * 4096,
                length: 4096,
                checksum: i as u32,
            })
            .unwrap();
        }
        assert_eq!(dir.len(), known_types.len());

        // Upsert on an existing type should succeed (replace, not grow)
        dir.upsert(SectionDirectoryEntry {
            section_type: SectionType::Catalog,
            version: 2,
            flags: SectionType::Catalog.default_flags(),
            offset: SECTION_DATA_OFFSET,
            length: 8192,
            checksum: 999,
        })
        .unwrap();
        assert_eq!(dir.len(), known_types.len());
    }

    #[test]
    fn from_bytes_too_short_header() {
        // Less than 8 bytes: should fail
        let result = SectionDirectory::from_bytes(&[0, 0, 0]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("too short"), "unexpected error: {err}");
    }

    #[test]
    fn from_bytes_count_exceeds_max() {
        let mut buf = vec![0u8; DIRECTORY_PAGE_SIZE];
        // Set entry count to 200 (exceeds MAX_SECTIONS = 127)
        buf[0..4].copy_from_slice(&200u32.to_le_bytes());
        let result = SectionDirectory::from_bytes(&buf);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("200") && err.contains("127"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn from_bytes_truncated_entries() {
        // Header says 2 entries but data is too short to hold them
        let mut buf = vec![0u8; 16]; // 8 header + only 8 bytes (need 2*32=64)
        buf[0..4].copy_from_slice(&2u32.to_le_bytes());
        let result = SectionDirectory::from_bytes(&buf);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("too short"), "unexpected error: {err}");
    }

    #[test]
    fn from_bytes_unknown_optional_section_type_is_skipped() {
        let mut buf = vec![0u8; DIRECTORY_PAGE_SIZE];
        // 2 entries: known Catalog + unknown optional type 99
        buf[0..4].copy_from_slice(&2u32.to_le_bytes());

        // Entry 0: Catalog (required), version 2
        let e0 = 8;
        buf[e0..e0 + 4].copy_from_slice(&(SectionType::Catalog as u32).to_le_bytes());
        buf[e0 + 4] = 2;
        buf[e0 + 5] = SectionType::Catalog.default_flags().to_byte();
        buf[e0 + 8..e0 + 16].copy_from_slice(&SECTION_DATA_OFFSET.to_le_bytes());
        buf[e0 + 16..e0 + 24].copy_from_slice(&64u64.to_le_bytes());
        buf[e0 + 24..e0 + 28].copy_from_slice(&0u32.to_le_bytes());

        // Entry 1: unknown type 99, optional (required bit clear)
        let e1 = 8 + SectionDirectoryEntry::SIZE;
        buf[e1..e1 + 4].copy_from_slice(&99u32.to_le_bytes());
        buf[e1 + 4] = 1;
        buf[e1 + 5] = SectionFlags {
            required: false,
            mmap_able: true,
        }
        .to_byte();
        buf[e1 + 8..e1 + 16].copy_from_slice(&(SECTION_DATA_OFFSET + 4096).to_le_bytes());
        buf[e1 + 16..e1 + 24].copy_from_slice(&128u64.to_le_bytes());
        buf[e1 + 24..e1 + 28].copy_from_slice(&1u32.to_le_bytes());

        let dir = SectionDirectory::from_bytes(&buf).expect("optional unknown must not fail open");
        assert_eq!(dir.len(), 1, "optional unknown entry must be skipped");
        assert!(dir.find(SectionType::Catalog).is_some());
    }

    #[test]
    fn from_bytes_unknown_required_section_type_fails_closed() {
        let mut buf = vec![0u8; DIRECTORY_PAGE_SIZE];
        // 1 entry: unknown type 99 with required flag set
        buf[0..4].copy_from_slice(&1u32.to_le_bytes());
        buf[8..12].copy_from_slice(&99u32.to_le_bytes());
        buf[12] = 1; // version
        buf[13] = SectionFlags {
            required: true,
            mmap_able: false,
        }
        .to_byte();
        let result = SectionDirectory::from_bytes(&buf);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown required section type") && err.contains("99"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn all_section_types_round_trip() {
        let all_types = [
            SectionType::Catalog,
            SectionType::LpgStore,
            SectionType::RdfStore,
            SectionType::VectorStore,
            SectionType::TextIndex,
            SectionType::RdfRing,
            SectionType::PropertyIndex,
        ];
        let mut dir = SectionDirectory::new();
        for (i, st) in all_types.iter().enumerate() {
            dir.upsert(SectionDirectoryEntry {
                section_type: *st,
                version: (i as u8) + 1,
                flags: SectionFlags {
                    required: i % 2 == 0,
                    mmap_able: i % 3 == 0,
                },
                offset: SECTION_DATA_OFFSET + (i as u64) * 8192,
                length: (i as u64 + 1) * 1024,
                checksum: (i as u32) * 111,
            })
            .unwrap();
        }

        let bytes = dir.to_bytes();
        let dir2 = SectionDirectory::from_bytes(&bytes).unwrap();
        assert_eq!(dir2.len(), all_types.len());

        for (i, st) in all_types.iter().enumerate() {
            let entry = dir2.find(*st).unwrap();
            assert_eq!(entry.version, (i as u8) + 1);
            assert_eq!(entry.flags.required, i % 2 == 0);
            assert_eq!(entry.flags.mmap_able, i % 3 == 0);
            assert_eq!(entry.offset, SECTION_DATA_OFFSET + (i as u64) * 8192);
            assert_eq!(entry.length, (i as u64 + 1) * 1024);
            assert_eq!(entry.checksum, (i as u32) * 111);
        }
    }

    #[test]
    fn checksum_changes_with_content() {
        let mut dir = SectionDirectory::new();
        let c_empty = dir.checksum();

        dir.upsert(SectionDirectoryEntry {
            section_type: SectionType::Catalog,
            version: 1,
            flags: SectionType::Catalog.default_flags(),
            offset: SECTION_DATA_OFFSET,
            length: 512,
            checksum: 42,
        })
        .unwrap();

        let c_with_entry = dir.checksum();
        assert_ne!(c_empty, c_with_entry);
    }

    #[test]
    fn default_creates_empty() {
        let dir = SectionDirectory::default();
        assert!(dir.is_empty());
        assert_eq!(dir.len(), 0);
        assert!(dir.entries().is_empty());
    }

    #[test]
    fn find_returns_none_for_missing() {
        let dir = SectionDirectory::new();
        assert!(dir.find(SectionType::LpgStore).is_none());
    }

    #[test]
    fn remove_nonexistent_returns_false() {
        let mut dir = SectionDirectory::new();
        assert!(!dir.remove(SectionType::RdfStore));
    }
}
