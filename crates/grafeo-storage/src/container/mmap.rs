//! Memory-mapped section access for the `.grafeo` container.
//!
//! After a section is flushed to the container file, it can be memory-mapped
//! for zero-copy read access. The OS page cache manages eviction, providing
//! graceful degradation when data exceeds available RAM.
//!
//! Only sections with `flags.mmap_able = true` can be mapped (index sections:
//! VectorStore, TextIndex, RdfRing, PropertyIndex). Data sections (Catalog,
//! LpgStore, RdfStore) must be deserialized into RAM.

use std::sync::Arc;

use bytes::Bytes;
use grafeo_common::storage::SectionType;

use super::page_fetcher::AccessHint;

/// A read-only memory-mapped view of a section in the `.grafeo` container.
///
/// Created by [`GrafeoFileManager::mmap_section`](crate::file::GrafeoFileManager::mmap_section).
/// The mapping remains valid as long as this struct is alive, independent of
/// the file manager's mutex. The OS page cache serves reads: warm data is
/// zero-copy, cold pages fault in transparently from disk.
///
/// # Lifecycle
///
/// 1. Engine flushes dirty sections to the container via `write_sections()`
/// 2. Engine calls `mmap_section()` for index sections it wants to keep accessible
/// 3. Engine drops the in-memory copy of the section data
/// 4. Reads go through the `MmapSection` (zero-copy from page cache)
/// 5. On next checkpoint, the engine **drops all mmaps first**, then writes
///
/// # Platform note
///
/// On Windows, the OS rejects writes to a file with active memory mappings
/// (error 1224: `ERROR_USER_MAPPED_FILE`). All `MmapSection` handles must
/// be dropped before calling `write_sections()` or `write_snapshot()`.
/// On Linux/macOS, writes succeed with active mappings (old mappings see
/// stale data), but the drop-before-write lifecycle is used on all platforms
/// for consistency.
pub struct MmapSection {
    mmap: memmap2::Mmap,
    section_type: SectionType,
    checksum: u32,
}

impl MmapSection {
    /// Creates a new `MmapSection`.
    ///
    /// Called internally by `GrafeoFileManager::mmap_section()` after
    /// CRC verification.
    pub(crate) fn new(mmap: memmap2::Mmap, section_type: SectionType, checksum: u32) -> Self {
        Self {
            mmap,
            section_type,
            checksum,
        }
    }

    /// Returns the section data as a byte slice (zero-copy).
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.mmap
    }

    /// The section type this mapping covers.
    #[must_use]
    pub fn section_type(&self) -> SectionType {
        self.section_type
    }

    /// The CRC-32 checksum of the section data (verified on creation).
    #[must_use]
    pub fn checksum(&self) -> u32 {
        self.checksum
    }

    /// The byte length of the mapped section.
    #[must_use]
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Whether the mapping is zero-length.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }

    /// Transfers a shared mapping owner into refcounted [`Bytes`].
    ///
    /// Clones and slices of the returned `Bytes` retain the mapping until the
    /// final view is dropped. This is the safe bridge used by container-backed
    /// CompactStore reads: the bytes never borrow the file manager and no
    /// fabricated `'static` lifetime is involved.
    #[must_use]
    pub fn into_bytes(self: Arc<Self>) -> Bytes {
        Bytes::from_owner(MmapBytesOwner { mapping: self })
    }

    /// Advise the OS about the expected access pattern for a range.
    ///
    /// On Unix this delegates to `madvise` via `memmap2`. On Windows
    /// (and other platforms without a portable equivalent) it is a no-op.
    /// Out-of-range arguments and underlying errors are silently ignored:
    /// advice is a hint, not a contract.
    pub fn advise(&self, offset: usize, len: usize, hint: AccessHint) {
        // On Windows there is no portable madvise without `unsafe` FFI,
        // so this is a no-op. The args are intentionally unused there.
        let _ = (offset, len, hint);
        #[cfg(unix)]
        {
            use memmap2::Advice;
            // memmap2's safe `Advice` enum exposes only the read-side
            // hints; `MADV_DONTNEED` lives on `UncheckedAdvice` because
            // it can zero-fill subsequent reads, so it requires `unsafe`.
            // Treating `DontNeed` as a no-op here keeps the call safe;
            // if we ever need real eviction, plumb it through an
            // `unsafe` path in a dedicated helper.
            let advice = match hint {
                AccessHint::Sequential => Some(Advice::Sequential),
                AccessHint::Random => Some(Advice::Random),
                AccessHint::WillNeed => Some(Advice::WillNeed),
                AccessHint::DontNeed => None,
            };
            if let Some(advice) = advice {
                // Out-of-range or otherwise failing advise is best-effort.
                let _ = self.mmap.advise_range(advice, offset, len);
            }
        }
    }
}

/// `Bytes::from_owner` needs an owner that directly exposes the mapped bytes.
/// Keeping the `Arc` here makes every `Bytes` clone/slice participate in the
/// mapping lifetime rather than tying it to a file-manager lock or scope.
struct MmapBytesOwner {
    mapping: Arc<MmapSection>,
}

impl AsRef<[u8]> for MmapBytesOwner {
    fn as_ref(&self) -> &[u8] {
        self.mapping.as_bytes()
    }
}

impl AsRef<[u8]> for MmapSection {
    fn as_ref(&self) -> &[u8] {
        &self.mmap
    }
}

impl std::fmt::Debug for MmapSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapSection")
            .field("section_type", &self.section_type)
            .field("len", &self.mmap.len())
            .field("checksum", &format_args!("{:#010X}", self.checksum))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;

    use super::MmapSection;
    use grafeo_common::storage::SectionType;

    #[test]
    fn bytes_views_retain_and_then_release_the_mapping_owner() {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        file.write_all(b"mapped CompactStore bytes")
            .expect("write payload");
        file.flush().expect("flush payload");

        #[allow(unsafe_code)]
        let mmap =
            unsafe { memmap2::MmapOptions::new().map(file.as_file()) }.expect("mmap payload");
        let mapping = Arc::new(MmapSection::new(mmap, SectionType::CompactStore, 0));
        let weak = Arc::downgrade(&mapping);
        let bytes = Arc::clone(&mapping).into_bytes();
        let slice = bytes.slice(7..);

        drop(mapping);
        assert!(weak.upgrade().is_some(), "Bytes must retain the mapping");
        assert_eq!(&slice[..], b"CompactStore bytes");

        drop(bytes);
        assert!(
            weak.upgrade().is_some(),
            "a live Bytes slice must retain the mapping"
        );
        drop(slice);
        assert!(
            weak.upgrade().is_none(),
            "the mapping must release after the final Bytes view drains"
        );
    }
}
