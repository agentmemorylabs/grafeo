//! One immutable generation's in-process base (G-EM0.4a).
//!
//! A [`BaseGeneration`] owns the CompactStore **section** mapping of one
//! immutable generation container plus the zero-copy store deserialized from
//! it, tagged with the generation's durable identity. See [`super`] (the
//! `lease` module) for the full lease/transition contract.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use grafeo_common::storage::SectionType;
use grafeo_common::utils::error::{Error, Result};
use grafeo_core::graph::compact::CompactStore;
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_storage::file::GrafeoFileManager;

/// One immutable generation's in-process base: identity + the owned section mapping.
///
/// The base owns the CompactStore section's mmap-backed `Bytes` (via
/// [`GrafeoFileManager::mmap_section`], which CRC-validates the mapped region)
/// and the `Arc<CompactStore>` deserialized from it; the codec columns hold
/// zero-copy `Bytes::slice` views into the mapping, so the mapping is released
/// only when both the store and this owner drop. This struct tags the mapping
/// with the generation's durable identity so a reader can prove which
/// immutable base a snapshot serves. `BaseGeneration` is reference-counted;
/// the OS mapping it wraps is released only when the *last* strong reference
/// (owner, registry, or read snapshot) drops — that is the "final mapping
/// release" the tests prove via weak refs.
pub struct BaseGeneration {
    /// Manifest publication sequence of the selected generation (identity).
    publication_sequence: u64,
    /// Caller-supplied generation identifier.
    generation_id: String,
    /// Absolute path of the immutable generation container.
    generation_abs_path: PathBuf,
    /// The owned CompactStore-section mapping. The store's columns hold
    /// zero-copy `Bytes::slice` views that share this same allocation, so the
    /// mapping stays live as long as the store does; the OS mapping is released
    /// once both this field and the store's slices are dropped.
    _section_bytes: Bytes,
    /// The base store, deserialized from the mapped section bytes.
    store: Arc<CompactStore>,
    /// H-ADOPT.6: Catalog section bytes (read via the CRC-verified data path —
    /// Catalog is not mmap-able). `None` for legacy publications without the
    /// section.
    catalog_bytes: Option<Bytes>,
    /// H-ADOPT.6: VectorStore section bytes, mmap-backed (CRC-validated at
    /// map time). `None` for legacy publications without the section.
    vector_bytes: Option<Bytes>,
    /// H-ADOPT.6: PropertyIndex section bytes, mmap-backed.
    property_index_bytes: Option<Bytes>,
    /// H-ADOPT.6: TextIndex section bytes, mmap-backed.
    text_index_bytes: Option<Bytes>,
}

impl std::fmt::Debug for BaseGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseGeneration")
            .field("publication_sequence", &self.publication_sequence)
            .field("generation_id", &self.generation_id)
            .field("generation_abs_path", &self.generation_abs_path)
            .field("node_count", &self.store.total_nodes())
            .finish_non_exhaustive()
    }
}

impl BaseGeneration {
    /// Open a generation container as an in-process base.
    ///
    /// Maps the CompactStore **section** of the generation `.grafeo` container
    /// (not the whole file) via [`GrafeoFileManager::mmap_section`] and
    /// deserializes a zero-copy store from the mapped, CRC-validated bytes.
    /// Fails closed on any open/map/decode error (never serves a torn
    /// generation).
    pub(super) fn open(
        publication_sequence: u64,
        generation_id: String,
        generation_abs_path: PathBuf,
    ) -> Result<Self> {
        let manager = GrafeoFileManager::open_read_only(&generation_abs_path)?;
        let directory = manager
            .read_section_directory()?
            .ok_or_else(|| Error::Internal("generation has no section directory".into()))?;
        let entry = directory
            .find(SectionType::CompactStore)
            .ok_or_else(|| Error::Internal("generation has no CompactStore section".into()))?;
        let section = manager.mmap_section(entry)?;
        let section_bytes = Arc::new(section).into_bytes();

        let mut cs_section = CompactStoreSection::empty();
        cs_section.deserialize_from_bytes(section_bytes.clone())?;
        let store = cs_section.store().ok_or_else(|| {
            Error::Internal("empty CompactStoreSection after generation open".into())
        })?;

        // H-ADOPT.6 item 3: map the optional Catalog + derived index sections.
        // Catalog is not mmap-able (read via the CRC-verified data path); the
        // index sections are mmap-backed, zero-copy, CRC-validated at map time.
        // A corrupt-but-present section fails the open here (fail-closed —
        // never a silent fallback-to-rebuild). Legacy publications without
        // these sections simply yield `None`.
        let catalog_bytes = match directory.find(SectionType::Catalog) {
            Some(entry) => Some(Bytes::from(manager.read_section_data(entry).map_err(
                |e| Error::Internal(format!("generation Catalog section read failed: {e}")),
            )?)),
            None => None,
        };
        let vector_bytes = match directory.find(SectionType::VectorStore) {
            Some(entry) => Some(Arc::new(manager.mmap_section(entry)?).into_bytes()),
            None => None,
        };
        let property_index_bytes = match directory.find(SectionType::PropertyIndex) {
            Some(entry) => Some(Arc::new(manager.mmap_section(entry)?).into_bytes()),
            None => None,
        };
        let text_index_bytes = match directory.find(SectionType::TextIndex) {
            Some(entry) => Some(Arc::new(manager.mmap_section(entry)?).into_bytes()),
            None => None,
        };

        Ok(Self {
            publication_sequence,
            generation_id,
            generation_abs_path,
            _section_bytes: section_bytes,
            store,
            catalog_bytes,
            vector_bytes,
            property_index_bytes,
            text_index_bytes,
        })
    }

    /// Manifest publication sequence of this base.
    #[must_use]
    pub fn publication_sequence(&self) -> u64 {
        self.publication_sequence
    }

    /// The generation identifier.
    #[must_use]
    pub fn generation_id(&self) -> &str {
        &self.generation_id
    }

    /// Absolute path of the immutable generation container.
    #[must_use]
    pub fn generation_abs_path(&self) -> &Path {
        &self.generation_abs_path
    }

    /// The base store, served from the owned mmap-backed bytes.
    #[must_use]
    pub fn store(&self) -> Arc<CompactStore> {
        Arc::clone(&self.store)
    }

    /// Catalog section bytes (H-ADOPT.6), CRC-verified; `None` when the
    /// generation carries no Catalog section (legacy publication).
    #[must_use]
    pub fn catalog_bytes(&self) -> Option<Bytes> {
        self.catalog_bytes.clone()
    }

    /// VectorStore section bytes (H-ADOPT.6), mmap-backed; `None` when the
    /// generation carries no VectorStore section.
    #[must_use]
    pub fn vector_section_bytes(&self) -> Option<Bytes> {
        self.vector_bytes.clone()
    }

    /// PropertyIndex section bytes (H-ADOPT.6), mmap-backed.
    #[must_use]
    pub fn property_index_bytes(&self) -> Option<Bytes> {
        self.property_index_bytes.clone()
    }

    /// TextIndex section bytes (H-ADOPT.6), mmap-backed.
    #[must_use]
    pub fn text_index_bytes(&self) -> Option<Bytes> {
        self.text_index_bytes.clone()
    }
}
