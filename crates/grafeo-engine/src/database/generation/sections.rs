//! Generation section capture + emission (H-ADOPT.6 items 2–3).
//!
//! Wire the four generation index sections (Catalog, VectorStore, TextIndex,
//! PropertyIndex) onto both generation publication paths:
//!
//! - **Capture at the consistency point** ([locked decision 3]): catalog +
//!   index state are captured at the same instant as the generation payload
//!   source — the live-graph freeze for direct builds, the freeze handle's
//!   capture (inside the writer barrier) for handoff builds.
//! - **Emission** ([locked decision 2/4]): the Catalog and TextIndex
//!   sections are serialized once at capture (catalog is always < 10 KB;
//!   text postings mirror `build_sections`'s conditions) and emitted from the
//!   captured bytes. The PropertyIndex section is encoded one index at a
//!   time into an unlinked spool file under the generation root, so its
//!   postings are never all resident: on a code-index graph with twelve
//!   high-cardinality keys the section is ~520 MiB, and building it in RAM
//!   added ~2.6 GiB of anonymous memory to publication. The VectorStore
//!   section is **never materialized** — the item 0B streaming encoder
//!   streams the v2 envelope straight to the container sink, with
//!   `exact_len` from the arithmetic pass.
//!
//! Section presence mirrors `build_sections` (engine `database/mod.rs`):
//! emit when the DB has the corresponding indexes; omit otherwise. Absent
//! sections are simply not included — an empty-index open stays green.
//!
//! [locked decision 2]: https://example.invalid (see packet H-ADOPT.6)

use std::cell::Cell;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use grafeo_common::storage::section::{Section, SectionType};
use grafeo_common::utils::error::{Error, Result};
use grafeo_core::graph::lpg::LpgStore;
use grafeo_core::index::property::{
    PropertyIndexBlobEncoder, PropertyIndexSection, encode_property_index_section_prefix,
};
use grafeo_storage::file::generation_writer::ExactSectionSource;

use crate::database::GrafeoDB;

/// A small section's serialized bytes + directory version, captured at the
/// consistency point. One instance = one section.
#[derive(Debug, Clone)]
pub struct SerializedSection {
    /// Exact serialized section payload.
    pub bytes: Vec<u8>,
    /// Section directory version byte.
    pub version: u8,
}

/// Catalog + index section state captured at the generation consistency point.
///
/// Cloneable so handoff builds can carry it in the [`FrozenEpochHandle`]
/// (freeze → build_publish_frozen). The VectorStore entry list holds only the
/// registered index Arcs (schema-bounded) — the topology bytes are streamed
/// from those entries at emission, never materialized.
///
/// [`FrozenEpochHandle`]: crate::database::generation::epoch_handoff::FrozenEpochHandle
#[derive(Clone)]
pub struct GenerationSectionCapture {
    /// Catalog section (always emitted).
    pub catalog: SerializedSection,
    /// PropertyIndex section, when the DB has registered property indexes.
    pub property_index: Option<SpooledSection>,
    /// TextIndex section, when the DB has registered text indexes.
    #[cfg(feature = "text-index")]
    pub text_index: Option<SerializedSection>,
    /// Vector index registrations at capture; emitted via the streaming encoder.
    #[cfg(feature = "vector-index")]
    pub vector_entries: Vec<(String, Arc<grafeo_core::index::vector::VectorIndexKind>)>,
}

impl std::fmt::Debug for GenerationSectionCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg = f.debug_struct("GenerationSectionCapture");
        dbg.field("catalog", &self.catalog)
            .field("property_index", &self.property_index);
        #[cfg(feature = "text-index")]
        dbg.field("text_index", &self.text_index);
        #[cfg(feature = "vector-index")]
        dbg.field(
            "vector_entries",
            &self
                .vector_entries
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
        );
        dbg.finish()
    }
}

/// Thin [`ExactSectionSource`] over pre-serialized bytes (Catalog,
/// PropertyIndex, TextIndex — all small). `exact_len` is the buffer length;
/// `copy_to` writes the buffer once.
pub struct SerializedSectionSource {
    section_type: SectionType,
    version: u8,
    bytes: Vec<u8>,
}

impl SerializedSectionSource {
    /// Wraps one captured small section.
    #[must_use]
    pub fn new(section_type: SectionType, version: u8, bytes: Vec<u8>) -> Self {
        Self {
            section_type,
            version,
            bytes,
        }
    }
}

impl ExactSectionSource for SerializedSectionSource {
    fn section_type(&self) -> SectionType {
        self.section_type
    }

    fn directory_version(&self) -> u8 {
        self.version
    }

    fn exact_len(&self) -> u64 {
        u64::try_from(self.bytes.len()).expect("section buffer length fits u64")
    }

    fn copy_to(&mut self, sink: &mut dyn Write) -> Result<()> {
        sink.write_all(&self.bytes).map_err(Error::Io)
    }
}

/// A section whose small prefix is held in memory and whose body was
/// streamed to an unlinked spool file at capture.
///
/// Cloning shares the spool; the file is removed by the OS when the last
/// clone drops (or the process exits), so a failed or crashed build leaves
/// nothing behind.
#[derive(Clone)]
pub struct SpooledSection {
    inner: Arc<SpooledSectionInner>,
}

struct SpooledSectionInner {
    version: u8,
    prefix: Vec<u8>,
    body: Mutex<File>,
    body_len: u64,
}

impl SpooledSection {
    /// Exact section length: prefix plus spooled body.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.inner.prefix.len() as u64 + self.inner.body_len
    }

    /// Whether the section has no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Section directory version byte.
    #[must_use]
    pub fn version(&self) -> u8 {
        self.inner.version
    }

    /// Writes the prefix, then copies the spooled body from its start.
    ///
    /// # Errors
    ///
    /// Returns an error if the spool cannot be read, is shorter than
    /// recorded, or the sink fails.
    pub fn copy_to(&self, sink: &mut dyn Write) -> Result<()> {
        sink.write_all(&self.inner.prefix).map_err(Error::Io)?;
        let mut body = self
            .inner
            .body
            .lock()
            .map_err(|_| Error::Internal("section spool lock poisoned".into()))?;
        body.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
        let mut reader = BufReader::with_capacity(SPOOL_IO_BYTES, (&mut *body).take(self.inner.body_len));
        let copied = std::io::copy(&mut reader, sink).map_err(Error::Io)?;
        if copied != self.inner.body_len {
            return Err(Error::Internal(format!(
                "section spool truncated: copied {copied} of {} bytes",
                self.inner.body_len
            )));
        }
        Ok(())
    }
}

impl std::fmt::Debug for SpooledSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpooledSection")
            .field("version", &self.inner.version)
            .field("prefix_len", &self.inner.prefix.len())
            .field("body_len", &self.inner.body_len)
            .finish()
    }
}

/// Buffer size for spool writes and reads.
const SPOOL_IO_BYTES: usize = 1024 * 1024;

/// [`ExactSectionSource`] over a [`SpooledSection`].
pub struct SpooledSectionSource {
    section_type: SectionType,
    section: SpooledSection,
}

impl SpooledSectionSource {
    /// Wraps one spooled section.
    #[must_use]
    pub fn new(section_type: SectionType, section: SpooledSection) -> Self {
        Self {
            section_type,
            section,
        }
    }
}

impl ExactSectionSource for SpooledSectionSource {
    fn section_type(&self) -> SectionType {
        self.section_type
    }

    fn directory_version(&self) -> u8 {
        self.section.version()
    }

    fn exact_len(&self) -> u64 {
        self.section.len()
    }

    fn copy_to(&mut self, sink: &mut dyn Write) -> Result<()> {
        self.section.copy_to(sink)
    }
}

/// [`ExactSectionSource`] backed by the item 0B streaming VectorStore encoder.
///
/// `exact_len` runs the arithmetic pass ([`VectorStoreSection::stream_len`])
/// and caches the result; `copy_to` streams the exact v2 envelope to the sink
/// ([`VectorStoreSection::stream_to`]). The section bytes are NEVER
/// materialized (H-ADOPT.6 locked decision 4).
#[cfg(feature = "vector-index")]
pub struct VectorStoreSectionSource {
    section: grafeo_core::index::vector::VectorStoreSection,
    cached_len: Cell<Option<u64>>,
}

#[cfg(feature = "vector-index")]
impl VectorStoreSectionSource {
    /// Wraps a VectorStore section for streaming emission.
    #[must_use]
    pub fn new(section: grafeo_core::index::vector::VectorStoreSection) -> Self {
        Self {
            section,
            cached_len: Cell::new(None),
        }
    }
}

#[cfg(feature = "vector-index")]
impl ExactSectionSource for VectorStoreSectionSource {
    fn section_type(&self) -> SectionType {
        SectionType::VectorStore
    }

    fn directory_version(&self) -> u8 {
        self.section.version()
    }

    fn exact_len(&self) -> u64 {
        if let Some(len) = self.cached_len.get() {
            return len;
        }
        match self.section.stream_len() {
            Ok(len) => {
                self.cached_len.set(Some(len));
                len
            }
            // The same meta-encoding failure makes copy_to fail identically;
            // the container writer fail-closes on written != exact_len, so a
            // 0 here can never silently emit a valid section.
            Err(_) => 0,
        }
    }

    fn copy_to(&mut self, sink: &mut dyn Write) -> Result<()> {
        self.section
            .stream_to(sink)
            .map(|_| ())
            .map_err(|e| Error::Internal(format!("VectorStore section stream failed: {e}")))
    }
}

impl GrafeoDB {
    /// Capture catalog + index section state at the generation consistency
    /// point (H-ADOPT.6 locked decision 3).
    ///
    /// Mirrors `build_sections`'s emission conditions: property/text/vector
    /// sections are captured only when the DB has the corresponding indexes.
    /// The catalog is serialized now (small, always in RAM); the
    /// PropertyIndex is spooled to an unlinked file in `spool_dir`; the
    /// VectorStore keeps only its registered index Arcs — the topology bytes
    /// are streamed at emission.
    ///
    /// `spool_dir` should be the generation root: it is on the same disk as
    /// the publication, never a RAM-backed `/tmp`.
    ///
    /// # Errors
    ///
    /// Returns an error when no LPG store exists, the spool cannot be
    /// written, or a section serialization fails.
    #[cfg(all(feature = "generation", feature = "lpg", feature = "compact-store"))]
    pub(crate) fn capture_generation_sections(
        &self,
        spool_dir: &Path,
    ) -> Result<GenerationSectionCapture> {
        let store = self
            .layered_store
            .as_ref()
            .map(|layered| layered.overlay_store())
            .or_else(|| self.store.clone())
            .ok_or_else(|| Error::Internal("no LPG store for generation section capture".into()))?;

        // Catalog: schema + index metadata, always emitted.
        let catalog = crate::database::catalog_section::CatalogSection::new(
            Arc::clone(&self.catalog),
            Arc::clone(&store),
            {
                let tm = Arc::clone(&self.transaction_manager);
                move || tm.current_epoch().as_u64()
            },
        );
        let catalog = SerializedSection {
            bytes: catalog.serialize()?,
            version: catalog.version(),
        };

        // PropertyIndex: mirror build_sections (overlay snapshot + layered
        // rescan so base rows stay in the postings).
        let property_index = capture_property_index(self, &store, spool_dir)?;

        #[cfg(feature = "text-index")]
        let text_index = {
            let indexes = store.text_index_entries();
            if indexes.is_empty() {
                None
            } else {
                let section = grafeo_core::index::text::TextIndexSection::new(indexes);
                Some(SerializedSection {
                    bytes: section.serialize()?,
                    version: section.version(),
                })
            }
        };

        #[cfg(feature = "vector-index")]
        let vector_entries = store.vector_index_entries();

        Ok(GenerationSectionCapture {
            catalog,
            property_index,
            #[cfg(feature = "text-index")]
            text_index,
            #[cfg(feature = "vector-index")]
            vector_entries,
        })
    }
}

/// Assemble the [`ExactSectionSource`] adapters for a captured section state.
///
/// The CompactStore section is built separately by the caller and prepended,
/// so the container layout stays CompactStore-first. Absent sections are
/// simply not included.
pub fn generation_section_sources(
    capture: GenerationSectionCapture,
) -> Vec<Box<dyn ExactSectionSource>> {
    let mut out: Vec<Box<dyn ExactSectionSource>> = Vec::new();
    out.push(Box::new(SerializedSectionSource::new(
        SectionType::Catalog,
        capture.catalog.version,
        capture.catalog.bytes,
    )));
    if let Some(property) = capture.property_index {
        out.push(Box::new(SpooledSectionSource::new(
            SectionType::PropertyIndex,
            property,
        )));
    }
    #[cfg(feature = "text-index")]
    if let Some(text) = capture.text_index {
        out.push(Box::new(SerializedSectionSource::new(
            SectionType::TextIndex,
            text.version,
            text.bytes,
        )));
    }
    #[cfg(feature = "vector-index")]
    if !capture.vector_entries.is_empty() {
        out.push(Box::new(VectorStoreSectionSource::new(
            grafeo_core::index::vector::VectorStoreSection::new(capture.vector_entries),
        )));
    }
    out
}

/// Mirror of `build_sections`'s PropertyIndex emission: registered keys are
/// postings-rebuilt from the full layered graph so base rows are included,
/// not just the overlay heap snapshot.
///
/// Byte-identical to `PropertyIndexSection::from_snapshots(..).serialize()`,
/// but each key's postings are encoded and written to the spool before the
/// next key is read, so at most one key's encoded postings are resident.
fn capture_property_index(
    db: &GrafeoDB,
    store: &Arc<LpgStore>,
    spool_dir: &Path,
) -> Result<Option<SpooledSection>> {
    // Postings are rebuilt from the full layered graph for every registered
    // key, so base rows are included. The overlay's own postings are not read:
    // for a restored mapped index they would be a base-sized decode that is
    // thrown away here anyway.
    let keys = store.property_index_keys();
    if keys.is_empty() {
        return Ok(None);
    }
    let graph = db.graph_store();
    let node_ids = graph.node_ids();
    let mut spool = tempfile::tempfile_in(spool_dir).map_err(|e| {
        Error::Internal(format!(
            "create PropertyIndex spool in {}: {e}",
            spool_dir.display()
        ))
    })?;
    let mut directory = Vec::with_capacity(keys.len());
    {
        let mut writer = BufWriter::with_capacity(SPOOL_IO_BYTES, &mut spool);
        for prop in &keys {
            let prop_key = grafeo_common::types::PropertyKey::new(prop);
            let mut encoder = PropertyIndexBlobEncoder::new();
            for &node_id in &node_ids {
                if let Some(value) = graph.get_node_property(node_id, &prop_key) {
                    encoder.push(&value, node_id)?;
                }
            }
            directory.push(encoder.write_to(&mut writer)?);
        }
        writer.flush().map_err(Error::Io)?;
    }
    let named: Vec<(&str, _)> = keys
        .iter()
        .map(String::as_str)
        .zip(directory.iter().copied())
        .collect();
    let prefix = encode_property_index_section_prefix(&named)?;
    let body_len = directory.iter().map(|blob| blob.byte_len).sum();
    Ok(Some(SpooledSection {
        inner: Arc::new(SpooledSectionInner {
            version: PropertyIndexSection::empty().version(),
            prefix,
            body: Mutex::new(spool),
            body_len,
        }),
    }))
}

// ── Open-side restoration (H-ADOPT.6 item 3, locked decision 5) ─────────

impl GrafeoDB {
    /// Restore the Catalog section into the fresh overlay + catalog BEFORE the
    /// layered wiring and WAL replay (locked decision 5): replay applies the
    /// post-boundary schema delta on top of the restored catalog — replay's
    /// register calls are idempotent, so the overlay is safe.
    ///
    /// A legacy publication without a Catalog section leaves the fresh catalog
    /// in place (backward-compat path).
    ///
    /// # Errors
    ///
    /// Returns an error when the Catalog bytes fail to deserialize (corrupt
    /// section → fail closed) or no overlay LpgStore exists.
    #[cfg(all(
        feature = "generation",
        feature = "lpg",
        feature = "compact-store",
        feature = "mmap"
    ))]
    pub(crate) fn restore_generation_catalog(
        &self,
        lease: &crate::database::generation::lease::GenerationLease,
    ) -> Result<()> {
        let Some(bytes) = lease.catalog_bytes() else {
            // Legacy publication without sections: the fresh catalog stays.
            return Ok(());
        };
        let store = self.store.as_ref().ok_or_else(|| {
            Error::Internal("no overlay LpgStore for generation Catalog restore".into())
        })?;
        let mut section = crate::database::catalog_section::CatalogSection::new(
            Arc::clone(&self.catalog),
            Arc::clone(store),
            {
                let tm = Arc::clone(&self.transaction_manager);
                move || tm.current_epoch().as_u64()
            },
        );
        section.deserialize(&bytes)?;
        Ok(())
    }

    /// Restore the derived index sections (VectorStore, PropertyIndex,
    /// TextIndex) AFTER `wire_generation_layered` and BEFORE WAL replay
    /// (locked decision 5): replayed post-boundary writes land on the
    /// restored postings/topology.
    ///
    /// Mirrors `load_from_sections`'s per-section semantics: read-only opens
    /// keep the index sections mmap-backed (zero-copy), writable opens
    /// hydrate heap topologies/postings. A registered-but-absent VectorStore
    /// section, or a corrupt present section, fails closed — never a silent
    /// fallback-to-rebuild.
    ///
    /// # Errors
    ///
    /// Returns an error when a present section fails to decode or when the
    /// Catalog registered vector shells without a VectorStore section.
    #[cfg(all(
        feature = "generation",
        feature = "lpg",
        feature = "compact-store",
        feature = "mmap"
    ))]
    pub(crate) fn restore_generation_indexes(
        &self,
        read_only: bool,
        lease: &crate::database::generation::lease::GenerationLease,
    ) -> Result<()> {
        let store = self.store.as_ref().ok_or_else(|| {
            Error::Internal("no overlay LpgStore for generation index restore".into())
        })?;

        // VectorStore: rehydrate HNSW topology into the catalog-registered
        // shells (mmap-backed on read-only opens, heap on writable opens).
        #[cfg(feature = "vector-index")]
        {
            let indexes = store.vector_index_entries();
            if !indexes.is_empty() {
                let Some(bytes) = lease.vector_section_bytes() else {
                    return Err(Error::Serialization(
                        "Catalog registers vector indexes but the generation has no VectorStore section (fail-closed)".into(),
                    ));
                };
                let mut section = grafeo_core::index::vector::VectorStoreSection::new(indexes);
                if read_only {
                    section.restore_from_mapped_bytes(bytes)?;
                } else {
                    section.deserialize(&bytes)?;
                }
            }
        }

        // PropertyIndex: install the mapped postings zero-copy on every open,
        // read-only or writable. The `Bytes` own the section mapping, so the
        // postings stay file-backed for as long as the index exists, with no
        // heap copy proportional to the base. Writable opens stay correct
        // because the installed index keeps a heap write delta that WAL
        // replay and later writes maintain (D5).
        if let Some(bytes) = lease.property_index_bytes() {
            let mapped_set = grafeo_core::index::property::parse_property_index_section(bytes)?;
            for idx in mapped_set.indexes() {
                store.install_mapped_property_index(Arc::new(idx.clone()));
            }
            let _ = mapped_set.accounting();
        }

        // TextIndex: v2 mapped payloads stay file-backed on read-only opens;
        // legacy v1 hydrates the catalog-registered heap shells.
        #[cfg(feature = "text-index")]
        if let Some(bytes) = lease.text_index_bytes() {
            use grafeo_core::index::text::{
                TextIndexSection, is_mapped_text_payload, parse_text_index_section,
            };
            let indexes = store.text_index_entries();
            if read_only {
                if is_mapped_text_payload(&bytes) {
                    let mapped_set = parse_text_index_section(bytes)?;
                    for idx in mapped_set.indexes() {
                        store.install_mapped_text_index(Arc::new(idx.clone()));
                    }
                } else {
                    // Legacy v1 over a mapped region: copy into section decode.
                    let mut section = TextIndexSection::new(indexes);
                    section.deserialize(&bytes)?;
                }
            } else {
                let mut section = TextIndexSection::new(indexes);
                section.deserialize(&bytes)?;
                if let Some(mapped_set) = section.take_mapped() {
                    for idx in mapped_set.indexes() {
                        store.install_mapped_text_index(Arc::new(idx.clone()));
                    }
                }
            }
        }

        Ok(())
    }
}
