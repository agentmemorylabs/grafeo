//! G-VECBUILD.1 E1 — spill a vector property column to the `MmapStorage`
//! seam before HNSW construction (B1 spill-before-build).
//!
//! The builder's overlay tail window (≤ the G-MIDFLUSH.1 drain threshold)
//! still holds `Value::Vector` heaps when the HNSW build starts. E1 drains
//! that column write-through into the existing spill file layout
//! (`vectors_<Label%3Aproperty>.bin` under the configured spill path) and
//! registers the storage in the shared `vector_spill_storages` registry so
//! [`make_vector_accessor`](super::GrafeoDB::make_vector_accessor) and the
//! build-side accessor serve the spilled vectors via mmap.
//!
//! Publish safety (SRV1 Gap B): the `section:VectorStore` consumer
//! registered at `with_config` time holds a weak reference to the
//! **pre-`compact()`** store, so after the builder's `compact()` its
//! reload would fail closed (`store dropped`) and generation publish would
//! emit empty embedding columns. E1 therefore re-registers a consumer bound
//! to the live store, preserving the spill registry, so the publish path's
//! reload-from-spill / re-spill machinery fires unchanged.

#[cfg(all(
    feature = "lpg",
    feature = "vector-index",
    feature = "mmap",
    not(feature = "temporal")
))]
mod imp {
    use std::sync::Arc;

    use grafeo_common::grafeo_info;
    use grafeo_common::types::{NodeId, PropertyKey, Value};
    use grafeo_common::utils::error::{Error, Result};
    use grafeo_core::index::vector::{MmapStorage, VectorStorage as _};

    use super::super::GrafeoDB;
    use super::super::section_consumer::VectorIndexConsumer;

    /// Outcome of one [`GrafeoDB::spill_vector_column_to_disk`] call.
    #[derive(Debug, Clone)]
    pub struct SpillVectorColumnReport {
        /// Label of the spilled column.
        pub label: String,
        /// Property name of the spilled column.
        pub property: String,
        /// Number of vectors written to the spill file.
        pub vectors_spilled: u64,
        /// Raw f32 payload bytes written (`vectors_spilled * dims * 4`).
        pub bytes_spilled: u64,
        /// Absolute path of the spill file.
        pub spill_file: std::path::PathBuf,
        /// True when the column was already spilled (no-op call).
        pub already_spilled: bool,
    }

    impl GrafeoDB {
        /// Drains the named `label:property` vector column from the live
        /// property store into an `MmapStorage` spill file (G-VECBUILD.1 E1).
        ///
        /// After this call, `get_node_property` returns `None` for the
        /// drained column and spill-aware accessors serve the vectors from
        /// mmap. The drain covers the **live store only** (post-`compact()`
        /// that is the overlay): vectors already living in mid-build tier
        /// files (G-MIDFLUSH.1 drains) are mmap-backed already and are not
        /// rewritten.
        ///
        /// Fail-closed: any write/flush failure restores the drained column
        /// and unlinks the partial spill file.
        ///
        /// # Errors
        ///
        /// - no spill path configured,
        /// - spill registry not initialized (external-store constructors),
        /// - a column that holds values but no vectors (mixed-type data bug),
        /// - dimension mismatch inside the column,
        /// - any I/O failure (column restored, file unlinked).
        ///
        /// An EMPTY column is a legitimate no-op (returns zero counts): the
        /// G-MIDFLUSH.1 tail window may already be fully tier-resident.
        pub fn spill_vector_column_to_disk(
            &self,
            label: &str,
            property: &str,
        ) -> Result<SpillVectorColumnReport> {
            let key = format!("{label}:{property}");

            let spill_dir = self
                .buffer_manager()
                .config()
                .spill_path
                .clone()
                .ok_or_else(|| {
                    Error::Internal(format!(
                        "spill_vector_column_to_disk {key}: no spill path configured"
                    ))
                })?;

            let registry = match self.vector_spill_storages.as_ref() {
                Some(map) => Arc::clone(map),
                None => {
                    return Err(Error::Internal(format!(
                        "spill_vector_column_to_disk {key}: vector spill registry not initialized"
                    )));
                }
            };

            if registry.read().contains_key(&key) {
                let spill_file = spill_file_for_key(&spill_dir, &key);
                return Ok(SpillVectorColumnReport {
                    label: label.to_string(),
                    property: property.to_string(),
                    vectors_spilled: 0,
                    bytes_spilled: 0,
                    spill_file,
                    already_spilled: true,
                });
            }

            let store = self.lpg_store();
            let prop_key = PropertyKey::new(property);
            let drained = store.drain_node_property_column(&prop_key);
            if drained.is_empty() {
                // Legitimate no-op: the tail window holds no values for this
                // column (e.g. G-MIDFLUSH.1 drains moved every vector into
                // mmap tiers already). Report zero work, not an error, so
                // the builder can treat "nothing left to spill" as success.
                return Ok(SpillVectorColumnReport {
                    label: label.to_string(),
                    property: property.to_string(),
                    vectors_spilled: 0,
                    bytes_spilled: 0,
                    spill_file: spill_file_for_key(&spill_dir, &key),
                    already_spilled: false,
                });
            }

            let mut dimensions: Option<usize> = None;
            let mut vectors: Vec<(NodeId, Arc<[f32]>)> = Vec::with_capacity(drained.len());
            let mut non_vector: Vec<(NodeId, Value)> = Vec::new();
            for (id, value) in drained {
                match value {
                    Value::Vector(v) => {
                        if let Some(expected) = dimensions {
                            if v.len() != expected {
                                let restore = vectors
                                    .into_iter()
                                    .map(|(id, vec)| (id, Value::Vector(vec)))
                                    .chain(non_vector);
                                store.restore_node_property_column(&prop_key, restore);
                                return Err(Error::Internal(format!(
                                    "spill_vector_column_to_disk {key}: dimension mismatch \
                                     (expected {expected}, found {} on node {})",
                                    v.len(),
                                    id.0
                                )));
                            }
                        } else {
                            dimensions = Some(v.len());
                        }
                        vectors.push((id, v));
                    }
                    other => non_vector.push((id, other)),
                }
            }
            let Some(dims) = dimensions else {
                store.restore_node_property_column(&prop_key, non_vector.into_iter());
                return Err(Error::Internal(format!(
                    "spill_vector_column_to_disk {key}: column holds no vectors"
                )));
            };

            // Restore helper for every fallible step below: put the drained
            // vectors + non-vector values back, then unlink any partial file.
            let restore = |store: &Arc<grafeo_core::graph::lpg::LpgStore>,
                           vectors: &[(NodeId, Arc<[f32]>)],
                           non_vector: &[(NodeId, Value)]| {
                let values = vectors
                    .iter()
                    .cloned()
                    .map(|(id, vec)| (id, Value::Vector(vec)))
                    .chain(non_vector.iter().cloned());
                store.restore_node_property_column(&prop_key, values);
            };

            if let Err(e) = std::fs::create_dir_all(&spill_dir) {
                restore(store, &vectors, &non_vector);
                return Err(Error::Internal(format!(
                    "spill_vector_column_to_disk {key}: create spill dir {}: {e}",
                    spill_dir.display()
                )));
            }

            let spill_file = spill_file_for_key(&spill_dir, &key);
            let mmap_storage = match MmapStorage::create(&spill_file, dims) {
                Ok(storage) => storage,
                Err(e) => {
                    restore(store, &vectors, &non_vector);
                    return Err(Error::Internal(format!(
                        "spill_vector_column_to_disk {key}: create {}: {e}",
                        spill_file.display()
                    )));
                }
            };

            #[allow(clippy::cast_possible_truncation)]
            let vectors_spilled = vectors.len() as u64;
            let mut bytes_spilled: u64 = 0;
            for (id, vector) in &vectors {
                if let Err(e) = mmap_storage.insert(*id, vector) {
                    let _ = std::fs::remove_file(&spill_file);
                    restore(store, &vectors, &non_vector);
                    return Err(Error::Internal(format!(
                        "spill_vector_column_to_disk {key}: insert node {}: {e}",
                        id.0
                    )));
                }
                #[allow(clippy::cast_possible_truncation)]
                {
                    bytes_spilled += (vector.len() * std::mem::size_of::<f32>()) as u64;
                }
            }
            if let Err(e) = mmap_storage.flush() {
                let _ = std::fs::remove_file(&spill_file);
                restore(store, &vectors, &non_vector);
                return Err(Error::Internal(format!(
                    "spill_vector_column_to_disk {key}: flush: {e}"
                )));
            }

            // Non-vector values that shared the column go back to the store.
            if !non_vector.is_empty() {
                store.restore_node_property_column(&prop_key, non_vector.into_iter());
            }

            registry.write().insert(key.clone(), Arc::new(mmap_storage));

            // Re-register the VectorStore consumer bound to the LIVE store.
            // After `compact()` the consumer registered at `with_config` time
            // holds a dead weak reference; publish's reload-from-spill (SRV1
            // Gap B) must see a live consumer or it would emit a generation
            // with empty embedding columns.
            self.buffer_manager()
                .unregister_consumer("section:VectorStore");
            let consumer = VectorIndexConsumer::with_spilled_registry(
                self.lpg_store(),
                Some(spill_dir),
                registry,
            );
            self.buffer_manager().register_consumer(Arc::new(consumer));

            grafeo_info!(
                "vector column spilled to disk: :{label}({property}) - {vectors_spilled} vectors, {bytes_spilled} bytes -> {path}",
                path = spill_file.display()
            );

            Ok(SpillVectorColumnReport {
                label: label.to_string(),
                property: property.to_string(),
                vectors_spilled,
                bytes_spilled,
                spill_file,
                already_spilled: false,
            })
        }
    }

    /// Sanitized spill file name for a `label:property` key. Matches the
    /// `VectorIndexConsumer::spill_index` convention so both paths agree on
    /// one file per column.
    fn spill_file_for_key(spill_dir: &std::path::Path, key: &str) -> std::path::PathBuf {
        let safe_key = key.replace('%', "%25").replace(':', "%3A");
        spill_dir.join(format!("vectors_{safe_key}.bin"))
    }
}
