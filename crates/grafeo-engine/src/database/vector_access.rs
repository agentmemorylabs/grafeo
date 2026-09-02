//! Spill-aware vector accessor construction for GrafeoDB.
//!
//! Shared by ANN search and exact indexed vector reads so both paths use the
//! same committed inline/ForceDisk accessor seam.

#[cfg(feature = "vector-index")]
impl super::GrafeoDB {
    /// Creates a vector accessor for the given label/property, using spilled
    /// MmapStorage if the index has been spilled to disk.
    #[cfg(all(feature = "mmap", not(feature = "temporal")))]
    pub(super) fn make_vector_accessor<'a>(
        &'a self,
        label: &str,
        property: &str,
    ) -> grafeo_core::index::vector::VectorAccessorKind<'a> {
        let key = format!("{label}:{property}");
        if let Some(ref spill_map) = self.vector_spill_storages {
            let map = spill_map.read();
            if let Some(storage) = map.get(&key) {
                return grafeo_core::index::vector::VectorAccessorKind::Spilled(
                    grafeo_core::index::vector::SpillableVectorAccessor::new(
                        self.graph_store_ref(),
                        property,
                        std::sync::Arc::clone(storage)
                            as std::sync::Arc<dyn grafeo_core::index::vector::VectorStorage>,
                    ),
                );
            }
        }
        grafeo_core::index::vector::VectorAccessorKind::Property(
            grafeo_core::index::vector::PropertyVectorAccessor::new(
                self.graph_store_ref(),
                property,
            ),
        )
    }

    /// Creates a vector accessor (no spill support when mmap or temporal unavailable).
    #[cfg(not(all(feature = "mmap", not(feature = "temporal"))))]
    pub(super) fn make_vector_accessor<'a>(
        &'a self,
        _label: &str,
        property: &str,
    ) -> grafeo_core::index::vector::VectorAccessorKind<'a> {
        grafeo_core::index::vector::VectorAccessorKind::Property(
            grafeo_core::index::vector::PropertyVectorAccessor::new(
                self.graph_store_ref(),
                property,
            ),
        )
    }

    /// BUILD-side vector accessor (G-VECBUILD.1 E2).
    ///
    /// Unlike [`Self::make_vector_accessor`] (serving path, borrowed
    /// layer-only store), this accessor reads through the CALLER's owned
    /// `graph_store()` view — a `TierChainView` while G-MIDFLUSH.1 mid-build
    /// tiers exist — and falls back to the spill storage registered by
    /// `spill_vector_column_to_disk` (E1). The HNSW build loop must see both
    /// sources or it silently skips tier/spill-resident vectors.
    #[cfg(all(feature = "mmap", not(feature = "temporal")))]
    pub(super) fn build_vector_accessor<'a>(
        &'a self,
        graph: &'a std::sync::Arc<dyn grafeo_core::graph::GraphStoreSearch>,
        label: &str,
        property: &str,
    ) -> grafeo_core::index::vector::VectorAccessorKind<'a> {
        let key = format!("{label}:{property}");
        if let Some(ref spill_map) = self.vector_spill_storages {
            let map = spill_map.read();
            if let Some(storage) = map.get(&key) {
                return grafeo_core::index::vector::VectorAccessorKind::Spilled(
                    grafeo_core::index::vector::SpillableVectorAccessor::new(
                        &**graph,
                        property,
                        std::sync::Arc::clone(storage)
                            as std::sync::Arc<dyn grafeo_core::index::vector::VectorStorage>,
                    ),
                );
            }
        }
        grafeo_core::index::vector::VectorAccessorKind::Property(
            grafeo_core::index::vector::PropertyVectorAccessor::new(&**graph, property),
        )
    }

    /// BUILD-side vector accessor without spill support (mmap/temporal off).
    #[cfg(not(all(feature = "mmap", not(feature = "temporal"))))]
    pub(super) fn build_vector_accessor<'a>(
        &'a self,
        graph: &'a std::sync::Arc<dyn grafeo_core::graph::GraphStoreSearch>,
        _label: &str,
        property: &str,
    ) -> grafeo_core::index::vector::VectorAccessorKind<'a> {
        grafeo_core::index::vector::VectorAccessorKind::Property(
            grafeo_core::index::vector::PropertyVectorAccessor::new(&**graph, property),
        )
    }

    /// True when the named vector column was drained to a spill file by
    /// `spill_vector_column_to_disk` (G-VECBUILD.1 E1).
    #[cfg(all(feature = "mmap", not(feature = "temporal")))]
    pub(super) fn vector_column_is_spilled(&self, label: &str, property: &str) -> bool {
        self.vector_spill_storages
            .as_ref()
            .is_some_and(|map| map.read().contains_key(&format!("{label}:{property}")))
    }
}
