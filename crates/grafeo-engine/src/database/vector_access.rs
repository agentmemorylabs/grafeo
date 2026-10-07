//! Spill-aware vector accessor construction for GrafeoDB.
//!
//! Shared by ANN search and exact indexed vector reads so both paths use the
//! same committed inline/ForceDisk accessor seam.
//!
//! Every write-side HNSW insert must use it too (AMH #175): an insert reads
//! its neighbours' vectors to link the new node and prune neighbour lists.
//! Once a ForceDisk open has drained the column into a spill file, a
//! property-only accessor sees every existing node as vectorless, so the new
//! node is never linked and pruning drops edges of existing nodes.

/// Spilled vector storages by `label:property`, shared by the database and
/// its sessions.
#[cfg(all(feature = "vector-index", feature = "mmap", not(feature = "temporal")))]
pub(crate) type VectorSpillRegistry = std::sync::Arc<
    parking_lot::RwLock<
        std::collections::HashMap<String, std::sync::Arc<grafeo_core::index::vector::MmapStorage>>,
    >,
>;

/// Accessor over `store` that falls back to the spill registered for
/// `label:property` (inline values win: written after the spill).
#[cfg(all(feature = "vector-index", feature = "mmap", not(feature = "temporal")))]
pub(crate) fn spill_aware_accessor<'a>(
    store: &'a dyn grafeo_core::graph::GraphStore,
    registry: Option<&VectorSpillRegistry>,
    label: &str,
    property: &str,
) -> grafeo_core::index::vector::VectorAccessorKind<'a> {
    if let Some(registry) = registry
        && let Some(storage) = registry.read().get(&format!("{label}:{property}"))
    {
        return grafeo_core::index::vector::VectorAccessorKind::Spilled(
            grafeo_core::index::vector::SpillableVectorAccessor::new(
                store,
                property,
                std::sync::Arc::clone(storage)
                    as std::sync::Arc<dyn grafeo_core::index::vector::VectorStorage>,
            ),
        );
    }
    grafeo_core::index::vector::VectorAccessorKind::Property(
        grafeo_core::index::vector::PropertyVectorAccessor::new(store, property),
    )
}

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
        spill_aware_accessor(
            self.graph_store_ref(),
            self.vector_spill_storages.as_ref(),
            label,
            property,
        )
    }

    /// The spill registry, for sessions (AMH #175): their commit-time HNSW
    /// inserts must read neighbour vectors through the same accessor.
    #[cfg(all(feature = "mmap", not(feature = "temporal")))]
    pub(crate) fn vector_spill_registry(&self) -> Option<VectorSpillRegistry> {
        self.vector_spill_storages.clone()
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
        spill_aware_accessor(
            &**graph,
            self.vector_spill_storages.as_ref(),
            label,
            property,
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
