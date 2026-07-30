//! Split memory accounting for mapped CompactStore open.

/// Settled anonymous budget for schema/owner/metadata handles (16 MiB).
pub const SCHEMA_OWNER_BUDGET_BYTES: usize = 16 * 1024 * 1024;

/// Returns the default schema/owner budget.
#[must_use]
pub const fn default_schema_owner_budget() -> usize {
    SCHEMA_OWNER_BUDGET_BYTES
}

/// Split accounting for a CompactStore instance.
///
/// `memory_bytes()` alone is not evidence for Milestone R: callers must
/// reconcile these categories at two graph sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CompactMemoryAccounting {
    /// File-backed mapped payload and index segment bytes retained via
    /// `Bytes` slices into the container mapping (not anonymous heap).
    pub mapped_payload_index_bytes: usize,
    /// Anonymous owner/schema metadata (labels, type maps, table maps,
    /// statistics, mapping handles) that is not proportional to durable
    /// graph cardinality beyond schema scale.
    pub anonymous_owner_schema_bytes: usize,
    /// Anonymous bytes that scale with nodes/edges/rows/blocks/string
    /// volume. For a successful mapped v5 open this must be zero.
    pub anonymous_proportional_structure_bytes: usize,
    /// Explicitly configured read-cache residency (currently unused; 0).
    pub read_cache_bytes: usize,
    /// Transient query-scratch high-water mark observed since open.
    pub query_scratch_high_water_bytes: usize,
    /// Configured schema/owner budget; overflow fails closed at open.
    pub schema_owner_budget_bytes: usize,
}

impl CompactMemoryAccounting {
    /// Creates accounting with the default schema/owner budget.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self {
            schema_owner_budget_bytes: SCHEMA_OWNER_BUDGET_BYTES,
            ..Self::default()
        }
    }

    /// Returns `true` when owner/schema bytes exceed the hard budget.
    #[must_use]
    pub fn schema_budget_exceeded(&self) -> bool {
        self.anonymous_owner_schema_bytes > self.schema_owner_budget_bytes
    }

    /// Settled anonymous bytes (owner/schema + proportional + cache).
    #[must_use]
    pub fn settled_anonymous_bytes(&self) -> usize {
        self.anonymous_owner_schema_bytes
            .saturating_add(self.anonymous_proportional_structure_bytes)
            .saturating_add(self.read_cache_bytes)
    }

    /// Returns `true` when this open is graph-disk-native: no proportional
    /// anonymous structures and schema within budget.
    #[must_use]
    pub fn is_disk_native_graph(&self) -> bool {
        self.anonymous_proportional_structure_bytes == 0 && !self.schema_budget_exceeded()
    }
}
