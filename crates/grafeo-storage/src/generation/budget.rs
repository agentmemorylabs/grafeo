//! Generation budget (G-EM0.W0-A1, W0 contract §8).
//!
//! Every build receives an explicit, non-optional [`GenerationBudget`]. Each
//! input buffer, output buffer, sort arena, oversized-record buffer, schema
//! allocation, and temporary file reserves against the appropriate counter
//! before allocation/extension and releases on drop/delete. A reservation
//! failure returns a typed budget error before crossing the limit.

/// Full generation build budget (W0 contract §8).
///
/// All fields are mandatory; there are no hidden defaults. The acceptance
/// benchmark uses the values documented in the contract.
#[derive(Debug, Clone, Copy)]
pub struct GenerationBudget {
    /// Maximum anonymous (RSS-anon) memory for the build process.
    pub max_anon_bytes: u64,
    /// Maximum temporary disk bytes across all live runs, merge outputs,
    /// remap runs, partial generation, and unpublished final generation.
    pub max_temp_bytes: u64,
    /// Bytes of unsorted arena accumulated in memory before flushing a run.
    pub sort_run_bytes: u64,
    /// I/O buffer capacity for run readers/writers.
    pub io_buffer_bytes: u64,
    /// Merge fan-in. Must be in `2..=64`. Acceptance fixtures use 32.
    pub merge_fan_in: u32,
    /// Maximum single record (key + payload + framing) before rejection.
    pub max_record_bytes: u64,
    /// Maximum retained schema metadata (labels, types, keys, directories).
    pub max_schema_bytes: u64,
}

impl GenerationBudget {
    /// Acceptance configuration per W0 §8.
    ///
    /// Headroom invariant (mirrors core `acceptance_linux`): 32 MiB runs so
    /// `2 * sort_run_bytes + io_buffer_bytes + spool_slack < max_anon_bytes`.
    /// 64 MiB runs packed two arenas to exactly 128 MiB with zero slack for
    /// the flush I/O overlap charge (`BudgetExceeded` at ~135266228).
    #[must_use]
    pub const fn acceptance() -> Self {
        Self {
            max_anon_bytes: 128 * 1024 * 1024,
            max_temp_bytes: 0, // caller supplies from fixture disk envelope
            sort_run_bytes: 32 * 1024 * 1024,
            io_buffer_bytes: 1024 * 1024,
            merge_fan_in: 32,
            max_record_bytes: 8 * 1024 * 1024,
            max_schema_bytes: 16 * 1024 * 1024,
        }
    }

    /// Validate budget bounds. `max_temp_bytes` may be zero (caller-supplied).
    ///
    /// # Errors
    /// Returns [`GenerationBudgetError`] if `merge_fan_in` is outside `2..=64`,
    /// or if any byte limit is zero (except `max_temp_bytes`).
    pub fn validate(&self) -> Result<(), GenerationBudgetError> {
        if !(2..=64).contains(&self.merge_fan_in) {
            return Err(GenerationBudgetError::InvalidMergeFanIn(self.merge_fan_in));
        }
        if self.sort_run_bytes == 0 {
            return Err(GenerationBudgetError::ZeroField("sort_run_bytes"));
        }
        if self.io_buffer_bytes == 0 {
            return Err(GenerationBudgetError::ZeroField("io_buffer_bytes"));
        }
        if self.max_record_bytes == 0 {
            return Err(GenerationBudgetError::ZeroField("max_record_bytes"));
        }
        if self.max_schema_bytes == 0 {
            return Err(GenerationBudgetError::ZeroField("max_schema_bytes"));
        }
        if self.max_anon_bytes == 0 {
            return Err(GenerationBudgetError::ZeroField("max_anon_bytes"));
        }
        Ok(())
    }
}

/// Budget validation error.
#[derive(Debug)]
pub enum GenerationBudgetError {
    /// `merge_fan_in` outside `2..=64`.
    InvalidMergeFanIn(u32),
    /// A byte limit was zero (must be positive).
    ZeroField(&'static str),
}

impl std::fmt::Display for GenerationBudgetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMergeFanIn(v) => {
                write!(f, "merge_fan_in {v} out of range 2..=64")
            }
            Self::ZeroField(name) => write!(f, "budget field '{name}' must be non-zero"),
        }
    }
}

impl std::error::Error for GenerationBudgetError {}

/// External-sort-specific view of [`GenerationBudget`].
///
/// The external sort machinery only needs: run size, temp limit, fan-in,
/// record cap, and I/O buffer. This thin projection avoids passing the full
/// build budget into low-level I/O code.
#[derive(Debug, Clone, Copy)]
pub struct ExternalSortBudget {
    /// Bytes of unsorted arena before flush.
    pub sort_run_bytes: u64,
    /// Maximum temporary disk bytes.
    pub max_temp_bytes: u64,
    /// Maximum anonymous (in-memory arena) bytes.
    pub max_anon_bytes: u64,
    /// Merge fan-in (`2..=64`).
    pub merge_fan_in: u32,
    /// Maximum single record bytes.
    pub max_record_bytes: u64,
    /// I/O buffer bytes.
    pub io_buffer_bytes: usize,
}

impl ExternalSortBudget {
    /// Project from a full [`GenerationBudget`].
    #[must_use]
    pub fn from_budget(b: &GenerationBudget) -> Self {
        Self {
            sort_run_bytes: b.sort_run_bytes,
            max_temp_bytes: b.max_temp_bytes,
            max_anon_bytes: b.max_anon_bytes,
            merge_fan_in: b.merge_fan_in,
            max_record_bytes: b.max_record_bytes,
            io_buffer_bytes: usize::try_from(b.io_buffer_bytes).unwrap_or(usize::MAX),
        }
    }

    /// Test defaults (small buffers to force multi-run / multi-pass).
    #[must_use]
    pub const fn for_tests() -> Self {
        Self {
            sort_run_bytes: 64 * 1024,
            max_temp_bytes: 256 * 1024 * 1024,
            max_anon_bytes: 64 * 1024 * 1024,
            merge_fan_in: 4,
            max_record_bytes: 8 * 1024 * 1024,
            io_buffer_bytes: 64 * 1024,
        }
    }

    /// Validate fan-in range.
    ///
    /// # Errors
    /// Returns [`GenerationBudgetError`] if fan-in is outside `2..=64`.
    pub fn validate(&self) -> Result<(), GenerationBudgetError> {
        if !(2..=64).contains(&self.merge_fan_in) {
            return Err(GenerationBudgetError::InvalidMergeFanIn(self.merge_fan_in));
        }
        Ok(())
    }
}
