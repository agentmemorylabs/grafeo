//! Generation budgets and phase metrics.

use super::error::GenerationError;

/// Explicit non-optional generation budget (packet Decision D).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationBudget {
    /// Max anonymous working set charge.
    pub max_anon_bytes: u64,
    /// Max temporary disk bytes (runs + partial outputs).
    pub max_temp_bytes: u64,
    /// Bytes per sorted run before flush.
    pub sort_run_bytes: u64,
    /// I/O buffer size.
    pub io_buffer_bytes: u64,
    /// Merge fan-in cap (`2..=64`).
    pub merge_fan_in: u32,
    /// Max single canonical record size.
    pub max_record_bytes: u64,
    /// Max retained schema metadata bytes.
    pub max_schema_bytes: u64,
}

impl GenerationBudget {
    /// Acceptance-fixture defaults (small enough for unit tests).
    #[must_use]
    pub const fn for_tests() -> Self {
        Self {
            max_anon_bytes: 64 * 1024 * 1024,
            max_temp_bytes: 256 * 1024 * 1024,
            sort_run_bytes: 64 * 1024,
            io_buffer_bytes: 64 * 1024,
            merge_fan_in: 4,
            max_record_bytes: 8 * 1024 * 1024,
            max_schema_bytes: 16 * 1024 * 1024,
        }
    }

    /// Linux acceptance configuration from the packet (section 8).
    #[must_use]
    pub const fn acceptance_linux() -> Self {
        Self {
            max_anon_bytes: 128 * 1024 * 1024,
            max_temp_bytes: 4 * 1024 * 1024 * 1024,
            sort_run_bytes: 64 * 1024 * 1024,
            io_buffer_bytes: 1024 * 1024,
            merge_fan_in: 32,
            max_record_bytes: 8 * 1024 * 1024,
            max_schema_bytes: 16 * 1024 * 1024,
        }
    }

    /// Validates fan-in and non-zero buffers.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::InvalidInput`] when bounds are violated.
    pub fn validate(&self) -> Result<(), GenerationError> {
        if !(2..=64).contains(&self.merge_fan_in) {
            return Err(GenerationError::InvalidInput(format!(
                "merge_fan_in must be in 2..=64, got {}",
                self.merge_fan_in
            )));
        }
        if self.sort_run_bytes == 0 || self.io_buffer_bytes == 0 {
            return Err(GenerationError::InvalidInput(
                "sort_run_bytes and io_buffer_bytes must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

/// Truthful counters reported by generation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GenerationMetrics {
    /// Peak temporary disk bytes charged.
    pub temp_bytes_peak: u64,
    /// Current temporary disk charge.
    pub temp_bytes_current: u64,
    /// Peak anonymous working-set bytes charged.
    pub anon_bytes_peak: u64,
    /// Current anonymous working-set charge.
    pub anon_bytes_current: u64,
    /// Peak retained schema-metadata bytes charged.
    pub schema_bytes_peak: u64,
    /// Current retained schema-metadata charge.
    pub schema_bytes_current: u64,
    /// Number of sorted runs formed.
    pub run_count: u64,
    /// Max simultaneously open run files during merge.
    pub max_open_runs: u64,
    /// Number of recursive merge passes.
    pub merge_passes: u64,
    /// Records processed.
    pub record_count: u64,
    /// Global dictionary unique string count.
    pub global_string_count: u64,
}

impl GenerationMetrics {
    /// Reserves `bytes` against the temp budget.
    ///
    /// # Errors
    ///
    /// [`GenerationError::BudgetExceeded`] when the reservation would cross the limit.
    pub fn reserve_temp(&mut self, bytes: u64, limit: u64) -> Result<(), GenerationError> {
        let next =
            self.temp_bytes_current
                .checked_add(bytes)
                .ok_or(GenerationError::BudgetExceeded {
                    counter: "temp_bytes",
                    requested: bytes,
                    limit,
                })?;
        if next > limit {
            return Err(GenerationError::BudgetExceeded {
                counter: "temp_bytes",
                requested: next,
                limit,
            });
        }
        self.temp_bytes_current = next;
        self.temp_bytes_peak = self.temp_bytes_peak.max(next);
        Ok(())
    }

    /// Releases previously reserved temporary bytes.
    pub fn release_temp(&mut self, bytes: u64) {
        self.temp_bytes_current = self.temp_bytes_current.saturating_sub(bytes);
    }

    /// Reserves `bytes` against the anonymous working-set budget.
    ///
    /// # Errors
    ///
    /// [`GenerationError::BudgetExceeded`] when the reservation would cross the limit.
    pub fn reserve_anon(&mut self, bytes: u64, limit: u64) -> Result<(), GenerationError> {
        let next =
            self.anon_bytes_current
                .checked_add(bytes)
                .ok_or(GenerationError::BudgetExceeded {
                    counter: "max_anon_bytes",
                    requested: bytes,
                    limit,
                })?;
        if next > limit {
            return Err(GenerationError::BudgetExceeded {
                counter: "max_anon_bytes",
                requested: next,
                limit,
            });
        }
        self.anon_bytes_current = next;
        self.anon_bytes_peak = self.anon_bytes_peak.max(next);
        Ok(())
    }

    /// Releases previously reserved anonymous bytes.
    pub fn release_anon(&mut self, bytes: u64) {
        self.anon_bytes_current = self.anon_bytes_current.saturating_sub(bytes);
    }

    /// Reserves `bytes` against the retained schema-metadata budget.
    ///
    /// # Errors
    ///
    /// [`GenerationError::BudgetExceeded`] when the reservation would cross the limit.
    pub fn reserve_schema(&mut self, bytes: u64, limit: u64) -> Result<(), GenerationError> {
        let next = self.schema_bytes_current.checked_add(bytes).ok_or(
            GenerationError::BudgetExceeded {
                counter: "max_schema_bytes",
                requested: bytes,
                limit,
            },
        )?;
        if next > limit {
            return Err(GenerationError::BudgetExceeded {
                counter: "max_schema_bytes",
                requested: next,
                limit,
            });
        }
        self.schema_bytes_current = next;
        self.schema_bytes_peak = self.schema_bytes_peak.max(next);
        Ok(())
    }

    /// Releases previously reserved schema bytes.
    pub fn release_schema(&mut self, bytes: u64) {
        self.schema_bytes_current = self.schema_bytes_current.saturating_sub(bytes);
    }
}
