//! Bounded external two-pass global dictionary (G-EM0.5b Phase 0).
//!
//! Pass 0: append every string use `(string_bytes, use_kind, owner_key)` to a
//! bounded external spool. No dedup in RAM.
//!
//! Pass 1: external-sort occurrences by `(string_bytes, use_kind, owner_key)`,
//! dedup adjacent bytes, assign deterministic UTF-8 byte-lexicographic `u32`
//! codes; stream `StringOffsets`, `StringBytes`, and one `DictionaryCodeIndex`
//! record per code into sinks; emit a spillable `(use_kind, owner_key) → code`
//! remap for pass 2.
//!
//! Phase 0 implements the machinery; Phase 2's streaming builder drives it.

use crate::graph::compact::generation::budget::{GenerationBudget, GenerationMetrics};
use crate::graph::compact::generation::error::GenerationError;
use crate::graph::compact::generation::runs::{
    CancelToken, ExternalRunMerger, ExternalRunSink, SortRecord,
};
use crate::graph::compact::mapped::CODE_INDEX_RECORD_LEN;

/// String occurrence kinds for the global dictionary sort key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum StringUseKind {
    /// Node table label.
    Label = 0,
    /// Property/column key.
    PropertyKey = 1,
    /// Edge type.
    EdgeType = 2,
    /// Dictionary-encoded column value.
    DictValue = 3,
    /// Zone-map string bound.
    ZoneString = 4,
}

/// One string occurrence pushed during pass 0.
#[derive(Debug, Clone)]
pub struct StringOccurrence {
    /// The string bytes (UTF-8).
    pub string: Vec<u8>,
    /// What kind of use.
    pub use_kind: StringUseKind,
    /// Owner key (table_id + column_id or similar, opaque).
    pub owner_key: Vec<u8>,
}

impl StringOccurrence {
    /// Encodes as a [`SortRecord`] with key = `string || use_kind || owner_key`.
    ///
    /// This gives lexicographic sort by `(string_bytes, use_kind, owner_key)`
    /// as required by the packet §5.
    #[must_use]
    pub fn to_sort_record(&self) -> SortRecord {
        let mut key = Vec::with_capacity(self.string.len() + 1 + self.owner_key.len());
        key.extend_from_slice(&self.string);
        key.push(self.use_kind as u8);
        key.extend_from_slice(&self.owner_key);
        SortRecord::new(key, Vec::new())
    }

    /// Decodes the string bytes from a sort record key.
    ///
    /// The key layout is `string || use_kind_byte || owner_key`. Because
    /// `use_kind` is a single byte and `owner_key` is opaque, we recover the
    /// string by stripping the trailing `1 + owner_key_len` bytes. For Phase 0
    /// (no owner_key), the string is `key[..key.len()-1]`.
    #[must_use]
    pub fn string_from_key(key: &[u8], owner_key_len: usize) -> &[u8] {
        let end = key.len().saturating_sub(1 + owner_key_len);
        &key[..end]
    }
}

/// Result of pass 1: sorted unique strings with assigned codes.
#[derive(Debug, Clone)]
pub struct BoundedDictionary {
    /// Unique strings in UTF-8 byte-lexicographic order (code = index).
    pub strings: Vec<String>,
}

impl BoundedDictionary {
    /// Number of unique strings.
    #[must_use]
    pub fn len(&self) -> usize {
        self.strings.len()
    }

    /// Returns `true` when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }

    /// Lookup code for a string (linear scan; use code index for large dicts).
    #[must_use]
    pub fn code(&self, s: &str) -> Option<u32> {
        self.strings.iter().position(|x| x == s).map(|i| {
            #[allow(clippy::cast_possible_truncation)]
            let code = i as u32;
            code
        })
    }

    /// Builds `StringOffsets` segment body bytes (u64 LE offset array with
    /// trailing sentinel). Byte-identical to `build_string_segments().0`.
    #[must_use]
    pub fn string_offsets_bytes(&self) -> Vec<u8> {
        let mut offsets = Vec::with_capacity((self.strings.len() + 1) * 8);
        let mut byte_pos = 0u64;
        for s in &self.strings {
            offsets.extend_from_slice(&byte_pos.to_le_bytes());
            byte_pos += s.len() as u64;
        }
        offsets.extend_from_slice(&byte_pos.to_le_bytes());
        offsets
    }

    /// Builds `StringBytes` segment body bytes (concatenated UTF-8).
    /// Byte-identical to `build_string_segments().1`.
    #[must_use]
    pub fn string_bytes_body(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for s in &self.strings {
            bytes.extend_from_slice(s.as_bytes());
        }
        bytes
    }

    /// Builds `DictionaryCodeIndex` segment body bytes (sorted by string
    /// content). Byte-identical to `build_dictionary_code_index`.
    #[must_use]
    pub fn code_index_bytes(&self) -> Vec<u8> {
        let mut records: Vec<(u64, u32, u32, &str)> = Vec::with_capacity(self.strings.len());
        let mut offset = 0u64;
        for (i, s) in self.strings.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let code = i as u32;
            #[allow(clippy::cast_possible_truncation)]
            let len = s.len() as u32;
            records.push((offset, len, code, s.as_str()));
            offset += u64::from(len);
        }
        records.sort_by(|a, b| a.3.cmp(b.3));
        let mut out = Vec::with_capacity(records.len() * CODE_INDEX_RECORD_LEN);
        for (off, len, code, _) in records {
            out.extend_from_slice(&off.to_le_bytes());
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&code.to_le_bytes());
        }
        out
    }
}

/// Pass 0 + Pass 1 driver: pushes occurrences into an external run sink,
/// merges, dedupes, and assigns codes.
pub struct DictionaryPassDriver {
    budget: GenerationBudget,
    metrics: GenerationMetrics,
    cancel: Option<CancelToken>,
}

impl DictionaryPassDriver {
    /// Creates a driver with the given budget.
    #[must_use]
    pub fn new(budget: GenerationBudget) -> Self {
        Self {
            budget,
            metrics: GenerationMetrics::default(),
            cancel: None,
        }
    }

    /// Attaches a cancel token.
    #[must_use]
    pub fn with_cancel(mut self, token: CancelToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Metrics snapshot.
    #[must_use]
    pub fn metrics(&self) -> &GenerationMetrics {
        &self.metrics
    }

    /// Runs pass 0 (push occurrences) + pass 1 (sort, dedup, assign codes).
    ///
    /// Uses the provided sink and merger for external sort. For Phase 0
    /// tests, these are `InMemoryRunSink` / `InMemoryRunMerger`.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::WireWidthOverflow`] when unique string
    /// count exceeds `u32::MAX`, or propagates sink/merger errors.
    pub fn build(
        &mut self,
        occurrences: Vec<StringOccurrence>,
        sink: &mut dyn ExternalRunSink,
        merger: &mut dyn ExternalRunMerger,
    ) -> Result<BoundedDictionary, GenerationError> {
        self.budget.validate()?;

        // ── Pass 0: push occurrences into the sink ────────────────────
        for occ in &occurrences {
            if let Some(c) = &self.cancel {
                c.check()?;
            }
            sink.push(occ.to_sort_record())?;
        }
        let runs = sink.finish()?;

        // ── Pass 1: merge, dedup, assign codes ────────────────────────
        let mut sorted_keys: Vec<Vec<u8>> = Vec::new();
        merger.merge_all(
            &runs,
            &self.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            &mut |rec: &SortRecord| {
                sorted_keys.push(rec.key.clone());
                Ok(())
            },
        )?;

        // Dedup adjacent strings (keys are sorted by string || use_kind || owner_key,
        // so identical strings are adjacent regardless of use_kind/owner_key).
        let mut unique_strings: Vec<String> = Vec::new();
        let mut prev_string: Option<&[u8]> = None;
        for key in &sorted_keys {
            // For Phase 0, owner_key is empty, so string = key[..key.len()-1].
            // The general case strips 1 + owner_key_len from the end.
            let string_bytes = StringOccurrence::string_from_key(key, 0);
            let is_dup = prev_string.is_some_and(|p| p == string_bytes);
            if !is_dup {
                let s = std::str::from_utf8(string_bytes)
                    .map_err(|_| {
                        GenerationError::Codec("invalid UTF-8 in dictionary string".into())
                    })?
                    .to_string();
                unique_strings.push(s);
                prev_string = Some(string_bytes);
            }
        }

        if unique_strings.len() > u32::MAX as usize {
            return Err(GenerationError::WireWidthOverflow {
                what: "global_string_dictionary",
                count: unique_strings.len() as u64,
                max: u64::from(u32::MAX),
            });
        }

        self.metrics.global_string_count = unique_strings.len() as u64;

        // Cleanup sink temp data.
        sink.cleanup();
        merger.cleanup();

        Ok(BoundedDictionary {
            strings: unique_strings,
        })
    }
}
