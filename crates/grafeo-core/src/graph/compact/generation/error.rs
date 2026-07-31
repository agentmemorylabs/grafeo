//! Typed errors for source-true generation.

use std::fmt;

/// Fail-closed generation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationError {
    /// Duplicate original node ID.
    DuplicateNodeId(u64),
    /// Duplicate original edge ID.
    DuplicateEdgeId(u64),
    /// Endpoint original node ID is missing from the identity map.
    MissingEndpoint {
        /// Edge original ID.
        edge_id: u64,
        /// Missing node original ID.
        node_id: u64,
        /// Whether the missing endpoint was the source.
        is_source: bool,
    },
    /// Endpoint resolved to a different node table than the relationship expects.
    WrongTableEndpoint {
        /// Edge original ID.
        edge_id: u64,
        /// Node original ID.
        node_id: u64,
        /// Expected table id.
        expected_table: u16,
        /// Actual table id.
        actual_table: u16,
        /// Whether the wrong endpoint was the source.
        is_source: bool,
    },
    /// A wire-width counter overflowed (`u32`/`u16`).
    WireWidthOverflow {
        /// What overflowed.
        what: &'static str,
        /// Observed count.
        count: u64,
        /// Maximum allowed.
        max: u64,
    },
    /// Schema or input inconsistency.
    InvalidInput(String),
    /// Budget reservation failed.
    BudgetExceeded {
        /// Counter name.
        counter: &'static str,
        /// Requested bytes.
        requested: u64,
        /// Limit.
        limit: u64,
    },
    /// Cancellation observed.
    Cancelled,
    /// Run/merge I/O failure.
    Io(String),
    /// Serialization / codec failure.
    Codec(String),
    /// A null/missing value was supplied where production column codecs
    /// require a concrete value. W0 fails closed rather than mapping
    /// null/missing to an empty string / zero default (packet §8).
    NullValue {
        /// Column/owner context (table + key).
        context: String,
        /// Row position of the null value.
        row: usize,
    },
    /// A value kind that no production v5 column codec can represent
    /// (e.g. `Bytes`, `Timestamp`, `List`, `Map`, `Path`, counters). W0 fails
    /// closed rather than Debug-stringifying it (packet §8).
    UnsupportedValue {
        /// `Value` variant name.
        kind: &'static str,
        /// Column/owner context.
        context: String,
    },
    /// A column mixes value kinds that production cannot encode losslessly
    /// into one codec (e.g. `String` + `Int64`, `Bool` + `Int64`, `Vector` +
    /// scalar). W0 fails closed rather than Display/Debug coercing to string.
    MixedColumnTypes {
        /// Column/owner context.
        context: String,
        /// Distinct value-kind names observed.
        kinds: Vec<&'static str>,
    },
}

impl fmt::Display for GenerationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateNodeId(id) => write!(f, "duplicate original node id {id}"),
            Self::DuplicateEdgeId(id) => write!(f, "duplicate original edge id {id}"),
            Self::MissingEndpoint {
                edge_id,
                node_id,
                is_source,
            } => write!(
                f,
                "edge {edge_id}: missing {} endpoint node {node_id}",
                if *is_source { "source" } else { "destination" }
            ),
            Self::WrongTableEndpoint {
                edge_id,
                node_id,
                expected_table,
                actual_table,
                is_source,
            } => write!(
                f,
                "edge {edge_id}: {} endpoint node {node_id} on table {actual_table}, expected {expected_table}",
                if *is_source { "source" } else { "destination" }
            ),
            Self::WireWidthOverflow { what, count, max } => {
                write!(f, "{what} count {count} exceeds wire max {max}")
            }
            Self::InvalidInput(msg) => write!(f, "invalid generation input: {msg}"),
            Self::BudgetExceeded {
                counter,
                requested,
                limit,
            } => write!(
                f,
                "generation budget exceeded on {counter}: requested {requested}, limit {limit}"
            ),
            Self::Cancelled => write!(f, "generation cancelled"),
            Self::Io(msg) => write!(f, "generation I/O error: {msg}"),
            Self::Codec(msg) => write!(f, "generation codec error: {msg}"),
            Self::NullValue { context, row } => write!(
                f,
                "null/missing value at row {row} in {context}: production column codecs require a concrete value"
            ),
            Self::UnsupportedValue { kind, context } => write!(
                f,
                "unsupported value kind {kind} in {context}: no production v5 column codec represents it"
            ),
            Self::MixedColumnTypes { context, kinds } => write!(
                f,
                "mixed value kinds {:?} in {context}: cannot encode losslessly into one production codec",
                kinds
            ),
        }
    }
}

impl std::error::Error for GenerationError {}
