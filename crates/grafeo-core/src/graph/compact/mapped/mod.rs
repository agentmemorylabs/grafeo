//! Mapped CompactStore v5 views and ownership accounting (G-EM0.2).
//!
//! Proportional graph structures (CSR arrays, string dictionaries, ID
//! lookups, column bodies, zone maps) are exposed as checked views over
//! a retained container mapping. Bounded schema/owner metadata may live
//! on the anonymous heap inside the declared budgets.

pub(crate) mod accounting;
pub(crate) mod directory;
pub(crate) mod id_lookup;
pub(crate) mod string_dict;
pub(crate) mod views;

pub use accounting::{
    CompactMemoryAccounting, SCHEMA_OWNER_BUDGET_BYTES, default_schema_owner_budget,
};
pub use directory::{
    DIRECTORY_ENTRY_LEN, FORMAT_VERSION_V5, HEADER_LEN, SegmentDirectory, SegmentEntry,
    SegmentKind, V5_HEADER_LEN, parse_segment_directory, parse_v5_header, slice_segment_checked,
    validate_segment_range,
};
pub use id_lookup::{
    MappedEdgeIdLookup, MappedNodeIdLookup, write_edge_id_record, write_node_id_record,
};
pub use string_dict::{MappedStringDictionary, build_string_segments};
pub use views::{U32View, U64View, read_u16_le, read_u32_le, read_u64_le};

#[cfg(test)]
#[path = "mapped_graph_tests.rs"]
mod mapped_graph_tests;
