//! Mapped CompactStore v5/v6 views and ownership accounting (G-EM0.2).
//!
//! Proportional graph structures (CSR arrays, string dictionaries, ID
//! lookups, column bodies, zone maps) are exposed as checked views over
//! a retained container mapping. Bounded schema/owner metadata may live
//! on the anonymous heap inside the declared budgets.

pub(crate) mod accounting;
pub(crate) mod code_index;
pub(crate) mod directory;
pub(crate) mod id_index;
pub(crate) mod id_lookup;
pub(crate) mod label_membership;
pub(crate) mod payload_version;
pub(crate) mod presence;
pub(crate) mod string_dict;
pub(crate) mod views;
pub(crate) mod zone_maps;

pub use accounting::{
    CompactMemoryAccounting, SCHEMA_OWNER_BUDGET_BYTES, default_schema_owner_budget,
};
pub use code_index::{CODE_INDEX_RECORD_LEN, DictionaryCodeIndex, build_dictionary_code_index};
pub use directory::{
    DIRECTORY_ENTRY_LEN, FORMAT_VERSION_V5, HEADER_LEN, SegmentDirectory, SegmentEntry,
    SegmentKind, V5_HEADER_LEN, layout_flags, parse_segment_directory, parse_v5_header,
    slice_segment_checked, validate_segment_range,
};
pub use id_index::{
    ID_INDEX_RECORD_LEN, MappedNodeIdIndex, id_index_record_bytes, write_id_index_record,
};
pub use id_lookup::{
    MappedEdgeIdLookup, MappedNodeIdLookup, write_edge_id_record, write_node_id_record,
};
pub use label_membership::{
    LabelMembership, LabelMembershipView, MEMBERSHIP_HEADER_LEN, MEMBERSHIP_RECORD_LEN,
    write_membership_segment,
};
pub use payload_version::{
    BLOCK_INDEX_RECORD_LEN_V5, BLOCK_INDEX_RECORD_LEN_V6, BlockIndexRecord, DISC_F32_VECTOR_WIDE,
    DISC_I8_VECTOR_WIDE, DirectoryEntryFields, FORMAT_VERSION_V6, PayloadVersion,
    PayloadVersionPolicy, WireOverflow, read_block_index_record, read_directory_entry,
    vector_body_header, write_block_index_record, write_directory_entry,
};
pub use presence::{
    PRESENCE_RECORD_HEADER_LEN, RowBitmapView, bitmap_bytes, pack_bits, unpack_bits,
    write_null_segment, write_presence_segment,
};
pub use string_dict::{MappedStringDictionary, build_string_segments};
pub use views::{U32View, U64View, read_u16_le, read_u32_le, read_u64_le};
pub use zone_maps::{
    TABLE_ZONE_BLOCK_SENTINEL, ZONE_MAP_RECORD_LEN, build_zone_map_segments, parse_block_zone_maps,
    parse_table_zone_maps, write_zone_map_record,
};

#[cfg(test)]
#[path = "mapped_graph_tests.rs"]
mod mapped_graph_tests;
