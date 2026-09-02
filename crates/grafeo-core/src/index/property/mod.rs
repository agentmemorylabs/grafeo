//! Property hash-index section and mapped RO views (G-E1.RO).
//!
//! Property indexes map property values → node id sets for O(log N) point
//! lookup. The on-disk section is mmap-friendly: sorted value/id pairs over a
//! retained container mapping, so read-only reopen does not allocate
//! anonymous memory proportional to postings.

mod mapped;
mod section;

pub use mapped::{
    MappedPropertyIndex, MappedPropertyIndexSet, PROPERTY_INDEX_MAGIC, PROPERTY_INDEX_VERSION,
    PropertyIndexMemoryAccounting, PropertyIndexSnapshot, encode_property_index_section,
    parse_property_index_section,
};
pub use section::PropertyIndexSection;
