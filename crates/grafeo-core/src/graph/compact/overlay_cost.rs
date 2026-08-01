//! Retained-capacity cost estimation for overlay mutations (G-EM0.5a).
//!
//! Admission must account *retained* capacity, not only logical payload: a
//! mutation retains the node/edge object, its labels/type string, its property
//! map, and the bookkeeping set entries that track it. These estimators give a
//! deterministic, allocation-free upper-ish bound the admission controller can
//! charge without inspecting allocator internals.
//!
//! Estimates are deliberately conservative (round up) so the hard limit is a
//! real ceiling on retained overlay bytes, not an under-count.

use std::mem::size_of;

use arcstr::ArcStr;
use grafeo_common::types::Value;

/// Per-entity fixed overhead (id, flags, map headers, set slots).
const NODE_FIXED_OVERHEAD: usize = 64;
/// Per-edge fixed overhead (id, src, dst, type handle, map headers, set slots).
const EDGE_FIXED_OVERHEAD: usize = 80;
/// Per-property-map-entry overhead (key handle + value enum slot + hash slot).
const PROPERTY_ENTRY_OVERHEAD: usize = 48;
/// Per dirty/deletion set entry (id + hash slot).
const SET_ENTRY_BYTES: usize = 16;

/// Estimates the retained bytes of a single `Value`.
///
/// Recurses into containers (`List`, `Map`, `Path`) and charges the backing
/// storage of `String`/`Bytes`/`Vector`. `Arc`-shared payloads are charged
/// once per estimate; the overlay retains at least one strong reference, so
/// this is a sound retained-capacity charge.
#[must_use]
pub fn value_retained_bytes(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Int64(_) | Value::Float64(_) => size_of::<Value>(),
        Value::String(s) => size_of::<Value>() + s.len(),
        Value::Bytes(b) => size_of::<Value>() + b.len(),
        Value::Vector(v) => size_of::<Value>() + v.len() * size_of::<f32>(),
        Value::Timestamp(_)
        | Value::Date(_)
        | Value::Time(_)
        | Value::Duration(_)
        | Value::ZonedDatetime(_) => size_of::<Value>() + 32,
        Value::List(items) => {
            size_of::<Value>() + items.iter().map(value_retained_bytes).sum::<usize>()
        }
        Value::Map(map) => {
            size_of::<Value>()
                + map
                    .iter()
                    .map(|(k, v)| k.as_str().len() + value_retained_bytes(v))
                    .sum::<usize>()
        }
        Value::Path { nodes, edges } => {
            size_of::<Value>()
                + nodes.iter().map(value_retained_bytes).sum::<usize>()
                + edges.iter().map(value_retained_bytes).sum::<usize>()
        }
        Value::GCounter(map) => {
            size_of::<Value>()
                + map
                    .keys()
                    .map(|k| k.len() + size_of::<u64>())
                    .sum::<usize>()
        }
        Value::OnCounter { pos, neg } => {
            size_of::<Value>()
                + pos
                    .keys()
                    .map(|k| k.len() + size_of::<u64>())
                    .sum::<usize>()
                + neg
                    .keys()
                    .map(|k| k.len() + size_of::<u64>())
                    .sum::<usize>()
        }
        // `Value` is `#[non_exhaustive]`: charge a conservative fixed bound for
        // any variant added upstream so admission stays a sound ceiling.
        _ => size_of::<Value>() + 64,
    }
}

/// Estimates retained bytes for creating a node with the given labels.
///
/// Charges the fixed node overhead plus each label string. Property cost is
/// charged separately via [`property_retained_bytes`] as properties are set.
#[must_use]
pub fn node_creation_retained_bytes(labels: &[&str]) -> usize {
    NODE_FIXED_OVERHEAD + labels.iter().map(|l| l.len()).sum::<usize>() + SET_ENTRY_BYTES // dirty_node_ids entry
}

/// Estimates retained bytes for creating an edge of the given type.
#[must_use]
pub fn edge_creation_retained_bytes(edge_type: &str) -> usize {
    EDGE_FIXED_OVERHEAD + edge_type.len() + SET_ENTRY_BYTES // dirty_edge_ids entry
}

/// Estimates retained bytes for one property key/value pair.
#[must_use]
pub fn property_retained_bytes(key: &str, value: &Value) -> usize {
    PROPERTY_ENTRY_OVERHEAD + key.len() + value_retained_bytes(value)
}

/// Estimates retained bytes for a base-deletion set entry (node or edge).
#[must_use]
pub const fn deletion_entry_retained_bytes() -> usize {
    SET_ENTRY_BYTES
}

/// Estimates retained bytes contributed by a label string handle.
#[must_use]
pub fn label_retained_bytes(label: &ArcStr) -> usize {
    label.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use grafeo_common::types::Value;

    #[test]
    fn scalar_values_charge_at_least_their_size() {
        assert!(value_retained_bytes(&Value::Int64(1)) >= size_of::<Value>());
        assert!(value_retained_bytes(&Value::Bool(true)) >= size_of::<Value>());
    }

    #[test]
    fn string_value_charges_payload() {
        let v = Value::String(arcstr::ArcStr::from("hello world"));
        assert!(value_retained_bytes(&v) >= size_of::<Value>() + 11);
    }

    #[test]
    fn vector_value_charges_floats() {
        let v = Value::Vector(vec![0.0f32; 384].into());
        assert!(value_retained_bytes(&v) >= size_of::<Value>() + 384 * size_of::<f32>());
    }

    #[test]
    fn nested_list_recurses() {
        let inner = Value::String(arcstr::ArcStr::from("abcdef"));
        let v = Value::List(vec![inner, Value::Int64(7)].into());
        let bytes = value_retained_bytes(&v);
        assert!(bytes >= size_of::<Value>() * 2 + 6);
    }

    #[test]
    fn node_and_edge_creation_are_nonzero_and_monotonic() {
        let small = node_creation_retained_bytes(&["A"]);
        let big = node_creation_retained_bytes(&["A", "VeryLongLabelName"]);
        assert!(big > small);
        assert!(edge_creation_retained_bytes("KNOWS") > 0);
    }

    #[test]
    fn property_cost_grows_with_key_and_value() {
        let k = "documentation_json";
        let small = property_retained_bytes(k, &Value::Int64(1));
        let big =
            property_retained_bytes(k, &Value::String(arcstr::ArcStr::from("x".repeat(4096))));
        assert!(big > small);
    }
}
