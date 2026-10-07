//! Read-only audit for values a compaction before AMH #183 may have
//! fabricated.
//!
//! Before #183, building a `CompactStore` (`compact()`, a tier drain, a
//! generation built from a compact base) stored an absent property as its
//! column's type default (`""`, `0`, `false`, a full-width zero vector), and
//! `compact()` could swap the targets and properties of a source's edges.
//! Once stored, such a default reads back as a real value: it cannot be told
//! apart from one written on purpose, and recompacting does not repair it.
//!
//! [`GrafeoDB::audit_fabricated_defaults`] reports the candidates by reading
//! the stored values themselves (not a correlated column such as
//! `embedding_dimensions`):
//!
//! - every nonempty vector whose components are all `0.0`, with whether the
//!   vector index on that label and property serves the node. Inline
//!   properties are read, and every registered vector index's property is
//!   also read through the serving accessor, which falls back to a ForceDisk
//!   spill file (a spilled column is absent from the property map). Nothing
//!   is reloaded or rebuilt;
//! - every empty string under the keys the caller names;
//! - every edge whose type, endpoints or properties differ from a trusted
//!   baseline of edge identities (a swap keeps counts, endpoints and CSR
//!   consistency, so only a baseline can show it).
//!
//! A hit is suspicious, not proof. Repair means rebuilding from the trusted
//! source, vector index topology included.

use std::collections::BTreeMap;

use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};

/// An edge as a trusted source says it is: identity, type, endpoints and,
/// when given, its exact properties.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeIdentity {
    /// Edge id.
    pub id: EdgeId,
    /// Edge type.
    pub edge_type: String,
    /// Source node.
    pub src: NodeId,
    /// Destination node.
    pub dst: NodeId,
    /// The edge's exact properties, or `None` to check identity only.
    pub properties: Option<BTreeMap<String, Value>>,
}

/// A nonempty, all-zero vector property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZeroVector {
    /// The node.
    pub node: NodeId,
    /// The audited label the node was found under.
    pub label: String,
    /// The property.
    pub property: String,
    /// Its dimensions.
    pub dimensions: usize,
    /// Whether the vector index on `label`/`property` serves the node, or
    /// `None` when there is no such index.
    pub indexed: Option<bool>,
}

/// An empty string under an audited key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmptyString {
    /// The node.
    pub node: NodeId,
    /// The audited label the node was found under.
    pub label: String,
    /// The property.
    pub property: String,
}

/// An edge that differs from its baseline.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeMismatch {
    /// What the baseline says.
    pub expected: EdgeIdentity,
    /// What the graph holds (`properties` always filled), or `None` when the
    /// edge is missing.
    pub found: Option<EdgeIdentity>,
}

/// What [`GrafeoDB::audit_fabricated_defaults`] found.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DefaultsAudit {
    /// Nodes read.
    pub nodes_scanned: usize,
    /// Baseline edges compared.
    pub edges_checked: usize,
    /// Nonempty all-zero vectors.
    pub zero_vectors: Vec<ZeroVector>,
    /// Empty strings under the audited keys.
    pub empty_strings: Vec<EmptyString>,
    /// Edges that differ from the baseline.
    pub edge_mismatches: Vec<EdgeMismatch>,
}

impl DefaultsAudit {
    /// Whether nothing suspicious was found.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.zero_vectors.is_empty()
            && self.empty_strings.is_empty()
            && self.edge_mismatches.is_empty()
    }
}

impl super::GrafeoDB {
    /// Audits the graph, read-only, for values a compaction before AMH #183
    /// may have fabricated (see the [module docs](self)).
    ///
    /// Reads every node of `labels` (every label when empty): reports each
    /// nonempty all-zero vector, and each empty string under `string_keys`.
    /// Compares each edge of `edge_baseline` with the graph. Reads through
    /// the same view as queries (the layered base plus overlay on a
    /// compacted database or generation root).
    #[must_use]
    pub fn audit_fabricated_defaults(
        &self,
        labels: &[&str],
        string_keys: &[&str],
        edge_baseline: &[EdgeIdentity],
    ) -> DefaultsAudit {
        let store = self.graph_store();
        let mut audit = DefaultsAudit::default();
        let all_labels;
        let labels: Vec<&str> = if labels.is_empty() {
            all_labels = store.all_labels();
            all_labels.iter().map(String::as_str).collect()
        } else {
            labels.to_vec()
        };
        let string_keys: Vec<PropertyKey> =
            string_keys.iter().map(|k| PropertyKey::new(*k)).collect();

        for &label in &labels {
            for id in store.nodes_by_label(label) {
                let Some(node) = store.get_node(id) else {
                    continue;
                };
                audit.nodes_scanned += 1;
                let mut zero_vectors: Vec<(&PropertyKey, usize)> = Vec::new();
                for (key, value) in node.properties.iter() {
                    match value {
                        Value::Vector(v) if !v.is_empty() && v.iter().all(|x| *x == 0.0) => {
                            zero_vectors.push((key, v.len()));
                        }
                        Value::String(s) if s.is_empty() && string_keys.contains(key) => {
                            audit.empty_strings.push(EmptyString {
                                node: id,
                                label: label.to_string(),
                                property: key.as_str().to_string(),
                            });
                        }
                        _ => {}
                    }
                }
                for (key, dimensions) in zero_vectors {
                    #[cfg(feature = "vector-index")]
                    let indexed = self
                        .lpg_store()
                        .get_vector_index(label, key.as_str())
                        .map(|index| index.contains(id));
                    #[cfg(not(feature = "vector-index"))]
                    let indexed = None;
                    audit.zero_vectors.push(ZeroVector {
                        node: id,
                        label: label.to_string(),
                        property: key.as_str().to_string(),
                        dimensions,
                        indexed,
                    });
                }
            }
        }

        // Vectors the property map does not show: a ForceDisk spill drains
        // an indexed column out of it. Read every registered index's
        // property through the serving (spill-aware) accessor.
        #[cfg(feature = "vector-index")]
        for (key, index) in self.lpg_store().vector_index_entries() {
            let Some((label, property)) = key.split_once(':') else {
                continue;
            };
            if !labels.contains(&label) {
                continue;
            }
            let accessor = self.make_vector_accessor(label, property);
            for id in store.nodes_by_label(label) {
                let reported = audit
                    .zero_vectors
                    .iter()
                    .any(|z| z.node == id && z.label == label && z.property == property);
                if reported {
                    continue;
                }
                if let Some(v) =
                    grafeo_core::index::vector::VectorAccessor::get_vector(&accessor, id)
                    && !v.is_empty()
                    && v.iter().all(|x| *x == 0.0)
                {
                    audit.zero_vectors.push(ZeroVector {
                        node: id,
                        label: label.to_string(),
                        property: property.to_string(),
                        dimensions: v.len(),
                        indexed: Some(index.contains(id)),
                    });
                }
            }
        }

        for expected in edge_baseline {
            audit.edges_checked += 1;
            let found = store.get_edge(expected.id).map(|edge| EdgeIdentity {
                id: edge.id,
                edge_type: edge.edge_type.to_string(),
                src: edge.src,
                dst: edge.dst,
                properties: Some(
                    edge.properties
                        .iter()
                        .map(|(k, v)| (k.as_str().to_string(), v.clone()))
                        .collect(),
                ),
            });
            let matches = found.as_ref().is_some_and(|f| {
                f.edge_type == expected.edge_type
                    && f.src == expected.src
                    && f.dst == expected.dst
                    && expected
                        .properties
                        .as_ref()
                        .is_none_or(|want| f.properties.as_ref() == Some(want))
            });
            if !matches {
                audit.edge_mismatches.push(EdgeMismatch {
                    expected: expected.clone(),
                    found,
                });
            }
        }
        audit
    }
}
