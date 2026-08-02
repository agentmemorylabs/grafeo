//! Typed generation input identities, graph rows, and record sources.

use super::error::GenerationError;
use grafeo_common::types::{EdgeId, NodeId, PropertyKey, Value};
use grafeo_common::utils::hash::FxHashMap;

/// Original writable-namespace node identity (may be sparse/large).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OriginalNodeId(u64);

impl OriginalNodeId {
    /// Wraps a raw original node id.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw u64.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Converts to production [`NodeId`].
    #[must_use]
    pub fn to_node_id(self) -> NodeId {
        NodeId::new(self.0)
    }
}

impl From<u64> for OriginalNodeId {
    fn from(v: u64) -> Self {
        Self::new(v)
    }
}

/// Original writable-namespace edge identity (may be sparse/large).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OriginalEdgeId(u64);

impl OriginalEdgeId {
    /// Wraps a raw original edge id.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw u64.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Converts to production [`EdgeId`].
    #[must_use]
    pub fn to_edge_id(self) -> EdgeId {
        EdgeId::new(self.0)
    }
}

impl From<u64> for OriginalEdgeId {
    fn from(v: u64) -> Self {
        Self::new(v)
    }
}

/// Declared relationship table schema used for endpoint table checks.
#[derive(Debug, Clone)]
pub struct RelSchemaDecl {
    /// Relationship type name.
    pub edge_type: String,
    /// Expected source node label / table.
    pub src_label: String,
    /// Expected destination node label / table.
    pub dst_label: String,
}

impl RelSchemaDecl {
    /// Constructs a schema declaration.
    #[must_use]
    pub fn new(
        edge_type: impl Into<String>,
        src_label: impl Into<String>,
        dst_label: impl Into<String>,
    ) -> Self {
        Self {
            edge_type: edge_type.into(),
            src_label: src_label.into(),
            dst_label: dst_label.into(),
        }
    }
}

/// One node row in the generation input.
#[derive(Debug, Clone)]
pub struct GenerationNode {
    /// Original sparse ID.
    pub id: OriginalNodeId,
    /// Canonical logical label vector: sorted (UTF-8 byte-lexicographic),
    /// deduplicated, and non-empty (G-EM0.5b D0.8.0 item 1).
    ///
    /// The source-true contract carries **all** of a node's logical labels.
    /// No source adapter may select one "primary" label and discard the rest.
    /// The physical table is the first canonical label (`labels[0]`); the
    /// writer emits every logical membership into the mapped label-membership
    /// companion segment.
    pub labels: Vec<String>,
    /// Properties keyed by name. Absence of a key means the row does not
    /// carry that property; a present `Value::Null` is a stored null.
    pub properties: FxHashMap<PropertyKey, Value>,
}

impl GenerationNode {
    /// Convenience constructor for a single-label node.
    ///
    /// # Panics
    ///
    /// Never panics; a single label always satisfies the canonical contract.
    #[must_use]
    pub fn new(id: impl Into<OriginalNodeId>, label: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            labels: vec![label.into()],
            properties: FxHashMap::default(),
        }
    }

    /// Constructs a node from an explicit label set, canonicalizing it
    /// (sorted + deduplicated).
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::InvalidInput`] when the label set is empty.
    pub fn with_labels(
        id: impl Into<OriginalNodeId>,
        labels: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, GenerationError> {
        let mut canonical: Vec<String> = labels.into_iter().map(Into::into).collect();
        canonical.sort();
        canonical.dedup();
        if canonical.is_empty() {
            return Err(GenerationError::InvalidInput(
                "GenerationNode must carry at least one label".into(),
            ));
        }
        Ok(Self {
            id: id.into(),
            labels: canonical,
            properties: FxHashMap::default(),
        })
    }

    /// Returns the canonical physical-table label (`labels[0]`).
    ///
    /// This is the deterministic physical-grouping rule of D0.8.0 item 2; it
    /// is **not** a substitute for the full logical label set.
    #[must_use]
    pub fn physical_label(&self) -> &str {
        self.labels[0].as_str()
    }

    /// Validates the canonical label contract (sorted, deduped, non-empty).
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError::InvalidInput`] on a violation.
    pub fn validate_labels(&self) -> Result<(), GenerationError> {
        if self.labels.is_empty() {
            return Err(GenerationError::InvalidInput(
                "GenerationNode labels must be non-empty".into(),
            ));
        }
        for w in self.labels.windows(2) {
            if w[1] <= w[0] {
                return Err(GenerationError::InvalidInput(format!(
                    "GenerationNode labels not canonical (sorted+deduped): {:?}",
                    self.labels
                )));
            }
        }
        Ok(())
    }

    /// Adds a property.
    #[must_use]
    pub fn with_prop(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.properties.insert(PropertyKey::new(key), value.into());
        self
    }
}

/// One complete edge record in the generation input.
#[derive(Debug, Clone)]
pub struct GenerationEdge {
    /// Original sparse edge ID.
    pub id: OriginalEdgeId,
    /// Source original node ID.
    pub src: OriginalNodeId,
    /// Destination original node ID.
    pub dst: OriginalNodeId,
    /// Relationship type / table edge type.
    pub edge_type: String,
    /// Properties (kept with the edge through sort).
    pub properties: FxHashMap<PropertyKey, Value>,
}

impl GenerationEdge {
    /// Convenience constructor.
    #[must_use]
    pub fn new(
        id: impl Into<OriginalEdgeId>,
        src: impl Into<OriginalNodeId>,
        dst: impl Into<OriginalNodeId>,
        edge_type: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            src: src.into(),
            dst: dst.into(),
            edge_type: edge_type.into(),
            properties: FxHashMap::default(),
        }
    }

    /// Adds a property.
    #[must_use]
    pub fn with_prop(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.properties.insert(PropertyKey::new(key), value.into());
        self
    }
}

/// Bounded streaming node record source (W0-A2).
pub trait NodeRecordSource {
    /// Streams the next node record, or returns `Ok(None)` at clean EOF.
    ///
    /// # Errors
    /// Returns [`GenerationError`] on stream reading failure.
    fn next_node(&mut self) -> Result<Option<GenerationNode>, GenerationError>;
}

/// Bounded streaming edge record source (W0-A2).
pub trait EdgeRecordSource {
    /// Streams the next edge record, or returns `Ok(None)` at clean EOF.
    ///
    /// # Errors
    /// Returns [`GenerationError`] on stream reading failure.
    fn next_edge(&mut self) -> Result<Option<GenerationEdge>, GenerationError>;
}

/// Slice-backed node source adapter for test and fixture use.
#[derive(Debug)]
pub struct SliceNodeSource<'a> {
    slice: &'a [GenerationNode],
    cursor: usize,
}

impl<'a> SliceNodeSource<'a> {
    /// Wraps a slice of generation nodes.
    #[must_use]
    pub fn new(slice: &'a [GenerationNode]) -> Self {
        Self { slice, cursor: 0 }
    }
}

impl NodeRecordSource for SliceNodeSource<'_> {
    fn next_node(&mut self) -> Result<Option<GenerationNode>, GenerationError> {
        if self.cursor < self.slice.len() {
            let n = self.slice[self.cursor].clone();
            self.cursor += 1;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }
}

/// Slice-backed edge source adapter for test and fixture use.
#[derive(Debug)]
pub struct SliceEdgeSource<'a> {
    slice: &'a [GenerationEdge],
    cursor: usize,
}

impl<'a> SliceEdgeSource<'a> {
    /// Wraps a slice of generation edges.
    #[must_use]
    pub fn new(slice: &'a [GenerationEdge]) -> Self {
        Self { slice, cursor: 0 }
    }
}

impl EdgeRecordSource for SliceEdgeSource<'_> {
    fn next_edge(&mut self) -> Result<Option<GenerationEdge>, GenerationError> {
        if self.cursor < self.slice.len() {
            let e = self.slice[self.cursor].clone();
            self.cursor += 1;
            Ok(Some(e))
        } else {
            Ok(None)
        }
    }
}

/// Full generation input graph.
#[derive(Default)]
pub struct GenerationInput {
    /// Nodes (buffered or empty if using a streaming source).
    pub nodes: Vec<GenerationNode>,
    /// Edges (buffered or empty if using a streaming source).
    pub edges: Vec<GenerationEdge>,
    /// Optional relationship schema declarations for wrong-table checks.
    pub rel_schemas: Vec<RelSchemaDecl>,
    node_cursor: usize,
    edge_cursor: usize,
}

impl NodeRecordSource for GenerationInput {
    fn next_node(&mut self) -> Result<Option<GenerationNode>, GenerationError> {
        if self.node_cursor < self.nodes.len() {
            let n = self.nodes[self.node_cursor].clone();
            self.node_cursor += 1;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }
}

impl EdgeRecordSource for GenerationInput {
    fn next_edge(&mut self) -> Result<Option<GenerationEdge>, GenerationError> {
        if self.edge_cursor < self.edges.len() {
            let e = self.edges[self.edge_cursor].clone();
            self.edge_cursor += 1;
            Ok(Some(e))
        } else {
            Ok(None)
        }
    }
}

impl std::fmt::Debug for GenerationInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenerationInput")
            .field("nodes_count", &self.nodes.len())
            .field("edges_count", &self.edges.len())
            .field("rel_schemas", &self.rel_schemas)
            .finish()
    }
}

impl GenerationInput {
    /// Empty input.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates node streaming source.
    #[must_use]
    pub fn node_source(&self) -> SliceNodeSource<'_> {
        SliceNodeSource::new(&self.nodes)
    }

    /// Creates edge streaming source.
    #[must_use]
    pub fn edge_source(&self) -> SliceEdgeSource<'_> {
        SliceEdgeSource::new(&self.edges)
    }

    /// Adds a node.
    #[must_use]
    pub fn node(mut self, node: GenerationNode) -> Self {
        self.nodes.push(node);
        self
    }

    /// Adds an edge.
    #[must_use]
    pub fn edge(mut self, edge: GenerationEdge) -> Self {
        self.edges.push(edge);
        self
    }

    /// Declares a relationship schema (enables wrong-table fail-closed checks).
    #[must_use]
    pub fn rel_schema(mut self, decl: RelSchemaDecl) -> Self {
        self.rel_schemas.push(decl);
        self
    }
}
