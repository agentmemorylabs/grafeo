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
    /// Canonical node-table label (single label key for W0 fixtures).
    pub label: String,
    /// Properties keyed by name.
    pub properties: FxHashMap<PropertyKey, Value>,
}

impl GenerationNode {
    /// Convenience constructor.
    #[must_use]
    pub fn new(id: impl Into<OriginalNodeId>, label: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
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

/// Full generation input graph.
#[derive(Default)]
pub struct GenerationInput {
    /// Nodes (buffered or empty if using a streaming source).
    pub nodes: Vec<GenerationNode>,
    /// Edges (buffered or empty if using a streaming source).
    pub edges: Vec<GenerationEdge>,
    /// Optional relationship schema declarations for wrong-table checks.
    pub rel_schemas: Vec<RelSchemaDecl>,
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
