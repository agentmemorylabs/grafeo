//! Store dispatch for the session's direct node/edge APIs.
//!
//! On a plain database the direct APIs (`Session::get_node`,
//! `set_node_property`, `delete_edge`, ...) work on the session's concrete
//! [`LpgStore`]. A layered database (a generation root, or a database after
//! `compact()`) splits the graph into an immutable compact *base* and a
//! mutable *overlay*; the session's `LpgStore` is only the overlay, so base
//! elements are reachable solely through the
//! [`LayeredStore`](grafeo_core::graph::compact::layered::LayeredStore).
//! [`DirectStore`] picks the right target so the direct APIs see and mutate
//! base elements with the same store calls queries use.

use std::sync::Arc;

use grafeo_common::types::{EdgeId, EpochId, NodeId, TransactionId, Value};
#[cfg(feature = "compact-store")]
use grafeo_core::graph::GraphStoreSearch;
#[cfg(feature = "compact-store")]
use grafeo_core::graph::compact::layered::LayeredStore;
use grafeo_core::graph::lpg::{Edge, LpgStore, Node};
use grafeo_core::graph::{Direction, GraphStoreMut};

/// Target of a direct node/edge API call.
pub(super) enum DirectStore {
    /// The session's concrete store (plain database, or a named graph).
    Lpg(Arc<LpgStore>),
    /// The default graph of a layered database.
    #[cfg(feature = "compact-store")]
    Layered {
        /// The session's read view (the layered store, or the tier chain
        /// while a generation build is in flight), shared with queries.
        read: Arc<dyn GraphStoreSearch>,
        /// The raw layered store. Not the session's WAL-wrapped write store:
        /// the direct APIs log their own WAL records, and the wrapper's
        /// `*_versioned` methods fall back to the unversioned ones.
        write: Arc<LayeredStore>,
    },
}

impl DirectStore {
    /// Whether this is the layered target.
    pub(super) fn is_layered(&self) -> bool {
        match self {
            Self::Lpg(_) => false,
            #[cfg(feature = "compact-store")]
            Self::Layered { .. } => true,
        }
    }

    pub(super) fn get_node_versioned(
        &self,
        id: NodeId,
        epoch: EpochId,
        transaction_id: TransactionId,
    ) -> Option<Node> {
        match self {
            Self::Lpg(store) => store.get_node_versioned(id, epoch, transaction_id),
            #[cfg(feature = "compact-store")]
            Self::Layered { read, .. } => read.get_node_versioned(id, epoch, transaction_id),
        }
    }

    pub(super) fn get_edge_versioned(
        &self,
        id: EdgeId,
        epoch: EpochId,
        transaction_id: TransactionId,
    ) -> Option<Edge> {
        match self {
            Self::Lpg(store) => store.get_edge_versioned(id, epoch, transaction_id),
            #[cfg(feature = "compact-store")]
            Self::Layered { read, .. } => read.get_edge_versioned(id, epoch, transaction_id),
        }
    }

    pub(super) fn edges_from(&self, node: NodeId, direction: Direction) -> Vec<(NodeId, EdgeId)> {
        match self {
            Self::Lpg(store) => store.edges_from(node, direction).collect(),
            #[cfg(feature = "compact-store")]
            Self::Layered { read, .. } => read.edges_from(node, direction),
        }
    }

    pub(super) fn out_degree(&self, node: NodeId) -> usize {
        match self {
            Self::Lpg(store) => store.out_degree(node),
            #[cfg(feature = "compact-store")]
            Self::Layered { read, .. } => read.out_degree(node),
        }
    }

    pub(super) fn in_degree(&self, node: NodeId) -> usize {
        match self {
            Self::Lpg(store) => store.in_degree(node),
            #[cfg(feature = "compact-store")]
            Self::Layered { read, .. } => read.in_degree(node),
        }
    }

    pub(super) fn set_node_property(
        &self,
        id: NodeId,
        key: &str,
        value: Value,
        transaction_id: Option<TransactionId>,
    ) {
        match (self, transaction_id) {
            (Self::Lpg(store), Some(tid)) => store.set_node_property_versioned(id, key, value, tid),
            (Self::Lpg(store), None) => store.set_node_property(id, key, value),
            #[cfg(feature = "compact-store")]
            (Self::Layered { write, .. }, Some(tid)) => {
                write.set_node_property_versioned(id, key, value, tid);
            }
            #[cfg(feature = "compact-store")]
            (Self::Layered { write, .. }, None) => write.set_node_property(id, key, value),
        }
    }

    pub(super) fn set_edge_property(
        &self,
        id: EdgeId,
        key: &str,
        value: Value,
        transaction_id: Option<TransactionId>,
    ) {
        match (self, transaction_id) {
            (Self::Lpg(store), Some(tid)) => store.set_edge_property_versioned(id, key, value, tid),
            (Self::Lpg(store), None) => store.set_edge_property(id, key, value),
            #[cfg(feature = "compact-store")]
            (Self::Layered { write, .. }, Some(tid)) => {
                write.set_edge_property_versioned(id, key, value, tid);
            }
            #[cfg(feature = "compact-store")]
            (Self::Layered { write, .. }, None) => write.set_edge_property(id, key, value),
        }
    }

    pub(super) fn delete_node(
        &self,
        id: NodeId,
        epoch: EpochId,
        transaction_id: Option<TransactionId>,
    ) -> bool {
        match (self, transaction_id) {
            (Self::Lpg(store), Some(tid)) => store.delete_node_versioned(id, epoch, tid),
            (Self::Lpg(store), None) => store.delete_node(id),
            #[cfg(feature = "compact-store")]
            (Self::Layered { write, .. }, Some(tid)) => write.delete_node_versioned(id, epoch, tid),
            #[cfg(feature = "compact-store")]
            (Self::Layered { write, .. }, None) => write.delete_node(id),
        }
    }

    pub(super) fn delete_edge(
        &self,
        id: EdgeId,
        epoch: EpochId,
        transaction_id: Option<TransactionId>,
    ) -> bool {
        match (self, transaction_id) {
            (Self::Lpg(store), Some(tid)) => store.delete_edge_versioned(id, epoch, tid),
            (Self::Lpg(store), None) => store.delete_edge(id),
            #[cfg(feature = "compact-store")]
            (Self::Layered { write, .. }, Some(tid)) => write.delete_edge_versioned(id, epoch, tid),
            #[cfg(feature = "compact-store")]
            (Self::Layered { write, .. }, None) => write.delete_edge(id),
        }
    }
}
