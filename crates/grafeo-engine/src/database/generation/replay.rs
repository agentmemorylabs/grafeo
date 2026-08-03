//! H-ADOPT.3 Phase B generation-tail WAL replay.
//!
//! Replays the selected generation's post-boundary WAL into a fresh layered
//! overlay. The stream has one serial-transaction pending buffer, applies only
//! committed records in durable order, and never mutates the WAL.

use std::path::Path;
use std::sync::Arc;

use grafeo_common::types::{EdgeId, EpochId, NodeId, TransactionId};
use grafeo_core::graph::compact::layered::LayeredStore;
use grafeo_core::graph::lpg::LpgStore;
#[cfg(feature = "triple-store")]
use grafeo_core::graph::rdf::{RdfStore, Term, Triple};
use grafeo_core::graph::{GraphStore, GraphStoreMut};
use grafeo_storage::generation::wal_cursor::{ReplayFrame, WalCursorError, replay_stream_from};
use grafeo_storage::wal::WalRecord;

use crate::catalog::{
    Catalog, EdgeTypeDefinition, GraphTypeDefinition, NodeTypeDefinition, ProcedureDefinition,
    PropertyDataType, TypeConstraint, TypedProperty,
};
use crate::database::generation::manifest::WalBoundary;
use crate::transaction::TransactionManager;

/// Fresh runtime targets restored from the selected generation's WAL tail.
pub struct ReplayTarget<'a> {
    /// Layered compact base plus fresh writable overlay.
    pub layered: &'a LayeredStore,
    /// Fresh root catalog receiving schema DDL records.
    pub catalog: &'a Catalog,
    /// Optional RDF store receiving RDF WAL records.
    #[cfg(feature = "triple-store")]
    pub rdf_store: Option<&'a RdfStore>,
    /// Transaction manager receiving epoch, commit, and ID-floor restore state.
    pub transaction_manager: &'a TransactionManager,
}

/// Classification of the active WAL file's terminal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalTailClass {
    /// The stream ended at a committed boundary.
    Clean,
    /// Complete uncommitted records at the active-file tail were discarded.
    TornTail {
        /// Active WAL sequence.
        seq: u64,
        /// First partial or absent frame position after the discarded tail.
        byte_offset: u64,
    },
}

/// Observable result of generation-tail replay.
#[derive(Debug)]
pub struct ReplayReport {
    /// Number of committed data/schema records applied to replay targets.
    pub applied_records: u64,
    /// Number of transaction commit records successfully flushed.
    pub committed_transactions: u64,
    /// Active-file terminal classification.
    pub tail: WalTailClass,
    /// Maximum of the boundary epoch and replayed epoch advances.
    pub final_epoch: EpochId,
    /// Maximum of the boundary transaction ID and replayed commit IDs.
    pub max_transaction_id: TransactionId,
}

/// Failure scanning or applying a generation WAL tail.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// Typed Phase A WAL scanner failure.
    #[error("wal scan: {0}")]
    Scan(#[from] WalCursorError),
    /// A decoded record could not be applied to its target.
    #[error("apply at seq={seq} offset={offset}: {detail}")]
    Apply {
        /// WAL sequence containing the record.
        seq: u64,
        /// Frame's length-prefix byte offset.
        offset: u64,
        /// Failure detail.
        detail: String,
    },
    /// A decoded record violates a recovery invariant and cannot be skipped.
    #[error("non-recoverable record at seq={seq} offset={offset}: {detail}")]
    NonRecoverable {
        /// WAL sequence containing the record.
        seq: u64,
        /// Frame's length-prefix byte offset.
        offset: u64,
        /// Invariant violation detail.
        detail: String,
    },
}

#[derive(Default)]
struct LpgReplayCursor {
    current_graph: Option<String>,
    named_target: Option<Arc<LpgStore>>,
}

fn apply_error(frame: &ReplayFrame, detail: impl Into<String>) -> ReplayError {
    ReplayError::Apply {
        seq: frame.log_sequence,
        offset: frame.byte_offset,
        detail: detail.into(),
    }
}

fn non_recoverable(frame: &ReplayFrame, detail: impl Into<String>) -> ReplayError {
    ReplayError::NonRecoverable {
        seq: frame.log_sequence,
        offset: frame.byte_offset,
        detail: detail.into(),
    }
}

fn node_exists(target: &ReplayTarget<'_>, cursor: &LpgReplayCursor, id: NodeId) -> bool {
    match cursor.named_target.as_deref() {
        Some(store) => store.get_node(id).is_some(),
        None => target.layered.get_node(id).is_some(),
    }
}

fn edge_exists(target: &ReplayTarget<'_>, cursor: &LpgReplayCursor, id: EdgeId) -> bool {
    match cursor.named_target.as_deref() {
        Some(store) => store.get_edge(id).is_some(),
        None => target.layered.get_edge(id).is_some(),
    }
}

#[cfg(feature = "triple-store")]
fn rdf_store<'a>(
    target: &'a ReplayTarget<'_>,
    frame: &ReplayFrame,
) -> Result<&'a RdfStore, ReplayError> {
    target
        .rdf_store
        .ok_or_else(|| apply_error(frame, "RDF WAL record has no replay target"))
}

#[cfg(feature = "triple-store")]
fn parse_rdf_triple(
    frame: &ReplayFrame,
    subject: &str,
    predicate: &str,
    object: &str,
) -> Result<Triple, ReplayError> {
    let subject = Term::from_ntriples(subject)
        .ok_or_else(|| non_recoverable(frame, "invalid N-Triples subject term"))?;
    let predicate = Term::from_ntriples(predicate)
        .ok_or_else(|| non_recoverable(frame, "invalid N-Triples predicate term"))?;
    let object = Term::from_ntriples(object)
        .ok_or_else(|| non_recoverable(frame, "invalid N-Triples object term"))?;
    Ok(Triple::new(subject, predicate, object))
}

#[allow(clippy::too_many_lines)]
fn apply_record(
    frame: &ReplayFrame,
    target: &ReplayTarget<'_>,
    cursor: &mut LpgReplayCursor,
) -> Result<(), ReplayError> {
    match &frame.record {
        WalRecord::CreateNode { id, labels } => {
            let labels: Vec<&str> = labels.iter().map(String::as_str).collect();
            match cursor.named_target.as_deref() {
                Some(store) => store
                    .create_node_with_id(*id, &labels)
                    .map_err(|error| apply_error(frame, error.to_string()))?,
                None => target
                    .layered
                    .replay_create_node_with_id(*id, &labels)
                    .map_err(|error| apply_error(frame, error.to_string()))?,
            }
        }
        WalRecord::DeleteNode { id } => match cursor.named_target.as_deref() {
            Some(store) => {
                store.delete_node(*id);
            }
            None => {
                target.layered.delete_node(*id);
            }
        },
        WalRecord::CreateEdge {
            id,
            src,
            dst,
            edge_type,
        } => match cursor.named_target.as_deref() {
            Some(store) => store
                .create_edge_with_id(*id, *src, *dst, edge_type)
                .map_err(|error| apply_error(frame, error.to_string()))?,
            None => target
                .layered
                .replay_create_edge_with_id(*id, *src, *dst, edge_type)
                .map_err(|error| apply_error(frame, error.to_string()))?,
        },
        WalRecord::DeleteEdge { id } => match cursor.named_target.as_deref() {
            Some(store) => {
                store.delete_edge(*id);
            }
            None => {
                target.layered.delete_edge(*id);
            }
        },
        WalRecord::SetNodeProperty { id, key, value } => {
            if !node_exists(target, cursor, *id) {
                return Err(apply_error(
                    frame,
                    format!("node {id} does not exist before property write"),
                ));
            }
            match cursor.named_target.as_deref() {
                Some(store) => store.set_node_property(*id, key, value.clone()),
                None => target.layered.set_node_property(*id, key, value.clone()),
            }
        }
        WalRecord::SetEdgeProperty { id, key, value } => {
            if !edge_exists(target, cursor, *id) {
                return Err(apply_error(
                    frame,
                    format!("edge {id} does not exist before property write"),
                ));
            }
            match cursor.named_target.as_deref() {
                Some(store) => store.set_edge_property(*id, key, value.clone()),
                None => target.layered.set_edge_property(*id, key, value.clone()),
            }
        }
        WalRecord::RemoveNodeProperty { id, key } => match cursor.named_target.as_deref() {
            Some(store) => {
                store.remove_node_property(*id, key);
            }
            None => {
                target.layered.remove_node_property(*id, key);
            }
        },
        WalRecord::RemoveEdgeProperty { id, key } => match cursor.named_target.as_deref() {
            Some(store) => {
                store.remove_edge_property(*id, key);
            }
            None => {
                target.layered.remove_edge_property(*id, key);
            }
        },
        WalRecord::AddNodeLabel { id, label } => match cursor.named_target.as_deref() {
            Some(store) => {
                store.add_label(*id, label);
            }
            None => {
                target.layered.add_label(*id, label);
            }
        },
        WalRecord::RemoveNodeLabel { id, label } => match cursor.named_target.as_deref() {
            Some(store) => {
                store.remove_label(*id, label);
            }
            None => {
                target.layered.remove_label(*id, label);
            }
        },
        WalRecord::CreateNodeType {
            name,
            properties,
            constraints,
        } => {
            let definition = NodeTypeDefinition {
                name: name.clone(),
                properties: properties
                    .iter()
                    .map(|(name, type_name, nullable)| TypedProperty {
                        name: name.clone(),
                        data_type: PropertyDataType::from_type_name(type_name),
                        nullable: *nullable,
                        default_value: None,
                    })
                    .collect(),
                constraints: constraints
                    .iter()
                    .map(|(kind, properties)| match kind.as_str() {
                        "unique" => TypeConstraint::Unique(properties.clone()),
                        "primary_key" => TypeConstraint::PrimaryKey(properties.clone()),
                        "not_null" if !properties.is_empty() => {
                            TypeConstraint::NotNull(properties[0].clone())
                        }
                        _ => TypeConstraint::Unique(properties.clone()),
                    })
                    .collect(),
                parent_types: Vec::new(),
            };
            let _ = target.catalog.register_node_type(definition);
        }
        WalRecord::DropNodeType { name } => {
            let _ = target.catalog.drop_node_type(name);
        }
        WalRecord::CreateEdgeType {
            name,
            properties,
            constraints,
        } => {
            let definition = EdgeTypeDefinition {
                name: name.clone(),
                properties: properties
                    .iter()
                    .map(|(name, type_name, nullable)| TypedProperty {
                        name: name.clone(),
                        data_type: PropertyDataType::from_type_name(type_name),
                        nullable: *nullable,
                        default_value: None,
                    })
                    .collect(),
                constraints: constraints
                    .iter()
                    .map(|(kind, properties)| match kind.as_str() {
                        "unique" => TypeConstraint::Unique(properties.clone()),
                        "primary_key" => TypeConstraint::PrimaryKey(properties.clone()),
                        "not_null" if !properties.is_empty() => {
                            TypeConstraint::NotNull(properties[0].clone())
                        }
                        _ => TypeConstraint::Unique(properties.clone()),
                    })
                    .collect(),
                source_node_types: Vec::new(),
                target_node_types: Vec::new(),
            };
            let _ = target.catalog.register_edge_type_def(definition);
        }
        WalRecord::DropEdgeType { name } => {
            let _ = target.catalog.drop_edge_type_def(name);
        }
        WalRecord::CreateIndex { .. } => {
            // Indexes are derived and rebuilt from replayed data on startup.
        }
        WalRecord::DropIndex { .. } => {
            // Indexes are derived and rebuilt from replayed data on startup.
        }
        WalRecord::CreateConstraint { .. } => {
            // Constraints are carried by replayed node/edge type definitions.
        }
        WalRecord::DropConstraint { .. } => {
            // Constraints are carried by replayed node/edge type definitions.
        }
        WalRecord::CreateGraphType {
            name,
            node_types,
            edge_types,
            open,
        } => {
            let definition = GraphTypeDefinition {
                name: name.clone(),
                allowed_node_types: node_types.clone(),
                allowed_edge_types: edge_types.clone(),
                open: *open,
            };
            let _ = target.catalog.register_graph_type(definition);
        }
        WalRecord::DropGraphType { name } => {
            let _ = target.catalog.drop_graph_type(name);
        }
        WalRecord::CreateSchema { name } => {
            let _ = target.catalog.register_schema_namespace(name.clone());
        }
        WalRecord::DropSchema { name } => {
            let _ = target.catalog.drop_schema_namespace(name);
        }
        WalRecord::AlterNodeType { name, alterations } => {
            for (action, property_name, type_name, nullable) in alterations {
                if action == "add" {
                    let property = TypedProperty {
                        name: property_name.clone(),
                        data_type: PropertyDataType::from_type_name(type_name),
                        nullable: *nullable,
                        default_value: None,
                    };
                    let _ = target.catalog.alter_node_type_add_property(name, property);
                } else if action == "drop" {
                    let _ = target
                        .catalog
                        .alter_node_type_drop_property(name, property_name);
                }
            }
        }
        WalRecord::AlterEdgeType { name, alterations } => {
            for (action, property_name, type_name, nullable) in alterations {
                if action == "add" {
                    let property = TypedProperty {
                        name: property_name.clone(),
                        data_type: PropertyDataType::from_type_name(type_name),
                        nullable: *nullable,
                        default_value: None,
                    };
                    let _ = target.catalog.alter_edge_type_add_property(name, property);
                } else if action == "drop" {
                    let _ = target
                        .catalog
                        .alter_edge_type_drop_property(name, property_name);
                }
            }
        }
        WalRecord::AlterGraphType { name, alterations } => {
            for (action, type_name) in alterations {
                if action == "add_node" {
                    let _ = target
                        .catalog
                        .alter_graph_type_add_node_type(name, type_name.clone());
                } else if action == "drop_node" {
                    let _ = target
                        .catalog
                        .alter_graph_type_drop_node_type(name, type_name);
                } else if action == "add_edge" {
                    let _ = target
                        .catalog
                        .alter_graph_type_add_edge_type(name, type_name.clone());
                } else if action == "drop_edge" {
                    let _ = target
                        .catalog
                        .alter_graph_type_drop_edge_type(name, type_name);
                }
            }
        }
        WalRecord::CreateProcedure {
            name,
            params,
            returns,
            body,
        } => {
            let definition = ProcedureDefinition {
                name: name.clone(),
                params: params.clone(),
                returns: returns.clone(),
                body: body.clone(),
            };
            let _ = target.catalog.register_procedure(definition);
        }
        WalRecord::DropProcedure { name } => {
            let _ = target.catalog.drop_procedure(name);
        }
        WalRecord::CreateNamedGraph { name } => {
            target
                .layered
                .overlay_store()
                .create_graph(name)
                .map_err(|error| apply_error(frame, error.to_string()))?;
        }
        WalRecord::DropNamedGraph { name } => {
            target.layered.overlay_store().drop_graph(name);
            if cursor.current_graph.as_deref() == Some(name.as_str()) {
                cursor.current_graph = None;
                cursor.named_target = None;
            }
        }
        WalRecord::SwitchGraph { name } => {
            cursor.current_graph.clone_from(name);
            cursor.named_target = match &cursor.current_graph {
                None => None,
                Some(name) => Some(
                    target
                        .layered
                        .overlay_store()
                        .graph_or_create(name)
                        .map_err(|error| apply_error(frame, error.to_string()))?,
                ),
            };
        }
        #[cfg(feature = "triple-store")]
        WalRecord::InsertRdfTriple {
            subject,
            predicate,
            object,
            graph,
        } => {
            let store = rdf_store(target, frame)?;
            let triple = parse_rdf_triple(frame, subject, predicate, object)?;
            match graph {
                Some(name) => {
                    store.graph_or_create(name).insert(triple);
                }
                None => {
                    store.insert(triple);
                }
            }
        }
        #[cfg(feature = "triple-store")]
        WalRecord::DeleteRdfTriple {
            subject,
            predicate,
            object,
            graph,
        } => {
            let store = rdf_store(target, frame)?;
            let triple = parse_rdf_triple(frame, subject, predicate, object)?;
            match graph {
                Some(name) => {
                    store.graph_or_create(name).remove(&triple);
                }
                None => {
                    store.remove(&triple);
                }
            }
        }
        #[cfg(feature = "triple-store")]
        WalRecord::ClearRdfGraph { graph } => {
            rdf_store(target, frame)?.clear_graph(graph.as_deref());
        }
        #[cfg(feature = "triple-store")]
        WalRecord::CreateRdfGraph { name } => {
            rdf_store(target, frame)?.create_graph(name);
        }
        #[cfg(feature = "triple-store")]
        WalRecord::DropRdfGraph { name } => {
            let store = rdf_store(target, frame)?;
            match name {
                None => store.clear(),
                Some(name) => {
                    store.drop_graph(name);
                }
            }
        }
        #[cfg(not(feature = "triple-store"))]
        WalRecord::InsertRdfTriple { .. } => {
            return Err(non_recoverable(
                frame,
                "RDF WAL record requires the triple-store feature",
            ));
        }
        #[cfg(not(feature = "triple-store"))]
        WalRecord::DeleteRdfTriple { .. } => {
            return Err(non_recoverable(
                frame,
                "RDF WAL record requires the triple-store feature",
            ));
        }
        #[cfg(not(feature = "triple-store"))]
        WalRecord::ClearRdfGraph { .. } => {
            return Err(non_recoverable(
                frame,
                "RDF WAL record requires the triple-store feature",
            ));
        }
        #[cfg(not(feature = "triple-store"))]
        WalRecord::CreateRdfGraph { .. } => {
            return Err(non_recoverable(
                frame,
                "RDF WAL record requires the triple-store feature",
            ));
        }
        #[cfg(not(feature = "triple-store"))]
        WalRecord::DropRdfGraph { .. } => {
            return Err(non_recoverable(
                frame,
                "RDF WAL record requires the triple-store feature",
            ));
        }
        WalRecord::TransactionCommit { .. } => {
            return Err(non_recoverable(
                frame,
                "transaction commit reached the data-record applier",
            ));
        }
        WalRecord::TransactionAbort { .. } => {
            return Err(non_recoverable(
                frame,
                "transaction abort reached the data-record applier",
            ));
        }
        WalRecord::Checkpoint { .. } => {
            return Err(non_recoverable(
                frame,
                "checkpoint reached the data-record applier",
            ));
        }
        WalRecord::EpochAdvance { .. } => {
            return Err(non_recoverable(
                frame,
                "epoch advance reached the data-record applier",
            ));
        }
    }
    Ok(())
}

/// Replays committed WAL records after `boundary` into a fresh generation
/// overlay, catalog, RDF store, and transaction-manager recovery state.
///
/// # Errors
///
/// Returns [`ReplayError::Scan`] for typed Phase A scanner failures,
/// [`ReplayError::Apply`] when a committed record cannot be applied, or
/// [`ReplayError::NonRecoverable`] for a decoded stream invariant violation.
#[allow(clippy::too_many_lines)]
pub fn replay_generation_wal(
    wal_dir: &Path,
    boundary: WalBoundary,
    target: &ReplayTarget<'_>,
) -> Result<ReplayReport, ReplayError> {
    let mut stream = replay_stream_from(wal_dir, &boundary.to_cursor())?;
    // Serial-transaction invariant: data records carry no transaction ID, so
    // exactly one pending transaction can exist in the stream.
    let mut pending: Vec<ReplayFrame> = Vec::new();
    let mut awaiting_epoch: Option<TransactionId> = None;
    let mut cursor = LpgReplayCursor::default();
    let mut applied_records = 0u64;
    let mut committed_transactions = 0u64;
    let mut final_epoch = EpochId::new(boundary.overlay_epoch);
    let mut max_transaction_id = TransactionId::new(boundary.transaction_id);

    for frame in stream.by_ref() {
        let frame = frame?;
        match &frame.record {
            WalRecord::TransactionCommit { transaction_id } => {
                if let Some(previous) = awaiting_epoch {
                    return Err(non_recoverable(
                        &frame,
                        format!(
                            "transaction {transaction_id} committed before epoch advance for transaction {previous}"
                        ),
                    ));
                }
                for pending_frame in pending.drain(..) {
                    apply_record(&pending_frame, target, &mut cursor)?;
                    applied_records += 1;
                }
                committed_transactions += 1;
                max_transaction_id = max_transaction_id.max(*transaction_id);
                awaiting_epoch = Some(*transaction_id);
            }
            WalRecord::TransactionAbort { transaction_id } => {
                if let Some(previous) = awaiting_epoch {
                    return Err(non_recoverable(
                        &frame,
                        format!(
                            "transaction {transaction_id} aborted before epoch advance for committed transaction {previous}"
                        ),
                    ));
                }
                pending.clear();
            }
            WalRecord::Checkpoint { transaction_id: _ } => {
                // Metadata pass-through: never flushes or clears the open tx.
            }
            WalRecord::EpochAdvance { epoch } => {
                let Some(transaction_id) = awaiting_epoch.take() else {
                    return Err(non_recoverable(
                        &frame,
                        format!("epoch advance {epoch} has no preceding commit"),
                    ));
                };
                final_epoch = final_epoch.max(*epoch);
                target
                    .transaction_manager
                    .mark_committed(transaction_id, *epoch);
            }
            WalRecord::CreateNode { .. }
            | WalRecord::DeleteNode { .. }
            | WalRecord::CreateEdge { .. }
            | WalRecord::DeleteEdge { .. }
            | WalRecord::SetNodeProperty { .. }
            | WalRecord::SetEdgeProperty { .. }
            | WalRecord::RemoveNodeProperty { .. }
            | WalRecord::RemoveEdgeProperty { .. }
            | WalRecord::AddNodeLabel { .. }
            | WalRecord::RemoveNodeLabel { .. }
            | WalRecord::CreateNodeType { .. }
            | WalRecord::DropNodeType { .. }
            | WalRecord::CreateEdgeType { .. }
            | WalRecord::DropEdgeType { .. }
            | WalRecord::CreateIndex { .. }
            | WalRecord::DropIndex { .. }
            | WalRecord::CreateConstraint { .. }
            | WalRecord::DropConstraint { .. }
            | WalRecord::CreateGraphType { .. }
            | WalRecord::DropGraphType { .. }
            | WalRecord::CreateSchema { .. }
            | WalRecord::DropSchema { .. }
            | WalRecord::AlterNodeType { .. }
            | WalRecord::AlterEdgeType { .. }
            | WalRecord::AlterGraphType { .. }
            | WalRecord::CreateProcedure { .. }
            | WalRecord::DropProcedure { .. }
            | WalRecord::CreateNamedGraph { .. }
            | WalRecord::DropNamedGraph { .. }
            | WalRecord::SwitchGraph { .. }
            | WalRecord::InsertRdfTriple { .. }
            | WalRecord::DeleteRdfTriple { .. }
            | WalRecord::ClearRdfGraph { .. }
            | WalRecord::CreateRdfGraph { .. }
            | WalRecord::DropRdfGraph { .. } => {
                if let Some(previous) = awaiting_epoch {
                    return Err(non_recoverable(
                        &frame,
                        format!(
                            "data record appeared before epoch advance for committed transaction {previous}"
                        ),
                    ));
                }
                pending.push(frame);
            }
        }
    }

    let Some((seq, byte_offset)) = stream.stopped_at() else {
        return Err(ReplayError::NonRecoverable {
            seq: boundary.log_sequence,
            offset: boundary.byte_offset,
            detail: "WAL stream exhausted without a terminal position".to_string(),
        });
    };
    let tail = if pending.is_empty() {
        WalTailClass::Clean
    } else {
        pending.clear();
        WalTailClass::TornTail { seq, byte_offset }
    };

    target.transaction_manager.sync_epoch(final_epoch);
    target
        .transaction_manager
        .restore_transaction_floor(max_transaction_id);
    target.layered.overlay_store().sync_epoch(final_epoch);

    Ok(ReplayReport {
        applied_records,
        committed_transactions,
        tail,
        final_epoch,
        max_transaction_id,
    })
}

#[cfg(test)]
mod tests {
    use grafeo_common::types::{EdgeId, EpochId, NodeId, PropertyKey, TransactionId, Value};
    use grafeo_core::graph::GraphStore;
    use grafeo_core::graph::compact::{from_graph_store_preserving_ids, layered::LayeredStore};
    use grafeo_core::graph::lpg::LpgStore;
    #[cfg(feature = "triple-store")]
    use grafeo_core::graph::rdf::{RdfStore, Term, Triple};
    use grafeo_storage::wal::{WalManager, WalRecord};
    use tempfile::TempDir;

    use crate::catalog::Catalog;
    use crate::database::generation::manifest::WalBoundary;
    use crate::transaction::TransactionManager;

    use super::{ReplayError, ReplayReport, ReplayTarget, WalTailClass, replay_generation_wal};

    struct WalFixture {
        dir: TempDir,
        wal: WalManager,
    }

    impl WalFixture {
        fn new() -> Self {
            let dir = TempDir::new().unwrap();
            let wal = WalManager::open(dir.path()).unwrap();
            Self { dir, wal }
        }

        fn log(&self, record: WalRecord) {
            self.wal.log(&record).unwrap();
        }

        fn committed(&self, records: impl IntoIterator<Item = WalRecord>, tx: u64, epoch: u64) {
            for record in records {
                self.log(record);
            }
            self.log(WalRecord::TransactionCommit {
                transaction_id: TransactionId::new(tx),
            });
            self.log(WalRecord::EpochAdvance {
                epoch: EpochId::new(epoch),
            });
        }
    }

    struct FixtureTarget {
        layered: LayeredStore,
        catalog: Catalog,
        transaction_manager: TransactionManager,
        #[cfg(feature = "triple-store")]
        rdf_store: RdfStore,
    }

    impl FixtureTarget {
        fn new() -> Self {
            let base = LpgStore::new().unwrap();
            let person = base.create_node(&["Person"]);
            let city = base.create_node(&["City"]);
            let obsolete = base.create_node(&["Obsolete"]);
            assert_eq!(person, NodeId::new(0));
            assert_eq!(city, NodeId::new(1));
            assert_eq!(obsolete, NodeId::new(2));
            base.set_node_property(person, "name", Value::from("base-person"));
            let old_edge = base.create_edge(person, city, "OLD");
            assert_eq!(old_edge, EdgeId::new(0));
            base.set_edge_property(old_edge, "active", Value::Bool(true));

            let compact = from_graph_store_preserving_ids(&base).unwrap();
            Self {
                layered: LayeredStore::new(compact, 2, 0).unwrap(),
                catalog: Catalog::new(),
                transaction_manager: TransactionManager::new(),
                #[cfg(feature = "triple-store")]
                rdf_store: RdfStore::new(),
            }
        }

        fn replay_target(&self) -> ReplayTarget<'_> {
            ReplayTarget {
                layered: &self.layered,
                catalog: &self.catalog,
                transaction_manager: &self.transaction_manager,
                #[cfg(feature = "triple-store")]
                rdf_store: Some(&self.rdf_store),
            }
        }
    }

    fn boundary() -> WalBoundary {
        WalBoundary {
            log_sequence: 0,
            byte_offset: 0,
            overlay_epoch: 3,
            transaction_id: 7,
        }
    }

    fn assert_clean_report(
        report: &ReplayReport,
        applied: u64,
        committed: u64,
        epoch: u64,
        max_tx: u64,
    ) {
        assert_eq!(report.applied_records, applied);
        assert_eq!(report.committed_transactions, committed);
        assert_eq!(report.tail, WalTailClass::Clean);
        assert_eq!(report.final_epoch, EpochId::new(epoch));
        assert_eq!(report.max_transaction_id, TransactionId::new(max_tx));
    }

    fn primary_records() -> [WalRecord; 11] {
        [
            WalRecord::CreateNode {
                id: NodeId::new(10),
                labels: vec!["Person".to_string()],
            },
            WalRecord::SetNodeProperty {
                id: NodeId::new(10),
                key: "name".to_string(),
                value: Value::from("Ada"),
            },
            WalRecord::AddNodeLabel {
                id: NodeId::new(10),
                label: "Engineer".to_string(),
            },
            WalRecord::RemoveNodeLabel {
                id: NodeId::new(10),
                label: "Person".to_string(),
            },
            WalRecord::SetNodeProperty {
                id: NodeId::new(0),
                key: "status".to_string(),
                value: Value::from("active"),
            },
            WalRecord::RemoveNodeProperty {
                id: NodeId::new(0),
                key: "name".to_string(),
            },
            WalRecord::CreateEdge {
                id: EdgeId::new(11),
                src: NodeId::new(10),
                dst: NodeId::new(1),
                edge_type: "LIVES_IN".to_string(),
            },
            WalRecord::SetEdgeProperty {
                id: EdgeId::new(11),
                key: "since".to_string(),
                value: Value::Int64(2026),
            },
            WalRecord::RemoveEdgeProperty {
                id: EdgeId::new(11),
                key: "unused".to_string(),
            },
            WalRecord::DeleteEdge { id: EdgeId::new(0) },
            WalRecord::DeleteNode { id: NodeId::new(2) },
        ]
    }

    fn assert_primary_state(target: &FixtureTarget) {
        let created = target.layered.get_node(NodeId::new(10)).unwrap();
        assert_eq!(
            created.properties.get(&PropertyKey::new("name")),
            Some(&Value::from("Ada"))
        );
        assert!(
            created
                .labels
                .iter()
                .any(|label| label.as_str() == "Engineer")
        );
        assert!(
            !created
                .labels
                .iter()
                .any(|label| label.as_str() == "Person")
        );

        let promoted = target.layered.get_node(NodeId::new(0)).unwrap();
        assert_eq!(
            promoted.properties.get(&PropertyKey::new("status")),
            Some(&Value::from("active"))
        );
        assert!(!promoted.properties.contains_key(&PropertyKey::new("name")));

        let created_edge = target.layered.get_edge(EdgeId::new(11)).unwrap();
        assert_eq!(created_edge.src, NodeId::new(10));
        assert_eq!(created_edge.dst, NodeId::new(1));
        assert_eq!(created_edge.edge_type.as_str(), "LIVES_IN");
        assert_eq!(
            created_edge.properties.get(&PropertyKey::new("since")),
            Some(&Value::Int64(2026))
        );
        assert!(target.layered.get_edge(EdgeId::new(0)).is_none());
        assert!(target.layered.get_node(NodeId::new(2)).is_none());
    }

    #[test]
    fn replay_committed_mutations_in_order_and_restores_epoch_and_transaction_floor() {
        let fixture = WalFixture::new();
        fixture.committed(primary_records(), 40, 9);
        let target = FixtureTarget::new();

        let report =
            replay_generation_wal(fixture.dir.path(), boundary(), &target.replay_target()).unwrap();

        assert_clean_report(&report, 11, 1, 9, 40);
        assert_primary_state(&target);
        assert_eq!(target.layered.current_epoch(), EpochId::new(9));
        assert!(target.transaction_manager.begin() > TransactionId::new(40));
    }

    #[test]
    fn replay_abort_discards_buffered_records_without_error() {
        let fixture = WalFixture::new();
        fixture.log(WalRecord::CreateNode {
            id: NodeId::new(20),
            labels: vec!["Aborted".to_string()],
        });
        fixture.log(WalRecord::TransactionAbort {
            transaction_id: TransactionId::new(20),
        });
        let target = FixtureTarget::new();

        let report =
            replay_generation_wal(fixture.dir.path(), boundary(), &target.replay_target()).unwrap();

        assert_clean_report(&report, 0, 0, 3, 7);
        assert!(target.layered.get_node(NodeId::new(20)).is_none());
    }

    #[test]
    fn epoch_advance_without_preceding_commit_is_non_recoverable() {
        let fixture = WalFixture::new();
        fixture.log(WalRecord::EpochAdvance {
            epoch: EpochId::new(8),
        });
        let target = FixtureTarget::new();

        let error = replay_generation_wal(fixture.dir.path(), boundary(), &target.replay_target())
            .unwrap_err();

        assert!(matches!(error, ReplayError::NonRecoverable { .. }));
    }

    #[test]
    fn same_wal_bytes_replay_to_identical_fresh_overlay_state() {
        let fixture = WalFixture::new();
        fixture.committed(primary_records(), 40, 9);
        let first = FixtureTarget::new();
        let second = FixtureTarget::new();

        let first_report =
            replay_generation_wal(fixture.dir.path(), boundary(), &first.replay_target()).unwrap();
        let second_report =
            replay_generation_wal(fixture.dir.path(), boundary(), &second.replay_target()).unwrap();

        assert_clean_report(&first_report, 11, 1, 9, 40);
        assert_clean_report(&second_report, 11, 1, 9, 40);
        for id in [NodeId::new(0), NodeId::new(10)] {
            let first_node = first.layered.get_node(id).unwrap();
            let second_node = second.layered.get_node(id).unwrap();
            assert_eq!(first_node.id, second_node.id);
            assert_eq!(first_node.labels, second_node.labels);
            assert_eq!(first_node.properties, second_node.properties);
        }
        let first_edge = first.layered.get_edge(EdgeId::new(11)).unwrap();
        let second_edge = second.layered.get_edge(EdgeId::new(11)).unwrap();
        assert_eq!(first_edge.id, second_edge.id);
        assert_eq!(first_edge.src, second_edge.src);
        assert_eq!(first_edge.dst, second_edge.dst);
        assert_eq!(first_edge.edge_type, second_edge.edge_type);
        assert_eq!(first_edge.properties, second_edge.properties);
        assert_eq!(
            first.layered.get_node(NodeId::new(2)).is_none(),
            second.layered.get_node(NodeId::new(2)).is_none()
        );
    }

    #[test]
    fn property_write_before_create_in_same_transaction_is_typed_apply_error() {
        let fixture = WalFixture::new();
        fixture.committed(
            [
                WalRecord::SetNodeProperty {
                    id: NodeId::new(99),
                    key: "name".to_string(),
                    value: Value::from("too-early"),
                },
                WalRecord::CreateNode {
                    id: NodeId::new(99),
                    labels: vec!["Person".to_string()],
                },
            ],
            99,
            10,
        );
        let target = FixtureTarget::new();

        let error = replay_generation_wal(fixture.dir.path(), boundary(), &target.replay_target())
            .unwrap_err();

        assert!(matches!(error, ReplayError::Apply { .. }));
        assert!(target.layered.get_node(NodeId::new(99)).is_none());
    }

    #[test]
    fn named_graph_cursor_and_schema_ddl_replay_against_overlay_and_catalog() {
        let fixture = WalFixture::new();
        fixture.committed(
            [
                WalRecord::CreateNamedGraph {
                    name: "tenant".to_string(),
                },
                WalRecord::SwitchGraph {
                    name: Some("tenant".to_string()),
                },
                WalRecord::CreateNode {
                    id: NodeId::new(50),
                    labels: vec!["TenantNode".to_string()],
                },
                WalRecord::SwitchGraph { name: None },
                WalRecord::CreateNodeType {
                    name: "Account".to_string(),
                    properties: vec![("email".to_string(), "string".to_string(), false)],
                    constraints: vec![("unique".to_string(), vec!["email".to_string()])],
                },
                WalRecord::CreateSchema {
                    name: "app".to_string(),
                },
            ],
            50,
            11,
        );
        fixture.committed(
            [
                WalRecord::SwitchGraph {
                    name: Some("tenant".to_string()),
                },
                WalRecord::DropNamedGraph {
                    name: "tenant".to_string(),
                },
                // Dropping the active graph resets the cursor to default.
                WalRecord::CreateNode {
                    id: NodeId::new(51),
                    labels: vec!["DefaultNode".to_string()],
                },
            ],
            51,
            12,
        );
        let target = FixtureTarget::new();

        let report =
            replay_generation_wal(fixture.dir.path(), boundary(), &target.replay_target()).unwrap();

        assert_clean_report(&report, 9, 2, 12, 51);
        assert!(target.layered.overlay_store().graph("tenant").is_none());
        assert!(target.layered.get_node(NodeId::new(50)).is_none());
        assert!(target.layered.get_node(NodeId::new(51)).is_some());
        assert!(target.catalog.get_node_type("Account").is_some());
        assert!(target.catalog.schema_exists("app"));
    }

    #[cfg(feature = "triple-store")]
    #[test]
    fn rdf_lifecycle_arms_apply_well_formed_terms() {
        let fixture = WalFixture::new();
        let first = ("<urn:s:first>", "<urn:p>", "\"first\"");
        let named = ("<urn:s:named>", "<urn:p>", "\"named\"");
        let retained = ("<urn:s:retained>", "<urn:p>", "\"retained\"");
        fixture.committed(
            [
                WalRecord::CreateRdfGraph {
                    name: "urn:g".to_string(),
                },
                WalRecord::InsertRdfTriple {
                    subject: first.0.to_string(),
                    predicate: first.1.to_string(),
                    object: first.2.to_string(),
                    graph: None,
                },
                WalRecord::InsertRdfTriple {
                    subject: named.0.to_string(),
                    predicate: named.1.to_string(),
                    object: named.2.to_string(),
                    graph: Some("urn:g".to_string()),
                },
                WalRecord::DeleteRdfTriple {
                    subject: named.0.to_string(),
                    predicate: named.1.to_string(),
                    object: named.2.to_string(),
                    graph: Some("urn:g".to_string()),
                },
                WalRecord::ClearRdfGraph { graph: None },
                WalRecord::DropRdfGraph {
                    name: Some("urn:g".to_string()),
                },
                WalRecord::InsertRdfTriple {
                    subject: retained.0.to_string(),
                    predicate: retained.1.to_string(),
                    object: retained.2.to_string(),
                    graph: None,
                },
            ],
            60,
            13,
        );
        let target = FixtureTarget::new();

        let report =
            replay_generation_wal(fixture.dir.path(), boundary(), &target.replay_target()).unwrap();

        assert_clean_report(&report, 7, 1, 13, 60);
        let triple = Triple::new(
            Term::from_ntriples(retained.0).unwrap(),
            Term::from_ntriples(retained.1).unwrap(),
            Term::from_ntriples(retained.2).unwrap(),
        );
        assert!(target.rdf_store.contains(&triple));
        assert_eq!(target.rdf_store.len(), 1);
        assert!(target.rdf_store.graph("urn:g").is_none());
    }

    #[cfg(feature = "triple-store")]
    #[test]
    fn malformed_rdf_term_fails_closed_as_non_recoverable() {
        let fixture = WalFixture::new();
        fixture.committed(
            [WalRecord::InsertRdfTriple {
                subject: "not-an-ntriples-term".to_string(),
                predicate: "<urn:p>".to_string(),
                object: "\"value\"".to_string(),
                graph: None,
            }],
            61,
            14,
        );
        let target = FixtureTarget::new();

        let error = replay_generation_wal(fixture.dir.path(), boundary(), &target.replay_target())
            .unwrap_err();

        assert!(matches!(error, ReplayError::NonRecoverable { .. }));
        assert_eq!(target.rdf_store.len(), 0);
    }

    #[test]
    fn uncommitted_active_file_tail_is_discarded_after_committed_prefix() {
        let fixture = WalFixture::new();
        fixture.committed(
            [WalRecord::CreateNode {
                id: NodeId::new(70),
                labels: vec!["Committed".to_string()],
            }],
            70,
            15,
        );
        fixture.log(WalRecord::CreateNode {
            id: NodeId::new(71),
            labels: vec!["Torn".to_string()],
        });
        let target = FixtureTarget::new();

        let report =
            replay_generation_wal(fixture.dir.path(), boundary(), &target.replay_target()).unwrap();

        assert_eq!(report.applied_records, 1);
        assert_eq!(report.committed_transactions, 1);
        assert!(
            matches!(report.tail, WalTailClass::TornTail { seq: 0, byte_offset } if byte_offset > 0)
        );
        assert!(target.layered.get_node(NodeId::new(70)).is_some());
        assert!(target.layered.get_node(NodeId::new(71)).is_none());
    }
}
