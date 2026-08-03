//! H-ADOPT.3 Phase B generation-tail WAL replay.

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

    fn primary_records() -> Vec<WalRecord> {
        vec![
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
