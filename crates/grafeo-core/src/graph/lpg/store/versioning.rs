use super::rollback_cleanup::DiscardedEdge;
use super::{LpgStore, PropertyUndoEntry};
use crate::graph::lpg::{EdgeRecord, NodeRecord};
use grafeo_common::memory::AllocError;
use grafeo_common::types::{EdgeId, EpochId, NodeId, TransactionId};
#[cfg(feature = "tiered-storage")]
use grafeo_common::utils::hash::FxHashMap;
use std::sync::atomic::Ordering;

#[cfg(feature = "temporal")]
use grafeo_common::types::PropertyKey;

#[cfg(not(feature = "tiered-storage"))]
use grafeo_common::mvcc::VersionChain;

#[cfg(feature = "tiered-storage")]
use grafeo_common::mvcc::{ColdVersionRef, HotVersionRef, VersionIndex};

impl LpgStore {
    /// How many node and edge version entries commit
    /// (`finalize_version_epochs`) and rollback (`discard_uncommitted_versions`,
    /// savepoint rollback) have visited since the store was created.
    ///
    /// A test hook: the difference across one commit or rollback shows
    /// whether it cost O(the transaction's changes) or O(the store) (#410).
    #[doc(hidden)]
    #[must_use]
    pub fn transaction_versions_walked(&self) -> u64 {
        self.transaction_versions_walked.load(Ordering::Relaxed)
    }

    /// Adds `n` to [`Self::transaction_versions_walked`].
    pub(super) fn count_versions_walked(&self, n: usize) {
        self.transaction_versions_walked
            .fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Records a change of `transaction_id`, for its commit and rollback.
    ///
    /// Writes outside a transaction (`TransactionId::SYSTEM`) are final when
    /// they happen and record nothing.
    pub(super) fn record_change(&self, transaction_id: TransactionId, entry: PropertyUndoEntry) {
        if transaction_id == TransactionId::SYSTEM {
            return;
        }
        self.property_undo_log
            .write()
            .entry(transaction_id)
            .or_default()
            .push(entry);
    }

    /// Records one change per entry of `entries` (a batch create), under one
    /// lock.
    pub(super) fn record_changes(
        &self,
        transaction_id: TransactionId,
        entries: impl IntoIterator<Item = PropertyUndoEntry>,
    ) {
        if transaction_id == TransactionId::SYSTEM {
            return;
        }
        self.property_undo_log
            .write()
            .entry(transaction_id)
            .or_default()
            .extend(entries);
    }

    /// Discards everything a transaction changed in this store (rollback).
    ///
    /// Replays the transaction's changes in reverse, so the cost is
    /// O(changes), and leaves the counters and statistics as they were.
    /// Entities the transaction created are removed with their secondary
    /// state (labels, label/property/text index entries, property columns,
    /// adjacency, edge-type and live counts).
    #[doc(hidden)]
    pub fn discard_uncommitted_versions(&self, transaction_id: TransactionId) {
        self.rollback_transaction_properties(transaction_id);
    }

    /// Nodes and edges that have a version of `transaction_id`: the ones it
    /// created or deleted.
    fn versioned_by(&self, transaction_id: TransactionId) -> (Vec<NodeId>, Vec<EdgeId>) {
        let mut node_ids = Vec::new();
        let mut edge_ids = Vec::new();
        if let Some(entries) = self.property_undo_log.read().get(&transaction_id) {
            for entry in entries {
                match entry {
                    PropertyUndoEntry::NodeCreated { node_id }
                    | PropertyUndoEntry::NodeDeleted { node_id, .. } => node_ids.push(*node_id),
                    PropertyUndoEntry::EdgeCreated { edge_id }
                    | PropertyUndoEntry::EdgeDeleted { edge_id, .. } => edge_ids.push(*edge_id),
                    PropertyUndoEntry::NodeProperty { .. }
                    | PropertyUndoEntry::EdgeProperty { .. }
                    | PropertyUndoEntry::LabelAdded { .. }
                    | PropertyUndoEntry::LabelRemoved { .. } => {}
                }
            }
        }
        (node_ids, edge_ids)
    }

    /// Makes a transaction's versions visible at `commit_epoch` (commit).
    ///
    /// Walks the transaction's changes, so the cost is O(changes). Also
    /// advances the store's epoch so non-transactional reads can see the
    /// newly committed versions.
    #[doc(hidden)]
    pub fn finalize_version_epochs(&self, transaction_id: TransactionId, commit_epoch: EpochId) {
        let (node_ids, edge_ids) = self.versioned_by(transaction_id);
        self.count_versions_walked(node_ids.len() + edge_ids.len());

        #[cfg(not(feature = "tiered-storage"))]
        {
            let mut nodes = self.nodes.write();
            for id in &node_ids {
                if let Some(chain) = nodes.get_mut(id) {
                    chain.finalize_epochs(transaction_id, commit_epoch);
                }
            }
            drop(nodes);
            let mut edges = self.edges.write();
            for id in &edge_ids {
                if let Some(chain) = edges.get_mut(id) {
                    chain.finalize_epochs(transaction_id, commit_epoch);
                }
            }
        }
        #[cfg(feature = "tiered-storage")]
        {
            let mut versions = self.node_versions.write();
            for id in &node_ids {
                if let Some(index) = versions.get_mut(id) {
                    index.finalize_epochs(transaction_id, commit_epoch);
                }
            }
            drop(versions);
            let mut versions = self.edge_versions.write();
            for id in &edge_ids {
                if let Some(index) = versions.get_mut(id) {
                    index.finalize_epochs(transaction_id, commit_epoch);
                }
            }
        }

        #[cfg(feature = "temporal")]
        self.finalize_pending_values(transaction_id, commit_epoch);

        self.sync_epoch(commit_epoch);
    }

    /// Replaces the PENDING epochs of the property and label versions a
    /// transaction wrote with its commit epoch. Only its own entities are
    /// touched, so other open transactions' writes stay pending.
    #[cfg(feature = "temporal")]
    fn finalize_pending_values(&self, transaction_id: TransactionId, commit_epoch: EpochId) {
        let mut node_values: Vec<(NodeId, PropertyKey)> = Vec::new();
        let mut edge_values: Vec<(EdgeId, PropertyKey)> = Vec::new();
        let mut label_nodes: Vec<NodeId> = Vec::new();
        if let Some(entries) = self.property_undo_log.read().get(&transaction_id) {
            for entry in entries {
                match entry {
                    PropertyUndoEntry::NodeProperty { node_id, key, .. } => {
                        node_values.push((*node_id, key.clone()));
                    }
                    PropertyUndoEntry::EdgeProperty { edge_id, key, .. } => {
                        edge_values.push((*edge_id, key.clone()));
                    }
                    PropertyUndoEntry::NodeCreated { node_id }
                    | PropertyUndoEntry::LabelAdded { node_id, .. }
                    | PropertyUndoEntry::LabelRemoved { node_id, .. } => label_nodes.push(*node_id),
                    // A delete writes a PENDING tombstone for each value it removed.
                    PropertyUndoEntry::NodeDeleted {
                        node_id,
                        properties,
                        ..
                    } => {
                        node_values
                            .extend(properties.iter().map(|(key, _)| (*node_id, key.clone())));
                    }
                    PropertyUndoEntry::EdgeDeleted {
                        edge_id,
                        properties,
                        ..
                    } => {
                        edge_values
                            .extend(properties.iter().map(|(key, _)| (*edge_id, key.clone())));
                    }
                    PropertyUndoEntry::EdgeCreated { .. } => {}
                }
            }
        }
        if !node_values.is_empty() {
            let mut columns = self.node_properties.columns_write();
            for (id, key) in &node_values {
                if let Some(column) = columns.get_mut(key) {
                    column.finalize_pending_for(*id, commit_epoch);
                }
            }
        }
        if !edge_values.is_empty() {
            let mut columns = self.edge_properties.columns_write();
            for (id, key) in &edge_values {
                if let Some(column) = columns.get_mut(key) {
                    column.finalize_pending_for(*id, commit_epoch);
                }
            }
        }
        if !label_nodes.is_empty() {
            let mut labels = self.node_labels.write();
            for id in &label_nodes {
                if let Some(log) = labels.get_mut(id) {
                    log.finalize_pending(commit_epoch);
                }
            }
        }
    }

    /// Removes a node that `transaction_id` created, when the transaction
    /// rolls back: its version, labels, label/property/text index entries,
    /// properties and count. Nothing of it was ever visible to others.
    pub(super) fn discard_created_node(&self, id: NodeId, transaction_id: TransactionId) {
        self.count_versions_walked(1);
        #[cfg(not(feature = "tiered-storage"))]
        {
            let mut nodes = self.nodes.write();
            let Some(chain) = nodes.get_mut(&id) else {
                return;
            };
            chain.remove_versions_by(transaction_id);
            if !chain.is_empty() {
                return;
            }
            nodes.remove(&id);
        }
        #[cfg(feature = "tiered-storage")]
        {
            let mut versions = self.node_versions.write();
            let Some(index) = versions.get_mut(&id) else {
                return;
            };
            index.remove_versions_by(transaction_id);
            if !index.is_empty() {
                return;
            }
            versions.remove(&id);
        }

        #[cfg(feature = "text-index")]
        self.remove_from_all_text_indexes(id);
        self.cleanup_discarded_node_secondaries(&[id]);
    }

    /// Removes an edge that `transaction_id` created, when the transaction
    /// rolls back: its version, adjacency entries, properties and counts.
    pub(super) fn discard_created_edge(&self, id: EdgeId, transaction_id: TransactionId) {
        self.count_versions_walked(1);
        #[cfg(not(feature = "tiered-storage"))]
        let record = {
            let mut edges = self.edges.write();
            let Some(chain) = edges.get_mut(&id) else {
                return;
            };
            let record = chain.latest().copied();
            chain.remove_versions_by(transaction_id);
            if !chain.is_empty() {
                return;
            }
            edges.remove(&id);
            record
        };
        #[cfg(feature = "tiered-storage")]
        let record = {
            let mut versions = self.edge_versions.write();
            let Some(index) = versions.get_mut(&id) else {
                return;
            };
            // Own PENDING versions are visible to the creating transaction.
            let record = index
                .visible_to(EpochId::PENDING, transaction_id)
                .or_else(|| index.latest())
                .and_then(|version| self.read_edge_record(&version));
            index.remove_versions_by(transaction_id);
            if !index.is_empty() {
                return;
            }
            versions.remove(&id);
            record
        };
        let Some(record) = record else {
            return;
        };
        self.cleanup_discarded_edge_secondaries(&[DiscardedEdge {
            id,
            src: record.src,
            dst: record.dst,
            type_id: record.type_id,
        }]);
    }

    /// Garbage collects old versions that are no longer visible to any transaction.
    ///
    /// Versions older than `min_epoch` are pruned from version chains, keeping
    /// at most one old version per entity as a baseline. Empty chains are removed.
    #[cfg(not(feature = "tiered-storage"))]
    #[doc(hidden)]
    pub fn gc_versions(&self, min_epoch: EpochId) {
        {
            let mut nodes = self.nodes.write();
            for chain in nodes.values_mut() {
                chain.gc(min_epoch);
            }
            nodes.retain(|_, chain| !chain.is_empty());
        }
        {
            let mut edges = self.edges.write();
            for chain in edges.values_mut() {
                chain.gc(min_epoch);
            }
            edges.retain(|_, chain| !chain.is_empty());
        }

        // GC old property and label versions
        #[cfg(feature = "temporal")]
        {
            self.node_properties.gc(min_epoch);
            self.edge_properties.gc(min_epoch);
            let mut labels = self.node_labels.write();
            for log in labels.values_mut() {
                log.gc(min_epoch);
            }
            labels.retain(|_, log| !log.is_empty());
        }
    }

    /// Garbage collects old versions (tiered storage variant).
    #[cfg(feature = "tiered-storage")]
    #[doc(hidden)]
    pub fn gc_versions(&self, min_epoch: EpochId) {
        {
            let mut versions = self.node_versions.write();
            for index in versions.values_mut() {
                index.gc(min_epoch);
            }
            versions.retain(|_, index| !index.is_empty());
        }
        {
            let mut versions = self.edge_versions.write();
            for index in versions.values_mut() {
                index.gc(min_epoch);
            }
            versions.retain(|_, index| !index.is_empty());
        }

        // GC old property and label versions
        #[cfg(feature = "temporal")]
        {
            self.node_properties.gc(min_epoch);
            self.edge_properties.gc(min_epoch);
            let mut labels = self.node_labels.write();
            for log in labels.values_mut() {
                log.gc(min_epoch);
            }
            labels.retain(|_, log| !log.is_empty());
        }
    }

    /// Freezes an epoch from hot (arena) storage to cold (compressed) storage.
    ///
    /// This is called by the transaction manager when an epoch becomes eligible
    /// for freezing (no active transactions can see it). The freeze process:
    ///
    /// 1. Collects all hot version refs for the epoch
    /// 2. Reads the corresponding records from arena
    /// 3. Compresses them into a `CompressedEpochBlock`
    /// 4. Updates `VersionIndex` entries to point to cold storage
    /// 5. The arena can be deallocated after all epochs in it are frozen
    ///
    /// # Arguments
    ///
    /// * `epoch` - The epoch to freeze
    ///
    /// # Returns
    ///
    /// The number of records frozen (nodes + edges).
    #[doc(hidden)]
    #[cfg(feature = "tiered-storage")]
    #[allow(unsafe_code)]
    pub fn freeze_epoch(&self, epoch: EpochId) -> usize {
        // Collect node records to freeze
        let mut node_records: Vec<(u64, NodeRecord)> = Vec::new();
        let mut node_hot_refs: Vec<(NodeId, HotVersionRef)> = Vec::new();

        {
            let versions = self.node_versions.read();
            for (node_id, index) in versions.iter() {
                for hot_ref in index.hot_refs_for_epoch(epoch) {
                    let arena = self
                        .arena_allocator
                        .arena(hot_ref.arena_epoch)
                        .expect("arena epoch must exist for hot version ref");
                    // SAFETY: The offset was returned by alloc_value_with_offset for a NodeRecord
                    let record: &NodeRecord = unsafe { arena.read_at(hot_ref.arena_offset) };
                    node_records.push((node_id.as_u64(), *record));
                    node_hot_refs.push((*node_id, *hot_ref));
                }
            }
        }

        // Collect edge records to freeze
        let mut edge_records: Vec<(u64, EdgeRecord)> = Vec::new();
        let mut edge_hot_refs: Vec<(EdgeId, HotVersionRef)> = Vec::new();

        {
            let versions = self.edge_versions.read();
            for (edge_id, index) in versions.iter() {
                for hot_ref in index.hot_refs_for_epoch(epoch) {
                    let arena = self
                        .arena_allocator
                        .arena(hot_ref.arena_epoch)
                        .expect("arena epoch must exist for hot version ref");
                    // SAFETY: The offset was returned by alloc_value_with_offset for an EdgeRecord
                    let record: &EdgeRecord = unsafe { arena.read_at(hot_ref.arena_offset) };
                    edge_records.push((edge_id.as_u64(), *record));
                    edge_hot_refs.push((*edge_id, *hot_ref));
                }
            }
        }

        let total_frozen = node_records.len() + edge_records.len();

        if total_frozen == 0 {
            return 0;
        }

        // Freeze to compressed storage
        let (node_entries, edge_entries) =
            self.epoch_store
                .freeze_epoch(epoch, node_records, edge_records);

        // Build lookup maps for index entries
        let node_entry_map: FxHashMap<u64, _> = node_entries
            .iter()
            .map(|e| (e.entity_id, (e.offset, e.length)))
            .collect();
        let edge_entry_map: FxHashMap<u64, _> = edge_entries
            .iter()
            .map(|e| (e.entity_id, (e.offset, e.length)))
            .collect();

        // Update version indexes to use cold refs
        {
            let mut versions = self.node_versions.write();
            for (node_id, hot_ref) in &node_hot_refs {
                if let Some(index) = versions.get_mut(node_id)
                    && let Some(&(offset, length)) = node_entry_map.get(&node_id.as_u64())
                {
                    let cold_ref = ColdVersionRef {
                        epoch,
                        block_offset: offset,
                        length,
                        created_by: hot_ref.created_by,
                        deleted_epoch: hot_ref.deleted_epoch,
                        deleted_by: hot_ref.deleted_by,
                    };
                    index.freeze_epoch(epoch, std::iter::once(cold_ref));
                }
            }
        }

        {
            let mut versions = self.edge_versions.write();
            for (edge_id, hot_ref) in &edge_hot_refs {
                if let Some(index) = versions.get_mut(edge_id)
                    && let Some(&(offset, length)) = edge_entry_map.get(&edge_id.as_u64())
                {
                    let cold_ref = ColdVersionRef {
                        epoch,
                        block_offset: offset,
                        length,
                        created_by: hot_ref.created_by,
                        deleted_epoch: hot_ref.deleted_epoch,
                        deleted_by: hot_ref.deleted_by,
                    };
                    index.freeze_epoch(epoch, std::iter::once(cold_ref));
                }
            }
        }

        total_frozen
    }

    /// Returns the epoch store for cold storage statistics.
    #[doc(hidden)]
    #[cfg(feature = "tiered-storage")]
    #[must_use]
    pub fn epoch_store(&self) -> &crate::codec::EpochStore {
        &self.epoch_store
    }

    // === Recovery Support ===

    /// Creates a node with a specific ID, created at `epoch`, by the system
    /// transaction.
    ///
    /// Used by WAL recovery to restore nodes with their original IDs, and by
    /// `LayeredStore` to copy a base node into the overlay (at epoch 0, so the
    /// copy is visible to every snapshot, like the base row it stands for).
    /// Never lowers the ID allocator. The caller must ensure IDs don't
    /// conflict with existing nodes.
    ///
    /// # Errors
    ///
    /// Returns [`AllocError`] if the arena allocator cannot allocate space
    /// (only possible with the `tiered-storage` feature).
    #[cfg(not(feature = "tiered-storage"))]
    #[doc(hidden)]
    pub fn create_node_with_id_at(
        &self,
        id: NodeId,
        labels: &[&str],
        epoch: EpochId,
    ) -> Result<(), AllocError> {
        let mut record = NodeRecord::new(id, epoch);
        // reason: label count per node is bounded by practical limits, fits u16
        #[allow(clippy::cast_possible_truncation)]
        record.set_label_count(labels.len() as u16);

        #[cfg(not(feature = "temporal"))]
        self.register_node_labels(id, labels);
        #[cfg(feature = "temporal")]
        self.register_node_labels(id, labels, epoch);

        // Create version chain with initial version (using SYSTEM tx for recovery)
        let chain = VersionChain::with_initial(record, epoch, TransactionId::SYSTEM);
        self.nodes.write().insert(id, chain);
        self.live_node_count.fetch_add(1, Ordering::Relaxed);

        // Update next_node_id if necessary to avoid future collisions
        let id_val = id.as_u64();
        let _ = self
            .next_node_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if id_val >= current {
                    Some(id_val + 1)
                } else {
                    None
                }
            });
        Ok(())
    }

    /// Creates a node with a specific ID during recovery.
    /// (Tiered storage version)
    ///
    /// # Errors
    ///
    /// Returns [`AllocError`] if the arena allocator cannot create an epoch
    /// or allocate space for the node record.
    #[cfg(feature = "tiered-storage")]
    #[doc(hidden)]
    pub fn create_node_with_id_at(
        &self,
        id: NodeId,
        labels: &[&str],
        epoch: EpochId,
    ) -> Result<(), AllocError> {
        let mut record = NodeRecord::new(id, epoch);
        // reason: label count per node is bounded by practical limits, fits u16
        #[allow(clippy::cast_possible_truncation)]
        record.set_label_count(labels.len() as u16);

        #[cfg(not(feature = "temporal"))]
        self.register_node_labels(id, labels);
        #[cfg(feature = "temporal")]
        self.register_node_labels(id, labels, epoch);

        // Allocate record in arena and get offset (create epoch if needed)
        let arena = self.arena_allocator.arena_or_create(epoch)?;
        let (offset, _stored) = arena.alloc_value_with_offset(record)?;

        // Create HotVersionRef (using SYSTEM tx for recovery)
        let hot_ref = HotVersionRef::new(epoch, epoch, offset, TransactionId::SYSTEM);
        let mut versions = self.node_versions.write();
        versions.insert(id, VersionIndex::with_initial(hot_ref));
        self.live_node_count.fetch_add(1, Ordering::Relaxed);

        // Update next_node_id if necessary to avoid future collisions
        let id_val = id.as_u64();
        let _ = self
            .next_node_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if id_val >= current {
                    Some(id_val + 1)
                } else {
                    None
                }
            });
        Ok(())
    }

    /// Creates an edge with a specific ID, created at `epoch`, by the system
    /// transaction. Edge counterpart of
    /// [`create_node_with_id_at`](Self::create_node_with_id_at).
    ///
    /// # Errors
    ///
    /// Returns [`AllocError`] if the arena allocator cannot allocate space
    /// (only possible with the `tiered-storage` feature).
    #[cfg(not(feature = "tiered-storage"))]
    #[doc(hidden)]
    pub fn create_edge_with_id_at(
        &self,
        id: EdgeId,
        src: NodeId,
        dst: NodeId,
        edge_type: &str,
        epoch: EpochId,
    ) -> Result<(), AllocError> {
        let type_id = self.get_or_create_edge_type_id(edge_type);

        let record = EdgeRecord::new(id, src, dst, type_id, epoch);
        let chain = VersionChain::with_initial(record, epoch, TransactionId::SYSTEM);
        self.edges.write().insert(id, chain);

        // Update adjacency
        self.forward_adj.add_edge(src, dst, id);
        if let Some(ref backward) = self.backward_adj {
            backward.add_edge(dst, src, id);
        }

        self.live_edge_count.fetch_add(1, Ordering::Relaxed);
        self.increment_edge_type_count(type_id);

        // Update next_edge_id if necessary
        let id_val = id.as_u64();
        let _ = self
            .next_edge_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if id_val >= current {
                    Some(id_val + 1)
                } else {
                    None
                }
            });
        Ok(())
    }

    /// Creates an edge with a specific ID during recovery.
    /// (Tiered storage version)
    ///
    /// # Errors
    ///
    /// Returns [`AllocError`] if the arena allocator cannot create an epoch
    /// or allocate space for the edge record.
    #[cfg(feature = "tiered-storage")]
    #[doc(hidden)]
    pub fn create_edge_with_id_at(
        &self,
        id: EdgeId,
        src: NodeId,
        dst: NodeId,
        edge_type: &str,
        epoch: EpochId,
    ) -> Result<(), AllocError> {
        let type_id = self.get_or_create_edge_type_id(edge_type);

        let record = EdgeRecord::new(id, src, dst, type_id, epoch);

        // Allocate record in arena and get offset (create epoch if needed)
        let arena = self.arena_allocator.arena_or_create(epoch)?;
        let (offset, _stored) = arena.alloc_value_with_offset(record)?;

        // Create HotVersionRef (using SYSTEM tx for recovery)
        let hot_ref = HotVersionRef::new(epoch, epoch, offset, TransactionId::SYSTEM);
        let mut versions = self.edge_versions.write();
        versions.insert(id, VersionIndex::with_initial(hot_ref));

        // Update adjacency
        self.forward_adj.add_edge(src, dst, id);
        if let Some(ref backward) = self.backward_adj {
            backward.add_edge(dst, src, id);
        }

        self.live_edge_count.fetch_add(1, Ordering::Relaxed);
        self.increment_edge_type_count(type_id);

        // Update next_edge_id if necessary
        let id_val = id.as_u64();
        let _ = self
            .next_edge_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if id_val >= current {
                    Some(id_val + 1)
                } else {
                    None
                }
            });
        Ok(())
    }

    /// Creates a node with a specific ID at the current epoch (WAL recovery).
    ///
    /// See [`create_node_with_id_at`](Self::create_node_with_id_at).
    ///
    /// # Errors
    ///
    /// Returns [`AllocError`] if the arena allocator cannot allocate space
    /// (only possible with the `tiered-storage` feature).
    #[doc(hidden)]
    pub fn create_node_with_id(&self, id: NodeId, labels: &[&str]) -> Result<(), AllocError> {
        self.create_node_with_id_at(id, labels, self.current_epoch())
    }

    /// Creates an edge with a specific ID at the current epoch (WAL recovery).
    ///
    /// See [`create_edge_with_id_at`](Self::create_edge_with_id_at).
    ///
    /// # Errors
    ///
    /// Returns [`AllocError`] if the arena allocator cannot allocate space
    /// (only possible with the `tiered-storage` feature).
    #[doc(hidden)]
    pub fn create_edge_with_id(
        &self,
        id: EdgeId,
        src: NodeId,
        dst: NodeId,
        edge_type: &str,
    ) -> Result<(), AllocError> {
        self.create_edge_with_id_at(id, src, dst, edge_type, self.current_epoch())
    }

    /// Sets the current epoch during recovery.
    #[doc(hidden)]
    pub fn set_epoch(&self, epoch: EpochId) {
        self.current_epoch.store(epoch.as_u64(), Ordering::SeqCst);
    }
}
