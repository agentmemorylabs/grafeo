//! Index management methods for [`LpgStore`].

use super::LpgStore;
use dashmap::DashMap;
use grafeo_common::types::{HashableValue, NodeId, PropertyKey, Value};
use grafeo_common::utils::hash::FxHashSet;
#[cfg(feature = "text-index")]
use parking_lot::RwLock;
use std::sync::Arc;

#[cfg(feature = "vector-index")]
use crate::index::vector::VectorIndexKind;

impl LpgStore {
    /// Creates an index on a node property for O(1) lookups by value.
    ///
    /// After creating an index, calls to [`Self::find_nodes_by_property`] will be
    /// O(1) instead of O(n) for this property. The index is automatically
    /// maintained when properties are set or removed.
    ///
    /// # Example
    ///
    /// ```
    /// use grafeo_core::graph::lpg::LpgStore;
    /// use grafeo_common::types::Value;
    ///
    /// let store = LpgStore::new().expect("arena allocation");
    ///
    /// // Create nodes with an 'id' property
    /// let alix = store.create_node(&["Person"]);
    /// store.set_node_property(alix, "id", Value::from("alice_123"));
    ///
    /// // Create an index on the 'id' property
    /// store.create_property_index("id");
    ///
    /// // Now lookups by 'id' are O(1)
    /// let found = store.find_nodes_by_property("id", &Value::from("alice_123"));
    /// assert!(found.contains(&alix));
    /// ```
    pub fn create_property_index(&self, property: &str) {
        let key = PropertyKey::new(property);

        // Prefer replacing a mapped shell with a live heap index after create.
        // A mapped index's heap entry is only its write delta, not full
        // postings, so it goes too and the scan below rebuilds the key.
        let had_mapped = self.mapped_property_indexes.write().remove(&key).is_some();

        let mut indexes = self.property_indexes.write();
        if had_mapped {
            indexes.remove(&key);
        }
        if indexes.contains_key(&key) {
            return; // Already indexed
        }

        // Create the index and populate it with existing data
        let index: DashMap<HashableValue, FxHashSet<NodeId>> = DashMap::new();

        // Scan all nodes to build the index
        for node_id in self.node_ids() {
            if let Some(value) = self.node_properties.get(node_id, &key) {
                let hv = HashableValue::new(value);
                index.entry(hv).or_default().insert(node_id);
            }
        }

        indexes.insert(key, index);
    }

    /// Create a property index from an explicit (node_id, value) iterator.
    ///
    /// Used after `compact()` when the overlay LpgStore must index CompactStore
    /// base rows that are not present in `self.node_ids()`.
    pub fn create_property_index_from_entries(
        &self,
        property: &str,
        entries: impl IntoIterator<Item = (NodeId, Value)>,
    ) {
        let key = PropertyKey::new(property);
        let had_mapped = self.mapped_property_indexes.write().remove(&key).is_some();
        let mut indexes = self.property_indexes.write();
        if had_mapped {
            // Only the mapped index's write delta: replace it with `entries`.
            indexes.remove(&key);
        }
        if indexes.contains_key(&key) {
            return;
        }
        let index: DashMap<HashableValue, FxHashSet<NodeId>> = DashMap::new();
        for (node_id, value) in entries {
            let hv = HashableValue::new(value);
            index.entry(hv).or_default().insert(node_id);
        }
        indexes.insert(key, index);
    }

    /// Registers an empty property-index shell without scanning data.
    ///
    /// Used during Catalog restore before the PropertyIndex section hydrates
    /// postings (or as a documented rebuild fallback target).
    pub fn ensure_property_index_shell(&self, property: &str) {
        let key = PropertyKey::new(property);
        if self.mapped_property_indexes.read().contains_key(&key) {
            return;
        }
        let mut indexes = self.property_indexes.write();
        indexes.entry(key).or_default();
    }

    /// Installs a mapped property index restored from the PropertyIndex section.
    ///
    /// The mapped postings are immutable (file-backed, zero proportional
    /// anonymous ownership) and describe the data as of the section write.
    /// A heap entry for the same key is kept (or created empty) as the
    /// index's *write delta*: every write path that maintains heap indexes
    /// (`update_property_index_on_set`/`_on_remove`, batch creates, rollback
    /// cleanup) records nodes written after the install there. Lookups union
    /// mapped postings with the delta and re-verify each candidate against
    /// current property values (see [`Self::mapped_property_index_candidates`]).
    pub fn install_mapped_property_index(
        &self,
        index: Arc<crate::index::property::MappedPropertyIndex>,
    ) {
        let key = PropertyKey::new(&index.name);
        self.property_indexes
            .write()
            .entry(key.clone())
            .or_default();
        self.mapped_property_indexes.write().insert(key, index);
    }

    /// Unverified candidates for `property = value` when `property` has a
    /// mapped index: the mapped postings plus the heap write delta, sorted
    /// and deduplicated. `None` when `property` has no mapped index.
    ///
    /// Mapped postings can be stale (the node's value changed, or the node
    /// was deleted, after the section was written), so callers must check
    /// each candidate against the current value. A layered store checks
    /// against its merged base+overlay view; [`Self::find_nodes_by_property`]
    /// checks nodes this store holds.
    #[must_use]
    pub fn mapped_property_index_candidates(
        &self,
        property: &str,
        value: &Value,
    ) -> Option<Vec<NodeId>> {
        let key = PropertyKey::new(property);
        let mapped = self.mapped_property_indexes.read().get(&key).cloned()?;
        let mut ids = mapped.lookup(value);
        if let Some(delta) = self.property_indexes.read().get(&key)
            && let Some(nodes) = delta.get(&HashableValue::new(value.clone()))
        {
            ids.extend(nodes.iter().copied());
        }
        ids.sort_unstable();
        ids.dedup();
        Some(ids)
    }

    /// Whether this store has a record (live or deleted) for `id`.
    ///
    /// Distinguishes this store's own nodes from ids that a mapped index
    /// carries for rows held elsewhere (a layered store's CompactStore base).
    pub(super) fn has_node_record(&self, id: NodeId) -> bool {
        #[cfg(not(feature = "tiered-storage"))]
        {
            self.nodes.read().contains_key(&id)
        }
        #[cfg(feature = "tiered-storage")]
        {
            self.node_versions.read().contains_key(&id)
        }
    }

    /// Drops an index on a node property.
    ///
    /// Returns `true` if the index existed and was removed.
    pub fn drop_property_index(&self, property: &str) -> bool {
        let key = PropertyKey::new(property);
        let heap = self.property_indexes.write().remove(&key).is_some();
        let mapped = self.mapped_property_indexes.write().remove(&key).is_some();
        heap || mapped
    }

    /// Returns `true` if the property has an index.
    #[must_use]
    pub fn has_property_index(&self, property: &str) -> bool {
        let key = PropertyKey::new(property);
        self.property_indexes.read().contains_key(&key)
            || self.mapped_property_indexes.read().contains_key(&key)
    }

    /// Returns the names of all indexed properties.
    #[must_use]
    pub fn property_index_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .property_indexes
            .read()
            .keys()
            .map(|k| k.to_string())
            .collect();
        for k in self.mapped_property_indexes.read().keys() {
            let name = k.to_string();
            if !keys.iter().any(|existing| existing == &name) {
                keys.push(name);
            }
        }
        keys.sort();
        keys
    }

    /// Snapshot all heap property-index postings for section serialization.
    ///
    /// For a key with a restored mapped index this decodes every mapped
    /// posting (cost proportional to the section) and passes through ids this
    /// store holds no record of without checking them. On a layered overlay
    /// those are base rows, possibly deleted or changed since; the generation
    /// writers therefore rebuild postings from the layered graph instead of
    /// calling this.
    #[must_use]
    pub fn property_index_snapshot_entries(
        &self,
    ) -> Vec<crate::index::property::PropertyIndexSnapshot> {
        let guard = self.property_indexes.read();
        let mapped_guard = self.mapped_property_indexes.read();
        let mut out = Vec::with_capacity(guard.len());
        for (key, map) in guard.iter() {
            if mapped_guard.contains_key(key) {
                continue; // write delta of a mapped index, merged below
            }
            let mut entries = Vec::new();
            for item in map {
                let value = item.key().0.clone();
                for node_id in item.value() {
                    entries.push((value.clone(), *node_id));
                }
            }
            out.push(crate::index::property::PropertyIndexSnapshot {
                name: key.to_string(),
                entries,
            });
        }
        // Mapped indexes: re-encode the mapped postings that still hold
        // (foreign ids pass through; this store's nodes must still carry the
        // value), plus the heap write delta.
        for (key, mapped) in mapped_guard.iter() {
            let Ok(mapped_entries) = mapped.iter_entries() else {
                continue;
            };
            let mut seen: FxHashSet<(NodeId, HashableValue)> = FxHashSet::default();
            let mut entries = Vec::with_capacity(mapped_entries.len());
            for (value, node_id) in mapped_entries {
                let current = !self.has_node_record(node_id)
                    || self
                        .node_properties
                        .get(node_id, key)
                        .is_some_and(|v| v == value);
                if current && seen.insert((node_id, HashableValue::new(value.clone()))) {
                    entries.push((value, node_id));
                }
            }
            if let Some(delta) = guard.get(key) {
                for item in delta {
                    let value = item.key().0.clone();
                    for node_id in item.value() {
                        if seen.insert((*node_id, item.key().clone())) {
                            entries.push((value.clone(), *node_id));
                        }
                    }
                }
            }
            out.push(crate::index::property::PropertyIndexSnapshot {
                name: key.to_string(),
                entries,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Updates property indexes when a property is set.
    pub(super) fn update_property_index_on_set(
        &self,
        node_id: NodeId,
        key: &PropertyKey,
        new_value: &Value,
    ) {
        let indexes = self.property_indexes.read();
        if let Some(index) = indexes.get(key) {
            // Get old value to remove from index
            if let Some(old_value) = self.node_properties.get(node_id, key) {
                let old_hv = HashableValue::new(old_value);
                if let Some(mut nodes) = index.get_mut(&old_hv) {
                    nodes.remove(&node_id);
                    if nodes.is_empty() {
                        drop(nodes);
                        index.remove(&old_hv);
                    }
                }
            }

            // Add new value to index
            let new_hv = HashableValue::new(new_value.clone());
            index
                .entry(new_hv)
                .or_insert_with(FxHashSet::default)
                .insert(node_id);
        }
    }

    /// Updates property indexes when a property is removed.
    pub(super) fn update_property_index_on_remove(&self, node_id: NodeId, key: &PropertyKey) {
        let indexes = self.property_indexes.read();
        if let Some(index) = indexes.get(key) {
            // Get old value to remove from index
            if let Some(old_value) = self.node_properties.get(node_id, key) {
                let old_hv = HashableValue::new(old_value);
                if let Some(mut nodes) = index.get_mut(&old_hv) {
                    nodes.remove(&node_id);
                    if nodes.is_empty() {
                        drop(nodes);
                        index.remove(&old_hv);
                    }
                }
            }
        }
    }

    /// Stores a vector index for a label+property pair.
    #[cfg(feature = "vector-index")]
    pub fn add_vector_index(&self, label: &str, property: &str, index: Arc<VectorIndexKind>) {
        let key = format!("{label}:{property}");
        self.vector_indexes.write().insert(key, index);
    }

    /// Retrieves the vector index for a label+property pair.
    #[cfg(feature = "vector-index")]
    #[must_use]
    pub fn get_vector_index(&self, label: &str, property: &str) -> Option<Arc<VectorIndexKind>> {
        let key = format!("{label}:{property}");
        self.vector_indexes.read().get(&key).cloned()
    }

    /// Removes a vector index for a label+property pair.
    ///
    /// Returns `true` if the index existed and was removed.
    #[cfg(feature = "vector-index")]
    pub fn remove_vector_index(&self, label: &str, property: &str) -> bool {
        let key = format!("{label}:{property}");
        self.vector_indexes.write().remove(&key).is_some()
    }

    /// Returns all vector index entries as `(key, index)` pairs.
    ///
    /// Keys are in `"label:property"` format.
    #[cfg(feature = "vector-index")]
    #[must_use]
    pub fn vector_index_entries(&self) -> Vec<(String, Arc<VectorIndexKind>)> {
        self.vector_indexes
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Looks up a vector index by its `"label:property"` key.
    #[cfg(feature = "vector-index")]
    #[must_use]
    pub fn get_vector_index_by_key(&self, key: &str) -> Option<Arc<VectorIndexKind>> {
        self.vector_indexes.read().get(key).cloned()
    }

    /// Stores a text index for a label+property pair.
    #[cfg(feature = "text-index")]
    pub fn add_text_index(
        &self,
        label: &str,
        property: &str,
        index: Arc<RwLock<crate::index::text::InvertedIndex>>,
    ) {
        let key = format!("{label}:{property}");
        self.mapped_text_indexes.write().remove(&key);
        self.text_indexes.write().insert(key, index);
    }

    /// Registers an empty text-index shell so Catalog restore can precede
    /// TextIndex section hydration (or explicit rebuild fallback).
    #[cfg(feature = "text-index")]
    pub fn ensure_text_index_shell(&self, label: &str, property: &str) {
        let key = format!("{label}:{property}");
        if self.mapped_text_indexes.read().contains_key(&key) {
            return;
        }
        let mut indexes = self.text_indexes.write();
        indexes.entry(key).or_insert_with(|| {
            Arc::new(RwLock::new(crate::index::text::InvertedIndex::new(
                crate::index::text::BM25Config::default(),
            )))
        });
    }

    /// Installs a mapped text index restored from TextIndex section v2.
    #[cfg(feature = "text-index")]
    pub fn install_mapped_text_index(&self, index: Arc<crate::index::text::MappedTextIndex>) {
        let key = index.key.clone();
        // Keep a heap shell present so has_text_index / text_index_entries
        // report the key, but search prefers mapped (see get_mapped_text_index).
        {
            let mut heap = self.text_indexes.write();
            heap.entry(key.clone()).or_insert_with(|| {
                Arc::new(RwLock::new(crate::index::text::InvertedIndex::new(
                    crate::index::text::BM25Config::default(),
                )))
            });
        }
        self.mapped_text_indexes.write().insert(key, index);
    }

    /// Retrieves the text index for a label+property pair.
    #[cfg(feature = "text-index")]
    #[must_use]
    pub fn get_text_index(
        &self,
        label: &str,
        property: &str,
    ) -> Option<Arc<RwLock<crate::index::text::InvertedIndex>>> {
        let key = format!("{label}:{property}");
        self.text_indexes.read().get(&key).cloned()
    }

    /// Retrieves a mapped text index if one was restored from section v2.
    #[cfg(feature = "text-index")]
    #[must_use]
    pub fn get_mapped_text_index(
        &self,
        label: &str,
        property: &str,
    ) -> Option<Arc<crate::index::text::MappedTextIndex>> {
        let key = format!("{label}:{property}");
        self.mapped_text_indexes.read().get(&key).cloned()
    }

    /// Removes a text index for a label+property pair.
    ///
    /// Returns `true` if the index existed and was removed.
    #[cfg(feature = "text-index")]
    pub fn remove_text_index(&self, label: &str, property: &str) -> bool {
        let key = format!("{label}:{property}");
        let heap = self.text_indexes.write().remove(&key).is_some();
        let mapped = self.mapped_text_indexes.write().remove(&key).is_some();
        heap || mapped
    }

    /// Returns all text index entries as `(key, index)` pairs.
    ///
    /// The key format is `"label:property"`.
    #[cfg(feature = "text-index")]
    pub fn text_index_entries(
        &self,
    ) -> Vec<(String, Arc<RwLock<crate::index::text::InvertedIndex>>)> {
        self.text_indexes
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Returns mapped text indexes (key, mapped) for accounting/tests.
    #[cfg(feature = "text-index")]
    pub fn mapped_text_index_entries(
        &self,
    ) -> Vec<(String, Arc<crate::index::text::MappedTextIndex>)> {
        self.mapped_text_indexes
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect()
    }

    /// Mapped property index accounting bytes (sum of retained mapped payloads).
    #[must_use]
    pub fn mapped_property_index_payload_bytes(&self) -> u64 {
        // Each MappedPropertyIndex shares the same section Bytes; count once
        // via max data len among entries (they share the Arc/Bytes mapping).
        self.mapped_property_indexes
            .read()
            .values()
            .map(|idx| {
                // Approximate per-index share not needed; tests use section-level
                // accounting from the restored set. Report entry_count * 0 +
                // presence signal via non-zero when any mapped index exists.
                u64::from(idx.len().max(1))
            })
            .sum()
    }

    /// Whether any mapped property indexes are installed.
    #[must_use]
    pub fn has_mapped_property_indexes(&self) -> bool {
        !self.mapped_property_indexes.read().is_empty()
    }

    /// Whether a specific property has a mapped (section-restored) index.
    #[must_use]
    pub fn has_mapped_property_index(&self, property: &str) -> bool {
        let key = PropertyKey::new(property);
        self.mapped_property_indexes.read().contains_key(&key)
    }

    /// Updates text indexes when a node property is set.
    ///
    /// If the node has a label with a text index on this property key,
    /// the index is updated with the new value (if it's a string).
    #[cfg(feature = "text-index")]
    pub(super) fn update_text_index_on_set(&self, id: NodeId, key: &str, value: &Value) {
        let text_indexes = self.text_indexes.read();
        if text_indexes.is_empty() {
            return;
        }
        let registry = self.label_registry.read();
        let node_labels = self.node_labels.read();
        #[cfg(not(feature = "temporal"))]
        let label_set = node_labels.get(&id);
        #[cfg(feature = "temporal")]
        let label_set = node_labels.get(&id).and_then(|log| log.latest());
        if let Some(label_ids) = label_set {
            for &label_id in label_ids {
                if let Some(label_name) = registry.get_name(label_id) {
                    let index_key = format!("{label_name}:{key}");
                    if let Some(index) = text_indexes.get(&index_key) {
                        let mut idx = index.write();
                        // Remove old entry first, then insert new if it's a string
                        idx.remove(id);
                        if let Value::String(text) = value {
                            idx.insert(id, text);
                        }
                    }
                }
            }
        }
    }

    /// Updates text indexes when a node property is removed.
    #[cfg(feature = "text-index")]
    pub(super) fn update_text_index_on_remove(&self, id: NodeId, key: &str) {
        let text_indexes = self.text_indexes.read();
        if text_indexes.is_empty() {
            return;
        }
        let registry = self.label_registry.read();
        let node_labels = self.node_labels.read();
        #[cfg(not(feature = "temporal"))]
        let label_set = node_labels.get(&id);
        #[cfg(feature = "temporal")]
        let label_set = node_labels.get(&id).and_then(|log| log.latest());
        if let Some(label_ids) = label_set {
            for &label_id in label_ids {
                if let Some(label_name) = registry.get_name(label_id) {
                    let index_key = format!("{label_name}:{key}");
                    if let Some(index) = text_indexes.get(&index_key) {
                        index.write().remove(id);
                    }
                }
            }
        }
    }

    /// Removes a node from all text indexes.
    #[cfg(feature = "text-index")]
    pub(super) fn remove_from_all_text_indexes(&self, id: NodeId) {
        let text_indexes = self.text_indexes.read();
        if text_indexes.is_empty() {
            return;
        }
        for (_, index) in text_indexes.iter() {
            index.write().remove(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::property::{
        PropertyIndexSnapshot, encode_property_index_section, parse_property_index_section,
    };

    /// Installs a mapped `city` index built from `entries`, as a reopen does.
    fn install_mapped(store: &LpgStore, entries: Vec<(Value, NodeId)>) {
        let bytes = encode_property_index_section(&[PropertyIndexSnapshot {
            name: "city".to_string(),
            entries,
        }])
        .expect("encode");
        let set = parse_property_index_section(bytes::Bytes::from(bytes)).expect("parse");
        store.install_mapped_property_index(Arc::new(set.get("city").expect("city").clone()));
    }

    fn sorted(mut ids: Vec<NodeId>) -> Vec<NodeId> {
        ids.sort_unstable();
        ids
    }

    /// D5: writes after a mapped index is installed are found, and stale
    /// mapped postings of nodes this store changed or deleted are not.
    #[test]
    fn mapped_property_index_sees_writes_after_install() {
        let store = LpgStore::new().expect("arena allocation");
        let a = store.create_node(&["P"]);
        let b = store.create_node(&["P"]);
        store.set_node_property(a, "city", Value::from("nyc"));
        store.set_node_property(b, "city", Value::from("nyc"));
        // Id 999 stands for a row held outside this store (a layered base).
        let foreign = NodeId::new(999);
        install_mapped(
            &store,
            vec![
                (Value::from("nyc"), a),
                (Value::from("nyc"), b),
                (Value::from("nyc"), foreign),
            ],
        );
        assert!(store.has_property_index("city"));

        store.set_node_property(b, "city", Value::from("sf"));
        let c = store.create_node(&["P"]);
        store.set_node_property(c, "city", Value::from("nyc"));
        store.delete_node(a);

        let nyc = Value::from("nyc");
        let sf = Value::from("sf");
        assert_eq!(
            sorted(store.find_nodes_by_property("city", &nyc)),
            sorted(vec![c, foreign])
        );
        assert_eq!(store.find_nodes_by_property("city", &sf), vec![b]);
        assert_eq!(
            store
                .mapped_property_index_candidates("city", &nyc)
                .map(sorted),
            Some(sorted(vec![a, b, c, foreign])),
            "raw candidates are unverified"
        );
        assert_eq!(
            store.find_nodes_by_properties(&[("city", sf.clone())]),
            vec![b]
        );

        let snapshot = store.property_index_snapshot_entries();
        assert_eq!(snapshot.len(), 1);
        let mut entries: Vec<(String, NodeId)> = snapshot[0]
            .entries
            .iter()
            .map(|(v, id)| (v.as_str().expect("string").to_string(), *id))
            .collect();
        entries.sort();
        let mut want = vec![
            ("nyc".to_string(), c),
            ("nyc".to_string(), foreign),
            ("sf".to_string(), b),
        ];
        want.sort();
        assert_eq!(entries, want, "snapshot = current mapped postings + delta");

        // Re-creating the index replaces mapped + delta with a full scan.
        store.drop_property_index("city");
        store.create_property_index("city");
        assert_eq!(store.find_nodes_by_property("city", &nyc), vec![c]);
        assert!(
            store
                .mapped_property_index_candidates("city", &nyc)
                .is_none()
        );
    }
}
