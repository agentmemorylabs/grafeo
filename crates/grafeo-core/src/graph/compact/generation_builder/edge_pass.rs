//! Bounded edge pass (G-EM0.5b D0.8.5).
//!
//! Replays edge rows one at a time, resolves both endpoints through the
//! mapped [`MappedNodeIdIndex`] (no graph-sized `FxHashMap`), and streams
//! forward and reverse CSR through the explicit three-stage dependency chain:
//!
//! 1. external-sort resolved edges by `(rel_table_id, src_off, dst_off,
//!    edge_offset)`; merge to stream **forward** offsets/targets, assigning
//!    the actual forward position while writing reverse records keyed by
//!    `(rel_table_id, dst_off, src_off, edge_offset)` + that position;
//! 2. externally sort those reverse records;
//! 3. merge to stream **reverse** offsets/targets and real `ForwardPositions`.
//!
//! Duplicate endpoint pairs, self-loops, and sparse IDs are preserved.
//! Relationship tables are discovered by external sort of
//! `(edge_type, src_table_id, dst_table_id)` and assigned lexicographic IDs
//! under `max_schema_bytes`. Original IDs are always preserved; reverse CSR
//! and real `ForwardPositions` are always emitted (no dense-only or
//! forward-only mode).

use crate::graph::compact::generation::{
    CancelToken, ExternalRunMerger, ExternalRunSink, GenerationBudget, GenerationError,
    GenerationMetrics, RunSetLease, RunStore, SortRecord,
};
use crate::graph::compact::generation_builder::live_graph::LogicalLabelLookup;
use crate::graph::compact::generation_builder::node_pass::encode_single_value;
use crate::graph::compact::generation_builder::staging;
use crate::graph::compact::mapped::id_index::MappedNodeIdIndex;
use grafeo_common::utils::hash::FxHashMap;

/// Relationship-table schema entry (schema-bounded).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelTableKey {
    /// Edge type.
    pub edge_type: String,
    /// Source physical table id.
    pub src_table_id: u16,
    /// Destination physical table id.
    pub dst_table_id: u16,
}

/// Drives the bounded edge staging + CSR pass.
pub struct EdgePass<'a> {
    budget: &'a GenerationBudget,
    cancel: Option<&'a CancelToken>,
}

impl<'a> EdgePass<'a> {
    /// Creates the pass driver.
    #[must_use]
    pub fn new(budget: &'a GenerationBudget, cancel: Option<&'a CancelToken>) -> Self {
        Self { budget, cancel }
    }

    fn check(&self) -> Result<(), GenerationError> {
        if let Some(c) = self.cancel {
            c.check()?;
        }
        Ok(())
    }

    /// Stage edges once into the edge-row run + a rel-key discovery run.
    ///
    /// # Errors
    ///
    /// Source, budget, or I/O failure.
    pub fn stage(
        &self,
        edges: &mut dyn crate::graph::compact::generation::EdgeRecordSource,
        run_store: &mut dyn RunStore,
    ) -> Result<(RunSetLease, RunSetLease, u64), GenerationError> {
        let mut row_sink = run_store.sink("edge-rows", self.budget)?;
        let mut key_sink = run_store.sink("rel-keys", self.budget)?;
        let mut count = 0u64;

        while let Some(edge) = edges.next_edge()? {
            self.check()?;
            let props = staging::encode_properties(&edge.properties)?;
            let key = staging::edge_row_key(&edge.edge_type, edge.id.as_u64());
            let payload = staging::edge_row_payload(edge.src.as_u64(), edge.dst.as_u64(), &props);
            row_sink.push(SortRecord::new(key, payload))?;
            count += 1;
        }

        let rows = row_sink.finish()?;
        let keys = key_sink.finish()?;
        Ok((rows, keys, count))
    }

    /// Resolve endpoints and emit forward-CSR sort records + rel keys.
    ///
    /// Streams the edge-row run; for each edge, resolves `(src, dst)` via the
    /// mapped ID index (fail closed on a missing endpoint), assigns the rel
    /// table id, and pushes:
    /// - a forward sort record keyed `(rel_id, src_off, dst_off, edge_id)` with
    ///   the framed properties payload;
    /// - a rel-key record keyed `(edge_type, src_tid, dst_tid)` for external
    ///   dedup → lexicographic rel-id assignment.
    ///
    /// `rel_id_of` maps a [`RelTableKey`] to its assigned rel table id
    /// (schema-bounded, built from the rel-key run).
    ///
    /// When `rel_decls` is non-empty, each edge's src/dst logical label sets are
    /// checked for membership of the declared `src_label`/`dst_label` (B9).
    ///
    /// # Errors
    ///
    /// [`GenerationError::MissingEndpoint`], [`GenerationError::WrongTableEndpoint`],
    /// schema, budget, or I/O failure.
    pub fn resolve_and_stage_forward(
        &self,
        edge_rows: &RunSetLease,
        edge_merger: &mut dyn ExternalRunMerger,
        id_index: &MappedNodeIdIndex,
        rel_id_of: &dyn Fn(&RelTableKey) -> Option<u16>,
        fwd_sink: &mut dyn ExternalRunSink,
        metrics: &mut GenerationMetrics,
        label_lookup: &LogicalLabelLookup,
        rel_decls: &[crate::graph::compact::generation::RelSchemaDecl],
    ) -> Result<u64, GenerationError> {
        // Build schema-bounded edge_type → (src_label, dst_label) map.
        use grafeo_common::utils::hash::FxHashMap;
        let decl_map: FxHashMap<String, (String, String)> = rel_decls
            .iter()
            .map(|d| {
                (
                    d.edge_type.clone(),
                    (d.src_label.clone(), d.dst_label.clone()),
                )
            })
            .collect();
        let mut total = 0u64;
        edge_merger.merge_all(
            &edge_rows.handles,
            self.budget,
            metrics,
            self.cancel,
            &mut |rec| {
                let (edge_type, original_id) = staging::split_edge_row_key(&rec.key)?;
                let (src, dst, props) = staging::split_edge_row_payload(&rec.payload)?;
                let (src_tid, src_off) =
                    id_index
                        .lookup(src)
                        .ok_or(GenerationError::MissingEndpoint {
                            edge_id: original_id,
                            node_id: src,
                            is_source: true,
                        })?;
                let (dst_tid, dst_off) =
                    id_index
                        .lookup(dst)
                        .ok_or(GenerationError::MissingEndpoint {
                            edge_id: original_id,
                            node_id: dst,
                            is_source: false,
                        })?;
                // B9: validate endpoints against RelSchemaDecl if declared.
                if let Some((exp_src, exp_dst)) = decl_map.get(edge_type) {
                    if !label_lookup.has_label(src, src_tid, exp_src) {
                        return Err(GenerationError::WrongTableEndpoint {
                            edge_id: original_id,
                            node_id: src,
                            expected_table: src_tid,
                            actual_table: src_tid,
                            is_source: true,
                        });
                    }
                    if !label_lookup.has_label(dst, dst_tid, exp_dst) {
                        return Err(GenerationError::WrongTableEndpoint {
                            edge_id: original_id,
                            node_id: dst,
                            expected_table: dst_tid,
                            actual_table: dst_tid,
                            is_source: false,
                        });
                    }
                }
                let key = RelTableKey {
                    edge_type: edge_type.to_string(),
                    src_table_id: src_tid,
                    dst_table_id: dst_tid,
                };
                let rel_id = rel_id_of(&key).ok_or_else(|| {
                    GenerationError::Codec(format!("rel table not assigned: {key:?}"))
                })?;

                // Forward sort key: rel_id u16 BE || src_off u64 BE || dst_off
                // u64 BE || edge_id u64 BE. Payload = framed properties.
                let mut fkey = Vec::with_capacity(2 + 8 + 8 + 8);
                fkey.extend_from_slice(&rel_id.to_be_bytes());
                fkey.extend_from_slice(&src_off.to_be_bytes());
                fkey.extend_from_slice(&dst_off.to_be_bytes());
                fkey.extend_from_slice(&original_id.to_be_bytes());
                fwd_sink.push(SortRecord::new(fkey, props.to_vec()))?;
                total += 1;
                Ok(())
            },
        )?;
        Ok(total)
    }
}

/// Builds the rel-table schema (lexicographic rel ids) from staged edge rows.
///
/// Streams the edge-row run once to collect distinct `(edge_type, src_tid,
/// dst_tid)` keys (each resolution via `id_index`), sorts them
/// lexicographically, and assigns rel table ids. Retained schema is bounded
/// by `max_schema_bytes`.
///
/// # Errors
///
/// Source, budget, or I/O failure.
pub fn discover_rel_tables(
    edge_rows: &RunSetLease,
    merger: &mut dyn ExternalRunMerger,
    id_index: &MappedNodeIdIndex,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<(Vec<RelTableKey>, FxHashMap<RelTableKey, u16>), GenerationError> {
    let mut keys: Vec<RelTableKey> = Vec::new();
    merger.merge_all(&edge_rows.handles, budget, metrics, cancel, &mut |rec| {
        let (edge_type, original_id) = staging::split_edge_row_key(&rec.key)?;
        let (src, dst, _props) = staging::split_edge_row_payload(&rec.payload)?;
        let (src_tid, _) = id_index
            .lookup(src)
            .ok_or(GenerationError::MissingEndpoint {
                edge_id: original_id,
                node_id: src,
                is_source: true,
            })?;
        let (dst_tid, _) = id_index
            .lookup(dst)
            .ok_or(GenerationError::MissingEndpoint {
                edge_id: original_id,
                node_id: dst,
                is_source: false,
            })?;
        let key = RelTableKey {
            edge_type: edge_type.to_string(),
            src_table_id: src_tid,
            dst_table_id: dst_tid,
        };
        if keys.last() != Some(&key) {
            keys.push(key);
        }
        Ok(())
    })?;
    keys.sort();
    keys.dedup();
    let mut map = FxHashMap::default();
    for (i, k) in keys.iter().enumerate() {
        let rid = u16::try_from(i).map_err(|_| GenerationError::WireWidthOverflow {
            what: "rel_table_id",
            count: i as u64,
            max: u64::from(u16::MAX),
        })?;
        map.insert(k.clone(), rid);
    }
    Ok((keys, map))
}

/// Replays the forward CSR run in merge order and explodes edge properties
/// into the shared occurrence run (table_id = `0x8000 | rel_id`, row = CSR pos).
///
/// # Errors
///
/// Codec, budget, or I/O failure.
pub fn explode_occurrences_from_forward(
    fwd_lease: &RunSetLease,
    merger: &mut dyn ExternalRunMerger,
    occ_sink: &mut dyn ExternalRunSink,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<(), GenerationError> {
    let mut fwd_pos: u64 = 0;
    let mut cur_rel: Option<u16> = None;

    merger.merge_all(&fwd_lease.handles, budget, metrics, cancel, &mut |rec| {
        if rec.key.len() != 26 {
            return Err(GenerationError::Codec(format!(
                "forward key len {} != 26",
                rec.key.len()
            )));
        }
        let rel = u16::from_be_bytes([rec.key[0], rec.key[1]]);
        if cur_rel != Some(rel) {
            cur_rel = Some(rel);
            fwd_pos = 0;
        }
        let table_id = 0x8000 | rel;
        let props = staging::decode_properties(&rec.payload)?;
        for (key, value) in &props {
            let mut occ_key = Vec::with_capacity(2 + key.as_str().len() + 8);
            occ_key.extend_from_slice(&table_id.to_be_bytes());
            occ_key.extend_from_slice(key.as_str().as_bytes());
            occ_key.extend_from_slice(&fwd_pos.to_be_bytes());
            let mut payload = Vec::new();
            encode_single_value(&mut payload, value)?;
            occ_sink.push(SortRecord::new(occ_key, payload))?;
        }
        fwd_pos += 1;
        Ok(())
    })?;
    Ok(())
}
