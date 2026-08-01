//! Bounded node pass (G-EM0.5b D0.8.2 + D0.8.4).
//!
//! One-shot node source → three external artifacts, with **zero** graph-sized
//! heap state:
//!
//! 1. **node-row run** — key `(physical_label, original_id)`, payload framed
//!    properties. Already in per-table dense order (sorted by label then id),
//!    so replaying it yields dense offsets directly.
//! 2. **ID-index run** — key `original_id` (BE), payload empty; merged +
//!    adjacent-dedup-rejected, then joined to dense offsets to emit the
//!    fixed-width [`MappedNodeIdIndex`] records. Never a `FxHashMap`.
//! 3. **occurrence run** — key `(table_id, prop_key, row_offset)`, payload one
//!    framed value. The column pass external-sorts this to build each column
//!    incrementally (geometry pass → body pass, D0.8.6) without collecting a
//!    column's values.
//!
//! The schema (sorted labels → table ids, per-table row counts) is the only
//! retained structure and is charged to `max_schema_bytes`.

use crate::graph::compact::generation::{
    CancelToken, ExternalRunMerger, ExternalRunSink, GenerationBudget, GenerationError,
    GenerationMetrics, NodeRecordSource, RunSetLease, RunStore, SortRecord,
};
use crate::graph::compact::generation_builder::staging;
use grafeo_common::utils::hash::{FxHashMap, FxHashSet};

/// Physical node-table schema (bounded by `max_schema_bytes`).
#[derive(Debug, Default)]
pub struct NodeSchema {
    /// Sorted unique physical labels (table order → table_id).
    pub labels: Vec<String>,
    /// label → table_id.
    pub label_to_table_id: FxHashMap<String, u16>,
    /// Per-table dense row count.
    pub table_row_counts: Vec<u64>,
    /// Total nodes.
    pub total_nodes: u64,
}

/// Leases + schema produced by the node pass.
pub struct NodePassOutput {
    /// Physical schema.
    pub schema: NodeSchema,
    /// Sorted node-row runs (replayed for occurrence explosion + columns).
    pub node_rows: RunSetLease,
    /// Sorted ID runs (merged to reject duplicate IDs).
    pub id_runs: RunSetLease,
    /// Sorted membership runs (replayed after dictionary pass to emit
    /// NodeLabelMembership). Key = (physical_label, original_id), payload =
    /// framed logical labels. Only present when at least one node has >1 label.
    pub membership_runs: Option<RunSetLease>,
}

/// Drives the bounded node staging pass.
pub struct NodePass<'a> {
    budget: &'a GenerationBudget,
    cancel: Option<&'a CancelToken>,
}

impl<'a> NodePass<'a> {
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

    /// Stage the source once: node-row run + ID run + label schema.
    ///
    /// # Errors
    ///
    /// Source, budget, or I/O failure.
    pub fn stage(
        &self,
        nodes: &mut dyn NodeRecordSource,
        run_store: &mut dyn RunStore,
    ) -> Result<NodePassOutput, GenerationError> {
        let mut row_sink = run_store.sink("node-rows", self.budget)?;
        let mut id_sink = run_store.sink("node-ids", self.budget)?;
        let mut membership_sink = run_store.sink("membership", self.budget)?;
        let mut labels: FxHashSet<String> = FxHashSet::default();
        let mut count = 0u64;
        let mut has_multi_label = false;

        while let Some(node) = nodes.next_node()? {
            self.check()?;
            node.validate_labels()?;
            let physical = node.physical_label().to_string();
            labels.insert(physical.clone());
            let props = staging::encode_properties(&node.properties)?;
            row_sink.push(SortRecord::new(
                staging::node_row_key(&physical, node.id.as_u64()),
                props,
            ))?;
            id_sink.push(SortRecord::new(
                node.id.as_u64().to_be_bytes().to_vec(),
                Vec::new(),
            ))?;

            // Emit membership record for multi-label nodes
            if node.labels.len() > 1 {
                has_multi_label = true;
                let payload = staging::encode_labels(&node.labels)?;
                membership_sink.push(SortRecord::new(
                    staging::node_row_key(&physical, node.id.as_u64()),
                    payload,
                ))?;
            }

            count += 1;
        }

        let node_rows = row_sink.finish()?;
        let id_runs = id_sink.finish()?;
        let membership_runs = if has_multi_label {
            Some(membership_sink.finish()?)
        } else {
            membership_sink.cleanup();
            None
        };

        let mut label_vec: Vec<String> = labels.into_iter().collect();
        label_vec.sort();
        let mut label_to_table_id = FxHashMap::default();
        for (i, l) in label_vec.iter().enumerate() {
            let tid = u16::try_from(i).map_err(|_| GenerationError::WireWidthOverflow {
                what: "node_table_id",
                count: i as u64,
                max: u64::from(u16::MAX),
            })?;
            label_to_table_id.insert(l.clone(), tid);
        }

        Ok(NodePassOutput {
            schema: NodeSchema {
                labels: label_vec,
                label_to_table_id,
                table_row_counts: Vec::new(),
                total_nodes: count,
            },
            node_rows,
            id_runs,
            membership_runs,
        })
    }

    /// Reject duplicate original IDs by adjacency in the ID run.
    ///
    /// # Errors
    ///
    /// [`GenerationError::DuplicateNodeId`] on an adjacent duplicate.
    pub fn reject_duplicate_ids(
        &self,
        out: &NodePassOutput,
        merger: &mut dyn ExternalRunMerger,
        metrics: &mut GenerationMetrics,
    ) -> Result<(), GenerationError> {
        let mut prev: Option<u64> = None;
        merger.merge_all(
            &out.id_runs.handles,
            self.budget,
            metrics,
            self.cancel,
            &mut |rec| {
                if rec.key.len() != 8 {
                    return Err(GenerationError::Codec("node id key not 8 bytes".into()));
                }
                let id = u64::from_be_bytes(rec.key[..8].try_into().unwrap());
                if prev == Some(id) {
                    return Err(GenerationError::DuplicateNodeId(id));
                }
                prev = Some(id);
                Ok(())
            },
        )
    }

    /// Replay the node-row run, assign dense offsets, and explode properties
    /// into a property-occurrence run for the bounded column pass (D0.8.6).
    ///
    /// Also emits the fixed-width `(original_id, table_id, dense_offset)`
    /// ID-index records into `id_index_out` (a bounded spool, not a resident
    /// map), and returns per-table row counts.
    ///
    /// The occurrence key is `(table_id u16 BE, prop_key, row_offset u64 BE)`
    /// with the framed property value as payload. External-sorting this key
    /// groups a column's values in row order, so the column pass can build
    /// each column incrementally without collecting all values.
    ///
    /// # Errors
    ///
    /// Source, codec, budget, or I/O failure.
    #[allow(clippy::too_many_arguments)]
    pub fn explode_occurrences(
        &self,
        out: &mut NodePassOutput,
        node_row_merger: &mut dyn ExternalRunMerger,
        occ_sink: &mut dyn ExternalRunSink,
        id_index_sink: &mut dyn ExternalRunSink,
        metrics: &mut GenerationMetrics,
    ) -> Result<Vec<u64>, GenerationError> {
        let ntables = out.schema.labels.len();
        let mut table_counts = vec![0u64; ntables];
        let label_to_tid = &out.schema.label_to_table_id;

        node_row_merger.merge_all(
            &out.node_rows.handles,
            self.budget,
            metrics,
            self.cancel,
            &mut |rec| {
                let (label, original_id) = staging::split_node_row_key(&rec.key)?;
                let tid = *label_to_tid
                    .get(label)
                    .ok_or_else(|| GenerationError::Codec(format!("unknown label {label}")))?;
                let off = table_counts[tid as usize];
                table_counts[tid as usize] += 1;
                // D0.8.4: emit sortable ID-index records — key = original_id BE
                // for external sort; payload = table_id LE || dense_offset LE.
                // Never accumulate into a resident Vec.
                let mut id_key = Vec::with_capacity(8);
                id_key.extend_from_slice(&original_id.to_be_bytes());
                let mut id_payload = Vec::with_capacity(10);
                id_payload.extend_from_slice(&tid.to_le_bytes());
                id_payload.extend_from_slice(&off.to_le_bytes());
                id_index_sink.push(SortRecord::new(id_key, id_payload))?;

                // Explode each property into an occurrence record.
                let props = staging::decode_properties(&rec.payload)?;
                for (key, value) in &props {
                    let mut occ_key = Vec::with_capacity(2 + key.as_str().len() + 8);
                    occ_key.extend_from_slice(&tid.to_be_bytes());
                    occ_key.extend_from_slice(key.as_str().as_bytes());
                    occ_key.extend_from_slice(&off.to_be_bytes());
                    let mut payload = Vec::new();
                    encode_single_value(&mut payload, value)?;
                    occ_sink.push(SortRecord::new(occ_key, payload))?;
                }
                Ok(())
            },
        )?;
        out.schema.table_row_counts = table_counts.clone();
        Ok(table_counts)
    }
}

/// Encodes one value into a standalone framed record (tag + body), mirroring
/// `staging::encode_properties` value encoding for a single occurrence.
pub(crate) fn encode_single_value(
    out: &mut Vec<u8>,
    v: &grafeo_common::types::Value,
) -> Result<(), GenerationError> {
    use grafeo_common::types::Value;
    match v {
        Value::Bool(b) => {
            out.push(2);
            out.push(u8::from(*b));
        }
        Value::Int64(n) => {
            out.push(1);
            out.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float64(f) => {
            out.push(4);
            out.extend_from_slice(&f.to_le_bytes());
        }
        Value::String(s) => {
            out.push(3);
            let b = s.as_bytes();
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
        Value::Vector(vec) => {
            out.push(5);
            out.extend_from_slice(&(vec.len() as u16).to_le_bytes());
            for f in vec.iter() {
                out.extend_from_slice(&f.to_le_bytes());
            }
        }
        Value::Null => {
            out.push(0); // present-null marker (D0.8.0 three-way)
        }
        other => {
            return Err(GenerationError::UnsupportedValue {
                kind: "unsupported occurrence value",
                context: format!("{other:?}"),
            });
        }
    }
    Ok(())
}
