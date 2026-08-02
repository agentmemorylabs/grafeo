//! Membership pass: emit NodeLabelMembership segment from membership runs.
//!
//! After the dictionary pass resolves label codes, this pass replays the
//! membership run (keyed by `(physical_label, original_id)`, payload = framed
//! logical labels), resolves each label to its global dictionary code via the
//! schema_strings map, and emits the NodeLabelMembership segment.
//!
//! The segment is only emitted when at least one node has multiple labels.
//!
//! ## Boundedness
//!
//! The membership records must be sorted by `(table_id, offset, label_code)`
//! for the reader's binary search, but the membership run is keyed by
//! `(physical_label, original_id)`. We therefore re-sort the resolved records
//! through the external-sort infrastructure (a second run keyed by
//! `(table_id, offset, label_code)`), never holding all records in memory.

use crate::graph::compact::generation::emit::sink::SegmentSink;
use crate::graph::compact::generation::{
    CancelToken, ExternalRunMerger, GenerationBudget, GenerationError, GenerationMetrics,
    RunSetLease, RunStore, SortRecord,
};
use crate::graph::compact::generation_builder::staging;
use crate::graph::compact::mapped::id_index::MappedNodeIdIndex;
use crate::graph::compact::mapped::label_membership::{
    LabelMembership, MEMBERSHIP_HEADER_LEN, MEMBERSHIP_RECORD_LEN,
};
use grafeo_common::utils::hash::FxHashMap;

/// Emits the NodeLabelMembership segment from the membership run.
///
/// Two bounded passes:
/// 1. Replay the membership run, resolve each logical label to its global
///    dictionary code and each node to `(table_id, offset)`, and push a
///    re-sort record keyed by `(table_id, offset, label_code)`.
/// 2. Merge the re-sorted run and stream the fixed-width membership records
///    into the sink in ascending order.
///
/// # Errors
///
/// Returns [`GenerationError`] on merge, decode, budget, or sink failure.
#[allow(clippy::too_many_arguments)]
pub fn emit_membership_segment(
    membership_lease: &RunSetLease,
    merger: &mut dyn ExternalRunMerger,
    id_index: &MappedNodeIdIndex,
    schema_strings: &FxHashMap<String, u32>,
    run_store: &mut dyn RunStore,
    sink: &mut dyn SegmentSink,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<(), GenerationError> {
    // Pass 1: resolve and re-sort by (table_id, offset, label_code).
    let mut sort_sink = run_store.sink("membership-sort", budget)?;
    merger.merge_all(
        &membership_lease.handles,
        budget,
        metrics,
        cancel,
        &mut |rec| {
            let (_physical_label, original_id) = staging::split_node_row_key(&rec.key)?;
            let logical_labels = staging::decode_labels(&rec.payload)?;

            let (table_id, dense_offset) = id_index.lookup(original_id).ok_or_else(|| {
                GenerationError::Codec(format!(
                    "membership pass: node {} not found in ID index",
                    original_id
                ))
            })?;
            let offset =
                u32::try_from(dense_offset).map_err(|_| GenerationError::WireWidthOverflow {
                    what: "membership_node_offset",
                    count: dense_offset,
                    max: u64::from(u32::MAX),
                })?;

            for label in logical_labels {
                let label_code = schema_strings.get(&label).copied().ok_or_else(|| {
                    GenerationError::Codec(format!(
                        "membership pass: label '{}' not found in schema_strings",
                        label
                    ))
                })?;

                // Sort key: table_id u16 BE || offset u32 BE || label_code u32 BE.
                let mut key = Vec::with_capacity(10);
                key.extend_from_slice(&table_id.to_be_bytes());
                key.extend_from_slice(&offset.to_be_bytes());
                key.extend_from_slice(&label_code.to_be_bytes());
                sort_sink.push(SortRecord::new(key, Vec::new()))?;
            }

            Ok(())
        },
    )?;
    let sort_lease = sort_sink.finish()?;

    // Pass 2: stream the sorted records into the membership segment.
    // Count records first for the header (one merge to count, one to write
    // would double the I/O; instead write a placeholder header and patch is
    // not possible on a streaming sink, so we count via the run handles).
    let record_count: u64 = sort_lease.handles.iter().map(|h| h.record_count).sum();
    let count = u32::try_from(record_count).map_err(|_| GenerationError::WireWidthOverflow {
        what: "membership_record_count",
        count: record_count,
        max: u64::from(u32::MAX),
    })?;

    let mut header = [0u8; MEMBERSHIP_HEADER_LEN];
    header[0..4].copy_from_slice(&count.to_le_bytes());
    sink.write(&header)?;

    let mut record = [0u8; MEMBERSHIP_RECORD_LEN];
    run_store.merger("membership-sort")?.merge_all(
        &sort_lease.handles,
        budget,
        metrics,
        cancel,
        &mut |rec| {
            if rec.key.len() != 10 {
                return Err(GenerationError::Codec("membership sort key width".into()));
            }
            let m = LabelMembership {
                node_table_id: u16::from_be_bytes([rec.key[0], rec.key[1]]),
                node_offset: u32::from_be_bytes([rec.key[2], rec.key[3], rec.key[4], rec.key[5]]),
                label_code: u32::from_be_bytes([rec.key[6], rec.key[7], rec.key[8], rec.key[9]]),
            };
            record.copy_from_slice(&m.to_bytes());
            sink.write(&record)?;
            Ok(())
        },
    )?;

    Ok(())
}
