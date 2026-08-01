//! Streaming CSR emission (G-EM0.5b D0.8.5, three-stage chain).
//!
//! Emits forward + reverse CSR for every relationship table without retaining
//! a complete adjacency, position, or CSR vector.
//!
//! **Stage 1 — forward merge.** Merge the forward sort run (keyed
//! `(rel_id, src_off, dst_off, edge_id)`). Streaming per rel table: write
//! forward offsets (with empty-row fills + final sentinel) and forward
//! targets to their sinks; assign each edge its actual forward position and
//! write a reverse record keyed `(rel_id, dst_off, src_off, edge_id)` with
//! payload = that position.
//!
//! **Stage 2 — reverse sort.** Externally sort the reverse records.
//!
//! **Stage 3 — reverse merge.** Merge to stream reverse offsets (empty-row
//! fills + sentinel), reverse targets, and real `ForwardPositions`.
//!
//! All segment bytes flow into [`SegmentSink`]s; only the current rel
//! table's running offset cursor is retained (schema-bounded).

use crate::graph::compact::generation::emit::sink::SegmentSink;
use crate::graph::compact::generation::{
    CancelToken, ExternalRunMerger, ExternalRunSink, GenerationBudget, GenerationError,
    GenerationMetrics, RunSetLease, SortRecord,
};

/// Per-rel-table row counts needed for CSR sentinel/fill emission.
pub struct RelTableGeometry {
    /// Rel table id.
    pub rel_id: u16,
    /// Source table row count.
    pub src_rows: u64,
    /// Destination table row count.
    pub dst_rows: u64,
}

/// Splits a forward sort key `(rel_id, src_off, dst_off, edge_id)`.
fn split_fwd_key(key: &[u8]) -> Result<(u16, u64, u64, u64), GenerationError> {
    if key.len() != 26 {
        return Err(GenerationError::Codec(format!(
            "forward key len {} != 26",
            key.len()
        )));
    }
    let rel = u16::from_be_bytes([key[0], key[1]]);
    let src = u64::from_be_bytes(key[2..10].try_into().unwrap());
    let dst = u64::from_be_bytes(key[10..18].try_into().unwrap());
    let eid = u64::from_be_bytes(key[18..26].try_into().unwrap());
    Ok((rel, src, dst, eid))
}

/// Splits a reverse record key `(rel_id, dst_off, src_off, edge_id)`.
fn split_rev_key(key: &[u8]) -> Result<(u16, u64, u64, u64), GenerationError> {
    split_fwd_key(key)
}

/// Stage 1: merge the forward run, stream forward offsets/targets, and emit
/// reverse records (key + forward-position payload) to `rev_sink`.
///
/// `geometry(rel_id)` yields the table's row counts. Forward offsets are
/// written as u32 LE (row_count + 1 entries with empty-row fills + sentinel);
/// forward targets as u32 LE per edge.
///
/// # Errors
///
/// Codec, budget, I/O, or offset-overflow failure.
#[allow(clippy::too_many_arguments)]
pub fn stream_forward_csr(
    fwd_lease: &RunSetLease,
    merger: &mut dyn ExternalRunMerger,
    geometry: &dyn Fn(u16) -> RelTableGeometry,
    fwd_offsets_sink: &mut dyn SegmentSink,
    fwd_targets_sink: &mut dyn SegmentSink,
    rev_sink: &mut dyn ExternalRunSink,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<(), GenerationError> {
    let mut cur_rel: Option<u16> = None;
    let mut geo = RelTableGeometry {
        rel_id: 0,
        src_rows: 0,
        dst_rows: 0,
    };
    let mut next_src_row: u64 = 0;
    let mut fwd_pos: u64 = 0;

    merger.merge_all(&fwd_lease.handles, budget, metrics, cancel, &mut |rec| {
        let (rel, src_off, dst_off, _eid) = split_fwd_key(&rec.key)?;

        // Rel table transition: flush prior sentinel, start new table.
        if cur_rel != Some(rel) {
            if let Some(prev) = cur_rel {
                // Fill remaining offsets up to sentinel.
                let g = geometry(prev);
                for _ in next_src_row..=g.src_rows {
                    fwd_offsets_sink.write(&(fwd_pos as u32).to_le_bytes())?;
                }
            }
            cur_rel = Some(rel);
            geo = geometry(rel);
            next_src_row = 0;
            fwd_pos = 0;
        }

        // Fill empty source rows before this edge.
        while next_src_row < src_off {
            fwd_offsets_sink.write(&(fwd_pos as u32).to_le_bytes())?;
            next_src_row += 1;
        }
        // First edge of a new src row: write its starting offset.
        if next_src_row == src_off {
            fwd_offsets_sink.write(&(fwd_pos as u32).to_le_bytes())?;
            next_src_row += 1;
        }

        // Forward target = dst offset.
        fwd_targets_sink.write(&(dst_off as u32).to_le_bytes())?;

        // Reverse record: key (rel, dst_off, src_off, edge_id), payload
        // = forward position (u32).
        let mut rkey = Vec::with_capacity(26);
        rkey.extend_from_slice(&rel.to_be_bytes());
        rkey.extend_from_slice(&dst_off.to_be_bytes());
        rkey.extend_from_slice(&src_off.to_be_bytes());
        rkey.extend_from_slice(&_eid.to_be_bytes());
        rev_sink.push(SortRecord::new(
            rkey,
            (fwd_pos as u32).to_le_bytes().to_vec(),
        ))?;

        fwd_pos += 1;
        Ok(())
    })?;

    // Final table sentinel.
    if let Some(rel) = cur_rel {
        let g = geometry(rel);
        let _ = g;
        let gg = geo;
        for _ in next_src_row..=gg.src_rows {
            fwd_offsets_sink.write(&(fwd_pos as u32).to_le_bytes())?;
        }
    }
    Ok(())
}

/// Stage 3: merge the sorted reverse run, streaming reverse offsets/targets
/// and real `ForwardPositions`.
///
/// # Errors
///
/// Codec, budget, I/O, or offset-overflow failure.
pub fn stream_reverse_csr(
    rev_lease: &RunSetLease,
    merger: &mut dyn ExternalRunMerger,
    geometry: &dyn Fn(u16) -> RelTableGeometry,
    rev_offsets_sink: &mut dyn SegmentSink,
    rev_targets_sink: &mut dyn SegmentSink,
    fwd_positions_sink: &mut dyn SegmentSink,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<(), GenerationError> {
    let mut cur_rel: Option<u16> = None;
    let mut geo = RelTableGeometry {
        rel_id: 0,
        src_rows: 0,
        dst_rows: 0,
    };
    let mut next_dst_row: u64 = 0;
    let mut rev_pos: u64 = 0;

    merger.merge_all(&rev_lease.handles, budget, metrics, cancel, &mut |rec| {
        let (rel, dst_off, src_off, _eid) = split_rev_key(&rec.key)?;
        if rec.payload.len() != 4 {
            return Err(GenerationError::Codec("reverse payload not u32".into()));
        }
        let fwd_pos = u32::from_le_bytes(rec.payload[..4].try_into().unwrap());

        if cur_rel != Some(rel) {
            if let Some(prev) = cur_rel {
                let g = geometry(prev);
                for _ in next_dst_row..=g.dst_rows {
                    rev_offsets_sink.write(&(rev_pos as u32).to_le_bytes())?;
                }
            }
            cur_rel = Some(rel);
            geo = geometry(rel);
            next_dst_row = 0;
            rev_pos = 0;
        }

        while next_dst_row < dst_off {
            rev_offsets_sink.write(&(rev_pos as u32).to_le_bytes())?;
            next_dst_row += 1;
        }
        if next_dst_row == dst_off {
            rev_offsets_sink.write(&(rev_pos as u32).to_le_bytes())?;
            next_dst_row += 1;
        }

        rev_targets_sink.write(&(src_off as u32).to_le_bytes())?;
        fwd_positions_sink.write(&fwd_pos.to_le_bytes())?;
        rev_pos += 1;
        Ok(())
    })?;

    if cur_rel.is_some() {
        let gg = geo;
        for _ in next_dst_row..=gg.dst_rows {
            rev_offsets_sink.write(&(rev_pos as u32).to_le_bytes())?;
        }
    }
    Ok(())
}
