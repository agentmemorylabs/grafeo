//! End-to-end bounded generation orchestrator (G-EM0.5b Phase 2b).
//!
//! Composes the bounded passes into a single streaming build that returns a
//! [`V5PayloadLease`] — never a whole-payload `Vec<u8>`:
//!
//! 1. **Node pass** — stage node source once; replay to assign dense offsets,
//!    build the mapped [`MappedNodeIdIndex`], and explode properties into a
//!    `(table_id, prop_key, row_offset)` occurrence run.
//! 2. **Edge pass** — resolve endpoints via the ID index, discover rel
//!    tables, stage forward CSR records.
//! 3. **String occurrence collection** — replay occurrence runs to collect
//!    all string occurrences (labels, prop keys, string values, edge types).
//! 4. **Global dictionary** — external-merge string occurrences into
//!    `StringOffsets`/`StringBytes`/`DictionaryCodeIndex` + a remap run.
//! 5. **Column bodies** — replay the occurrence run per column into bounded
//!    sinks (geometry-informed, no whole-graph collection).
//! 6. **CSR** — three-stage forward/reverse/ForwardPositions into sinks.
//! 7. **Metadata + directories + ID lookups + zone maps** — from bounded
//!    pass outputs.
//! 8. **Assemble** — `V5PayloadLease` over the ordered descriptors.
//!
//! Every large body flows through a [`SpoolSegmentSink`]; the only retained
//! structures are schema-bounded (labels, rel tables, per-column geometry)
//! and charged to `max_schema_bytes`.

use crate::graph::compact::generation::emit::global_dict::{
    StreamingDictionary, StringUseKind, occurrence_record,
};
use crate::graph::compact::generation::emit::payload_lease::V5PayloadLease;
use crate::graph::compact::generation::emit::sink::{SegmentSink, SpoolSegmentSink};
use crate::graph::compact::generation::{
    CancelToken, EdgeRecordSource, GenerationBudget, GenerationError, GenerationMetrics,
    NodeRecordSource, RunSetLease, RunStore,
};
use crate::graph::compact::generation_builder::column_pass::{
    ColumnGeometry, compute_column_geometries,
};
use crate::graph::compact::generation_builder::csr_pass::{self, RelTableGeometry};
use crate::graph::compact::generation_builder::edge_pass::{self, RelTableKey};
use crate::graph::compact::generation_builder::emit_columns::{
    emit_column_bodies, write_directory_segments,
};
use crate::graph::compact::generation_builder::emit_ids::{
    build_block_zone_maps, build_edge_id_lookup, build_edge_original_ids, build_metadata,
    build_node_id_lookup, build_node_original_ids, build_table_zone_maps,
    count_edges_per_rel_table,
};
use crate::graph::compact::generation_builder::emit_meta::CodecKind;
use crate::graph::compact::generation_builder::node_pass::{self, NodeSchema};
use crate::graph::compact::mapped::SegmentKind;
use crate::graph::compact::mapped::id_index::MappedNodeIdIndex;
use grafeo_common::utils::hash::FxHashMap;
use std::path::PathBuf;

/// Configuration for one bounded build.
#[derive(Debug, Clone)]
pub struct BoundedBuildConfig {
    /// Explicit generation budget (non-optional).
    pub budget: GenerationBudget,
    /// Job temp root (spools, runs, ID index spool).
    pub temp_dir: PathBuf,
    /// Correlation ID for artifact naming.
    pub correlation_id: String,
    /// In-memory cap per spool sink before spilling.
    pub spool_buf_cap: usize,
}

/// The bounded orchestrator. Drives one full build.
pub struct BoundedGenerationBuilder {
    config: BoundedBuildConfig,
    metrics: GenerationMetrics,
    cancel: Option<CancelToken>,
}

impl BoundedGenerationBuilder {
    /// Creates a builder.
    #[must_use]
    pub fn new(config: BoundedBuildConfig) -> Self {
        Self {
            config,
            metrics: GenerationMetrics::default(),
            cancel: None,
        }
    }

    /// Attaches a cancel token.
    #[must_use]
    pub fn with_cancel(mut self, token: CancelToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Borrows the metrics.
    #[must_use]
    pub fn metrics(&self) -> &GenerationMetrics {
        &self.metrics
    }

    fn make_sink(
        &self,
        kind: SegmentKind,
        alignment: u16,
        element_width: u32,
        file_id: &str,
    ) -> SpoolSegmentSink {
        SpoolSegmentSink::new(
            kind,
            1,
            0x0001,
            alignment,
            element_width,
            &self.config.temp_dir,
            format!("{}-{file_id}", self.config.correlation_id),
            self.config.spool_buf_cap,
        )
    }

    /// Runs the full bounded build, returning a streaming payload lease.
    ///
    /// # Errors
    ///
    /// [`GenerationError`] on any pass failure.
    pub fn build(
        &mut self,
        nodes: &mut dyn NodeRecordSource,
        edges: &mut dyn EdgeRecordSource,
        run_store: &mut dyn RunStore,
    ) -> Result<V5PayloadLease, GenerationError> {
        self.config.budget.validate()?;
        std::fs::create_dir_all(&self.config.temp_dir)
            .map_err(|e| GenerationError::Io(format!("create temp dir: {e}")))?;
        let budget = self.config.budget;

        // ── 1. Node pass ─────────────────────────────────────────────
        let npass = node_pass::NodePass::new(&budget, self.cancel.as_ref());
        let mut node_out = npass.stage(nodes, run_store)?;
        npass.reject_duplicate_ids(
            &node_out,
            run_store.merger("node-ids")?.as_mut(),
            &mut self.metrics,
        )?;
        let mut occ_sink = run_store.sink("occ", &budget)?;
        let mut id_index_bytes: Vec<u8> = Vec::new();
        let table_counts = npass.explode_occurrences(
            &mut node_out,
            run_store.merger("node-rows")?.as_mut(),
            occ_sink.as_mut(),
            &mut id_index_bytes,
            &mut self.metrics,
        )?;
        let occ_lease = occ_sink.finish()?;
        // ID-index records were emitted in per-table dense order (label, then
        // id); the mapped index requires global ascending order by
        // `original_id`. Sort the fixed-width records (the index is a resident
        // binary-search structure by design — 18 bytes/row, no per-row map).
        sort_id_index_records(&mut id_index_bytes)?;
        let id_index = MappedNodeIdIndex::new(bytes::Bytes::from(id_index_bytes))?;
        let node_schema = node_out.schema;

        // ── 2. Edge pass ─────────────────────────────────────────────
        let epass = edge_pass::EdgePass::new(&budget, self.cancel.as_ref());
        let (edge_rows, _rel_keys, total_edges) = epass.stage(edges, run_store)?;
        let (rel_keys, rel_id_map) = edge_pass::discover_rel_tables(
            &edge_rows,
            run_store.merger("edge-rows")?.as_mut(),
            &id_index,
            &budget,
            &mut self.metrics,
            self.cancel.as_ref(),
        )?;
        let rel_id_of = |k: &RelTableKey| rel_id_map.get(k).copied();
        let mut fwd_sink = run_store.sink("fwd-csr", &budget)?;
        epass.resolve_and_stage_forward(
            &edge_rows,
            run_store.merger("edge-rows")?.as_mut(),
            &id_index,
            &rel_id_of,
            fwd_sink.as_mut(),
            &mut self.metrics,
        )?;
        let fwd_lease = fwd_sink.finish()?;

        // ── 3. String occurrence collection ──────────────────────────
        let mut str_occ_sink = run_store.sink("str-occ", &budget)?;
        collect_string_occurrences(
            &node_schema,
            &rel_keys,
            &occ_lease,
            run_store.merger("occ")?.as_mut(),
            str_occ_sink.as_mut(),
            &budget,
            &mut self.metrics,
            self.cancel.as_ref(),
        )?;
        let str_occ_lease = str_occ_sink.finish()?;

        // ── 4. Global dictionary ─────────────────────────────────────
        let mut offsets_sink = Box::new(self.make_sink(SegmentKind::StringOffsets, 8, 8, "stroff"));
        let mut bytes_sink = Box::new(self.make_sink(SegmentKind::StringBytes, 1, 1, "strbytes"));
        let mut code_index_sink =
            Box::new(self.make_sink(SegmentKind::DictionaryCodeIndex, 8, 16, "codeidx"));
        let mut remap_sink = run_store.sink("remap", &budget)?;
        let mut dict = StreamingDictionary::new(&budget, &mut self.metrics);
        let (_dict_count, dict_strings) = dict.run(
            &str_occ_lease,
            run_store.merger("str-occ")?.as_mut(),
            offsets_sink.as_mut(),
            bytes_sink.as_mut(),
            code_index_sink.as_mut(),
            remap_sink.as_mut(),
            self.cancel.as_ref(),
        )?;
        let _remap_lease = remap_sink.finish()?;
        let string_index: FxHashMap<String, u32> = dict_strings
            .iter()
            .enumerate()
            .map(|(i, s)| (s.clone(), i as u32))
            .collect();

        // ── 5. Column geometry + bodies ──────────────────────────────
        let table_row_count = |tid: u16| table_counts[tid as usize];
        let geometries = compute_column_geometries(
            &occ_lease,
            run_store.merger("occ")?.as_mut(),
            &budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            &table_row_count,
        )?;
        let mut bodies_sink =
            Box::new(self.make_sink(SegmentKind::ColumnBodies, 1, 0, "colbodies"));
        let col_result = emit_column_bodies(
            &occ_lease,
            run_store.merger("occ")?.as_mut(),
            &geometries,
            &string_index,
            bodies_sink.as_mut(),
            &budget,
            &mut self.metrics,
            self.cancel.as_ref(),
        )?;

        // ── 6. CSR three-stage chain ─────────────────────────────────
        let src_rows_of = |rel_id: u16| -> (u64, u64) {
            let key = &rel_keys[rel_id as usize];
            (
                table_counts[key.src_table_id as usize],
                table_counts[key.dst_table_id as usize],
            )
        };
        let geo = |rel_id: u16| {
            let (src_rows, dst_rows) = src_rows_of(rel_id);
            RelTableGeometry {
                rel_id,
                src_rows,
                dst_rows,
            }
        };
        let mut fwd_off_sink =
            Box::new(self.make_sink(SegmentKind::ForwardCsrOffsets, 4, 4, "fwdoff"));
        let mut fwd_tgt_sink =
            Box::new(self.make_sink(SegmentKind::ForwardCsrTargets, 4, 4, "fwdtgt"));
        let mut rev_sink = run_store.sink("rev-csr", &budget)?;
        csr_pass::stream_forward_csr(
            &fwd_lease,
            run_store.merger("fwd-csr")?.as_mut(),
            &geo,
            fwd_off_sink.as_mut(),
            fwd_tgt_sink.as_mut(),
            rev_sink.as_mut(),
            &budget,
            &mut self.metrics,
            self.cancel.as_ref(),
        )?;
        let rev_lease = rev_sink.finish()?;
        let mut rev_off_sink =
            Box::new(self.make_sink(SegmentKind::ReverseCsrOffsets, 4, 4, "revoff"));
        let mut rev_tgt_sink =
            Box::new(self.make_sink(SegmentKind::ReverseCsrTargets, 4, 4, "revtgt"));
        let mut pos_sink = Box::new(self.make_sink(SegmentKind::ForwardPositions, 4, 4, "fwdpos"));
        csr_pass::stream_reverse_csr(
            &rev_lease,
            run_store.merger("rev-csr")?.as_mut(),
            &geo,
            rev_off_sink.as_mut(),
            rev_tgt_sink.as_mut(),
            pos_sink.as_mut(),
            &budget,
            &mut self.metrics,
            self.cancel.as_ref(),
        )?;

        // ── 7–8. Metadata, directories, ID lookups, zone maps, assemble ──
        self.emit_all(
            &node_schema,
            &rel_keys,
            &geometries,
            &col_result,
            &string_index,
            &id_index,
            &table_counts,
            &fwd_lease,
            run_store,
            offsets_sink,
            bytes_sink,
            code_index_sink,
            bodies_sink,
            fwd_off_sink,
            fwd_tgt_sink,
            rev_off_sink,
            rev_tgt_sink,
            pos_sink,
            total_edges,
        )
    }

    /// Emission stage: metadata, directories, ID lookups, zone maps, assemble.
    #[allow(clippy::too_many_arguments)]
    fn emit_all(
        &mut self,
        node_schema: &NodeSchema,
        rel_keys: &[RelTableKey],
        geometries: &[ColumnGeometry],
        col_result: &crate::graph::compact::generation_builder::emit_columns::ColumnEmissionResult,
        string_index: &FxHashMap<String, u32>,
        id_index: &MappedNodeIdIndex,
        table_counts: &[u64],
        fwd_lease: &RunSetLease,
        run_store: &mut dyn RunStore,
        offsets_sink: Box<dyn SegmentSink>,
        bytes_sink: Box<dyn SegmentSink>,
        code_index_sink: Box<dyn SegmentSink>,
        bodies_sink: Box<dyn SegmentSink>,
        fwd_off_sink: Box<dyn SegmentSink>,
        fwd_tgt_sink: Box<dyn SegmentSink>,
        rev_off_sink: Box<dyn SegmentSink>,
        rev_tgt_sink: Box<dyn SegmentSink>,
        pos_sink: Box<dyn SegmentSink>,
        total_edges: u64,
    ) -> Result<V5PayloadLease, GenerationError> {
        // Build metadata.
        let node_col_keys = build_node_col_keys(geometries, node_schema.labels.len());
        let rel_col_keys = build_rel_col_keys(geometries, rel_keys.len());
        let rel_keys_flat: Vec<(String, u16, u16)> = rel_keys
            .iter()
            .map(|k| (k.edge_type.clone(), k.src_table_id, k.dst_table_id))
            .collect();
        let rel_edge_counts = count_edges_per_rel_table(
            fwd_lease,
            run_store.merger("fwd-csr")?.as_mut(),
            &self.config.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            rel_keys.len(),
        )?;
        let meta = build_metadata(
            &node_schema.labels,
            table_counts,
            &node_col_keys,
            &rel_keys_flat,
            &rel_edge_counts,
            &rel_col_keys,
            string_index,
            geometries,
            node_schema.total_nodes,
            total_edges,
        )?;

        // Build directories.
        let mut node_dir = Vec::new();
        let mut rel_dir = Vec::new();
        let mut col_dir = Vec::new();
        let mut col_block_index = Vec::new();
        let node_tables: Vec<(u16, u64, Vec<u32>)> = node_schema
            .labels
            .iter()
            .enumerate()
            .map(|(tid, _)| {
                let col_indices: Vec<u32> = col_result
                    .columns
                    .iter()
                    .filter(|c| c.kind != CodecKind::Dict || true) // all columns for this table
                    .filter(|c| {
                        geometries.iter().any(|g| {
                            g.table_id == tid as u16 && g.key == col_key_for(c, geometries)
                        })
                    })
                    .map(|c| c.column_index)
                    .collect();
                (tid as u16, table_counts[tid], col_indices)
            })
            .collect();
        let rel_tables: Vec<(u16, u16, u16, u64, Vec<u32>)> = rel_keys
            .iter()
            .enumerate()
            .map(|(rid, k)| {
                let col_indices: Vec<u32> = col_result
                    .columns
                    .iter()
                    .filter(|c| {
                        geometries.iter().any(|g| {
                            g.table_id == (0x8000 | rid as u16)
                                && g.key == col_key_for(c, geometries)
                        })
                    })
                    .map(|c| c.column_index)
                    .collect();
                (rid as u16, k.src_table_id, k.dst_table_id, rel_edge_counts[rid], col_indices)
            })
            .collect();
        write_directory_segments(
            &col_result.columns,
            &node_tables,
            &rel_tables,
            &mut node_dir,
            &mut rel_dir,
            &mut col_dir,
            &mut col_block_index,
        )?;

        // Build ID lookups.
        let node_lookup = build_node_id_lookup(id_index);
        let node_orig = build_node_original_ids(id_index, table_counts);
        let edge_lookup = build_edge_id_lookup(
            fwd_lease,
            run_store.merger("fwd-csr")?.as_mut(),
            &self.config.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
        )?;
        let edge_orig = build_edge_original_ids(
            fwd_lease,
            run_store.merger("fwd-csr")?.as_mut(),
            &self.config.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
        )?;

        // Build zone maps.
        let table_zm = build_table_zone_maps(geometries, string_index)?;
        let block_zm = build_block_zone_maps(&col_result.columns, geometries, string_index)?;

        // Finish all sinks → descriptors.
        let mut descriptors = vec![
            offsets_sink.finish()?,
            bytes_sink.finish()?,
            code_index_sink.finish()?,
            bodies_sink.finish()?,
            fwd_off_sink.finish()?,
            fwd_tgt_sink.finish()?,
            rev_off_sink.finish()?,
            rev_tgt_sink.finish()?,
            pos_sink.finish()?,
        ];

        // Add metadata, directories, ID lookups, zone maps as resident descriptors.
        let meta_desc = crate::graph::compact::generation::emit::descriptor::SegmentDescriptor {
            kind: SegmentKind::Metadata,
            encoding_version: 1,
            flags: 0x0001,
            alignment: 1,
            element_width: 0,
            length: meta.len() as u64,
            crc: crc32fast::hash(&meta),
            element_count: 0,
            body: crate::graph::compact::generation::emit::descriptor::SegmentBody::Resident(
                bytes::Bytes::from(meta),
            ),
        };
        descriptors.push(meta_desc);

        let node_dir_desc = make_resident_desc(SegmentKind::NodeTableDirectory, 8, 24, &node_dir);
        let rel_dir_desc = make_resident_desc(SegmentKind::RelTableDirectory, 8, 24, &rel_dir);
        let col_dir_desc = make_resident_desc(SegmentKind::ColumnDirectory, 8, 24, &col_dir);
        let col_block_desc =
            make_resident_desc(SegmentKind::ColumnBlockIndex, 4, 12, &col_block_index);
        let node_lookup_desc = make_resident_desc(SegmentKind::NodeIdLookup, 8, 24, &node_lookup);
        let node_orig_desc = make_resident_desc(SegmentKind::NodeOriginalIds, 8, 8, &node_orig);
        let edge_lookup_desc = make_resident_desc(SegmentKind::EdgeIdLookup, 8, 24, &edge_lookup);
        let edge_orig_desc = make_resident_desc(SegmentKind::EdgeOriginalIds, 8, 8, &edge_orig);
        let table_zm_desc = make_resident_desc(SegmentKind::TableZoneMaps, 8, 40, &table_zm);
        let block_zm_desc = make_resident_desc(SegmentKind::BlockZoneMaps, 8, 40, &block_zm);

        descriptors.push(node_dir_desc);
        descriptors.push(rel_dir_desc);
        descriptors.push(col_dir_desc);
        descriptors.push(col_block_desc);
        descriptors.push(node_lookup_desc);
        descriptors.push(node_orig_desc);
        descriptors.push(edge_lookup_desc);
        descriptors.push(edge_orig_desc);
        if !table_zm.is_empty() {
            descriptors.push(table_zm_desc);
        }
        if !block_zm.is_empty() {
            descriptors.push(block_zm_desc);
        }

        // Add presence/null companions.
        if !col_result.presence.is_empty() {
            let mut presence_bytes = Vec::new();
            crate::graph::compact::mapped::presence::write_presence_segment(
                &mut |b| Ok::<_, String>(presence_bytes.extend_from_slice(b)),
                &col_result.presence,
            )
            .map_err(GenerationError::Codec)?;
            descriptors.push(make_resident_desc(
                SegmentKind::ColumnRowPresence,
                1,
                0,
                &presence_bytes,
            ));
        }
        if !col_result.null.is_empty() {
            let mut null_bytes = Vec::new();
            crate::graph::compact::mapped::presence::write_null_segment(
                &mut |b| Ok::<_, String>(null_bytes.extend_from_slice(b)),
                &col_result.null,
            )
            .map_err(GenerationError::Codec)?;
            descriptors.push(make_resident_desc(
                SegmentKind::ColumnRowNull,
                1,
                0,
                &null_bytes,
            ));
        }

        // Sort by kind ascending.
        descriptors.sort_by_key(|d| d.kind.as_u16());

        Ok(V5PayloadLease::with_temp_dir(
            descriptors,
            node_schema.total_nodes,
            total_edges,
            true,
            self.metrics.clone(),
            Some(self.config.temp_dir.clone()),
        ))
    }
}

/// Collects string occurrences from the node schema, rel keys, and occurrence run.
#[allow(clippy::too_many_arguments)]
fn collect_string_occurrences(
    node_schema: &NodeSchema,
    rel_keys: &[RelTableKey],
    occ_lease: &RunSetLease,
    merger: &mut dyn crate::graph::compact::generation::ExternalRunMerger,
    str_occ_sink: &mut dyn crate::graph::compact::generation::ExternalRunSink,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<(), GenerationError> {
    // Node labels.
    for label in &node_schema.labels {
        str_occ_sink.push(occurrence_record(
            label.as_bytes(),
            StringUseKind::Label,
            &[],
        ))?;
    }
    // Edge types.
    for k in rel_keys {
        str_occ_sink.push(occurrence_record(
            k.edge_type.as_bytes(),
            StringUseKind::EdgeType,
            &[],
        ))?;
    }
    // Prop keys + string values from the occurrence run.
    merger.merge_all(&occ_lease.handles, budget, metrics, cancel, &mut |rec| {
        let (_tid, prop, _off) = split_occ_key(&rec.key)?;
        str_occ_sink.push(occurrence_record(
            prop.as_bytes(),
            StringUseKind::PropertyKey,
            &[],
        ))?;
        // String values.
        if !rec.payload.is_empty() && rec.payload[0] == 3 {
            let b = rec
                .payload
                .get(1..)
                .ok_or_else(|| GenerationError::Codec("str occ".into()))?;
            if b.len() >= 4 {
                let len = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
                if let Some(s) = b.get(4..4 + len) {
                    str_occ_sink.push(occurrence_record(s, StringUseKind::DictValue, &[]))?;
                }
            }
        }
        Ok(())
    })?;
    Ok(())
}

fn split_occ_key(key: &[u8]) -> Result<(u16, &str, u64), GenerationError> {
    if key.len() < 10 {
        return Err(GenerationError::Codec("occ key short".into()));
    }
    let tid = u16::from_be_bytes([key[0], key[1]]);
    let off_start = key.len() - 8;
    let prop = std::str::from_utf8(&key[2..off_start])
        .map_err(|_| GenerationError::Codec("occ key utf8".into()))?;
    let off = u64::from_be_bytes(key[off_start..].try_into().unwrap());
    Ok((tid, prop, off))
}

fn build_node_col_keys(geometries: &[ColumnGeometry], ntables: usize) -> Vec<Vec<String>> {
    let mut keys = vec![Vec::new(); ntables];
    for g in geometries {
        if (g.table_id as usize) < ntables {
            keys[g.table_id as usize].push(g.key.clone());
        }
    }
    for k in &mut keys {
        k.sort();
    }
    keys
}

fn build_rel_col_keys(geometries: &[ColumnGeometry], nrels: usize) -> Vec<Vec<String>> {
    let mut keys = vec![Vec::new(); nrels];
    for g in geometries {
        if g.table_id >= 0x8000 {
            let rid = (g.table_id - 0x8000) as usize;
            if rid < nrels {
                keys[rid].push(g.key.clone());
            }
        }
    }
    for k in &mut keys {
        k.sort();
    }
    keys
}

fn col_key_for(
    col: &crate::graph::compact::generation_builder::emit_columns::EmittedColumn,
    geometries: &[ColumnGeometry],
) -> String {
    geometries
        .get(col.column_index as usize)
        .map(|g| g.key.clone())
        .unwrap_or_default()
}

fn make_resident_desc(
    kind: SegmentKind,
    alignment: u16,
    element_width: u32,
    bytes: &[u8],
) -> crate::graph::compact::generation::emit::descriptor::SegmentDescriptor {
    crate::graph::compact::generation::emit::descriptor::SegmentDescriptor {
        kind,
        encoding_version: 1,
        flags: 0x0001,
        alignment,
        element_width,
        length: bytes.len() as u64,
        crc: crc32fast::hash(bytes),
        element_count: if element_width > 0 {
            (bytes.len() / element_width as usize) as u32
        } else {
            0
        },
        body: crate::graph::compact::generation::emit::descriptor::SegmentBody::Resident(
            bytes::Bytes::from(bytes.to_vec()),
        ),
    }
}

/// Sorts fixed-width ID-index records by `original_id` (the first `u64` of
/// each 18-byte record, little-endian), returning a new sorted buffer.
///
/// The node pass emits records in per-table dense order (sorted by label, then
/// id within a table); the [`MappedNodeIdIndex`] requires a single global
/// ascending run over `original_id`. The index is a resident binary-search
/// structure by design (18 bytes/row), so sorting the record set is bounded by
/// the index size, not the graph payload.
///
/// # Errors
///
/// [`GenerationError::Codec`] if the buffer is not a multiple of the record
/// width.
fn sort_id_index_records(buf: &mut Vec<u8>) -> Result<(), GenerationError> {
    use crate::graph::compact::mapped::id_index::{ID_INDEX_RECORD_LEN, write_id_index_record};
    if !buf.len().is_multiple_of(ID_INDEX_RECORD_LEN) {
        return Err(GenerationError::Codec(format!(
            "id index buffer len {} not multiple of {ID_INDEX_RECORD_LEN}",
            buf.len()
        )));
    }
    // Decode into (original_id, table_id, dense_offset) tuples, sort by id,
    // and re-serialize. Schema-bounded (one tuple per node).
    let mut records: Vec<(u64, u16, u64)> = Vec::with_capacity(buf.len() / ID_INDEX_RECORD_LEN);
    for chunk in buf.chunks_exact(ID_INDEX_RECORD_LEN) {
        let original_id = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
        let table_id = u16::from_le_bytes(chunk[8..10].try_into().unwrap());
        let dense_offset = u64::from_le_bytes(chunk[10..18].try_into().unwrap());
        records.push((original_id, table_id, dense_offset));
    }
    records.sort_unstable_by_key(|&(id, _, _)| id);
    buf.clear();
    for (original_id, table_id, dense_offset) in records {
        write_id_index_record(buf, original_id, table_id, dense_offset);
    }
    Ok(())
}
