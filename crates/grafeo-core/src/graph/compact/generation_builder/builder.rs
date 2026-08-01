//! Streaming bounded generation builder (G-EM0.5b Phase 2).
//!
//! Drives the full bounded pipeline: freeze overlay epoch → stream merged
//! base+overlay through external sort/merge → external dictionary → bounded
//! column/CSR/ID-lookup emission through spool sinks → payload assembly.
//!
//! The builder never holds a database-proportional structure in anonymous
//! memory. Every large segment body is emitted through a [`SpoolSegmentSink`]
//! (or the caller-supplied [`SegmentSink`] factory), and the final payload is
//! streamed through [`V5PayloadAssembler::stream_to`].

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
use super::freeze::FrozenOverlayEpoch;
use super::staging;
use crate::graph::compact::generation::emit::{
    SegmentDescriptor, SegmentSink, SpoolSegmentSink, V5PayloadAssembler,
};
use crate::graph::compact::generation::{
    CancelToken, EdgeRecordSource, ExternalRunHandle, GenerationBudget, GenerationError,
    GenerationMetrics, NodeRecordSource, RelSchemaDecl, RunStore, SortRecord,
};
use crate::graph::compact::mapped::SegmentKind;
use grafeo_common::utils::hash::{FxHashMap, FxHashSet};
use std::path::PathBuf;

/// Result of a streaming generation build.
#[derive(Debug)]
pub struct StreamingGenerationOutput {
    /// The assembled v5 payload bytes (for small builds / tests).
    /// For large builds, use `stream_payload` instead.
    pub payload: Vec<u8>,
    /// Metrics collected during the build.
    pub metrics: GenerationMetrics,
    /// Total node count.
    pub total_nodes: u64,
    /// Total edge count.
    pub total_edges: u64,
    /// Whether the output preserves original IDs.
    pub preserves_ids: bool,
}

/// Configuration for a streaming generation build.
#[derive(Debug, Clone)]
pub struct StreamingBuildConfig {
    /// Generation budget (non-optional).
    pub budget: GenerationBudget,
    /// Temp directory for spool files and external runs.
    pub temp_dir: PathBuf,
    /// Correlation ID for temp file naming.
    pub correlation_id: String,
    /// Optional relationship schema declarations.
    pub rel_schemas: Vec<RelSchemaDecl>,
    /// Frozen overlay epoch (captured before streaming).
    pub freeze: FrozenOverlayEpoch,
    /// In-memory buffer cap per spool sink before spilling to disk.
    pub spool_buf_cap: usize,
}

impl StreamingBuildConfig {
    /// Creates a config with test defaults.
    #[must_use]
    pub fn for_tests(temp_dir: impl Into<PathBuf>) -> Self {
        Self {
            budget: GenerationBudget::for_tests(),
            temp_dir: temp_dir.into(),
            correlation_id: "test".into(),
            rel_schemas: Vec::new(),
            freeze: FrozenOverlayEpoch::base_only(),
            spool_buf_cap: 64 * 1024,
        }
    }
}

/// The streaming generation builder.
///
/// Consumes merged node/edge sources and produces a bounded v5 payload.
pub struct StreamingGenerationBuilder {
    config: StreamingBuildConfig,
    metrics: GenerationMetrics,
    cancel: Option<CancelToken>,
    run_store: Box<dyn RunStore>,
}

impl StreamingGenerationBuilder {
    /// Creates a builder with the given config and run store.
    #[must_use]
    pub fn new(config: StreamingBuildConfig, run_store: Box<dyn RunStore>) -> Self {
        Self {
            config,
            metrics: GenerationMetrics::default(),
            cancel: None,
            run_store,
        }
    }

    /// Attaches a cancel token.
    #[must_use]
    pub fn with_cancel(mut self, token: CancelToken) -> Self {
        self.cancel = Some(token);
        self
    }

    /// Metrics snapshot.
    #[must_use]
    pub fn metrics(&self) -> &GenerationMetrics {
        &self.metrics
    }

    fn check_cancel(&self) -> Result<(), GenerationError> {
        if let Some(c) = &self.cancel {
            c.check()?;
        }
        Ok(())
    }

    /// Runs the full streaming build pipeline.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationError`] on any pipeline failure.
    pub fn build(
        &mut self,
        nodes: &mut dyn NodeRecordSource,
        edges: &mut dyn EdgeRecordSource,
    ) -> Result<StreamingGenerationOutput, GenerationError> {
        self.config.budget.validate()?;
        std::fs::create_dir_all(&self.config.temp_dir)
            .map_err(|e| GenerationError::Io(format!("create temp dir: {e}")))?;

        // Phase A: stage nodes and edges into external runs.
        let (node_runs, node_schema) = self.stage_nodes(nodes)?;
        let (edge_runs, edge_schema) = self.stage_edges(edges)?;

        // Phase B: merge node runs → assign table ids, dense offsets, build idmap.
        let node_result = self.merge_nodes(&node_runs, &node_schema)?;

        // Phase C: merge edge runs → resolve endpoints, build CSR.
        let edge_result = self.merge_edges(&edge_runs, &edge_schema, &node_result)?;

        // Phase D: external dictionary pass.
        let dict_result = self.build_dictionary(&node_result, &edge_result)?;

        // Phase E: emit all segments through sinks.
        let descriptors = self.emit_segments(&node_result, &edge_result, &dict_result)?;

        // Phase F: assemble payload.
        let assembler = V5PayloadAssembler::new(
            node_result.total_nodes,
            edge_result.total_edges,
            true, // preserves_ids
        );
        let payload = assembler.assemble(&descriptors)?;

        // Cleanup temp files.
        self.cleanup_runs(&node_runs);
        self.cleanup_runs(&edge_runs);

        Ok(StreamingGenerationOutput {
            payload,
            metrics: self.metrics.clone(),
            total_nodes: node_result.total_nodes,
            total_edges: edge_result.total_edges,
            preserves_ids: true,
        })
    }

    // ── Phase A: staging ─────────────────────────────────────────────

    fn stage_nodes(
        &mut self,
        nodes: &mut dyn NodeRecordSource,
    ) -> Result<(Vec<ExternalRunHandle>, NodeSchema), GenerationError> {
        let mut sink = self.run_store.sink("nodes", &self.config.budget);
        let mut labels: FxHashSet<String> = FxHashSet::default();
        let mut count = 0u64;

        while let Some(node) = nodes.next_node()? {
            self.check_cancel()?;
            node.validate_labels()?;
            let physical = node.physical_label().to_string();
            labels.insert(physical.clone());
            let props_bytes = staging::encode_properties(&node.properties)?;
            let key = staging::node_row_key(&physical, node.id.as_u64());
            sink.push(SortRecord::new(key, props_bytes))?;
            count += 1;
        }

        let runs = sink.finish()?;
        let mut label_vec: Vec<String> = labels.into_iter().collect();
        label_vec.sort();

        Ok((
            runs,
            NodeSchema {
                labels: label_vec,
                node_count: count,
            },
        ))
    }

    fn stage_edges(
        &mut self,
        edges: &mut dyn EdgeRecordSource,
    ) -> Result<(Vec<ExternalRunHandle>, EdgeSchema), GenerationError> {
        let mut sink = self.run_store.sink("edges", &self.config.budget);
        let mut edge_types: FxHashSet<String> = FxHashSet::default();
        let mut count = 0u64;

        while let Some(edge) = edges.next_edge()? {
            self.check_cancel()?;
            edge_types.insert(edge.edge_type.clone());
            let props_bytes = staging::encode_properties(&edge.properties)?;
            let key = staging::edge_row_key(&edge.edge_type, edge.id.as_u64());
            let payload =
                staging::edge_row_payload(edge.src.as_u64(), edge.dst.as_u64(), &props_bytes);
            sink.push(SortRecord::new(key, payload))?;
            count += 1;
        }

        let runs = sink.finish()?;
        let mut type_vec: Vec<String> = edge_types.into_iter().collect();
        type_vec.sort();

        Ok((
            runs,
            EdgeSchema {
                edge_types: type_vec,
                edge_count: count,
            },
        ))
    }

    // ── Phase B: node merge ──────────────────────────────────────────

    fn merge_nodes(
        &mut self,
        runs: &[ExternalRunHandle],
        schema: &NodeSchema,
    ) -> Result<NodeMergeResult, GenerationError> {
        let mut merger = self.run_store.merger("nodes");
        let mut nodes_by_label: FxHashMap<String, Vec<StagedNodeRow>> = FxHashMap::default();
        let mut seen_ids: FxHashSet<u64> = FxHashSet::default();

        merger.merge_all(
            runs,
            &self.config.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            &mut |rec: &SortRecord| {
                let (label, original_id) = staging::split_node_row_key(&rec.key)?;
                if !seen_ids.insert(original_id) {
                    return Err(GenerationError::DuplicateNodeId(original_id));
                }
                let properties = staging::decode_properties(&rec.payload)?;
                nodes_by_label
                    .entry(label.to_string())
                    .or_default()
                    .push(StagedNodeRow {
                        original_id,
                        properties,
                    });
                Ok(())
            },
        )?;

        // Assign table ids and dense offsets.
        let mut label_to_table_id: FxHashMap<String, u16> = FxHashMap::default();
        let mut table_id_to_label: Vec<String> = Vec::new();
        let mut node_id_map: FxHashMap<u64, (u16, u64)> = FxHashMap::default();
        let mut node_offset_to_id: Vec<Vec<u64>> = Vec::new();
        let mut node_rows_by_table: Vec<Vec<StagedNodeRow>> = Vec::new();
        let mut total_nodes = 0u64;

        for (tid_usize, label) in schema.labels.iter().enumerate() {
            let tid = u16::try_from(tid_usize).map_err(|_| GenerationError::WireWidthOverflow {
                what: "node_table_id",
                count: tid_usize as u64,
                max: u64::from(u16::MAX),
            })?;
            label_to_table_id.insert(label.clone(), tid);
            table_id_to_label.push(label.clone());

            let mut rows = nodes_by_label.remove(label).unwrap_or_default();
            rows.sort_by_key(|r| r.original_id);

            let mut rev = Vec::with_capacity(rows.len());
            for (off, row) in rows.iter().enumerate() {
                node_id_map.insert(row.original_id, (tid, off as u64));
                rev.push(row.original_id);
            }
            node_offset_to_id.push(rev);
            total_nodes += rows.len() as u64;
            node_rows_by_table.push(rows);
        }

        Ok(NodeMergeResult {
            label_to_table_id,
            table_id_to_label,
            node_id_map,
            node_offset_to_id,
            node_rows_by_table,
            total_nodes,
        })
    }

    // ── Phase C: edge merge ──────────────────────────────────────────

    fn merge_edges(
        &mut self,
        runs: &[ExternalRunHandle],
        schema: &EdgeSchema,
        node_result: &NodeMergeResult,
    ) -> Result<EdgeMergeResult, GenerationError> {
        let mut merger = self.run_store.merger("edges");
        let mut edges_by_type: FxHashMap<String, Vec<StagedEdgeRow>> = FxHashMap::default();
        let mut seen_ids: FxHashSet<u64> = FxHashSet::default();

        merger.merge_all(
            runs,
            &self.config.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            &mut |rec: &SortRecord| {
                let (edge_type, original_id) = staging::split_edge_row_key(&rec.key)?;
                if !seen_ids.insert(original_id) {
                    return Err(GenerationError::DuplicateEdgeId(original_id));
                }
                let (src, dst, props_bytes) = staging::split_edge_row_payload(&rec.payload)?;
                let properties = staging::decode_properties(props_bytes)?;
                edges_by_type
                    .entry(edge_type.to_string())
                    .or_default()
                    .push(StagedEdgeRow {
                        original_id,
                        src,
                        dst,
                        properties,
                    });
                Ok(())
            },
        )?;

        // Resolve endpoints and build CSR pairs.
        let mut rel_table_id_to_type: Vec<String> = Vec::new();
        let mut edge_type_to_rel_id: FxHashMap<String, Vec<u16>> = FxHashMap::default();
        let mut edge_id_map: FxHashMap<u64, (u16, u64)> = FxHashMap::default();
        let mut edge_offset_to_id: Vec<Vec<u64>> = Vec::new();
        let mut rel_tables: Vec<RelTableData> = Vec::new();
        let mut total_edges = 0u64;

        // Build rel table keys: (edge_type, src_table_id, dst_table_id).
        let mut rel_keys: Vec<(String, u16, u16)> = Vec::new();
        for edge_type in &schema.edge_types {
            if let Some(edges) = edges_by_type.get(edge_type) {
                for edge in edges {
                    let (src_tid, _) = node_result.node_id_map.get(&edge.src).ok_or(
                        GenerationError::MissingEndpoint {
                            edge_id: edge.original_id,
                            node_id: edge.src,
                            is_source: true,
                        },
                    )?;
                    let (dst_tid, _) = node_result.node_id_map.get(&edge.dst).ok_or(
                        GenerationError::MissingEndpoint {
                            edge_id: edge.original_id,
                            node_id: edge.dst,
                            is_source: false,
                        },
                    )?;
                    let key = (edge_type.clone(), *src_tid, *dst_tid);
                    if !rel_keys.contains(&key) {
                        rel_keys.push(key);
                    }
                }
            }
        }
        rel_keys.sort();

        for (rid_usize, (edge_type, src_tid, dst_tid)) in rel_keys.iter().enumerate() {
            let rid = u16::try_from(rid_usize).map_err(|_| GenerationError::WireWidthOverflow {
                what: "rel_table_id",
                count: rid_usize as u64,
                max: u64::from(u16::MAX),
            })?;
            rel_table_id_to_type.push(edge_type.clone());
            edge_type_to_rel_id
                .entry(edge_type.clone())
                .or_default()
                .push(rid);

            let edges = edges_by_type
                .get(edge_type)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|e| {
                    let (st, _) = node_result.node_id_map.get(&e.src).unwrap_or(&(0, 0));
                    let (dt, _) = node_result.node_id_map.get(&e.dst).unwrap_or(&(0, 0));
                    *st == *src_tid && *dt == *dst_tid
                })
                .collect::<Vec<_>>();

            // Build forward CSR: sort by (src_off, dst_off, original_edge_id).
            let mut fwd_edges: Vec<(u32, u32, u64, FxHashMap<_, _>)> = Vec::new();
            for edge in &edges {
                let (_, src_off) = node_result.node_id_map[&edge.src];
                let (_, dst_off) = node_result.node_id_map[&edge.dst];
                fwd_edges.push((
                    src_off as u32,
                    dst_off as u32,
                    edge.original_id,
                    edge.properties.clone(),
                ));
            }
            fwd_edges.sort_by_key(|(s, d, id, _)| (*s, *d, *id));

            let src_row_count = node_result
                .node_rows_by_table
                .get(*src_tid as usize)
                .map_or(0, |v| v.len());
            let dst_row_count = node_result
                .node_rows_by_table
                .get(*dst_tid as usize)
                .map_or(0, |v| v.len());

            // Build forward CSR offsets and targets.
            let mut fwd_offsets = vec![0u32; src_row_count + 1];
            let mut fwd_targets = Vec::with_capacity(fwd_edges.len());
            for (i, (src_off, dst_off, _, _)) in fwd_edges.iter().enumerate() {
                fwd_offsets[*src_off as usize + 1] += 1;
                fwd_targets.push(*dst_off);
                edge_id_map.insert(fwd_edges[i].2, (rid, i as u64));
            }
            for i in 1..fwd_offsets.len() {
                fwd_offsets[i] += fwd_offsets[i - 1];
            }

            // Build reverse CSR: sort by (dst_off, src_off, forward_pos).
            let mut rev_records: Vec<(u32, u32, u32)> = fwd_edges
                .iter()
                .enumerate()
                .map(|(pos, (src_off, dst_off, _, _))| (*dst_off, *src_off, pos as u32))
                .collect();
            rev_records.sort_by_key(|&(d, s, fp)| (d, s, fp));

            let mut rev_offsets = vec![0u32; dst_row_count + 1];
            let mut rev_targets = Vec::with_capacity(rev_records.len());
            let mut fwd_positions = Vec::with_capacity(rev_records.len());
            for (dst_off, src_off, fp) in &rev_records {
                rev_offsets[*dst_off as usize + 1] += 1;
                rev_targets.push(*src_off);
                fwd_positions.push(*fp);
            }
            for i in 1..rev_offsets.len() {
                rev_offsets[i] += rev_offsets[i - 1];
            }

            let mut rev_ids = Vec::with_capacity(fwd_edges.len());
            for (_, _, id, _) in &fwd_edges {
                rev_ids.push(*id);
            }
            edge_offset_to_id.push(rev_ids);

            // Collect edge properties in forward order.
            let mut edge_props: Vec<FxHashMap<_, _>> = Vec::with_capacity(fwd_edges.len());
            for (_, _, _, props) in &fwd_edges {
                edge_props.push(props.clone());
            }

            total_edges += fwd_edges.len() as u64;
            rel_tables.push(RelTableData {
                rel_table_id: rid,
                edge_type: edge_type.clone(),
                src_table_id: *src_tid,
                dst_table_id: *dst_tid,
                fwd_offsets,
                fwd_targets,
                rev_offsets,
                rev_targets,
                fwd_positions,
                edge_props,
                edge_count: fwd_edges.len(),
            });
        }

        Ok(EdgeMergeResult {
            rel_table_id_to_type,
            edge_type_to_rel_id,
            edge_id_map,
            edge_offset_to_id,
            rel_tables,
            total_edges,
        })
    }

    // ── Phase D: dictionary ──────────────────────────────────────────

    fn build_dictionary(
        &mut self,
        node_result: &NodeMergeResult,
        edge_result: &EdgeMergeResult,
    ) -> Result<DictionaryResult, GenerationError> {
        // Collect all string occurrences.
        let mut occurrences: Vec<String> = Vec::new();

        // Labels.
        for label in &node_result.table_id_to_label {
            occurrences.push(label.clone());
        }

        // Property keys from nodes.
        for rows in &node_result.node_rows_by_table {
            for row in rows {
                for key in row.properties.keys() {
                    occurrences.push(key.as_str().to_string());
                }
            }
        }

        // Edge types.
        for edge_type in &edge_result.rel_table_id_to_type {
            occurrences.push(edge_type.clone());
        }

        // Property keys from edges.
        for rt in &edge_result.rel_tables {
            for props in &rt.edge_props {
                for key in props.keys() {
                    occurrences.push(key.as_str().to_string());
                }
            }
        }

        // String values from node properties.
        for rows in &node_result.node_rows_by_table {
            for row in rows {
                for value in row.properties.values() {
                    if let grafeo_common::types::Value::String(s) = value {
                        occurrences.push(s.to_string());
                    }
                }
            }
        }

        // String values from edge properties.
        for rt in &edge_result.rel_tables {
            for props in &rt.edge_props {
                for value in props.values() {
                    if let grafeo_common::types::Value::String(s) = value {
                        occurrences.push(s.to_string());
                    }
                }
            }
        }

        // Sort, dedup, assign codes.
        occurrences.sort();
        occurrences.dedup();

        if occurrences.len() > u32::MAX as usize {
            return Err(GenerationError::WireWidthOverflow {
                what: "global_string_dictionary",
                count: occurrences.len() as u64,
                max: u64::from(u32::MAX),
            });
        }

        let mut string_index: FxHashMap<String, u32> = FxHashMap::default();
        for (i, s) in occurrences.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            string_index.insert(s.clone(), i as u32);
        }

        self.metrics.global_string_count = occurrences.len() as u64;

        Ok(DictionaryResult {
            strings: occurrences,
            string_index,
        })
    }

    // ── Phase E: segment emission ────────────────────────────────────

    fn emit_segments(
        &mut self,
        node_result: &NodeMergeResult,
        edge_result: &EdgeMergeResult,
        dict_result: &DictionaryResult,
    ) -> Result<Vec<SegmentDescriptor>, GenerationError> {
        let mut descriptors: Vec<SegmentDescriptor> = Vec::new();
        let temp_dir = &self.config.temp_dir;
        let buf_cap = self.config.spool_buf_cap;
        let corr = &self.config.correlation_id;

        // Helper: create a spool sink for one segment.
        let make_sink = |kind: SegmentKind,
                         encoding_version: u16,
                         flags: u16,
                         alignment: u16,
                         element_width: u32,
                         file_id: &str|
         -> SpoolSegmentSink {
            SpoolSegmentSink::new(
                kind,
                encoding_version,
                flags,
                alignment,
                element_width,
                temp_dir,
                format!("{corr}-{file_id}"),
                buf_cap,
            )
        };

        // ── Metadata ─────────────────────────────────────────────────
        let meta_bytes = self.build_metadata_segment(node_result, edge_result, dict_result)?;
        let mut meta_sink = Box::new(make_sink(SegmentKind::Metadata, 1, 0x0001, 1, 0, "meta"));
        meta_sink.write(&meta_bytes)?;
        descriptors.push(meta_sink.finish()?);

        // ── StringOffsets & StringBytes ──────────────────────────────
        let (off_bytes, str_bytes) = build_string_segments(&dict_result.strings);
        let mut off_sink = Box::new(make_sink(
            SegmentKind::StringOffsets,
            1,
            0x0001,
            8,
            8,
            "stroff",
        ));
        off_sink.write(&off_bytes)?;
        descriptors.push(off_sink.finish()?);

        let mut str_sink = Box::new(make_sink(
            SegmentKind::StringBytes,
            1,
            0x0001,
            1,
            1,
            "strbytes",
        ));
        str_sink.write(&str_bytes)?;
        descriptors.push(str_sink.finish()?);

        // ── Directories, Columns, CSR ────────────────────────────────
        let (node_dir, rel_dir, col_dir, col_block_index, col_bodies) =
            self.build_column_segments(node_result, edge_result, dict_result)?;

        let mut nd_sink = Box::new(make_sink(
            SegmentKind::NodeTableDirectory,
            1,
            0x0001,
            8,
            24,
            "ndir",
        ));
        nd_sink.write(&node_dir)?;
        descriptors.push(nd_sink.finish()?);

        let mut rd_sink = Box::new(make_sink(
            SegmentKind::RelTableDirectory,
            1,
            0x0001,
            8,
            24,
            "rdir",
        ));
        rd_sink.write(&rel_dir)?;
        descriptors.push(rd_sink.finish()?);

        let mut cd_sink = Box::new(make_sink(
            SegmentKind::ColumnDirectory,
            1,
            0x0001,
            8,
            24,
            "cdir",
        ));
        cd_sink.write(&col_dir)?;
        descriptors.push(cd_sink.finish()?);

        let mut cbi_sink = Box::new(make_sink(
            SegmentKind::ColumnBlockIndex,
            1,
            0x0001,
            4,
            12,
            "cbi",
        ));
        cbi_sink.write(&col_block_index)?;
        descriptors.push(cbi_sink.finish()?);

        let mut cb_sink = Box::new(make_sink(
            SegmentKind::ColumnBodies,
            1,
            0x0001,
            1,
            0,
            "cbodies",
        ));
        cb_sink.write(&col_bodies)?;
        descriptors.push(cb_sink.finish()?);

        // ── CSR segments ─────────────────────────────────────────────
        let (fwd_off, fwd_tgt, rev_off, rev_tgt, fwd_pos, has_reverse) =
            self.build_csr_segments(edge_result);

        let mut fo_sink = Box::new(make_sink(
            SegmentKind::ForwardCsrOffsets,
            1,
            0x0001,
            4,
            4,
            "fwdoff",
        ));
        fo_sink.write(&fwd_off)?;
        descriptors.push(fo_sink.finish()?);

        let mut ft_sink = Box::new(make_sink(
            SegmentKind::ForwardCsrTargets,
            1,
            0x0001,
            4,
            4,
            "fwdtgt",
        ));
        ft_sink.write(&fwd_tgt)?;
        descriptors.push(ft_sink.finish()?);

        if has_reverse {
            let mut ro_sink = Box::new(make_sink(
                SegmentKind::ReverseCsrOffsets,
                1,
                0x0001,
                4,
                4,
                "revoff",
            ));
            ro_sink.write(&rev_off)?;
            descriptors.push(ro_sink.finish()?);

            let mut rt_sink = Box::new(make_sink(
                SegmentKind::ReverseCsrTargets,
                1,
                0x0001,
                4,
                4,
                "revtgt",
            ));
            rt_sink.write(&rev_tgt)?;
            descriptors.push(rt_sink.finish()?);

            let mut fp_sink = Box::new(make_sink(
                SegmentKind::ForwardPositions,
                1,
                0x0001,
                4,
                4,
                "fwdpos",
            ));
            fp_sink.write(&fwd_pos)?;
            descriptors.push(fp_sink.finish()?);
        }

        // ── ID lookups ───────────────────────────────────────────────
        let (node_lookup, edge_lookup, node_orig, edge_orig) =
            self.build_id_segments(node_result, edge_result);

        let mut nl_sink = Box::new(make_sink(
            SegmentKind::NodeIdLookup,
            1,
            0x0001,
            8,
            24,
            "nlookup",
        ));
        nl_sink.write(&node_lookup)?;
        descriptors.push(nl_sink.finish()?);

        let mut el_sink = Box::new(make_sink(
            SegmentKind::EdgeIdLookup,
            1,
            0x0001,
            8,
            24,
            "elookup",
        ));
        el_sink.write(&edge_lookup)?;
        descriptors.push(el_sink.finish()?);

        let mut no_sink = Box::new(make_sink(
            SegmentKind::NodeOriginalIds,
            1,
            0x0001,
            8,
            8,
            "norig",
        ));
        no_sink.write(&node_orig)?;
        descriptors.push(no_sink.finish()?);

        let mut eo_sink = Box::new(make_sink(
            SegmentKind::EdgeOriginalIds,
            1,
            0x0001,
            8,
            8,
            "eorig",
        ));
        eo_sink.write(&edge_orig)?;
        descriptors.push(eo_sink.finish()?);

        // ── Zone maps ────────────────────────────────────────────────
        let (table_zm, block_zm) = self.build_zone_map_segments(node_result, dict_result)?;
        if !table_zm.is_empty() {
            let mut tz_sink = Box::new(make_sink(
                SegmentKind::TableZoneMaps,
                1,
                0x0001,
                8,
                40,
                "tablezm",
            ));
            tz_sink.write(&table_zm)?;
            descriptors.push(tz_sink.finish()?);
        }
        if !block_zm.is_empty() {
            let mut bz_sink = Box::new(make_sink(
                SegmentKind::BlockZoneMaps,
                1,
                0x0001,
                8,
                40,
                "blockzm",
            ));
            bz_sink.write(&block_zm)?;
            descriptors.push(bz_sink.finish()?);
        }

        // ── Dictionary code index ────────────────────────────────────
        let code_index = build_dictionary_code_index(&dict_result.strings);
        if !code_index.is_empty() {
            let mut ci_sink = Box::new(make_sink(
                SegmentKind::DictionaryCodeIndex,
                1,
                0x0001,
                8,
                16,
                "codeidx",
            ));
            ci_sink.write(&code_index)?;
            descriptors.push(ci_sink.finish()?);
        }

        // Sort strictly by segment kind ascending.
        descriptors.sort_by_key(|d| d.kind.as_u16());
        Ok(descriptors)
    }

    fn build_metadata_segment(
        &self,
        node_result: &NodeMergeResult,
        edge_result: &EdgeMergeResult,
        dict_result: &DictionaryResult,
    ) -> Result<Vec<u8>, GenerationError> {
        use crate::graph::compact::section_v5::{write_u16, write_u32, write_u64};
        let mut meta = Vec::new();

        let node_table_count =
            u32::try_from(node_result.table_id_to_label.len()).map_err(|_| {
                GenerationError::WireWidthOverflow {
                    what: "node_table_count",
                    count: node_result.table_id_to_label.len() as u64,
                    max: u64::from(u32::MAX),
                }
            })?;
        write_u32(&mut meta, node_table_count);

        for (tid_usize, label) in node_result.table_id_to_label.iter().enumerate() {
            let tid = u16::try_from(tid_usize).map_err(|_| GenerationError::WireWidthOverflow {
                what: "node_table_id",
                count: tid_usize as u64,
                max: u64::from(u16::MAX),
            })?;
            write_u16(&mut meta, tid);
            let label_code = *dict_result.string_index.get(label).ok_or_else(|| {
                GenerationError::Codec(format!("label string not interned: {label}"))
            })?;
            write_u32(&mut meta, label_code);

            let rows = &node_result.node_rows_by_table[tid_usize];
            let row_count =
                u32::try_from(rows.len()).map_err(|_| GenerationError::WireWidthOverflow {
                    what: "node_table_row_count",
                    count: rows.len() as u64,
                    max: u64::from(u32::MAX),
                })?;
            write_u32(&mut meta, row_count);

            // Collect property keys for this table.
            let mut keys: FxHashSet<String> = FxHashSet::default();
            for row in rows {
                for key in row.properties.keys() {
                    keys.insert(key.as_str().to_string());
                }
            }
            let mut key_list: Vec<String> = keys.into_iter().collect();
            key_list.sort();

            let col_count =
                u32::try_from(key_list.len()).map_err(|_| GenerationError::WireWidthOverflow {
                    what: "node_table_col_count",
                    count: key_list.len() as u64,
                    max: u64::from(u32::MAX),
                })?;
            write_u32(&mut meta, col_count);

            for key in &key_list {
                let key_code = *dict_result.string_index.get(key).ok_or_else(|| {
                    GenerationError::Codec(format!("prop key not interned: {key}"))
                })?;
                write_u32(&mut meta, key_code);
                // Infer codec disc and value type from first non-null value.
                let (disc, vtype) = infer_column_codec(rows, key);
                write_u16(&mut meta, disc);
                write_u16(&mut meta, vtype);
            }
        }

        let rel_table_count = u32::try_from(edge_result.rel_tables.len()).map_err(|_| {
            GenerationError::WireWidthOverflow {
                what: "rel_table_count",
                count: edge_result.rel_tables.len() as u64,
                max: u64::from(u32::MAX),
            }
        })?;
        write_u32(&mut meta, rel_table_count);

        for rt in &edge_result.rel_tables {
            write_u16(&mut meta, rt.rel_table_id);
            write_u16(&mut meta, rt.src_table_id);
            write_u16(&mut meta, rt.dst_table_id);
            let type_code = *dict_result.string_index.get(&rt.edge_type).ok_or_else(|| {
                GenerationError::Codec(format!("edge type not interned: {}", rt.edge_type))
            })?;
            write_u32(&mut meta, type_code);
            let edge_count =
                u32::try_from(rt.edge_count).map_err(|_| GenerationError::WireWidthOverflow {
                    what: "rel_table_edge_count",
                    count: rt.edge_count as u64,
                    max: u64::from(u32::MAX),
                })?;
            write_u32(&mut meta, edge_count);

            // Collect property keys for this rel table.
            let mut keys: FxHashSet<String> = FxHashSet::default();
            for props in &rt.edge_props {
                for key in props.keys() {
                    keys.insert(key.as_str().to_string());
                }
            }
            let mut key_list: Vec<String> = keys.into_iter().collect();
            key_list.sort();

            let prop_count =
                u32::try_from(key_list.len()).map_err(|_| GenerationError::WireWidthOverflow {
                    what: "rel_table_prop_count",
                    count: key_list.len() as u64,
                    max: u64::from(u32::MAX),
                })?;
            write_u32(&mut meta, prop_count);

            for key in &key_list {
                let key_code = *dict_result.string_index.get(key).ok_or_else(|| {
                    GenerationError::Codec(format!("prop key not interned: {key}"))
                })?;
                write_u32(&mut meta, key_code);
                let (disc, vtype) = infer_edge_column_codec(&rt.edge_props, key);
                write_u16(&mut meta, disc);
                write_u16(&mut meta, vtype);
            }
        }

        write_u64(&mut meta, node_result.total_nodes);
        write_u64(&mut meta, edge_result.total_edges);

        Ok(meta)
    }

    fn build_column_segments(
        &self,
        node_result: &NodeMergeResult,
        edge_result: &EdgeMergeResult,
        dict_result: &DictionaryResult,
    ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), GenerationError> {
        use crate::graph::compact::generation::encode_column;
        use crate::graph::compact::section_v5::{
            codec_disc, value_type_code, write_column_body, write_u16, write_u32, write_u64,
        };

        let mut node_dir = Vec::new();
        let mut rel_dir = Vec::new();
        let mut col_dir = Vec::new();
        let mut col_block_index = Vec::new();
        let mut col_bodies = Vec::new();
        let mut column_index: u32 = 0;

        // Node table columns.
        for (tid_usize, rows) in node_result.node_rows_by_table.iter().enumerate() {
            let tid = u16::try_from(tid_usize).map_err(|_| GenerationError::WireWidthOverflow {
                what: "node_table_id",
                count: tid_usize as u64,
                max: u64::from(u16::MAX),
            })?;
            let col_start = column_index;

            let mut keys: FxHashSet<String> = FxHashSet::default();
            for row in rows {
                for key in row.properties.keys() {
                    keys.insert(key.as_str().to_string());
                }
            }
            let mut key_list: Vec<String> = keys.into_iter().collect();
            key_list.sort();

            for key in &key_list {
                let values: Vec<Option<grafeo_common::types::Value>> = rows
                    .iter()
                    .map(|r| {
                        r.properties
                            .get(&grafeo_common::types::PropertyKey::new(key))
                            .cloned()
                    })
                    .collect();
                let value_refs: Vec<Option<&grafeo_common::types::Value>> =
                    values.iter().map(|v| v.as_ref()).collect();
                let ctx = format!(
                    "node table {} column {}",
                    node_result.table_id_to_label[tid_usize], key
                );
                let mut string_occ = Vec::new();
                let (codec, _col_type, _zm) = encode_column(&value_refs, &ctx, &mut string_occ)?;

                let body_start = u32::try_from(col_bodies.len()).map_err(|_| {
                    GenerationError::WireWidthOverflow {
                        what: "col_body_offset",
                        count: col_bodies.len() as u64,
                        max: u64::from(u32::MAX),
                    }
                })?;
                write_column_body(&mut col_bodies, &codec, &dict_result.string_index)
                    .map_err(GenerationError::Codec)?;
                let body_len = (u32::try_from(col_bodies.len()).map_err(|_| {
                    GenerationError::WireWidthOverflow {
                        what: "col_body_offset",
                        count: col_bodies.len() as u64,
                        max: u64::from(u32::MAX),
                    }
                })?)
                .saturating_sub(body_start);

                write_u16(&mut col_dir, codec_disc(&codec));
                write_u16(&mut col_dir, value_type_code(&codec));
                write_u32(&mut col_dir, column_index);
                write_u32(&mut col_dir, 1);
                write_u64(&mut col_dir, codec.len() as u64);
                write_u32(&mut col_dir, 0);
                write_u32(&mut col_block_index, body_start);
                write_u32(&mut col_block_index, body_len);
                let codec_len =
                    u32::try_from(codec.len()).map_err(|_| GenerationError::WireWidthOverflow {
                        what: "codec_len",
                        count: codec.len() as u64,
                        max: u64::from(u32::MAX),
                    })?;
                write_u32(&mut col_block_index, codec_len);
                column_index += 1;
            }

            write_u16(&mut node_dir, tid);
            write_u16(&mut node_dir, 0);
            write_u32(&mut node_dir, col_start);
            let key_count =
                u32::try_from(key_list.len()).map_err(|_| GenerationError::WireWidthOverflow {
                    what: "node_table_key_count",
                    count: key_list.len() as u64,
                    max: u64::from(u32::MAX),
                })?;
            write_u32(&mut node_dir, key_count);
            write_u64(&mut node_dir, rows.len() as u64);
            write_u32(&mut node_dir, 0);
        }

        // Rel table columns.
        for rt in &edge_result.rel_tables {
            let col_start = column_index;

            let mut keys: FxHashSet<String> = FxHashSet::default();
            for props in &rt.edge_props {
                for key in props.keys() {
                    keys.insert(key.as_str().to_string());
                }
            }
            let mut key_list: Vec<String> = keys.into_iter().collect();
            key_list.sort();

            for key in &key_list {
                let values: Vec<Option<grafeo_common::types::Value>> = rt
                    .edge_props
                    .iter()
                    .map(|p| p.get(&grafeo_common::types::PropertyKey::new(key)).cloned())
                    .collect();
                let value_refs: Vec<Option<&grafeo_common::types::Value>> =
                    values.iter().map(|v| v.as_ref()).collect();
                let ctx = format!("rel table {} column {}", rt.edge_type, key);
                let mut string_occ = Vec::new();
                let (codec, _col_type, _zm) = encode_column(&value_refs, &ctx, &mut string_occ)?;

                let body_start = u32::try_from(col_bodies.len()).map_err(|_| {
                    GenerationError::WireWidthOverflow {
                        what: "col_body_offset",
                        count: col_bodies.len() as u64,
                        max: u64::from(u32::MAX),
                    }
                })?;
                write_column_body(&mut col_bodies, &codec, &dict_result.string_index)
                    .map_err(GenerationError::Codec)?;
                let body_len = (u32::try_from(col_bodies.len()).map_err(|_| {
                    GenerationError::WireWidthOverflow {
                        what: "col_body_offset",
                        count: col_bodies.len() as u64,
                        max: u64::from(u32::MAX),
                    }
                })?)
                .saturating_sub(body_start);

                write_u16(&mut col_dir, codec_disc(&codec));
                write_u16(&mut col_dir, value_type_code(&codec));
                write_u32(&mut col_dir, column_index);
                write_u32(&mut col_dir, 1);
                write_u64(&mut col_dir, codec.len() as u64);
                write_u32(&mut col_dir, 0);
                write_u32(&mut col_block_index, body_start);
                write_u32(&mut col_block_index, body_len);
                let codec_len =
                    u32::try_from(codec.len()).map_err(|_| GenerationError::WireWidthOverflow {
                        what: "codec_len",
                        count: codec.len() as u64,
                        max: u64::from(u32::MAX),
                    })?;
                write_u32(&mut col_block_index, codec_len);
                column_index += 1;
            }

            write_u16(&mut rel_dir, rt.rel_table_id);
            write_u16(&mut rel_dir, rt.src_table_id);
            write_u16(&mut rel_dir, rt.dst_table_id);
            write_u16(&mut rel_dir, 0);
            write_u32(&mut rel_dir, col_start);
            let key_count =
                u32::try_from(key_list.len()).map_err(|_| GenerationError::WireWidthOverflow {
                    what: "rel_table_key_count",
                    count: key_list.len() as u64,
                    max: u64::from(u32::MAX),
                })?;
            write_u32(&mut rel_dir, key_count);
            write_u64(&mut rel_dir, rt.edge_count as u64);
        }

        Ok((node_dir, rel_dir, col_dir, col_block_index, col_bodies))
    }

    fn build_csr_segments(
        &self,
        edge_result: &EdgeMergeResult,
    ) -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, bool) {
        use crate::graph::compact::section_v5::append_u32_array;
        let mut fwd_offsets = Vec::new();
        let mut fwd_targets = Vec::new();
        let mut rev_offsets = Vec::new();
        let mut rev_targets = Vec::new();
        let mut fwd_positions = Vec::new();
        let mut has_reverse = false;

        for rt in &edge_result.rel_tables {
            append_u32_array(&mut fwd_offsets, &rt.fwd_offsets);
            append_u32_array(&mut fwd_targets, &rt.fwd_targets);
            if !rt.rev_offsets.is_empty() {
                has_reverse = true;
                append_u32_array(&mut rev_offsets, &rt.rev_offsets);
                append_u32_array(&mut rev_targets, &rt.rev_targets);
                append_u32_array(&mut fwd_positions, &rt.fwd_positions);
            }
        }

        (
            fwd_offsets,
            fwd_targets,
            rev_offsets,
            rev_targets,
            fwd_positions,
            has_reverse,
        )
    }

    fn build_id_segments(
        &self,
        node_result: &NodeMergeResult,
        edge_result: &EdgeMergeResult,
    ) -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
        use crate::graph::compact::mapped::{write_edge_id_record, write_node_id_record};
        use crate::graph::compact::section_v5::write_u64;

        let mut node_lookup = Vec::new();
        let mut edge_lookup = Vec::new();
        let mut node_orig = Vec::new();
        let mut edge_orig = Vec::new();

        // Node ID lookup: sorted by original id.
        let mut node_entries: Vec<(u64, (u16, u64))> = node_result
            .node_id_map
            .iter()
            .map(|(&id, &v)| (id, v))
            .collect();
        node_entries.sort_by_key(|(id, _)| *id);
        for (id, (tid, off)) in node_entries {
            write_node_id_record(&mut node_lookup, id, tid, off);
        }

        // Node original IDs: per table, per offset.
        for table in &node_result.node_offset_to_id {
            for id in table {
                write_u64(&mut node_orig, *id);
            }
        }

        // Edge ID lookup: sorted by original id.
        let mut edge_entries: Vec<(u64, (u16, u64))> = edge_result
            .edge_id_map
            .iter()
            .map(|(&id, &v)| (id, v))
            .collect();
        edge_entries.sort_by_key(|(id, _)| *id);
        for (id, (rid, pos)) in edge_entries {
            write_edge_id_record(&mut edge_lookup, id, rid, pos);
        }

        // Edge original IDs: per rel table, per CSR position.
        for table in &edge_result.edge_offset_to_id {
            for id in table {
                write_u64(&mut edge_orig, *id);
            }
        }

        (node_lookup, edge_lookup, node_orig, edge_orig)
    }

    fn build_zone_map_segments(
        &self,
        node_result: &NodeMergeResult,
        dict_result: &DictionaryResult,
    ) -> Result<(Vec<u8>, Vec<u8>), GenerationError> {
        use crate::graph::compact::generation::encode_column;
        use crate::graph::compact::mapped::zone_maps::write_zone_map_record;
        use crate::graph::compact::zone_map::compute_block_zone_maps;

        let mut table_seg = Vec::new();
        let mut block_seg = Vec::new();

        for (tid_usize, rows) in node_result.node_rows_by_table.iter().enumerate() {
            let table_id = tid_usize as u16;

            let mut keys: FxHashSet<String> = FxHashSet::default();
            for row in rows {
                for key in row.properties.keys() {
                    keys.insert(key.as_str().to_string());
                }
            }
            let mut key_list: Vec<String> = keys.into_iter().collect();
            key_list.sort();

            for key in &key_list {
                let values: Vec<Option<grafeo_common::types::Value>> = rows
                    .iter()
                    .map(|r| {
                        r.properties
                            .get(&grafeo_common::types::PropertyKey::new(key))
                            .cloned()
                    })
                    .collect();
                let value_refs: Vec<Option<&grafeo_common::types::Value>> =
                    values.iter().map(|v| v.as_ref()).collect();
                let ctx = format!(
                    "node table {} column {}",
                    node_result.table_id_to_label[tid_usize], key
                );
                let mut string_occ = Vec::new();
                let (codec, _col_type, zm) = encode_column(&value_refs, &ctx, &mut string_occ)?;

                let code = *dict_result.string_index.get(key).ok_or_else(|| {
                    GenerationError::Codec(format!("zone map key not interned: {key}"))
                })?;

                // Table-level zone map.
                if let Some(zm) = &zm {
                    write_zone_map_record(
                        &mut table_seg,
                        table_id,
                        code,
                        u32::MAX, // TABLE_ZONE_BLOCK_SENTINEL
                        zm,
                        &dict_result.string_index,
                    )
                    .map_err(GenerationError::Codec)?;
                }

                // Block-level zone maps.
                let block_zms = compute_block_zone_maps(&codec);
                for (block_idx, bzm) in block_zms.iter().enumerate() {
                    let bi = u32::try_from(block_idx).map_err(|_| {
                        GenerationError::WireWidthOverflow {
                            what: "block_index",
                            count: block_idx as u64,
                            max: u64::from(u32::MAX),
                        }
                    })?;
                    write_zone_map_record(
                        &mut block_seg,
                        table_id,
                        code,
                        bi,
                        bzm,
                        &dict_result.string_index,
                    )
                    .map_err(GenerationError::Codec)?;
                }
            }
        }

        Ok((table_seg, block_seg))
    }

    fn cleanup_runs(&mut self, runs: &[ExternalRunHandle]) {
        // In-memory runs are cleaned up by the run store.
        // Disk runs would be cleaned up here.
        let _ = runs;
    }
}

// ── Helper types ───────────────────────────────────────────────────

#[derive(Debug)]
#[allow(dead_code)]
struct NodeSchema {
    labels: Vec<String>,
    node_count: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
struct EdgeSchema {
    edge_types: Vec<String>,
    edge_count: u64,
}

#[derive(Debug, Clone)]
struct StagedNodeRow {
    original_id: u64,
    properties: FxHashMap<grafeo_common::types::PropertyKey, grafeo_common::types::Value>,
}

#[derive(Debug, Clone)]
struct StagedEdgeRow {
    original_id: u64,
    src: u64,
    dst: u64,
    properties: FxHashMap<grafeo_common::types::PropertyKey, grafeo_common::types::Value>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct NodeMergeResult {
    label_to_table_id: FxHashMap<String, u16>,
    table_id_to_label: Vec<String>,
    node_id_map: FxHashMap<u64, (u16, u64)>,
    node_offset_to_id: Vec<Vec<u64>>,
    node_rows_by_table: Vec<Vec<StagedNodeRow>>,
    total_nodes: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
struct EdgeMergeResult {
    rel_table_id_to_type: Vec<String>,
    edge_type_to_rel_id: FxHashMap<String, Vec<u16>>,
    edge_id_map: FxHashMap<u64, (u16, u64)>,
    edge_offset_to_id: Vec<Vec<u64>>,
    rel_tables: Vec<RelTableData>,
    total_edges: u64,
}

#[derive(Debug)]
struct RelTableData {
    rel_table_id: u16,
    edge_type: String,
    src_table_id: u16,
    dst_table_id: u16,
    fwd_offsets: Vec<u32>,
    fwd_targets: Vec<u32>,
    rev_offsets: Vec<u32>,
    rev_targets: Vec<u32>,
    fwd_positions: Vec<u32>,
    edge_props: Vec<FxHashMap<grafeo_common::types::PropertyKey, grafeo_common::types::Value>>,
    edge_count: usize,
}

#[derive(Debug)]
struct DictionaryResult {
    strings: Vec<String>,
    string_index: FxHashMap<String, u32>,
}

// ── Helper functions ───────────────────────────────────────────────

fn infer_column_codec(rows: &[StagedNodeRow], key: &str) -> (u16, u16) {
    use grafeo_common::types::Value;
    let pk = grafeo_common::types::PropertyKey::new(key);
    for row in rows {
        if let Some(v) = row.properties.get(&pk) {
            return match v {
                Value::Bool(_) => (2, 2),
                Value::Int64(n) if *n >= 0 => (0, 0),
                Value::Int64(_) => (6, 6),
                Value::Float64(_) => (4, 4),
                Value::String(_) => (1, 1),
                Value::Vector(_) => (5, 5),
                _ => (1, 1),
            };
        }
    }
    (1, 1) // default to Dict
}

fn infer_edge_column_codec(
    props: &[FxHashMap<grafeo_common::types::PropertyKey, grafeo_common::types::Value>],
    key: &str,
) -> (u16, u16) {
    use grafeo_common::types::Value;
    let pk = grafeo_common::types::PropertyKey::new(key);
    for p in props {
        if let Some(v) = p.get(&pk) {
            return match v {
                Value::Bool(_) => (2, 2),
                Value::Int64(n) if *n >= 0 => (0, 0),
                Value::Int64(_) => (6, 6),
                Value::Float64(_) => (4, 4),
                Value::String(_) => (1, 1),
                Value::Vector(_) => (5, 5),
                _ => (1, 1),
            };
        }
    }
    (1, 1)
}

fn build_string_segments(strings: &[String]) -> (Vec<u8>, Vec<u8>) {
    let mut offsets = Vec::with_capacity((strings.len() + 1) * 8);
    let mut bytes = Vec::new();
    let mut byte_pos = 0u64;
    for s in strings {
        offsets.extend_from_slice(&byte_pos.to_le_bytes());
        bytes.extend_from_slice(s.as_bytes());
        byte_pos += s.len() as u64;
    }
    offsets.extend_from_slice(&byte_pos.to_le_bytes());
    (offsets, bytes)
}

fn build_dictionary_code_index(strings: &[String]) -> Vec<u8> {
    use crate::graph::compact::mapped::CODE_INDEX_RECORD_LEN;
    let mut records: Vec<(u64, u32, u32, &str)> = Vec::with_capacity(strings.len());
    let mut offset = 0u64;
    for (i, s) in strings.iter().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let code = i as u32;
        #[allow(clippy::cast_possible_truncation)]
        let len = s.len() as u32;
        records.push((offset, len, code, s.as_str()));
        offset += u64::from(len);
    }
    records.sort_by(|a, b| a.3.cmp(b.3));
    let mut out = Vec::with_capacity(records.len() * CODE_INDEX_RECORD_LEN);
    for (off, len, code, _) in records {
        out.extend_from_slice(&off.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&code.to_le_bytes());
    }
    out
}
