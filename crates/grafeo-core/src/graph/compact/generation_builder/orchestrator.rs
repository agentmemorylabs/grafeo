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
    NodeRecordSource, RelSchemaDecl, RunSetLease, RunStore,
};
use crate::graph::compact::generation_builder::column_pass::{
    ColumnGeometry, compute_column_geometries,
};
use crate::graph::compact::generation_builder::csr_pass::{self, RelTableGeometry};
use crate::graph::compact::generation_builder::edge_pass::{self, RelTableKey};
use crate::graph::compact::generation::emit::dict_column_lookup::DictChunkCatalog;
use crate::graph::compact::generation_builder::emit_columns::{
    emit_column_bodies, write_directory_segments,
};
use crate::graph::compact::generation_builder::emit_ids::{
    build_block_zone_maps, build_edge_id_lookup, build_edge_original_ids, build_metadata,
    build_node_id_lookup, build_node_original_ids, build_table_zone_maps,
    count_edges_per_rel_table,
};
use crate::graph::compact::generation_builder::live_graph::LogicalLabelLookup;
use crate::graph::compact::generation_builder::emit_meta::CodecKind;
use crate::graph::compact::generation_builder::node_pass::{self, NodeSchema};
use crate::graph::compact::generation_builder::staging;
use crate::graph::compact::mapped::SegmentKind;
use crate::graph::compact::mapped::id_index::MappedNodeIdIndex;
use grafeo_common::utils::hash::FxHashMap;
use std::path::PathBuf;

/// RAII cleanup for the job temp directory until payload-lease construction.
///
/// On build failure, cancellation, or unwind before [`V5PayloadLease`] is
/// returned, removes `build-tmp` and any intermediate artifacts (`dictchunks.bin`,
/// spool files, ID index). Disarmed on success so the lease owns cleanup.
struct JobTempGuard {
    path: PathBuf,
    armed: bool,
}

impl JobTempGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for JobTempGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

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
    /// Optional relationship schema declarations for endpoint table validation
    /// (B9). When non-empty, each edge's src/dst table labels are checked
    /// against the declared src_label/dst_label. Schema-bounded (not
    /// graph-proportional).
    pub rel_schemas: Vec<RelSchemaDecl>,
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
        &mut self,
        kind: SegmentKind,
        alignment: u16,
        element_width: u32,
        file_id: &str,
    ) -> Result<SpoolSegmentSink, GenerationError> {
        // Charge the in-memory spool buffer before allocation (truthful anon ledger).
        self.metrics.reserve_anon(
            self.config.spool_buf_cap as u64,
            self.config.budget.max_anon_bytes,
        )?;
        Ok(SpoolSegmentSink::new(
            kind,
            1,
            0x0001,
            alignment,
            element_width,
            &self.config.temp_dir,
            format!("{}-{file_id}", self.config.correlation_id),
            self.config.spool_buf_cap,
        ))
    }

    /// Fold the run store's peak *concurrent* anonymous arena bytes into the
    /// authoritative job-level anon ledger. The whole-job anonymous footprint
    /// is the maximum concurrent total of live sink arenas (several sinks are
    /// live at once — e.g. the node pass holds row + id + membership arenas
    /// simultaneously), not the max of each sink's individual peak. Called once
    /// at the end of the build. The reserve/release pair updates
    /// `anon_bytes_peak` without leaving a live charge (arenas are flushed).
    fn fold_job_anon(&mut self, run_store: &dyn crate::graph::compact::generation::RunStore) {
        let peak = run_store.job_anon_peak();
        if peak > 0 {
            // Record the historical concurrent peak directly. This is not a
            // live allocation — the arenas are already flushed — so we bypass
            // the budget check and just update the high-water mark.
            self.metrics.anon_bytes_peak = self.metrics.anon_bytes_peak.max(peak);
        }
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
        let mut job_temp = JobTempGuard::new(self.config.temp_dir.clone());
        let budget = self.config.budget;
        // Sort arenas: DiskRunStore charges run bytes as temp (not anon).
        // InMemoryRunStore charges arena growth as records are pushed.
        // Do NOT pre-charge 2×sort_run_bytes against max_anon_bytes — under
        // acceptance_linux that is exactly 128 MiB and leaves zero headroom
        // for spool buffers, making the locked profile unsatisfiable.

        // ── 1. Node pass ─────────────────────────────────────────────
        let npass = node_pass::NodePass::new(&budget, self.cancel.as_ref());
        let mut node_out = npass.stage(nodes, run_store)?;
        npass.reject_duplicate_ids(
            &node_out,
            run_store.merger("node-ids")?.as_mut(),
            &mut self.metrics,
        )?;
        let mut occ_sink = run_store.sink("occ", &budget)?;
        let mut id_index_sink = run_store.sink("id-index", &budget)?;
        let table_counts = npass.explode_occurrences(
            &mut node_out,
            run_store.merger("node-rows")?.as_mut(),
            occ_sink.as_mut(),
            id_index_sink.as_mut(),
            &mut self.metrics,
        )?;
        let id_index_lease = id_index_sink.finish()?;
        // Occurrence run stays open until edge properties are appended (D0.8.0).
        // Charge run-file temp for the job ledger (truthful nonzero counters).
        let id_run_temp: u64 = id_index_lease.handles.iter().map(|h| h.byte_len).sum();
        self.metrics
            .reserve_temp(id_run_temp, budget.max_temp_bytes)?;
        // D0.8.4: external-sort ID-index records by original_id, stream the
        // fixed-width file, then open a read-only mapped view. Never retain a
        // resident Vec of the index.
        let id_index = self.materialize_mapped_id_index(run_store, &id_index_lease, &budget)?;
        let node_schema = node_out.schema;
        let membership_runs = node_out.membership_runs.take();
        let label_lookup = LogicalLabelLookup::build(
            &node_schema.labels,
            membership_runs.as_ref(),
            run_store,
            &budget,
            &mut self.metrics,
            self.cancel.as_ref(),
        )?;

        // Charge schema: node_schema (labels + label_to_table_id + table_row_counts)
        let schema_charge = node_schema.labels.iter().map(|s| s.len() as u64 + 8).sum::<u64>()
            + (node_schema.label_to_table_id.len() as u64 * 40) // FxHashMap overhead estimate
            + (node_schema.table_row_counts.len() as u64 * 8);
        self.metrics
            .reserve_schema(schema_charge, budget.max_schema_bytes)?;

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

        // Charge schema: rel_keys + rel_id_map (edge type schema)
        let rel_schema_charge = rel_keys
            .iter()
            .map(|k| k.edge_type.len() as u64 + 16)
            .sum::<u64>()
            + (rel_id_map.len() as u64 * 48); // FxHashMap<RelTableKey, u16> overhead
        self.metrics
            .reserve_schema(rel_schema_charge, budget.max_schema_bytes)?;

        let rel_id_of = |k: &RelTableKey| rel_id_map.get(k).copied();
        let mut fwd_sink = run_store.sink("fwd-csr", &budget)?;
        epass.resolve_and_stage_forward(
            &edge_rows,
            run_store.merger("edge-rows")?.as_mut(),
            &id_index,
            &rel_id_of,
            fwd_sink.as_mut(),
            &mut self.metrics,
            &label_lookup,
            &self.config.rel_schemas,
        )?;
        let fwd_lease = fwd_sink.finish()?;
        edge_pass::explode_occurrences_from_forward(
            &fwd_lease,
            run_store.merger("fwd-csr")?.as_mut(),
            occ_sink.as_mut(),
            &budget,
            &mut self.metrics,
            self.cancel.as_ref(),
        )?;
        let occ_lease = occ_sink.finish()?;
        let occ_temp: u64 = occ_lease.handles.iter().map(|h| h.byte_len).sum();
        self.metrics.reserve_temp(occ_temp, budget.max_temp_bytes)?;

        let rel_edge_counts = count_edges_per_rel_table(
            &fwd_lease,
            run_store.merger("fwd-csr")?.as_mut(),
            &self.config.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            rel_keys.len(),
        )?;

        // ── 3. String occurrence collection ──────────────────────────
        let mut str_occ_sink = run_store.sink("str-occ", &budget)?;
        collect_string_occurrences(
            &node_schema,
            &rel_keys,
            &occ_lease,
            membership_runs.as_ref(),
            run_store.merger("occ")?.as_mut(),
            str_occ_sink.as_mut(),
            &budget,
            &mut self.metrics,
            self.cancel.as_ref(),
        )?;
        let str_occ_lease = str_occ_sink.finish()?;

        // ── 4. Global dictionary ─────────────────────────────────────
        let mut offsets_sink = Box::new(self.make_sink(SegmentKind::StringOffsets, 8, 8, "stroff")?);
        let mut bytes_sink = Box::new(self.make_sink(SegmentKind::StringBytes, 1, 1, "strbytes")?);
        let mut code_index_sink =
            Box::new(self.make_sink(SegmentKind::DictionaryCodeIndex, 8, 16, "codeidx")?);
        let mut remap_sink = run_store.sink("remap", &budget)?;
        let mut dict = StreamingDictionary::new(&budget, &mut self.metrics);
        dict.run(
            &str_occ_lease,
            run_store.merger("str-occ")?.as_mut(),
            offsets_sink.as_mut(),
            bytes_sink.as_mut(),
            code_index_sink.as_mut(),
            remap_sink.as_mut(),
            self.cancel.as_ref(),
        )?;
        let remap_lease = remap_sink.finish()?;

        // ── 4b. Consume the remap run (bounded) ───────────────────────
        // The dictionary pass re-emitted every string occurrence as a remap
        // record: key = use_kind || owner_key, payload = (string, code).
        // One streaming merge builds:
        //  - the schema-scoped string→code map (labels, prop keys, edge
        //    types — bounded by schema, NOT by dict values), and
        //  - a disk-backed per-column chunk file for DictValue strings
        //    (one chunk per Dict column, read one column at a time by the
        //    column-body and zone-map passes and discarded).
        // This replaces the graph-proportional `FxHashMap<String, u32>` over
        // ALL unique strings (D0.8.3: consume the remap run, never retain it).
        let catalog_path = self
            .config
            .temp_dir
            .join(format!("{}-dictchunks.cat", self.config.correlation_id));
        let schema_strings = consume_remap_run(
            &remap_lease,
            run_store.merger("remap")?.as_mut(),
            &budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            &self.config.temp_dir,
            &catalog_path,
        )?;

        // Charge schema: schema_strings (labels, prop keys, edge types — schema-bounded)
        let schema_strings_charge = schema_strings
            .iter()
            .map(|(s, _)| s.len() as u64 + 8)
            .sum::<u64>()
            + (schema_strings.len() as u64 * 40); // FxHashMap overhead
        self.metrics
            .reserve_schema(schema_strings_charge, budget.max_schema_bytes)?;

        drop(remap_lease);

        // ── 5. Column geometry + bodies ──────────────────────────────
        let table_row_count = |tid: u16| {
            if tid >= 0x8000 {
                rel_edge_counts[(tid - 0x8000) as usize]
            } else {
                table_counts[tid as usize]
            }
        };
        let geometries = compute_column_geometries(
            &occ_lease,
            run_store.merger("occ")?.as_mut(),
            &budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            &table_row_count,
        )?;

        // Charge schema: column geometries (schema-bounded metadata)
        let geo_charge = geometries
            .iter()
            .map(|g| {
                g.key.len() as u64 + 64 // key string + ColumnGeometry struct overhead
            })
            .sum::<u64>();
        self.metrics
            .reserve_schema(geo_charge, budget.max_schema_bytes)?;

        let mut bodies_sink =
            Box::new(self.make_sink(SegmentKind::ColumnBodies, 1, 0, "colbodies")?);
        let mut presence_sink =
            Box::new(self.make_sink(SegmentKind::ColumnRowPresence, 1, 0, "colpres")?);
        let mut null_sink = Box::new(self.make_sink(SegmentKind::ColumnRowNull, 1, 0, "colnull")?);
        let mut chunk_catalog =
            DictChunkCatalog::open(&catalog_path, &self.config.temp_dir).map_err(|e| {
                GenerationError::Io(format!(
                    "open dict catalog {}: {e}",
                    catalog_path.display()
                ))
            })?;
        let col_result = emit_column_bodies(
            &occ_lease,
            run_store.merger("occ")?.as_mut(),
            &geometries,
            &mut chunk_catalog,
            bodies_sink.as_mut(),
            presence_sink.as_mut(),
            null_sink.as_mut(),
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
            Box::new(self.make_sink(SegmentKind::ForwardCsrOffsets, 4, 4, "fwdoff")?);
        let mut fwd_tgt_sink =
            Box::new(self.make_sink(SegmentKind::ForwardCsrTargets, 4, 4, "fwdtgt")?);
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
            Box::new(self.make_sink(SegmentKind::ReverseCsrOffsets, 4, 4, "revoff")?);
        let mut rev_tgt_sink =
            Box::new(self.make_sink(SegmentKind::ReverseCsrTargets, 4, 4, "revtgt")?);
        let mut pos_sink = Box::new(self.make_sink(SegmentKind::ForwardPositions, 4, 4, "fwdpos")?);
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
        // Fold the peak concurrent anonymous arena bytes across all sinks into
        // the authoritative job-level anon ledger BEFORE the lease snapshots
        // the metrics, so the reported anon_bytes_peak is truthful.
        self.fold_job_anon(run_store);
        let job_temp_dir = self.config.temp_dir.clone();
        let lease = self.emit_all(
            &node_schema,
            &rel_keys,
            &geometries,
            &col_result,
            &schema_strings,
            &id_index,
            &table_counts,
            &fwd_lease,
            membership_runs.as_ref(),
            run_store,
            &catalog_path,
            &job_temp_dir,
            offsets_sink,
            bytes_sink,
            code_index_sink,
            bodies_sink,
            presence_sink,
            null_sink,
            fwd_off_sink,
            fwd_tgt_sink,
            rev_off_sink,
            rev_tgt_sink,
            pos_sink,
            total_edges,
        )?;
        run_store.cleanup_job_artifacts()?;
        job_temp.disarm();
        Ok(lease)
    }

    /// External-sort ID-index runs, stream a fixed-width file, mmap/open it
    /// via [`RunStore::map_id_index_file`], and charge temp + mapped counters.
    fn materialize_mapped_id_index(
        &mut self,
        run_store: &mut dyn RunStore,
        id_index_lease: &RunSetLease,
        budget: &GenerationBudget,
    ) -> Result<MappedNodeIdIndex, GenerationError> {
        use crate::graph::compact::mapped::id_index::{ID_INDEX_RECORD_LEN, id_index_record_bytes};
        use std::io::Write;

        let id_index_path = self
            .config
            .temp_dir
            .join(format!("{}-id-index.bin", self.config.correlation_id));
        let mut file = std::fs::File::create(&id_index_path).map_err(|e| {
            GenerationError::Io(format!("create ID index {}: {e}", id_index_path.display()))
        })?;
        let mut file_len = 0u64;
        let mut merger = run_store.merger("id-index")?;
        merger.merge_all(
            &id_index_lease.handles,
            budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            &mut |rec| {
                if rec.key.len() != 8 || rec.payload.len() != 10 {
                    return Err(GenerationError::Codec(format!(
                        "id-index record width: key={} payload={}",
                        rec.key.len(),
                        rec.payload.len()
                    )));
                }
                let original_id = u64::from_be_bytes(
                    rec.key[0..8]
                        .try_into()
                        .map_err(|_| GenerationError::Codec("id-index key width".into()))?,
                );
                let table_id = u16::from_le_bytes(
                    rec.payload[0..2]
                        .try_into()
                        .map_err(|_| GenerationError::Codec("id-index tid width".into()))?,
                );
                let dense_offset = u64::from_le_bytes(
                    rec.payload[2..10]
                        .try_into()
                        .map_err(|_| GenerationError::Codec("id-index offset width".into()))?,
                );
                let bytes = id_index_record_bytes(original_id, table_id, dense_offset);
                file.write_all(&bytes)
                    .map_err(|e| GenerationError::Io(format!("write ID index record: {e}")))?;
                file_len = file_len.saturating_add(ID_INDEX_RECORD_LEN as u64);
                Ok(())
            },
        )?;
        file.sync_all()
            .map_err(|e| GenerationError::Io(format!("sync ID index: {e}")))?;
        drop(file);

        self.metrics.reserve_temp(file_len, budget.max_temp_bytes)?;
        let id_index = run_store.map_id_index_file(&id_index_path)?;
        self.metrics
            .reserve_mapped(id_index.byte_len() as u64, budget.max_mapped_bytes)?;
        Ok(id_index)
    }

    /// Emission stage: metadata, directories, ID lookups, zone maps, assemble.
    #[allow(clippy::too_many_arguments)]
    fn emit_all(
        &mut self,
        node_schema: &NodeSchema,
        rel_keys: &[RelTableKey],
        geometries: &[ColumnGeometry],
        col_result: &crate::graph::compact::generation_builder::emit_columns::ColumnEmissionResult,
        schema_strings: &FxHashMap<String, u32>,
        id_index: &MappedNodeIdIndex,
        table_counts: &[u64],
        fwd_lease: &RunSetLease,
        membership_runs: Option<&RunSetLease>,
        run_store: &mut dyn RunStore,
        catalog_path: &std::path::Path,
        temp_dir: &std::path::Path,
        offsets_sink: Box<dyn SegmentSink>,
        bytes_sink: Box<dyn SegmentSink>,
        code_index_sink: Box<dyn SegmentSink>,
        bodies_sink: Box<dyn SegmentSink>,
        presence_sink: Box<dyn SegmentSink>,
        null_sink: Box<dyn SegmentSink>,
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
        let rel_edge_counts_emit = count_edges_per_rel_table(
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
            &rel_edge_counts_emit,
            &rel_col_keys,
            schema_strings,
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
                    .filter(|c| {
                        c.table_id == tid as u16
                            && geometries.iter().any(|g| {
                                g.table_id == tid as u16 && g.key == c.key
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
                        c.table_id == (0x8000 | rid as u16)
                            && geometries.iter().any(|g| {
                                g.table_id == (0x8000 | rid as u16) && g.key == c.key
                            })
                    })
                    .map(|c| c.column_index)
                    .collect();
                (
                    rid as u16,
                    k.src_table_id,
                    k.dst_table_id,
                    rel_edge_counts_emit[rid],
                    col_indices,
                )
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

        // Build ID lookups — streamed into spool sinks (no graph-proportional
        // resident vectors). Node lookup is index-order (already sorted by
        // original_id); the other three go through external sorts keyed by
        // their output order.
        let mut node_lookup_sink =
            Box::new(self.make_sink(SegmentKind::NodeIdLookup, 8, 24, "nodeidlk")?);
        build_node_id_lookup(id_index, node_lookup_sink.as_mut())?;
        let mut node_orig_sink =
            Box::new(self.make_sink(SegmentKind::NodeOriginalIds, 8, 8, "nodeorig")?);
        build_node_original_ids(
            id_index,
            run_store,
            &self.config.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            node_orig_sink.as_mut(),
        )?;
        let mut edge_lookup_sink =
            Box::new(self.make_sink(SegmentKind::EdgeIdLookup, 8, 24, "edgeidlk")?);
        build_edge_id_lookup(
            fwd_lease,
            run_store.merger("fwd-csr")?.as_mut(),
            &self.config.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            run_store,
            edge_lookup_sink.as_mut(),
        )?;
        let mut edge_orig_sink =
            Box::new(self.make_sink(SegmentKind::EdgeOriginalIds, 8, 8, "edgeorig")?);
        build_edge_original_ids(
            fwd_lease,
            run_store.merger("fwd-csr")?.as_mut(),
            &self.config.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            run_store,
            edge_orig_sink.as_mut(),
        )?;

        // Build zone maps. String bounds resolve through the per-column
        // DictValue chunk file (schema-scoped key codes come from
        // `schema_strings`); each builder reads the file sequentially in
        // column order, one column's map at a time.
        let table_zm = {
            let mut catalog = DictChunkCatalog::open(catalog_path, temp_dir)?;
            build_table_zone_maps(geometries, schema_strings, &mut catalog)?
        };
        let block_zm = {
            let mut catalog = DictChunkCatalog::open(catalog_path, temp_dir)?;
            build_block_zone_maps(&col_result.columns, geometries, schema_strings, &mut catalog)?
        };
        // Catalog + per-column `.dict` files are intermediates; remove the
        // catalog now (`.dict` files are removed with the job temp dir).
        let _ = std::fs::remove_file(catalog_path);

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

        // Add metadata, directories, ID lookups, zone maps as descriptors.
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
        let table_zm_desc = make_resident_desc(SegmentKind::TableZoneMaps, 8, 40, &table_zm);
        let block_zm_desc = make_resident_desc(SegmentKind::BlockZoneMaps, 8, 40, &block_zm);

        descriptors.push(node_dir_desc);
        descriptors.push(rel_dir_desc);
        descriptors.push(col_dir_desc);
        descriptors.push(col_block_desc);
        // ID-lookup segments stream from spool sinks (disk-backed bodies).
        descriptors.push(node_lookup_sink.finish()?);
        descriptors.push(node_orig_sink.finish()?);
        descriptors.push(edge_lookup_sink.finish()?);
        descriptors.push(edge_orig_sink.finish()?);
        if !table_zm.is_empty() {
            descriptors.push(table_zm_desc);
        }
        if !block_zm.is_empty() {
            descriptors.push(block_zm_desc);
        }

        if col_result.emitted_presence {
            descriptors.push(presence_sink.finish()?);
        }
        if col_result.emitted_null {
            descriptors.push(null_sink.finish()?);
        }

        // Add the NodeLabelMembership companion segment (only when at least
        // one node carries more than one logical label). Emitted through a
        // spool sink; records are re-sorted by (table_id, offset, label_code)
        // via the external-sort infrastructure (bounded, no resident vector).
        if let Some(membership_lease) = membership_runs {
            let mut membership_sink =
                Box::new(self.make_sink(SegmentKind::NodeLabelMembership, 8, 16, "memb")?);
            crate::graph::compact::generation_builder::membership_pass::emit_membership_segment(
                membership_lease,
                run_store.merger("membership")?.as_mut(),
                id_index,
                schema_strings,
                run_store,
                membership_sink.as_mut(),
                &self.config.budget,
                &mut self.metrics,
                self.cancel.as_ref(),
            )?;
            descriptors.push(membership_sink.finish()?);
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

/// Collects string occurrences from the node schema, rel keys, occurrence run,
/// and membership runs (for multi-label nodes).
#[allow(clippy::too_many_arguments)]
fn collect_string_occurrences(
    node_schema: &NodeSchema,
    rel_keys: &[RelTableKey],
    occ_lease: &RunSetLease,
    membership_runs: Option<&RunSetLease>,
    merger: &mut dyn crate::graph::compact::generation::ExternalRunMerger,
    str_occ_sink: &mut dyn crate::graph::compact::generation::ExternalRunSink,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
) -> Result<(), GenerationError> {
    // Node labels (physical labels from schema).
    for label in &node_schema.labels {
        str_occ_sink.push(occurrence_record(
            label.as_bytes(),
            StringUseKind::Label,
            &[],
        )?)?;
    }
    // Edge types.
    for k in rel_keys {
        str_occ_sink.push(occurrence_record(
            k.edge_type.as_bytes(),
            StringUseKind::EdgeType,
            &[],
        )?)?;
    }
    // Membership labels (logical labels from multi-label nodes).
    if let Some(membership_lease) = membership_runs {
        merger.merge_all(
            &membership_lease.handles,
            budget,
            metrics,
            cancel,
            &mut |rec| {
                let labels = staging::decode_labels(&rec.payload)?;
                for label in labels {
                    str_occ_sink.push(occurrence_record(
                        label.as_bytes(),
                        StringUseKind::Label,
                        &[],
                    )?)?;
                }
                Ok(())
            },
        )?;
    }
    // Prop keys + string values from the occurrence run.
    merger.merge_all(&occ_lease.handles, budget, metrics, cancel, &mut |rec| {
        let (tid, prop, _off) = split_occ_key(&rec.key)?;
        str_occ_sink.push(occurrence_record(
            prop.as_bytes(),
            StringUseKind::PropertyKey,
            &[],
        )?)?;
        // String values. The DictValue owner_key carries the column
        // identity (`table_id u16 BE || prop_key`), so the dictionary
        // pass's remap stream groups each column's strings together for
        // the per-column chunk map (D0.8.3).
        if !rec.payload.is_empty() && rec.payload[0] == 3 {
            let b = rec
                .payload
                .get(1..)
                .ok_or_else(|| GenerationError::Codec("str occ".into()))?;
            if b.len() >= 4 {
                let len = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
                if let Some(s) = b.get(4..4 + len) {
                    let prop_b = prop.as_bytes();
                    let prop_len = u16::try_from(prop_b.len()).map_err(|_| {
                        GenerationError::WireWidthOverflow {
                            what: "dict_value_owner_prop_len",
                            count: prop_b.len() as u64,
                            max: u64::from(u16::MAX),
                        }
                    })?;
                    let mut owner = Vec::with_capacity(4 + prop_b.len());
                    owner.extend_from_slice(&tid.to_be_bytes());
                    owner.extend_from_slice(&prop_len.to_be_bytes());
                    owner.extend_from_slice(prop_b);
                    str_occ_sink.push(occurrence_record(s, StringUseKind::DictValue, &owner)?)?;
                }
            }
        }
        Ok(())
    })?;
    Ok(())
}

/// Consumes the global-dictionary remap run (bounded, D0.8.3).
///
/// The dictionary pass re-emitted every string occurrence as a remap record:
/// key = `use_kind u8 || owner_key`, payload = `str_len u32 LE || string ||
/// code u32 LE`. One streaming merge produces:
///
/// 1. The **schema-scoped** string→code map — labels, property keys, edge
///    types (and zone strings). Bounded by schema (column count), never by
///    the number of dictionary values.
/// 2. A disk-backed **per-column `.dict` catalog** for `DictValue` records:
///    one seek/mmap chunk per column (in `(table_id, prop_key)` order). The
///    column-body and zone-map passes open one chunk at a time in lockstep
///    with the occurrence run — only the offset table is resident, never a
///    `HashMap<String, u32>`.
///
/// Catalog layout (LE, repeated until EOF):
/// `[tid u16][prop_len u16][prop][path_len u16][relative .dict path]`
///
/// # Errors
///
/// Codec, I/O, or budget failure.
#[allow(clippy::too_many_arguments)]
fn consume_remap_run(
    remap_lease: &RunSetLease,
    merger: &mut dyn crate::graph::compact::generation::ExternalRunMerger,
    budget: &GenerationBudget,
    metrics: &mut GenerationMetrics,
    cancel: Option<&CancelToken>,
    temp_dir: &std::path::Path,
    catalog_path: &std::path::Path,
) -> Result<FxHashMap<String, u32>, GenerationError> {
    use crate::graph::compact::generation::emit::global_dict::StringUseKind;
    use std::io::Write;

    let mut schema: FxHashMap<String, u32> = FxHashMap::default();
    let file = std::fs::File::create(catalog_path).map_err(|e| {
        GenerationError::Io(format!("create dict catalog {}: {e}", catalog_path.display()))
    })?;
    let mut catalog = std::io::BufWriter::with_capacity(64 * 1024, file);

    let mut current: Option<DictChunkStreamer> = None;

    merger.merge_all(&remap_lease.handles, budget, metrics, cancel, &mut |rec| {
        let Some(&kind) = rec.key.first() else {
            return Err(GenerationError::Codec("empty remap key".into()));
        };
        match kind {
            k if k == StringUseKind::DictValue as u8 => {
                // key = use_kind || tid u16 BE || prop_len u16 BE || prop || string
                // payload = code u32 LE
                if rec.key.len() < 5 {
                    return Err(GenerationError::Codec(
                        "dict remap owner key too short".into(),
                    ));
                }
                if rec.payload.len() != 4 {
                    return Err(GenerationError::Codec(
                        "dict remap code payload width".into(),
                    ));
                }
                let tid = u16::from_be_bytes([rec.key[1], rec.key[2]]);
                let prop_len = u16::from_be_bytes([rec.key[3], rec.key[4]]) as usize;
                if rec.key.len() < 5 + prop_len {
                    return Err(GenerationError::Codec(
                        "dict remap prop truncated".into(),
                    ));
                }
                let prop = &rec.key[5..5 + prop_len];
                let string = &rec.key[5 + prop_len..];
                let code = u32::from_le_bytes(rec.payload[0..4].try_into().unwrap());
                let is_new_col = current
                    .as_ref()
                    .is_none_or(|c| c.tid != tid || c.prop.as_slice() != prop);
                if is_new_col {
                    if let Some(prev) = current.take() {
                        prev.finish_into(&mut catalog, temp_dir)?;
                    }
                    current = Some(DictChunkStreamer::open(tid, prop, temp_dir)?);
                }
                current
                    .as_mut()
                    .expect("just opened")
                    .push_unique(string, code)?;
                Ok(())
            }
            k if k == StringUseKind::Label as u8
                || k == StringUseKind::PropertyKey as u8
                || k == StringUseKind::EdgeType as u8
                || k == StringUseKind::ZoneString as u8 =>
            {
                if rec.payload.len() < 8 {
                    return Err(GenerationError::Codec("remap payload too short".into()));
                }
                let slen = u32::from_le_bytes(rec.payload[0..4].try_into().unwrap()) as usize;
                let string = rec
                    .payload
                    .get(4..4 + slen)
                    .ok_or_else(|| GenerationError::Codec("remap string truncated".into()))?;
                let code_end = 4 + slen;
                if code_end + 4 != rec.payload.len() {
                    return Err(GenerationError::Codec(
                        "remap payload trailing bytes".into(),
                    ));
                }
                let code =
                    u32::from_le_bytes(rec.payload[code_end..code_end + 4].try_into().unwrap());
                let s = std::str::from_utf8(string)
                    .map_err(|_| GenerationError::Codec("remap string not UTF-8".into()))?;
                schema.insert(s.to_string(), code);
                Ok(())
            }
            other => Err(GenerationError::Codec(format!(
                "bad remap use_kind {other}"
            ))),
        }
    })?;
    if let Some(prev) = current.take() {
        prev.finish_into(&mut catalog, temp_dir)?;
    }
    catalog
        .flush()
        .map_err(|e| GenerationError::Io(format!("flush dict catalog: {e}")))?;
    Ok(schema)
}

/// Streams one Dict column's distinct (string, code) pairs to a body spool,
/// retaining only the last string for adjacent dedup.
struct DictChunkStreamer {
    tid: u16,
    prop: Vec<u8>,
    dict_name: String,
    body_path: PathBuf,
    offsets_path: PathBuf,
    body: Option<std::io::BufWriter<std::fs::File>>,
    offsets: Option<std::io::BufWriter<std::fs::File>>,
    body_bytes: u64,
    count: u32,
    last: Option<Vec<u8>>,
}

impl DictChunkStreamer {
    fn open(tid: u16, prop: &[u8], temp_dir: &std::path::Path) -> Result<Self, GenerationError> {
        let hash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            prop.hash(&mut h);
            h.finish()
        };
        let dict_name = format!("dictchunk-{tid}-{hash}.dict");
        let body_path = temp_dir.join(format!("dictchunk-{tid}-{hash}.body"));
        let offsets_path = temp_dir.join(format!("dictchunk-{tid}-{hash}.off"));
        let body_file = std::fs::File::create(&body_path).map_err(|e| {
            GenerationError::Io(format!("create dict chunk body {}: {e}", body_path.display()))
        })?;
        let off_file = std::fs::File::create(&offsets_path).map_err(|e| {
            GenerationError::Io(format!(
                "create dict chunk offsets {}: {e}",
                offsets_path.display()
            ))
        })?;
        Ok(Self {
            tid,
            prop: prop.to_vec(),
            dict_name,
            body_path,
            offsets_path,
            body: Some(std::io::BufWriter::with_capacity(64 * 1024, body_file)),
            offsets: Some(std::io::BufWriter::with_capacity(64 * 1024, off_file)),
            body_bytes: 0,
            count: 0,
            last: None,
        })
    }

    fn push_unique(&mut self, string: &[u8], code: u32) -> Result<(), GenerationError> {
        use std::io::Write;
        if self.last.as_deref() == Some(string) {
            return Ok(());
        }
        let slen = u32::try_from(string.len()).map_err(|_| GenerationError::WireWidthOverflow {
            what: "dict_chunk_str_len",
            count: string.len() as u64,
            max: u64::from(u32::MAX),
        })?;
        {
            let offsets = self
                .offsets
                .as_mut()
                .ok_or_else(|| GenerationError::Io("dict chunk offsets closed".into()))?;
            offsets
                .write_all(&self.body_bytes.to_le_bytes())
                .map_err(|e| GenerationError::Io(format!("write dict chunk offset: {e}")))?;
        }
        let body = self
            .body
            .as_mut()
            .ok_or_else(|| GenerationError::Io("dict chunk body closed".into()))?;
        body.write_all(&slen.to_le_bytes())
            .and_then(|_| body.write_all(string))
            .and_then(|_| body.write_all(&code.to_le_bytes()))
            .map_err(|e| GenerationError::Io(format!("write dict chunk entry: {e}")))?;
        self.body_bytes = self
            .body_bytes
            .checked_add(4 + u64::from(slen) + 4)
            .ok_or(GenerationError::WireWidthOverflow {
                what: "dict_chunk_body_bytes",
                count: u64::MAX,
                max: u64::MAX,
            })?;
        self.count = self
            .count
            .checked_add(1)
            .ok_or(GenerationError::WireWidthOverflow {
                what: "dict_chunk_entry_count",
                count: u64::from(u32::MAX) + 1,
                max: u64::from(u32::MAX),
            })?;
        self.last = Some(string.to_vec());
        Ok(())
    }

    fn finish_into(
        mut self,
        catalog: &mut std::io::BufWriter<std::fs::File>,
        temp_dir: &std::path::Path,
    ) -> Result<(), GenerationError> {
        use std::io::{Read, Write};
        if let Some(mut body) = self.body.take() {
            body.flush()
                .map_err(|e| GenerationError::Io(format!("flush dict chunk body: {e}")))?;
        }
        if let Some(mut offsets) = self.offsets.take() {
            offsets
                .flush()
                .map_err(|e| GenerationError::Io(format!("flush dict chunk offsets: {e}")))?;
        }
        let prop_len =
            u16::try_from(self.prop.len()).map_err(|_| GenerationError::WireWidthOverflow {
                what: "dict_chunk_prop_len",
                count: self.prop.len() as u64,
                max: u64::from(u16::MAX),
            })?;
        let dict_path = temp_dir.join(&self.dict_name);
        let mut out = std::fs::File::create(&dict_path).map_err(|e| {
            GenerationError::Io(format!("create dict chunk {}: {e}", dict_path.display()))
        })?;
        out.write_all(&self.count.to_le_bytes())
            .map_err(|e| GenerationError::Io(format!("write dict count: {e}")))?;
        {
            let offsets_path = std::mem::take(&mut self.offsets_path);
            let mut offsets = std::fs::File::open(&offsets_path).map_err(|e| {
                GenerationError::Io(format!(
                    "reopen dict chunk offsets {}: {e}",
                    offsets_path.display()
                ))
            })?;
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = offsets.read(&mut buf).map_err(|e| {
                    GenerationError::Io(format!("read dict chunk offsets: {e}"))
                })?;
                if n == 0 {
                    break;
                }
                out.write_all(&buf[..n]).map_err(|e| {
                    GenerationError::Io(format!("copy dict chunk offsets: {e}"))
                })?;
            }
            let _ = std::fs::remove_file(&offsets_path);
        }
        {
            let body_path = std::mem::take(&mut self.body_path);
            let mut body = std::fs::File::open(&body_path).map_err(|e| {
                GenerationError::Io(format!("reopen dict chunk body {}: {e}", body_path.display()))
            })?;
            let mut buf = [0u8; 64 * 1024];
            loop {
                let n = body
                    .read(&mut buf)
                    .map_err(|e| GenerationError::Io(format!("read dict chunk body: {e}")))?;
                if n == 0 {
                    break;
                }
                out.write_all(&buf[..n])
                    .map_err(|e| GenerationError::Io(format!("copy dict chunk body: {e}")))?;
            }
            let _ = std::fs::remove_file(&body_path);
        }

        let rel = self.dict_name.as_bytes();
        let rlen = u16::try_from(rel.len()).map_err(|_| GenerationError::WireWidthOverflow {
            what: "dict_catalog_path_len",
            count: rel.len() as u64,
            max: u64::from(u16::MAX),
        })?;
        catalog
            .write_all(&self.tid.to_le_bytes())
            .and_then(|_| catalog.write_all(&prop_len.to_le_bytes()))
            .and_then(|_| catalog.write_all(&self.prop))
            .and_then(|_| catalog.write_all(&rlen.to_le_bytes()))
            .and_then(|_| catalog.write_all(rel))
            .map_err(|e| GenerationError::Io(format!("write dict catalog entry: {e}")))?;
        Ok(())
    }
}

impl Drop for DictChunkStreamer {
    fn drop(&mut self) {
        self.body.take();
        self.offsets.take();
        if !self.body_path.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.body_path);
        }
        if !self.offsets_path.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.offsets_path);
        }
    }
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
