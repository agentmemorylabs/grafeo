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

use crate::graph::compact::generation::emit::dict_column_lookup::DictChunkCatalog;
use crate::graph::compact::generation::emit::global_dict::StreamingDictionary;
use crate::graph::compact::generation::emit::payload_lease::V5PayloadLease;
use crate::graph::compact::generation::emit::sink::{SegmentSink, SpoolSegmentSink};
use crate::graph::compact::generation::ledger::{AnonReservation, JobAnonLedger};
use crate::graph::compact::generation::{
    CancelToken, EdgeRecordSource, GenerationBudget, GenerationError, GenerationMetrics,
    NodeRecordSource, RelSchemaDecl, RunSetLease, RunStore,
};
use crate::graph::compact::generation_builder::column_pass::{
    ColumnGeometry, compute_column_geometries,
};
use crate::graph::compact::generation_builder::csr_pass::{self, RelTableGeometry};
use crate::graph::compact::generation_builder::dict_pass::{
    build_node_col_keys, build_rel_col_keys, collect_string_occurrences, consume_remap_run,
    make_resident_desc,
};
use crate::graph::compact::generation_builder::edge_pass::{self, RelTableKey};
use crate::graph::compact::generation_builder::emit_columns::{
    emit_column_bodies, write_directory_segments,
};
use crate::graph::compact::generation_builder::emit_ids::{
    build_block_zone_maps, build_edge_id_lookup, build_edge_original_ids, build_metadata,
    build_node_id_lookup, build_node_original_ids, build_table_zone_maps,
    count_edges_per_rel_table,
};
use crate::graph::compact::generation_builder::live_graph::LogicalLabelLookup;
use crate::graph::compact::generation_builder::node_pass::{self, NodeSchema};
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
    /// R1.6: frozen overlay epoch for payload identity.
    pub frozen_epoch: u64,
}

/// The bounded orchestrator. Drives one full build.
pub struct BoundedGenerationBuilder {
    config: BoundedBuildConfig,
    metrics: GenerationMetrics,
    cancel: Option<CancelToken>,
    /// Shared enforcing anon ledger for the whole job (R2). Created from
    /// `budget.max_anon_bytes` at construction. Spool buffers, block zone
    /// maps, and any other orchestrator-owned anonymous allocations are
    /// charged here via RAII guards. The run store's sinks charge the same
    /// ledger (via `RunStore::job_anon_ledger`), so the whole-job peak
    /// reflects concurrently live arenas across all passes.
    job_anon: std::sync::Arc<JobAnonLedger>,
}

impl BoundedGenerationBuilder {
    /// Creates a builder.
    #[must_use]
    pub fn new(config: BoundedBuildConfig) -> Self {
        let job_anon = std::sync::Arc::new(JobAnonLedger::new(config.budget.max_anon_bytes));
        Self {
            config,
            metrics: GenerationMetrics::default(),
            cancel: None,
            job_anon,
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
        spool_guards: &mut Vec<AnonReservation>,
    ) -> Result<SpoolSegmentSink, GenerationError> {
        // R2: charge the in-memory spool buffer against the enforcing
        // whole-job ledger BEFORE allocation. The RAII guard is held in the
        // caller's LOCAL `spool_guards` Vec (R3 MAJOR-3) until the build ends
        // or unwinds — so the builder's own spool charges release on ANY exit
        // from build() via RAII, reconciling the builder's contribution to zero.
        let buf_bytes = self.config.spool_buf_cap as u64;
        let guard =
            self.job_anon
                .reserve(buf_bytes)
                .map_err(|e| GenerationError::BudgetExceeded {
                    counter: "max_anon_bytes",
                    requested: buf_bytes,
                    limit: self.config.budget.max_anon_bytes,
                })?;
        spool_guards.push(guard);
        // Mirror onto per-sink observational metrics.
        self.metrics
            .reserve_anon(buf_bytes, self.config.budget.max_anon_bytes)?;
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

    /// R2-M5: verify ALL resource categories have zero current charges at
    /// build exit. Categories: anonymous (job ledger), temp, schema, mapped.
    /// Run/chunk files are charged as temp; spool/copy buffers, merge I/O,
    /// and generated-output zone maps are charged as anon. Every category
    /// must reconcile to zero on success, typed failure, cancellation, and
    /// final lease drop — never silent leftovers.
    fn verify_all_categories_zero(&self) -> Result<(), GenerationError> {
        let snap = self.job_anon.snapshot();
        if snap.current != 0 {
            return Err(GenerationError::BudgetExceeded {
                counter: "max_anon_bytes",
                requested: snap.current,
                limit: 0,
            });
        }
        if self.metrics.temp_bytes_current != 0 {
            return Err(GenerationError::BudgetExceeded {
                counter: "temp_bytes",
                requested: self.metrics.temp_bytes_current,
                limit: 0,
            });
        }
        if self.metrics.schema_bytes_current != 0 {
            return Err(GenerationError::BudgetExceeded {
                counter: "max_schema_bytes",
                requested: self.metrics.schema_bytes_current,
                limit: 0,
            });
        }
        if self.metrics.mapped_bytes_current != 0 {
            return Err(GenerationError::BudgetExceeded {
                counter: "mapped_bytes",
                requested: self.metrics.mapped_bytes_current,
                limit: 0,
            });
        }
        Ok(())
    }

    /// R2: verify the enforcing ledger has zero current charges at build exit.
    /// All RAII guards (spool buffers, block zone maps) must have been dropped.
    /// The run store's sinks release via their own guards on flush/cleanup.
    fn verify_zero_charges(&self) -> Result<(), GenerationError> {
        let snap = self.job_anon.snapshot();
        if snap.current != 0 {
            return Err(GenerationError::BudgetExceeded {
                counter: "max_anon_bytes",
                requested: snap.current,
                limit: 0, // zero expected
            });
        }
        Ok(())
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

        // R2: unify the enforcing ledger. If the run store provides one
        // (DiskRunStore does), adopt it so orchestrator spool charges and
        // sink arena charges share ONE whole-job counter. In-memory stores
        // return None; the builder keeps its own ledger.
        if let Some(store_ledger) = run_store.job_anon_ledger() {
            self.job_anon = std::sync::Arc::clone(store_ledger);
        }

        // R3 (MAJOR-3): spool buffer guards are a LOCAL Vec, not a builder
        // field. On ANY exit from build() — success, `?` early-return, or
        // unwind — this Vec drops and releases every spool charge via RAII, so
        // the builder's own contribution to the enforcing ledger reconciles to
        // zero. We do NOT assert zero on the failure path: the caller-owned
        // `run_store` may still hold sink arena charges until it is dropped.
        // verify_zero_charges() runs only on the success path below.
        let mut spool_guards: Vec<AnonReservation> = Vec::new();

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
        let mut offsets_sink = Box::new(self.make_sink(
            SegmentKind::StringOffsets,
            8,
            8,
            "stroff",
            &mut spool_guards,
        )?);
        let mut bytes_sink = Box::new(self.make_sink(
            SegmentKind::StringBytes,
            1,
            1,
            "strbytes",
            &mut spool_guards,
        )?);
        let mut code_index_sink = Box::new(self.make_sink(
            SegmentKind::DictionaryCodeIndex,
            8,
            16,
            "codeidx",
            &mut spool_guards,
        )?);
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

        let mut bodies_sink = Box::new(self.make_sink(
            SegmentKind::ColumnBodies,
            1,
            0,
            "colbodies",
            &mut spool_guards,
        )?);
        let mut presence_sink = Box::new(self.make_sink(
            SegmentKind::ColumnRowPresence,
            1,
            0,
            "colpres",
            &mut spool_guards,
        )?);
        let mut null_sink = Box::new(self.make_sink(
            SegmentKind::ColumnRowNull,
            1,
            0,
            "colnull",
            &mut spool_guards,
        )?);
        let mut chunk_catalog = DictChunkCatalog::open(&catalog_path, &self.config.temp_dir)
            .map_err(|e| {
                GenerationError::Io(format!("open dict catalog {}: {e}", catalog_path.display()))
            })?;
        let mut col_result = emit_column_bodies(
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
            &self.job_anon,
        )?;

        // R3 (MAJOR-2): block_zone_maps are now charged INSIDE
        // emit_column_bodies (reserve-before-store), with the RAII guards held
        // on `col_result.zone_map_guards`. Those guards are dropped after
        // emit_all consumes the zone maps (see below), just before
        // verify_zero_charges. No after-the-fact charging happens here.

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
        let mut fwd_off_sink = Box::new(self.make_sink(
            SegmentKind::ForwardCsrOffsets,
            4,
            4,
            "fwdoff",
            &mut spool_guards,
        )?);
        let mut fwd_tgt_sink = Box::new(self.make_sink(
            SegmentKind::ForwardCsrTargets,
            4,
            4,
            "fwdtgt",
            &mut spool_guards,
        )?);
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
        let mut rev_off_sink = Box::new(self.make_sink(
            SegmentKind::ReverseCsrOffsets,
            4,
            4,
            "revoff",
            &mut spool_guards,
        )?);
        let mut rev_tgt_sink = Box::new(self.make_sink(
            SegmentKind::ReverseCsrTargets,
            4,
            4,
            "revtgt",
            &mut spool_guards,
        )?);
        let mut pos_sink = Box::new(self.make_sink(
            SegmentKind::ForwardPositions,
            4,
            4,
            "fwdpos",
            &mut spool_guards,
        )?);
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
            &mut spool_guards,
        )?;
        run_store.cleanup_job_artifacts()?;

        // R3 (MAJOR-2): release the block_zone_maps charges now that emit_all
        // has consumed the zone maps via build_block_zone_maps. Dropping the
        // guards reconciles the zone-map charge to zero before the ledger is
        // verified below.
        col_result.zone_map_guards.clear();

        // R3 (MAJOR-3): release all spool buffer RAII guards (local Vec) and
        // verify the enforcing ledger has zero current charges. Every sink
        // arena was released on flush/cleanup; every spool buffer guard is
        // dropped here. (On a failure path above, `spool_guards` would instead
        // drop via RAII at build() exit — no zero-assertion there.)
        spool_guards.clear();

        // R2-M5: release ALL resource categories on the success path so the
        // multi-category snapshot verifies zero. Run/chunk temp was charged
        // for id_index_lease + occ_lease handles; schema was charged for
        // node_schema + rel_keys + schema_strings + geometries; mapped was
        // charged for the ID index mmap. All are consumed/dropped by now.
        // The ID index file temp charge equals its mapped length (same file).
        let id_index_bytes = id_index.byte_len() as u64;
        self.metrics.release_temp(id_run_temp);
        self.metrics.release_temp(occ_temp);
        self.metrics.release_temp(id_index_bytes);
        self.metrics.release_schema(schema_charge);
        self.metrics.release_schema(rel_schema_charge);
        self.metrics.release_schema(schema_strings_charge);
        self.metrics.release_schema(geo_charge);
        self.metrics.release_mapped(id_index_bytes);

        // R2-M5: verify ALL categories (anon, temp, schema, mapped) are zero.
        self.verify_all_categories_zero()?;

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
        spool_guards: &mut Vec<AnonReservation>,
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
                            && geometries
                                .iter()
                                .any(|g| g.table_id == tid as u16 && g.key == c.key)
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
                            && geometries
                                .iter()
                                .any(|g| g.table_id == (0x8000 | rid as u16) && g.key == c.key)
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
            Box::new(self.make_sink(SegmentKind::NodeIdLookup, 8, 24, "nodeidlk", spool_guards)?);
        build_node_id_lookup(id_index, node_lookup_sink.as_mut())?;
        let mut node_orig_sink = Box::new(self.make_sink(
            SegmentKind::NodeOriginalIds,
            8,
            8,
            "nodeorig",
            spool_guards,
        )?);
        build_node_original_ids(
            id_index,
            run_store,
            &self.config.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            node_orig_sink.as_mut(),
        )?;
        let mut edge_lookup_sink =
            Box::new(self.make_sink(SegmentKind::EdgeIdLookup, 8, 24, "edgeidlk", spool_guards)?);
        build_edge_id_lookup(
            fwd_lease,
            run_store.merger("fwd-csr")?.as_mut(),
            &self.config.budget,
            &mut self.metrics,
            self.cancel.as_ref(),
            run_store,
            edge_lookup_sink.as_mut(),
        )?;
        let mut edge_orig_sink = Box::new(self.make_sink(
            SegmentKind::EdgeOriginalIds,
            8,
            8,
            "edgeorig",
            spool_guards,
        )?);
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
        //
        // R2-M4: the output `table_zm`/`block_zm` Vecs are graph-proportional
        // (block zone maps scale with row count). While they're built, the
        // source `col_result.columns[].block_zone_maps` are still live and
        // already charged via `zone_map_guards`. Charge the OUTPUT vectors
        // BEFORE building so the dual-live overlap is admitted by the ledger.
        let table_zm_predicted = (geometries.len() as u64)
            .saturating_mul(crate::graph::compact::mapped::ZONE_MAP_RECORD_LEN as u64);
        let block_zm_predicted: u64 = col_result
            .columns
            .iter()
            .map(|c| {
                (c.block_zone_maps.len() as u64)
                    .saturating_mul(crate::graph::compact::mapped::ZONE_MAP_RECORD_LEN as u64)
            })
            .sum();
        let zm_output_total = table_zm_predicted.saturating_add(block_zm_predicted);
        let zm_output_guard = if zm_output_total > 0 {
            Some(self.job_anon.reserve(zm_output_total).map_err(|_| {
                GenerationError::BudgetExceeded {
                    counter: "max_anon_bytes",
                    requested: zm_output_total,
                    limit: self.config.budget.max_anon_bytes,
                }
            })?)
        } else {
            None
        };
        let table_zm = {
            let mut catalog = DictChunkCatalog::open(catalog_path, temp_dir)?;
            build_table_zone_maps(geometries, schema_strings, &mut catalog)?
        };
        let block_zm = {
            let mut catalog = DictChunkCatalog::open(catalog_path, temp_dir)?;
            build_block_zone_maps(
                &col_result.columns,
                geometries,
                schema_strings,
                &mut catalog,
            )?
        };
        // R2-M4: reconcile actual vs predicted (output may be smaller if some
        // columns have no zone maps). Release the guard; the Vecs are about to
        // be consumed by make_resident_desc and dropped.
        drop(zm_output_guard);
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
            let mut membership_sink = Box::new(self.make_sink(
                SegmentKind::NodeLabelMembership,
                8,
                16,
                "memb",
                spool_guards,
            )?);
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
        )
        .with_frozen_epoch(self.config.frozen_epoch))
    }
}
