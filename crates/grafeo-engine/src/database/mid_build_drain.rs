//! G-MIDFLUSH.1 M2 — window-tier drain (O(window)).
//!
//! `GrafeoDB::drain_overlay_to_tier` is the builder-scoped tier drain.
//! Each drain captures the overlay-only window via
//! `from_graph_store_preserving_ids(overlay)` → writes a tier file via
//! `create_versioned_sections_streaming(CompactStoreSectionSource)` → mmap
//! reopens → pushes to `mid_build_tiers` → watermark swaps the overlay.
//! It NEVER calls `build_and_publish_generation`, `publish_generation`,
//! `live_graph_sources_bounded`, or `BaseGeneration::open`.
//!
//! Feature-gated: `generation + lpg + compact-store + mmap + generation-streaming`
//! (mmap for the bounded reopen; generation-streaming for the tier writer).

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use grafeo_common::utils::error::{Error, Result};
use grafeo_core::graph::compact::CompactStore;
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_storage::file::GrafeoFileManager;
use serde::{Deserialize, Serialize};

use super::GrafeoDB;

/// Report for one `drain_overlay_to_tier` invocation (G-MIDFLUSH.1 M2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MidBuildDrainReport {
    /// Monotonic drain sequence for this database (1-based).
    pub drain_seq: u64,
    /// Rows drained (overlay node ids at freeze instant).
    pub rows_drained: u64,
    /// Overlay anon heap KB before the drain.
    pub anon_kb_before: u64,
    /// Overlay anon heap KB after the drain (fresh empty overlay).
    pub anon_kb_after: u64,
    /// Wall ms for the whole drain (freeze → window build → tier write → mmap → swap).
    pub wall_ms: u64,
    /// Nodes in the tier file just written.
    pub tier_node_count: u64,
    /// Edges in the tier file (zero during node-phase drains per guardrail).
    pub tier_edge_count: u64,
    /// SHA-256 of the tier file (for diagnostics / determinism).
    #[serde(default)]
    pub tier_sha256: Option<[u8; 32]>,
    /// Kept for compat with M1 callers that read these fields (always None in M2).
    #[serde(default)]
    pub generation_sha256: Option<[u8; 32]>,
    /// Compat: base counts (equal to tier counts in M2 window report).
    #[serde(default)]
    pub base_node_count: u64,
    #[serde(default)]
    pub base_edge_count: u64,
}

/// Open a generation-backed tier file's CompactStore section as a zero-copy
/// `Arc<CompactStore>` via `mmap_section` + `deserialize_from_mapped_bytes`.
///
/// Reused from M1 (`open_compact_store_from_generation_file`).
pub(crate) fn open_compact_store_from_generation_file(path: &Path) -> Result<Arc<CompactStore>> {
    let manager = GrafeoFileManager::open_read_only(path)?;
    let directory = manager
        .read_section_directory()?
        .ok_or_else(|| Error::Internal("generation has no section directory".into()))?;
    let entry = directory
        .find(grafeo_common::storage::SectionType::CompactStore)
        .ok_or_else(|| Error::Internal("generation has no CompactStore section".into()))?;
    let section = manager.mmap_section(entry)?;
    let section_bytes: Bytes = Arc::new(section).into_bytes();
    let mut cs_section = CompactStoreSection::empty();
    cs_section.deserialize_from_mapped_bytes(section_bytes)?;
    let store = cs_section.store().ok_or_else(|| {
        Error::Internal("empty CompactStoreSection after mid-build drain open".into())
    })?;
    Ok(store)
}

fn next_drain_id(correlation_id: &str, seq: u64) -> String {
    let safe: String = correlation_id
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect();
    format!("midflush-{safe}-{seq:020}")
}

#[allow(dead_code)]
fn map_generation_budget_error(
    e: grafeo_core::graph::compact::generation::GenerationError,
) -> Error {
    Error::Internal(format!("mid-build drain generation build: {e}"))
}

impl GrafeoDB {
    /// Drain the current overlay window into a new tier file (G-MIDFLUSH.1 M2).
    ///
    /// O(window) anonymous + O(window) file I/O per drain. Never calls
    /// `build_and_publish_generation` / `publish_generation` /
    /// `live_graph_sources_bounded` / `BaseGeneration::open`.
    ///
    /// Fail-closed: any error leaves the overlay and tier list intact.
    #[cfg(all(
        feature = "generation",
        feature = "lpg",
        feature = "compact-store",
        feature = "mmap",
        feature = "generation-streaming"
    ))]
    #[allow(clippy::cast_possible_truncation)]
    pub fn drain_overlay_to_tier(
        &mut self,
        tier_root: &Path,
        correlation_id: &str,
    ) -> Result<MidBuildDrainReport> {
        let wall_start = Instant::now();

        // ── Preconditions ───────────────────────────────────────────────
        let layered = self.layered_store.as_ref().ok_or_else(|| {
            Error::Internal(
                "drain_overlay_to_tier requires a layered store (call compact() or open a generation root first)".into(),
            )
        })?.clone();
        if self.read_only {
            return Err(Error::Internal(
                "drain_overlay_to_tier requires a writable database".into(),
            ));
        }
        if layered.handoff_active() {
            return Err(Error::Internal(
                "drain_overlay_to_tier: epoch handoff already active".into(),
            ));
        }

        let anon_before = layered.overlay_memory_bytes() as u64;

        // ── Freeze barrier + empty check ────────────────────────────────
        let overlay_store = layered.overlay_store();
        let overlay_node_count = overlay_store.node_count();
        let overlay_edge_count = overlay_store.edge_count();
        // Use the layered freeze epoch's dirty sets as the emptiness signal.
        // When there are zero dirty/deleted ids and zero nodes/edges in the
        // overlay, nothing to drain.
        let frozen = layered.generation_freeze_epoch();
        let is_empty = frozen.overlay_node_ids.is_empty()
            && frozen.overlay_edge_ids.is_empty()
            && frozen.deleted_base_node_ids.is_empty()
            && frozen.deleted_base_edge_ids.is_empty()
            && overlay_node_count == 0
            && overlay_edge_count == 0;
        if is_empty {
            let seq = self.next_drain_seq();
            let anon_after = layered.overlay_memory_bytes() as u64;
            return Ok(MidBuildDrainReport {
                drain_seq: seq,
                rows_drained: 0,
                anon_kb_before: anon_before / 1024,
                anon_kb_after: anon_after / 1024,
                wall_ms: wall_start.elapsed().as_millis() as u64,
                tier_node_count: 0,
                tier_edge_count: 0,
                tier_sha256: None,
                generation_sha256: None,
                base_node_count: 0,
                base_edge_count: 0,
            });
        }
        // Also early-return when the freeze says empty but there are nodes
        // (should not happen, but handle: treat as empty).
        if frozen.overlay_node_ids.is_empty() && overlay_node_count == 0 {
            let seq = self.next_drain_seq();
            let anon_after = layered.overlay_memory_bytes() as u64;
            return Ok(MidBuildDrainReport {
                drain_seq: seq,
                rows_drained: 0,
                anon_kb_before: anon_before / 1024,
                anon_kb_after: anon_after / 1024,
                wall_ms: wall_start.elapsed().as_millis() as u64,
                tier_node_count: 0,
                tier_edge_count: 0,
                tier_sha256: None,
                generation_sha256: None,
                base_node_count: 0,
                base_edge_count: 0,
            });
        }

        let rows_drained = overlay_node_count as u64;

        // ── Watermark capture BEFORE swap ───────────────────────────────
        let next_node = overlay_store.next_node_id();
        let next_edge = overlay_store.next_edge_id();
        drop(overlay_store);

        // ── Window capture (O(window) anon): overlay-only ───────────────
        // Build CompactStore from ONLY the overlay rows.
        let overlay_arc = layered.overlay_store();
        let window_store = {
            // Use the overlay LpgStore directly as the GraphStore source.
            // This is O(window) by construction.
            let src: &dyn grafeo_core::graph::traits::GraphStore = overlay_arc.as_ref();
            grafeo_core::graph::compact::from_graph_store_preserving_ids(src)
                .map_err(|e| Error::Internal(format!("window CompactStore build: {e}")))?
        };
        let tier_node_count = window_store.total_nodes();
        let tier_edge_count = window_store.total_edges();

        // ── Tier write (O(window) file, O(1) anon) ──────────────────────
        std::fs::create_dir_all(tier_root).map_err(|e| {
            Error::Internal(format!(
                "mid-build drain: create tier root {}: {e}",
                tier_root.display()
            ))
        })?;

        let seq = self.next_drain_seq();
        let tier_id = next_drain_id(correlation_id, seq);
        let tier_path = tier_root.join(format!("tier-{seq:020}-{tier_id}.grafeo"));

        // If tier file exists from a prior invocation (e.g. leftover /data/tmp), remove it.
        if tier_path.exists() {
            let _ = std::fs::remove_file(&tier_path);
        }

        // Build a minimal GenerationContainerHeader + CompactStoreSectionSource
        // and stream it via create_versioned_sections_streaming.
        // The tier file reuses the generation container framing so the existing
        // mmap + deserialize helper can reopen it.
        {
            use grafeo_core::graph::compact::generation::collect_and_assign_global_codes;
            use grafeo_core::graph::traits::GraphStore;
            use grafeo_storage::file::generation_writer::{
                CompactStoreSectionSource, GenerationContainerHeader, OsGenerationFileOps,
                create_versioned_sections_streaming,
            };

            let header = GenerationContainerHeader {
                epoch: 0,
                transaction_id: 0,
                node_count: tier_node_count,
                edge_count: tier_edge_count,
            };
            // Build global string dictionary from window's labels + property keys + string values.
            let mut string_occ: Vec<String> = Vec::new();
            for label in window_store.all_labels() {
                string_occ.push(label);
            }
            for et in window_store.all_edge_types() {
                string_occ.push(et);
            }
            for key in window_store.all_property_keys() {
                string_occ.push(key);
            }
            // Collect string property values by scanning nodes + edges
            for nid in window_store.node_ids() {
                if let Some(node) = window_store.get_node(nid) {
                    for (_k, v) in &node.properties {
                        if let grafeo_common::types::Value::String(s) = v {
                            string_occ.push(s.to_string());
                        }
                    }
                }
            }
            for nid in window_store.node_ids() {
                for (_dst, eid) in window_store.edges_from(nid, grafeo_core::graph::Direction::Outgoing) {
                    if let Some(edge) = window_store.get_edge(eid) {
                        for (_k, v) in &edge.properties {
                            if let grafeo_common::types::Value::String(s) = v {
                                string_occ.push(s.to_string());
                            }
                        }
                    }
                }
            }
            let global_strings = collect_and_assign_global_codes(string_occ)
                .map_err(|e| Error::Internal(format!("global strings: {e}")))?;
            let section = CompactStoreSectionSource::new(window_store, global_strings)
                .map_err(|e| Error::Internal(format!("CompactStoreSectionSource: {e}")))?;
            let mut sections: Vec<Box<dyn grafeo_storage::file::generation_writer::ExactSectionSource>> =
                vec![Box::new(section)];
            create_versioned_sections_streaming(&tier_path, &header, &mut sections, &OsGenerationFileOps)
                .map_err(|e| Error::Internal(format!("tier write failed: {e}")))?;
        }

        // Compute tier SHA-256 (diagnostics) — streaming, bounded.
        let tier_sha256 = {
            use grafeo_storage::file::generation_writer::{GenerationFileOps, OsGenerationFileOps};
            OsGenerationFileOps.sha256(&tier_path).ok()
        };

        // ── mmap reopen + push onto tier list ───────────────────────────
        let reopened = open_compact_store_from_generation_file(&tier_path).map_err(|e| {
            Error::Internal(format!(
                "mid-build drain reopen failed for {}: {e}",
                tier_path.display()
            ))
        })?;
        self.mid_build_tiers.write().push(Arc::clone(&reopened));

        // ── Watermark swap ──────────────────────────────────────────────
        layered.reset_overlay_with_watermark(next_node, next_edge);
        // Keep GrafeoDB::store in sync with the fresh overlay (GrafeoDB::lpg_store()
        // returns `self.store`, not `layered.overlay_store()`; after compact they alias
        // but after a swap they diverge unless we update here).
        #[cfg(feature = "lpg")]
        {
            self.store = Some(layered.overlay_store());
        }

        let anon_after = layered.overlay_memory_bytes() as u64;
        let wall_ms = wall_start.elapsed().as_millis() as u64;

        Ok(MidBuildDrainReport {
            drain_seq: seq,
            rows_drained,
            anon_kb_before: anon_before / 1024,
            anon_kb_after: anon_after / 1024,
            wall_ms,
            tier_node_count,
            tier_edge_count,
            tier_sha256,
            generation_sha256: tier_sha256,
            base_node_count: tier_node_count,
            base_edge_count: tier_edge_count,
        })
    }

    #[cfg(all(
        feature = "generation",
        feature = "lpg",
        feature = "compact-store",
        feature = "mmap",
        feature = "generation-streaming"
    ))]
    fn next_drain_seq(&self) -> u64 {
        use std::sync::atomic::Ordering;
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        SEQ.fetch_add(1, Ordering::SeqCst)
    }

    /// Live overlay anon heap bytes for the LayeredStore attached to this DB.
    #[must_use]
    pub fn mid_flush_overlay_bytes(&self) -> Option<usize> {
        self.layered_store
            .as_ref()
            .map(|l| l.overlay_memory_bytes())
    }
}
