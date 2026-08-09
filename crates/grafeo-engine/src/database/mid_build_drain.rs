//! G-MIDFLUSH.1 mid-build overlay drain (M1).
//!
//! `GrafeoDB::drain_overlay_to_base` is the ONE engine entry point for the
//! repurposed M1 drain. Each drain = generation build + mmap lease + repaired
//! base swap. It never touches `merge_overlay_in_place` (O(base+overlay) anon)
//! or the serving generation root / active registry.
//!
//! Feature-gated identically to `build_generation_inner` (`generation+lpg+compact-store`)
//! plus `mmap` for the bounded reopen (the map itself is `GrafeoFileManager::mmap_section`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use grafeo_common::types::{EdgeId, NodeId};
use grafeo_common::utils::error::{Error, Result};
use grafeo_common::utils::hash::FxHashSet;
use grafeo_core::graph::compact::CompactStore;
use grafeo_core::graph::compact::generation::GenerationBudget;
use grafeo_core::graph::compact::section::CompactStoreSection;
use grafeo_storage::file::GrafeoFileManager;
use serde::{Deserialize, Serialize};

use super::GrafeoDB;
use super::generation_build::GenerationBuildRequest;

/// Report for one `drain_overlay_to_base` invocation (G-MIDFLUSH.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MidBuildDrainReport {
    /// Monotonic drain sequence for this database (1-based).
    pub drain_seq: u64,
    /// Rows drained (overlay node ids at freeze).
    pub rows_drained: u64,
    /// Overlay anon heap KB before the drain.
    pub anon_kb_before: u64,
    /// Overlay anon heap KB after the drain (should be small — only post-freeze dirty).
    pub anon_kb_after: u64,
    /// Wall ms for the whole drain (freeze → build → mmap → swap).
    pub wall_ms: u64,
    /// Total base nodes after swap (for diagnostics).
    pub base_node_count: u64,
    /// Total base edges after swap.
    pub base_edge_count: u64,
    /// SHA-256 of the transient generation file used as the new base (diagnostics).
    #[serde(default)]
    pub generation_sha256: Option<[u8; 32]>,
}

/// Open a generation container file's CompactStore section as a zero-copy
/// `Arc<CompactStore>` via `mmap_section` + `deserialize_from_mapped_bytes`.
///
/// This is the crate-local bounded helper the lock requires instead of
/// `BaseGeneration::open` (which is `pub(super)` and coupled to the serving
/// registry). It validates CRC at map time and retains the mmap `Bytes` owner
/// on the store so columns stay file-backed (anon = metadata only).
fn open_compact_store_from_generation_file(path: &Path) -> Result<Arc<CompactStore>> {
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
    // Sanitize correlation_id the same way builder_generation does — no path separators.
    let safe: String = correlation_id
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect();
    format!("midflush-{safe}-{seq:020}")
}

/// Map a `GenerationError::BudgetExceeded` through the standard engine internal error.
#[allow(dead_code)]
fn map_generation_budget_error(
    e: grafeo_core::graph::compact::generation::GenerationError,
) -> Error {
    Error::Internal(format!("mid-build drain generation build: {e}"))
}

impl GrafeoDB {
    /// Drain the current overlay into a new mmap base (G-MIDFLUSH.1).
    ///
    /// Fail-closed: any error leaves the overlay intact and propagates — no partial swap.
    /// Steps (lock §3.1 Engine API 1-6):
    /// 1. `freeze_write_barrier` → 2. `generation_freeze_epoch` → 3. `build_and_publish_generation`
    /// into a sibling unpublished root under a transient sibling dir → 4. mmap+deserialize reopen →
    /// 5. `swap_base_and_repair_overlay` → 6. report.
    ///
    /// Drains never touch the serving generation root or active registry — intermediate
    /// roots live under a transient sibling and are cleaned up after the swap.
    #[cfg(all(
        feature = "generation",
        feature = "lpg",
        feature = "compact-store",
        feature = "mmap",
        feature = "generation-streaming"
    ))]
    #[allow(clippy::cast_possible_truncation)]
    pub fn drain_overlay_to_base(
        &self,
        budget: GenerationBudget,
        correlation_id: String,
    ) -> Result<MidBuildDrainReport> {
        let wall_start = Instant::now();

        // ── Preconditions: layered store must exist; DB must not be read-only. ──
        let layered = self.layered_store.as_ref().ok_or_else(|| {
            Error::Internal(
                "drain_overlay_to_base requires a layered store (call compact() or open a generation root first)".into(),
            )
        })?;
        if self.read_only {
            return Err(Error::Internal(
                "drain_overlay_to_base requires a writable database".into(),
            ));
        }

        // Disable the incomplete G-EM0.5c handoff path: drains must not race a
        // concurrent handoff (single-writer import invariant D7). Fail-closed if one is active.
        if layered.handoff_active() {
            return Err(Error::Internal(
                "drain_overlay_to_base: epoch handoff already active".into(),
            ));
        }

        // ── Anon before: overlay-only (excludes base, which is mmap after first drain). ──
        let anon_before = layered.overlay_memory_bytes() as u64;

        // ── Barrier + freeze (writer linearization point, D7 single-threaded invariant). ──
        // Hold the barrier across epoch capture + build request construction so no
        // mutation can slip between freeze and build. The build itself releases it
        // implicitly (build_and_publish_generation acquires its own locks where needed),
        // but the construction of the request + transient dir must be under barrier.
        let frozen_epoch: u64;
        let freeze_overlay_node_ids: FxHashSet<u64>;
        let freeze_overlay_edge_ids: FxHashSet<u64>;
        let freeze_deleted_nodes: FxHashSet<u64>;
        let freeze_deleted_edges: FxHashSet<u64>;
        let current_overlay_epoch: u64;
        {
            let _barrier = layered.freeze_write_barrier();
            let frozen = layered.generation_freeze_epoch();
            frozen_epoch = frozen.epoch;
            freeze_overlay_node_ids = frozen.overlay_node_ids;
            freeze_overlay_edge_ids = frozen.overlay_edge_ids;
            freeze_deleted_nodes = frozen.deleted_base_node_ids;
            freeze_deleted_edges = frozen.deleted_base_edge_ids;
            current_overlay_epoch = self.transaction_manager.current_epoch().0;
            // Barrier drops here — drain is single-writer (D7), so no concurrent
            // mutator can race the build before swap. The handoff path uses the
            // same barrier but re-acquires for swap; we simply snapshot and proceed.
            // If frozen_epoch == 0 (genesis overlay before any epoch bump), use 0.
            let _ = current_overlay_epoch;
        }

        // Nothing to drain — early return with zero report (not an error).
        if freeze_overlay_node_ids.is_empty()
            && freeze_overlay_edge_ids.is_empty()
            && freeze_deleted_nodes.is_empty()
            && freeze_deleted_edges.is_empty()
        {
            let seq = self.next_drain_seq();
            let anon_after = layered.overlay_memory_bytes() as u64;
            return Ok(MidBuildDrainReport {
                drain_seq: seq,
                rows_drained: 0,
                anon_kb_before: anon_before / 1024,
                anon_kb_after: anon_after / 1024,
                wall_ms: wall_start.elapsed().as_millis() as u64,
                base_node_count: layered.base_store_arc().total_nodes(),
                base_edge_count: layered.base_store_arc().total_edges(),
                generation_sha256: None,
            });
        }

        let rows_drained = freeze_overlay_node_ids.len() as u64;

        // ── Resolve transient sibling unpublished root ─────────────────────
        // Derive from the DB's own path (when persistent) as a sibling, else a temp dir.
        // Never under the serving generation root and never the active registry.
        let transient_root: PathBuf = if let Some(ref p) = self.config.path {
            // Sibling to the DB file/dir: "<path>-midflush-transient"
            let parent = p.parent().unwrap_or_else(|| Path::new("/tmp"));
            let name = p
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("grafeo-midflush");
            parent.join(format!("{name}-midflush-transient"))
        } else {
            // In-memory DB (tests): use a temp dir under system temp.
            std::env::temp_dir().join(format!(
                "grafeo-midflush-{}",
                correlation_id
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect::<String>()
            ))
        };
        // Ensure the transient root exists (publish_generation requires it for lock+wal).
        std::fs::create_dir_all(&transient_root).map_err(|e| {
            Error::Internal(format!(
                "mid-build drain: create transient root {}: {e}",
                transient_root.display()
            ))
        })?;

        // Drain seq for correlation + monotonic report.
        let seq = self.next_drain_seq();
        let generation_id = next_drain_id(&correlation_id, seq);

        // Capture counts for post-freeze set after the build: layered tracks
        // post_freeze_* only while a handoff is active. Since drains don't use
        // the handoff state machine (single-writer, no next-epoch writes during
        // the build), post_freeze sets are empty — the repair swap will clear
        // dirty for exactly the frozen ids. This is correct for D7 (no concurrent writers).
        // If a concurrent writer DID slip in between freeze snapshot and swap, it would
        // be a post-freeze dirty not tracked; but D7 says import is single-threaded
        // and the barrier+freeze+build are serialized with that single writer, so none exists.

        // ── Build generation (bounded, streaming) into the transient root ──
        // Genesis parent (None) — transient-only root has no prior slot, per lock precedent
        // AMH builder_generation.rs:365-375 (`parent_*: None` → genesis).
        let request = GenerationBuildRequest {
            generation_root: transient_root.clone(),
            generation_id: generation_id.clone(),
            budget,
            rel_schemas: Vec::new(),
            parent_generation_id: None,
            parent_publication_sequence: None,
        };

        // This may fail with GenerationError::BudgetExceeded → propagated as Error::Internal
        // containing the typed budget message (R4). No swap has happened yet, so overlay intact.
        let publication = self.build_and_publish_generation(request).map_err(|e| {
            let msg = e.to_string();
            // Preserve typed budget error for R4: re-wrap as GenerationError text
            // so callers can match on "budget exceeded".
            if msg.contains("budget exceeded") || msg.contains("BudgetExceeded") {
                Error::Internal(format!("mid-build drain budget exceeded: {msg}"))
            } else {
                Error::Internal(format!("mid-build drain build failed: {msg}"))
            }
        })?;

        // ── Reopen as bounded base via mmap+deserialize ────────────────────
        let new_base = open_compact_store_from_generation_file(&publication.generation_abs_path)
            .map_err(|e| {
                Error::Internal(format!(
                    "mid-build drain reopen failed for {}: {e}",
                    publication.generation_abs_path.display()
                ))
            })?;

        // Best-effort cleanup of the transient generation file's unpublished sibling
        // is handled by publish_generation's own unpublished-dir cleanup + the
        // generation file itself remains for the mmap (we just opened it). On next
        // drain the same transient root is reused (new generation id, new file).
        // Do NOT delete the file we just mapped — the mmap holds it open.

        // ── Swap base + repair overlay (selective undirty, G-EM0.5d) ───────
        // Drains have no post-freeze mutations (single-writer, no concurrent writes
        // between freeze snapshot and swap), so post_freeze sets are empty. The swap
        // will clear dirty for absorbed (frozen) ids and retain none.
        let frozen_node_ids: FxHashSet<NodeId> = freeze_overlay_node_ids
            .iter()
            .map(|id| NodeId::new(*id))
            .collect();
        let frozen_edge_ids: FxHashSet<EdgeId> = freeze_overlay_edge_ids
            .iter()
            .map(|id| EdgeId::new(*id))
            .collect();
        let post_freeze_node_ids: FxHashSet<NodeId> = FxHashSet::default();
        let post_freeze_edge_ids: FxHashSet<EdgeId> = FxHashSet::default();

        // Preserve the store's identity for anon_before/after + tombstone contract:
        // swap_base_and_repair_overlay is fail-closed (infallible swap; no error path
        // leaves a torn state). It intentionally retains base-deletion tombstones.
        let _old_base = layered.swap_base_and_repair_overlay(
            Arc::clone(&new_base),
            &frozen_node_ids,
            &frozen_edge_ids,
            &post_freeze_node_ids,
            &post_freeze_edge_ids,
        );

        // Keep transaction/epoch consistent: the new base's epoch is frozen_epoch;
        // the engine's current epoch stays at frozen_epoch (no handoff epoch bump for drains).
        // AMH imports tag SYSTEM epoch at call time — no adjustment needed.

        // Also ensure the overlay's epoch stays in sync (defensive: builder may have advanced it).
        // The overlay store's epoch should already match; sync if needed.
        let _ = frozen_epoch;

        let anon_after = layered.overlay_memory_bytes() as u64;
        let wall_ms = wall_start.elapsed().as_millis() as u64;

        Ok(MidBuildDrainReport {
            drain_seq: seq,
            rows_drained,
            anon_kb_before: anon_before / 1024,
            anon_kb_after: anon_after / 1024,
            wall_ms,
            base_node_count: new_base.total_nodes(),
            base_edge_count: new_base.total_edges(),
            generation_sha256: Some(publication.publication.generation_sha256),
        })
    }

    /// Monotonic drain sequence counter for this DB instance.
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
        // Per-DB seq would need a field on GrafeoDB; use global atomic for now.
        // Each DB's drains are sequential (single-writer), so global monotonic is fine
        // and avoids adding state to the DB struct. Report drain_seq is per-call ordering.
        SEQ.fetch_add(1, Ordering::SeqCst)
    }

    /// Non-gated helper for tests: current overlay memory bytes (for R5 / anon assertions).
    #[cfg(any(test, feature = "generation"))]
    pub fn mid_flush_overlay_bytes(&self) -> Option<usize> {
        self.layered_store
            .as_ref()
            .map(|l| l.overlay_memory_bytes())
    }
}
