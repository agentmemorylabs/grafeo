---
title: Layered overlay as a diff for base entities (D10)
description: Design for replacing LayeredStore copy-up with per-entity diffs merged at read time.
tags:
  - architecture
  - storage
  - decisions
---

# Layered overlay as a diff for base entities (D10)

**Status:** design + slice 1 (node property diffs). **Grounded in:** fork trunk `83710123` plus fork PR #38 (overlay-only deletes).
**Decision source:** AMH `docs/planning/disk-backed-memory-graph/DESIGN.md` D10, §4.3, §11.2 (gap G6).

## 1. Problem

A writable generation root is a [`LayeredStore`](../../../crates/grafeo-core/src/graph/compact/layered.rs):
an immutable, memory-mapped `CompactStore` **base** plus a mutable `LpgStore` **overlay**.

Today, the first write to a base entity **copies the whole entity into the overlay** (`ensure_in_overlay`,
`ensure_edge_in_overlay`): every label and every property, at the same id, created at epoch 0. The id is
then marked *dirty*, and reads of a dirty id are served from the overlay copy alone.

What triggers a copy-up (`layered.rs`, `GraphStoreMut for LayeredStore`):

| Write | Copies up |
|---|---|
| `set_node_property[_versioned]`, `remove_node_property[_versioned]` | the node |
| `add_label[_versioned]`, `remove_label[_versioned]` | the node |
| `set_edge_property[_versioned]`, `remove_edge_property[_versioned]` | the edge **and both endpoints** |
| `create_edge[_versioned]`, `batch_create_edges`, `create_edges_batch_versioned`, `replay_create_edge_with_id` | **both endpoints** of the new edge |

For an AMH memory entity (`MemoryEntity`, with a 2048–4096-dim `embedding`), this means the following:

- appending one observation (`SET m.observations_json = …`) copies the 8–16 KB embedding into the overlay heap;
- so does creating one relation between two existing entities, for **both** endpoints;
- so does a provenance update (`REMOVE m.embedding_provider, m.embedding_model`).

The overlay budget (DESIGN §4.1) is consumed by vectors nobody changed: about 850 touched entities per 8 MiB.

Copy-up is also the root of a bug class: the dirty copy *replaces* the base view, so anything the copy misses or
gets wrong becomes visible.
- **D4:** a promoted node hid its base adjacency.
- **#13 / #15:** journal ordering, and persistence of deletes of promoted rows.
- **#38 / AMH #161:** rows that never got a dirty mark could not be deleted.
- **Incomplete-rollback leftovers (AMH #163 context):** a copy-up the journal "forgot" stays as a stale full copy.

## 2. Model

Neo4j's TxState model (DESIGN §11.2), adapted to an immutable base:

- **New entities** live in the overlay **whole**, as today.
- **Base entities** that are written get an overlay **diff row** at the same id. A diff row holds:
  - the entity's **labels** (copied, as they are small; this keeps label reads and label writes unchanged);
  - **only the properties written since the base**: set values, plus a **removal tombstone** for removed base properties;
  - for edges: id, `src`, `dst` and type (all fixed), plus the changed properties.
- **Added edges** are overlay edges, as today, whatever their endpoints are.
- **Removed base entities and edges** are base tombstones (`deleted_from_base_*`), as today. Removed overlay rows and
  diff rows use overlay MVCC deletion, as today (#38).
- The base is never written. Nothing here reads Kùzu/DuckDB's "update in place" as permission to touch the mapped base.

**The effective row of a dirty base id is `merge(base row, diff row)`:**
- labels: from the diff row;
- each property: the diff row's value if it has the key (a tombstone means *absent*), else the base value;
- MVCC visibility (created/deleted epoch, deleting transaction): from the diff row.

A full copy-up is a diff that overrides every property, so **old overlays stay valid** under the new read rule. No
format migration is needed, and #13, #15 and #38's paths keep working unchanged (§8).

### 2.1 Removal tombstone

A removed base property is stored in the diff row as `Value::Null`.
- **Same meaning as the query layer:** `SET x = null` already means "remove"; Grafeo does not distinguish a null-valued
  property from an absent one.
- **No new transaction machinery:** the tombstone is an ordinary overlay property write, so it gets the overlay's
  versioning and undo for free:
  - `set_node_property_versioned` records the prior overlay state in the property undo log;
  - rollback restores "absent", which means "base value visible again";
  - with `temporal`, the tombstone is a dated entry like any other value.
- **Merged views never expose it:** every merge path filters `Null`.
- **Direct overlay readers are listed in §5.7.** Each either goes through the merge or is made to filter `Null`.

### 2.2 What stays the same

- **Dirty tracking:** dirty still means "the overlay has a row for this id" (diff row, full copy, or a new row
  created through `LayeredStore`).
- **Copy-up identity:** the diff row is created at epoch 0 by the system transaction, exactly as the copy is today,
  so older snapshots keep seeing the base entity.
- **The copy-up journal** (`LayerChange::NodeCopyUp/EdgeCopyUp`, owners, `undo_layers_locked`) is unchanged; it now
  undoes a diff row instead of a copy.
- **The post-freeze sets and `swap_base_and_repair_overlay`** are unchanged (§6).
- **Incomplete-rollback leftovers become harmless:** an empty diff row merges to exactly the base row. A forgotten
  copy-up no longer pins stale values.

## 3. Write path

`ensure_in_overlay(id)` becomes `ensure_diff_row(id)`:
- creates the overlay node at `id` with the base labels, at epoch 0, **with no properties**;
- journals it, marks it dirty, and charges labels-only retained bytes.

Then:

| Write on a base entity | Overlay effect |
|---|---|
| set property `k = v` | diff row `k = v` (versioned as today) |
| remove property `k` | if the base row has `k`: diff row `k = Null` (tombstone), returning the merged old value; else an overlay remove |
| add/remove label | on the diff row's label set (it holds all labels) |
| create edge with a base endpoint | `ensure_diff_row(endpoint)`: labels only (see §9 on dropping it) |
| set/remove edge property | edge diff row (`src`/`dst`/type plus changed properties); endpoints as above |

**Cost after the change:** a single-property update on a base entity costs the property plus one labels-only row,
instead of the whole entity.

## 4. WAL and replay

**Unchanged, and that is the point.**
- A write logs only the logical operation: `SetNodeProperty {id, key, value}`, `RemoveNodeProperty`, label records.
  This holds for the session's direct APIs (`session/mod.rs`) and for Cypher through `WalGraphStore`.
- The copy-up itself was never logged. Generation-root replay (`generation/replay.rs`) calls the `LayeredStore`
  mutators, which re-derive the overlay row through `ensure_*`. Under D10 replay re-derives the diff row in the
  same way.
- So WAL bytes per write were already about the size of the changed property; the copy cost RAM, not WAL. The
  slice-1 measurement pins both.
- **Tombstones replay too:** a replayed `RemoveNodeProperty` on a base node writes the `Null` diff value.

## 5. Read path: every merge point

### 5.1 Point reads
`get_node`, `get_node_versioned`, `get_node_at_epoch`, `get_node_property`, `get_*_batch` / `*_selective_batch`,
and the edge twins:
- for a dirty id that the base also has, return `merge(base, diff)`;
- for a dirty id the base lacks (a new row), return the overlay row as today;
- the "miss on a dirty id" re-check for a concurrent rollback purge stays.

`get_node_property(id, k)` reads the diff value first (a tombstone means `None`), then the base value. It never
builds the whole node, so reading one property of a diff row never touches the embedding.
- The base fallback applies only while the diff row is live in the overlay, so a deleted diff row does not
  resurrect base values.
- For an id that is *not* dirty but that the base has, the base alone answers (see §6, absorbed rows).

### 5.2 Scans and adjacency
`node_ids`, `nodes_by_label`, `node_count`, `edge_count`, `edges_from`, `neighbors`, the degrees: **unchanged**.
They already treat dirty ids as overlay-owned and read overlay adjacency unconditionally, and labels are complete
on the diff row.

### 5.3 Property-index probes
`find_nodes_by_property`, `find_nodes_by_properties`, `find_nodes_in_range`: "base ∪ added − removed" per probe.

- **Base postings of a dirty id stay candidates.** Today they are dropped as owned by the overlay copy; after the
  change they are verified against the merged value. A diff row that does not override `k` keeps the base value.
- **Overlay postings** cover only diff values and new rows.
- **Overlay-index path** (`has_property_index`, and the mapped index + write delta path):
  - `LpgStore::update_property_index_on_set` removes the old posting using the *overlay's* old value. A diff row has
    none, so the base value's posting goes stale.
  - Every candidate from this path is therefore verified against the merged value, as the mapped path already does
    (`mapped_property_index_candidates`).
  - Tombstones (`Null`) are never returned as matches.
- **Zone maps** (`node_property_might_match`) are an OR of both layers. They stay correct (conservative).

### 5.4 Text index
- Text indexes live on the overlay and are maintained by `LpgStore::set_node_property`, keyed by node id and gated
  on the overlay's labels. The diff row carries the labels, so a write to an indexed text property on a base node
  updates its document as before.
- Unchanged properties were (re)indexed by the copy. They need no work now: their documents were indexed when the
  index was built over the layered view.
- A tombstone (`Null`) removes the document, as a removal did.

### 5.5 Vector search
- **Vector reads that go through the layered view are unaffected:** the accessor and `read_indexed_node_vector`
  (`database/vector_access.rs`, `vector_read.rs`), DB-level `vector_search`, and the session's vector intents.
  An unchanged embedding is now read from the base, which is exactly what a clean base node did.
- **`LayeredStore::vector_search` (the planner/`GraphStoreSearch` path)** forwards to the overlay
  `LpgStore::vector_search`, whose accessor and brute-force fallback see overlay rows only.
  - Clean base nodes were already invisible on this path (pre-existing). Copied nodes were visible only because
    of the copy.
  - D10 makes a diff row behave like a clean base node there.
  - The fix is the explicit merge of DESIGN §5 (per-tier search plus an exact flat scan of the overlay's changed
    vectors): **slice 4**.
- **ForceDisk spill (production).**
  - The spill consumers (`section_consumer.rs` `VectorIndexConsumer::spill`, `vector_spill_build.rs`) drain the
    overlay's vector column into mmap files.
  - A merged read of a drained key on a diff row would fall back to the **stale base vector**.
  - Slice 1 therefore never spills a diff row's vector: only changed embeddings of base nodes stay on the heap,
    bounded by the overlay budget, while new nodes' vectors still spill.
  - Pinned by `force_disk_reopen_keeps_a_diff_row_vector`.

### 5.6 Visibility and history
`is_*_visible_*` and `filter_visible_*` are unchanged, since visibility comes from the diff row.
`get_*_history` returns the diff row's history merged with the base row as the epoch-0 state.

### 5.7 Readers that bypass `LayeredStore`
Engine code that reads the overlay `LpgStore` itself sees only the diff. Slice 1 handles each one:

| Reader | Slice 1 |
|---|---|
| Handoff freeze capture (`epoch_handoff/handoff.rs` `capture_frozen_overlay_payloads`) | captures `LayeredStore::materialize_overlay_node(row)` (§6) |
| Live-graph build source (`generation_builder/live_graph.rs` `OverlayNodeCursor`) | merges with the base row it skips |
| Mid-build tier drain (`database/mid_build_drain.rs`) | when the overlay holds diff rows, builds the window from a scratch store of materialized rows (still O(window)) |
| `GrafeoDB::{add,remove}_node_label`, `remove_node_property`, `get_node_labels` (`crud.rs`) | routed through `LayeredStore` / the merged `get_node`. This also fixes clean base nodes, where these were no-ops before. |
| Vector spill consumers | skip diff rows (§5.5) |
| `GrafeoDB::get_node_at_epoch`, history, temporal property history; `Session::get_*_history` (`active_lpg_store`) | `temporal`-only. They read overlay rows, so a diff row's history lacks unchanged properties. Not in AMH's feature set. **Slice 4** gives them a merged view (the base row is the epoch-0 state). |
| `export_snapshot`, `iter_nodes`, `save` (`persistence.rs`) | already miss base rows entirely on a layered DB (pre-existing). Unchanged; listed so nobody relies on them. |
| `LpgStore::find_nodes_by_property` on a mapped index, `property_index_snapshot_entries` | keep diff rows only if the overlay value matches. `LayeredStore` never relies on them (it verifies mapped candidates itself), and generation writers rebuild postings from the layered graph. Direct overlay callers should not use them on a layered store. |

## 6. Compaction (freeze, build, install)

- **The builder treats an overlay row as a whole-row replacement.**
  - The base cursor skips every overlay id (`generation_builder/freeze.rs` `node_shadowed`).
  - Duplicate ids are rejected.
  - So every consumer that feeds overlay rows to a build must feed **materialized** rows: `merge(base row, diff
    row)`. Slice 1 does this at all three sources (handoff capture, live-graph cursor, mid-build drain).
  - The cost is reading each touched base row once per compaction, which the build reads anyway.
- **Absorbed rows.**
  - `swap_base_and_repair_overlay` leaves rows the new base absorbed in the overlay, undirtied (as today).
  - Two rules keep them harmless:
    1. Reads of an id the base has go to the base alone. Slice 1 makes `get_node_property` follow this too; an
       absorbed diff row may still hold a tombstone for a key the new base dropped.
    2. Materialization merges whenever the base has the id, dirty or not. Merging an absorbed diff into the base
       that absorbed it is a no-op.
  - Dropping absorbed rows at install would free their memory. It belongs with the online-compaction work (P3),
    not here.
- **Post-freeze diffs:** a diff row written after the freeze stays dirty through the repair swap and merges over the
  new base. That is correct because a diff is cumulative and idempotent (set, tombstone, label edits), unlike a full
  copy taken from the old base.
- **Retire → install window:** unchanged. The pre-existing gap (writes after retire are not tracked, fork #38) affects
  diff rows exactly as it did copies. It is a P3 prerequisite.

## 7. Rollback

- **Property writes on a diff row** are overlay property writes, undone by the overlay's property undo log as today.
  This includes tombstones (§2.1).
- **The diff row itself** is a copy-up journal entry: rollback of its only owner purges it, as today. Because it
  carries no base data, a purge that races a reader's "miss on dirty id" re-check falls back to the identical base row.
- **Base tombstones, overlay deletes, label writes:** unchanged (fork #14, #38).
- **Order** stays overlay undo first, then layer undo (`abort_transaction`).

## 8. Coexistence and migration

- **#13 (journal through overlay delete), #15 (deleted promoted ids in the deletion log), #38 (overlay-only deletes):**
  unchanged code paths. A diff row is an overlay row at a base id, exactly as a copy was.
  - `snapshot_deleted_promoted_*` still lists dirty ids whose overlay row is deleted and whose base row exists.
- **Old overlays with full copies** (compact-file snapshots, a WAL replayed by an older binary): read correctly,
  because a full copy is a diff that overrides everything.
- **The reverse is not true:** a binary *without* D10 that opens an overlay containing diff rows would serve them as
  full copies and lose the unchanged base properties.
  - Generation roots never persist the overlay (the WAL is replayed through `ensure_*`), so a downgrade is safe for
    them.
  - Compact files that persist an overlay section are not safe to downgrade once written by a D10 binary.
  -   - Mitigation, if a downgrade is ever needed: run an epoch handoff (or `compact()` / save on the D10 binary)
    first. It materializes every diff into the base.

## 9. Slices

Each slice is one fork PR with tests, against `fix/root-mount-covers-nested`.

1. **Node property diffs.**
   - `ensure_diff_row` for nodes; merged node point reads; tombstones; property-index verification.
   - Compaction materializes merged rows (§6); the direct readers of §5.7 are handled.
   - **Exit:** a single-property update on a base entity with a 2048-dim vector retains roughly the property's size
     in overlay bytes and `RssAnon`, not the vector's (measured), and no existing test regresses.
2. **Edge property diffs:** the same for `ensure_edge_in_overlay`.
3. **Endpoint rows on edge create:** measure whether labels-only endpoint rows can be dropped entirely (no overlay row
   for an endpoint whose only change is a new edge).
4. **Vector search merge:** the explicit overlay flat scan of §5.5, shared with the per-tier vector work (DESIGN §5).

## 10. Test plan

**Slice 1 (this PR)**
- **New `tests/generation_root_overlay_diff.rs`** (8 tests):
  - SET keeps unchanged base properties, across reopen and a handoff;
  - REMOVE is a tombstone, across reopen and a handoff;
  - rollback restores the base view;
  - property lookups with and without an index, including multi-key with one key changed, and range;
  - a relation create keeps both base endpoints whole (no embedding copy);
  - the DB-level label and removal APIs;
  - a ForceDisk reopen keeps a diff row's new vector;
  - the cost measurement.
- **Existing suites must stay green unchanged**, except tests that inspect overlay contents directly to assert a
  copy was made (listed in the PR with the reason for each rewrite):
  - `layered_session_direct_api`, `layered_rollback_base_mutations`, `layered_dirty_node_adjacency`,
    `layered_db_counts`, `compact_store_epoch_handoff`, `generation_root_wal_replay`, the `layered.rs` unit tests;
  - #38's `generation_root_overlay_delete` (deletes of diff rows);
  - #37's ForceDisk handoff test once rebased.
- **Measurement:** the cost test reports overlay bytes, WAL bytes and `RssAnon` per update, on trunk vs this branch.

**Adoption (AMH), per slice**
- The PR #89 probe tests (`crates/am-graph/tests/grafeo_layered_session_probe.rs`) at the new pin.
- Lane 2's differential workload (AMH #162): the 1k/10k cross-backend replay must stay EQUIVALENT. Its workload
  appends observations to base entities, so it exercises diffs.
- The SIGKILL replay tests (`generation_root_wal_replay.rs`): killing mid-write after a base-entity SET must reopen to
  the merged view.

**Later slices** add edge diffs (2), endpoint rows (3) and the vector merge (4), each with its own targets, and
re-run the above.
