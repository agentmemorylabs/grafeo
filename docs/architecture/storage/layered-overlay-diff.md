---
title: Layered overlay as a diff for base entities (D10)
description: Design for replacing LayeredStore copy-up with per-entity diffs merged at read time.
tags:
  - architecture
  - storage
  - decisions
---

# Layered overlay as a diff for base entities (D10)

**Status:** slices 1–3 implemented together in fork PR #40 (the separate slice-2/3 PRs #42 and #43 were folded in); slice 4 open. **Grounded in:** fork trunk `9b423f72`, which includes fork PR #38 (overlay-only deletes) and #37 (ForceDisk handoff).
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

A persisted full row is **not** read as a diff: a full copy with a physically removed key would resurrect the key.
Compact files therefore persist overlay rows whole and convert them on load (§8). #13, #15 and #38's paths keep
working unchanged.

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
| create edge with a base endpoint | nothing for the endpoint (slice 3; slice 1 made a labels-only row) |
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
  - **Rollback** (AMH #190 R3-F1): the overlay undo of a `SET` on an inherited, heap-indexed property has no old
    value, so it drops the node from the posting of the value it wrote and puts nothing back, even when the value was
    unchanged. A *missing* posting is not caught by verification, so the rollback repair (§7) re-adds the node under
    its merged value. A stale extra posting can remain; verification filters it.
- **Zone maps** (`node_property_might_match`) are an OR of both layers. They stay correct (conservative).

### 5.4 Text index
- Text indexes live on the overlay and are maintained by `LpgStore::set_node_property`, keyed by node id and gated
  on the overlay's labels. The diff row carries the labels, so a write to an indexed text property on a base node
  updates its document as before.
- Unchanged properties were (re)indexed by the copy. They need no work now: their documents were indexed when the
  index was built over the layered view.
- A tombstone (`Null`) removes the document, as a removal did.
- **Rollback is the exception.** The overlay undo of a `SET` on an *inherited* text property has no old value (the
  diff row never held it), so it removes the replacement document and has nothing to reinsert. After every
  rollback and savepoint rollback, the session therefore re-syncs the text documents of the base nodes the undone
  entries touched against the merged row (the rollback repair, §7). Nothing is copied back into the overlay,
  embeddings included. Pinned by `rollback_of_an_inherited_text_property_restores_its_document` (full rollback,
  savepoint rollback, rollback onto a committed diff, exact `text_search` results) and
  `nested_savepoints_restore_the_text_document_of_each_level`.
- **Rolled-back deletes** (AMH #190 R3-F2): restoring a deleted diff row restores only the diff's own values, so an
  inherited text document stayed missing. The repair includes deleted nodes. Pinned by
  `delete_rollback_restores_inherited_secondaries`.

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
  - The consumer learns which rows are diff rows from the layered store, so it must be bound **after** the layered
    wiring. A generation-root open always did that; a compact-file open (`GrafeoDB::with_config`) registered its
    consumers first, so the spill took a diff row's new vector and the read served the base vector (review r2,
    P1). `with_config` now registers consumers after `wire_layered_after_load`.
  - Pinned by `force_disk_reopen_keeps_a_diff_row_vector` (generation root) and
    `force_disk_open_keeps_a_diff_row_vector` (compact file; an overlay-only node spills, the diff row stays),
    both with exact indexed reads and ANN.
- **Batch-create HNSW inserts.** `GrafeoDB::batch_create_nodes` and `batch_create_nodes_with_props` (`crud.rs`), like
  every write-side insert since fork #45 (AMH #175), read neighbour vectors through the merged view plus the spill
  registry (`build_vector_accessor`), so a base node's inherited embedding is readable with or without a diff row.
  Before #45 they read the overlay alone, and on a generation root a batch-created node was never found by vector
  search. Pinned by `batch_creates_over_base_diff_rows_are_searchable` (clean and touched base; every node its own
  nearest neighbour and reachable from every query).

### 5.6 Visibility and history
`is_*_visible_*` and `filter_visible_*` are unchanged, since visibility comes from the diff row.
`get_*_history` merges each entry of a dirty base node's history with the base row: the diff entry's values over the
base values, tombstones removed. This applies through `LayeredStore`, `GrafeoDB::get_{node,edge}_history` and
`Session::get_{node,edge}_history`. `GrafeoDB::get_{node,edge}_at_epoch` read the merged layered epoch reader.
These APIs are not `temporal`-gated; only the per-property history API below them is.

### 5.7 Readers that bypass `LayeredStore`
Engine code that reads the overlay `LpgStore` itself sees only the diff. Slice 1 handles each one:

| Reader | Slice 1 |
|---|---|
| Handoff freeze capture (`epoch_handoff/handoff.rs` `capture_frozen_overlay_payloads`) | captures **raw** diff rows; the frozen build sources (`epoch_handoff/records.rs`) merge each with its base row as the build consumes it (§6) |
| Live-graph build source (`generation_builder/live_graph.rs` `OverlayNode/EdgeCursor`) | merges each row with the base row it skips, one record at a time |
| Mid-build tier drain (`database/mid_build_drain.rs`) | refused unless the base is empty (§6); with an empty base no overlay row is a diff |
| Compact-file overlay section (`LpgStoreSection`) | persists overlay rows of base entities **whole** (`with_row_materializer`); the load path turns them back into diffs (§8) |
| `GrafeoDB::{add,remove}_node_label`, `remove_node_property`, `get_node_labels` (`crud.rs`) | routed through `LayeredStore` / the merged `get_node`. This also fixes clean base nodes, where these were no-ops before. |
| Vector spill consumers | skip diff rows (§5.5) |
| `GrafeoDB::get_{node,edge}_at_epoch`, `get_{node,edge}_history`; `Session::get_{node,edge}_history` | read through the layered view (§5.6) |
| `GrafeoDB::get_{node,edge}_property_at_epoch` / property history (`temporal`-only) | still read overlay entries: a diff row's per-property history lacks the base's epoch-0 values. Not in AMH's feature set; **slice 4** |
| `GrafeoDB::batch_create_nodes`, `batch_create_nodes_with_props` HNSW insert accessors | merged view plus spill since fork #45 (§5.5) |
| `LayeredStore::vector_search` (planner / `GraphStoreSearch` path) | **interim gap, slice 4**: forwards to the overlay `LpgStore` with an overlay-only accessor. A base node with a diff row is now invisible there, as a clean base node already was. DB-level `vector_search` and `read_indexed_node_vector` use the merged accessor and are unaffected (§5.5) |
| `export_snapshot`, `iter_nodes`, `save` (`persistence.rs`) | already miss base rows entirely on a layered DB (pre-existing). Unchanged; listed so nobody relies on them. |
| `LpgStore::find_nodes_by_property` on a mapped index, `property_index_snapshot_entries` | keep diff rows only if the overlay value matches. `LayeredStore` never relies on them (it verifies mapped candidates itself), and generation writers rebuild postings from the layered graph. Direct overlay callers should not use them on a layered store. |

## 6. Compaction (freeze, build, install)

- **The builder treats an overlay row as a whole-row replacement.**
  - The base cursor skips every overlay id (`generation_builder/freeze.rs` `node_shadowed`).
  - Duplicate ids are rejected.
  - So every source that feeds overlay rows to a build must yield **materialized** rows: `merge(base row, diff
    row)`.
  - **Merged lazily, never held (DESIGN R1).** The handoff freeze captures raw diff rows: a touched base node with a
    2048-dim embedding costs its diff (measured: ~20 B of payload for a small SET), not its embedding.
    - The frozen build sources (`FrozenNodeSource` / `FrozenEdgeSource`) and the live-graph cursors merge each
      record with the (immutable, still current) base row **as the build consumes it**, one record at a time, under
      the builder's own budget.
    - The inherited embedding is read once per touched node per compaction, which the build reads anyway, and
      dropped after the record is staged.
  - **Tier drains need an empty base.** The final tier-chain build (`tier_chain_sources.rs`) walks tiers + overlay
    and carries no original base rows, so a drain over a non-empty base was already incomplete, and a diff row
    cannot be a whole tier row. `drain_overlay_to_tier` refuses a non-empty base before any state changes.
    - Every in-tree caller drains a fresh build, whose base is empty.
    - AMH's bulk import ignores the drain's result (`let _ = maybe_midflush(..)`), so a refusal costs memory
      headroom, not data.
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
- **The rollback repair.** Before the undo runs, the session reads the nodes whose properties or labels the
  transaction changed, or that it deleted, from the overlay's undo log (`LpgStore::undo_log_node_ids`, from the
  savepoint's position for a savepoint rollback). After the property undo and the layer undo,
  `LayeredStore::reconcile_rolled_back_secondaries` repairs the secondary entries of the base nodes among them
  against the merged row: heap equality postings (§5.3) and text documents (§5.4). The undo of an inherited
  property, or the restore of a deleted diff row, has no inherited value to restore.
- **The diff row itself** is a copy-up journal entry: rollback of its only owner purges it, as today. Because it
  carries no base data, a purge that races a reader's "miss on dirty id" re-check falls back to the identical base row.
- **Base tombstones, overlay deletes, label writes:** unchanged (fork #14, #38).
- **Order** stays overlay undo first, then layer undo (`abort_transaction`).

## 8. Coexistence and migration

- **#13 (journal through overlay delete), #15 (deleted promoted ids in the deletion log), #38 (overlay-only deletes):**
  unchanged code paths. A diff row is an overlay row at a base id, exactly as a copy was.
  - `snapshot_deleted_promoted_*` still lists dirty ids whose overlay row is deleted and whose base row exists.
- **Compact files persist overlay rows whole; the load path turns them back into diffs.**
  - A layered compact file's overlay section is written by `LpgStoreSection::with_row_materializer`: each overlay
    row of a base entity is merged with its base row (removed keys absent). That is also exactly what pre-D10
    binaries wrote, since they held full copies and removed keys physically.
  - On load, `LayeredStore::adopt_persisted_full_rows` writes a `Null` tombstone for every base key such a row
    lacks.
  - So one on-disk meaning ("the whole row") serves both binaries, and no format marker is needed.
  - **Why not read old rows as diffs:** a full copy with a physically removed key would resurrect that key
    (review r1, must-fix 1). The fixture `tests/fixtures/legacy_layered_overlay.grafeo`, written by fork trunk
    `850e69f3`, pins this; without the conversion, `a.q` comes back.
  - **Pre-existing, not D10:** `compact()` after a property removal brings the key back as its column's type
    default (`""`, zeros). The in-memory CompactStore builder encodes absent string/vector values that way when
    other rows of the label carry the key. Same on trunk; pinned as an ignored test.
- **Downgrade:** a pre-D10 binary reading a D10 compact file sees whole rows, so it reads it correctly.
  Generation roots never persist the overlay (their WAL replays through the mutators), so they are safe in both
  directions.

## 9. Slices

Slices 1–3 ship together as fork PR #40, against `fix/root-mount-covers-nested`; slice 4 is its own PR.

1. **Node property diffs.**
   - `ensure_diff_row` for nodes; merged node point reads; tombstones; property-index verification.
   - Compaction materializes merged rows (§6); the direct readers of §5.7 are handled.
   - **Exit:** a single-property update on a base entity with a 2048-dim vector retains roughly the property's size
     in overlay bytes and `RssAnon`, not the vector's (measured), and no existing test regresses.
2. **Edge property diffs** (done): the same for `ensure_edge_in_overlay`. That covers merged `get_edge*` and
   `get_edge_property` reads, tombstones, merged capture in all three build sources, and
   `GrafeoDB::remove_edge_property`. Edges have no property indexes, so no probe changes are needed.
3. **No endpoint rows on edge create** (done).
   - **Measured first:** a labels-only endpoint row cost ~206 B, i.e. 412 of the 1070 B per relation between two base
     entities. The edge itself is ~658 B.
   - **Now:** an endpoint whose only change is a new edge gets no overlay row. The overlay keys adjacency by node id,
     `edges_from` always reads overlay adjacency, and endpoint reads merge with the base. The session's direct edge
     create always worked this way.
   - **Applies to** the live creates, batch creates, WAL replay and edge copy-up.
   - **Unchanged:** already-dirty endpoints keep their journal touch and post-freeze record.
   - **Also:** `GrafeoDB::validate` checks overlay edge endpoints through the merged view.
4. **Vector search merge:** the explicit overlay flat scan of §5.5, shared with the per-tier vector work (DESIGN §5),
   plus merged per-property `temporal` history (§5.7). After lane 3's AMH #174 fix, rebased on it.

## 10. Test plan

**Slices 1–3 (fork PR #40)**
- **`tests/generation_root_overlay_diff.rs`** (20 tests), each with exact-state oracles (whole property maps and
  labels through `get_node`, `get_node_property`, the epoch read, and DB and session history: present exactly
  when the row is dirty, in epoch order, tombstone-free, latest entry exact; edges also check identity, endpoints,
  type and both edge-history APIs):
  - SET keeps unchanged base properties, across reopen and a handoff; REMOVE is a tombstone, likewise;
  - rollback restores the base view; rollback to an already-committed non-empty diff;
  - property lookups with and without an index, including multi-key with one key changed, and range; a removed
    property never matches (`Null` equality, open range);
  - a relation create keeps both base endpoints whole; exact adjacency and degrees with no endpoint rows through
    rollback, commit, a refused plain `DELETE`, `DETACH DELETE`, reopen and a handoff;
  - the DB-level label and removal APIs, for nodes and edges;
  - edge SET/REMOVE across reopen and a handoff; edge rollback;
  - post-freeze SET/REMOVE (install refuses; reopen replays);
  - the freeze holds diffs (one record per touched entity, < 1 KiB each), and every entity of the new base is whole;
  - the tier drain refuses a non-empty base;
  - a ForceDisk reopen keeps a diff row's new vector;
  - rollback of an inherited text property restores its document (full, savepoint, onto a committed diff);
  - batch creates over base diff rows are searchable (every node its own nearest neighbour, all reachable);
  - the cost measurements (property update, relation).
- **`tests/layered_overlay_diff_compact_file.rs`** (3 tests + 1 ignored): the pre-D10 fixture, the same scenario
  written by this binary, and a compact-file ForceDisk open; the ignored one pins the pre-existing `compact()`
  limit (AMH #183, fixed separately in fork #47).
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

**Slice 4** adds the vector merge with its own targets, and re-runs the above.
