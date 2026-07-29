# Track C — Clean-close fast path

**Branch:** `perf/close-fast-path` @ worktree `/data/worktrees/grafeo-close-fast-path`  
**Base pin:** `9781320f` (`agent/txn-session-batch-20260726`)  
**Does not touch:** `diagnostics/close-forensics`, AM `/data/worktrees/am-diagnostics-grafeo-close-forensics`

## Root cause (proven in code)

`GrafeoDB::close()` previously always called `checkpoint_to_file(..., FlushReason::Explicit)`.

In `flush.rs`:

```text
if reason == FlushReason::Explicit || section.is_dirty() {
    targets.push((..., section.serialize()?));
}
```

`build_sections()` constructs **ephemeral** `LpgStoreSection` / `CatalogSection` / `VectorStoreSection` wrappers with `dirty = false` every time. So `is_dirty()` is almost always false, and **Explicit always re-serializes the full LPG** — even for a no-write reopen+close.

Phase 1 attribution (`PHASE1_CLOSE_ATTRIBUTION.md`): ~137.6s inside `GrafeoDB::close()`, ~97% CPU-bound — consistent with full LPG serialize, not HNSW free (~97ms) or BufferManager (~10.5s).

## Fast path (implemented)

Close chooses:

| Condition | Flush reason |
|-----------|--------------|
| `wal == None` (WAL disabled) | `Explicit` (always) |
| `container_flush_required` (WAL recovery applied records, or index DDL) | `Explicit` |
| `wal.record_count() > 0` (session mutations this open) | `Explicit` |
| else (clean no-write session) | `Checkpoint` → 0 sections → **skip serialize** |

Safety net retained: if Checkpoint writes 0 sections but `record_count() > 0`, retry `Explicit`.

Open-path vector/index restore may temporarily mark flush-required; open resets the flag to the recovery signal only so clean sessions stay eligible.

## Correctness tests (`grafeo_file.rs`)

- `clean_close_does_not_advance_checkpoint_iteration` — reopen, no writes, close; header iteration unchanged; data intact
- `dirty_close_persists_and_advances_iteration` — mutate+close advances iteration; data durable
- `wal_recovery_forces_checkpoint_on_close` — sibling copy with unrecovered sidecar; reopen+close folds WAL and advances iteration
- Existing `wal_disabled_single_file_persists_on_close` must still pass (WAL-off → always Explicit)

## Staging measure (under `/data/tmp` only)

Copy staging sidecar under `/data/tmp`, never `/data/grafeo`. Compare clean open→close wall before/after when MemAvailable allows (~11 GiB open RSS historically).

## Operational risk

- **Low for AM idle eviction** (WAL on, read-only session): intended win.
- **Must not** skip flush after crash recovery (gated by `recovered_wal_needs_flush`).
- **Must not** skip when WAL disabled (always Explicit).
- Index DDL without WAL records marks `container_flush_required`.
- Residual: any future mutation path that neither WALs nor marks the flag would be a durability hole — review new APIs against this gate.
