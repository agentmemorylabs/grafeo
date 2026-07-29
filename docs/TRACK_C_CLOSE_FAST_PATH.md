# Track C — Clean-close fast path

**Branch:** `perf/close-fast-path` @ worktree `/data/worktrees/grafeo-close-fast-path`  
**Base pin:** `9781320f` (`agent/txn-session-batch-20260726`)  
**Head:** see `git rev-parse HEAD` on this branch  
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

- `clean_close_does_not_advance_checkpoint_iteration`
- `dirty_close_persists_and_advances_iteration`
- `wal_recovery_forces_checkpoint_on_close`
- Existing `wal_disabled_single_file_persists_on_close` still passes
- `staging_clean_close_bench` (ignored) — set `GRAFEO_CLOSE_BENCH_PATH` under `/data/tmp`

## Staging measure (under `/data/tmp` only)

Disposable copies of staging `am-personal.grafeo` (163205 nodes / 1403962 edges). Never `/data/grafeo`.

| Build | open_ms | clean_close_ms | iteration |
|-------|--------:|---------------:|-----------|
| BEFORE `9781320f` | 9548 | **26318** | 448→449 (full rewrite) |
| AFTER `perf/close-fast-path` | 17841 | **131** | 448 unchanged |

~200× clean-close wall on this DB. Full code-index sidecar (~137s Phase1) not re-measured: MemAvailable was 4–7 GiB (needs ~11 GiB open RSS).

Artifacts: `/data/tmp/grafeo-close-fast-path-TRACK_C_SUMMARY.md`,  
`/data/tmp/grafeo-close-fast-path-measure-*/{before,after}-clean-close.log`

## Operational risk

- **Low for AM idle eviction** (WAL on, read-only session): intended win.
- **Must not** skip flush after crash recovery (gated by `recovered_wal_needs_flush`).
- **Must not** skip when WAL disabled (always Explicit).
- Index DDL without WAL records marks `container_flush_required`.
- Residual: any future mutation path that neither WALs nor marks the flag would be a durability hole — review new APIs against this gate.
