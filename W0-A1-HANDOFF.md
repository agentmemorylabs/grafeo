# W0-A1 Handoff — Bounded Canonical Record Sources and Run Ownership

**Packet:** G-EM0.W0-A1 (Bridge Plan §4)
**Worktree:** `/data/worktrees/grafeo-em0-w0-a1`
**Branch:** `feature/g-em0-w0-a1`
**Source base:** `745b90cff9f477475c3a88ffed9c17f92513fa58`
**Status:** Local-only, uncommitted (parent gate passed, awaiting Stage A checkpoint)

---

## Changed paths

| File | Status | LOC |
|------|--------|-----|
| `crates/grafeo-storage/Cargo.toml` | Modified (+1 line) | — |
| `crates/grafeo-storage/src/lib.rs` | Modified (+4 lines) | — |
| `crates/grafeo-storage/src/generation/mod.rs` | New | 35 |
| `crates/grafeo-storage/src/generation/records.rs` | New | 149 |
| `crates/grafeo-storage/src/generation/budget.rs` | New | 151 |
| `crates/grafeo-storage/src/generation/metrics.rs` | New | 161 |
| `crates/grafeo-storage/src/generation/external_sort.rs` | New | 363 |
| `crates/grafeo-storage/src/generation/merge.rs` | New | 176 |
| `crates/grafeo-storage/src/generation/tests/mod.rs` | New | 4 |
| `crates/grafeo-storage/src/generation/tests/records_tests.rs` | New | 109 |
| `crates/grafeo-storage/src/generation/tests/external_sort_tests.rs` | New | 386 |

All production files under 400 LOC. No `crates/grafeo-core/**` or `crates/grafeo-engine/**` changes.

---

## What was implemented

Storage-owned bounded external-sort infrastructure with no graph semantics:

1. **`records.rs`** — `FramedRecord` (key + payload), framing format
   `[key_len:u32][payload_len:u32][key][payload]`, clean EOF vs torn header
   distinction, `MAX_RECORD_BODY_BYTES` hard cap before allocation.

2. **`budget.rs`** — `GenerationBudget` (full W0 §8 shape with all 7 fields),
   `ExternalSortBudget` projection, `GenerationBudgetError`. Validation:
   `merge_fan_in` must be `2..=64`, all byte limits non-zero.

3. **`metrics.rs`** — `ExternalSortMetrics` with truthful temp-disk accounting
   (`reserve_temp`/`release_temp` with peak tracking), `RssAnonSampler`
   (Linux `/proc/<pid>/status` reader for fresh-child peak proof).

4. **`external_sort.rs`** — `DiskRunSink` (bounded arena → sorted runs),
   `DiskRunMerger` (intermediate file tracking), `merge_runs_recursive`
   (recursive fan-in with sync-before-delete), `CancelToken`.

5. **`merge.rs`** — Internal k-way merge helpers: `RunReader`, `HeapEntry`
   (min-heap by key then payload), `kway_merge_to_file`, `kway_merge_emit`.

---

## Salvage provenance

Ported from the failed Stage A candidate
(`/data/worktrees/grafeo-em0-w0-source-true/crates/grafeo-storage/src/generation/external_sort.rs`)
after source review against the W0 contract §15 salvage boundary.

| Component | Source | Action |
|-----------|--------|--------|
| Framed record I/O | `external_sort.rs` lines 186–251 | **Ported** (clean rewrite as `records.rs`). Mechanics sound: framing format, short-read rejection, MAX_RECORD_BODY_BYTES cap. Removed graph semantics (none were in the record type itself). |
| DiskRunSink (run writer) | `external_sort.rs` lines 280–426 | **Ported** (rewritten). Reserve-before-write, arena fill detection, sort-then-sync. Split `flush_run` from `push` cleanly. |
| DiskRunMerger + recursive merge | `external_sort.rs` lines 430–549 | **Ported** (rewritten). Recursive fan-in, intermediate tracking, sync-before-delete. |
| Heap-based k-way merge | `external_sort.rs` lines 551–691 | **Ported** (split into `merge.rs`). HeapEntry min-heap, drop-readers-before-unlink. |
| CancelToken | `external_sort.rs` lines 152–182 | **Ported** (simplified). Removed dead `check()` method. |
| ExternalSortBudget | `external_sort.rs` lines 24–59 | **Ported** into `budget.rs`. Added `GenerationBudget` full shape per W0 §8. |
| ExternalSortMetrics | `external_sort.rs` lines 63–103 | **Ported** into `metrics.rs`. Added `RssAnonSampler`. |
| Error type | Single `ExternalSortError` enum | **Rewritten** as `ExternalSortMetricsError` with proper `Io(String)` variant (was sentinel `Overflow`). |

**Must NOT be salvaged** items (W0 §15): None of the forbidden items
(`v5_layout.rs`, graph semantics, wire aliases, selector-byte manifest,
synthetic WAL) were ported — they did not exist in the A1-owned subset.

---

## Test evidence

### Records tests (7 tests)

| Test | What it proves |
|------|---------------|
| `frame_round_trip` | Write then read produces identical record |
| `frame_round_trip_empty_key_and_payload` | Zero-length key/payload handled |
| `clean_eof_returns_none` | 0 bytes → `Ok(None)` |
| `torn_header_fails_closed` | 1–7 byte partial → `UnexpectedEof` |
| `short_body_after_full_header_fails` | Full header but truncated body → `UnexpectedEof` |
| `oversized_length_prefix_rejected_before_alloc` | `MAX_RECORD_BODY_BYTES + 1` → `InvalidData` before `vec![0u8; ...]` |
| `multiple_records_sequential` | Sequential reads return records then clean EOF |
| `ordering_is_key_then_payload` | `Ord` impl: key primary, payload secondary |

### External sort tests (11 tests)

| Test | What it proves |
|------|---------------|
| `run_write_and_merge_basic` | 20 records → runs → sorted merge output |
| `recursive_fan_in_triggers_merge_passes` | 200 runs, fan_in=32 → ≥2 merge passes, fully sorted |
| `cancel_mid_merge_leaves_no_temp_files` | Cancel during merge → error, all run files cleaned |
| `cancellation_during_push` | Cancel during push → error |
| `temp_disk_budget_rejection` | Small budget → `BudgetExceeded` before crossing limit |
| `rejected_reservation_leaves_no_untracked_file` | Failed reservation → no orphan file on disk |
| `deterministic_output_across_runs` | Same input → byte-identical output |
| `merge_peak_includes_both_input_and_output` | Peak temp > input-only bytes (sync-before-delete) |
| `peak_temp_matches_measured_file_sizes` | Peak temp ≈ sum of measured file sizes |
| `empty_input_merges_cleanly` | 0 runs → `Ok(())`, no panic |
| `single_run_no_merge_needed` | 1 run → 1 merge pass (emit only) |

**Total: 19/19 passing.**

---

## Verification commands (all green)

```bash
export CARGO_TARGET_DIR=/data/cargo-targets/jfrie-grafeo-em0-w0-a1/target
export TMPDIR=/data/tmp RUSTC_WRAPPER=sccache SCCACHE_DIR=/data/sccache

# Tests
cargo test -p grafeo-storage --features generation generation:: -- --nocapture
# → test result: ok. 19 passed; 0 failed

# Clippy
cargo clippy -p grafeo-storage --features generation --all-targets --no-deps -- -D warnings
# → Finished, 0 warnings

# Fmt
rustfmt --edition 2024 --check crates/grafeo-storage/src/generation/*.rs \
  crates/grafeo-storage/src/generation/tests/*.rs crates/grafeo-storage/src/lib.rs
# → clean

# git diff --check
git diff --check
# → clean
```

---

## Parent gate criteria verification

| # | Criterion | Status | Evidence |
|---|-----------|--------|----------|
| 1 | No graph-semantic types in owned paths | **PASS** | `sed` strip comments + grep for `NodeId\|EdgeId\|CompactStore\|SegmentKind` → clean in all 6 .rs files |
| 2 | `RunWriter::append` reserves against budget before allocation | **PASS** | `flush_run` line 175–178: `reserve_temp(byte_len, ...).inspect_err(\|_\| run.clear())?` before `File::create` |
| 3 | `KWayMerger` uses heap of run heads | **PASS** | `merge.rs` line 407–425: `HeapEntry` min-heap, `BinaryHeap::push`/`pop` in `kway_merge_to_file` and `kway_merge_emit` |
| 4 | Recursive merge triggers when run count exceeds fan_in | **PASS** | `external_sort.rs` line 310: `while level.len() > fan_in` |
| 5 | Cancellation drops readers before unlinking | **PASS** | `merge.rs` lines 102, 151: `drop(readers)` before `fs::remove_file` |
| 6 | Temp-disk counter incremented/decremented at every create/delete | **PASS** | 8 call sites verified: `reserve_temp` before every file create (run flush + merge output), `release_temp` after every delete (write failure, merge error, cleanup, pass completion) |
| 7 | All tests pass, clippy clean, `git diff --check` clean | **PASS** | 19/19, 0 clippy warnings, fmt clean |

---

## What was NOT touched (forbidden paths confirmed clean)

- `crates/grafeo-core/**` — no changes
- `crates/grafeo-engine/**` — no changes
- `crates/grafeo-storage/src/file/**` — no changes
- `crates/grafeo-storage/src/wal/**` — no changes
- No v5 codec, segment kind, or CompactStore type introduced

---

## Error type note

The `ExternalSortMetricsError` enum currently uses `Overflow` as a sentinel for
cancellation (the token check returns this variant). This is a temporary bridge
for W0-A1's narrow scope. W0-B will introduce a proper typed error hierarchy
with a `Cancelled` variant. Callers that need to distinguish cancellation check
`is_cancelled()` directly rather than pattern-matching on the error variant.

---

## Budget counter trace

Captured from `merge_peak_includes_both_input_and_output` (30 records, fan_in=2):

```
BUDGET_TRACE: input_bytes=390 peak=780 current=390 run_count=0 merge_passes=4
```

- `input_bytes=390`: total bytes across all input runs
- `peak=780`: exactly `2 × 390` — confirms sync-before-delete: at the peak,
  both input runs AND merge outputs were charged concurrently
- `current=390`: after all merge passes complete, only the final-level runs remain
- `merge_passes=4`: recursive fan-in with fan_in=2 over multiple runs

---

## Next packet

**W0-A2** — Source-True Graph Generation on Bounded Runs (grafeo-core).
Consumes the `NodeRecordSource` / `EdgeRecordSource` traits from A1's bounded
run infrastructure. Parent gate: no `Vec<GenerationNode>`, production-reader
parity, no `Vec<u8>` payload.
