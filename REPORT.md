# Lane 1: crash-safe `.grafeo` checkpoints

Branch `fix/crash-atomic-checkpoint`, cut from `e0ff9bc8` (the rev agent-memory-hosted pins).

## Summary

- **Root cause found and reproduced.** A checkpoint overwrote section data and the section directory in place, at fixed offsets. Only the 4 KiB DB header was double-buffered. If the process died after the data overwrite and before the header write, the old header pointed at new bytes. A child-process test that aborts mid-checkpoint reproduces the exact production error, `section Catalog CRC mismatch`.
- **Second bug found and reproduced.** `wal_checkpoint()` marked the WAL as checkpointed (writing `checkpoint.meta`, which tells recovery to skip older log files) *before* it wrote the `.grafeo` file. If the WAL had rotated and the process died in that gap, committed data was lost on reopen, even with the corruption fixed.
- **Fix.** Every checkpoint now builds a complete new image in `<db>.checkpoint-tmp`, fsyncs it, `rename`s it over the database file, then fsyncs the directory. `wal_checkpoint()` now writes the file first and marks the WAL afterwards. The WAL position it records is captured *before* the snapshot.
- **After the fix**, all 42 crash points across 4 scenarios reopen to a consistent state with no data loss. The existing persistence, WAL, recovery and crash suites pass (counts below).
- **Rebase onto `main` (`4ebae02f`): not clean.** `main` is 181 commits behind the pinned rev. Replaying just my 2 commits onto `main` conflicts in `crates/grafeo-storage/src/file/manager.rs` only, because `main` doesn't have the fork's G-F0.1 `write_versioned_sections` and direct-mmap changes. The fix ports easily, but it is a manual port, not a clean rebase.

## 1. How a checkpoint wrote before this change

All single-file persistence goes through `GrafeoFileManager` (`crates/grafeo-storage/src/file/manager.rs`).

File layout (`file/format.rs`):

| Offset | Size | Contents |
|---|---|---|
| 0 | 4 KiB | FileHeader (written once at create) |
| 4 KiB | 4 KiB | DbHeader slot 0 |
| 8 KiB | 4 KiB | DbHeader slot 1 |
| `DIRECTORY_OFFSET` | 4 KiB | v2 section directory |
| `SECTION_DATA_OFFSET`+ | variable | sections, each page-aligned |

**Who calls it.** These all go through `flush::flush` (`grafeo-engine/src/database/flush.rs`), which serializes every section and calls `write_versioned_sections`:

- `GrafeoDB::close()`, then `remove_sidecar_wal()`
- `GrafeoDB::wal_checkpoint()` (also via `async_wal_checkpoint`)
- the periodic `CheckpointTimer`
- `async_write_snapshot`

`save_as_grafeo_file` (`persistence.rs`) creates a new file and calls `write_snapshot` (the v1 blob).

**`write_versioned_sections` before the fix.** This ran on the one open handle of the live file:

1. For each section: `seek(offset)` + `write_all`, starting at `SECTION_DATA_OFFSET`. **This overwrites the live sections in place.** The offsets are the same every time, so the old directory now points at new bytes.
2. `set_len(end)`, which truncates the live file.
3. `write_all` of the new directory at `DIRECTORY_OFFSET`. **This overwrites the live directory in place.**
4. `write_db_header` into the inactive slot.
5. `sync_all()`. **This was the only fsync**, and it came after every in-place write.
6. Update the in-memory active header and slot.

`write_snapshot` (v1) had the same shape: data written in place at `DATA_OFFSET`, then truncate, header, one fsync.

The two header slots protect only the 4 KiB header. A crash after step 1 leaves the old, valid header and old directory checksum describing bytes that have been replaced, which gives `section Catalog CRC mismatch` (Catalog is the first section loaded). A crash after step 3 gives `v2 section directory checksum mismatch`. Power loss before step 5 can also persist any subset of the page writes.

**WAL ordering.**

- `close()` syncs the WAL, flushes the file, then removes the sidecar WAL directory. That order was already correct.
- `wal_checkpoint()` called `wal.checkpoint()` **first**. That writes a Checkpoint record, then `checkpoint.meta` (temp + rename) with `log_sequence = current`, and runs `truncate_old_logs`. Only after that did it flush the `.grafeo` file. Recovery (`wal/recovery.rs`) skips every log file whose sequence is below `checkpoint.meta.log_sequence`.

**Other writers checked and left alone.**

- The E-M0 generation layout (`generation/publication.rs`, `file/generation_writer.rs`) already uses `create_new`, fsync, rename and directory fsync.
- `compact()` works in memory only.
- `backup.rs` writes backups through temp + rename.

## 2. Reproduction (failing first)

New test: `crates/grafeo-engine/tests/crash_atomic_checkpoint.rs`.

The existing crash tests (`crash_injection_single_file.rs`) catch the injected panic in-process. While the panic unwinds, `GrafeoDB::drop` runs `close()` again, and that completes the interrupted checkpoint, which hides torn writes. Those tests also only crash the *first* checkpoint, when there is no previous image to destroy.

The new test runs the checkpoint in a **child process**. The child's panic hook calls `std::process::abort()`, so the crash point added by the existing `maybe_crash` hooks (`testing-crash-injection` feature) kills the process immediately: no unwinding, no destructors, only the bytes already in the page cache. From the file's point of view this is the same as SIGKILL or an OOM kill.

Each scenario:

1. Establishes a good checkpoint (clean close).
2. Reopens and writes more data.
3. Dies at crash point *n*, for every *n* until the child completes.

The parent then reopens the file and checks the data.

Scenarios:

- `close_wal_on`: crash during the close checkpoint, WAL on.
- `wal_checkpoint`: crash during `wal_checkpoint()`, database left open.
- `close_wal_off`: crash during the close checkpoint, WAL off. The file is the only copy, so old or new is acceptable.
- `wal_checkpoint_rotated`: about 70 MiB is written first so the WAL rotates past its 64 MiB limit.

Command, on base `e0ff9bc8` plus the test only (commit `6611d03`):

```
cargo test -p grafeo-engine --features testing-crash-injection --test crash_atomic_checkpoint -- --test-threads=1
```

Result: **4 failed, 1 passed** (the passing one is the child entry point, a no-op in the parent).

```
close_wal_on: crash point 4 (write_sections:after_data): reopen failed: GRAFEO-X001: Internal error: section Catalog CRC mismatch: expected 0x485A2383, got 0xDDE56901
close_wal_on: crash point 5 (write_sections:after_directory): reopen failed: GRAFEO-X001: Internal error: v2 section directory checksum mismatch: header recorded 0x07E49276, computed 0x632E532D
close_wal_off: crash point 4 (write_sections:after_data): reopen failed: GRAFEO-X001: Internal error: section Catalog CRC mismatch: expected 0x485A2383, got 0xDDE56901
close_wal_off: crash point 5 (write_sections:after_directory): reopen failed: GRAFEO-X001: Internal error: v2 section directory checksum mismatch: header recorded 0x07E49276, computed 0x632E532D
wal_checkpoint_rotated: crash point 3 (flush:before_serialize): wrong data ["Alix", "Gus"]
wal_checkpoint_rotated: crash point 4 (flush:after_serialize): wrong data ["Alix", "Gus"]
wal_checkpoint_rotated: crash point 5 (write_sections:before_data): wrong data ["Alix", "Gus"]
wal_checkpoint_rotated: crash point 6 (write_sections:after_data): reopen failed: GRAFEO-X001: Internal error: section Catalog CRC mismatch: expected 0x485A2383, got 0x4BECC18F
wal_checkpoint_rotated: crash point 7 (write_sections:after_directory): reopen failed: GRAFEO-X001: Internal error: v2 section directory checksum mismatch: header recorded 0x07E49276, computed 0x400DF1BB
wal_checkpoint: crash point 6 (write_sections:after_data): reopen failed: GRAFEO-X001: Internal error: section Catalog CRC mismatch: expected 0x485A2383, got 0xDDE56901
wal_checkpoint: crash point 7 (write_sections:after_directory): reopen failed: GRAFEO-X001: Internal error: v2 section directory checksum mismatch: header recorded 0x07E49276, computed 0x632E532D
```

`wrong data ["Alix", "Gus"]` means the round-2 rows (`Jules`, `Vincent`) were committed and in the WAL, but were lost on reopen.

**The WAL-ordering bug is independent of the corruption bug.** With the atomic-publish fix in place but `admin.rs` reverted to the old order, `wal_checkpoint_rotated` still fails at 6 crash points, `flush:before_serialize` through `checkpoint:before_rename`, each with `wrong data ["Alix", "Gus"]`.

## 3. Fix

### `crates/grafeo-storage/src/file/manager.rs`

New `publish_image()`, used by both `write_versioned_sections` and `write_snapshot`. The live file is never written again. Steps:

1. Remove any stale `<target>.checkpoint-tmp`, then `create_new` it in the same directory.
2. `try_lock_exclusive` the staging file, so the published path is never unlocked, and copy the old file's permissions onto it.
3. Write the FileHeader, the payload (sections + directory, or the v1 blob), the new DbHeader in the next slot, and an EMPTY header in the other slot.
4. **fsync #1**: staging file `sync_all()`.
5. `rename(tmp, target)`, which atomically replaces the database file.
6. Swap the manager's handle to the new file. The old handle and its lock are dropped after the rename on Unix, and before it on Windows (see caveats). Update the in-memory active header and slot.
7. **fsync #2**: fsync the parent directory (Unix).

Any error before the rename deletes the staging file and leaves the old image untouched. An error from the directory fsync is returned to the caller, so `close()` keeps the sidecar WAL. The manager has already switched to the new image, which is what is on disk.

Supporting changes:

- `target` is the canonicalized path, so a symlinked database is replaced at its real location instead of the link being overwritten.
- `open()` (writable) deletes a stale staging file after it acquires the lock. `open_read_only()` leaves it alone.
- New crash points: `checkpoint:before_rename` and `checkpoint:after_rename`. `write_snapshot:after_header_write` was removed because the header is now written to the staging file.
- The header format is unchanged. Readers still pick the higher iteration, so files written before this change open, and files written after it open on older builds.

### `crates/grafeo-engine/src/database/admin.rs`: `wal_checkpoint()`

For the single-file format:

1. `wal.sync()`, then capture `wal.current_sequence()`.
2. `checkpoint_to_file()`.
3. Only then `wal.checkpoint_covering(tx, epoch, captured_seq)`.

The non-file (WAL-directory) path is unchanged.

### `crates/grafeo-storage/src/wal/{log.rs,typed.rs}`

- New `TypedWal::checkpoint_covering()`. `complete_checkpoint` takes an optional cap, and the recorded `log_sequence` is `min(current, cap)`, so records written into a log that rotated out during the snapshot are still replayed.
- `truncate_old_logs` now also refuses to delete files at or above the recorded sequence. Plain `checkpoint()` behaves exactly as before.

### Tests

- `crates/grafeo-engine/tests/crash_atomic_checkpoint.rs`: the process-death tests described above.
- `crates/grafeo-storage/src/file/manager.rs`, `tests::atomic_publish`:
  - no staging file is left behind
  - a stale staging file is removed on open and the old image is kept
  - a read-only open leaves the staging file alone
  - slots alternate across repeated checkpoints and reopen
  - the exclusive lock is held on the new image
  - file mode is preserved
  - a symlink target is replaced and the link is kept
  - an in-process crash at all 6 storage-level points of a second checkpoint reopens to old or new (`testing-crash-injection`)
- `crates/grafeo-engine/tests/crash_injection_single_file.rs`: `crash_before_sidecar_wal_removal_recovered_on_reopen` used crash point 9, but `close:before_remove_sidecar_wal` was point 8, so that test never crashed where it claimed to. It now uses point 10, the correct number after the two new points, and lists the sequence in a comment.

## 4. What is proven (commands run in this session, Linux x86_64, rustc/cargo 1.99.0)

| Command | Result |
|---|---|
| `cargo test -p grafeo-engine --features testing-crash-injection --test crash_atomic_checkpoint -- --test-threads=1 --nocapture` (on the fix) | **5 passed**, 0 failed. 42 crash points exercised: close_wal_on 10, close_wal_off 10, wal_checkpoint 11, wal_checkpoint_rotated 11. All reopen with no error and the expected rows. |
| Same, with only `admin.rs` reverted (`rotation` filter) | 1 failed, as expected: proves the WAL reorder is needed (§2) |
| `cargo test -p grafeo-engine --features testing-crash-injection,compact-store,vector-index,text-index,generation,generation-streaming --test backup_restore --test compact_store_large_string_persistence --test compact_store_manifest_recovery --test crash_injection_single_file --test crash_atomic_checkpoint --test generation_root_wal_replay --test schema_wal_replay --test generation_sections ...` | backup_restore 11, compact_store_large_string_persistence 2, compact_store_manifest_recovery 11, crash_atomic_checkpoint 5, crash_injection_single_file 5 (+3 ignored), generation_root_wal_replay 4, generation_sections 6, schema_wal_replay 4: **all passed** |
| `cargo test --no-fail-fast -p grafeo-engine --features testing-crash-injection,compact-store,vector-index,text-index,generation,generation-streaming,cypher --test session_wal_durability --test vector_quant_reopen --test wal_directory --test wal_recovery` | session_wal_durability 10, vector_quant_reopen 7, wal_directory 10, wal_recovery 24: **all passed** |
| `cargo test -p grafeo-engine --features testing-crash-injection --test crash_injection_single_file -- --include-ignored` | **8 passed** (includes the 3 normally ignored crash tests) |
| `cargo test -p grafeo-storage --features grafeo-file,testing-crash-injection,encryption,generation --lib` | **271 passed**, 0 failed. Covers the WAL, recovery, file manager (incl. 9 new), and generation fault tests. The `FAILED` lines in the raw log come from intentionally aborted child processes in `generation::fresh_process_faults`; the parent tests pass. |
| `cargo test -p grafeo-engine --features testing-crash-injection,async-storage --lib` | **1109 passed**, 0 failed |
| `cargo clippy -p grafeo-storage --features grafeo-file,testing-crash-injection,encryption --all-targets -- -D warnings` | clean |
| `cargo clippy -p grafeo-engine --features testing-crash-injection --lib --test crash_atomic_checkpoint --test crash_injection_single_file` | no warnings in changed code |

One run note: `session_wal_durability` failed once in my first invocation, because I had left out the `cypher` feature (`Unknown query language: 'cypher'`). It is a WAL-directory test and does not touch this code. It passed with `cypher` enabled.

## 5. What is NOT proven or was skipped

- **Power loss / kernel crash.** The tests kill the process, so the page cache survives. That is the OOM/SIGKILL case from the incident. Durability against power loss depends on the fsync ordering (staging fsync → rename → directory fsync), which I checked by review only. No dm-flakey or LazyFS style testing was done.
- **Windows and macOS were not run.** On Windows the old handle is closed *before* the rename because Windows refuses to replace an open file. That leaves a short window where the old file is unlocked: another process could open it and then see it replaced. The Windows rename-failure path reopens and relocks the old file. None of this was executed here, and CI on the fork runs a Windows and macOS matrix.
- **Not run:**
  - the whole workspace
  - bindings (python, node, ...)
  - `grafeo-spec-tests`
  - the root `tests/` directory
  - all engine integration tests outside the list above
  - `--all-features` clippy. It fails before reaching this code on toolchain 1.99 because of pre-existing `grafeo-core` lints (`missing_docs`, `chunks_exact`, ...), and those errors exist without my change.
- The `encryption` feature compiles and its existing manager tests pass. Encrypted checkpoints were not crash-tested.
- No performance measurement. A checkpoint already rewrote every section, so the I/O volume is about the same. It now needs **free disk space for a second full copy** while the checkpoint runs, and it adds one directory fsync.

## 6. Platform caveats

- **Directory fsync.** On Linux and most Unix systems the rename is only durable after the parent directory is fsynced, which this change does. On macOS, `fsync` does not flush the drive cache (`F_FULLFSYNC` would). That was already true for every fsync in Grafeo, and this change does not address it.
- **`rename` atomicity** holds within one filesystem. The staging file is always in the same directory as the database. On network filesystems (NFS, SMB) the guarantees are weaker.
- **Open mmaps and old readers.** Mappings and read-only handles opened before a checkpoint keep the *old inode*, so they see a consistent old image instead of torn bytes. The old file's disk space is freed only when the last of them closes. Before this change those mappings saw in-place overwrites. On Windows, an existing mapping of the database file will make the rename fail. The docs already say mappings must be dropped before a checkpoint on Windows, because in-place writes failed there too.
- **Ownership** of the file (uid/gid) is not preserved; mode bits are. If the server ever runs checkpoints as a different user than the file's owner, the file's owner will change.
- **Leftover staging file.** A crash before the rename leaves `<db>.grafeo.checkpoint-tmp` (up to one image in size). The next writable open removes it. Tooling that copies the data directory should ignore `*.checkpoint-tmp`.

## 7. Open questions for the owner

1. **Port to `main`?** The fix doesn't rebase cleanly onto `4ebae02f` (conflicts in `manager.rs` only). Should I prepare a port for `main` or upstream, or is fork-only fine?
2. **WAL-directory `wal_checkpoint()`.** In WAL-directory mode (no `.grafeo` file), `wal_checkpoint()` still writes `checkpoint.meta`. The comment in `close()` says doing that in directory mode would make recovery skip older WAL files and lose data. This is pre-existing behavior and I left it unchanged. agent-memory-hosted uses the single-file format, so it is probably unaffected, but please confirm.
3. **Transactions spanning a rotated-out log.** WAL recovery skips older log files by sequence. A transaction whose early records are in a skipped file but whose commit record is in a replayed file would replay only partially. This is pre-existing and unchanged. Capturing the sequence before the snapshot narrows the window but doesn't close it.
4. **`checkpoint.meta` durability.** `write_checkpoint_metadata` does temp + rename without a directory fsync. After power loss the old meta can come back. That is safe (more WAL replays), so I left it.
5. **Replay over the newer image.** After a crash between rename and sidecar removal, the next open replays the whole sidecar WAL over the *new* image. The tests show the result is correct, which matches the existing `crash_before_sidecar_wal_removal` test. I didn't audit that every WAL record type replays idempotently.
6. **Weak in-process crash tests.** The existing in-process crash tests are weak, because the `Drop` → `close()` during unwind finishes the checkpoint. Should they move to the child-process harness? Not done here, to keep the diff small.
