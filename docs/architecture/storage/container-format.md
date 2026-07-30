# `.grafeo` Container Format Specification

The `.grafeo` file is the single-file persistence format for Grafeo databases.
It stores data in typed **sections**, each independently addressable, checksummed,
and (for index sections) memory-mappable.

## File Layout

```
Offset    Size     Contents
────────────────────────────────────────────────────
0x0000    4 KiB    FileHeader (magic, version, page size)
0x1000    4 KiB    DbHeader H1 (iteration, checksum, metadata)
0x2000    4 KiB    DbHeader H2 (alternating crash-safe copy)
0x3000    4 KiB    Section Directory (type/offset/length/CRC entries)
0x4000+   varies   Section data (page-aligned per section)
```

Total header overhead: 16 KiB. All regions are page-aligned (4 KiB boundaries).

---

## FileHeader (0x0000, 4 KiB)

Written once at database creation. Never modified afterwards.

| Offset | Size | Type | Field | Description |
|--------|------|------|-------|-------------|
| 0 | 4 | `[u8; 4]` | `magic` | `0x47524146` ("GRAF") |
| 4 | 4 | `u32 LE` | `format_version` | `1` (current) |
| 8 | 4 | `u32 LE` | `page_size` | Always `4096` |
| 12 | 8 | `u64 LE` | `creation_timestamp_ms` | Unix epoch milliseconds |
| 20 | 32 | `[u8; 32]` | `creator_version` | UTF-8 Grafeo version, zero-padded |
| 52 | 4044 | - | (reserved) | Zero-filled |

The header is serialized with bincode and zero-padded to 4 KiB.

**Validation on open:**

- `magic` must equal `b"GRAF"` (reject otherwise)
- `format_version` must be `<= FORMAT_VERSION` (reject unknown future versions)

---

## DbHeader H1/H2 (0x1000 and 0x2000, 4 KiB each)

Two alternating header slots provide crash safety. On each checkpoint, the
**inactive** slot is overwritten with the new state, then fsynced. If the process
crashes mid-write, the other slot still contains valid metadata.

| Field | Size | Type | Description |
|-------|------|------|-------------|
| `iteration` | 8 | `u64 LE` | Monotonic counter, higher = current |
| `checksum` | 4 | `u32 LE` | CRC-32 of section directory (v2) or snapshot (v1) |
| `snapshot_length` | 8 | `u64 LE` | `0` for v2 section format, `>0` for v1 blob format |
| `epoch` | 8 | `u64 LE` | MVCC epoch at checkpoint |
| `transaction_id` | 8 | `u64 LE` | Last committed transaction ID |
| `node_count` | 8 | `u64 LE` | LPG node count |
| `edge_count` | 8 | `u64 LE` | LPG edge count |
| `timestamp_ms` | 8 | `u64 LE` | Checkpoint timestamp (Unix epoch ms) |
| (reserved) | ~3940 | - | Zero-filled to 4 KiB |

**Active header selection:** On open, read both H1 and H2. The header with
the higher `iteration` value is the active state. If both are empty
(`iteration == 0`), the database has never been checkpointed.

**v1/v2 detection:** If the active header has `snapshot_length > 0`, the file
uses the v1 blob format (a single bincode snapshot starting at `DATA_OFFSET`).
If `snapshot_length == 0` and `iteration > 0`, the file uses the v2 section
format with a section directory at `0x3000`.

---

## Section Directory (0x3000, 4 KiB)

A fixed-size page containing an array of section entries. Each entry is 32 bytes.
Maximum capacity: 127 sections (`(4096 - 8) / 32`).

### Directory Header

| Offset | Size | Type | Field |
|--------|------|------|-------|
| 0 | 4 | `u32 LE` | `entry_count` |
| 4 | 4 | `u32 LE` | `reserved` (zero) |

### Directory Entry (32 bytes each, starting at offset 8)

| Offset | Size | Type | Field | Description |
|--------|------|------|-------|-------------|
| 0 | 4 | `u32 LE` | `section_type` | Section type ID (see table below) |
| 4 | 1 | `u8` | `version` | Per-section format version (see Truthful directory versions) |
| 5 | 1 | `u8` | `flags` | Bit 0: required, Bit 1: mmap-able |
| 6 | 2 | `u16 LE` | `reserved` | Zero |
| 8 | 8 | `u64 LE` | `offset` | Byte offset from file start |
| 16 | 8 | `u64 LE` | `length` | Byte length of section data |
| 24 | 4 | `u32 LE` | `checksum` | CRC-32 of section data |
| 28 | 4 | `u32 LE` | `reserved` | Zero |

Remaining bytes after the last entry are zero-filled to 4 KiB.

### Truthful directory versions (G-F0.1)

The directory entry `version` byte is the per-section format version declared
by that section's `Section::version()` implementation.

- **New writers** (`GrafeoFileManager::write_versioned_sections`, used by
  engine flush) record each section's declared version in the directory.
- **Legacy callers** of `write_sections` still emit directory version `1` for
  every section (tests and opaque-byte writers).
- **Historical caveat:** shared writers prior to G-F0.1 often recorded outer
  directory version `1` for every section even when the payload itself was a
  later format (for example CompactStore payload v2/v3 with outer entry v1).
  Readers must continue to dispatch from the **payload** header (for example
  the CompactStore `GCST` version byte), not require outer directory version
  equals payload version. Do **not** add a strict outer-equals-payload gate
  that would reject valid historical files.
- **Unknown section types:** if the type id is not recognized and
  `flags.required` is clear, the entry is skipped so older binaries can open
  files that carry newer optional indexes. If `flags.required` is set, open
  fails closed.

---

## Section Types

Values and default flags match `SectionType` / `SectionType::default_flags`
in `grafeo-common` (not every historical doc snapshot).

| Value | Name | Required | Mmap-able | Description |
|-------|------|----------|-----------|-------------|
| 1 | `Catalog` | yes | no | Schema defs, index metadata, epoch, config |
| 2 | `LpgStore` | yes | no | Nodes, edges, properties, named graphs |
| 3 | `RdfStore` | no | no | RDF triples, named graphs |
| 4 | `CompactStore` | yes | yes | Columnar compact base (`GCST` payload) |
| 5 | `OverlayDeletions` | no | no | Layered base-deletion tombstones |
| 10 | `VectorStore` | no | yes | Embeddings + HNSW topology |
| 11 | `TextIndex` | no | yes | BM25 postings + term dictionary |
| 12 | `RdfRing` | no | yes | Wavelet trees + dictionary |
| 20 | `PropertyIndex` | no | yes | Property hash/btree indexes |

**Type ranges:**

- 1-9: Data sections (authoritative, cannot be rebuilt)
- 10-19: Index sections (derived, can be rebuilt from data)
- 20+: Reserved for acceleration structures

**Flags:**

- **Bit 0 (required):** If set, older binaries that don't recognize this
  section type must refuse to open the file. If clear, the section can be
  safely skipped (the database opens without that index).
- **Bit 1 (mmap-able):** Capability flag: the section layout is intended to
  support memory-mapped access. It does **not** mean the current container
  open path always mmaps that section (see Memory-Mapped Section Access).

**Empty sections** are omitted from the directory entirely. If no RDF data
exists, there is no `RdfStore` entry. CompactStore appears only after
`compact()` (or an equivalent layered write).

---

## Section Data (0x4000+)

Sections are written sequentially after the directory, each starting at a
page-aligned (4 KiB) offset. The next section starts at the first 4 KiB
boundary after the previous section ends.

```
0x4000  [Catalog data ................] pad
0x5000  [LpgStore data ..............] pad
0xA000  [VectorStore data ...........] pad
...
```

### Per-type encoding

Encoding is **not** uniformly bincode:

| Section | Encoding | Notes |
|---------|----------|-------|
| `Catalog`, `LpgStore`, `RdfStore` | bincode | Fully deserialized into RAM on ordinary open |
| `CompactStore` | Custom **`GCST`** payload (see below) | Column codecs + section-level strings; CRC32 trailer |
| `OverlayDeletions` | dedicated deletions codec | Base tombstones for layered reopen |
| `VectorStore`, `TextIndex`, `RdfRing`, `PropertyIndex` | bincode (v1 today) | `mmap_able` marks future/optional zero-copy layouts |

The directory entry `version` byte is the per-section format version.
**Historical caveat:** shared writers prior to the truthful directory-version
work (`G-F0.1`) often recorded outer directory version `1` for every section
even when the payload itself was CompactStore v2/v3. Readers must continue to
dispatch from the **payload** header (e.g. `GCST` version byte), not require
outer directory version equals payload version. Do not add a strict
outer-equals-payload gate that would reject valid historical files.

Payload-version dispatch remains owned by each section deserializer. Directory
version metadata records the writer-declared section version for tooling and
independent evolution; it is not a substitute payload parser or a strict
outer-version gate.

### CompactStore payload (`GCST`)

```text
Magic: "GCST" (4 bytes)
Payload version: u8
Flags: u8
… column tables, zone maps, CSR, optional ID maps …
CRC32: u32 LE over all preceding payload bytes
```

| Payload version | Section-level string length | Status |
|-----------------|----------------------------|--------|
| 1 | `u16` LE | Read-only compatibility |
| 2 | `u16` LE | Read-only compatibility |
| 3 | `u16` LE | Read-only compatibility |
| **4** | **`u32` LE** | **Current writer** |
| 5 | `u32` LE | Selected by G-EM0.R0; not emitted or read until G-EM0.1 |

“Section-level string” means labels, property keys, edge types, and zone-map
`Value::String` min/max fields (the path that previously panicked above
65,535 bytes). Column dictionary string bodies already used `u32` lengths
and are unchanged in v4.

- The E-0 writer emits payload version **4** (legacy/current on the accepted
  base).
- G-EM0.R0 selects payload version **5** for disk-native reopen. Version 5
  keeps the outer `CompactStore` section type but adds a checked in-payload
  range directory so readers can map metadata, columns, CSR arrays,
  dictionaries, zone maps, and ID lookup ranges without copying the section.
  See [CompactStore v5 mapped layout](compact-store-v5-mapped-layout.md) for
  the source inventory, exact header/directory contract, compatibility rules,
  and RED verification list.
- Current E-0 readers accept v1–v4. Version 5 is selected but not emitted or
  read until G-EM0.1 lands; existing v1–v4 readers remain supported throughout.
- Old binaries that only understand ≤v3 must **fail closed** on v4 (no silent
  misparse). Binaries that understand ≤v4 must fail closed on v5.
- Overflow of the active length width returns `Error::Serialization`
  (`GRAFEO-X002`), never a panic in a destructor.

Truthful recording of each section’s declared directory version for *all*
section types is specified and tested in the separate `G-F0.1` packet; E-0
only changes the CompactStore payload codec.

---

## Checkpoint Flow

```
Checkpoint(reason):
  1. Collect target sections based on reason:
     - Explicit:   all sections (dirty or clean)
     - Periodic:   dirty sections only (skip if none dirty)
     - Eviction:   lowest-priority dirty section only
  2. For each target section:
     a. Serialize section data to bytes (Section::serialize())
     b. Compute CRC-32
  3. Write sections to new page-aligned offsets in the file
  4. Build section directory with updated entries
  5. Write section directory at 0x3000
  6. Build new DbHeader (increment iteration, set checksum/counts)
  7. Write DbHeader to inactive slot (H1 or H2)
  8. fsync
  9. (Engine) truncate WAL
```

**Dirty tracking:** Each section has an `is_dirty()` flag. Mutations in the
store mark the corresponding section dirty. Periodic checkpoints skip sections
that haven't changed since the last flush.

**Crash safety:** If the process crashes at any point during steps 3-7, the
active header still points to the previous valid state. The dual-header
alternation ensures atomicity of the commit point (step 8).

---

## Memory-Mapped Section Access

`flags.mmap_able = true` is a **capability** bit (the layout is intended to
support mmap). It is **not** proof that the current open path maps that
section.

### Current container open (as of CompactStore E-0)

Ordinary container reopen still:

1. Reads the CompactStore section into an owned `Vec<u8>` (`read_section_data`)
2. Copies into `Bytes` at the `Section::deserialize` boundary
3. Reconstructs heap-resident `CompactStore` state (including dictionary strings)

ForceDisk / spill can later re-serialize the live store into a sidecar and mmap
that sidecar, but only **after** the eager load. Peak open memory therefore
still tracks the full compact payload until a separate direct-container-mmap
lifecycle (E-M0) lands.

Do **not** advertise direct mmap reopen of the CompactStore section inside the
`.grafeo` container until that dedicated open-path work ships. The
`mmap_section` helper and `deserialize_from_bytes(Bytes)` entry points exist
for that future path; they are not the default container open today.

### Intended mmap lifecycle (indexes / future CompactStore)

Where an implementation actually maps a section:

1. Engine flushes dirty sections via checkpoint
2. Engine calls `mmap_section()` for eligible sections
3. CRC-32 is verified against the mapped bytes (also warms page cache)
4. Engine drops any temporary in-memory copy of the section data
5. Reads go through the mmap (OS page cache manages eviction)
6. Before next checkpoint: drop all mmaps, then write

**Platform note:** On Windows, the OS rejects writes to a file with active
memory mappings (error 1224). All mmap handles must be dropped before
`write_sections()`. On Linux/macOS, writes succeed with active mappings
but the drop-before-write lifecycle is used on all platforms for consistency.

---

## Recovery

```
Open database:
  1. Read FileHeader at 0x0000, validate magic and format_version
  2. Read both DbHeaders (H1 at 0x1000, H2 at 0x2000)
  3. Select active header (highest iteration)
  4. Detect format:
     - snapshot_length > 0: v1 blob format (read snapshot at DATA_OFFSET)
     - snapshot_length == 0 && iteration > 0: v2 section format
  5. For v2: read section directory at 0x3000
  6. For each directory entry:
     a. Read section data at entry.offset
     b. Verify CRC-32
     c. Deserialize into RAM (or mmap if configured as ForceDisk)
  7. If sidecar WAL exists: replay committed transactions since last flush
  8. Database is ready
```

---

## Periodic Checkpoints

When `Config::checkpoint_interval` is set, a background thread periodically
flushes sections to the container. This bounds the WAL size and limits
data loss on crash to at most one interval.

The timer polls a shutdown flag every 100 ms. On database close, the timer
is stopped before the final checkpoint to prevent races.

---

## File Locking

- **Exclusive lock** on create/open (read-write mode): prevents concurrent
  writers on the same file.
- **Shared lock** on open (read-only mode): allows multiple concurrent
  readers.
- Locks are released on close or drop.

---

## Size Estimates

| Component | Size |
|-----------|------|
| Fixed overhead (headers + directory) | 16 KiB |
| Empty database (headers + empty catalog + LPG) | ~20 KiB |
| Per-section overhead | 32 bytes (directory entry) + page alignment padding |
| Typical 10K-node LPG | ~1-5 MB |
| 1M-vector HNSW index (384-dim, f32) | ~1.5 GB |

---

## Version History

| Version | Format | Notes |
|---------|--------|-------|
| v1 (0.5.0-0.5.34) | Monolithic blob at `DATA_OFFSET` | Single bincode snapshot |
| v2 (0.5.35+) | Section-based with directory at `0x3000` | Independent sections, mmap capability flags |
| CompactStore payload v1–v3 | `GCST` section body | Section-level strings use `u16` lengths |
| CompactStore payload **v4** | `GCST` section body | Section-level strings use `u32` lengths; current writer |
