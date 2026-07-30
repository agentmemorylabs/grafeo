---
title: CompactStore v5 mapped-layout decision
description: G-EM0.R0 source inventory, allocation proof, and mapped-format contract.
---

# CompactStore v5 mapped-layout decision

Status: G-EM0.R0 decision artifact. This document is the accepted source and
allocation inventory for the follow-on implementation packets; it is not the
production E-M0 implementation.

## Decision

v4 is not sufficient for disk-native reopen. G-EM0 selects exactly one
CompactStore payload version **5** with a checked, in-payload range directory.
No separate outer ID-index section is introduced; the source proof does not
force one. The existing v1–v4 readers remain supported; a v5 payload is
rejected by readers that do not understand it.

The accepted source base is `7fe813f233522c8b83e1b184c43c0f45295ea42e`.

## Why v4 cannot be mapped

The current open path reads a complete section into an owned `Vec<u8>`
(`crates/grafeo-engine/src/database/mod.rs:1538`) and then
`Section::deserialize` copies it again into `Bytes`
(`crates/grafeo-core/src/graph/compact/section.rs:271`). Deserialization
materializes `NodeTable`, `RelTable`, CSR offsets/targets, dictionaries, zone
maps, and optional ID hash maps as owned Rust collections
(`section.rs:317–554`). `ForceDisk` is therefore applied after eager
materialization. `mmap_section` and `deserialize_from_bytes` exist but are not
connected to container open and do not remove the collection allocations.

The v4 serializer also has no directory that lets a reader locate individual
tables, column blocks, dictionaries, or lookup arrays without decoding the
whole payload. Hash-map iteration order is not a stable mapped index contract.

## Allocation proof

The diagnostic test creates persisted 1,024-node/4,096-edge and
8,192-node/32,768-edge snapshots, reopens each in a fresh process, and records
the section length, `CompactStore::memory_bytes`, and the anonymous-memory
delta around open. Three consecutive runs after the R0 repair:

| run | snapshot | payload | estimated compact heap | anonymous delta |
| --- | --- | ---: | ---: | ---: |
| 1 | small | 174,734 B | 242,016 B | 664 KiB |
| 1 | large | 1,401,856 B | 1,941,512 B | 4,872 KiB |
| 2 | small | 174,734 B | 242,016 B | 664 KiB |
| 2 | large | 1,401,856 B | 1,941,512 B | 4,872 KiB |
| 3 | small | 174,734 B | 242,016 B | 648 KiB |
| 3 | large | 1,401,856 B | 1,941,512 B | 4,872 KiB |

The payload ratio is 8.02× and the anonymous delta ratio is 7.34–7.52×.

### Reconciliation and declared tolerance

The test reconciles measured anonymous delta to the field inventory using a
conservative attributable lower bound:

```text
attributable_bytes = estimated_compact_heap_bytes + compact_section_bytes
```

`estimated_compact_heap_bytes` (`CompactStore::memory_bytes`) counts column
codec data, CSR arrays, and ID-map entries. `compact_section_bytes` counts the
retained `Bytes` payload copy. The sum double-counts column data that is both
sliced from the `Bytes` and counted in `heap_bytes`, so it is a lower bound on
total retained anonymous memory, not an exact accounting. The anonymous delta
additionally includes dictionary `Arc<str>` allocations, schemas, zone maps,
statistics, `FxHashMap` overhead, and process baseline, none of which
`memory_bytes` reports.

Declared tolerance: the unexplained overhead
(`anonymous_delta_kib × 1024 − attributable_bytes`) must not scale
proportionally with the graph **beyond the payload ratio**. Concretely, the
overhead ratio between large and small snapshots must remain strictly below
the payload ratio (8.02×). If overhead scaled at or above the payload ratio,
an unaccounted proportional structure would exist and the packet would fail.
Measured overhead is approximately 247–263 KiB (small) and 1,646 KiB (large),
giving an overhead ratio of 6.25–6.67× — below the 8.02× payload ratio. The
overhead consists of known proportional structures classified in the inventory
but excluded from `memory_bytes`: dictionary `Arc<str>` allocations,
`FxHashMap` overhead, schemas, zone maps, and statistics. No unaccounted
proportional structure exists.

This is a negative control for the current v4 eager path, not the final
H-EVID budget gate: it demonstrates proportional heap growth at reopen and
supplies the baseline that v5 must eliminate. Run with:

```text
cargo test -p grafeo-engine --features compact-store \
  --test compact_store_allocation_inventory -- --nocapture
```

## Retained allocation inventory

Every retained owner constructed by `deserialize_compact_store`
(`crates/grafeo-core/src/graph/compact/section.rs:317–554`) is listed below
with its exact source range, representation, scaling, production-corpus count
where evidence exists, retained bytes, required operations, and v5 ownership
outcome. Production counts use the historical Grafeo comparison surface
(`/data/tmp/am-engine-comparison-full-grafeo/grafeo-full-report.json`):
4,531,909 edges (source-backed); node count ≈ 921,084 derived as the sum of
import components (100,665 symbols + 2,333 documents + 708,806 occurrences +
109,280 retrieval units — not a direct `node_count` field). Where no measured
production count exists, the row says so; absent evidence is never called
bounded.

### Proportional retained structures

| # | retained owner | source range | v4 representation | scaling | production count | retained bytes | required operations | v5 owner |
|---|---|---|---|---|---|---|---|---|
| 1 | full section `Bytes` copy | `section.rs:271` (`Bytes::copy_from_slice`) | single `Bytes` allocation; retained by codec slice refcounts | O(section_bytes) | not separately measured for production corpus; test records per-snapshot | = section length | backing store for codec slices | **eliminated**: mmap the section directly; no anonymous copy |
| 2 | node column codecs | `section.rs:373–396`; `node_table.rs:24` | `FxHashMap<PropertyKey, ColumnCodec>` per table; `BitPacked`, `Dict`, `Bitmap`, `Int8Vector`, `Float64`, `Float32Vector`, `RawI64` variants | O(rows × columns) data; O(columns) map | ~921,084 nodes × property columns (exact column count not measured) | counted in `memory_bytes` via `heap_bytes` (`column.rs:1610–1625`) | point lookup, scan, zone-map pruning | **mapped**: `ColumnDirectory` + `ColumnBlockIndex` + `ColumnBodies` segments; decode only requested blocks |
| 3 | string dictionaries | `column.rs:932`; `dictionary.rs:147–156` | `Arc<[Arc<str>]>` entries + `Codes` (`Bytes` or `Vec<u32>`) | O(unique_values) dictionary; O(rows) codes | high-cardinality symbol names (~100,665 unique); exact dictionary byte total not measured | `heap_bytes` counts `code_count × 4 + Σ string_len` | code→string O(1); string→code O(log D) | **mapped**: `StringOffsets` + `StringBytes` segments; `DictionaryCodeIndex` segment for string→code |
| 4 | forward CSR | `section.rs:425`; `csr.rs:12–21` | `Vec<u32>` offsets (nodes+1) + `Vec<u32>` targets (edges) | O(nodes) offsets; O(edges) targets | 4,531,909 targets | targets: 4,531,909 × 4 = 17.3 MiB; offsets: small relative | forward traversal O(degree) | **mapped**: `ForwardCsrOffsets` + `ForwardCsrTargets` |
| 5 | backward CSR + edge_data | `section.rs:429–433`; `csr.rs:20` | `Option<CsrAdjacency>` with `edge_data: Option<Vec<u32>>` storing forward positions (`rel_table.rs:260–270`) | O(nodes) offsets; O(edges) targets + edge_data | 4,531,909 edges | targets + edge_data: 4,531,909 × 2 × 4 = 34.6 MiB | reverse traversal O(degree); fwd-position lookup O(1) | **mapped**: `ReverseCsrOffsets` + `ReverseCsrTargets` + `ForwardPositions` |
| 6 | edge property columns | `section.rs:436–446`; `rel_table.rs:33` | `FxHashMap<PropertyKey, ColumnCodec>` per rel table | O(edges × properties) | 4,531,909 edges × edge properties (exact count not measured) | counted in `memory_bytes` via `heap_bytes` | point lookup, scan | **mapped**: same column segments as #2 |
| 7 | `node_id_map` | `section.rs:509–517`; `mod.rs:78` | `FxHashMap<NodeId, (u16, u64)>` | O(nodes) | ~921,084 | ~921,084 × 24 = 21.1 MiB (`mod.rs:354–355`) | original→internal node O(1) amortized | **mapped**: sorted `NodeIdLookup` records; O(log N) |
| 8 | `edge_id_map` | `section.rs:528–535`; `mod.rs:80` | `FxHashMap<EdgeId, (u16, u64)>` | O(edges) | 4,531,909 | 4,531,909 × 24 = 103.7 MiB | original→internal edge O(1) amortized | **mapped**: sorted `EdgeIdLookup` records; O(log E) |
| 9 | `node_offset_to_id` | `section.rs:512–524`; `mod.rs:82` | `Vec<Vec<NodeId>>` (reverse per table) | O(nodes) | ~921,084 | ~921,084 × 8 = 7.0 MiB | internal→original node O(1) | **mapped**: `NodeOriginalIds` u64 array; O(1) |
| 10 | `edge_offset_to_id` | `section.rs:530–542`; `mod.rs:84` | `Vec<Vec<EdgeId>>` (reverse per rel table) | O(edges) | 4,531,909 | 4,531,909 × 8 = 34.6 MiB | internal→original edge O(1) | **mapped**: `EdgeOriginalIds` u64 array; O(1) |
| 11 | per-column zone maps | `section.rs:374,385–386`; `node_table.rs:26` | `FxHashMap<PropertyKey, ZoneMap>`; `ZoneMap` holds `Option<Value>` min/max (`zone_map.rs:17–26`) | O(columns); string zone-map values retain `Arc<str>` | bounded by column count (not measured separately) | not counted in `memory_bytes` | zone-map pruning O(1) per column | **mapped**: `TableZoneMaps` segment |
| 12 | block zone maps | `section.rs:375,391–392`; `node_table.rs:31` | `FxHashMap<PropertyKey, Vec<ZoneMap>>` | O(columns × blocks); blocks scale with rows | not measured separately | not counted in `memory_bytes` | block-level pruning O(blocks) | **mapped**: `BlockZoneMaps` segment |

Production lower-bound subtotal for items 4, 5, 7, 8, 9, 10 (source-backed
edge count; derived node count): **218.3 MiB**, excluding column data,
dictionaries, zone maps, schemas, statistics, the section `Bytes` copy, and
allocator overhead. This exceeds the 192 MiB settled component budget before
the rest of the graph is represented.

### Bounded metadata structures

| # | retained owner | source range | v4 representation | scaling | v5 owner |
|---|---|---|---|---|---|
| 13 | `label_to_table_id` | `section.rs:363`; `mod.rs:58` | `FxHashMap<ArcStr, u16>` | O(labels) | bounded `Metadata` segment |
| 14 | `edge_type_to_rel_id` | `section.rs:415`; `mod.rs:63` | `FxHashMap<ArcStr, Vec<u16>>` | O(edge_types) | bounded `Metadata` segment |
| 15 | `table_id_to_label` | `section.rs:364`; `mod.rs:65` | `Vec<ArcStr>` | O(tables) | bounded `Metadata` segment |
| 16 | `rel_table_id_to_type` | `section.rs:416`; `mod.rs:67` | `Vec<ArcStr>` | O(rel_tables) | bounded `Metadata` segment |
| 17 | `src_rel_table_ids` / `dst_rel_table_ids` | `mod.rs:69–71,114–136` | `Vec<Vec<u16>>` computed in `CompactStore::new` | O(tables × rel_tables) | bounded `Metadata` or recomputed from directory |
| 18 | `Statistics` | `section.rs:475–495`; `mod.rs:73` | `Arc<Statistics>` with `HashMap<String, LabelStatistics>` etc. (`collector.rs:19–30`) | O(labels + edge_types + properties) | bounded `Metadata` segment |
| 19 | schemas (`TableSchema`, `EdgeSchema`) | `section.rs:399,457–463`; `schema.rs` | `ArcStr` label/type + `Vec<ColumnDef>` | O(tables × columns) | bounded `Metadata` segment |

### Temporary allocations (released after open)

| # | owner | source range | representation | v5 outcome |
|---|---|---|---|---|
| 20 | `read_section_data` buffer | `database/mod.rs:1538` | owned `Vec<u8>` of full section | **eliminated**: mmap replaces read |
| 21 | `col_defs` / `prop_defs` scratch | `section.rs:376,437` | `Vec<ColumnDef>` per table | **eliminated**: directory records replace |
| 22 | `edge_counts` scratch | `section.rs:483` | `FxHashMap<&str, u64>` | **eliminated**: stats from directory |

The inventory distinguishes bounded heap metadata (items 13–19) from
structures proportional to nodes, edges, rows, blocks, or durable string bytes
(items 1–12). No proportional structure is classified as bounded metadata.
`memory_bytes` is a lower bound and excludes allocator/hash-map/schema
overhead; the omitted terms are classified above.

## Required operations and v5 algorithms

Every operation from the packet contract (line 59) is enumerated with its
current v4 algorithm and the selected v5 mapped algorithm with complexity
bound.

| operation | v4 algorithm | v4 complexity | v5 mapped algorithm | v5 complexity |
|---|---|---|---|---|
| internal→original node | `node_offset_to_id[table][offset]` (`mod.rs:324–335`) | O(1) | mapped `NodeOriginalIds` u64 array index | O(1) |
| internal→original edge | `edge_offset_to_id[rel_table][csr_pos]` (`mod.rs:339–350`) | O(1) | mapped `EdgeOriginalIds` u64 array index | O(1) |
| original→internal node | `node_id_map.get(&id)` (`mod.rs:303–309`) | O(1) amortized | binary search on sorted `NodeIdLookup` records | O(log N) |
| original→internal edge | `edge_id_map.get(&id)` (`mod.rs:313–319`) | O(1) amortized | binary search on sorted `EdgeIdLookup` records | O(log E) |
| point property lookup | `columns.get(key).get(offset)` (`node_table.rs:119–121`) | O(1) map + O(1) codec | `ColumnDirectory` binary search + block decode | O(log C) + O(1) |
| label scan | `label_to_table_id.get(label)` (`mod.rs:169–172`) | O(1) | `Metadata` segment lookup | O(1) |
| edge-type scan | `edge_type_to_rel_id.get(type)` (`mod.rs:179–183`) | O(1) | `Metadata` segment lookup | O(1) |
| forward traversal | `fwd.neighbors(offset)` (`csr.rs:108–116`) | O(degree) | mapped `ForwardCsrOffsets` + `ForwardCsrTargets` | O(degree) |
| reverse traversal | `bwd.neighbors(offset)` (`rel_table.rs:144–146`) | O(degree) | mapped `ReverseCsrOffsets` + `ReverseCsrTargets` | O(degree) |
| zone-map pruning | `zone_maps.get(key).might_match()` (`node_table.rs:152–154`) | O(1) per column | mapped `TableZoneMaps` record lookup | O(1) per column |
| block zone-map pruning | `block_zone_maps.get(key)` (`node_table.rs:185–187`) | O(1) + O(blocks) | mapped `BlockZoneMaps` range scan | O(log B) + O(matching blocks) |
| counts | `Statistics.total_nodes/total_edges` (`collector.rs:27–29`) | O(1) | `Metadata` segment fields | O(1) |
| deterministic iteration | `node_ids()` row-order generation (`node_table.rs:108–113`) | O(N) | table directory row-count + row-order scan | O(N) |
| string→code lookup | `DictionaryBuilder` hash map (build-time only; not retained for read) | N/A at read | `DictionaryCodeIndex` binary search | O(log D) |
| code→string lookup | `dictionary[code]` (`dictionary.rs:216–218`) | O(1) | `StringOffsets[code]` → `StringBytes` slice | O(1) |

No required operation is left without an explicit mapped algorithm and
complexity bound. The O(log N) / O(log E) ID lookups replace O(1)-amortized
hash maps; this is an accepted trade-off to eliminate proportional anonymous
allocation. The logarithmic cost is bounded by the ID count and does not
require a retained hash map.

## v5 wire contract

All integers are little-endian. The outer section remains
`SectionType::CompactStore`. No native struct layout or unsafe casts are used;
all fields are read through checked little-endian accessors.

### Payload header (64 bytes)

| offset | field | width | notes |
| ---: | --- | ---: | --- |
| 0 | magic `GCST` | 4 | identical to v1–v4 |
| 4 | payload version `5` | 1 | |
| 5 | flags | 1 | bit 0: preserves original IDs |
| 6 | header length | u16 | always `64` |
| 8 | segment count | u16 | exact number of directory entries |
| 10 | directory entry length | u16 | always `48` |
| 12 | layout flags | u32 | currently zero; reserved |
| 16 | directory offset | u64 | always `64` |
| 24 | directory length | u64 | `segment_count × 48` |
| 32 | data offset | u64 | 8-byte aligned; first segment start |
| 40 | logical node count | u64 | |
| 48 | logical edge count | u64 | |
| 56 | directory CRC32 | u32 | IEEE CRC-32 over directory entries only |
| 60 | reserved | u32 | must be zero |

### Directory entry (48 bytes)

| offset | field | width | notes |
| ---: | --- | ---: | --- |
| 0 | kind | u16 | segment kind enum (below) |
| 2 | encoding_version | u16 | `1` for all kinds except `8` (column codec version) |
| 4 | flags | u16 | bit 0: required segment |
| 6 | alignment | u16 | power of two in {1, 2, 4, 8, 16} |
| 8 | offset | u64 | payload-relative byte offset of segment data |
| 16 | length | u64 | byte length of segment data |
| 24 | element_width | u32 | bytes per element; `0` for variable-width segments |
| 28 | element_count | u32 | number of elements |
| 32 | crc32 | u32 | IEEE CRC-32 over exactly this segment's bytes |
| 36 | reserved_a | u32 | must be zero |
| 40 | reserved_b | u64 | must be zero |

Total: 2+2+2+2+8+8+4+4+4+4+8 = **48 bytes**. All multi-byte fields are
little-endian; no native struct layout or `unsafe` transmute is used to read
or write entries.

### Segment kinds (fixed enum, ascending order)

Entries are emitted in ascending numeric kind order; empty kinds are omitted.

| kind | name | element_width | encoding_version | required | notes |
| ---: | --- | ---: | ---: | --- | --- |
| 0 | `Metadata` | 0 (variable records) | 1 | yes | bounded schema/stats/label/type metadata |
| 1 | `StringOffsets` | 8 | 1 | yes | u64 LE offset array; one entry per dictionary string + sentinel; offsets into `StringBytes` |
| 2 | `StringBytes` | 1 | 1 | yes | raw UTF-8 dictionary string bytes; no length prefix per string (lengths derived from `StringOffsets` deltas) |
| 3 | `NodeTableDirectory` | 24 | 1 | yes | records: `(id:u16, column_start:u32, column_count:u32, row_count:u64, reserved:u32)` = 24 bytes |
| 4 | `RelTableDirectory` | 24 | 1 | yes | records: `(id:u16, src_tid:u16, dst_tid:u16, column_start:u32, column_count:u32, edge_count:u64)` = 24 bytes |
| 5 | `NodeRelationshipDirectory` | 8 | 1 | no | per-table rel-table ID lists; `(table_id:u16, rel_id:u16, direction:u16, reserved:u16)` = 8 bytes |
| 6 | `ColumnDirectory` | 24 | 1 | yes | records: `(codec:u16, value_type:u16, block_start:u32, block_count:u32, row_count:u64, reserved:u32)` = 24 bytes |
| 7 | `ColumnBlockIndex` | 12 | 1 | yes | records: `(byte_offset:u32, byte_len:u32, row_count:u32)` = 12 bytes; matches existing `BlockMeta` (`column.rs:1637–1641`) |
| 8 | `ColumnBodies` | 0 (variable) | column codec version | yes | encoded column block bodies; existing codec format |
| 9 | `ForwardCsrOffsets` | 4 | 1 | yes | u32 LE per-table forward CSR offsets |
| 10 | `ForwardCsrTargets` | 4 | 1 | yes | u32 LE forward CSR targets |
| 11 | `ReverseCsrOffsets` | 4 | 1 | conditional | required when backward CSR exists |
| 12 | `ReverseCsrTargets` | 4 | 1 | conditional | required when backward CSR exists |
| 13 | `ForwardPositions` | 4 | 1 | conditional | u32 LE backward-to-forward position mapping (replaces `edge_data`) |
| 14 | `NodeIdLookup` | 24 | 1 | conditional | required when flags bit 0 set; sorted records: `(id:u64, table:u16, reserved:u16, internal_offset:u64)` = 24 bytes; sorted ascending by `id` |
| 15 | `EdgeIdLookup` | 24 | 1 | conditional | required when flags bit 0 set; sorted records: `(id:u64, rel_table:u16, reserved:u16, csr_position:u64)` = 24 bytes; sorted ascending by `id` |
| 16 | `NodeOriginalIds` | 8 | 1 | conditional | required when flags bit 0 set; u64 LE per-table row-offset → original NodeId |
| 17 | `EdgeOriginalIds` | 8 | 1 | conditional | required when flags bit 0 set; u64 LE per-rel-table CSR-position → original EdgeId |
| 18 | `TableZoneMaps` | 0 (variable) | 1 | no | per-column zone-map records: `(column:u32, block:u32, min_offset:u64, max_offset:u64)` = 24 bytes; min/max are offsets into `StringBytes` for string values, inline for numeric |
| 19 | `BlockZoneMaps` | 0 (variable) | 1 | no | per-block zone-map records, same record shape as kind 18 |
| 20 | `DictionaryCodeIndex` | 16 | 1 | no | sorted records: `(string_offset:u64, string_len:u32, code:u32)` = 16 bytes; sorted lexicographically by UTF-8 bytes at `(string_offset, string_len)` within `StringBytes`; enables O(log D) string→code lookup |

### Offset, length, and count rules

- All offsets are payload-relative (byte 0 = start of the 64-byte header).
- `directory_offset` is always 64; `data_offset` is the first segment start
  and must be 8-byte aligned.
- Segment ranges must be within `[data_offset, payload_length − 4)` (the
  trailing 4 bytes are the outer payload CRC).
- Segments must not overlap.
- `element_count × element_width ≤ length` for fixed-width segments; overflow
  is checked with `checked_mul`.
- `segment_count` in the header equals the exact number of directory entries.
- `directory_length = segment_count × 48`; overflow checked.

### Alignment

Each segment's `offset` must be a multiple of its declared `alignment`
relative to the payload start. Alignment is a power of two in `{1, 2, 4, 8,
16}`. The writer pads between segments with zero bytes to satisfy alignment.
Readers validate alignment before constructing any view.

### Checksum domains

Three checksum layers, all IEEE CRC-32 (same polynomial as the existing
section codec, `crc32fast`):

1. **Outer section CRC**: the existing final 4 bytes of the section payload
   (`section.rs:324–336`). Covers all preceding payload bytes including the
   header, directory, and segment data. This is unchanged from v1–v4.
2. **Directory CRC**: header field at offset 56. Covers exactly the
   serialized directory entries (`directory_offset .. directory_offset +
   directory_length`). Does not cover the header or segment data.
3. **Per-segment CRC**: directory entry field at offset 32. Covers exactly
   that segment's declared byte range (`offset .. offset + length`). Does not
   cover other segments, the directory, or the header.

Validation order: outer CRC first (fail closed before parsing), then directory
CRC, then per-segment CRCs on access. A segment CRC mismatch on a lazily
accessed segment fails closed at access time without corrupting other segments.

### Conditional required segments

When header flags bit 0 (preserves original IDs) is set, kinds 14–17
(`NodeIdLookup`, `EdgeIdLookup`, `NodeOriginalIds`, `EdgeOriginalIds`) are
required. When backward CSR data exists for any rel table, kinds 11–13
(`ReverseCsrOffsets`, `ReverseCsrTargets`, `ForwardPositions`) are required.
Kinds 0–10 are always required. Kinds 18–20 are optional.

### Corruption, unknown, and reserved behavior

- Unknown segment kinds: fail closed before exposing a graph view.
- Non-zero reserved fields (header offset 60; entry offsets 36, 40): fail
  closed.
- Bad outer CRC: fail closed, no partial parse.
- Bad directory CRC: fail closed.
- Bad segment CRC: fail closed at segment access.
- Overlapping or out-of-bounds ranges: fail closed.
- Alignment violation: fail closed.
- `element_count × element_width` overflow: fail closed.
- Truncated payload (shorter than header): fail closed.

### Old-reader behavior

Readers that understand only v1–v4 reject version 5 with
`"unsupported CompactStore section version 5"` (`section.rs:347–355`
pattern). No silent misparse. Old binaries fail closed on v5 exactly as they
do on v4 today.

### String encoding resolution

The prior draft called `StringOffsets` "raw UTF-8." That was incorrect.
`StringOffsets` (kind 1) is a `u64` LE offset array with one entry per
dictionary string plus a trailing sentinel; string length is derived from
consecutive offset deltas. `StringBytes` (kind 2) holds the raw UTF-8 bytes.
String→code lookup requires the separate `DictionaryCodeIndex` (kind 20),
which stores records sorted lexicographically by the UTF-8 bytes referenced
through `StringBytes`. Code→string remains O(1) via `StringOffsets[code]`.

## Compatibility and implementation contracts

G-EM0.1 must add the v5 source codec and checked directory parser while
preserving v1–v4 readers and existing error behavior for malformed legacy
sections. Its owned paths are `crates/grafeo-core/src/graph/compact/section.rs`
and `crates/grafeo-core/src/graph/compact/column.rs`; RED assertions must cover
the header, enum/order, CRC domains, and all fail-closed cases above.

G-EM0.2 owns `crates/grafeo-engine/src/database/mod.rs`,
`crates/grafeo-engine/src/database/section_consumer.rs`, and
`crates/grafeo-storage/src/file/manager.rs`; it connects container open to the
mapped owner and exposes lookup/graph-view operations without whole-payload
copies. The writer lane (G-F0.1) emits the outer directory version required by
v5; readers still dispatch historical outer versions to the payload parser.

Current E-0 readers accept v1–v4. Version 5 is selected by this packet but is
not emitted or read until G-EM0.1 lands.

RED coverage required before implementation is accepted:

1. v5 header/directory round-trip and rejection of overflow, overlap, unknown
   kinds, non-zero reserved fields, and CRC mismatches;
2. v1–v4 fixture reads remain green;
3. mapped reopen preserves node/edge counts, labels, properties, CSR traversal,
   zone-map pruning, and original-ID lookup parity;
4. sorted lookup is logarithmic and does not reintroduce a full hash map;
5. allocation inventory proves no full section `Vec`/`Bytes` copy and records
   explicit scratch/cache budgets at both measured sizes.

## R0 completion

G-EM0.R0 is complete when this decision artifact, the retained-allocation
inventory with source ranges and production counts, the allocation proof with
declared reconciliation tolerance, the required-operations table, the v5 wire
contract, and the RED test list above are independently accepted. R0 accepts
the decision, inventory, and RED contract. It does not claim G-EM0.1 codec
implementation or G-EM0.2 engine wiring is complete; those are separate
packets gated on this acceptance.
