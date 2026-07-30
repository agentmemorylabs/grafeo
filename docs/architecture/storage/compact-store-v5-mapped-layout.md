---
title: CompactStore v5 mapped-layout decision
description: G-EM0.R0 source inventory, allocation proof, and mapped-format contract.
---

# CompactStore v5 mapped-layout decision

Status: G-EM0.R0 decision artifact. This document is a source and allocation
inventory for the follow-on implementation packets; it is not the production
E-M0 implementation.

## Decision

v4 is not sufficient for disk-native reopen. G-EM0 selects one v5
`CompactStore` payload with a checked, in-payload range directory. No separate
outer ID-index section is introduced. The existing v1-v4 readers remain
supported; a v5 payload is rejected by readers that do not understand it.

The accepted source base is `7fe813f233522c8b83e1b184c43c0f45295ea42e`.

## Why v4 cannot be mapped

The current open path reads a complete section into an owned `Vec<u8>` and
then `Section::deserialize` copies it again into `Bytes`. Deserialization
materializes `NodeTable`, `RelTable`, CSR offsets/targets, dictionaries, zone
maps, and optional ID hash maps as owned Rust collections. `ForceDisk` is
therefore applied after eager materialization. `mmap_section` and
`deserialize_from_bytes` exist, but are not connected to container open and do
not remove the collection allocations.

The v4 serializer also has no directory that lets a reader locate individual
tables, column blocks, dictionaries, or lookup arrays without decoding the
whole payload. Hash-map iteration order is not a stable mapped index contract.

## Allocation proof

The diagnostic test creates persisted 1,024-node/4,096-edge and
8,192-node/32,768-edge snapshots, reopens each in a fresh process, and records
the section length, `CompactStore::memory_bytes`, and the anonymous-memory
delta around open:

| snapshot | payload | estimated compact heap | anonymous delta |
| --- | ---: | ---: | ---: |
| small | 174,734 B | 242,016 B | 656 KiB |
| large | 1,401,856 B | 1,941,512 B | 4,864 KiB |

The payload ratio is 8.02x and the anonymous delta ratio is 7.42x. This is a
negative control for the current v4 eager path, not the final H-EVID budget
gate: it demonstrates proportional heap growth at reopen and supplies the
baseline that v5 must eliminate. Run with:

```text
cargo test -p grafeo-engine --features compact-store \
  --test compact_store_allocation_inventory -- --nocapture
```

## Retained allocation inventory

Every retained v4 field is classified below. The v5 owner is either a mapped
range, a bounded metadata object, or an explicitly budgeted cache.

| retained structure | current v4 allocation | v5 mapped owner |
| --- | --- | --- |
| node/relationship table directories | `Vec<NodeTable>`, `Vec<RelTable>` and per-table maps | directory ranges plus mapped scalar arrays |
| node and relationship columns | decoded `ColumnData`/codec buffers | column-block ranges; decode only requested values |
| string dictionaries | `Vec<Arc<str>>` and copied strings | string offsets/bytes ranges; sorted code index |
| zone maps | owned per-column zone maps | mapped zone-map ranges |
| CSR routing | `Vec<u32>` offsets/targets and edge data | mapped forward/reverse CSR ranges |
| original-ID lookup | `FxHashMap` plus reverse `Vec` | sorted `(id, table, offset)` records plus mapped reverse arrays |
| statistics | `Arc<GraphStats>` | bounded metadata snapshot |
| deserialization scratch | full section `Vec`, `Bytes` copy, codec scratch | bounded header/directory validation scratch |

The inventory is deliberately field-oriented for implementation ownership:
`node_tables`, `rel_tables`, label/type interners and reverse dictionaries,
property-key maps, per-table column maps, validity/code/block arrays, table and
block zone-map strings, forward/reverse nested CSR vectors, original-ID maps
and reverse ID vectors, preservation flags, `GraphStats`, and decoder scratch
are all covered by the rows above. Each scales with node/edge/table/dictionary
cardinality; no row is an unbounded per-open cache. G-EM0.1 must attach exact
byte counts and source line ranges to these rows before claiming the source
contract complete.

`memory_bytes` is a lower bound and excludes allocator/hash-map/schema
overhead. The omitted terms are explicitly classified above as proportional
v4 allocations; the diagnostic therefore proves scaling, not final peak
budget compliance.

## v5 wire contract

All integers are little-endian. The outer section remains `SectionType::CompactStore`.
The payload starts with this fixed 64-byte header:

| offset | field | width |
| ---: | --- | ---: |
| 0 | magic `GCST` | 4 |
| 4 | payload version `5` | 1 |
| 5 | flags (bit 0 preserves original IDs) | 1 |
| 6 | header length (`64`) | u16 |
| 8 | segment count | u16 |
| 10 | directory entry length (`48`) | u16 |
| 12 | layout flags (currently zero) | u32 |
| 16 | directory offset (`64`) | u64 |
| 24 | directory length | u64 |
| 32 | data offset (8-byte aligned) | u64 |
| 40 | logical node count | u64 |
| 48 | logical edge count | u64 |
| 56 | directory CRC32 | u32 |
| 60 | reserved (zero) | u32 |

Each 48-byte directory entry contains kind, encoding version, flags,
alignment, payload-relative offset/length, element width/count, CRC32, and
reserved words. Entries must be unique and ordered; all ranges must be within
the payload, non-overlapping, aligned as declared, and overflow-checked.
Alignment is a power of two in `{1, 2, 4, 8, 16}` and applies to the segment
start relative to the payload. Directory ordering is ascending numeric kind;
the header's `segment_count` is the exact number of entries. CRC32 is the
IEEE CRC-32 used by the existing section codec, with the directory checksum
covering only the serialized directory entries and each segment checksum
covering only that segment's bytes. `encoding_version=1` records are little
endian and use the declared element width; no implicit host layout is valid.
Unknown kinds, non-zero reserved fields, bad checksums, and inconsistent
counts fail closed before exposing a graph view. Mapped bytes are read through
checked little-endian accessors; arbitrary byte slices are never unsafe-cast.

The numeric segment-kind enum is fixed for v5 and entries are emitted in this
order (empty kinds are omitted, preserving order): `0 Metadata`, `1 StringOffsets`,
`2 StringBytes`, `3 NodeTableDirectory`, `4 RelTableDirectory`,
`5 NodeRelationshipDirectory`, `6 ColumnDirectory`, `7 ColumnBlockIndex`,
`8 ColumnBodies`, `9 ForwardCsrOffsets`, `10 ForwardCsrTargets`,
`11 ReverseCsrOffsets`, `12 ReverseCsrTargets`, `13 ForwardPositions`,
`14 NodeIdLookup`, `15 EdgeIdLookup`, `16 NodeOriginalIds`,
`17 EdgeOriginalIds`, `18 TableZoneMaps`, and `19 BlockZoneMaps`.
Encoding version `1` is the only accepted encoding for kinds 0, 3-7, 9-19;
kind 8 uses the existing column codec version and kinds 1-2 use raw UTF-8
bytes. The directory CRC covers the complete directory byte range; each
segment CRC covers exactly its declared payload range. Required kinds cover
metadata, string offsets/bytes, table directories, column directories/block indexes/bodies, forward and reverse CSR arrays,
original-ID arrays, sorted ID lookup records, and table/block zone maps. CSR
offsets and targets retain the current per-table `u32` bound. String offsets
and file ranges use `u64`.

ID lookup records are sorted by original ID as `(u64 id, u16 table,
u16 reserved, u64 internal_offset)`, giving O(log N) and O(log E) lookup.
Reverse arrays are mapped `u64` arrays for O(1) internal-to-original access.
Dictionary code order is preserved for value decoding; a separate sorted
`u32` index gives O(log D) string-to-code lookup while code-to-string remains
O(1). Metadata is capped at 16 MiB; larger structures are segmented and
budgeted under the global compact-store ceilings.

Metadata records are fixed-width `(kind:u16, flags:u16, first:u64, count:u64)`;
table and relationship directory records are `(id:u16, column_start:u32,
column_count:u32, row_count:u64)`; column records are `(codec:u16,
value_type:u16, block_start:u32, block_count:u32, row_count:u64)`; zone-map
records are `(column:u32, block:u32, min_offset:u64, max_offset:u64)`.
These records are directory metadata, not decoded values, and are sufficient
to locate every mapped body without a scan.

## Compatibility and implementation contracts

G-EM0.1 must add the v5 source codec and checked directory parser while
preserving v1-v4 readers and existing error behavior for malformed legacy
sections. Its owned paths are `crates/grafeo-core/src/graph/compact/section.rs`
and `crates/grafeo-core/src/graph/compact/column.rs`; RED assertions must cover
the header, enum/order, CRC domains, and all fail-closed cases above.
G-EM0.2 owns `crates/grafeo-engine/src/database/mod.rs`,
`crates/grafeo-engine/src/section_consumer.rs`, and
`crates/grafeo-storage/src/file/manager.rs`; it connects container open to the
mapped owner and exposes lookup/graph-view operations without whole-payload
copies. The writer lane (G-F0.1) emits the outer directory version required by
v5; readers still dispatch historical outer versions to the payload parser.

RED coverage required before implementation is accepted:

1. v5 header/directory round-trip and rejection of overflow, overlap, unknown
   kinds, non-zero reserved fields, and CRC mismatches;
2. v1-v4 fixture reads remain green;
3. mapped reopen preserves node/edge counts, labels, properties, CSR traversal,
   zone-map pruning, and original-ID lookup parity;
4. sorted lookup is logarithmic and does not reintroduce a full hash map;
5. allocation inventory proves no full section `Vec`/`Bytes` copy and records
   explicit scratch/cache budgets at both measured sizes.

This packet is complete when the source contracts and RED list above are
implemented and independently reviewed; it does not claim those later gates
are already complete.
