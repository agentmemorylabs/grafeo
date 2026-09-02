---
title: Compact Store
description: Convert a database to a layered columnar format for faster queries and lower memory usage; remains writable through an overlay.
tags:
  - performance
  - storage
  - compact-store
  - wasm
---

# Compact Store

CompactStore is a columnar graph format that trades some write performance for large
memory and query wins. After ingesting data, call `compact()` to switch the database
to a columnar layout with CSR adjacency. From 0.5.39, `compact()` is **non-destructive
and writable**: it produces a layered store with an immutable columnar base plus a
mutable overlay. Inserts and property updates after `compact()` land in the overlay;
`recompact()` merges the overlay back into a fresh base.

Graph queries keep working across supported languages after `compact()`, and
named graphs are preserved across `compact()` / `recompact()`.

**Index caveat (not closed by CompactStore E-0):** vector, text, and hybrid
indexes on a layered store are **incomplete** relative to a full LPG
database. Catalog / index-state preservation across layered checkpoint
(E-1) and immutable-base-plus-overlay vector serving (E-2) are separate
work. Do not treat post-compact vector/text/hybrid search as fully
production-complete until those land. Prefer verifying the paths you need
against the current release notes before relying on them.

**When to use it:** workloads that ingest once and query many times, or read-heavy
workloads with occasional updates. Code analysis tools, static knowledge graphs,
pre-built datasets for WASM or edge deployments.

## Performance

Measured on the same data, CompactStore vs the standard mutable LpgStore:

| Metric | LpgStore | CompactStore | Improvement |
|--------|----------|--------------|-------------|
| Memory per node (degree 5) | ~3,200 bytes | ~51 bytes | **63x** |
| Edge traversal (10K lookups) | 619 us | 5.3 us | **116x** |
| Property random access (10K) | 123 us | 10 us | **12x** |

The gains come from eliminating MVCC version chains, read locks, hash lookups, and
chunk decompression. CompactStore replaces those with array indexing and contiguous
memory reads.

## Quick Start

=== "Python"

    ```python
    import grafeo

    db = grafeo.GrafeoDB()

    # Ingest data (read-write phase)
    db.execute("INSERT (:Person {name: 'Alix', age: 30})")
    db.execute("INSERT (:Person {name: 'Gus', age: 25})")
    db.execute("INSERT (:City {name: 'Amsterdam'})")
    db.execute("""
        MATCH (p:Person {name: 'Alix'}), (c:City {name: 'Amsterdam'})
        INSERT (p)-[:LIVES_IN]->(c)
    """)

    # Switch to compact mode (subsequent writes go to a mutable overlay)
    db.compact()

    # Queries work as before, but faster
    result = db.execute("MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name")
    ```

=== "Node.js"

    ```typescript
    import { GrafeoDB } from '@grafeo-db/node';

    const db = GrafeoDB.create();

    await db.execute("INSERT (:Person {name: 'Alix', age: 30})");
    await db.execute("INSERT (:City {name: 'Amsterdam'})");
    await db.execute(`
        MATCH (p:Person {name: 'Alix'}), (c:City {name: 'Amsterdam'})
        INSERT (p)-[:LIVES_IN]->(c)
    `);

    db.compact();

    const result = await db.execute(
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name"
    );
    ```

=== "WASM"

    ```javascript
    import init, { Database } from '@grafeo-db/wasm';
    await init();

    const db = new Database();
    db.execute("INSERT (:Person {name: 'Alix', age: 30})");
    db.execute("INSERT (:City {name: 'Amsterdam'})");

    db.compact();

    const result = db.execute(
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name"
    );
    ```

=== "C"

    ```c
    #include "grafeo.h"

    GrafeoDatabase *db = grafeo_open_memory();

    grafeo_execute(db, "INSERT (:Person {name: 'Alix', age: 30})");
    grafeo_execute(db, "INSERT (:City {name: 'Amsterdam'})");

    grafeo_compact(db);

    GrafeoResult *r = grafeo_execute(db,
        "MATCH (p:Person) RETURN p.name");
    ```

=== "Rust"

    ```rust
    use grafeo::GrafeoDB;

    let mut db = GrafeoDB::new_in_memory();

    db.execute("INSERT (:Person {name: 'Alix', age: 30})")?;
    db.execute("INSERT (:City {name: 'Amsterdam'})")?;

    db.compact()?;

    let result = db.execute(
        "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.name"
    )?;
    ```

## How It Works

`compact()` performs four steps:

1. **Scans** all nodes from the current store, grouped by label
2. **Infers** column types from property values and builds per-label columnar tables
3. **Builds** forward and backward CSR adjacency for each edge type
4. **Swaps** the database to a layered store: the new columnar tables become the
   immutable base and a mutable overlay is attached on top to absorb subsequent
   writes. `recompact()` later folds the overlay back into a fresh base.

The result is a `CompactStore` backed by:

- **Per-label columnar tables** with typed codecs (bit-packed integers, dictionary-encoded
  strings, boolean bitmaps)
- **Double-indexed CSR** (Compressed Sparse Row) for O(degree) forward and backward traversal
- **Zone maps** (min/max statistics per column) for predicate pushdown

## Type Mapping

Property values are automatically mapped to the most efficient columnar codec:

| Value type | Codec | Notes |
|------------|-------|-------|
| `Int64` (non-negative) | BitPacked | Auto-determined bit width |
| `Bool` | Bitmap | 1 bit per value |
| `String` | Dictionary | Deduplicated string table |
| `Float64` | Float64 (native) | 8 bytes per value, since 0.5.40 |
| `Vector` (f32) | Float32Vector (native) | Contiguous float32 storage, since 0.5.40 |
| Mixed `Int64 + Float64` | Float64 (native) | Columns coalesce to `Float64` when both types appear |
| Negative `Int64` | Dictionary | Serialized as string |
| `List`, `Map`, `Timestamp`, etc. | Dictionary | Serialized as string |

!!! note
    Before 0.5.40, `Float64` and `Vector` columns fell back to dictionary encoding,
    which preserved data but lost typed semantics for range scans. Native codecs
    now retain those semantics without a dictionary round-trip. Dictionary fallback
    still applies to negative integers and complex values (`List`, `Map`, etc.).

## Writes After `compact()`

Since 0.5.39, `compact()` returns a layered store: an immutable columnar base plus a
mutable overlay. New inserts and property updates land in the overlay and are visible
to subsequent queries (`get_node`, property reads, pattern matching, `list_graphs`).

Call `recompact()` to merge the overlay back into a fresh base:

    db.compact()
    db.execute("INSERT (:Person {name: 'Mia'})")   # lands in overlay
    db.recompact()                                  # merges overlay into new base

**Indexes on layered stores:** creating vector/text indexes after `compact()` may
succeed for overlay-local data, but **base-only vectors are not fully served
from the immutable CompactStore** until the E-2 layered vector work lands, and
catalog/index descriptors are not fully preserved through every layered
checkpoint path until E-1. Treat index coverage as partial unless your release
explicitly claims otherwise.

## Persistence and format

On a persistent `.grafeo` database, `compact()` + explicit `close()` checkpoints
a **CompactStore** section (`GCST` payload) into the container alongside the
overlay LPG section. Reopen reconstructs a layered store from that section.

- **Current payload version:** **4** (section-level strings use `u32` lengths).
- **Readers** accept CompactStore payload v1–v4; **old writers/readers** that
  only understand ≤v3 cannot read v4 (fail closed).
- **Container open today** still eagerly loads the CompactStore section into
  owned memory (then may spill under ForceDisk). Direct mmap of the container
  CompactStore section is **not** the default open path yet.
- Historical files may carry outer directory version `1` while the GCST payload
  is v2/v3/v4; dispatch from the payload header, not a strict outer-version
  equality check.

## Limitations

- **Overlay write path**: writes go through the overlay, which is less optimized than
  `LpgStore`'s full MVCC path. Sustained write-heavy workloads should stay on `LpgStore`
  or call `recompact()` periodically.
- **Multi-label nodes**: nodes with multiple labels are stored under a compound key
  (e.g., `"Actor|Person"`, sorted alphabetically). A query like `MATCH (n:Person)` will
  not match nodes stored under `"Actor|Person"`. Workarounds:
    - **Preferred:** use a single label per node before compacting.
    - **Alternative:** query the compound label explicitly, e.g., `MATCH (n:Actor:Person)` (labels in alphabetical order).
    - **Alternative:** assign a canonical "primary" label and store additional labels as a list property instead.
- **Layered index completeness**: see the index caveat above (E-1/E-2).
- **Open memory**: container reopen is not yet disk-native for CompactStore; large
  compacted graphs still allocate proportional anonymous memory on open until
  the direct-mmap lifecycle lands.

## Feature Flag

CompactStore requires the `compact-store` feature flag.

**Engine-level named profiles** (`grafeo-engine` / `grafeo` crate): `embedded`,
`browser`, `server`, and `full` do **not** automatically enable `compact-store`.
Enable it explicitly: `cargo build -p grafeo-engine --features compact-store`
(and add `grafeo-file` / `mmap` when you need persistence or spill).

**Binding-level defaults** (checked against current binding manifests):

| Binding | Default profile | Includes `compact-store` |
|---------|-----------------|--------------------------|
| Python (`crates/bindings/python`) | `embedded` | Yes |
| Node.js (`crates/bindings/node`) | `embedded` | Yes |
| C (`crates/bindings/c`) | `embedded` | Yes |
| WASM (`crates/bindings/wasm`) | `edge` | Yes |

For custom Rust builds: `cargo build --features compact-store`.
