# CompactStore payload fixtures (immutable)

Generated from exact AMLabs Grafeo pin `9781320f1eb975c9a40e7e13838cc04a35e6f8c8`
**before** the E-0.1 CompactStore v4 string-codec change. Do not regenerate against
the v4 writer.

Graph: two `Person` nodes (Alix age=30, Gus age=25) and one `KNOWS` edge, built
with `from_graph_store_preserving_ids`.

| File | Version byte | Size | SHA-256 |
|------|--------------|------|---------|
| `compact_store_v1_small.bin` | 1 | 280 | `287857884fbb9c5401d77ec17b12d12c22575b508510f1f19fd3e610edd110f2` |
| `compact_store_v2_small.bin` | 2 | 304 | `12e4afb16e36172c46e59f41ee0a31c9d95234fafdca8e6a627021ff8ce917a0` |
| `compact_store_v3_small.bin` | 3 | 355 | `62227ca683fafa1d4b4f61368ba1730e568fb29f473502f0ab345268836f1d8a` |

E-0.1 readers must continue to decode these bytes. Round-tripping only through the
new writer is not a substitute for fixture decode tests.
