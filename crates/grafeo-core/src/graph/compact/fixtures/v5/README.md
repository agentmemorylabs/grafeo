# v5 payload golden fixtures (G-EM0.5b D0.8)

Frozen eager reference bytes for the streaming bounded builder parity tests. Each
`<name>.v5.bin` is the output of the eager `generate_compact_store` +
`serialize_v5_with_string_order(Lexicographic)` path for one adversarial input,
committed so the parity tests never run the eager heap path as a runtime oracle.

Generated on branch `g-em0-5b` against the eager v5 writer. Regenerate only after
an **intentional** format change:

```
cargo test -p grafeo-core --features generation-streaming \
  generation_builder::tests::regenerate_v5_golden_fixtures -- --ignored
```

Then commit the updated `.bin` files and refresh the SHA-256s below.

| Fixture | Bytes | SHA-256 |
|---------|-------|---------|
| `simple.v5.bin` | 1772 | `cf5c94299f1d937c44f73a571a1e87aab4b8fc68b91251b6a3dab50af74a58d7` |
| `complex.v5.bin` | 2844 | `25c64eeec86c54a2535a997d82107ca53a33376481ed9eb18e3bef769d5e5ce9` |
| `sparse_ids.v5.bin` | 1636 | `73cbe9d00836045cafa07af6a8d16bd6b6a96c95abd7813124194539cba8eb31` |
| `duplicate_endpoints.v5.bin` | 1468 | `7f13b8e786ca82cb40102cd619cd3b6581a7b508a9ad05ae4a0308c1ff9ad622` |
| `self_loops.v5.bin` | 1692 | `9a7c3e62273390fa893837f6d5b0a92388d35b9afe365746de3106f20a90433d` |
| `high_cardinality_strings.v5.bin` | 7940 | `6d71596bd2cbff561ec76394129674c26e2aea86d90c5384b1efc3a2da1cc151` |
| `signed_ints.v5.bin` | 1644 | `d68f26f74e03c79b91925cd3ffb6cfa1ceb7a2c54ac6a3cb51d7a947c41835ea` |
| `vectors.v5.bin` | 1436 | `65244dec07d76ca4c52f21165166240a81c6bd7af9cf5e223172ec27fe9de350` |

## Adversarial domain coverage

- `simple` — single label, one string prop, one int prop.
- `complex` — multi-label, multi-prop (string/int/float/bool), multi-edge-type.
- `sparse_ids` — IDs ≥ 2^42 exercising the preserve-ID path.
- `duplicate_endpoints` — same src/dst, distinct edge IDs.
- `self_loops` — self-loop edges and a cross edge.
- `high_cardinality_strings` — 50 unique names + 7-category dict stress.
- `signed_ints` — negative ints (RawI64 codec).
- `vectors` — vector column codec.
