# Response to codex-review-15 (round 15, exhaustive pass) — disposition of all items

The plan was revised in place to DRAFT v16. Every major finding and all 27 follow-ups were adopted.
Section references are to v16.

## New findings 1–5 (majors)

1. **JSON.jl surrogate corruption — Adopted (Avro-owned decoder).** §4.2: guarded JSON strings are decoded
   by an Avro-owned decoder from their raw spans into WTF-8 (a high surrogate immediately followed by a
   low one becomes the scalar; every other surrogate code unit keeps its own 3-byte sequence, so distinct
   code-unit sequences stay distinct; the printer re-escapes every surrogate sequence); JSON.jl is used
   only for structure and positions; decoding writes into reserved scratch; tests for lone high/low,
   high/high, low/low, low/high, valid pairs, separated surrogates and both demonstrated collision pairs
   in keys and values for every context (decision 47).
2. **Alias repair — Adopted.** §4.2: aliases are "any string" and match by exact WTF-8 bytes; under
   `allow_invalid_names=true` invalid writer names are admitted as bytes with `repaired_names` set in the
   graph's `GraphInfo`, so a reader alias repairs them; direct writer-invalid/reader-alias fixtures
   including a surrogate-bearing name; surrogate-bearing defaults under `allow_invalid_defaults=true`
   are retained with `valid=false` (decision 47).
3. **Dense identity and memo work — Adopted.** §4.2/§4.4: every node carries `id::FrozenRef{Int32}` and
   `graph::FrozenRef{GraphInfo}` (recorded limits, repair flags, counts) filled by `freeze!` on every
   creation path (parser, public constructors, `Avro.schema(T)`) — no side table; pair tables are
   sorted per-node partner vectors, binary-searched, with every inspected entry and insertion move
   charged to `max_resolution_work`; gates for parser-, constructor- and type-derived cyclic graphs and
   one node paired with many partners near the limit (decision 46).
4. **Writer/Reader invariant counterexamples — Adopted.** §4.4/§4.9: streamed `Table` materialisation
   is chunked (per-block exact chunks, one final exact assembly; deterministic 2× peak included in the
   writer preflight — no geometric growth); the `Writer` rejects schemas whose `GraphInfo` carries
   repair flags unless the matching `allow_invalid_names`/`allow_invalid_defaults=true` is passed; the
   guaranteed consumers are named (`Reader`/`Rows`/`Table` from bytes, mapped path, `mmap=false`,
   non-seekable `IO`) under identical `Limits`, the same codec availability and fresh or sufficient
   admission state; gates: one near-ceiling file through all four modes, repaired-schema rejection and
   opt-in, exhausted admission (decision 48).
5. **`Avro.write` schema precedence — Adopted.** §4.9/§5.1: explicit `schema=` → the effective retained
   schema of an Avro source (`Avro.schema(src)`; `Avro.writerschema(src)` exposes the file's writer
   schema) → `Tables.schema` derivation; gates for direct copies with enum/fixed/decimal/logical/general
   unions and for evolved copies (decision 49).

## Follow-ups (all 27 adopted)

Map capacity retained at `npairs` after compaction; probe reservation `sizeof(T) + 8 × fieldcount + 64`;
string copies listed in category (c); admission scan/sort calibrated with compared bytes and moves and
common-prefix names at flush/carry boundaries; comparison-work definition includes permutation/reference
moves and named-type table work; `minsize` yields `∞` for required recursive cycles and uses the exact
varint length of the branch index; the allocation hook is defined as test-mode assertions in the
package's own allocation wrappers with an enumerated native/dependency exception list; "logically
unreachable, reservation returned" replaces "physically freed"; snappy's whole-block cancellation
granularity stated; duplicate map keys keep the first occurrence's position with the last value; the
immutability contract covers public operations and frozen-container methods (`getfield` on backing is
outside it); do-block forms and a finalizer fallback for `Rows`/`Reader`; `decimal_byteorder` on
`Reader`; `Avro.register!`/`Avro.lookup` signatures; code-unit-exact metadata with retained raw spans;
ambiguous field aliases are `ResolutionError`s; duplicate header keys are `DataError`s; standalone
`{"type":"error"}` parses as an error record; `EnumValue`/`UnionValue` indices are 1-based with
`Avro.ordinal` (decision 50); branch-recovery fixtures with several named branches; default equality
with `isequal` semantics (signed zero, quoted non-finite); an ignored `namespace` must still be a JSON
string (Java's broader acceptance recorded); CI fuzzing bounded to a recorded sample with the full loop
under `AVRO_FUZZ_ITERATIONS`; the latency statement is a worst-density gate; `show` uses the recorded
parse limits like `==`; the RPC deferral checklist extended (effective error unions, framing, HTTP);
the review log is in ascending order.

No material objection to any round-15 item remains on Claude's side; DRAFT v16 is submitted for round 16.
