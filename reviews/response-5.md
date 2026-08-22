# Response to codex-review-5 (round 5) — disposition of all items

The plan was revised in place to DRAFT v6. Every carried item and every new finding was adopted.
Section references are to v6.

## Carried items

* **R1-1 / R4-1 / R4-2 (limits, codec memory, work budget) — Adopted.** See new findings 1–4 below.
* **R4-10 (logical sort order) — Adopted.** See new finding 7.
* **R4-16 (option validation) — Adopted.** §4.4: the `Limits` constructor validates cross-field
  relations (`max_block_bytes + max_codec_memory + max_block_output_bytes ≤ max_total_bytes`,
  `max_block_output_bytes ≤ max_total_bytes`, `max_datum_bytes ≤ max_total_bytes`, `max_codec_memory ≥
  8 MiB`); the budget floor is raised to 1 GiB so the defaults satisfy the relations (64 + 128 + 256 MiB).
* **R4-19 (coverage) — Adopted.** Coverage is explicitly an informational report, not a gate (§9.14, §11).

## New findings 1–15

1. **[blocker] Default Writer output rejected by default Reader — Adopted.** §4.3/§4.4/§4.9: the writer
   enforces the **identical** work rule as the reader (values ≤ 64 × encoded bytes + allowance, per
   datum, per block, per operation) and flushes a block before the next datum would violate the block
   rule; a single datum that violates the rule is a `LimitError` on both sides. The invariant therefore
   holds by construction and is tested for null, empty-record, empty-fixed, all-null-field record,
   nested-empty-array and nested-all-null-record roots. `block_datums` is gone (the rule replaces it).
2. **[blocker] Parallel transient memory — Adopted.** §4.9: a weighted transient-memory semaphore
   (capacity `max_total_bytes`) from which every worker reserves its decompressed buffer, codec memory,
   chunk/offset estimate and, in stage 3, the assembly copy **before allocating**; waiting is ordinary
   blocking, never a `LimitError`; the constructor guarantees one block's worst case fits; semantic
   accounting stays separate and ordered; peak-RSS test with eight workers on 64 MiB blocks (§9.11).
3. **[major] Work rule amplification — Adopted.** §4.4: block framing (count/size varints + sync) is
   credited as input, the allowance is **one per operation** (never per block), so the 655 KiB file of
   32,768 empty blocks admits ≈ 43.1M datums instead of 2^31; ceilings lowered (`max_block_bytes`
   64 MiB, `max_block_count` 2^24, `max_blocks` 2^28, `max_block_output_bytes` 256 MiB).
4. **[major] Codec-memory contract — Adopted.** §4.4/§4.9: `max_codec_memory` is defined as a decoder
   window/dictionary cap (xz `memlimit`; zstd `windowLogMax`, with zstd's ≈ 128 KiB fixed context
   overhead documented as outside the cap); bzip2's ≤ 3.7 MiB decoder need yields the ≥ 8 MiB
   constructor floor; the conversion is the integer `Int32(63 - leading_zeros(UInt64(x)))`; the fixture
   description is corrected (window log 30 advertised; fails below 1 GiB caps, succeeds at ≥ 1 GiB;
   default 128 MiB = window log 27).
5. **[major] Global numeric-attribute validation — Adopted.** §3/§4.2 (decision 23): only `fixed.size`
   is schema syntax; `precision`/`scale` and unknown attributes are metadata, preserved verbatim and
   never validated as such; a recognised logical type is evaluated in context and malformed attributes
   drop the annotation (Java-compatible); the three probe schemas are accepted and listed as Phase 1
   gate cases.
6. **[major] `Int32` scale — Adopted.** `Avro.Decimal`/`Avro.WideDecimal` carry `scale::Int`; any `Int`
   precision/scale is representable; tests on both sides of `typemax(Int32)` (§4.6, §4.8).
7. **[major] Comparison APIs disagree — Adopted.** §4.12 (decision 18): `compare` compares the canonical
   encoding of native values; agreement with `comparebytes` is guaranteed for Avro.jl-produced encodings
   and tested as a property; non-canonical encodings (non-minimal decimal `00 00`, mixed-case UUID) are
   documented to compare as raw bytes in `comparebytes`, with vectors in both matrices.
8. **[major] Resolution work — Adopted.** §4.4/§4.7: `resolve(...; limits)` with `max_resolution_work`
   charging match attempts, memo entries and resolving-plan nodes; a wide-union limit case in §9.5.
9. **[major] Type-derived naming collisions — Adopted.** §4.8 (decision 24): parameter-aware names
   (`Box_Int64`, sanitised, hash-suffixed beyond 128 characters), collision of distinct Julia types on
   one fullname is an `ArgumentError`, and union members mapping to the same unnamed kind
   (`Union{String,Symbol}`) are an `ArgumentError`, never silently merged.
10. **[major] Typed Symbol interning — Adopted.** §4.5/§4.8/§6 (decision 19): `Symbol` targets decode
    through the operation's admission object (`names=` on `DatumReader`/`decode`/`decodesingle`/`Rows`/
    `Table`); encoding needs no admission; repeated-file tests (§9.10).
11. **[minor] Decimal scale on encode — Adopted.** Exact scale equality required (§4.3, §4.6).
12. **[minor] Scan count-based skipping with filters — Adopted.** §6: count-based block skipping only
    without a row-dependent filter; with a filter, qualifying rows are counted; strict mode validates
    every remaining block after the limit, fast mode stops decompressing; tests across block boundaries.
13. **[minor] Ignored namespace — Adopted.** §4.2: the fullname algorithm runs first; an ignored
    namespace is never validated; both Java-accepted cases are tests.
14. **[minor] Blanket Java-readability promise — Adopted.** §7/§8.5/§13: the promise is scoped to
    schemas both oracles accept, with the spec-over-oracle exceptions (colliding JSON labels, repaired
    invalid schemas) listed in the docs and tested separately.
15. **[nit] Principle wording — Adopted.** §4.1 principle 6 now names the skipped-string exception.

## Non-blocking follow-ups

* Coverage stays an informational report (§9.14, §11).
* Budget scopes stated (§4.4): container `Writer`/`Reader`/`Table`/`Rows`/`compare`/`tojson`/`fromjson`/
  `resolve` own one lifetime budget; each prepared call and one-shot `decode`/`encode` gets a fresh one.
* `DatumReader` is immutable and shareable (per-call state/budget); `DatumWriter` is single-owner;
  `writer(x)` returns a fresh owned buffer (§4.5, §4.14, §5.2).
* Quoted non-finite float defaults recorded as decision 25 (§4.11, §14).

No material objection to any round-5 finding remains on Claude's side; DRAFT v6 is submitted for round 6.
