# Response to codex-review-12 (round 12) — disposition of all items

The plan was revised in place to DRAFT v13. Every finding and every follow-up was adopted. Section
references are to v13.

## Carried rows

* **R1-1 / R4-2 / R5-1 / R5-2 / R5-3 and the round-6…11 partial rows — Adopted.** Each remaining defect
  is one of findings 1–7 below; the mechanisms marked resolved are unchanged.

## New findings 1–12

1. **[major] Rebuilds vs exact map accounting — Adopted (no rebuilds).** §4.4 "Map-probe bound": the
   hash index is never rebuilt, re-seeded or grown; a key whose probe sequence exceeds 64 slots lives in
   a sorted `Int32` overflow index charged at its actual capacity (reserved before each growth, ≤
   `nunique` entries); all four parts of `Avro.Map` are charged at actual allocated capacity; the
   storage oracle includes forced-overflow maps (§4.6, §9.10, decision 35).
2. **[major] Seed-dependent acceptance / unraiseable `LimitError` — Adopted.** The overflow index makes
   construction and admission non-failing and deterministic in acceptance (the seed only decides which
   index a key lands in); memory is a function of the counts alone; there is no probe-related
   `LimitError`; the work bound is ≤ 64 probes + log2(nunique) + 1 comparisons per lookup, charged to the
   new comparison rule (§4.4).
3. **[major] `parseschema(::IO)` — Adopted.** §4.2 step 1: `IO` sources are read incrementally into an
   Avro-owned, step-wise reserved buffer that stops at `max_schema_bytes + 1` bytes (JSON.jl's
   `lazy(io)` is never used on an `IO`); tests on a non-seekable over-limit stream and both byte
   boundaries (§9.10, decision 41).
4. **[major] Invalid UTF-8 in JSON — Adopted.** §4.2 step 1: the pre-scan validates strict UTF-8 (no
   overlongs, surrogates or code points above U+10FFFF) and every escape (paired surrogates, no invalid
   escapes) before `JSON.lazy`, for schemas and datum JSON alike; `SchemaError`/`DataError` with negative
   cases for keys, names, props, defaults, datum strings, map keys and union labels (§9.10, decision 41).
5. **[major] Sort work unbounded — Adopted.** §4.4 "Comparison rule": `max_compare_bytes_per_byte = 64`
   (a raisable `Limits` field) bounds every key byte compared by duplicate-key detection and map
   construction/lookups to `64 × input bytes + allowance`; the `Budget` counts compared bytes; Phase 2's
   latency gate adds wide-object and long-common-prefix fixtures (decision 43).
6. **[major] `Avro.schema(x)` budget — Adopted.** §5.1: `Avro.schema(x; limits=Limits())`, sharing one
   budget with the type derivation it delegates to (decision 39).
7. **[major] User constructors on the fast route — Adopted (constructor-free construction).** §4.8: the
   fast route builds `NamedTuple`s directly and structs the way `Serialization.deserialize` does
   (`jl_new_struct_uninit` + field stores), so no user code runs inside the ceiling; documented like
   `deserialize`; regression test with an allocating, throwing positional constructor (decision 42).
8. **[minor] Decoded-key equality — Adopted.** §4.2: duplicates are detected over decoded keys (escapes
   unescaped into a charged scratch buffer; order and equality over decoded UTF-8 bytes); tests for
   simple/BMP escapes, surrogate pairs, mixed spellings.
9. **[minor] Codec-member work — Adopted.** §4.4: every codec member (compressed, empty or skippable)
   counts as one value in the work rule; dense-member latency case (§9.10).
10. **[minor] Source-span ownership — Adopted.** §4.2: `DefaultValue` keeps an owned immutable copy of the
    source text (category (e)); a test mutates the caller's buffer after parsing.
11. **[minor] Typed shells — Adopted.** §4.4: fast-route shells charge `sizeof(T)` plus approved payload;
    the oracle covers generated nested, padded, mutable, reference-bearing and nullable layouts.
12. **[minor] Stale summaries — Adopted.** Decision 30 lists five categories; §13 names both global-state
    exceptions; schema `==`/`hash`/`show` use a bounded deterministic visited-pair table and `Limits()`
    scratch space on already-admitted graphs (§4.2).

## Non-blocking follow-ups (all adopted)

* Decoded-key equality, codec-member work, owned spans, typed-shell oracle, summaries, and the budget
  treatment of schema `==`/`show` are all in v13. Deferrals unchanged.

No material objection to any round-12 finding remains on Claude's side; DRAFT v13 is submitted for round 13.
