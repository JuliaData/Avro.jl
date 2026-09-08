# Response to codex-review-13 (round 13) — disposition of all items

The plan was revised in place to DRAFT v14. Every finding and every follow-up was adopted. Section
references are to v14. The recurring source of findings — randomness and growth inside the guarded
path — was removed outright rather than patched: nothing in the guarded path hashes untrusted keys any
more.

## Carried rows

* **R1-1 / R2-new-6 / R4-2 / R5-1 / R5-3 and the round-6…12 partial rows — Adopted.** Each remaining
  defect is one of findings 1–6 below; the mechanisms marked resolved are unchanged.

## New findings 1–11

1. **[major] Seed-dependent overflow memory and comparison work — Adopted (hashing removed).** §4.4
   "Map structure": `Avro.Map` is a sorted-permutation map built once by a deterministic merge sort
   (scratch reserved before the sort); `getindex` is a binary search with a fixed per-call bound; there
   are no seeds, probe limits, overflow indexes, rebuilds or growth, so memory and work depend only on
   counts and key bytes and acceptance cannot depend on anything random; the writer's preflight charges
   the reader's deterministic comparison work for every map it writes, so the comparison rule is part of
   the identical-limits invariant (§4.4 invariant; decision 35).
2. **[major] Admission-table contradictions and O(n²) movement — Adopted (separate structure).** §4.4:
   the symbol-admission table is a sorted-runs (log-structured) structure — a 1,024-entry recent buffer
   scanned linearly, sorted and merged with equal-size runs when full — giving O(log N) amortised moves
   per insert and ≤ log2(N ÷ 1024) + 1 binary searches per lookup, every move and comparison charged
   to the admitting operation's comparison rule (a moved entry counts as 4 bytes) and every run/merge
   scratch reserved before allocation; one-million-admission latency test and capacity-boundary tests
   (§6, §9.10).
3. **[major] Immutable struct construction — Adopted.** §4.8: structs, mutable and immutable alike, are
   built with `Expr(:new, T, fields...)` in a function generated for the compile-time type `T` (the form
   a default constructor lowers to; no temporary vector, no boxing; consistent with the trim rule), so
   no user constructor runs; every field is always provided, so no partially initialised object exists;
   tests over mutable, immutable, zero-field, isbits and reference-bearing layouts on every supported
   version (decision 42).
4. **[major] `sizeof(T)` undercharges heap objects — Adopted.** §4.4: the typed shell of `T` is measured
   once per plan from a probe instance built by the same constructor-free builder with zero/empty
   field values (`Base.summarysize` minus referenced payload), exact for mutable heap objects (8-byte
   minimum with no fields), inline immutable non-isbits structs, and isbits layouts; the oracle includes
   zero-field mutable structs and arrays of them.
5. **[major] Stale map gate — Adopted.** §9.10 now gates adversarial-key construction (no rebuild, no
   error, last-wins, memory equal to the count-based formula, compared bytes within the comparison
   rule), the admission latency/capacity tests, and identical acceptance of near-ceiling maps across
   `ntasks` and repeated runs.
6. **[major] Lone surrogates in metadata — Adopted (contextual).** §4.2: escape *syntax* is validated
   lexically; the Unicode-scalar requirement applies to Avro string contexts (names, namespaces,
   aliases, enum symbols, default strings, datum strings, map keys, union labels), while `doc` and
   custom properties preserve unpaired surrogate escapes verbatim as owned raw JSON text (U+FFFD on
   materialisation, documented; fingerprints unaffected because canonical form strips metadata); schema
   and OCF fixtures with lone-surrogate metadata (decision 44).
7. **[minor] Comparison budget after transfer — Adopted.** Returned maps retain no budget; `getindex`
   has a fixed per-call bound; only construction is charged (§4.4, §4.6).
8. **[minor] Non-contiguous sources — Adopted.** §4.2: non-contiguous byte sources are copied into an
   Avro-owned reserved buffer before `JSON.lazy`; same rule for `fromjson`; stepped-view and
   custom-vector tests.
9. **[minor] Skippable-frame wording — Adopted.** §4.9: no output or codec-workspace charge, one
   member/value charge.
10. **[minor] Stale summaries — Adopted.** `schema(x)` is in the budget-scope list; §4.11 says "sorted
    decoded-key".
11. **[minor] Fixture and latency summaries — Adopted.** §13 says "all in-scope vendored Apache
    fixtures"; `max_compare_bytes_per_byte` is a provisional constant recorded by the Phase 2 latency
    gate (§4.4).

## Non-blocking follow-ups (all adopted)

* All five minor items are in v14. Deferrals unchanged.

No material objection to any round-13 finding remains on Claude's side; DRAFT v14 is submitted for round 14.
