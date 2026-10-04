# Response to codex-review-17 (round 17) — disposition of all items

The plan was revised in place to DRAFT v18. Every finding and every follow-up was adopted. Section
references are to v18.

## New findings

1. **[major] Contextual attribute validation — Adopted.** §4.2: attributes are grammar only in the
   context where the specification defines them (records/enums/fixed names and aliases, record fields,
   enum symbols and string defaults, array/map item schemas, fixed size) and are metadata preserved
   verbatim elsewhere (`{"type":"int","name":123}`, `size:"x"` on an array); `logicalType`/`precision`/
   `scale` are always contextual metadata whose malformed annotations drop to the underlying schema and
   never reject it; per-context fixtures (decision 55).
2. **[major] Float token cap — Adopted (no cap).** §4.2: floats are parsed by linear-time, correctly
   rounded parsers over the whole token, bounded only by the document size (the 2,000-digit `1.000…`
   case parses as `1.0`).
3. **[major] `float` double-rounding — Adopted.** §4.2: separate correctly rounded `Float32` and
   `Float64` parsers; the `0x3f800001` vector is gated byte-for-byte against Java's `jsontofrag` for
   defaults and `fromjson` (decision 55).
4. **[major] `Tables.resolve` unbounded — Adopted (Avro-owned resolver).** §6/§4.4: binding is done by
   an Avro-owned resolver with `Tables.resolve`'s exact semantics (gated by equality against the pinned
   implementation over a generated selector × filter matrix), over a sorted column-name vector, counting
   selectors and filter nodes against the new `max_scan_nodes` (4,096) with checked expansion
   arithmetic and a reserved peak; the `Tables.resolve` exception is gone from the allocation list and
   from decision 54; rename results are validated as Avro names before decoding.
5. **[major] Invariant scope and portability — Adopted.** §4.4: the invariant is scoped to generic
   default consumption (`reader_schema=nothing`, no `T`, `instants=:exact`, `decimal_byteorder=:big`,
   `validate=:strict`, matching repair options) and made portable by conservative maxima (the largest
   storage constant recorded across supported Julia versions and the pinned codec libraries' reported
   requirements) with cross-read gates on every supported version (decision 57).
6. **[major] Parallel-only memory — Adopted (sequential fallback).** §4.9: when the lowest uncommitted
   block is denied while any parallel-only reservation is held, or the parallel-only overhead plus the
   first block cannot be admitted, the operation restarts on the sequential direct path (once; inside
   the ≤ 2 × statement); gate: a near-ceiling filtered scan whose global masks do not fit while the
   sequential execution does (decision 56).
7. **[major] Complete-work gate — Adopted.** §4.9/§9.11/§12: the ratio counters are attempts,
   decompressions, values, comparisons/moves and filter evaluations — the same list everywhere;
   assembly copies are parallel-only work excluded from the ratio with their own exact bound
   `assembly_bytes ≤ committed_bytes` (decision 56).
8. **[major] Phase ordering of the latency gate — Adopted.** §4.4: Phase 2 calibrates datum and schema
   shapes; Phase 4a adds the container shapes (32,768-empty-block file, dense codec members) once the
   container reader exists; together they fix the constants.
9. **[minor] Budget ownership — Adopted.** Public schema constructors take `limits=` (raw node
   constructors are private); `DatumWriter` calls precharge the retained `Encoder` capacity;
   `encodesingle`/`decodesingle`/`register!`/`lookup` are in the scope list (§4.4).
10. **[minor] Accounting clauses — Adopted.** `npairs` capacity everywhere (§4.6); decision 48 states the
    exact streamed peak; referenced payload counted once with both sets of reference slots (§4.4).
11. **[minor] Raw numbers and pre-scan — Adopted.** Raw number tokens compare and hash lexically (§4.2);
    the pre-scan allocates nothing and the reader rediscovers token ends.
12. **[minor] API and Scan synchronisation — Adopted.** `Avro.write` signature carries the repair
    options; `Table` removed from the typed-`T` list (§4.5); renames validated; the widening
    transformation of defaults, aliases, props and logical annotations is defined and provenance is
    cleared when no exact transformation exists (§6).
13. **[minor] Admission affordability — Adopted.** §4.4 no longer asserts "always affords"; the Phase 2
    gate establishes it (raising the allowance/multiplier or shrinking the recent-buffer step).
14. **[nit] Stale summaries — Adopted.** Phase 1 wording, decisions 41/54, Phase 4c counters, and the
    review-log order (ascending) are fixed.

No material objection to any round-17 item remains on Claude's side; DRAFT v18 is submitted for round 18.
