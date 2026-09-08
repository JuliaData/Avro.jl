# Response to codex-review-14 (round 14) — disposition of all items

The plan was revised in place to DRAFT v15. Every finding and every follow-up was adopted. Section
references are to v15.

## Carried rows

* **R1-1 / R2-new-6 / R4-2 / R5-3 and the round-6…13 partial rows — Adopted.** Each remaining defect is
  one of findings 1–6 below; the mechanisms marked resolved are unchanged.

## New findings 1–6

1. **[major] Duplicate-heavy map construction — Adopted.** §4.4: the in-package merge sort runs over all
   `npairs` candidates; the construction peak is the `npairs` permutation plus `cld(npairs, 2)` scratch,
   both reserved before the sort; duplicate groups are resolved last-wins and the permutation compacted
   in place to `nunique`; `npairs ≤ typemax(Int32)` is checked; gates at odd sizes (23, 1001) and
   near-ceiling all-duplicate maps (§9.10).
2. **[major] Surrogate contexts — Adopted (uniform decoding, contextual validation).** §4.2: JSON strings
   are decoded uniformly into Julia `String`s that preserve lone surrogate escapes as surrogate byte
   sequences (JSON.jl's own representation; never U+FFFD), with duplicate detection, equality, sorting
   and freezing over those bytes and a printer that re-escapes them; the Unicode-scalar requirement is
   applied after the fullname algorithm to names and namespaces actually used, enum symbols, default
   strings, datum strings, map keys and union labels, while ignored namespaces, aliases, `doc`, custom
   property names/values and nested metadata keys are preserved verbatim; fixtures for each context
   incl. OCF headers (decision 44).
3. **[major] Pointer-incompatible strings — Adopted.** §4.2 step 1: only `String`, `SubString{String}`,
   `Vector{UInt8}`, `Mmap` vectors and unit-stride views are used in place; every other `AbstractString`
   or byte vector is copied into an Avro-owned reserved buffer before the pre-scan, for `parseschema`
   and `fromjson`; exact-boundary tests.
4. **[major] Incremental category-(e) tables — Adopted (dense node IDs).** §4.2/§4.4/§4.5/§4.7: every
   schema node gets a dense ID at parse; single-node keys use node-indexed arrays (plan memo, `PlanRef`
   targets); pair keys use per-node small vectors of partner IDs (equality visits, resolution memo,
   resolving-plan pairs) with every insertion and scan charged to `max_resolution_work`; the named-type
   table is a sorted vector bounded by `max_named_types` with charged moves; `==` uses the larger of the
   two schemas' recorded parse limits; adversarial reverse-ordered, cyclic and near-limit tests
   (decision 46).
5. **[major] Admission carry merges — Adopted (deamortised).** §4.4: merges advance by a fixed step of at
   most 2,048 moved entries per admission, lookups search old and partially built runs during a merge,
   staging is transactional, and maintenance is the table's own charged work; a one-symbol operation
   costs a fixed bound that the default allowance always affords; gates across every power-of-two
   boundary up to `max_names`.
6. **[major] Implicit vector growth — Adopted (growth rule).** §4.4 "Growth rule": no charged buffer is
   grown with `push!`/`resize!`/`sizehint!`; growth allocates a reserved exact-capacity replacement
   through package-owned builders (generic collections, streamed columns, the `Encoder`, owned JSON/IO
   buffers, compressor buffers), with the allocation-hook gate (§4.3, §4.9, decision 45).

## Non-blocking follow-ups (all adopted)

* `npairs ≤ typemax(Int32)` check; 4 KiB reserved before the per-`T` probe; `Avro.Table` has no `T` and
  admits only column names (§4.1, §6); `==`/`show` use the recorded parse limits of the admitted schemas;
  "probes"/"equal-hash" wording replaced; decisions 43/44 reordered.

No material objection to any round-14 finding remains on Claude's side; DRAFT v15 is submitted for round 15.
