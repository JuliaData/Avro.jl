# Codex review round 17

## Disposition check (round-16 items)

I verified [DRAFT v17](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:3) and [response-16.md](/Users/jacob.quinn/.julia/dev/Avro/reviews/response-16.md:1).

| Round-16 item | Status | Evidence |
|---|---|---|
| 1. JSON numeric-token blocker | **PARTIALLY RESOLVED** | The Avro-owned reader removes JSON.jl’s unbounded `BigInt`/`BigFloat` path (§4.2, lines 321–372). However, lines 358–361 add an unraiseable token limit and route `float` through `Float64`; see new findings 2–3. |
| 2. `Table` for every root | **RESOLVED** | `Reader`/`Rows` cover all roots. `Table` covers record roots only (§4.4, lines 612–619; §4.9, lines 998–1003). |
| 3. Streamed chunk-shell accounting | **PARTIALLY RESOLVED** | The exact `nblocks × ncolumns` peak is specified at lines 601–611 and 1024–1029. Decision 48 still calls it a deterministic “2× peak” at lines 2109–2112. |
| 4. Parallel-priority counterexample | **RESOLVED** | A denied non-lowest block releases its reservations and requeues (§4.9, lines 1055–1068). The exact forced schedule is recorded. |
| 5. Eviction inside a datum | **RESOLVED** | Permit waits are cancellable. Workers inspect cancellation during decompression, reservations, and bounded inner loops (lines 1069–1079). |
| 6. Scan allocation bypass | **PARTIALLY RESOLVED** | The filter evaluator and residual boundary are fixed at lines 1471–1476 and 1503–1507. `Tables.resolve` remains unbounded; see new finding 4. |
| 7. Scan schema provenance | **RESOLVED** | Selected, renamed, widened, and cleared provenance rules are at lines 1507–1513. |
| 8. Repaired schemas and PCF | **RESOLVED** | PCF-dependent and JSON APIs reject repaired names (§4.2, lines 396–401). |
| 9. Graph composition | **PARTIALLY RESOLVED** | Fresh-ID copying and creation gates exist at lines 234–239 and 1760–1761. Public-constructor budget ownership remains unspecified. |
| 10. Repair options in peripheral APIs | **PARTIALLY RESOLVED** | Operative text forwards both options at line 1186. The public `Avro.write` signature at line 1409 omits them. |
| 11. Schema semantic edges | **PARTIALLY RESOLVED** | Default equality, `iserror`, and alias rules are present at lines 260–281 and 817–824. The new attribute table conflicts with contextual metadata handling; see new finding 1. |
| 12. Legacy decimal framing | **RESOLVED** | Recovery is restricted to bytes decimals and declared-size-16 fixed decimals (§4.9, lines 1007–1015). |
| 13. Allocation and lifecycle boundaries | **PARTIALLY RESOLVED** | The allocation exceptions, Snappy path, and Writer cleanup exist at lines 1128–1135 and 1177–1185. A fresh prepared-`DatumWriter` budget does not precharge its retained encoder capacity. |

| Round-16 follow-up | Status | Evidence |
|---|---|---|
| 1. `npairs` capacity | **PARTIALLY RESOLVED** | Lines 651–656 and 717–718 correctly retain `npairs` capacity. Line 793 still says the permutation capacity is `nunique`. |
| 2. Owned metadata spans | **RESOLVED** | Lines 335–336. |
| 3. `Reader.decimal_byteorder` | **RESOLVED** | Lines 946–947 and 1406. |
| 4. Schema `show` limits | **RESOLVED** | Lines 1312–1313. |
| 5. Duplicate OCF metadata fixtures | **RESOLVED** | Lines 946–948 and 1760. |
| 6. Stale geometric streamed growth | **RESOLVED** | Streamed tables now use exact chunks and one assembly. |
| 7. Fixed fuzz sample | **RESOLVED** | Lines 1703–1711 and Phase 2. |
| 8. Dense-ID creation gates | **RESOLVED** | Lines 1760–1761. |
| 9. Complete-work counters | **PARTIALLY RESOLVED** | Lines 1086–1088 include all counters, but lines 1089–1090 immediately exclude assembly. Test and phase summaries name only decompression/value counters. |
| 10. Non-record `Rows` copying | **RESOLVED** | Lines 1193–1195 and 1449–1452. |

## New findings

1. **[major] Attribute validation is global instead of contextual.**

   **Claim:** Implementing lines 358 and 369–372 literally rejects valid metadata and invalid logical annotations that must fall back to their underlying schema.

   **Evidence:** Lines 283–299 correctly say `logicalType`, `precision`, and `scale` are contextual metadata. Lines 369–372 instead require those attributes to have fixed global types. They also treat keys such as `name`, `fields`, and `size` as syntax even on schema forms where they are undefined metadata. Java probes accept `{"type":"int","name":123}`, an array with `size:"x"`, and a record with `precision:"x"`; malformed decimal attributes are ignored.

   **Recommendation:** Define recognized attributes per schema and field context. Validate an attribute only when it is grammar in that context. Preserve all other keys as properties. Invalid logical attributes must remove the annotation, not reject the schema. Make enum defaults explicitly string-valued.

2. **[major] The 1,024-byte floating-token cap is not a usable limit.**

   **Claim:** A valid float or double token longer than 1,024 bytes is reported as malformed and cannot be admitted by raising `Limits`.

   **Evidence:** Lines 360–361 impose the fixed cap. `Limits` at lines 470–504 has no corresponding field. The public JSON table says float/double accept JSON numbers. Fastavro 1.12.2 accepted `1.` followed by 2,000 zeroes as exactly `1.0`.

   **Recommendation:** Parse all tokens admitted by `max_schema_bytes` or `max_datum_bytes` with a linear bounded parser. Alternatively add `max_json_number_bytes`, raise `LimitError`, and permit explicit increases. Gate defaults and `fromjson` below, at, and above the limit.

3. **[major] Parsing Avro `float` through `Float64` double-rounds valid values.**

   **Claim:** The common `Float64` path in lines 358–361 can produce the wrong `Float32`.

   **Evidence:** For this token:

   ```text
   1.000000059604644775390625827180612553027674871408692069962853565812110900878906
   ```

   read-only Julia 1.12 probes produced:

   ```text
   parse(Float32, token)             == 0x3f800001
   Float32(parse(Float64, token))    == 0x3f800000
   ```

   Apache `jsontofrag` emitted `01 00 80 3f`, which is `0x3f800001`.

   **Recommendation:** Use separate correctly rounded `Float32` and `Float64` parsers. Add this exact vector for schema defaults and `fromjson`, with byte comparison against Java.

4. **[major] `Tables.resolve` is not column-count bounded.**

   **Claim:** The exception at lines 1477–1479 can allocate and perform work far beyond the declared ceiling.

   **Evidence:** The [pinned `scan.jl`](/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/tables-scan/src/scan.jl:454) uses growing vectors and `Set`s. Each repeated regex selection expands over every column before duplicate-output rejection. Allocation scales with selectors × columns, not only columns. Filter-tree size and depth are also unbounded. This also hashes untrusted field-name symbols despite Decision 35.

   **Recommendation:** Implement an Avro-owned resolver, or add scan-node, depth, expanded-column, and work limits. Compute the expansion with checked arithmetic and reserve a proven peak before dependency allocation. Gate repeated regex selectors and deep filter trees.

5. **[major] The Writer/Reader invariant is underqualified and not portable as stated.**

   **Claim:** Identical `Limits` do not by themselves guarantee that every named consumer accepts Writer output.

   **Evidence:** Lines 594–622 do not restrict `reader_schema`, typed `T`, or `instants`. Public consumers expose them at lines 1401–1406. An incompatible reader schema must fail, and `instants=:datetime` can reject an otherwise valid out-of-range timestamp. Storage charges are runtime-measured at lines 643–668, while codec requirements are library-reported at lines 954–967. A near-ceiling file can therefore cross the limit on another supported Julia or codec-library revision.

   **Recommendation:** Scope the invariant to generic default consumption: `reader_schema=nothing`, no typed target, `instants=:exact`, standard byte order, and matching repair options. For cross-version portability, use stable conservative maxima or require the same accounting and codec-estimator revision. Add supported-version cross-read gates if portability is intended.

6. **[major] Parallel-only memory can still change acceptance.**

   **Claim:** Self-eviction does not remove memory that exists only in the parallel algorithm.

   **Evidence:** Lines 1024–1054 add worker state, chunks, and assembly structures. A parallel filtered scan retains all masks globally at lines 1480–1490, while the sequential path uses per-block materialization at lines 1491–1492. These allocations cannot be evicted. Near the ceiling, `ntasks=1` can fit while `ntasks>1` fails, contrary to lines 1067–1068.

   **Recommendation:** Fall back to the sequential direct path whenever parallel-only overhead prevents progress. Alternatively use a common reference pipeline with the same fixed costs. Gate a near-ceiling filtered scan whose global masks do not fit but whose sequential execution does.

7. **[major] The complete-work gate is internally unexecutable.**

   **Claim:** Assembly work is both included in and excluded from the deterministic ratio.

   **Evidence:** Lines 1087–1088 require assembly-copy work to be no more than twice sequential. Lines 1089–1090 say assembly is not part of that bound. A sequential mapped decode can write directly into final columns and have an assembly counter of zero. Any parallel assembly then fails `parallel ≤ 2 × 0`. Lines 1792–1793 and Phase 4c name only attempts, decompression, and values.

   **Recommendation:** Either use the same assembly pipeline for the sequential reference or exclude parallel-only assembly from the ratio. Give assembly a separate exact byte bound. Make the operative rule, tests, and Phase 4c list the same counters.

8. **[major] Phase 2 requires a container benchmark before the container reader exists.**

   **Claim:** The phase ordering makes the full latency gate impossible.

   **Evidence:** The latency suite at lines 624–630 includes a 32,768-block OCF file. Phase 2 must run the full gate and fix the constants at line 1921. Container framing and reading are not implemented until Phase 4a at line 1923.

   **Recommendation:** Calibrate datum and schema work in Phase 2. Move the OCF block-density calibration to Phase 4a, or implement the required bounded container walker before Phase 2 finishes.

9. **[minor] Construction and retained-object budget ownership remains incomplete.**

   **Claim:** Some persistent allocations have no explicit budget transition.

   **Evidence:** A “public constructor” can deep-copy a frozen graph at lines 237–239, but no budget-bearing constructor API is listed. Prepared `DatumWriter` calls get fresh budgets at lines 729–732 while retaining the encoder described at lines 758–762. Single-object/cache operations are also absent from the authoritative scope list.

   **Recommendation:** Make raw schema-node constructors private or give them `limits=`. Precharge retained encoder capacity on every call. Add single-object operations to the scope list or state their enclosing budget.

10. **[minor] Several accounting clauses still disagree.**

    **Claim:** Stale text can cause conservative rejection or the wrong implementation formula.

    **Evidence:** Line 793 says map permutation capacity is `nunique`, while the authoritative rule says `npairs`. Decision 48 says “2× peak,” while lines 601–611 give the exact chunk formula. The streamed peak names both chunk and final referenced payload even though payload ownership moves rather than duplicates.

    **Recommendation:** Use `npairs` capacity everywhere. Replace “2×” with the exact formula. Count referenced payload once and count both sets of reference slots.

11. **[minor] Raw JSON-number and pre-scan semantics are incomplete.**

    **Claim:** Equality, hashing, and endpoint ownership are undefined.

    **Evidence:** Raw metadata numbers are stored at lines 362–364, while lines 268–281 only say properties compare as JSON values. The plan does not define whether `1`, `1.0`, `1e0`, `-0`, and `0` are equal. Line 321 says the pre-scan allocates nothing, but line 325 says it records every token endpoint.

    **Recommendation:** Prefer lexical-byte equality and hashing for raw tokens. Either charge an endpoint vector or state that recursive descent rediscovers token ends.

12. **[minor] Public API and Scan edge policies need synchronization.**

    **Claim:** Operative behavior is not fully reflected in the API contract.

    **Evidence:** The `Avro.write` signature at line 1409 omits both repair options. Line 748 still lists `Table` as a typed-`T` path although `Table` has no `T`. Scan renames are not explicitly checked against Avro name grammar, and the plan does not say which defaults, aliases, properties, or logical annotations survive a direct widening.

    **Recommendation:** Synchronize the signatures. Validate rename results before decoding. Define schema transformation for nullable and logical columns; clear provenance when no exact Avro transformation exists.

13. **[minor] The current admission assertion depends on constants that do not yet satisfy it.**

    **Claim:** Lines 555–557 say one-symbol admission always fits the default comparison allowance, but the current provisional constants do not prove that for 1,024 long-common-prefix names.

    **Evidence:** The current allowance is 65,536 and the multiplier is 64. A recent-buffer scan can compare roughly one million key bytes for one approximately 1 KiB admitted name.

    **Recommendation:** Let the Phase 2 gate increase the allowance/multiplier or reduce the recent-buffer step. Do not retain the unconditional “always affords” statement until the measured constants establish it.

14. **[nit] Stale summary text remains.**

    **Claim:** Several summaries no longer match the operative design.

    **Evidence:** Phase 1 still says “lazy traversal”; Decision 41 refers to a future JSON.jl parser call; Decision 35 says no guarded hashing despite the current `Tables.resolve` exception; Phase 4c omits complete-work counters; Round 16 appears before Round 15 in the review log.

    **Recommendation:** Run a final terminology and decision-log consistency pass.

## Non-blocking follow-ups (if any)

Findings 9–14 are non-blocking implementation follow-ups. They do not change the verdict by themselves. Resolve them while correcting the major contracts, so the next review can assess one coherent text.

## Milestone and gate assessment

Phase 1 is blocked by contextual-attribute finding 1. Phase 2 is blocked by findings 2, 3, and 8. Phase 4b’s invariant needs finding 5. Phase 4c cannot satisfy acceptance and work gates until findings 6–7 are fixed. Phase 4d remains blocked by finding 4.

I kept the previously accepted interoperability deviations and deferrals in scope without reopening them. I treated formula constants and stale summaries as minor unless they made a safety or acceptance gate impossible.

Validation was read-only. I inspected DRAFT v17, response 16, the pinned Avro specification, the pinned Tables Scan implementation, the public APIs, dependencies, and every phase gate. I verified the `Float32` rounding vector on Julia 1.12 and Java 1.12.2, and the long numeric token with fastavro 1.12.2. The package-review checklist drove the API, dependency, security, and milestone pass. I created or modified no files.

## Verdict

VERDICT: REVISE
