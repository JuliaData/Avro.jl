# Codex review round 18

I reviewed [DRAFT v18](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:3) and [response-17.md](/Users/jacob.quinn/.julia/dev/Avro/reviews/response-17.md:1) read-only.

## Disposition check (round-17 items)

| # | Round-17 item | Status | Evidence |
|---:|---|---|---|
| 1 | Contextual attribute validation | **PARTIALLY RESOLVED** | Logical annotations are now contextual (§4.2, lines 283–299). The grammar table at lines 375–385 still gives schema-object `type` the wrong grammar and treats `doc` as defined on fixed. See finding 1. |
| 2 | Floating-token cap | **RESOLVED** | Lines 359–369 remove the fixed cap and bound parsing by document size. |
| 3 | `Float32` double rounding | **RESOLVED** | Lines 361–366 require separate, correctly rounded `Float32` and `Float64` parsers and include the Java vector. |
| 4 | Unbounded `Tables.resolve` | **PARTIALLY RESOLVED** | Lines 1500–1510 replace it with an Avro-owned resolver. Scan work remains unbounded, and rename rejection contradicts exact Tables semantics. See findings 3–4. |
| 5 | Writer/Reader invariant scope and portability | **PARTIALLY RESOLVED** | Lines 628–637 correctly narrow consumer options and codec/admission state. The available-memory guard can still make identical configured limits reject the same output. See finding 9. |
| 6 | Parallel-only memory acceptance | **RESOLVED** | Lines 1093–1102 add sequential fallback when parallel-only memory prevents progress. |
| 7 | Complete-work gate | **PARTIALLY RESOLVED** | Lines 1117–1125 separate assembly and list the counters. A late fallback can still cause a third attempt and exceed the 2× bound. See finding 6. |
| 8 | Latency-gate ordering | **PARTIALLY RESOLVED** | Lines 646–653 split calibration between Phases 2 and 4a. Phase 2 still claims to finalize all constants at line 1968. See finding 8. |
| 9 | Construction and retained-object budgets | **RESOLVED** | Lines 750–759 cover constructors, SOE operations, and prepared writers; lines 1398–1403 expose constructor limits. |
| 10 | Accounting clauses | **PARTIALLY RESOLVED** | The authoritative formulas use `npairs` and count shared payload once (lines 621–627, 675–680, 741–742). Lines 553–559 and 1053–1055 still contradict those rules. |
| 11 | Raw JSON numbers and pre-scan | **RESOLVED** | Token endpoints are rediscovered without an endpoint vector (lines 321–326); raw-number identity is lexical (lines 367–369). |
| 12 | Public API and Scan synchronization | **PARTIALLY RESOLVED** | Write keywords and derived-schema rules are synchronized (lines 1222–1230, 1445, 1550–1559). Exact Scan equivalence still conflicts with rename validation. |
| 13 | Admission affordability | **RESOLVED** | Lines 570–577 make the constants provisional and require the boundary gate to establish affordability. |
| 14 | Listed stale summaries | **RESOLVED** | The specific JSON, Phase 4c, Decision 54, and review-log defects were corrected. New summary inconsistencies are listed below. |

## New findings

1. **[major] Schema-object attribute grammar is still incorrect.**

   **Claim:** The plan accepts invalid schema declarations and rejects valid fixed metadata.

   **Evidence:** Lines 375–380 allow `type` to be “string or schema” everywhere. The pinned specification requires a schema-object `type` to be a string type name. Only a record field’s `type` accepts an arbitrary schema. The same table defines `doc` on fixed, although fixed does not define `doc`; it is therefore arbitrary metadata. Java 1.12.2 rejects object- or array-valued schema-object `type` members and accepts numeric or object-valued fixed `doc`.

   **Recommendation:** Use separate grammar tables for schema objects and record fields. Require schema-object `type` to be a string. Treat fixed `doc` as unknown metadata. Add direct Java-backed fixtures.

2. **[major] Record-default validation rejects valid nested defaults.**

   **Claim:** Applying the ordinary record JSON rule to defaults rejects a nested record default that relies on its own field defaults.

   **Evidence:** Lines 1280 and 1283–1286 require every record field and prohibit unknown members for defaults. Java 1.12.2 accepts an outer default `{}` when the nested record field has default `1`. Java rejects it when the nested field has no default. Java and the pinned Python implementations also accept extra members in record defaults.

   **Recommendation:** Define a separate recursive default rule. Supply a missing nested field from its validated default. Reject it only when no default exists. Ignore unknown members semantically, while retaining the exact source span for re-emission. Gate parsing and reader-schema resolution.

3. **[major] `max_scan_nodes` does not bound Scan work or user-code execution.**

   **Claim:** The guarded Scan path still permits unsafe CPU amplification and arbitrary operations inside the ceiling.

   **Evidence:** `max_scan_nodes` counts syntax nodes only (lines 500–504). A default request can apply 4,096 selectors to 65,535 fields, or evaluate 4,096 predicate nodes for every row. Lines 1515–1520 push predicates through the Avro evaluator, while the pinned `Cmp` and `In` semantics invoke arbitrary `==`, ordering, and `in` methods. A `Set` hashes decoded strings; custom containers can allocate or execute arbitrary code. No absolute Scan-work counter exists.

   **Recommendation:** Add `max_scan_work`. Charge every selector/name match, regex effort, filter-node evaluation, and membership comparison. Restrict pushed predicates to a closed set of package-handled literals and containers. Build charged sorted membership indexes. Leave unsupported predicates in the post-transfer residual.

4. **[major] Scan cannot provide its stated exact resolver semantics.**

   **Claim:** The Avro resolver rejects rename results that the pinned Tables resolver accepts.

   **Evidence:** Lines 1500–1504 promise exact `Tables.resolve` semantics and equality against the pin. Lines 1509–1510 reject output names that are not valid Avro names. Tables accepts arbitrary `String` or `Symbol` renames, subject to duplicate-name rules. Lines 1561–1566 then require identical names and schemas.

   **Recommendation:** Accept Tables-valid renames. Clear Avro schema provenance when a name cannot be represented in an Avro record, so later writing requires an explicit schema. Alternatively, document a deliberate Scan deviation and narrow the equivalence gate.

5. **[major] Detached `Avro.Record` values lack symbol-admission provenance.**

   **Claim:** The recorded representation cannot implement both `Tables.AbstractRow` and caller-owned symbol admission.

   **Evidence:** Lines 673–674 and 821 define a record as only a schema reference plus `Vector{Any}`. Lines 1472–1484 require Tables names to pass the selected admission object and promise lazy interning. `decode` and `eachdatum` return detachable records, but `Reader` has no `names=` option. The record retains neither admitted symbols nor an admission object. `Tables.columnnames`, `propertynames`, and `NamedTuple(record)` therefore cannot honor the caller’s boundary.

   **Recommendation:** Retain and charge admission provenance or admitted names in a row wrapper, and override every Symbol-producing Tables path. Alternatively, remove `Tables.AbstractRow` from bare `Avro.Record` and provide a parent-aware row wrapper. Gate detached records under exhausted default and caller-owned admissions.

6. **[major] Sequential fallback breaks the two-attempt and 2× work contracts.**

   **Claim:** A late fallback can process one block three times.

   **Evidence:** Lines 1093–1099 restart from the beginning. Lines 1113–1128 separately allow a speculative attempt and one retry. A block can be evicted, retried and committed, followed by a later fallback that decodes it again. A filtered block can consequently be decompressed six times rather than the promised four.

   **Recommendation:** Reuse committed results when switching paths, or preserve a global attempt scope across fallback. Another option is to force fallback before any block receives its second attempt. Update Decisions 22, 34 and 56 and the forced-schedule gates.

7. **[major] Phase 4c requires Scan before Phase 4d implements it.**

   **Claim:** The Phase 4c acceptance gate is not executable in milestone order.

   **Evidence:** Phase 4c at line 1972 requires filter-evaluation counters, row-dependent filtered scans, and filtered fallback. Scan and its filter evaluator first enter scope in Phase 4d at line 1973.

   **Recommendation:** Move all filtered-Scan concurrency gates to Phase 4d, or move the necessary Scan implementation into Phase 4c.

8. **[major] The latency-calibration milestone remains contradictory.**

   **Claim:** Phase 2 must finalize constants before the container measurements exist.

   **Evidence:** Lines 646–653 say Phases 2 and 4a jointly fix the constants. Phase 2 at line 1968 still requires the latency gate to fix and record them. Phase 4a at line 1970 does not list container calibration. Decision 12 and the open risk at lines 2050–2054 and 2207 retain the Phase 2-only wording.

   **Recommendation:** Make Phase 2 values provisional. Add the 32,768-block and dense-codec-member measurements to Phase 4a. Finalize and record all constants only after that gate.

9. **[major] The available-memory guard invalidates the stated Writer/Reader invariant.**

   **Claim:** Identical configured `Limits` do not imply identical effective ceilings.

   **Evidence:** Lines 521–530 lower each operation’s ceiling from current available memory and explicitly place that guard outside the invariant. Lines 609–640 and Decision 12 still promise acceptance under identical configured limits. A writer can succeed with a 240 MiB peak on one host, while a reader with the same limits but less available memory fails before decoding.

   **Recommendation:** Require both effective ceilings to admit the writer-preflighted peak. Qualify cross-version portability by a finite certified Julia/JLL/accounting matrix. Keep low-memory failure as a separate safety guarantee instead of calling it identical-limits readability.

## Non-blocking follow-ups

1. **[minor] Accounting wording:** Lines 553–559 say an all-duplicate map retains one entry’s charge, but lines 675–680, 741–742, and 819 retain `npairs` capacity. Lines 1053–1055 can also double-count referenced payload. Use the authoritative formulas everywhere.

2. **[minor] Path-independent block output:** Lines 493 and 1072–1079 include parallel chunk shells in `max_block_output_bytes`, although direct sequential decoding has no such shells. Define one logical charge or require fallback when only parallel representation overhead trips the cap. Define `committed_bytes` precisely for the assembly bound.

3. **[minor] Budget channels:** Lines 750–759 list `register!`, `lookup`, `inspect`, and schema operations, but the APIs at lines 1247–1252 and 1406–1448 do not consistently expose or inherit a budget. State built-in-cache accounting, custom-store ownership, and budgets for `inspect` and `juliatype`.

4. **[minor] Comparison bounds:** Lines 584–587 assume at most `2^20` map candidates, but only the `Int32` bound is stated. Also define the comparison-rule denominator for constructed schemas that have no encoded input bytes.

5. **[minor] Fixed-decimal validation:** Lines 294–297 must use a checked, saturating constant-space comparison. They must not construct `2^(8n−1)` for a huge fixed size.

6. **[minor] Graph copying:** Lines 237–239 should require one graph-wide copy memo so the same frozen named child passed twice remains one definition plus references.

7. **[minor] Nullable unions:** Lines 812 and 888–890 admit `nothing` as null, but line 820 sends every bare value except `missing` to the non-null branch. State that both select null.

8. **[minor] Ordinary record JSON:** Line 1280 rejects unknown members, while Java accepts them when required fields exist. If strict rejection remains, record it as a deliberate deviation and qualify the negative-oracle gate at lines 1704–1706.

9. **[minor] Map-key conversion:** Line 819 accepts “string-convertible” keys without defining allowed conversions or collisions such as `:a` and `"a"`. Define conversion and duplicate encoded-key behavior.

10. **[minor] Scan edges:** Derived schemas retain aliases after rename without checking alias/name collisions (lines 1550–1556). Also define exact `max_scan_nodes` counting and reject cyclic mutable Scan graphs.

11. **[minor] Float-parser evidence:** Add midpoint, subnormal, overflow, underflow, huge-mantissa, and huge-exponent differential vectors, plus linear allocation/time checks.

12. **[minor] Schema edge contracts:** Define how public constructor `props` collisions are handled. Clarify that invalid defaults compare by exact lexical span; the phrase “raw JSON text bytes—not the JSON spelling” at lines 271–272 is contradictory.

13. **[minor] Schema stores and internal APIs:** Scope structural ambiguity safety to `SchemaCache` unless custom stores must implement it. Classify `Avro.plan` at lines 874–876 as internal or add it to the public API.

14. **[minor] Explicit interning:** Document `Symbol(::EnumValue)` as an explicit caller-owned interning action. State whether Julia’s permanent Symbol allocation is excluded from the operation ceiling but bounded by admission counts and bytes.

15. **[minor] Standalone `error`:** Define record-versus-error JSON printing, canonicalization, equality, and `SchemaCache` collision behavior while RPC remains deferred.

16. **[nit] Summary cleanup:** Line 2004 still calls the deterministic bound “CPU work”; Decision 34 omits comparison/move and filter counters; Decision 35 should say “no hash-table lookup of untrusted keys”; line 748 overstates that every corpus file decodes under defaults; Phase 1’s Java-metadata gate needs the recorded ignored-namespace deviation.

## Milestone and gate assessment

Phase 1 is blocked by findings 1–2. Phase 2 and Phase 4a are blocked by finding 8. Phase 4b needs findings 5 and 9. Phase 4c is blocked by findings 6–7. Phase 4d is blocked by findings 3–4.

I made these assumptions:

- Declared artifacts and gates remain implementation deliverables.
- I did not reopen the plan’s recorded interoperability deviations or deferred features.
- Scan pushdown remains inside the guarded path when it ships. Post-transfer residual code remains caller-owned.

I classified formula constants and editorial mismatches as minor unless they made a spec rule, safety promise, API guarantee, or phase gate impossible.

Validation was read-only. I inspected the full plan, response 17, the pinned Avro specification, the pinned Tables Scan source, public APIs, resource rules, codecs, and every milestone. I ran focused Java 1.12.2 and pinned Python probes for schema-object grammar and nested record defaults. The package-review checklist guided the dependency, API, security, and gate pass. Plan SHA-256: `3906afea6f2cbdb573316ba11b2c9f1f9f7d8e44bba90580dce564b36c84ca71`. Response SHA-256: `9df99b3d3107e6b4a14c4efaf51e5d7b3488bc3332f6e98fdf7f024e7c57ae21`. I created or modified no files.

## Verdict

VERDICT: REVISE
