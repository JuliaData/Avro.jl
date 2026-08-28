# Codex review round 19

DRAFT v19 is not implementation-ready, big dawg. Eight major issues remain. No blocker was found.

I reviewed [DRAFT v19](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:3), [response-18.md](/Users/jacob.quinn/.julia/dev/Avro/reviews/response-18.md:1), and the [round-18 review](/Users/jacob.quinn/.julia/dev/Avro/reviews/codex-review-18.md:1) read-only.

## Disposition check (round-18 items)

### Majors

| # | Round-18 item | Status | Evidence |
|---:|---|---|---|
| 1 | Schema-object grammar | **PARTIALLY RESOLVED** | Lines 378–393 separate schema-object and field grammar and fix `doc`. Lines 379–381 still allow object-wrapped named references and imply an object-wrapped union. See finding 1. |
| 2 | Recursive record defaults | **PARTIALLY RESOLVED** | Lines 403–405 define the correct recursive rule. Lines 1239 and 1242–1244 still apply the ordinary “every field present” record rule to non-union defaults. |
| 3 | Scan work and user code | **RESOLVED** | Scan pushdown is deferred to 2.1 at lines 1449–1463 and 1941–1943. No guarded Scan evaluator ships in 2.0. |
| 4 | Scan rename semantics | **RESOLVED** | Renames are deferred. The 2.1 note requires Tables-valid names and provenance clearing at lines 1457–1463. |
| 5 | Record admission provenance | **PARTIALLY RESOLVED** | Line 836 introduces admission-carrying `Avro.Row`. Lines 1041–1043 still say `Rows` yields `Avro.Record`. The public surface at lines 1388–1399 omits `Avro.Row`; typed `Rows(...; T=...)` also lacks a Tables-row contract. |
| 6 | Fallback versus attempt bound | **RESOLVED** | Eviction, retry, cancellation, and fallback were removed at lines 1073–1085. The replacement design has separate memory and failure-work defects below. |
| 7 | Phase 4c/4d ordering | **RESOLVED** | Phase 4c now covers parallel decoding. Projection starts in Phase 4d at lines 1870–1871. |
| 8 | Latency-calibration ordering | **PARTIALLY RESOLVED** | Lines 660–668 and Decision 12 make Phase 2 provisional and finalization dependent on Phase 4a. Phase 2 still “fixes” the constants at line 1866; Phase 4a omits final calibration at line 1868; line 2106 again names only Phase 2. |
| 9 | Effective ceilings and portability | **PARTIALLY RESOLVED** | Lines 641–646 now require both effective ceilings to admit the peak. Lines 647–650 rely on recorded runtime maxima, while the open Julia/JLL compatibility ranges at lines 1806–1818 admit revisions an older writer cannot preflight. |

### Minors

| # | Round-18 item | Status | Evidence |
|---:|---|---|---|
| 1 | Map accounting wording | **PARTIALLY RESOLVED** | Lines 689–695 and 754–757 retain `npairs` capacity. Lines 565–567 still say an all-duplicate map is charged for one entry after construction. |
| 2 | Path-independent block output | **PARTIALLY RESOLVED** | Lines 1073–1077 add a logical block-output charge. This does not account for actual parallel-only chunk and worker allocations under `max_total_bytes`. See finding 2. |
| 3 | Budget channels | **PARTIALLY RESOLVED** | Lines 765–773 say `register!` and `lookup` take operation limits. Their interfaces at lines 1206–1208 and 1364 omit them. `juliatype` at line 1370 remains outside the budget list. |
| 4 | Comparison bounds | **RESOLVED** | Lines 587–602 define compared work, no-input denominators, and the `max_total_values` bound. |
| 5 | Fixed-decimal validation | **RESOLVED** | Lines 294–298 require checked, saturating, constant-space arithmetic. |
| 6 | Graph copying | **RESOLVED** | Lines 237–239 require a graph-wide copy memo. |
| 7 | Nullable `nothing` | **RESOLVED** | Line 835 sends both `missing` and `nothing` to the null branch. |
| 8 | Ordinary record JSON | **PARTIALLY RESOLVED** | Line 1239 ignores unknown fields and promises `unknown=:error`. The `fromjson` signatures at lines 1219–1220 and 1386 omit that keyword. |
| 9 | Map-key conversion | **RESOLVED** | Line 834 limits keys to `AbstractString` or `Symbol`, defines conversion, and rejects converted-key collisions. |
| 10 | Scan edge contracts | **RESOLVED** | The affected Scan surface is deferred in full. |
| 11 | Float-parser evidence | **RESOLVED** | Lines 361–369 require the midpoint, subnormal, overflow, underflow, long-token, and linear-work vectors. |
| 12 | Schema edge contracts | **RESOLVED** | Lines 268–275 define exact lexical equality for invalid defaults and reject duplicate constructor properties. |
| 13 | Store policy and internal `plan` | **RESOLVED** | Lines 1206–1213 scope ambiguity policy to the built-in cache. `plan` remains internal at lines 889–891. |
| 14 | Explicit enum interning | **RESOLVED** | Line 832 makes `Symbol(::EnumValue)` an explicit caller-owned action outside the ceiling. |
| 15 | Standalone `error` | **RESOLVED** | Lines 174, 268–275, and 1208–1211 define printing, PCF, equality, and cache ambiguity. |

### Nit

**PARTIALLY RESOLVED.** The principal summary defects were fixed. Phase 1 still claims identical acceptance of Java’s ignored-namespace cases at line 1865, despite the deliberate non-string rejection at lines 396–397. The review log also places Round 18 before Round 17 at lines 2258 and 2272.

## New findings

1. **[major] Schema-object `type` still accepts invalid forms.**

   **Claim:** The grammar permits an object-wrapped named reference and an object-wrapped union.

   **Evidence:** Lines 379–381 allow a schema-object `type` string to be a named-type reference and broadly call it a complex-type keyword. A read-only Java 1.12.2 probe rejected both `{"type":"E"}` after defining `E` and `{"type":"union"}` with `SchemaParseException`. Named references are schema strings. Unions are JSON arrays.

   **Recommendation:** Restrict object `type` to primitive names and `record`, `error`, `enum`, `array`, `map`, or `fixed`. Permit named references only as schema strings and unions only as arrays. Add positive and negative fixtures for every form.

2. **[major] Parallel-only physical memory can change acceptance.**

   **Claim:** The lowest parallel block does not follow the same physical memory rule as the direct sequential path.

   **Evidence:** Lines 1072–1077 allocate per-block chunk columns for the lowest parallel block. The sequential direct path has no such allocation. Lines 1088–1090 also create charged worker state. A logical `max_block_output_bytes` charge cannot create physical headroom under `max_total_bytes`. A near-ceiling input can therefore fit with `ntasks=1` and fail with `ntasks>1`.

   The higher-block `W` at lines 1079–1084 also does not explicitly cover every simultaneous category-(a)–(d) allocation, such as replacement overlap and map-sort scratch.

   **Recommendation:** Decode the lowest block directly into its final-column slice. Create workers only from actual headroom. Define `W` as the complete representation-specific peak, including chunk storage, transient scratch, old/new replacement overlap, and per-block state. Alternatively, use the same physical chunk pipeline for `ntasks=1`.

3. **[major] Parallel work cannot equal sequential work on failures.**

   **Claim:** A higher block can perform work that sequential decoding never reaches.

   **Evidence:** Higher blocks decode ahead at lines 1078–1085. A lower content or cumulative failure becomes authoritative later at lines 1091–1099. If block 1 fails after block 2 was decoded, sequential execution never processes block 2. This contradicts the unconditional equality claims at lines 1100–1106 and 1976–1980 and the counter gate at lines 1727–1735.

   **Recommendation:** Limit work equality to successful operations. For failures, define and test a bound based on the admitted higher blocks. If literal equality is required, do not start a higher block until all lower blocks pass content and cumulative checks.

4. **[major] Identity-bearing union values can select the wrong branch.**

   **Claim:** The branch-recovery algorithm is ambiguous for records, enums, and fixed values.

   **Evidence:** Line 835 first matches `typeof(value)`. Every generic record has type `Avro.Record`; every enum uses `Avro.EnumValue`; every fixed value uses `Avro.Fixed`. Several named branches can therefore have the same representation type. The algorithm can choose the first branch instead of the branch identified by the value’s carried schema. This contradicts the identity-bearing gate at lines 1718–1719 and can write the wrong union index.

   **Recommendation:** Before representation-type matching, match an identity-bearing value against its carried named schema identity. For fixed, include size. Then use the existing conversion and first-accepting rules.

## Non-blocking follow-ups

- **[minor]** Synchronize `register!`, `lookup`, `juliatype`, `fromjson(...; unknown=...)`, and `Rows(...; select=...)` with their recorded budgets and options.
- **[minor]** Define duplicate projection selectors and `select` with typed or non-record `Rows`.
- **[minor]** Preserve root fullname, aliases, doc, properties, `iserror`, repair flags, and graph limits in projected schemas. Recursive roots need an old-root-to-new-root substitution and one graph-wide memo.
- **[minor]** Add the `Avro.Row` shell and ownership transfer to the storage oracle.
- **[minor]** Correct the all-duplicate map wording so retained `npairs` capacity remains charged.
- **[minor]** Remove the obsolete Scan development pin, Scan precompile entry, filter-mask wording, and fast-mode offset/filter language from the 2.0 plan.
- **[minor]** Remove `FixedSchema.doc`, or state that it never captures fixed `doc`; fixed `doc` is arbitrary metadata.
- **[minor]** Define empty-enum behavior. Java accepts it. It should canonicalize and fingerprint, have no finite datum, and use `minsize = ∞`.
- **[minor]** Define validation and ownership for public `Record`, `Fixed`, `EnumValue`, `UnionValue`, `Map`, and `Row` constructors.
- **[minor]** Explicitly reject a missing required `avro.schema` header entry. State whether strict `eachblock` walks exactly `count` datums before yielding.
- **[minor]** State that decimal precision and scale matching is additional to ordinary underlying-kind, fullname, and fixed-size resolution.
- **[minor]** Add GMP/`BigInt` to the allocation exceptions. Reserve the complete limb and temporary peak before constructing `WideDecimal`.
- **[minor]** Define non-JSON-encodable StructUtils defaults and the TimeZones extension’s precision, UTC conversion, and zone-loss behavior.
- **[minor]** State that type-alias rewriting first uses exact normalized fullnames, before the normal unqualified-name match.
- **[minor]** Carry a Stage-1 structural failure as a pending indexed failure while earlier blocks run, so a lower content failure still wins.
- **[nit]** Qualify the ignored-namespace Phase 1 gate and restore chronological Round 17/Round 18 log order.

## Milestone and gate assessment

Phase 1 is blocked by the schema-object grammar and the conflicting recursive-default rules. Phase 2 is blocked by identity-bearing union recovery and the still-contradictory latency milestone. Phase 4b is blocked by the `Row` contract. Phase 4c is blocked by physical acceptance and failure-path work accounting. The portable invariant also needs a finite certified Julia/JLL/accounting matrix.

Assumptions made:

- Declared fixtures, implementations, and gates are deliverables.
- I did not reopen accepted interoperability deviations or 2.x deferrals unless v19 contradicted them.
- “Supported versions” must be finite and testable; an open compatibility range is not itself a certified accounting matrix.

Decisions made without user direction:

- I treated stale wording as minor only when a single authoritative contract remained executable.
- I kept contradictions that can change schema acceptance, wire branch selection, memory acceptance, or mandatory gates at major severity.
- I accepted the complete Scan deferral as resolving the two Scan majors.

Validation was read-only. I inspected the complete plan, response 18, public APIs, accounting rules, codecs, tests, milestones, and decisions. The package-review checklist guided the dependency, resource, API, and gate audit. Read-only Java 1.12.2 probes confirmed rejection of object-wrapped named references and object-wrapped unions, and acceptance of an empty enum. I created or modified no files.

Plan SHA-256: `246a37106d17bf8f398bb9bd1e195bde352af0fc9c66d014e527cd5d5663a879`. Response SHA-256: `d7513200fa7b1e8c3a739e24f37993f4074c07fba2f29f43aa50e7e4eff38cf7`.

## Verdict

VERDICT: REVISE
