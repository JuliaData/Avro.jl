# Codex review round 22

DRAFT v22 still needs one major revision, big dawg. The remaining major is an unexecutable parallel failure-work gate.

## Disposition check (round-21 items)

### Majors

| # | Round-21 item | Status | Evidence |
|---:|---|---|---|
| 1 | `instants=:datetime` versus closed set `E` | **RESOLVED** | The option is removed. Exact timestamp wrappers remain outside `DateTime` at lines 835–871. Public APIs at lines 1467–1489 expose no `instants=`. |
| 2 | Typed `T` versus `instants=` | **RESOLVED** | Typed plans follow caller-supplied `T` at lines 805–810 and 866–870. |
| 3 | Parallel failure-work arithmetic | **PARTIALLY RESOLVED** | Lines 1161–1168 now distinguish head failure (`inflight`) from higher failure (`inflight−1`). However, the non-waiting head schedule at lines 1108–1116 permits one extra direct block during a higher-worker failure. See finding 1. |

### Minors

| # | Round-21 item | Status | Evidence |
|---:|---|---|---|
| 1 | Guard state and lifecycle | **RESOLVED** | Best-effort wording and the third global-state exception are at lines 195–213 and 535–559. |
| 2 | `Rows` modes and retained schemas | **RESOLVED** | Lines 1257–1266, 1450–1453, 1487–1488, and 1532–1539 distinguish all three modes. |
| 3 | Direct-head terminology | **PARTIALLY RESOLVED** | Role and worker counts are clear at lines 531 and 1108–1139, but “never waiting” conflicts with the higher-failure gate. |
| 4 | Committed-memory accounting | **RESOLVED** | `committed_payload`, capacity, and `committed_bytes` are disjoint at lines 1118–1125 and 1158–1161. |
| 5 | `max_block_output_bytes` formula | **RESOLVED** | One generic, consumer-independent estimate is defined at lines 1212–1218. |
| 6 | `Avro.Row` storage oracle | **RESOLVED** | Lines 724–730 exclude the already-accounted record and admission object. |
| 7 | Default `W` and CPU wording | **PARTIALLY RESOLVED** | Terms and CPU scope are correct at lines 1166–1172. The stated Phase 4a measurement precedes the concrete chunk and worker implementations; see finding 5. |
| 8 | Comparison calibration | **RESOLVED** | Lines 603–619 count comparisons and moves and allow a raisable resource rejection. |
| 9 | Security wording | **RESOLVED** | Lines 1988–1992 now prohibit unchecked or unreserved allocation. |
| 10 | Public API surface | **PARTIALLY RESOLVED** | Limits, cache, errors, and constructors appear at lines 1433–1452. Constructor defaults and accepted logical/recursive inputs remain incomplete; see finding 6. |
| 11 | Structural `props` collisions | **RESOLVED** for the original issue | Lines 272–280 and 1441 reject structural collisions. Context-specific scope still needs clarification; see finding 6. |
| 12 | `UnionValue` ownership | **RESOLVED** | Line 1452 defines a non-copying wrapper and limits construction validation to `index ≥ 1`. |
| 13 | Secondary time contracts | **RESOLVED** for the requested change | Float behavior and `schema(ZonedDateTime)` are defined at lines 365–375 and 862. Typed `ZonedDateTime` decode remains unclear; see finding 7. |
| 14 | Container boundaries | **RESOLVED** | Codec-name UTF-8 and compressed-size enforcement are at lines 1016–1020 and 1219–1222. |
| 15 | Metadata API | **PARTIALLY RESOLVED** | Abstract byte vectors are copied at lines 1201–1203, but the detailed default expression is invalid Julia; see finding 10. |
| 16 | Dense-ID wording | **RESOLVED** | `freeze!` assigns IDs on every creation path at lines 235–240 and 746–755. |
| 17 | Recursive-root projection | **RESOLVED** | Direct and mutual recursion are covered at lines 1545–1559. |

### Nits

| # | Round-21 item | Status | Evidence |
|---:|---|---|---|
| 1 | Deferred-Scan residue | **PARTIALLY RESOLVED** | The 2.0 mechanism is gone, but RC-ready and risk text still mention the future pin/release at lines 1972–1975 and 2190. |
| 2 | Executable-document cleanup | **PARTIALLY RESOLVED** | Line 2461 saves `first_row`, but line 2470 still uses loop-local `row`. |

## New findings

1. **[major] The non-waiting direct head violates the mandatory higher-failure bound.**

   **Claim:** A valid two-task schedule performs one more speculative block than the permitted `inflight−1`.

   **Evidence:** The head must decode the next block not assigned to a worker and “never wait” at lines 1108–1116. With `ntasks=2`, the head decodes block 0 while the sole worker decodes block 1. If the head finishes first, it can start block 2. If block 1 then fails, sequential decoding stops at block 1, but block 2 is surplus. Here `inflight=1`, while lines 1161–1165, 1826–1829, Phase 4c, and decision 22 permit zero surplus blocks.

   **Recommendation:** Use admission-wave barriers. After its direct block, the head must not start beyond an unresolved lower worker. It may wait for and commit that fully reserved worker safely. Alternatively, reserve and count one speculative head block and change every bound and gate accordingly.

2. **[minor] Writer preflight does not name every reader transient peak.**

   **Claim:** The enumerated “complete peak” omits map-sort scratch, exact-replacement overlap, and WTF-8 scratch.

   **Evidence:** Lines 644–655 list reader buffers, output, and streamed chunks. The missing terms are explicitly part of parallel `W` at lines 1123–1125 and category accounting at lines 709–715.

   **Recommendation:** Define one `reader_block_peak` function. Use it for Writer preflight, sequential decoding, and parallel `W`.

3. **[minor] Parallel admission omits persistent base charges and final table shells.**

   **Claim:** The admission equation does not name live header, schema, plan, block-table, coordinator, or final Tables-interface shells.

   **Evidence:** Lines 1119–1125 include payload, capacity, sequential reserve, workers, and `W`. Categories at lines 696–760 and the stored `Tables.Schema` contract at lines 1512–1518 require additional live objects.

   **Recommendation:** Charge `W` atomically against the Budget’s complete live total. Add oracle terms for the `Avro.Table` wrapper, column-reference container, stored schema, metadata/partition state, and row count.

4. **[minor] Parallel and RSS lifecycle gates are incomplete.**

   **Claim:** The RSS baseline can include early decode allocation, and interruption cleanup is not forced.

   **Evidence:** The child starts decoding immediately after its start line; the first 10 ms parent sample becomes the baseline at lines 1181–1187. Lines 545–547 promise cleanup on interruption, but Phase 4c does not interrupt a worker holding `W` or chunk references.

   **Recommendation:** Require a parent acknowledgement before decoding starts. Force interruption during decompression and assembly. Assert task joining, one-time reservation restoration, no post-return writes, and propagation of the original interruption.

5. **[minor] Concrete `W` is finalized before its representations exist.**

   **Claim:** Phase 4a cannot measure the complete chunk and worker peak described by `W`.

   **Evidence:** Lines 1169–1172 assign measurement to Phase 4a. Tables and chunks arrive in Phase 4b at line 1960; worker state arrives in Phase 4c at line 1961.

   **Recommendation:** Finalize codec/value constants in Phase 4a. Finalize concrete `W` after Phase 4b and before the Phase 4c gate, or require the final calculator and representation types in Phase 4a.

6. **[minor] The public schema-construction contract still requires invented policy.**

   **Claim:** It does not define recursive construction, logical annotation values, or explicit-null defaults completely.

   **Evidence:** Public constructors freeze immediately and accept constructed child schemas at lines 235–253 and 1436–1441; only parsing has register-before-fill recursion at lines 417–419. `logical=` accepts an unspecified value, while both a logical annotation and a datum are called `Decimal` at lines 294–305, 838, and 858. Internal absence is `nothing`, but `Field` also uses `default=nothing` at lines 306–316 and 1439. The blanket collision rule also conflicts with context-inapplicable attributes being metadata at lines 397–398.

   **Recommendation:** Either scope direct constructors to acyclic graphs or add a builder/reference API. Expose distinct logical annotation objects. Add a no-default sentinel. Reject only properties structurally emitted by that specific constructor. Publish mechanically complete signatures with `limits=Limits()`.

7. **[minor] Typed native conversions need complete rules.**

   **Claim:** Typed `ZonedDateTime`, narrow integer, `Float16`, and `Char` targets have unspecified conversions or failures.

   **Evidence:** Lines 805–810 promise typed construction. Line 862 says generic timestamp decode returns `Timestamp` and timezone reconstruction is explicit. Lines 919–930 add narrow and character mappings without corresponding decode rules.

   **Recommendation:** Reject typed `ZonedDateTime` or define an explicit timezone policy. Specify range, rounding, and string-length checks for every narrow target. Use `ConversionError` consistently.

8. **[minor] Partial writer-union resolution needs an explicit deferred-error gate.**

   **Claim:** Plan construction must not reject an entire writer union because one branch cannot resolve.

   **Evidence:** Lines 891–915 describe union-remap nodes but not the unmatched-branch sentinel. Resolution applies when the writer selects a branch.

   **Recommendation:** Gate writer `["int","string"]` to reader `"long"`: an integer datum succeeds; a string datum raises `ResolutionError`.

9. **[minor] Parser gates need three small completeness additions.**

   **Claim:** Reserved primitive names, complete JSON document syntax, and contextual property collisions are not gated precisely.

   **Evidence:** Line 407 says primitive names are never redefined, but a namespaced `a.int` is valid; only null-namespace primitive fullnames are reserved. Lines 327–401 do not explicitly name raw controls, BOM, malformed literals, invalid whitespace, trailing commas, and trailing content.

   **Recommendation:** Add both primitive-fullname cases and the missing RFC 8259 negatives. Apply property-collision rejection per schema context.

10. **[minor] The detailed Writer metadata default is not executable Julia.**

   **Claim:** `Dict{String,<:AbstractVector{UInt8}}()` cannot be constructed.

   **Evidence:** It appears at line 1201, while the public synopsis uses `Dict()` at line 1490. A read-only Julia 1.12 probe produced `MethodError`.

   **Recommendation:** Use `Dict{String,Vector{UInt8}}()` as the empty default while continuing to accept and copy abstract byte vectors.

11. **[minor] Nested cache equality needs the caller’s budget.**

   **Claim:** `SchemaCache.register!` can otherwise invoke public schema equality under the schemas’ larger recorded limits instead of its caller’s shared operation budget.

   **Evidence:** Public equality selects GraphInfo limits at lines 281–285. Cache collision checks use structural equality at lines 1284–1287. Nested operations must share one Budget at lines 785–795.

   **Recommendation:** Add an internal equality routine that accepts the caller’s Budget. Keep graph-selected limits only for direct public `==`.

12. **[nit] Fast-validation wording places compressed corruption at the wrong layer.**

   **Claim:** A datum-level sized-block jump cannot hide compressed corruption because decompression and codec integrity checks already occurred.

   **Evidence:** Lines 463–465 versus codec processing at lines 1015–1064.

   **Recommendation:** Replace “corrupt compressed payloads” with “malformed decompressed datum bytes.”

## Non-blocking follow-ups (if any)

Findings 2–11 are implementation-ready contract details and finding 12 is editorial. They do not block agreement by themselves. Also remove the remaining 2.1 Scan wording from the 2.0 RC/risk sections and replace `row` with `first_row` in Appendix B.

## Milestone and gate assessment

Phase 4c is blocked by finding 1. Its mandatory higher-failure gate cannot pass under the literal non-waiting schedule.

The accounting, lifecycle, API, and parser follow-ups should land in their named phases, but they are non-blocking under the agreed severity rule. No other blocker or major remains in schema grammar, binary encoding, datum JSON, SOE, OCF framing, codecs, resolution, PCF/fingerprints, logical wire forms, projection, or interoperability.

Assumptions made:

- The detailed operative text controls when a summary is merely stale.
- “Never waiting” at lines 1108–1116 is literal.
- The recorded 2.1 Scan deferral remains accepted.
- Declared fixtures and gates are implementation deliverables.

Decisions made without user direction:

- I treated formula terms, constructor syntax, and missing focused gates as minor when the existing contract lets an implementer settle them.
- I treated the head schedule as major because it makes a mandatory deterministic-work gate impossible.

Validation was read-only. I reviewed the complete v22 plan, response-21, round-21 review, pinned local Avro specification, dependency behavior, resource equations, APIs, tests, milestones, and decisions. I ran focused read-only Julia probes. No files were modified.

Plan SHA-256: `0f46e1b4b1ced0267bea11d56b4963c6999c4e41a766102879194f5cea77e8ab`

Response SHA-256: `98f5c20e01a8a5f37c83b42ede72632a6e739dcbf8807091f4d3b84c91a055d9`

## Verdict

VERDICT: REVISE
