# Codex review round 20

DRAFT v20 is not implementation-ready, captain. Four major issues remain. I found no blocker.

I reviewed [DRAFT v20](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:3), [response-19.md](/Users/jacob.quinn/.julia/dev/Avro/reviews/response-19.md:1), and [round 19](/Users/jacob.quinn/.julia/dev/Avro/reviews/codex-review-19.md:1) read-only.

## Disposition check (round-19 items)

### Majors

| # | Round-19 item | Status | Evidence |
|---:|---|---|---|
| 1 | Schema-object `type` grammar | **RESOLVED** | §4.2 lines 382–386 restrict object forms to primitive names and the six complex keywords. Named references are strings. Unions are arrays. |
| 2 | Parallel physical acceptance and complete `W` | **RESOLVED** | Lines 1086–1092 make the lowest block use the direct sequential path. Lines 1093–1106 reserve the complete higher-block peak and create workers only from headroom. |
| 3 | Failure-path work | **PARTIALLY RESOLVED** | Lines 1126–1132 correctly limit equality to successful operations and bound failure surplus. Mandatory gates and summaries remain unconditional at lines 1758–1766, 1898, 1925–1930, 2004–2008, and 2038–2041. See finding 4. |
| 4 | Identity-bearing union recovery | **RESOLVED** | Line 841 matches `Record`, `EnumValue`, and `Fixed` by carried schema identity before representation matching. Line 1749 supplies the gate. |

### Minors

| # | Round-19 item | Status | Evidence |
|---:|---|---|---|
| 1 | Signature and budget synchronization | **PARTIALLY RESOLVED** | Public signatures are corrected at lines 1393, 1417, and 1429. The detailed `SchemaStore` interface still omits `limits=` at lines 1234–1235, and §4.11 omits `unknown=` at lines 1247–1248. |
| 2 | Duplicate selectors and typed/non-record projection | **RESOLVED** | Lines 1052–1056 reject duplicates and unknown names and prohibit `select` with typed or non-record `Rows`. |
| 3 | Projected-schema attributes and recursion | **RESOLVED** | Lines 1054–1056 and 1483–1499 preserve root and field attributes and define recursive-root substitution with one memo. |
| 4 | `Avro.Row` accounting and ownership | **RESOLVED** | Lines 714–715 add its shell to the oracle. Lines 746–749 define ownership transfer. |
| 5 | All-duplicate map accounting | **RESOLVED** | Lines 563–570 and 760–763 retain and charge `npairs` capacity. |
| 6 | Obsolete Scan text | **PARTIALLY RESOLVED** | Active metadata is unpinned at lines 1837–1839. However `filter masks` remains at line 730, the old development pin remains at lines 1490–1494, and Phase 0 still requires a Tables SHA/bootstrap at line 1892. |
| 7 | `FixedSchema.doc` | **RESOLVED** | Line 240 removes the field. Lines 387–388 treat fixed `doc` as metadata. |
| 8 | Empty enums | **RESOLVED** | Lines 264–266 define parsing, PCF, fingerprints, `minsize`, and no-datum behavior. |
| 9 | Public value constructors | **PARTIALLY RESOLVED** | Lines 1394–1395 define validation, copying, and ownership. They do not expose `limits=`, and these constructors are absent from the budget scopes at lines 771–780. |
| 10 | Missing `avro.schema` and strict `eachblock` | **RESOLVED** | Lines 996–1000 require `avro.schema`. Lines 1047–1050 validate exactly `count` datums before strict-mode yield. |
| 11 | Decimal resolution | **RESOLVED** | Lines 864–867 give ordinary structural matching. Lines 889–895 add precision and scale requirements. |
| 12 | GMP and `WideDecimal` | **RESOLVED** | Lines 1153–1159 enumerate GMP and require the complete limb and temporary peak reservation. |
| 13 | StructUtils defaults and TimeZones | **RESOLVED** | Lines 914–917 define default errors and remedies. Line 847 defines UTC, precision, and zone-loss behavior. |
| 14 | Fullname-first alias matching | **RESOLVED** | Lines 864–867 perform normalized-fullname matching before unqualified-name matching. |
| 15 | Pending Stage-1 failure | **RESOLVED** | Lines 1117–1122 retain indexed structural failures while lower blocks run and select the lowest index. |

The round-19 nit is **RESOLVED**. Phase 1 now qualifies ignored namespaces at line 1893, and the review log is chronological.

## New findings

1. **[major] The writer does not enforce `max_block_output_bytes`.**

   **Claim:** A successfully closed writer can emit a block that a reader with identical limits rejects.

   **Evidence:** The hard reader cap is defined at line 504 and applied at lines 1091 and 1098. It is absent from the writer rules at lines 480–490, the invariant enumeration at lines 623–629, and the flush conditions at lines 1172–1175.

   One `array<string>` datum containing about four million empty strings is about 4 MiB on the wire. Under the plan’s formulas, the resulting `Vector{String}` needs about 32 MiB of slots plus 64 MiB of empty-string payload charges. It passes the datum, block, work, value, and 256 MiB total limits but exceeds the 64 MiB block-output cap.

   The flush list also omits `max_block_count`, which matters when callers raise the work constants for zero-size datums.

   **Recommendation:** Track the exact reader-side output estimate for each pending block. Flush before either `max_block_output_bytes` or `max_block_count` would be exceeded. Reject a single datum that exceeds the output cap. Add compact-wire, high-Julia-expansion fixtures.

2. **[major] Typed `Avro.Rows` has incompatible output contracts.**

   **Claim:** `Rows(src; T=...)` cannot both return arbitrary `T` values and always return admission-carrying `Avro.Row` values with a Tables row interface.

   **Evidence:** Lines 790–795 and 950–970 define typed plans that construct `T`. Lines 842, 1050–1052, and 1429–1430 instead say every record-root `Rows` yields `Avro.Row`. Lines 1474–1477 promise a Tables row table for every record root. Phase 4b line 1897 incorrectly extends `Avro.Row` even to non-record roots.

   Arbitrary `T` can be a scalar, dictionary, or custom StructUtils result. It need not implement the Tables row interface.

   **Recommendation:** Define separate modes:

   - `T=nothing` with a record root yields `Avro.Row` and implements Tables.
   - `T` supplied yields a plain iterator of `T`, unless a specific Tables-compatible restriction is met.
   - Non-record roots remain plain iterators.

   Update partition, admission, API, and Phase 4b gates accordingly.

3. **[major] The available-memory guard cannot provide its stated OOM guarantee.**

   **Claim:** The default guard can admit more memory than remains inside a constrained process.

   **Evidence:** Lines 535–540 compute `available` from host-wide free memory and cgroup total memory, then promise `LimitError` instead of an OOM kill. Lines 2127–2130 acknowledge that this does not measure remaining cgroup memory.

   For example, a 512 MiB cgroup already using 500 MiB on a host with 8 GiB free receives a 256 MiB effective ceiling, despite having about 12 MiB remaining.

   **Recommendation:** Include cgroup current usage, applicable process limits, runtime headroom, and package-wide concurrent reservations. If those values cannot be obtained, make the guard explicitly best-effort and remove the no-OOM guarantee. Add injected total/current-usage boundary tests.

4. **[major] The mandatory failure-work gates contradict the adopted failure bound.**

   **Claim:** The plan permits extra higher-block work on failures but still requires equality with sequential execution.

   **Evidence:** Lines 1126–1132 permit up to `inflight − 1` additional higher blocks on failure. The concurrency test at lines 1758–1766 and Phase 4c at line 1898 require equal counters while also testing failures. The security summary and Decisions 22/34 remain unconditional at lines 1925–1930, 2004–2008, and 2038–2041.

   **Recommendation:** State counter equality only for successful operations everywhere. Give failing operations a separate mandatory bound: no retry, at most `inflight − 1` speculative higher blocks, each within its block caps, with the same selected error.

## Non-blocking follow-ups

- **[minor]** Synchronize the detailed `SchemaStore` and `fromjson` signatures with §5.
- **[minor]** Add `limits=` and budget scopes to public schema and copying value constructors. Define what `UnionValue(index, x)` can validate without an enclosing union schema.
- **[minor]** Define or remove `Avro.Time{P}`. It is promised at lines 846 and 1419–1420 but is absent from `E` and has no representation contract.
- **[minor]** Define schema-free behavior for arrays and maps containing identity-bearing values. Root identity recovery does not cover these nested cases.
- **[minor]** Remove the remaining Scan artifacts: `filter masks`, the development-pin note, the Phase-0 Tables SHA/bootstrap, and stale skipped-OCF-block language.
- **[minor]** Correct the parallel prose that says every Stage-2 block uses a chunk and every commit copies one. The active lowest block is direct.
- **[minor]** Clarify whether `ntasks` and `inflight` include the direct lowest block. The worker-pool formula currently implies a worker even for `ntasks=1`.
- **[minor]** Replace the approximate default `W` at line 1133 with a bound that includes its declared chunk, scratch, overlap, and state terms.
- **[minor]** Narrow the constructor claim at lines 557–560. Its separate fraction checks do not prove every combined header, graph, buffer, workspace, and output peak fits.
- **[minor]** Narrow “no hashing anywhere” to no package-owned hash-table lookup or hash-dependent charge. Runtime symbol interning is already an explicit exception.
- **[minor]** Qualify the invariant as applying to output from a successfully closed writer. Caller-owned I/O can contain partial output after failure.
- **[minor]** Update the open risk at lines 2139–2140: constants are finalized after Phase 4a, not Phase 2.
- **[nit]** Put Decision 57 before Decision 58.

## Milestone and gate assessment

Phase 0 is blocked by the unsafe available-memory guarantee. Phase 2 and Phase 4b need a coherent typed-`Rows` contract. Phase 4a is blocked by the missing writer enforcement of `max_block_output_bytes`. Phase 4c still has an unexecutable failure-work gate.

Phase 1 and Phase 3 have no new blocker or major finding. The new direct-lowest/headroom model is otherwise coherent. Its liveness and successful-operation acceptance argument is executable.

Assumptions made:

- Declared artifacts and gates remain implementation deliverables.
- I accepted the finite certified Julia/JLL matrix and the recorded interoperability deviations.
- I did not reopen deferred 2.1 Scan functionality.

Decisions made without user direction:

- I treated stale wording as minor unless it changes a mandatory gate.
- I treated the unqualified Phase 4c counter requirement as major because the gate cannot pass together with the permitted failure surplus.
- I treated the cgroup-current-usage omission as major because it leaves an unsafe default while promising OOM prevention.

Validation was read-only. I inspected the complete plan, response 19, round-19 review, public API, resource model, codec and container contracts, tests, milestones, and decision summaries. Focused arithmetic established the compact-wire block-output counterexample. No files were modified.

Plan SHA-256: `c256c79e51905331eeda9b993349cf326dcbf3decb87ccf095c8a81977dc1c34`. Response SHA-256: `c7eb6fa1a2fd311461295b95946cabef29b9976789866af95ce8dc1d824aa4fd`.

## Verdict

VERDICT: REVISE
