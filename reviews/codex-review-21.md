# Codex review round 21

DRAFT v21 still needs revision, big dawg. I found no blocker. Three major issues remain.

I reviewed [DRAFT v21](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1), [response-20.md](/Users/jacob.quinn/.julia/dev/Avro/reviews/response-20.md:1), and [round 20](/Users/jacob.quinn/.julia/dev/Avro/reviews/codex-review-20.md:1) read-only.

## Disposition check (round-20 items)

### Majors

| # | Round-20 item | Status | Evidence |
|---:|---|---|---|
| 1 | Writer block-output and count limits | **RESOLVED** | §§4.3–4.4 include both limits at lines 480–504 and 629–647. Writer flushes or rejects at lines 1182–1200, including the four-million-empty-strings fixture. |
| 2 | Three `Rows` modes | **PARTIALLY RESOLVED** | The operative contract is correct at lines 1060–1067 and 1500–1507, and Phase 4b agrees at line 1926. The public synopsis at lines 1455–1456 still says every record-root mode yields `Avro.Row` and exposes Tables, which is false when `T` is supplied. |
| 3 | Available-memory guard | **PARTIALLY RESOLVED** | Lines 532–550 correctly use cgroup remaining memory and live reservations and explicitly make the guard best-effort. Lines 195–198 still promise `LimitError` instead of process death. This remaining contradiction is a non-blocking wording defect because the detailed mechanism is clear. |
| 4 | Successful and failing parallel-work bounds | **PARTIALLY RESOLVED** | Success-only equality is now consistent. The failure bound is arithmetically wrong: lines 1123–1125 define `inflight` as the number of higher blocks, but lines 1146–1149, 1791–1795, 1927, and 2033–2038 allow only `inflight − 1`. See finding 3. |

### Minors and nit

| # | Round-20 item | Status | Evidence |
|---:|---|---|---|
| 1 | `SchemaStore` and `fromjson` signatures | **RESOLVED** | Lines 1254–1279, 1419, and 1443. |
| 2 | Constructor budgets and `UnionValue` validation | **RESOLVED** | Lines 777–787 and 1421. Minor signature notation remains below. |
| 3 | Remove `Avro.Time{P}` | **RESOLVED** | Alignment uses `Avro.truncate`/`Avro.round` at line 853; the public list is corrected at lines 1445–1446. |
| 4 | Schema-free identity-bearing collections | **RESOLVED** | Lines 949–959. |
| 5 | Remove obsolete Scan artifacts | **PARTIALLY RESOLVED** | No dormant 2.0 Scan mechanism remains. Pin/bootstrap and future-Scan residue remains at lines 1866–1868, 1935–1939, 2155, and 2407. |
| 6 | Direct-lowest-block prose | **RESOLVED** | Lines 1089–1118 and 1127–1133 distinguish direct and chunked blocks. A new dynamic-head wording issue remains below. |
| 7 | `ntasks` and worker semantics | **RESOLVED** | Lines 1121–1126 define the caller plus at most `ntasks−1` workers; `ntasks=1` creates none. |
| 8 | Complete default `W` statement | **PARTIALLY RESOLVED** | The formula names the required terms at lines 1105–1117. Lines 1150–1153 still describe all scratch as “a few MiB” and report an unsupported ≈130 MiB total. |
| 9 | Narrow constructor guarantee | **RESOLVED** | Lines 554–567 distinguish component checks from runtime combined-peak enforcement. |
| 10 | Narrow no-hashing claim | **RESOLVED** | Lines 569–580 now concern package-owned hash tables and hash-dependent charges. |
| 11 | Successfully closed writer qualification | **RESOLVED** | Lines 629–635. |
| 12 | Phase 4a risk wording | **RESOLVED** | Lines 672–680 and 2170–2171. |
| — | Decision ordering nit | **RESOLVED** | Decisions 57 and 58 are ordered at lines 2142–2147. |

## New findings

1. **[major] `instants=:datetime` cannot be represented by the closed generic and column model.**

   **Claim:** `Avro.Table` cannot produce the promised `DateTime` columns under the documented closed type set.

   **Evidence:** Lines 803–806 restrict every `ColumnBuilder{e}` to `e ∈ E`. Lines 827–836 define `E` exactly, but omit `DateTime`. Lines 858–861 say `instants=:datetime` converts timestamps across `decode`, `Rows`, `Table`, defaults, and JSON. The Table option and example promise this at lines 1452–1453 and 2421.

   **Recommendation:** Add `DateTime` and its nullable and collection forms to `E`, column builders, storage accounting, compile warm-up, and oracle gates. Alternatively, move this conversion beyond the guarded generic/Table boundary and remove the Table option.

2. **[major] Explicit typed `T` and `instants=:datetime` can require incompatible results.**

   **Claim:** The same typed operation can be required to return both the caller’s `T` and `DateTime`.

   **Evidence:** Typed plans construct the supplied `T` at lines 797–802 and 960–985. Typed `Rows` returns plain `T` at lines 1504–1507. Lines 858–861 instead require every timestamp to become `DateTime`. `DatumReader` and `decode` expose both controls at lines 1435–1441. For example, `T=Avro.Timestamp{Millisecond}` conflicts directly with `instants=:datetime`.

   **Recommendation:** Make explicit `T` authoritative. Apply `instants` only when the target is implicit, or reject incompatible combinations. Gate scalar, nullable, collection, record, and typed-`Rows` cases.

3. **[major] The mandatory failure-work bound is off by one.**

   **Claim:** A valid two-task schedule violates the stated Phase 4c bound.

   **Evidence:** `inflight` is explicitly the number of higher blocks at lines 1123–1125. With `ntasks=2`, `inflight=1`. The sole higher worker can finish while the direct head later fails, producing one block of surplus work. The mandatory bound permits `inflight−1=0` at lines 1146–1149, 1791–1795, 1927, and 2033–2038.

   **Recommendation:** Allow at most `inflight` surplus blocks when the direct head fails. Use `inflight−1` only when the authoritative failure is itself one of the higher blocks. Add both schedules as forced gates.

## Non-blocking follow-ups (if any)

- **[minor] Guard state and lifecycle:** Remove the no-OOM promise at lines 195–198. Add `live_reservations` to the global-state exceptions at lines 207–213 and 1969–1970. Define checked/clamped arithmetic and exception-safe restoration after success, interruption, poisoning, task failure, abandonment, and finalization. Clarify how cgroup-resident allocations avoid double subtraction.

- **[minor] `Rows` synopsis and retained schemas:** Qualify lines 1455–1456 by mode. State explicitly that typed `Rows` can be written through its retained Avro schema but is not a Tables source. Define `Avro.schema(::Avro.Row)` or reject schema-free row encoding explicitly.

- **[minor] Parallel-head terminology:** Line 528 says `max_inflight_blocks=0` means `ntasks`, while lines 1123–1125 use `ntasks−1`. Also, a higher block already decoded into a chunk becomes the lowest uncommitted block after the preceding commit; it cannot retroactively become the “physically direct” block promised at lines 1095–1104. Define a designated direct head or admission wave.

- **[minor] Parallel accounting formula:** Define `committed` in line 1106 so category-(a) capacity is not added twice. Lines 1145–1146 currently use a separate `committed_bytes` that includes categories (a) and (b).

- **[minor] Block-output accounting:** Restore one authoritative, consumer-independent `max_block_output_bytes` formula. `Reader`, `Avro.Row`, and column storage have different physical representations. State whether row and chunk headers count.

- **[minor] `Avro.Row` storage oracle:** `Base.summarysize(row; exclude=Avro.Schema)` recursively includes its already-accounted `Record` and `SymbolAdmission`. Test the wrapper shell with those referents excluded.

- **[minor] Default `W` and CPU wording:** Replace the approximate “few MiB” scratch statement at lines 1150–1153 with the Phase 4a maximum. Apply the 1.5× CPU tolerance only to successful runs; on failure, CPU must be recorded only because one cap-bounded speculative block can dwarf an immediate sequential failure.

- **[minor] Comparison calibration:** Lines 596–606 count moved 4/8-byte entries but justify the multiplier using only key comparisons. Finalize the multiplier using both comparisons and moves and remove the universal “admits every legitimate input” claim if a resource limit can reject valid data.

- **[minor] Security wording:** Replace “no allocation sized by untrusted counts” at lines 1954–1955 with “no unchecked or unreserved allocation.” Exact final columns are intentionally sized from validated counts.

- **[minor] Public surface:** List `Avro.Limits`, `SchemaCache`, public error types, and concrete public schema-constructor signatures in §5. Synchronize `limits=Limits()` notation for `Fixed`, `EnumValue`, and `Map`.

- **[minor] Public constructor properties:** Explicitly reject `props` keys that collide with structural keys such as `type`, `name`, and `fields`. Rejecting only duplicate `props` keys at lines 271–278 does not prevent invalid duplicate output from the printer.

- **[minor] `UnionValue` ownership:** Line 1421 groups it with copying, budgeted constructors, but `UnionValue(index,x)` has no `limits` and apparently retains `x`. State that it is a non-copying wrapper or add the promised copy and budget.

- **[minor] Secondary time contracts:** Define conventional `Avro.schema(ZonedDateTime)` or require an explicit timestamp schema. State float JSON/default overflow, underflow, and signed-zero behavior alongside the differential vectors.

- **[minor] Container boundaries:** Require invalid UTF-8 in `avro.codec` to fail before codec lookup. Explicitly check compressed output against `max_block_bytes` before Writer emission, including an incompressible boundary fixture.

- **[minor] Metadata API:** The Writer signature accepts `Vector{UInt8}` values, but Appendix B passes `b"sensor"`, which is `Base.CodeUnits`. Either accept and copy `AbstractVector{UInt8}` or fix the example.

- **[minor] Dense-ID wording:** Category (e), line 741, still says IDs are assigned at parse time. Lines 234–239 correctly cover every creation path.

- **[minor] Projection gate:** Add direct and mutual recursive-root projection cases. The root-substitution mechanism is stated, but the active projection matrix does not name these cases.

- **[nit] Deferred-Scan residue:** Remove 2.0 readiness references to a `[sources]`/Tables bootstrap and recast line 2155 as a 2.1-only note.

- **[nit] Executable-document cleanup:** Remove the duplicated §8.2 heading at lines 1580–1581. Replace “eight workers” with `ntasks=8` at lines 1795 and 1927. Appendix B uses `row` after its loop scope at line 2411 and therefore is not executable as written.

## Milestone and gate assessment

Phase 2 is blocked by the incomplete closed value set and the unresolved `T`/`instants` precedence. Phase 4b is blocked by the promised `DateTime` Table output. Phase 4c is blocked by the off-by-one failure gate.

Phases 1, 3, and 4a have no new blocker or major finding. I found no further Avro binary, datum-JSON, OCF, codec, single-object, PCF, resolution, alias/default, logical-type, or sort-order violation.

Assumptions made:

- The detailed operative contracts control when a summary is merely stale.
- The 2.1 Scan and other recorded deferrals remain accepted.
- Declared fixtures and gates are implementation deliverables.

Decisions made without user direction:

- I treated stale API and summary text as minor when the executable mechanism is otherwise complete.
- I treated the failure arithmetic as major because its mandatory gate cannot pass.
- I treated both `DateTime` contradictions as major because the public APIs cannot satisfy their stated result contracts.

Validation was read-only. I used the Julia package-review workflow and inspected the complete plan, round-20 review, response, resource formulas, APIs, codecs, tests, milestones, and decision summaries. No files were modified.

Plan SHA-256: `b71432061d6f27dfe4ad02c7492b3edfd4626dbec530606088fba305d28faf28`.

Response SHA-256: `e56dcce6ce1e027bc614a3862e9169d14a2d75adae359a7dc19bdd14cbc9b674`.

## Verdict

VERDICT: REVISE
