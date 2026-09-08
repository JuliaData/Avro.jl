# Codex review round 16

Evidence refers to [DRAFT v16](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1) and [response-15.md](/Users/jacob.quinn/.julia/dev/Avro/reviews/response-15.md:1).

## Disposition check (round-15 items)

### Five major findings

| # | Round-15 item | Status | Evidence |
|---:|---|---|---|
| 1 | JSON.jl surrogate corruption | RESOLVED | The Avro-owned raw-span decoder, WTF-8 representation, contextual validation, charged scratch, and collision tests are specified at lines 318–346. |
| 2 | Alias-based repair | RESOLVED | Invalid writer names and exact WTF-8 alias matching are covered at lines 337–343. |
| 3 | Dense graph identity and memo work | PARTIALLY RESOLVED | Stored IDs, `GraphInfo`, and charged partner tables exist at lines 234–237 and 663–668. Lines 272–276 still say IDs are assigned at parse time and vectors are scanned linearly. Public constructors freeze children immediately, but cross-graph composition is undefined. |
| 4 | Writer/Reader invariant | PARTIALLY RESOLVED | Consumers, options, and source qualifications are at lines 571–595. The streamed `Table` peak omits chunk-header storage, `Table` cannot consume non-record roots, and its full gate appears before `Table` is implemented. |
| 5 | `Avro.write` schema precedence | RESOLVED | Explicit schema → effective Avro schema → `Tables.schema` is defined at lines 1139–1147, with accessors at lines 1319–1321. Scan-derived schema provenance is a separate new finding below. |

### Twenty-seven follow-ups

| # | Follow-up | Status | Evidence |
|---:|---|---|---|
| 1 | Map capacity retained at `npairs` | PARTIALLY RESOLVED | Lines 689–691 retain `npairs`; lines 623–628, 766, and 1683–1684 still require `nunique`. |
| 2 | Typed-probe reservation | RESOLVED | Checked pre-reservation is at lines 631–638. |
| 3 | String-source copies in category (c) | RESOLVED | Lines 649–657. |
| 4 | Admission scan/sort calibration | RESOLVED | Lines 541–552 and 597–603. |
| 5 | Comparison moves and named-table work | RESOLVED | Lines 541–546. |
| 6 | Recursive `minsize` and exact union-index size | RESOLVED | Lines 692–697. |
| 7 | Allocation-hook exception list | PARTIALLY RESOLVED | Lines 1087–1090 define package wrappers but enumerate only codec and `Mmap` exceptions. Runtime, `Base.summarysize`, symbol interning, and dependency allocations remain unclear. |
| 8 | “Logically unreachable” release wording | RESOLVED | Lines 1032–1039. |
| 9 | Snappy cancellation granularity | RESOLVED | Lines 1033–1035. |
| 10 | Duplicate-map iteration position | RESOLVED | Lines 687–691. |
| 11 | Immutability scope | RESOLVED | Lines 248–254. |
| 12 | `Rows`/`Reader` do-block and finalizer | RESOLVED | Lines 986–990. |
| 13 | `Reader.decimal_byteorder` | PARTIALLY RESOLVED | It appears in the public signature at line 1357 but not the operative `Reader` contract at lines 919–920. |
| 14 | `SchemaCache.register!`/`lookup` API | RESOLVED | Lines 1164–1169 and 1321. |
| 15 | Owned raw metadata spans | PARTIALLY RESOLVED | Lines 330–331 retain spans, but only default spans are explicitly copied and owned at lines 300–301. Caller vectors are otherwise used in place. |
| 16 | Ambiguous field aliases | RESOLVED | Lines 793–797. |
| 17 | Duplicate OCF metadata keys | PARTIALLY RESOLVED | Rejection is specified at lines 919–922, but the required malformed corpus does not explicitly name ordinary, `avro.schema`, and `avro.codec` duplicates. |
| 18 | Standalone `error` schema | RESOLVED | Lines 174 and 242. Equality details remain a minor follow-up. |
| 19 | One-based value positions and `ordinal` | RESOLVED | Lines 764–767 and 1320. |
| 20 | Identity-bearing branch fixtures | RESOLVED | Lines 1701–1702. |
| 21 | Signed-zero/non-finite default equality | RESOLVED | Lines 266–269. Invalid repaired defaults remain unspecified. |
| 22 | Ignored namespace remains JSON-string typed | RESOLVED | Lines 355–358. |
| 23 | Bounded CI fuzzing | RESOLVED | Lines 1639–1647. |
| 24 | Worst-density latency wording | RESOLVED | Lines 597–603. |
| 25 | `Base.show` uses recorded limits | NOT RESOLVED | `show` appears only in the API list at line 1329. No budget or recorded-limit rule is stated. |
| 26 | RPC deferral checklist | RESOLVED | Lines 174–175. |
| 27 | Review-log ordering | RESOLVED | Rounds 1–15 are ordered at lines 2072–2203. |

## New findings

### 1. [blocker] JSON numeric tokens bypass the memory and work model

**Claim.** A short untrusted JSON number can cause superlinear, unreserved dependency work before Avro can apply its budget.

**Evidence.** The pre-scan validates bytes, depth, UTF-8, and escapes, but not number tokens ([plan lines 318–354](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:318)). Pinned JSON.jl calls `parsenumber` even when merely skipping a number ([lazy.jl lines 687–701](/Users/jacob.quinn/.julia/packages/JSON/5zJAn/src/lazy.jl:687)). After `Int64` overflow, it performs a new `BigInt` multiply/add for each digit ([lines 623–631](/Users/jacob.quinn/.julia/packages/JSON/5zJAn/src/lazy.jl:623)); floating overflow can construct `BigFloat`.

A read-only Julia 1.12 probe measured:

```text
digits   cumulative allocation
4,000       8,328,232 bytes
8,000      31,078,184 bytes
16,000    116,216,104 bytes
32,000    446,254,888 bytes
```

These tokens are far below `max_schema_bytes`. Work grows superlinearly and is not charged. JSON.jl also explicitly accepts a leading `+` ([line 593](/Users/jacob.quinn/.julia/packages/JSON/5zJAn/src/lazy.jl:593)); its permissive mode accepts more than the three documented non-finite tokens. Retaining JSON.jl `BigInt` values also contradicts the frozen immutable-scalar contract.

**Recommendation.** Extend the Avro lexical scanner to validate number grammar and record token endpoints. Never call JSON.jl number materialization or `skip` for numeric tokens. Parse required integers with checked accumulation and required floats with a bounded `Float64` path. Store metadata numbers as charged immutable raw-number tokens. Test large integers, mantissas, and exponents in known fields, defaults, nested properties, skipped properties, and `fromjson`.

### 2. [major] The `Table` consumer guarantee is internally impossible

**Claim.** The invariant guarantees `Table` for every successful Writer output, but `Table` rejects non-record roots. Its full gate is also scheduled before `Table` exists.

**Evidence.** Lines 571–595 name `Reader`, `Rows`, and `Table` as guaranteed consumers and include null, fixed, and array roots. Lines 971–976 restrict `Table` to record roots. Phase 4a requires the complete invariant at line 1852; `Rows` and `Table` are introduced in Phase 4b at line 1853.

**Recommendation.** Guarantee all roots only for `Reader` and `Rows`. Scope `Table` to record roots. Keep the container-only invariant in Phase 4a and move the complete named-consumer/source-mode gate to Phase 4b.

### 3. [major] Streamed `Table` peak accounting omits per-block column headers

**Claim.** The “twice committed data” formula can undercount hundreds of megabytes of retained chunk shells.

**Evidence.** Non-seekable `Table` retains one exact column chunk per block before final assembly at lines 583–585 and 994–998. Category (a) charges every array header at lines 610–613, but the preflight does not include `nblocks × ncolumns` headers.

On Julia 1.12, both `Vector{Missing}(undef, 1)` and `Vector{Missing}(undef, 10_000)` have a 40-byte shell. A 1,001-field record over 10,000 one-row blocks therefore retains about 400.4 MB of chunk headers even when final column data is small. The current Writer preflight can accept that file under a 256 MiB limit, while streamed `Table` cannot.

**Recommendation.** Preflight the exact sum of all chunk shells and payloads, final shells and payloads, block tables, and assembly buffers. Add a wide mostly-null record with one payload field across many tiny blocks. Remove the stale geometric-growth instructions at lines 1435–1436, 1853, and 1987–1988.

### 4. [major] Parallel priority can reject a file that sequential decoding accepts

**Claim.** A higher speculative block can receive a final `LimitError` only because a lower block still holds transient memory.

**Evidence.** Lines 1024–1031 evict only blocks with indexes greater than requesting block `i`, then fail `i` if its request still does not fit. Consider a ceiling of 256 units, final capacity 40, block 0 transient reservation 150, and block 1 holding 20 while requesting 60. Parallel accounting reaches 270 and fails block 1. After block 0 commits one unit and releases 150, sequential block 1 needs only 121.

**Recommendation.** A denied block that is not the lowest uncommitted block must release its own reservations and wait or requeue. Only the lowest uncommitted block may produce the final budget failure. Add this exact forced schedule to the acceptance-equivalence gate.

### 5. [major] Cooperative eviction has no bounded acknowledgement inside a datum

**Claim.** A worker can hold memory indefinitely while the coordinator waits for its eviction acknowledgement.

**Evidence.** Lines 1032–1039 check eviction between decompression steps and between top-level datums. One datum can contain a large nested collection, map sort, comparison, or a blocked reservation request. The worker might not reach a between-datum check. This contradicts the bounded-wait liveness proof at lines 1052–1056.

**Recommendation.** Make every permit wait cancellable. Check eviction at each reservation and at bounded intervals in nested decoding, sorting, comparison, and admission loops. Gate eviction while a higher block is inside one large datum and while it waits for a permit.

### 6. [major] The pinned Scan implementation bypasses guarded allocation and no-hash contracts

**Claim.** Required Tables.Scan calls allocate and hash outside Avro’s accounting model.

**Evidence.** The plan mandates `Tables.resolve`, `Tables.filtermask`, and `Tables.scan` at lines 1415–1449. The pinned implementation:

- uses `collect`, `push!`, `append!`, `Set{Int}`, and `Set{Symbol}` in [scan.jl lines 454–481](/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/tables-scan/src/scan.jl:454);
- allocates row-sized predicate intermediates for nested Boolean expressions at [lines 539–550](/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/tables-scan/src/scan.jl:539);
- allocates a second final Boolean mask at [lines 563–580](/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/tables-scan/src/scan.jl:563).

This contradicts Decision 35 and the reserve-before-allocation gate. Deep `And`/`Or` trees can retain several row-sized arrays.

**Recommendation.** Use an Avro-owned allocation-aware resolver and filter evaluator, or obtain an upstream interface that accepts caller-provided buffers and reservations. Otherwise reserve a proven complete dependency peak and explicitly narrow the no-hash contract. Put residual conversion after an explicit ownership-transfer boundary. Gate deep predicates and residual conversions near the ceiling.

### 7. [major] Scan does not define effective-schema provenance

**Claim.** A scanned table can retain a schema that no longer describes its columns.

**Evidence.** `Avro.write` prefers the retained effective schema at lines 1139–1147, and `Avro.schema(table)` returns it at line 1319. Projection, rename, zero-column selection, direct promotion, and residual conversion change the table at lines 1419–1449, but no rule transforms or clears the retained Avro schema.

**Recommendation.** Derive a new effective schema for identity-preserving projection, rename, and supported promotion. Clear provenance and require `schema=` after arbitrary residual conversion. Gate writing projected, renamed, zero-column, promoted, and evolved scan results.

### 8. [major] Repaired schemas have no valid PCF or single-object policy

**Claim.** `canonical`, `fingerprint`, schema-cache registration, and single-object encoding are promised for repaired schemas even when no specification PCF exists.

**Evidence.** The parser can retain invalid used names at lines 305–343. PCF assumes a valid Avro schema and converts strings to UTF-8 ([spec PCF section](/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:735)). A lone-surrogate name has no valid UTF-8 PCF. Nevertheless, lines 373–378, 1159–1169, and 1321–1343 expose unconditional canonicalization, fingerprints, registration, and single-object encoding. Only OCF Writer checks repair flags.

**Recommendation.** Reject repaired-name schemas from `canonical`, `fingerprint`, `register!`, and `encodesingle` by default. Define the policy for repaired invalid defaults. If nonstandard fingerprints are wanted, require an explicit option and label them non-Avro. Add negative gates.

### 9. [minor] Graph-local IDs lack a composition rule

**Claim.** An already-frozen child schema cannot join another public schema graph without reassigning its filled-once `id` and `GraphInfo`.

**Evidence.** Lines 234–237 make both references graph-local and write-once. Public constructors freeze immediately at lines 248–250. Lines 272–276 and 663–668 also disagree about parse-time assignment and linear versus binary partner lookup.

**Recommendation.** Deep-clone imported child graphs under the parent budget, or reject cross-graph composition with an actionable error. Add parser-, type-derived-, public-constructor-, and reused-child gates. Validate that dense IDs fit `Int32`.

### 10. [minor] Repair support remains inconsistent in peripheral APIs

**Claim.** High-level copying and JSON conversion do not expose the repaired-schema policy.

**Evidence.** `Avro.write` and `tobuffer` cannot forward `allow_invalid_names/defaults`, although Writer requires them. JSON union labels and enum strings require valid Unicode, so some repaired schemas cannot use `tojson`/`fromjson`.

**Recommendation.** Forward repair options through `write` and `tobuffer`. State that repaired invalid names are binary/OCF-only where JSON labels cannot represent them, or add an explicit WTF-8 JSON mode.

### 11. [minor] Several schema semantic edges remain unspecified

**Claim.** Equality and validation are incomplete at a few repair/protocol boundaries.

**Evidence.**

- `valid=false` defaults have no decoded value or selected branch for the equality rule at lines 266–307.
- `RecordSchema.iserror` is omitted from equality/hash semantics.
- Alias resolution wording can confuse normalized-fullname alias matching with direct unqualified-name matching.
- Types for defined optional attributes such as `doc` and `aliases` are not listed explicitly.

**Recommendation.** Define invalid-default and `iserror` equality/hash/cache semantics. Separate alias fullname rewriting from direct unqualified-name matching. Add an attribute-type table and negative fixtures.

### 12. [minor] Legacy decimal repair does not define the framing correction

**Claim.** Byte-order reversal alone cannot recover every 1.x decimal file.

**Evidence.** V16 states that 1.x wrote fixed-16 regardless of the declared fixed size, but only specifies `decimal_byteorder=:little`. Avro.jl 1.1.2 wrote 16 bytes while its reader consumed the declared size.

**Recommendation.** State that the explicit legacy option consumes the known 16-byte representation, or limit recovery to schemas whose declared size is 16. Add a real non-16-size 1.1.2 fixture.

### 13. [minor] A few allocation and lifecycle boundaries need explicit implementation rules

**Claim.** The main ceiling contract is usable, but several small paths remain outside its written mechanism.

**Evidence.** The exception list does not cover `Base.summarysize`, runtime symbol interning, task-stack behavior, or similar dependency work. Snappy’s high-level `uncompress` allocates its predicted output internally. Prepared `DatumWriter` calls do not say how retained Encoder capacity is precharged to each fresh call budget. Writer has no do-block or finalizer-abort fallback.

**Recommendation.** Enumerate all exclusions. Use Snappy’s low-level length query, reserve, then decompress into an Avro-owned buffer. Precharge retained Encoder capacity. Add a Writer do-block form and a documented cleanup fallback.

## Non-blocking follow-ups (if any)

The following can be completed during implementation:

- Reconcile `npairs` versus `nunique` permutation capacity and its resource gate.
- Make raw metadata spans explicitly owned and charged.
- Add `decimal_byteorder` to the operative `Reader` contract.
- State `Base.show` behavior under recorded graph limits.
- Add explicit duplicate ordinary/`avro.schema`/`avro.codec` header fixtures.
- Remove stale geometric-growth wording.
- Reconcile Phase 2’s “100k” fuzz wording with the required `200 × 1,000` sample.
- Add the missing parser/public/type-derived dense-ID creation gates.
- Include comparison, move, filter, and assembly work in deterministic counters where those counters claim complete work.
- Clarify direct copying of non-record `Rows`.

## Milestone and gate assessment

Phase 1 and Phase 2 are blocked by JSON numeric-token parsing. Phase 2 also needs the repaired-schema fingerprint boundary. Phase 4a’s complete invariant gate is ordered before its consumers exist. Phase 4b needs exact streamed-chunk accounting. Phase 4c needs corrected priority and cancellable eviction. Phase 4d needs an allocation-aware Scan path and schema provenance.

The remaining specification surface, codec matrix, interoperability corpus, benchmark structure, CI split, and release-readiness levels remain strong.

Assumptions made:

- I treated comments, operative sections, milestones, and recorded decisions as normative.
- I reviewed Scan because v16 still requires its implementation and Phase 4d gates, even though the RC may remove it.
- I treated omitted unbounded memory/work terms as major. I treated constants, scratch sizes, and clear wording repairs as minor.

Decisions made without user direction:

- I retained the previously accepted skipped-string UTF-8 policy and recorded extensions.
- I did not reopen explicit RPC, big-decimal, inference, append, borrowed-view, or parallel-compression deferrals.
- I treated cumulative JSON allocation as evidence of unbounded work and missing reservations, not as a direct peak-RSS measurement.

Validation was read-only. I inspected DRAFT v16, response-15, the Round-15 review, the pinned specification, JSON.jl, Tables.Scan, and Avro.jl 1.1.2. I ran only in-memory Julia probes. I created or modified no files.

## Verdict

VERDICT: REVISE
