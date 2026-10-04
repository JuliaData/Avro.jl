# Codex review round 14

DRAFT v14 is not implementation-ready, big dawg. The redesign removes hashing and fixes most Round-13 defects. Six major issues remain.

Line references are to [DRAFT v14](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1).

## Disposition check (round-13 items)

A replacement is partially resolved when it removes the original mechanism but still fails the same guarantee.

### Primary carried rows

| Item | Status | Evidence |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | The accounting model is substantially complete at lines 414–634. Map construction, dynamic schema tables, vector growth, and pointer-incompatible strings can still allocate or work outside it. |
| R1-9 — Freezing and hashing | **RESOLVED** | Transitive freezing and stored hashes: lines 244–251. Fresh defaults: lines 286–296. |
| R1-10 — Schema-driven compilation | **RESOLVED** | Generic plans and the finite value set: lines 636–708. |
| R1-18 — Writer lifecycle | **RESOLVED** | Validation, poisoning, cleanup, abort, and atomic replacement: lines 1012–1053. |
| R1-20 — Parallel error determinism | **RESOLVED** | Ordered commits and lowest-index error selection: lines 977–990. |
| R1-25 — Union representation/resolution | **RESOLVED** | Generic union representation: line 695. Reader-directed resolution: lines 725–739. |
| R2-new-6 — Symbol admission | **PARTIALLY RESOLVED** | The separate sorted-runs structure removes the old map contradiction, lines 491–497, but a single admitting operation can inherit an unbounded carry merge. |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowed contract** | The conventional, non-round-trip contract remains explicit at lines 792–799. |

### Other carried rows

| Item | Status | Evidence |
|---|---|---|
| R4-1 — Codec memory | **RESOLVED** | Complete per-member requirements and multi-member handling: lines 853–892. |
| R4-2 — Work/allocation budget | **PARTIALLY RESOLVED** | The datum rules are explicit at lines 499–524, but the new map, admission, dynamic-table, source-copy, and vector-growth paths do not yet satisfy them. |
| R5-1 — Writer/Reader invariant | **RESOLVED for the prior codec/map-comparison claims** | Reader peak and deterministic map comparison work are included at lines 526–541. |
| R5-2 — Parallel transient memory | **RESOLVED for the parallel block mechanism** | Reservations, commits, assembly, and eviction: lines 919–1011. |
| R5-3 — Work amplification | **PARTIALLY RESOLVED** | Map comparisons and moves are counted at lines 499–508. Admission carries and category-(e) table operations remain unbounded per operation. |
| R5-4 — Codec-cap contract | **RESOLVED** | Lines 853–892 and 1023–1039. |
| R5-9 — Julia-derived names | **RESOLVED** | Lines 768–791. |
| R5-10 — Typed Symbol path coverage | **RESOLVED** | Lines 821–825 and 1294–1305. |

### Round-6 carried findings

| # | Item | Status |
|---:|---|---|
| 1 | Semaphore memory/liveness | **RESOLVED** — lines 919–1011. |
| 2 | Writer/Reader invariant | **RESOLVED** — lines 526–541. |
| 3 | Unsafe default memory | **RESOLVED** — lines 417–468. |
| 4 | Compressed-size work precheck | **RESOLVED** — expressly prohibited at lines 510–524. |
| 5 | Whole-file `mmap=false` allocation | **RESOLVED** — streamed path at lines 902–905. |
| 6 | Self-alias handling | **RESOLVED** — lines 333–335. |
| 7 | `fromjson` admission | **RESOLVED** — lines 1084–1093. |
| 8 | Julia name policy | **RESOLVED** — lines 768–791. |
| 9 | Codec-cap boundary cases | **RESOLVED** — lines 853–892. |
| 10 | Work ceilings | **PARTIALLY RESOLVED** — datum work is bounded, but admission and incremental category-(e) tables are not. |
| 11 | JSON-depth symmetry | **RESOLVED** — lines 419–422 and 1084–1090. |
| 12 | Duplicate named union branches | **RESOLVED** — lines 335–337. |
| 13 | Attribute wording | **RESOLVED** — lines 269–285. |

### Round-7 items

All eleven are **RESOLVED**:

1. Exact final-column allocation: lines 925–984.
2. Writer codec requirements: lines 1023–1039.
3. Actual reservations and priority: lines 919–990.
4. Portable defaults: lines 417–468.
5. Constructor relations: lines 470–482.
6. Empty unions: lines 256–259.
7. Peak-RSS method: lines 991–1011.
8. Alias normalization: lines 252–255.
9. Streamed compressed input: lines 902–905.
10. `Nothing → null`: lines 761–763.
11. Julia-name sanitization: lines 768–791.

### Round-8 items and follow-ups

All remain **RESOLVED** for their stated claims:

- Nullable-column storage and tag bytes: lines 556–593.
- Complete Zstandard requirement: lines 861–866.
- Eviction protocol and fixed worker pool: lines 919–990.
- Parallel RSS source/sampling: lines 991–1011.
- Non-UTF-8 metadata exception: lines 1391–1398.
- Filtered Scan sizing and per-pass attempts: lines 1328–1343.
- `fixed(0)` decimal handling: lines 281–285.
- Estimator dependencies and symbol checks: §11.
- Bzip2 floor, cooperative cancellation, ownership transfer, CPython pin, and accepted deferrals remain resolved.

### Round-9 items and follow-ups

| Item | Status | Evidence |
|---|---|---|
| XZ reservation | **RESOLVED** | Lines 856–861. |
| Representation storage | **PARTIALLY RESOLVED** | Typed shells are fixed at lines 577–586. Map construction and vector capacity remain incorrect. |
| Concatenated members | **RESOLVED** | Lines 869–892. |
| Filtered Scan attempts | **RESOLVED** | Lines 1328–1343. |
| Deterministic work gate | **RESOLVED for parallel block attempts** | Lines 943–971. |
| Interoperability qualification | **RESOLVED** | Lines 1391–1401. |
| Fixed worker pool | **RESOLVED** | Lines 922–925. |
| RSS method | **RESOLVED** | Lines 991–1011. |
| Ownership/task/RSS/deferral follow-ups | **RESOLVED** | §§4.4, 4.9, and Decision 11. |

### Round-10 findings 1–9

| # | Status |
|---:|---|
| 1 — `Base.Dict` capacity/rehash | **RESOLVED for the original claim** — hashing and `Dict` are removed from generic maps, lines 484–490. |
| 2 — Schema/plan graph accounting | **PARTIALLY RESOLVED** — category (e) exists at lines 603–610, but its incremental tables lack an executable bounded-work representation. |
| 3 — Identity-bearing storage oracle | **RESOLVED** — lines 583–586. |
| 4 — XZ Stream Padding | **RESOLVED** — lines 875–888. |
| 5 — Stale codec/map clauses | **RESOLVED for the reported clauses** — lines 1586–1601. |
| 6 — Empty/skippable Zstandard frames | **RESOLVED** — lines 869–888. |
| 7 — Output/yield ownership | **RESOLVED** — lines 611–615. |
| 8 — RSS sampler | **RESOLVED** — lines 991–1011. |
| 9 — Runtime layout guard | **RESOLVED for measured shells** — lines 556–586. |

### Round-11 findings 1–9

| # | Status | Evidence |
|---:|---|---|
| 1 — Exact `Avro.Map` formula | **PARTIALLY RESOLVED** | Retained capacities are deterministic, but construction omits its `npairs` permutation and exact scratch, lines 484–490 and 569–576. |
| 2 — JSON.jl duplicate `Set` | **RESOLVED** | Lines 320–327 and 603–607. |
| 3 — Typed conversion boundary | **RESOLVED** | Lines 800–825. |
| 4 — Schema-operation limits | **RESOLVED** | Lines 627–634 and 1222–1233. |
| 5 — Writer category-(e) peak | **RESOLVED for the prior claim** | Lines 526–541. |
| 6 — Map probe/work accounting | **RESOLVED for returned `Avro.Map`** | Binary search and charged construction are specified at lines 484–508. |
| 7 — Map capacity terms | **PARTIALLY RESOLVED** | Final capacities are named; the construction peak is incomplete. |
| 8 — Stale summaries | **RESOLVED** | Lines 1775–1799 and 1882–1886. |
| 9 — Layout global-state exception | **RESOLVED** | Lines 207–213 and 1797–1799. |

### Round-12 findings 1–12

| # | Status | Evidence |
|---:|---|---|
| 1 — Rebuilds versus map accounting | **PARTIALLY RESOLVED** | Rebuilds are gone, but the replacement allocation formula is incomplete. |
| 2 — Seed-dependent rejection | **RESOLVED** | No seeds or hash probes remain, lines 484–490. |
| 3 — Bounded schema `IO` | **RESOLVED** | Lines 297–303. |
| 4 — Invalid UTF-8 and escapes | **PARTIALLY RESOLVED** | Raw UTF-8 and escape syntax are covered. Aliases, ignored namespaces, and metadata keys still contradict the contextual policy. |
| 5 — Duplicate-key work | **RESOLVED for Unicode-scalar keys** | Lines 320–327 and 499–508. |
| 6 — `schema(x)` budget | **RESOLVED** | Lines 627–634 and 1226. |
| 7 — User constructors | **RESOLVED** | Generated `Expr(:new)` construction: lines 800–814. |
| 8 — Decoded-key equality | **PARTIALLY RESOLVED** | Ordinary escaped keys are handled. Lone-surrogate metadata keys lack a safe representation. |
| 9 — Codec-member work | **RESOLVED** | Lines 510–514 and 869–875. |
| 10 — Owned source spans | **RESOLVED** | Lines 286–291. |
| 11 — Typed shells | **RESOLVED** | Lines 577–586 and 1605–1607. |
| 12 — Category/global summaries | **RESOLVED** | Lines 1775–1799 and Decision 30. |

The Round-12 follow-ups for codec traversal, source ownership, typed-shell oracles, category summaries, caller map lookups, and accepted deferrals are resolved. Decoded-key equality remains partial only for the surrogate contexts described below.

### Round-13 new findings 1–11

| # | Item | Status | Evidence |
|---:|---|---|---|
| 1 | Random map acceptance | **PARTIALLY RESOLVED** | Randomness is gone, but the replacement cannot build duplicate-heavy maps within its declared allocation formula. |
| 2 | Admission contradiction/work | **PARTIALLY RESOLVED** | The table now has one coherent sorted-runs design. Its amortized merge cost does not satisfy the current per-operation comparison budget. |
| 3 | Immutable struct construction | **RESOLVED** | Lines 800–814. Read-only probes passed mutable, immutable, zero-field, and reference-bearing types on Julia 1.10.11 and 1.12.6. |
| 4 | Typed-shell undercharge | **RESOLVED** | Per-`T` measured shells: lines 577–586. |
| 5 | Stale map gate | **RESOLVED for the old contradiction** | The rebuild/third-failure requirements are gone at lines 1593–1601. |
| 6 | Lone-surrogate metadata | **PARTIALLY RESOLVED** | Plain `doc` and property values are addressed. Aliases, ignored namespaces, custom property names, and nested metadata keys are not. |
| 7 | Post-transfer map lookup budget | **RESOLVED** | Lines 487–489 and 694. |
| 8 | Non-contiguous byte sources | **RESOLVED for the stated byte-vector claim** | Lines 316–319 and 593–600. The `AbstractString` case is a new defect. |
| 9 | Skippable-frame charge | **RESOLVED** | Lines 869–875. |
| 10 | Budget scope and decoded-key wording | **RESOLVED** | Lines 627–634 and 1087–1091. |
| 11 | Fixture and latency summaries | **RESOLVED** | Lines 543–549 and 1777–1781. |

The five Round-13 non-blocking follow-ups, findings 7–11, are resolved for their exact original claims. The accepted deferrals remain explicit at lines 1830–1831.

## New findings

1. **[major] Duplicate-heavy `Avro.Map` construction cannot satisfy its allocation contract.**

   **Claim:** The plan sizes sorting structures from `nunique`, but `nunique` is unknown until all `npairs` have participated in duplicate detection.

   **Evidence:** [Lines 484–490](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:484) require deterministic merge sorting. [Lines 569–576](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:569) charge only a final `nunique` permutation and transient scratch. Lines 618–620 require last-wins compaction from decoded pairs. An all-duplicate map can have `npairs = 1_000_000` and `nunique = 1`; it still needs to represent and compare all one million candidate indices.

   The scratch formula is also wrong for odd lengths. Julia’s MergeSort requires `cld(n, 2)` entries, not `n ÷ 2` ([Julia 1.12 sort.jl](/Users/jacob.quinn/.julia/juliaup/julia-1.12.6+0.aarch64.apple.darwin14/Julia-1.12.app/Contents/Resources/julia/share/julia/base/sort.jl:2465)). Read-only probes produced:

   ```text
   n=23   scratch_before=11  scratch_after=12  alloc=336
   n=1001 scratch_before=500 scratch_after=501 alloc=7712
   ```

   **Recommendation:** Reserve an `npairs` `Int32` permutation and `cld(npairs, 2)` scratch. Sort all pairs, determine unique groups and last values, then allocate or compact the final permutation. Charge the full overlap. Prefer an in-package mergesort with caller-owned scratch. Add odd-size and near-ceiling all-duplicate gates.

2. **[major] The surrogate policy still rejects or corrupts spec-permitted schema text.**

   **Claim:** Context is determined too early and does not cover aliases or metadata keys.

   **Evidence:** The plan says a namespace ignored by the fullname algorithm is never validated at [line 156](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:156). It nevertheless rejects lone surrogates in every namespace and alias at lines 303–311, before the fullname algorithm at lines 328–330. It also promises custom properties are preserved without validation at lines 269–273, while lines 320–327 decode every object key as UTF-8.

   The specification says a dotted name makes the supplied namespace irrelevant ([spec:248–252](/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:248)). It permits unknown metadata attributes ([spec:43](/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:43)). It explicitly says any string is accepted as an alias and uses invalid aliases for schema repair ([spec:262–279](/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:262)).

   Java, avro-py, and fastavro all accepted:

   ```json
   {"type":"record","name":"a.R","namespace":"\uD800","fields":[]}
   ```

   Java canonicalized it to `{"name":"a.R","type":"record","fields":[]}`. Avro-py and fastavro also retained alias `"\uD800"`. All three accepted a custom property whose name and nested key contain lone surrogates.

   JSON.jl cannot supply the proposed ordinary decoded-key representation. Its current unescape path produced:

   ```text
   "\ud8000"  ed a0 80 30
   "\ud8011"  ed a0 81 31
   ```

   **Recommendation:** Apply the fullname algorithm before contextual namespace validation. Represent aliases and unknown property names/value subtrees with owned code-unit-safe tokens, not ordinary decoded UTF-8 strings from JSON.jl. Define duplicate detection, equality, hashing, and re-emission over that representation. Add ignored-namespace, invalid-name repair, alias, top-level property-name, nested-key, escaped-duplicate, and OCF fixtures.

3. **[major] Pointer-incompatible `AbstractString` inputs allocate outside the ceiling.**

   **Claim:** The source-normalization rule covers byte vectors but not the other public JSON source family.

   **Evidence:** `parseschema` accepts all `AbstractString`s at [line 1222](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1222). Line 299 says strings are used in place. Lines 316–319 and category (c), lines 593–600, only normalize byte sources. JSON.jl copies an `AbstractString` when `pointer(buf, 1)` is unavailable ([JSON lazy.jl:95–104](/Users/jacob.quinn/.julia/dev/JSON/src/lazy.jl:95)).

   A warmed Julia 1.12 probe on the same 1,000,008-byte JSON text measured:

   ```text
   String_alloc=96
   pointerless_AbstractString_alloc=3711792
   ```

   This copy occurs inside JSON.jl, after Avro’s reservation boundary.

   **Recommendation:** Detect pointer-compatible contiguous strings too. Otherwise reserve and copy into an Avro-owned `String` or byte vector before pre-scan and `JSON.lazy`. Apply this to `parseschema` and `fromjson`, with exact-boundary custom-string tests.

4. **[major] Incremental schema and resolution tables have no bounded-work representation.**

   **Claim:** Category (e) permits only structures that do not implement the required incremental cyclic algorithms safely.

   **Evidence:** Equality incrementally discovers visited pairs at [lines 260–266](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:260). `ParseContext` registers named types during traversal at lines 328–344. Plan construction keys recursive nodes by identity at lines 638–643. Resolution memoizes newly discovered writer/reader pairs at lines 712–715.

   Category (e), lines 603–608, permits sorted vectors or the build-once `Avro.Map`. A build-once map cannot accept DFS discoveries. Sorted-vector insertion can perform quadratic comparisons and moves. Those operations are not charged by `max_resolution_work`, which counts only match attempts, memo entries, and plan nodes.

   **Recommendation:** Specify deterministic incremental structures. For example, assign dense stable node IDs and use a bounded pair tree, or use charged balanced trees. Count every comparison, insertion, rotation, and move. Add reverse-ordered fullname, cyclic equality, and near-`max_resolution_work` adversarial tests.

5. **[major] The symbol-admission LSM cannot satisfy its per-operation work rule.**

   **Claim:** Amortized movement does not bound the operation that triggers a carry merge.

   **Evidence:** [Lines 491–497](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:491) merge every equal-sized run when the 1,024-entry recent buffer fills. Lines 499–508 charge every move and comparison to the current admitting operation. At the 16,384-name boundary, one admission can trigger 30,720 moved indices. At four charged bytes each, movement alone costs 122,880 bytes. The provisional default allowance is 65,536 at lines 433–435. A small one-symbol operation therefore fails well before the advertised one-million-name quota.

   **Recommendation:** Deamortize merges using a fixed step per admission, or retain prepaid merge credits in the admission object. Search both old and new runs during incremental merging. Make failed staging transactional. Gate one-symbol-per-operation admission across every power-of-two boundary through `max_names`.

6. **[major] `push!`, `resize!`, and `sizehint!` invalidate exact pre-allocation accounting.**

   **Claim:** Julia can allocate more vector capacity than requested. The plan neither reserves that hidden capacity nor measures it reliably.

   **Evidence:** Generic collections use `sizehint!` and `push!` at lines 616–620. Streamed columns use geometric `sizehint!`/`resize!` growth at lines 919–945. Julia 1.12 explicitly applies an over-allocation heuristic ([array.jl:1144–1148](/Users/jacob.quinn/.julia/juliaup/julia-1.12.6+0.aarch64.apple.darwin14/Julia-1.12.app/Contents/Resources/julia/share/julia/base/array.jl:1144)).

   A read-only probe of `sizehint!(Int[], 1)` returned `Base.summarysize == 64` on Julia 1.12, consistent with three retained slots rather than one. Julia 1.10 returned 40, which hides the retained capacity from the proposed `summarysize` oracle. The package therefore cannot prove that every allocation was preceded by a sufficient reservation.

   **Recommendation:** Use package-owned builders with explicit replacement arrays: allocate the exact new capacity, reserve old plus replacement storage, copy, then release the old reservation. Do not let Base growth operations allocate implicitly. Apply the rule to generic collections, streamed columns, encoders, owned JSON/IO buffers, and compressor buffers.

## Non-blocking follow-ups (if any)

- [minor] Validate `npairs ≤ typemax(Int32)` before using `Int32` map permutations, or use `Int`.
- [minor] Reserve a conservative shell before creating the per-`T` probe whose size is not yet known.
- [minor] Clarify whether `Avro.Table` truly supports a typed `T`; it is listed as a typed Symbol path, but its public signature has no `T`.
- [minor] Define behavior when `==`, `show`, or other Base schema operations need more than their fresh default scratch after a schema was admitted with raised limits.
- [minor] Replace the stale “probes” and “equal-hash keys” wording at lines 193 and 1586–1588.
- [nit] Put Decision 43 before Decision 44 at lines 1922–1925.

The scope remains credible. RPC/protocols, IDL, `big-decimal`, inference, append mode, borrowed views, and parallel compression remain acceptable deferrals.

## Milestone and gate assessment

- Phase 1 is blocked by the surrogate contexts, pointer-incompatible strings, and incremental ParseContext/equality-table design.
- Phase 2 is blocked by map construction, admission liveness, and implicit vector growth. Its current storage and allocation-hook gates cannot pass faithfully.
- Phase 3 is blocked by the unresolved resolution-memo representation.
- Phase 4a needs surrogate-bearing schema/OCF interoperability fixtures.
- Phase 4b is blocked by implicit streamed-column growth and admission liveness.
- The remaining binary, datum-JSON, single-object, sort, OCF/codec, resolution-rule, canonical-form, fingerprint, logical-type, interoperability, fuzz, and benchmark coverage is strong.
- The §2.2 audit table is unchanged. I found no new inaccurate audit claim.
- PR-ready, merge-ready, RC-ready, and release-ready remain correctly separated and ordered.

Assumptions: DRAFT v14 is authoritative. Declared fixtures and gates are implementation deliverables, not review preconditions. The pinned specification is normative; oracle behavior is supporting evidence.

Decisions without user direction: I kept the recorded deferrals. I treated a replacement as partial when it still failed the same safety or conformance guarantee. I classified implicit Base allocations as major because the plan promises reservation before every package allocation.

Validation: I inspected DRAFT v14, response-13, codex-review-13, the pinned specification, JSON.jl, and Julia Base on 1.10.11 and 1.12.6. Read-only probes covered `Expr(:new)`, MergeSort scratch, vector over-allocation, pointer-incompatible strings, surrogate namespaces/aliases/property keys, and Java/avro-py/fastavro parsing. No file was created or modified.

## Verdict

VERDICT: REVISE
