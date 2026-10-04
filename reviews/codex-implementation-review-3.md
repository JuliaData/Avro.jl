# Avro.jl 2.0 implementation review — round 3

Date: 2026-08-24

Review base and received revision head: `c9658a6`

Implementation head after round-three repairs: `8229c11`

I would not ship this tree as Avro.jl 2.0.0. The revision closes D04, D05, D07, and D08. It closes
the original limit-rejection part of D10 and the projection part of D09. It does not implement exact
accounting across construction, printing, caches, typed plans, or symbol admission. A new live probe
also proves that resident allocations remain published as pending reservations. Finally, the required
stand-alone compile gate fails twice on Julia 1.10.

## Scope and method

I reviewed `c9658a6..8229c11` against `AVRO_REWRITE_PLAN.md` v24, `../AGENTS.md`, and each D01–D11
claim in the round-two report. I inspected allocation order, reservation settlement, exact release
order, nested budget ownership, worker teardown, poisoning, and admission mutation. I ran the full
supported-version suites and the performance, quality, interop, RSS, smoke, typed-layout, and cold
compile checks listed below. Behavioral repairs started with focused failing regressions.

I kept the pre-existing untracked `test/Manifest.toml` unchanged. I did not fetch, push, change a
remote, rewrite history, or modify another checkout.

## Round-two finding dispositions

| Finding | Verdict | Current-head evidence |
|---|---|---|
| D01 — exact construction budget | **REVISE** | The shared outer scope exists at `src/schema.jl:1379-1390`, but allocation still precedes exact charge. `Props()` and `String(k)` precede or participate in the charge at `src/schema.jl:1320-1324`; frozen vectors and dictionaries then grow with `push!` and insertion at `src/schema.jl:1346-1364`. Finalizer memo tables are allocated before the first per-node reserve at `src/schema.jl:1421-1423,1459-1477`. Name and alias copies precede their reserves at `src/schema.jl:1521-1538`, and the enum symbol vector and index are built without exact reservations at `src/schema.jl:1556-1573`. Derivation has the same dynamic vector/dictionary growth at `src/types.jl:190-223,230-251,263-302`. This does not satisfy plan lines 769-794 and 808-817. |
| D02 — printers, canonical form, and fingerprints | **REVISE** | The main structural changes are real: `BoundedWriter` now uses exact-capacity replacement at `src/schema.jl:899-1017`, and canonical, fingerprint, and equivalence each use one budget at `src/canonical.jl:11-17,132-160`. Exact lifetime is still wrong. Each dense `schemaseen` table remains charged after its print has finished, including before hashing and before the second equivalence print. `boundedgrow!` also releases the old buffer charge at `src/schema.jl:932` before replacing the last reference at line 933. Near a ceiling these extra or early charges can change acceptance. |
| D03 — SchemaCache and single-object ownership | **REVISE** | Vector and IO single-object decode now share one budget at `src/singleobject.jl:137-196`, including resolving and typed plans. Cache replacement vectors are still allocated without the caller operation budget at `src/singleobject.jl:63-79`, and collision equality starts from an uncharged memo at line 59. A custom `SchemaStore` still calls the public `lookup(...; limits=)` at `src/singleobject.jl:158-160`, which opens a separate budget because the interface has no shared-budget route. `encodesingle` reserves its final output at lines 118-119 but never marks that allocation resident. |
| D04 — exact Writer header vectors | **PASS** | Writer header and preflight mirror vectors have exact lengths and indexed assignment at `src/container.jl:760-775,789-798`. Reader metadata indexes use exact-capacity replacement, reserve new storage first, and release old storage after replacement at `src/container.jl:96-125,133-224`. Adjacent residency and codec-copy defects are covered by the cross-cutting finding below; they do not refute the narrow vector-growth repair. |
| D05 — parallel pool lifetime | **PASS** | Pool state participates in admission at `src/parallel.jl:411-423`. The reservation happens before `FailBox`, job storage, the channel, and tasks at lines 435-455. Partial startup drains workers before release at lines 457-466. Retirement and final teardown settle workers, abandon jobs, clear all local references, and only then release the pool at lines 477-485 and 529-544. Worker tasks are `errormonitor`ed, and the lowest-index poisoning path remains intact. |
| D06 — measured typed-shell protocol | **REVISE** | Exact memo-vector allocation and replacement exist at `src/typed.jl:102-172`; generated shell-oracle coverage exists at `test/typed.jl:305-405`; and the Julia-version-specific array shell transfer is at `src/typed.jl:639-647,730-759`. The plan graph itself is not exact. Many returned `TypedPlan` nodes are constructed with no reservation at `src/typed.jl:175-193,297-311,390-391,488-501,598`. Construction-only memo and tuple/vector storage is not released. `MapTarget` at `src/typed.jl:650-660,763-789` has no immutable inline-shell transfer equivalent to arrays. The required complete measured protocol in plan lines 740-750 is therefore incomplete. |
| D07 — deterministic baselines and parse allocation | **PASS** | `benchmarks/bench_1x.jl:3-4` now matches the deterministic 2.0 row workload. Five cold 1.1.2 processes are recorded in `benchmarks/logs/avro112.log:12-22`; their medians are 0.945999958 s for write and 5.051266584 s for read plus materialisation, matching `test/perf.jl:9-11`. The final performance run passed all gates. See the dispute adjudication below for the 2,062 parse allocations. |
| D08 — live interop completeness | **PASS** | Six-codec Java and fastavro value/schema/metadata/count/codec checks are at `test/interop.jl:245-326`; positive and sized array/map blocks at lines 410-455; both resolution policies and both oracles at lines 457-504; both Java sort comparators over all 51 vectors at lines 506-541; and recorded schema/datum/block/JSON verdicts at lines 561-700. The supplied tools passed 58/58 datum checks and 1,586/1,586 §8.5 checks after the live harness repairs. |
| D09 — compile and projection gates | **REVISE** | Projection is complete: `test/projection.jl:184-218` partitions all 245 OCF fixtures into 81 readable records, 6 recorded codec rejects, 156 non-record roots, and 2 deliberate high-window rejects. Rows and Table cover every selection, both validation modes, and Table `ntasks ∈ {1,2,8}` at lines 232-354. The zero-column write path keeps its authoritative row count at `src/tables.jl:172-227`. The exact `<50 MB` assertion is restored at `test/gates.jl:181-183`, but final-head cold Julia 1.10 failed twice: 50.09375 MB and 51.9375 MB, both with zero new Avro specializations. Julia 1.12 passed at 36.8 MB and Julia 1.11 passed at 28.1 MB. Full-suite warm order is not a substitute for the required stand-alone result on every supported version. |
| D10 — transactional symbol admission | **REVISE** | The original limit failure is fixed: a new admission preflights the next cascade before mutation at `src/admission.jl:178-185`, and `test/admission.jl:98-134` proves identical repeated `LimitError` values and state. The table is not fully allocation-transactional or exact. Merge scratch is allocated directly at `src/admission.jl:65-75`; carry uses `sort!(copy(...))`, `empty!`, and `push!` at lines 110-115; and maintenance advances before the retained `String` and recent-vector growth at lines 185-189. An allocation failure can therefore follow maintenance mutation. `max_bytes` also omits deterministic shells, capacities, and replacement overlap required by plan lines 606-620. |
| D11 — repository style | **REVISE** | The long-form scan and empty-literal subparts are fixed: `src/` and `ext/` have zero implicit `function ... end` returns, zero `Any[]`, and zero plain `Vector{T}()` constructors. A recursive `Meta.parseall` scan still finds 581 assignment-form method definitions in `src/` and `ext/`, including `src/canonical.jl:104` and `src/limits.jl:226`. `../AGENTS.md` says “Always use an explicit `return` in functions,” and plan lines 1999-2000 incorporate that rule without an assignment-form exception. The recorded local-idiom exception cannot override either document. The touched `readheader`, `Writer`, and `decodeblocks!` paths also remain larger than the small-focused-function rule prefers. |

## New high-severity finding — resident allocations remain globally pending

Plan lines 562-568 require the global guard to subtract only reservations that are not resident. The
implementation subtracts `GUARD.pending` in `available_memory()` at `src/limits.jl:195-199` and relies
on `allocated!` at lines 301-314 to settle a successful allocation. Current `src/` has 125 `reserve!`
call sites but only 23 `allocated!` references, four of which are the bookkeeping definition and its
own release/close calls. Concrete missing settlements include Writer header storage at
`src/container.jl:762-797`, construction storage described under D01, and the final single-object
buffer at `src/singleobject.jl:118-125`.

A final-head live probe kept a Writer with 2 MiB of copied user metadata open. It reported:

```text
budget_reserved=4195604
budget_pending=4195501
budget_published=4195129
guard_delta=4195129
guard_after_close_delta=0
```

Almost every live byte was already resident but remained published as pending. The OS/cgroup counters
already see those resident pages, so `available_memory()` subtracts them again. A concurrent operation
can therefore receive a falsely low, state-dependent `LimitError(:available_memory).observed`. The
clamp in `allocated!` at `src/limits.jl:307` also hides settlement mismatches instead of detecting them.

## Dispute adjudications

### Parse allocations — accepted

The plan table labels its third column “informational” at `AVRO_REWRITE_PLAN.md:1937`; the
`parseschema(interop.avsc)` row has no ratio gate and places both 100 µs and 300 allocations in that
column at line 1947. The final run measured 51.8 µs and 2,062 allocations. I accept the calibrated
`<= 2,300` regression bound at `test/perf.jl:58-63`. The 300 value remains a reported miss, not a
release failure.

### Explicit returns — rejected

I accept the claim that all long-form functions now return explicitly. I reject the claimed exemption
for assignment-form methods. Neither `../AGENTS.md` nor the incorporated plan text states that
exception, and the current AST count is 581 functions. File-local and maintainer-wide idiom does not
change the written rule. I also fixed the two new typed empty constructors in `test/projection.jl` in
`8229c11`.

### Historical commit subjects — recorded, not repaired

The round-two commits `442c68e`, `c746e39`, `aa8e853`, `bd9c527`, and `67a260a` have noun-phrase
subjects rather than present-imperative summaries. The explicit no-history-rewrite boundary prevents
repair. Every commit added in this review has an imperative subject and exactly one required Codex
trailer.

## Findings fixed in this worktree

| Commits | Repair |
|---|---|
| `9b53d5c`, `d6644c1`, `78a9650` | Aligned and re-recorded the 1.1.2 workload; restored the exact compile threshold and exhaustive projection surface. |
| `8064cf3`, `29f12b2`, `8229c11` | Preserved zero-column row counts, declared the test dependency, and used required typed empty literals. |
| `0072ead`, `e2a6359` | Added cascading admission preflight and rejected budget-release underflow. |
| `cbddd2d`, `276a676`, `dac9d95`, `bd1d853` | Added bounded identity/accounting repairs and exact reader metadata indexes; mirrored the reader codec-name charge. |
| `c5c37a2`, `8c0a3ca`, `3a0d950` | Held the parallel pool through teardown, restored legacy null-cushion acceptance in fast mode, and made typed shell transfer match Julia 1.10 versus 1.11/1.12 layouts. |
| `e66bca7`, `f0d457f`, `b19dce0`, `ff5207e` | Completed the interop fixtures, kept negative schemas out of the valid sweep, fixed Java metadata argument order, and encoded sort vectors through the Java harness. |

## Verification results

The full product-source matrix below ran at `3a0d950`. The only later implementation-tree change,
`8229c11`, replaces two equivalent empty test constructors and does not change package source. The
stand-alone compile runs and all static checks ran after `8229c11`.

| Verification | Result |
|---|---|
| `AVRO_QUALITY_GATES=true julia +1.12 --project=. -e 'using Pkg; Pkg.test(julia_args=["-t4"])'` | 61,337/61,337 passed in 14m22.7s. Aqua and JET passed. Compile gate: zero new specializations and 38.1 MB RSS growth. Latency seconds: 1.9289 dense, 1.8449 column, 4.8529 depth-80, 0.2325 arrays, 0.3867 fromjson, 0.1403 map. |
| `AVRO_PERF=true` full Julia 1.12 suite, `-t8` | 61,342/61,342 passed in 16m00.2s. Write 0.105 s (9.0x vs 1.1.2); Table 0.088 s (57.2x); 8-task ratio 3.93; codec overhead zstd 0.98, deflate 1.05, snappy 0.98; projection 2.3x; prepared decode 1 allocation/29 ns; encode 0/41 ns; load 0.20 s; TTFT 0.55 s. |
| Julia 1.10 full suite, `-t4` | 61,326/61,326 passed in 12m30.0s. Compile gate: zero new specializations and 0.0 MB incremental RSS in suite order. |
| Julia 1.11 full suite with `--compiled-modules=no` on the driver and in `julia_args`, `-t4` | 61,326/61,326 passed in 17m53.9s. Compile gate: zero new specializations and 0.0 MB incremental RSS in suite order. |
| Stand-alone cold compile gate | Julia 1.12: 1,175/1,175, zero new, 36.8 MB, 2m08.8s. Julia 1.11 with compiled modules disabled: 1,175/1,175, zero new, 28.1 MB, 2m09.7s. Julia 1.10: 1,174/1,175 twice, zero new, **50.09375 MB** and **51.9375 MB**, 1m38.1s and 1m39.6s. |
| Supplied avro-tools 1.12.2 and supplied fastavro environment | 58/58 datum checks in 25s; 1,586/1,586 §8.5 matrix checks in 8m14.3s. |
| `AVRO_RSS_GATE=true`, Julia 1.12 `-t8` | 6/6 passed in 15.0s. A 2.2 MB file decoded to 2.0 GiB; baseline 316.7 MB, peak 3,076.8 MB, 4,096 MB ceiling, seven workers, in-flight high-water eight. |
| `AVRO_SMOKE=true`, Julia 1.12 | 5/5 passed in 11.4s. |
| Focused generated typed-layout suite | 200/200 passed on Julia 1.10 and Julia 1.12 after the version-specific shell-transfer repair. |
| Five cold Avro.jl 1.1.2 processes | Write samples: 1.1051045, 0.945999958, 1.203622292, 0.926025875, 0.937682916 s. Read-plus-materialise samples: 5.296807, 5.042072541, 5.109198583, 5.051266584, 5.032883875 s. Every output was 22,050,364 bytes. |
| Static audits | Recursive AST scan: 581 assignment-form methods. `Any[]`: zero. Plain `Vector{T}()` in `src/`/`ext/`: zero. `git diff --check`: clean. Every round-three commit has exactly one `Co-Authored-By: Codex <codex@openai.com>` trailer. |

The first full 1.12 attempt found a real legacy fast-projection regression and led to `8c0a3ca`. Two
live interop attempts then found the Java metadata argument-order defect and the missing Java encoded
sort helper, leading to `b19dce0` and `ff5207e`. The final runs above include those repairs.

I did not rerun docs, fixture generation, or cross-version artifact exchange after the decisive source
and release-gate failures above. Their round-two evidence is not used to claim current release
readiness.

## Assumptions and review decisions

* I treated plan v24 and `../AGENTS.md` as authoritative. A status-file rationale did not waive either.
* I separated narrow repaired findings from adjacent defects. This is why D04 and D05 pass while the
  global residency finding still blocks the same broad resource-safety surface.
* I did not weaken or add warm-order exceptions to the 50 MB compile gate after Julia 1.10 failed. Two
  isolated cold failures establish that D09 is not closed.
* I fixed bounded defects with clear ownership. I stopped short of a broad allocation-framework and
  whole-tree return rewrite because those changes require new design, regressions, and a fresh matrix.
  The `REVISE` verdict records that work; it is not approval of the current deviations.
* I preserved the unrelated untracked `test/Manifest.toml` and all remote state.

## Required revision work

1. Finish exact construction and printer accounting. Reserve before every schema/field/name/property,
   derivation table, and replacement allocation; use exact-capacity builders; release dead seen and
   construction memo storage in exact order.
2. Repair global guard residency. Every successful guarded allocation must call `allocated!`
   immediately, every failure must unwind the exact reservation, and mismatched settlement must fail
   instead of clamp. Add a concurrent-budget regression that proves resident bytes are not subtracted
   twice from `available_memory()` and that `LimitError.observed` is pure.
3. Keep SchemaCache, custom-store lookup, generic read/write/resolution plans, and single-object output
   in one exact caller budget. Charge cache replacement overlap and every plan/memo node.
4. Complete typed-plan accounting. Charge returned nodes and tuples, release construction-only storage,
   and implement exact immutable inline-shell transfer for typed maps as well as arrays.
5. Make SymbolAdmission exact and allocation-transactional. Prebuild exact recent/run/scratch
   replacements under `max_bytes`, hold replacement overlap, and publish maintenance and the new name
   only after every allocation succeeds.
6. Reduce the final-head Julia 1.10 stand-alone compile RSS result below the specified 50 MB without
   weakening, reordering, or adding a warm-only exception to the gate.
7. Apply the literal explicit-return rule to all 581 assignment-form methods in `src/` and `ext/`, and
   split the oversized touched functions where the documented small-function rule requires it.
8. After items 1–7, rerun the supported-version full suites, stand-alone cold compile gates, quality,
   performance, interop, RSS, smoke, docs, generator, and cross-version matrix from one final source
   head.

VERDICT: REVISE
