# Avro.jl 2.0 implementation review — round 4

Date: 2026-08-24

Round-three fixed point: `8c53ac7`

Received revision head: `a79c80c`

Implementation head after round-four repairs: `900ef66`

I would not ship this tree as Avro.jl 2.0.0. The revision fixes important parts of all eight requested
items. It does not close the exact-allocation contract in schema construction, plan construction,
global settlement order, typed-plan fallback, or symbol admission. The item-6 change also modifies the
specified warm-up. After I restored the specified protocol, the current head failed one of two fresh
Julia 1.10 runs at 54.1 MB.

## Scope and method

I reviewed `8c53ac7..a79c80c` against `AVRO_REWRITE_PLAN.md` v24, `../AGENTS.md`, and the round-three
required-work list. I then re-audited every changed allocation, release, custom-store, typed-plan,
admission, and parallel-pool path. I used two independent reviews as required by the repository review
workflow: one for repository standards and one for the requested implementation specification.

I preserved the pre-existing untracked `test/Manifest.toml`. I did not fetch, push, change remote
state, rewrite history, or edit another checkout.

## Round-three item dispositions

### 1. Exact construction and printer accounting — REVISE

The printer work is correct. `BoundedWriter` reserves exact replacement capacity, settles after
allocation, rebinds before releasing the old buffer, and releases each seen table when printing ends
at `src/schema.jl:1126-1258` and `src/canonical.jl:11-18,136-169`.

Construction and derivation are still not exact. `makeprops` charges a requested capacity and then
uses `sizehint!` and insertion growth at `src/schema.jl:1576-1604`. JSON properties use the same pattern
at `src/schema.jl:1627-1652`. Public aliases, enum symbols, field aliases, and record fields start from
empty vectors and use `push!` at `src/schema.jl:1817-1838,1856-1874,1933-1956,1980-2002`.
Derivation uses `sizehint!` and `push!` at `src/types.jl:196-214,220-238,253-269,289-331`.

A live Julia 1.12 probe showed the accounting mismatch:

```text
enum_len=1 enum_capacity=3
derived_fields_len=1 derived_fields_capacity=3
props_len=1 props_capacity=3
```

This violates the authoritative growth rule at `AVRO_REWRITE_PLAN.md:787-794`.

### 2. Global guard residency — REVISE

The counter contract is stronger. Oversettlement, resident-release mismatch, pending-unreserve
mismatch, and negative settlement values now throw at `src/limits.jl:335-382`. Checkpoint rollback
uses both the reserved and pending counters at `src/limits.jl:395-414`. The focused limits tests pass.

The global allocation order is still wrong. `charge!` calls `reserve!` and `allocated!` back to back at
`src/limits.jl:343-351`, before the caller performs its allocation. Construction uses that shorthand
before `Props()`, strings, vectors, nodes, and other storage. Native decoder workspaces are also marked
resident before `TranscodingStreams.initialize` allocates them at `src/codecs.jl:214-220,261-284` and
in both codec extensions. Therefore the guard can stop publishing a reservation while the allocation
is still pending.

The claimed concurrency regression is not concurrent. It creates and closes each holder sequentially
and injects a fixed `available=` value at `test/limits.jl:162-183`. That bypasses
`available_memory()` and cannot prove that a real concurrent operation receives a pure
`LimitError.observed` value.

### 3. SchemaCache, custom stores, plans, and single-object ownership — REVISE

The cache and single-object defects found in this round are fixed in `05a8320`. Cache replacements now
reserve and settle each exact vector separately and publish only after both vectors and all copies
succeed at `src/singleobject.jl:75-100`. Collision equality now uses an exact per-node table with exact
partner replacements and releases it before return at `src/schema.jl:969-1043`. A custom store used by
`decodesingle` must implement the shared-budget three-argument lookup at
`src/singleobject.jl:115-132`; the legacy nested-budget fallback is rejected. The single-object output
is settled at `src/singleobject.jl:141-162`.

Plan construction remains outside the exact allocation contract. The read and write memo vectors are
allocated without reservation at `src/plan_read.jl:66-78` and `src/plan_write.jl:52-64`. Record and
union plans use dynamic vectors and `push!` at `src/plan_read.jl:143-153` and
`src/plan_write.jl:128-137`. Resolution memo and result vectors use `push!` and `insert!` at
`src/resolution.jl:142-173,294-312,332-371`. These objects share the caller budget nominally, but their
actual storage is not reserved, settled, and released exactly.

### 4. Complete typed-plan accounting — REVISE

The revision adds concrete node charges, tuple charges, capture release, and the typed-map inline-shell
transfer at `src/typed.jl:205-245,830-860,970-1001`. The map layout regression passes.

This round fixed two missed exit paths in `05a8320`. Reader-default validation now releases the
temporary materialized value before charging the retained node at `src/typed.jl:700-720`. Any failed
typed-plan build now rolls the complete budget delta back after its construction frame unwinds at
`src/typed.jl:124-160`. The new regressions are at `test/typed.jl:458-472`.

The plan is still not exact. Node and memo-pair paths use `charge!` before allocation at
`src/typed.jl:174-220,700-720`. More importantly, the implementation explicitly leaves abandoned
fallback nodes charged until the operation ends at `src/typed.jl:205-212`. Those nodes are no longer
owned by the returned plan after the construction memo is released. This does not meet the required
construction-scratch release rule.

### 5. Exact, allocation-transactional SymbolAdmission — REVISE

The retained string, carried run, and predicted merge output are prebuilt before the accepted-name
mutation at `src/admission.jl:210-233`. Live merge output slots are counted until their sources are
replaced. These changes close the original predictable cascade failure.

The table is not exact or fully allocation-transactional. Its constructor allocates a 1,024-slot
recent vector and an outer runs vector but records `bytes == 0` at `src/admission.jl:46-52`.
`carry_unlocked!` mutates the recent vector and then grows `runs` with `push!` at
`src/admission.jl:124-131`. That outer-vector allocation can occur after maintenance and admission
mutation. `sort!` also occurs after the accepted name is published at `src/admission.jl:225-232`.

A live Julia 1.12 probe reported:

```text
initial_bytes=0 recent_capacity=1024 runs_capacity=0
after_carry_bytes=12205 recent_capacity=1024 runs_len=1 runs_capacity=3
```

The reported byte count therefore excludes live deterministic storage and outer replacement overlap.

### 6. Julia 1.10 stand-alone compile RSS — REVISE

I reject `ec90671` as the agreed fix. The plan specifies one warm schema for each member of `E` and
one builder for each `e`, followed by one measured batch of 1,000 random schemas at
`AVRO_REWRITE_PLAN.md:848-853`. The received gate calls `exercise(200)` before the baseline. Each
iteration creates five random schemas, so this adds an unmeasured 1,000-schema batch before the
specified measured batch.

This is a protocol change even though the assertion and measured loop remain textually unchanged.
`Sys.maxrss` is a process high-water mark. Collections between slices do not make it a live-retention
measurement and do not exclude transient inference or dispatch-cache peaks. The stated intent cannot
override the plan's explicit workload order.

I restored `exercise(2)` in `900ef66`. Before the other round-four source fixes, two isolated Julia
1.10 runs passed narrowly at 48.9 and 49.3 MB. At the final implementation head, two more isolated runs
reported 54.1 MB (failure) and 49.4 MB (pass), both with zero new Avro specializations. A gate that
fails one clean run is not closed.

### 7. Explicit returns and focused touched functions — REVISE

The assignment-form conversion is real. A recursive `Meta.parseall` scan finds zero assignment-form
methods in `src/` and `ext/`. This round added the missing outer `return` to the lock-based cache lookup
and restored the 33 missing blank separators introduced by the conversion. `git diff --check` is clean.

The touched `decodeblocks!` coordinator at `src/parallel.jl:511-594` is still 84 lines. It owns the
sequential branch, pool start and retirement, admission, direct-head decode, ordered commit,
poisoning, and teardown. The round-three split created helpers but did not split this oversized
function as required. `withplanbudget` at `src/container.jl:341-343` is also a no-op middle layer; this
is a minor depth issue, not the release blocker.

### 8. Full final-head matrix — REVISE

The matrix in `STATUS.md` ran at `39354db`, not at the received head `a79c80c` or the repaired head
`900ef66`. This round's current-head validation is listed below. I did not rerun the expensive
performance, interop, 2 GiB RSS, smoke, generator, or cross-version artifact legs after the source
audit and Julia 1.10 gate established release blockers. A complete matrix is required after the source
fixes converge.

## Independent review axes

### Standards review

The independent standards review found one missing explicit return in the cache lookup, 33 converted
methods without the required separator, one oversized `decodeblocks!` coordinator, and the minor
`withplanbudget` middle layer. Commits `05a8320` and `d6f3cb7` fix the return and all 33 separators.
The 84-line coordinator remains the blocking standards finding. The no-op layer remains a warning.

Standards totals at the final implementation head: **1 blocker, 1 warning**. Worst issue:
`decodeblocks!` still combines the full parallel lifecycle in one touched function.

### Specification review

The independent specification review found partial or failed implementation of items 1 through 7:
dynamic construction storage, pre-allocation settlement, an old custom-store fallback, typed scratch
leaks, omitted admission storage, the altered compile protocol, and incomplete function splitting.
This round fixes the custom-store fallback, collision memo, cache replacement transaction, negative
settlements, typed default scratch, failed typed-plan teardown, explicit return, and separators. The
remaining item-level failures are preserved above without merging or reranking them into the
standards axis.

Specification totals at the final implementation head: **6 blockers, 0 warnings**. Worst issue: the
authoritative exact-allocation contract is still violated by common schema, plan, and admission paths.

## Findings fixed in this worktree

| Commit | Repair |
|---|---|
| `05a8320` | Reject negative budget settlements; make cache replacement and collision equality exact and transactional; require shared-budget custom lookup; release typed default scratch and all failed typed-plan construction storage. |
| `d6f3cb7` | Restore the missing separators in methods converted during round three. |
| `900ef66` | Restore the plan's original compile-cost warm-up. |

Every behavioral repair started from a failing regression. The red results were three missing negative
settlement errors, 80 versus 32 retained bytes for a reader-default node, 478 leaked bytes after failed
typed-plan construction, acceptance of a legacy nested-budget store, and the missing budgeted equality
helper. All corresponding focused tests now pass.

## Verification results

| Verification | Result |
|---|---|
| Julia 1.12 full suite, `-t4` | **61,388/61,388 passed in 13m00.4s**. Compile gate: 32,221 specializations, zero new, 34.5 MB RSS. Latency seconds: 2.075 dense, 1.874 column, 4.772 depth-80, 0.153 arrays, 0.367 JSON, 0.140 map. |
| Focused limits/schema/typed/single-object/constructor tests | **1,171/1,171** on Julia 1.10; **1,171/1,171** on Julia 1.11 with `--compiled-modules=no`; the same focused files passed on Julia 1.12. |
| Stand-alone Julia 1.10 compile gate with restored protocol | Run 1: **1,174/1,175**, zero new, **54.1 MB**. Run 2: **1,175/1,175**, zero new, 49.4 MB. |
| Aqua and JET, Julia 1.12 | Aqua **10/10**; JET **1/1**, no reports. |
| Strict Documenter build | Passed from an isolated temporary environment that developed this worktree. The committed docs manifest points at the main checkout and was not used as evidence. |
| Static checks | Assignment-form methods in `src/` and `ext/`: **0**. `decodeblocks!`: **84 lines**. `git diff --check`: clean. Each round-four commit has exactly one required Codex trailer. |

The first standalone quality command used the root project and could not load test-only Aqua. I reran
it with the test environment and this worktree on the load path; Aqua and JET then passed. The first
docs command followed the committed absolute manifest path to the main checkout. I discarded that
signal and reran Documenter in an isolated environment developed against this worktree.

## Assumptions and decisions

* I treated the plan text and `../AGENTS.md` as authoritative. `STATUS.md` is evidence, not a waiver.
* I interpreted “high-water delta measures retention” as the intended reason for the collection
  slices, not permission to insert another full random workload before the specified baseline.
* I made the shared-budget custom-store method mandatory. A fallback to the public keyword method
  cannot prove that `decodesingle` has one operation budget.
* I fixed bounded ownership defects with focused regressions. I did not attempt a second rewrite of
  schema builders, all plan families, symbol admission, or the parallel coordinator inside this review.
* I stopped the expensive optional matrix after the source audit and one final-head Julia 1.10 gate
  failure established that the branch is not PR-ready.
* I preserved the unrelated untracked `test/Manifest.toml` and all remote state.

## Required revision work

1. Replace every charged construction and derivation `sizehint!`, `push!`, insertion, and comprehension
   with exact-capacity or exact-replacement builders. Reserve, allocate, settle, publish, and release in
   that order. Apply the same rule to generic read, write, and resolving plans and their memo tables.
2. Remove pre-allocation settlement. Replace `charge!` at allocation sites with explicit reservation
   and post-allocation settlement, including native codec initialization. Add a real overlapping-task
   guard regression that calls `available_memory()` and proves stable `LimitError.observed` values.
3. Release every abandoned typed-plan node and default on fallback after memo ownership is known. Keep
   the successful returned graph charged, and make all node and pair construction allocation-transactional.
4. Redesign `SymbolAdmission` so `max_bytes` includes recent/run shells, capacities, and replacement
   overlap. Prebuild an exact outer runs replacement and finish every fallible copy/sort before any
   maintenance or accepted-name state is published.
5. Make the unchanged stand-alone Julia 1.10 protocol pass reliably below 50 MB. Do not add an
   unmeasured random-schema batch or another warm-only exception.
6. Split `decodeblocks!` into small functions while preserving pool lifetime, lowest-index poisoning,
   ordered commit, and exact teardown. Remove or justify the no-op `withplanbudget` layer.
7. After items 1–6, rerun the full Julia 1.10/1.11/1.12 suites and all quality, performance, interop,
   RSS, smoke, docs, generator, cross-version, and stand-alone compile gates from one final head.

VERDICT: REVISE
