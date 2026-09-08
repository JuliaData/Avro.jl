# Avro.jl 2.0 implementation review — round 2

Date: 2026-08-24

Review base: `59a1e85`

Revision head received for review: `c6faaa4`

Implementation head after round-two repairs: `168a896`

Status-record head: `37a027e`

I would not ship this tree as Avro.jl 2.0.0. The review fixed 26 bounded findings in 28 repair commits.
Eleven round-one items still fail the agreed contract or a mandatory release gate. Eight items are now
accepted.

## Scope and method

I reviewed `59a1e85..c6faaa4` against `AVRO_REWRITE_PLAN.md` v24 and `../AGENTS.md`. I then reviewed
each repair at the current head. I used public-seam regressions for behavioral fixes. I kept the
pre-existing untracked `test/Manifest.toml` unchanged. I did not fetch, push, rewrite history, or touch
another checkout.

The review concentrated on budget arithmetic, allocation order, writer/reader acceptance equivalence,
parallel failure identity, typed resolution, and live oracle behavior. These were the highest-risk new
surfaces in the revision commits.

## Open and deferred findings

D01–D11 are new round-two findings that remain deferred for the next revision. They are not accepted
deviations.

### High severity

#### D01 — Schema construction still allocates before its operation budget

Public constructors copy properties, defaults, aliases, symbols, and fields before `finalizepublic!`
opens its budget. For example, `build` calls `makeprops` at `src/schema.jl:1203-1213`, and
`makeprops` recursively builds frozen JSON at `src/schema.jl:1216-1258`. The final charged walk starts
later at `src/schema.jl:1287-1304`. `Avro.schema(T)` also opens a derivation budget and then calls a
finalizer that opens a second budget at `src/types.jl:101-106`. The output-size check added in this
review rejects large retained properties, but it rejects them after those copies exist.

This violates the one-operation and reserve-before-allocation rules in plan lines 769-794 and 808-818.
R01 remains disputed.

#### D02 — Schema printers and fingerprints do not have exact one-operation ownership

`BoundedWriter` wraps an unreserved `IOBuffer` and charges fixed 4 KiB steps without controlling the
buffer's replacement capacity (`src/schema.jl:892-927`). The canonical seen table mutates before its
charge at `src/canonical.jl:40-46`. `fingerprint` calls `canonical` as a separate operation and hashes
after that budget ends (`src/canonical.jl:112-117`). `parsingequivalent` creates two independent
canonical budgets (`src/canonical.jl:125`).

This violates the exact-growth and nested-operation rules in plan lines 763-780 and 787-818. R02
remains disputed.

#### D03 — SchemaCache and single-object decode still split one operation across budgets

`SchemaCache` has retained-capacity limits but no caller operation budget (`src/singleobject.jl:26-38`).
`register!` canonicalizes first and opens another budget for equality (`src/singleobject.jl:48-79`).
`lookup` ignores its `limits` argument (`src/singleobject.jl:87-93`). `decodesingle` performs lookup
before its decode budget, can create a separate resolving-plan budget, and its IO overload buffers in
one budget before calling the vector overload with another (`src/singleobject.jl:130-168`).

The exact-capacity cache replacement is now transactional, but the required shared operation budget in
plan lines 808-818 is absent. R03 remains disputed.

#### D04 — Writer header vectors still use unreserved heuristic growth

The Writer now reserves copied key/value payload and exact codec output. It still creates `entries`,
`pfkeys`, and `pfvals` and grows them with `push!` at `src/container.jl:672-704`. Their vector shells,
capacities, and replacement overlap are not reserved. Plan lines 787-794 forbid `push!`, `resize!`, and
`sizehint!` for every charged buffer.

The compression part of R04 is fixed. The header-growth part is not. R04 remains disputed.

#### D05 — Parallel pool charges do not cover the pool lifetime

`FailBox`, the jobs vector, the channel, and worker tasks are allocated before `poolstate` is reserved
at `src/parallel.jl:405-429`. The pool charge is released whenever an admission wave drains at
`src/parallel.jl:463-465`, while the jobs vector, channel, and worker tasks stay live until the
`finally` block at `src/parallel.jl:475-488`. Plan lines 1163-1177 require the pool to be created only
from admitted headroom and charged once for its lifetime.

Block-table growth and byte-identical output-limit errors are fixed. The pool-lifetime defect keeps R06
disputed.

#### D06 — Typed-plan construction ignores its limit and shell-probe contract

`typedplan(..., limits)` does not use `limits`; it allocates and grows its memo vectors with no operation
budget (`src/typed.jl:100-123`). `measuredshell` builds an unreserved empty `Expr(:new, T)` probe and
does not true up against a reserved checked bound (`src/typed.jl:733-751`). An outer immutable shell can
include an inline nested struct while the nested `RecordTarget` reserves its shell again at
`src/typed.jl:725-730`. The current oracle checks only five simple NamedTuple layouts, with four
compared against `Base.summarysize`.

This does not implement plan lines 740-750. R10 remains disputed.

#### D07 — The mandatory performance gate is red and its 1.x baselines are not reproducible

The corrected five-process gate now asserts every listed target (`test/perf.jl:13-56`). The final
current-head run passed 15 of 16 assertions. The 8-thread ratio passed at 4.44. Schema parsing still
used 2,062 allocations against a maximum of 300.

The constants 1.401 s and 5.691 s at `test/perf.jl:9-10` do not match the committed 1.1.2 log values
0.969899 s and 5.035989 s at `benchmarks/logs/avro112.log:2,7`. The 1.x harness also uses random column
data (`benchmarks/bench_1x.jl:3-6`), while the 2.0 harness uses deterministic row values
(`test/perf/cold.jl:24-30`). This is not the same-host, same-session, like-for-like protocol in plan
lines 1912-1933. R11 remains disputed.

#### D08 — The live interop matrix does not satisfy all seven category requirements

The supplied tools passed 58 datum checks and 345 matrix checks. Representatives of all seven
categories execute. The supplied-tool run covered all six codecs and CRC-64, MD5, and SHA-256.
Category 1 checks decoded values but does not assert schema, metadata, datum count, or codec name
(`test/interop.jl:204-264`). Category 2 does not exercise positive and sized collection blocks as
independent oracle cases. Category 3 builds `schemacases` only from `roots/` and `schemas/`, excluding
evolution and Apache schemas (`test/interop.jl:122-127,266-277`). Resolution uses only three fixtures,
only the `:java` policy, and only the Java oracle (`test/interop.jl:279-301`). Sort coverage is 25 random
comparisons of one `{long,string}` record (`test/interop.jl:318-331`). The negative corpus contains
four OCF mutations, not malformed schemas, raw datums, blocks, and JSON
(`test/interop.jl:336-387`). Its tolerated-oracle assertions are one-way and do not require each
recorded tolerance to occur.

This is less than plan lines 1739-1752. R12 remains disputed.

#### D09 — Compile-cost and projection release gates remain incomplete or red

The stand-alone Julia 1.12 compile gate created 42 new Avro specializations and grew RSS by 156.3 MB.
Its limits are zero new specializations and less than 50 MB. The full-suite order reduced the observed
counts to 10 on Julia 1.12 and 8 on Julia 1.10 and 1.11, but did not make the gate pass. Julia 1.11 also
reported 71.1 MB RSS growth.

The corpus projection sweep only opens generated `*-null.avro` files, skips empty records, uses three
selection shapes, and tests `ntasks` 1 and 8 (`test/projection.jl:131-163`). It does not cover every
fixture, `select=()`, reverse/full selections, or `ntasks=2` as required by plan lines 1607-1612.

The RSS child, cross-version exchange, CI duplicate guards, and representative precompile struct are
accepted. R14 remains disputed.

### Medium severity

#### D10 — A failed symbol admission can mutate maintenance state

`admit!` now charges the locked table state, which fixes the snapshot race. It still copies `String(s)`
before the operation charge. More importantly, it runs `step_unlocked!` before the `max_names` and
`max_bytes` checks (`src/admission.jl:136-156`). A rejected admission can therefore advance a merge.
The failure tests check the visible count and name set, not the maintenance state. This contradicts the
transactional failure claim. R07 remains disputed.

### Low severity

#### D11 — The repository-wide explicit-return rule remains unmet

The round-two revision scope now uses explicit returns. A mechanical scan of `src/` and `ext/` still
finds more than 500 expression-bodied method candidates and five lines containing `Any[]`. Examples
include `src/limits.jl:122-145`, `src/plan_write.jl:67-68`, `src/generic.jl:45`, and
`src/jsonreader.jl:545`. `../AGENTS.md` states that functions must use explicit `return` and that code
must use `[]` instead of `Any[]`; a file-local idiom does not override that rule.

Seven supplied revision commits also have non-imperative subjects. The no-history-rewrite instruction
makes that a report-only historical violation. R19 remains disputed.

## Round-one item dispositions

| Item | Disposition | Evidence and argument |
|---|---|---|
| R01 | **Disputed** | Primitive `limits=` and the final charged graph walk work. Construction copies still precede the budget, and `schema(T)` stacks a new finalizer budget. See D01. |
| R02 | **Disputed** | JSON and canonical output use a charging writer, but its IOBuffer growth is not exact and fingerprint/equivalence stack operations. See D02. |
| R03 | **Disputed** | Exact cache replacement overlap is fixed. Cache lookup, registration, IO decode, and resolving work still do not share one operation budget. See D03. |
| R04 | **Disputed** | Budget-first headers and exact-capacity buffers sized by codec-specific proven bounds are fixed. Writer header vectors still grow with unreserved `push!`. See D04. |
| R05 | **Accepted** | Streamed metadata copies reserve before allocation. Byte and streamed header sources now have equivalent retained charges and near-limit acceptance (`src/container.jl:69-152`, `test/invariant.jl:15-59`). Mmap uses the same `BytesSource` implementation as the byte path. |
| R06 | **Disputed** | Exact 40-byte block-table growth and byte-identical per-row output errors are fixed. Pool allocation and charge lifetimes still differ. See D05. |
| R07 | **Disputed** | The operation budget now charges locked comparisons and the deterministic race test passes. Failed admission can still mutate merge state. See D10. |
| R08 | **Accepted** | Writer flush credits uncredited decompressed payload, framing, and members before sink writes. Tests compare writer/reader work counters for every loaded codec and enforce the tight rejection boundary (`src/container.jl:874-905`, `test/invariant.jl:85-123`). |
| R09 | **Accepted** | Byte and mapped sources pre-scan into `Rows{true}`, report `HasLength`, and return the exact length. Streams and structurally invalid pre-scans remain `SizeUnknown` (`src/tables.jl:267-307,355-364`). |
| R10 | **Disputed** | The formula was replaced by a measured shell, but the probe, memo, limit, and layout oracle requirements remain incomplete. See D06. |
| R11 | **Disputed** | The test protocol now uses five cold processes and asserts all numeric targets. The parser-allocation target fails, and the recorded baselines are not source-backed or like-for-like. See D07. |
| R12 | **Disputed** | The matrix now runs instead of silently executing zero resolution cases. It still omits required policy, oracle, sort, and negative-corpus coverage. See D08. |
| R13 | **Accepted** | This review added tracked `test/Manifest-v1.10.toml`, `test/Manifest-v1.11.toml`, and `test/Manifest-v1.12.toml`, verified exact runtime selection, and corrected `STATUS.md` from the final implementation head. The untracked generic test manifest was not used or committed. |
| R14 | **Disputed** | RSS sampling, the two-way cross-version exchange, CI duplicate guards, and representative precompile workload are accepted. The compile-cost gate is red and the projection sweep is not corpus-wide. See D09. |
| R15 | **Accepted** | The plan records the 0.08 GiB macOS measurement and the implementation uses `host_statistics64` free plus inactive pages (`AVRO_REWRITE_PLAN.md:548-559`, `src/limits.jl:195-218`). |
| R16 | **Accepted** | The plan now specifies heap schema nodes with `const` fields and documents the storage reason. The implementation matches that amendment (`AVRO_REWRITE_PLAN.md:224-260`, `src/schema.jl:63-117`). |
| R17 | **Accepted** | Public `parseschema` hard-codes strict parsing. Only a container header under `legacy=:avrojl1` can enable nameless fixed names (`src/schema.jl:300-318`, `src/container.jl:154-157`). Strict and ordinal legacy tests pass. |
| R18 | **Accepted** | The direct resolved route now handles eligibility, promotions, enum remaps, nullable unions, reader defaults, records, and recursion. Defaults are materialized fresh under the decode budget (`src/typed.jl:126-144,331-377,418-564`; `test/typed.jl:303-366`). The separate plan-construction accounting gap is R10. |
| R19 | **Disputed** | The partial round-two style pass does not satisfy the explicit repository-wide rule. See D11. |

## New findings fixed in this worktree

The behavioral repairs started with failing public or package-level regressions. The commits are small
and each has the required `Co-Authored-By: Codex <codex@openai.com>` trailer.

| Finding | Repair commit | Result |
|---|---|---|
| F25 — Schema printing credited reserved 4 KiB chunks instead of emitted bytes and counted nodes too early | `f3e7d6a` | Emitted bytes drive the work rule, and each node is counted after its bytes are credited. |
| F26 — Enum symbols and field aliases bypassed `max_name_bytes` | `6b98713` | Every graph name and alias is checked. |
| F27 — Nameless fixed ordinals counted all prior schema nodes | `088ec72` | Legacy fixed names use the nameless-fixed ordinal. |
| F28 — Resolved typed records aliased mutable defaults, bypassed default work, and overflowed on recursion | `96a5a95` | Defaults are fresh and charged; recursive plans use a memoized reference; eligibility is enforced. |
| F29 — A null writer wrapped into a nullable reader failed typed decode | `d4ae133` | The direct null target returns the target's `missing` or `nothing` convention. |
| F30 — Recursive public construction undercounted graph depth | `c3c9088` | Back-edges are checked at the correct depth. |
| F31 — Recursive references were missing from construction-walk value work | `d20d655` | Repeated graph references count as construction occurrences, so construction does not admit a graph whose printer later rejects. |
| F32 — Symbol admission charged a stale pre-lock snapshot | `0fd9c67` | Lookup and merge comparison work is charged under the lock. |
| F33 — Revised core methods used implicit returns | `f7b4ad7` | The first bounded style pass uses explicit returns. |
| F34 — Resolved enum remaps fell back despite the direct-route claim | `2be64ac` | String, Symbol, and native enum remaps use direct typed targets. |
| F35 — Byte and stream metadata headers had different peak charges | `626beb6` | Owned header buffers transfer equivalent charges. |
| F36 — Cache insertion ignored old/new exact-capacity overlap | `fc80075` | The overlap peak is checked before transactional replacement. |
| F37 — The plan's schema sketch contradicted its heap-node amendment | `253fb0d` | The plan sketch now matches the amended representation. |
| F38 — `max_block_output_bytes` errors differed by task count and stream path | `d1c7af3` | All three paths report the same first-row observed value. |
| F39 — Public construction admitted properties/defaults whose schema text exceeded its limit | `0da5038` | The final construction walk prints and bounds retained schema text. |
| F40 — The compile RSS gate measured a pre-warmed second batch | `8c4ce23` | The gate measures the first 1,000 schemas after the value-set warm-up. It now truthfully fails. |
| F41 — Resolution tests executed zero pairs and the Python re-encode oracle hid union identity loss | `81fccad` | Three explicit pairs run; fastavro compares values without re-encoding; the supplied-tool run covered all six available codecs and all fingerprints. |
| F42 — Performance extras came from the last process and parser allocations were not gated | `f6f9249` | Every field is a five-process median and every listed numeric target is asserted. |
| F43 — A default Writer could emit a file that a default Reader rejected under the work rule | `a14202b` | Header and flush work match reader accounting; the tight writer fails before output. |
| F44 — The work-equivalence regression covered only built-in codecs | `293652e` | It runs for every loaded codec, including bzip2 and xz. |
| F45 — Test dependencies were not pinned per supported Julia version | `925a2e1` | Exact versioned test manifests are tracked for 1.10, 1.11, and 1.12. |
| F46 — Revised files still contained implicit returns | `3bea112` | The revision-scope mechanical pass is complete. |
| F47 — TranscodingStreams retained about twice the charged compressor bound and the bzip2 bound was invalid | `821fd02` | Package-owned processing writes into one pre-sized exact-capacity buffer using native zlib, Zstd, and liblzma bounds plus the documented bzip2 bound formula. |
| F48 — The nameless-fixed tolerance was public outside legacy container framing | `f199a80` | Only private container-header parsing can enable it. |
| F49 — Block-table growth and final shrink did not match the charged capacity on every Julia version | `c580b29`, `1066c20` | Growth reserves exact replacements, and a logical `BlockTable` retains the charged physical capacity on Julia 1.10, 1.11, and 1.12. |
| F50 — Fixture generation reprocessed derived files, omitted root fastavro outputs, and suppressed root failures | `b93646e`, `168a896` | Generation replaces derived data and root outputs, asserts one output per base (19 with the current corpus) with no nested names, and fails on every root-generation error. |

## Verification results

The required release-matrix results below are from final implementation source `168a896`. Focused
repair checks were also run at each repair commit before the final full suites covered those paths
again.

| Verification | Result |
|---|---|
| `AVRO_QUALITY_GATES=true julia +1.12 --startup-file=no --project=. -e 'using Pkg; Pkg.test(julia_args=["-t4"])'` | 9,434 passed, 1 failed in 6m49.1s. The only failure was 10 new Avro specializations; RSS growth was 43.6 MB. Aqua 10/10 and JET 1/1 passed. Depth-80 latency was 4.83s. |
| `julia +1.10 --startup-file=no --project=. -e 'using Pkg; Pkg.test(julia_args=["-t4"])'` | 9,423 passed, 1 failed in 5m45.6s. The only failure was 8 new Avro specializations; RSS growth was 47.1 MB. Depth-80 latency was 5.24s. |
| Julia 1.11.9 with the tracked versioned root and test manifests, `--compiled-modules=no -t4`, full `test/runtests.jl` | 9,412 passed, 2 failed in 6m56.7s. Failures: 8 new specializations and 71.1 MB compile RSS growth. Depth-80 latency was 4.86s. |
| Stand-alone Julia 1.12 compile-cost gate | 1,173 passed, 2 failed in 55.1s. It created 42 specializations and grew RSS by 156.3 MB. |
| Supplied avro-tools 1.12.2 plus supplied CPython/fastavro 1.12.2, Julia 1.12 `-t4`, all six codecs loaded | 58/58 datum checks and 345/345 §8.5 matrix checks passed in about 2m08s. |
| `AVRO_PERF=true julia +1.12 --startup-file=no -t8 --project=. -e 'using Test, Avro; include("test/perf.jl")'` | 15/16 passed in 2m02.1s. The only failure was 2,062 parser allocations. Write was 0.098s; Table was 0.085s; the 8-thread ratio was 4.44; prepared decode was 1 allocation/29ns; prepared encode was 0 allocations/38ns. |
| `AVRO_RSS_GATE=true julia +1.12 --startup-file=no -t8 --project=. -e 'using Test, Avro; include("test/rssgate.jl")'` | 6/6 passed. Baseline 316.5 MB, peak 3,089.9 MB, ceiling 4,096 MB, 7 workers, in-flight high-water 8. |
| `AVRO_SMOKE=true` cross-package test on Julia 1.12 | 5/5 passed in 9.6s. |
| `julia +1.12 --startup-file=no --project=docs docs/make.jl` | Doctests, references, document checks, and HTML build passed. Deployment was correctly skipped outside CI. |
| `test/crossversion/write.jl` and `read.jl`, both 1.10→1.12 and 1.12→1.10 | 32/32 passed in each direction. Each producer wrote 16 files across four schemas and four codecs. |
| `test/fixtures/generate.sh` in a clean isolated copy with the supplied tools | Passed in 101.2s. The jar SHA-256 matched. The fastavro half produced 6 data and 13 root files for each of six codecs, for 19 per codec, with no nested derived names. |
| Focused codec suites on Julia 1.10 and 1.12 | 114/114 passed on each runtime. Compressor output was byte-identical to the prior implementation for a seeded 1 MiB input across the four transcoding codecs. |
| Focused container and invariant suites on Julia 1.10 and 1.12 | 573 container checks and 70 invariant checks passed on each runtime after the final relevant fixes. |
| `git diff --check c6faaa4..HEAD` and trailer audit | Passed. Every round-two commit has exactly one required trailer. The only worktree entry is the pre-existing untracked `test/Manifest.toml`. |

## Assumptions and review decisions

* I treated v24 and its recorded amendments as authoritative.
* I accepted an R-item only when its stated round-one defect and required gate were both satisfied. A
  repaired subpart did not make the whole item accepted.
* I assigned shared construction-accounting defects to R01–R03 or R10 instead of reopening R18 after
  the direct typed resolving semantics passed.
* I treated a mandatory red numeric gate as a release blocker. I did not waive the earlier 8-thread
  failure as host noise. I reran the gate in isolation, where it passed at 4.44; the parser-allocation
  failure remained.
* Julia 1.11 precompile cache workers stalled in the ordinary `Pkg.test` path. I did not terminate or
  alter the other checkout's processes. I ran the full suite with the tracked 1.11 manifests and
  compiled modules disabled. The two resulting failures are product gate failures, not harness errors.
* I fixed bounded defects whose ownership and public regression were clear. I deferred the remaining
  shared-budget, pool-lifetime, performance, corpus, and whole-tree mechanical changes because each
  needs a larger design or validation pass. This is not approval of those deviations.
* The seven non-imperative subjects in the supplied revision history are recorded but not changed because
  the user explicitly prohibited history rewrites.

## Required revision work

1. Put public schema construction, derivation, printing, canonicalization, fingerprinting,
   equivalence, SchemaCache operations, and single-object operations under one exact budget each.
   Reserve all copies, memo tables, and replacement capacities before allocation.
2. Replace Writer header `push!` growth with exact-capacity builders. Make failed symbol admission
   transactional, including maintenance state.
3. Reserve the parallel pool before creating its jobs, channel, and workers. Keep that charge until the
   pool and its storage die.
4. Implement the measured typed-shell protocol with a reserved bound, true-up, complete layout oracle,
   and budgeted memo construction.
5. Rebuild the Avro 1.1.2 baselines with the same deterministic workload and five-cold-process session.
   Reduce schema parsing to at most 300 allocations and make every current performance gate pass.
6. Complete §8.5 category 1 schema/metadata/count/codec assertions, category 2 positive/sized block
   cases, and category 3 schema coverage. Run resolution across both policies and both external
   oracles. Add the full sort vector surface and malformed schema, raw datum, block, and JSON corpus
   with exact expected verdicts.
7. Make the compile-cost gate pass in a stand-alone cold process on all supported Julia versions.
   Expand Table and Rows projection to every required fixture, selection, validation mode, and task
   count.
8. Apply and review the repository-wide explicit-return and typed-empty-literal pass.
9. After items 1–8, rerun the entire supported-version, quality, interop, performance, RSS, smoke,
   docs, generator, and cross-version matrix from the final source head.

VERDICT: REVISE
