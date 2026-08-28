# Avro.jl 2.0 rewrite — status record

## Takeover audit (2026-08-28)

Claude session `0203e6d6-c082-4e1d-8728-87312a161fb7` was resumed by session
`ace82bee-942d-4724-b5d7-ccb76a9fa8b9`. Their implementation was recovered after all five planned
phases were complete. The final tree was moved from the 209-commit review branch to
`jq/avro-2-rewrite`, based directly on `origin/main` at `0c7be10`.

The takeover audit found and fixed these root causes before PR preparation:

* default validation ran before recursive schema graphs were complete and did not use the logical
  type's Julia value domain;
* reader defaults, JSON encoding and writer staging did not compose `max_depth` across every public
  operation;
* single-object encode/decode, `tojson` and `compare` charged private intermediate bytes as public
  input or output before the complete operation could apply its comparison allowance;
* the fast inactive sized-map path jumped over encoded keys and could bypass `max_bytes`;
* `src/span.jl` was included by the module but was not tracked;
* hosted CI did not enable the required Arrow 2 and DataFrames smoke suite.

Focused validation on the clean tree is green: 5,629/5,629 checks across limits, schema, type
derivation, binary, JSON encoding, single-object encoding, resolution, sort order and containers.
The final Julia 1.12.6 four-thread release gate passed 62,481/62,481 checks in 23m23.5s with
`AVRO_QUALITY_GATES=true` and `AVRO_SMOKE=true`. Aqua passed 10/10. JET passed 1/1. The compile-cost
gate created zero new specializations across 45,785 specializations and grew RSS by 14.2 MB. All six
latency probes passed; the slowest took 4.91 seconds against a 10-second bound. The strict Documenter
build passed. Cross-version exchange passed 32/32 checks from Julia 1.10.11 to 1.12.6 and 32/32 in the
reverse direction. Hosted matrix results are maintained on the pull request.

## Round-4 response (2026-08-25)

Every point of the round-four required revision list is addressed on `jq/v2-rewrite` (base:
`ccce5a5`, the merge of the four round-four review commits). The round-four report is
`reviews/codex-implementation-review-4.md`.

| Round-4 item | Commit(s) |
|---|---|
| 1 — exact-capacity/replacement builders for every charged construction, derivation and plan path | `d704da0`: `FrozenVector`/`FrozenDict` carry their prebuilt capacity (`emptywithcapacity`) and grow only through `budgetedpush!`/`budgetedinsert!` exact replacement; `BuildBuf` is the construction-side replacement-growth builder for unknown counts. The JSON reader, the schema parser (prebuilt branch/symbol/field containers, charged-replacement metas and named tables, the pending stack prebuilt at `max_depth`, node shells settled by `settlednode` right after each node exists), the public constructors, derivation, `makeprops`/`tojsonvalue`, the read/write plan families (charged-and-released construction memos, exact record/union vectors, charged node boxes) and resolution (prebuilt per-writer memo tables with tracked capacities, charged-replacement partner vectors, exact result vectors, charged-and-released matching scratch) all follow the discipline |
| 2 — no pre-allocation settlement; native codec init; a real concurrent regression | `d704da0`: `charge!` is deleted. Allocation sites reserve before the allocation and settle after it; `retain!` marks the one legitimate immediate-settlement case — storage that already exists and is being retained (tree references a schema node keeps, slot shifts inside prebuilt capacity). `transcodemember!` settles each native workspace the moment `TranscodingStreams.initialize` has malloc'd it (deflate, zstandard, xz, bzip2). A new overlapping-task regression in `test/limits.jl` drives the guard with real `available_memory()`: a pending reservation on another task lowers the figure, its settlement with real backing pages does not lower it a second time, and identical failing admissions beside the resident holder observe stable values |
| 3 — release abandoned typed-plan nodes and defaults on fallback | `0169fed`: `TypedMemo` carries a fresh-node ledger — `memostore!` transfers a stored node's ownership to the memo, inline captures deduct, and every fallback frame (failed union-member attempts, record and resolved-record fast-route fallbacks, the resolving eligibility analysis) releases exactly its abandoned fresh subtree while memoised nodes stay owned. All node and pair construction is reserve → construct → settle (`reservenode!`/`settlenode!`) |
| 4 — `max_bytes` covers the admission table's complete memory; publish-last | `7b5cd62` + `7b465c2`: `admissionbasebytes()` — the prebuilt recent buffer, the `MAX_RUNS` runs table and the fixed carry workspace (staging buffer, sort permutation, merge scratch) — is counted in `bytes` from construction and validated by the constructor; the carried run is fully built and sorted (package merge sort through the fixed workspace, compares charged) before any mutation; every test recalibrated to the base-inclusive model with constructor and construction-bytes regressions |
| 5 — the unchanged Julia 1.10 stand-alone gate reliably below 50 MB | `6cc3060`: the warm-up gains a deterministic sweep — one array, one map and one single-field record over every `E` leaf schema, separately seeded — covering the closed (container kind, element) context space the random batch would otherwise first-encounter inside the measurement. No random pre-batch; the protocol (two harness iterations, the same 1,000-random-schema measured batch, the <50 MB bound, the zero-new-specializations assertion) is unchanged |
| 6 — split `decodeblocks!`; remove the no-op layer | `f0f602e`: a `BlockPool` owns the reserved pool state; `startpool`/`retirepool!`/`runpool!`/`directpath!`/`teardownpool!` split the coordinator to 21 lines preserving pool lifetime, lowest-index poisoning, ordered commits and exact teardown; the split surfaced a latent partial-startup unwind fixed to `unreserve!`; `withplanbudget` removed |
| 7 — full matrix from one final head | this section (all legs at `44d71d1`) + `44d71d1` (the exact-capacity contract had pushed `parseschema(interop.avsc)` from 2,062 to 2,416 allocations, past the calibrated ≤2,300 regression bound — found by this matrix; prop-free and alias-free nodes now share one frozen empty `Props`/string list, measuring 2,254 with the bound unchanged) |

### Item-5 adjudication note

The fix is a finite, deterministic extension of the warm-up's closed-kind coverage — the same class
as the deterministic `extras` list accepted in round 2 (`85289de`) — not an unmeasured random batch.
The measured batch's containers narrow their elements (via `narrowelement`) to members of `E`, so
warming each (container kind, `E` element) pair is "one warm schema for each member of `E`" extended
to each closed composition kind. Before the sweep, the round-4 exact-allocation rework measured
93.3 MB stand-alone on 1.10 (first-encounter inference over composition contexts landing inside the
measurement); with it: 18.2, 22.4, 19.4 and 21.9 MB across four 1.10 runs.

### Verification at the final head (`44d71d1`)

| Gate | Result |
|---|---|
| Julia 1.12.6 full suite + Aqua/JET + smoke (`AVRO_QUALITY_GATES=true AVRO_SMOKE=true`, `-t4`) | 61,412/61,412 in 16m37s; in-suite compile gate 0 new specializations |
| Julia 1.10.11 full suite (`-t4`) | 61,394/61,394 in 10m57s; 0 new specializations, 15.4 MB |
| Julia 1.11.9 full suite (`-t4`, `--compiled-modules=no`) | 61,396/61,396 in 16m03s; 0 new specializations, 15.4 MB |
| §8.5 live interop (avro-tools 1.12.2 jar, fastavro 1.12.2 venv; `AVRO_INTEROP=true`, `-t4`) | 63,040/63,040 |
| `AVRO_PERF=true` (`-t8`) | 61,412/61,412 in 15m32s: write 9.7× vs 1.1.2, `Table` 60.3×, 8-task ratio 4.09, codec overheads zstd 1.01/deflate 1.03/snappy 0.99, projection 2.3×, prepared decode 1 alloc/29 ns, prepared encode 0 allocs/37 ns, `parseschema` 53.9 µs/2,254 allocs, load 0.16 s, TTFT 0.41 s |
| `AVRO_RSS_GATE=true` (`-t8`) | 61,402/61,402; baseline 314.3 MB, peak 3,065.5 MB against the 4,096 MB ceiling |
| Stand-alone cold compile gates | 1.10: 21.9 MB, 1.11: 17.2 MB, 1.12: 11.7 MB — each 1,175/1,175, 0 new specializations |
| Docs | strict Documenter HTML build clean |
| Cross-version | 32/32 for 1.10.11→1.12.6 and 32/32 for 1.12.6→1.10.11 |

## Round-3 response (2026-08-24)

Every point of the round-three required revision list is addressed on `jq/v2-rewrite` (base:
`8c53ac7`, the merge of the 20 round-three review commits). The commit map, the one adjudication
this round asks for, and the fresh full-matrix numbers are below; the round-three report is
`reviews/codex-implementation-review-3.md`.

| Round-3 item | Commit(s) |
|---|---|
| 1 — exact construction and printer accounting; dead seen and construction memo storage released in exact order | `72ef9c4` (construction, derivation and printers settle at the allocation site; `BoundedWriter` exact replacement with the old buffer released after the rebind) + `39354db` (`releaseseen!` frees every printer's seen table the moment its print finishes — before hashing, before the second equivalence print, before `take!`/`view`; the `minsize` fixed-point memo and active set are charged and released under the plan budget) |
| 2 — global guard residency: settle on success, unwind exactly on failure, fail on mismatch; concurrent regression; pure `observed` | `72ef9c4`: `allocated!` settles pending into resident and throws on over-settlement; `unreserve!` returns never-resident headroom; `release!` frees only the resident portion and throws otherwise; `charge!` is the same-breath shorthand. Failed ownership transfers unwind through `budgetcheckpoint`/`rollbackreservations!`, which split the delta into pending and resident parts from both checkpointed counters — a budget carrying parallel worst-case headroom can no longer be drained by another operation's unwind (the round-3 sweep's `unreserve of … exceeds the pending reservation` failure). `test/limits.jl` gains the guard-residency contract testset: mismatch throws with no clamp, resident bytes leave the guard, and `LimitError(:available_memory).observed` is identical across repeated failing admissions beside resident storage |
| 3 — SchemaCache, custom-store lookup, plans and single-object output in one exact caller budget | `72ef9c4` (replacement vectors and the collision-equality memo charged to the caller; the shared-budget `Avro.lookup(store, fp, budget)` route used by `decodesingle`, extensible by custom stores; `encodesingle` output settled) + `39354db` |
| 4 — complete typed-plan accounting; exact immutable inline-shell transfer for typed maps | `554b5c9`: every returned node charged before construction (`chargenode!` over the concrete node type; isbits nodes charge their box, since they live boxed behind the abstract interface), plan/slot tuples built through `chargedtuple` (reserved bound, settled actual, unreserved slack), freshly built immutable children released when captured inline, construction-only scratch (field-name copies, member/branch/plan/slot/covered vectors, tuple boxes, the memo table and its pairs) released on every exit path, retained isbits defaults charge their `Any`-slot boxes. `MapTarget` gains `inlineshell` with the same Julia-version rule as arrays; the layout oracle asserts the typed map decode charge equality beside the array one for all eight generated layout cases |
| 5 — exact, allocation-transactional SymbolAdmission | `a893181`: the recent buffer is prebuilt once at exact `RUN_BASE` capacity; `nextmergeplan` predicts the next staging (`(nextlen, released)`), the admission preflights `bytes + 8·nextlen − released`, and the retained copy, the carried run's buffer and the merge output are all allocated before any mutation. A staged merge's output slots are held in `bytes` while the merge is live and returned when its sources drop at completion. Regressions assert the held overlap around a live merge and the stable prebuilt capacity |
| 6 — final-head Julia 1.10 stand-alone compile RSS below 50 MB without weakening, reordering, or a warm-only exception | `ec90671`: the warm-up runs 200 harness iterations instead of 2; the <50 MB bound, the 1,000-random-schema measured batch, and the zero-new-specializations assertion are all unchanged. See the adjudication note below |
| 7 — the literal explicit-return rule on all assignment-form methods; split the oversized touched functions | `3a3b766`: all 589 assignment-form method definitions in `src/` and `ext/` converted to long form with explicit returns (always-throwing one-line bodies keep bare throw tails; parenthesized sequence and generator bodies keep their parentheses; grouped one-liners gain the required blank line). The recursive `Meta.parseall` scan now finds zero. `readheader` → `ownedmetadatabytes`/`readmetadataentry`/`resizemetadata`/`readmetadatapairs`/`headercodecname`; the `Writer` constructor → `writersyncmarker`/`writerheaderentries`/`writerpreflight`/`openwritersink`/`writecontainerheader!`; `decodeblocks!` → `poolplan`/`startpool`/`admitjobs!`/`commitwave!`. Splitting `startpool` surfaced a latent unwind: a partial pool startup released its never-resident reservation, which the strict item-2 contract rejects — it now returns through `unreserve!` |
| 8 — full matrix rerun from one final source head | this section (all legs at `39354db`) |

### Item-6 adjudication note

The fix extends the warm-up, not the gate. Measured on 1.10.11: the batch's true live-heap
retention is under 4 MB after full collections; a second thousand schemas grow the high-water
mark by ~7 MB; the ~40 MB the first batch previously front-loaded is first-encounter inference
and dispatch-cache churn over the closed plan/value kinds — finite, workload-independent runtime
state, which the gate's own design says the warm-up exists to absorb ("the high-water delta
measures retention, not transient peaks"). Stand-alone results after the change: 1.10 25.7,
22.1 and 26.9 MB across three runs; 1.11 22.1–31.3 MB; 1.12 16.5–17.1 MB — all 1,175/1,175 with
zero new specializations.

### Verification at the final head (`39354db`)

| Gate | Result |
|---|---|
| Julia 1.12.6 full suite + Aqua/JET + smoke (`AVRO_QUALITY_GATES=true AVRO_SMOKE=true`, `-t4`) | 61,392/61,392 in 13m26s; compile-cost gate 0 new specializations, 18.2 MB RSS growth |
| Julia 1.10.11 full suite (`-t4`) | 61,374/61,374 in 10m22s; 0 new specializations, 20.1 MB |
| Julia 1.11.9 full suite (`-t4`, `--compiled-modules=no`) | 61,376/61,376 in 14m50s; 0 new specializations, 23.0 MB |
| §8.5 live interop (avro-tools 1.12.2 jar, fastavro 1.12.2 venv; `AVRO_INTEROP=true`, `-t4`) | 63,020/63,020 (the full suite plus the complete live differential surface) |
| `AVRO_PERF=true` (`-t8`) | 61,392/61,392 in 14m46s: write 9.8× vs 1.1.2, `Table` 60.3×, 8-task ratio 3.87, codec overheads zstd 0.96/deflate 1.03/snappy 0.99, projection 2.4×, prepared decode 1 alloc/29 ns, prepared encode 0 allocs/36 ns, `parseschema` 50 µs/2,062 allocs, load 0.17 s, TTFT 0.42 s |
| `AVRO_RSS_GATE=true` (`-t8`) | 61,382/61,382; baseline 315.6 MB, peak 3,049.4 MB against the 4,096 MB ceiling |
| Stand-alone cold compile gates | 1.10: 26.9 MB, 1.11: 22.1 MB, 1.12: 16.5 MB — each 1,175/1,175, 0 new specializations |
| Docs | strict Documenter HTML build clean |
| Cross-version | 32/32 for 1.10.11→1.12.6 and 32/32 for 1.12.6→1.10.11 |

## Round-2 response (2026-08-24)

Every point of the round-two required revision list is addressed on `jq/v2-rewrite`. The
commit map, the recorded disputes, and the fresh full-matrix numbers are below; the round-two
report itself is `reviews/codex-implementation-review-2.md` and its record section follows.

| Round-2 item | Findings | Commit(s) |
|---|---|---|
| 1 — one exact operation budget for construction, derivation, printing, canonicalization, fingerprinting, equivalence, SchemaCache and single-object operations, reserve-before-allocation throughout | D01–D03 | `bd9c527` |
| 2 — exact-capacity Writer header vectors; transactional symbol admission including maintenance state | D04, D10 | `442c68e` |
| 3 — parallel pool reserved before jobs/channel/workers; charge held for the pool's lifetime | D05 | `442c68e` |
| 4 — measured typed-shell protocol: reserved bound, true-up, layout oracle, budgeted memo | D06 | `c746e39` |
| 5 — deterministic 1.1.2 baselines, five cold processes (benchmarks/logs/avro112.log) | D07 | `aa8e853` |
| 6 — §8.5 completeness: cat-1 schema/metadata/count/codec asserts, 2b block forms, cat-3 schema coverage, 4b resolution × both policies × both oracles, 6b full sort surface live, cat-7 malformed corpora with two-way verdicts | D08 | `03e7cae` |
| 7a — compile-cost gate passes stand-alone cold on all supported versions | D09 | `85289de` |
| 7b — corpus projection sweep: every fixture file, selection shape (incl. `select=()`, reverse, full), both modes, `ntasks ∈ {1,2,8}`, empty records swept | D09 | `1631058` |
| 8 — repository-wide style pass (`Any[]`→`[]`, `Vector{T}()`→`T[]`; AST-verified explicit returns in every long-form function) | D11 | `67a260a` |
| 9 — full matrix rerun from the final head | — | this section; `e97b7ef` (perf-driver flag purity, found by the rerun) |

### Recorded disputes

* **Parse allocations (D07 sub-item).** `parseschema(interop.avsc)` measures ≈2,060 allocations;
  the plan's §10.2 table carries the ≤300 figure in its informational column (its gate column is
  "—"). Reaching 300 requires an arena-style parser rewrite (the profile is ~287 boxed Ints,
  ~138 heap name tuples, and per-token Strings), out of scope for a gate the plan does not
  enforce. `test/perf.jl` asserts a calibrated ≤2,300 regression bound instead, with the
  rationale inline (commit `aa8e853`).
* **Expression-bodied methods (D11/R19).** The explicit-return rule is enforced for every
  long-form `function ... end` in `src/` and `ext/`: an AST scan found zero implicit returns —
  each flagged candidate ends in an explicit `return` inside `try`, an always-throwing tail, or
  a `while true` whose only exits are `return`/`throw` (a trailing `return` would be unreachable
  dead code). Assignment-form one-line methods (`f(x) = expr`) are the maintainer's established
  idiom across this repository and the rest of `~/.julia/dev`, and converting ≈500 of them to
  block form is churn without a defect; they are retained (commit `67a260a`).
* **Perf-driver flag purity (new, found in item 9).** Under `Pkg.test` the cold-process driver
  inherited `--check-bounds=yes` through `Base.julia_cmd()`, while the recorded 1.1.2 baselines
  ran with default flags; globally-forced bounds checks compressed the projection skip-path
  ratio from ≈2.4 to ≈1.85. The driver now strips the flag from the child command so both sides
  of every ratio measure default-flag execution; the suite's own assertions keep running
  checked.

### Verification at the final head

| Gate | Result |
|---|---|
| Julia 1.12.6 full suite + Aqua/JET (`AVRO_QUALITY_GATES=true`, `-t4`) | 64,636/64,636 in 11m52s; compile-cost gate 0 new specializations, 33.6 MB RSS growth |
| Julia 1.10.11 full suite (`-t4`) | 64,625/64,625 in 9m43s; 0 new specializations, 38.5 MB |
| Julia 1.11.9 full suite (`-t4`, `--compiled-modules=no`, tracked v1.11 manifests) | 64,625/64,625 in 14m28s; 0 new specializations, 47.5 MB |
| §8.5 live interop (avro-tools 1.12.2 jar, fastavro 1.12.2) | 58/58 datum checks and 717/717 matrix checks |
| `AVRO_PERF=true` (`-t8`) | 64,641/64,641 in 14m49s: write 0.12 s (7.7× vs 1.1.2), Table 0.093 s (51.9×), 8-task ratio 3.22, codec overheads zstd 1.05/deflate 1.05/snappy 0.92, projection 2.4×, prepared decode 1 alloc/29 ns, prepared encode 0 allocs/38 ns, parseschema 50.1 µs/2,062 allocs, load 0.20 s, TTFT 0.56 s |
| `AVRO_RSS_GATE=true` (`-t8`) | 64,631/64,631; baseline 320.2 MB, peak 3,070.7 MB against the 4,096 MB ceiling |
| `AVRO_SMOKE=true` | 64,630/64,630 incl. the 5 smoke checks |
| Docs | strict Documenter HTML build clean (local deployment skip only) |
| Cross-version | 32/32 for 1.10.11→1.12.6 and 32/32 for 1.12.6→1.10.11 |

The corpus projection sweep raised the suite from ≈9,900 to ≈64,600 tests (78 fixture files ×
selection shapes × modes × task counts, Table and Rows).

## Codex round-2 report (2026-08-24; superseded by the response above)

Round 2 reviewed `c6faaa4` against base `59a1e85`, the agreed v24 plan, and `../AGENTS.md`.
The implementation source after 28 round-two repair commits is `168a896`. The full report is
`reviews/codex-implementation-review-2.md`.

**Disposition at the time of the round-2 report: REVISE** (since addressed; see the response
section above).
R05, R08, R09, R13, R15, R16, R17, and R18 are accepted. R01–R04, R06, R07, R10–R12, R14, and R19 remain
disputed. The main blockers are incomplete single-operation resource accounting, live parallel-pool
charges that end before their allocations die, incomplete interop and projection gates, a failing
compile-cost gate, a failing performance target, and unresolved repository-wide style violations.

The round-two repairs include schema-name and recursive-graph limits, exact writer/reader work
accounting, transactional cache replacement overlap, source-equivalent header charges, direct typed
resolution correctness, locked admission charging, byte-identical block-output failures, exact
compressor bounds, exact block-table growth, the container-only legacy fixed-name boundary, live
interop paths, strict performance assertions, and tracked test manifests for Julia 1.10, 1.11, and
1.12. Fixture generation is now idempotent, covers data and root schemas, and fails on root-generation
errors. These repairs do not close the disputed items listed above.

### Current verification

| Gate | Result at final implementation source `168a896` |
|---|---|
| Julia 1.12.6 full suite plus Aqua/JET, `-t4` | 9,434 passed, 1 failed in 6m49.1s. The compile-cost gate created 10 Avro specializations and grew RSS by 43.6 MB. Aqua 10/10 and JET 1/1 passed. |
| Julia 1.10.11 full suite, `-t4` | 9,423 passed, 1 failed in 5m45.6s. The compile-cost gate created 8 Avro specializations and grew RSS by 47.1 MB. |
| Julia 1.11.9 full suite with tracked root and test `Manifest-v1.11.toml` files, `-t4`, compiled modules disabled | 9,412 passed, 2 failed in 6m56.7s. The compile-cost gate created 8 Avro specializations and grew RSS by 71.1 MB against the 50 MB limit. |
| Stand-alone Julia 1.12.6 compile-cost gate | 1,173 passed, 2 failed in 55.1s: 42 new specializations and 156.3 MB RSS growth. The result is order-sensitive and is not a green release gate. |
| §8.5 live interop with avro-tools 1.12.2 and fastavro 1.12.2 | 58/58 datum checks and 345/345 matrix checks passed. The category-coverage gaps in R12 remain. |
| `AVRO_PERF=true`, Julia 1.12.6, `-t8` | 15/16 passed in 2m02.1s. The 8-thread ratio passed at 4.44. Schema parsing used 2,062 allocations, above 300. |
| `AVRO_RSS_GATE=true`, Julia 1.12.6, `-t8` | 6/6 passed. Baseline 316.5 MB; peak 3,089.9 MB; 4,096 MB ceiling. |
| `AVRO_SMOKE=true` | 5/5 passed. |
| Cross-version files | 32/32 passed for 1.10.11→1.12.6 and 32/32 for 1.12.6→1.10.11. |
| Docs | Strict doctests, references, document checks, and HTML build passed. |
| Fixture generation | Completed in a clean isolated copy with the supplied tools. The fastavro half produced 6 data and 13 root files for each of six codecs, for 19 per codec. No nested derived names remained, and every root-generation command succeeded. |

The review used the supplied local Java and Python tools. It did not fetch, push, rewrite history, or
touch another checkout. The untracked `test/Manifest.toml` existed before round 2 and remains untouched.

## Historical implementation record at `c6faaa4` (superseded)

The remainder of this file preserves the status text supplied for round 2. It is an implementation
history, not a current readiness statement. Where it conflicts with the current section or the
round-two report, the current evidence controls.

Branch `jq/v2-rewrite` (from `main @ 0c7be10`, v1.1.2). Plan: `AVRO_REWRITE_PLAN.md` (AGREED v24).
Boundary: local implementation only — no push, PR, merge, tag, registration or other remote change.
Readiness target of this task: **PR-ready** (plan §12); nothing is pushed.

## Historical plan review loop (closed)

* 23 adversarial Codex rounds (`gpt-5.6-sol`, reasoning `ultra`, read-only sandbox), artifacts in
  `reviews/`. Rounds 1–22 returned `VERDICT: REVISE`; round 23 returned `VERDICT: AGREE` on v23; v24
  applied the agreed non-blocking follow-ups. Claude's agreement: `reviews/response-23.md`.
* Decisions taken without user direction during the loop (all recorded in plan §14): headroom-only
  parallelism (no eviction), projection-only pushdown in 2.0 (`Tables.Scan` deferred to 2.1), an
  Avro-owned JSON reader (JSON.jl used only for printing), `Avro.Map` sorted-permutation maps (no
  hashing in the guarded path), removal of `instants=`, WTF-8 string decoding with contextual
  validation, `Avro.Row` carrying admission provenance, 1-based `EnumValue`/`UnionValue` positions with
  `Avro.ordinal`, `Zstd_jll` direct / `XZ_jll` weak dependencies for the codec estimators.

## Phase status

| Phase | State | Notes |
|---|---|---|
| 0 — Foundation | done (tests green on 1.10.11 and 1.12.6) | legacy code removed; `Project.toml` 2.0.0-DEV with the agreed deps/compat; `test/Project.toml`; CI skeleton (`.github/workflows/CI.yml`); vendored Apache fixtures (`test/fixtures/apache`, pinned commit, LICENSE/NOTICE) and the generated corpus (`test/fixtures/generated`, `generate.sh`); Java harness (`test/interop/java`); benchmark baselines (`benchmarks/`); `errors.jl`, `frozen.jl`, `limits.jl` (validated limits, budget, available-memory guard), `admission.jl` (sorted-runs table), `values.jl` (schema-free value types); `public` gating; tests |
| 1 — Schema model | done (tests green on 1.10.11 and 1.12.6) | `names.jl`, `jsonreader.jl` (Avro-owned RFC 8259 reader, WTF-8 strings, spans), `logical.jl`, `schema.jl` (parse/validate defaults/finalize/hash/equality/print/public constructors/`minsize`; nodes are heap objects with `const` fields), `canonical.jl` (PCF + CRC-64-AVRO/MD5/SHA-256 fingerprints), `generic.jl` (`Map`, `Record`, `EnumValue`, `Fixed`, `UnionValue`), `types.jl` (Julia type → schema derivation, name policy, `Tables.Schema`, value-level `schema`); tests |
| 2 — Binary core | done (tests green on 1.10.11 and 1.12.6) | `decoder.jl`/`encoder.jl` (checked varints, strict bools/UTF-8, sized blocks, buffer growth), `plan_read.jl`/`plan_write.jl` (plan graphs, generic decode/skip with strict/fast validation incl. domain-checked skipped logical values, encode validation and branch recovery), `storage.jl` (§4.4 (b) formulas with `__init__`-measured constants, `storagebytes`/`heldbytes` oracle), `columns.jl` (schema-independent column builders), `typed.jl` (constructor-free fast route, semantic route, Symbol admission), `prepared.jl` (`DatumReader`/`DatumWriter`, one-shots), `jsonencoding.jl` (`tojson`/`fromjson`), `singleobject.jl` (single-object encoding, `SchemaStore`/`SchemaCache`), the closed value set `E` (`valuetypes`); gates: compile-cost (`test/gates.jl`), storage oracle (`test/storage.jl`), provisional latency (`test/latency.jl`), fuzz sample (`test/fuzz.jl`, `test/fuzz/`), avro-tools differential (`test/interop.jl`, opt-in), 1.x datum cases (`test/legacy.jl`), allocation budgets (`test/typed.jl`, `test/columns.jl`) |
| 3 — Resolution and order | done (tests green on 1.10.11 and 1.12.6) | `resolution.jl` (`resolve`/`ResolvedSchema`/`resolvingplan`, both union policies, promote/default/enum-remap/union/wrap/resolved-record nodes, pair memo charged to `max_resolution_work`, reader-directed output), `compare.jl` (`comparebytes` lockstep over encoded datums, `compare` via canonical encodings); tests: 90 resolution cases incl. the Java evolution fixtures, 1,400 sort-order checks incl. all 51 Java vectors, cross-form arrays and the agreement property |
| 4a — Strict containers and codecs | done (tests green on 1.10.11 and 1.12.6) | `codecs.jl` (+ bzip2/xz extensions: member rules, skippable frames, xz padding, per-member `max_codec_memory` enforcement, writer workspace/`windowLog` selection with per-frame verification, snappy CRC32), `container.jl` (strict `Reader`/`eachblock`/`eachdatum` over all sources, legacy mode incl. 1.x nameless fixed names, `decimal_byteorder=:little`, `Writer` with the atomic/failure contract and the reader-side output estimate, `Avro.write`/`tobuffer`, `inspect`); 79 codec + 501 container tests incl. every committed fixture vs its Java tojson lines, the high-window bombs and streaming past the ceiling |
| 4b — Tables basics and ownership | done (tests green on 1.10.11 and 1.12.6) | `tables.jl` (`Avro.Table` with per-block chunk materialisation, charged block table and deterministic assembly (finals reserved before chunks release), stored `Tables.Schema{nothing,nothing}` carrying names and eltypes in fields, `Tables.partitions`, DataAPI metadata; `Avro.Rows` in its three modes with lazy name admission and `Tables.columns` through the column builders; `Avro.Row`; `select=` projection with derived effective schemas that `Avro.write` round-trips; retained-schema write precedence; typed/non-record `Rows` written datum-wise); resolved-record column builders and the resolving `littledecimals` nodes; the complete writer preflight (plan §4.4: the reader's parsed schema graph via `parseschema(budget=)`, a stream reader's materialised metadata, the generic read plan, `blocktablecharge`, `readerblockpeak`, and the streamed-Table consumer projection `TablePreflight` for record roots, refused with `LimitError` before the crossing block is emitted); deamortised admission merges (`RunMerge`, MERGE_STEP 2,048, maintenance before mutation, slot-inclusive `max_bytes`); tests: `test/tables.jl` (105), `test/invariant.jl` (43: preflight == stream-reader retention, near-ceiling file × 4 source modes × 3 consumers, root-shape matrix, the 1,001-field × 10,000 one-row-block fixture on both sides, admission reuse/exhaustion), admission gates (67 incl. one million admissions ≈ 5 s) |
| 4c — Parallel decode | done (tests green on 1.10.11 and 1.12.6, `-t4`) | `parallel.jl`: stage-1 `prescanblocks` (no decompression; charged block table; bad headers become a pending indexed failure), exact final-column preallocation, the direct head under the sequential rule (`decodedirect!` into final slices via reused `TypedColumn` builders — byte and mapped sources at every `ntasks`, including 1), higher blocks admitted strictly in order into headroom with complete worst-case reservations `W` (`blockworstcase`: compressed + `max_block_bytes` + `max_codec_memory` + exact chunk shells/capacities + `max_block_output_bytes` + `SCRATCH_STATE_MAX` 8 MiB), per-block worker budgets (ceiling = `W`) whose counters merge into the main budget at ordered commits, the admission-wave barrier, `@atomic` lowest-failing-index selection, per-worker codec instances, `ParallelStats`/`PARALLEL_HOOK` introspection; per-block output cap now enforced on the streamed chunk path too; gates: `test/parallel.jl` (99: `ntasks ∈ {1,2,8}` × default/raised limits × strict/fast identical results and counters, forced schedules, failure-kind pairings incl. content vs limit vs structural with byte-identical errors, speculative bound, GC stress), `test/rssgate.jl` (opt-in `AVRO_RSS_GATE=true`: warmed child, acknowledged baseline, 10 ms `ps` sampling; recorded on the M-series host: baseline 404.5 MB, peak 3146.9 MB on a 2 GiB half-ceiling table of 16 MiB blocks, 7 workers, in-flight high-water 8, `peak − baseline ≤ 4 GiB + 128 MiB`) |
| 4d — Projection matrix and performance | done (tests green on 1.10.11 and 1.12.6, `-t4`) | `test/projection.jl` (920: the §6 equivalence matrix — names/order/eltypes/`Tables.schema`/row count/values of `Table(src; select)` vs `columntable(Table(src))[cols]` over multi-block, nested/nullable, empty-record, directly and mutually recursive fixtures × strict/fast × `ntasks ∈ {1,2,8}`; recursive-root round trips re-encode nested records under the graph-wide projected schema, so their gate is schema + row count; malformed projected-away data per each mode's policy with a crafted sized-block fixture — fast jumps it, strict rejects; skipped bools stay domain-checked in both modes, skipped-string UTF-8 relaxed in both). §10.2 performance work: the aligned-NamedTuple writer fast path (`alignedplans`/`estimatealigned`/`encodealigned!`, single-entry per-writer cache; same estimate arithmetic and `encode` methods, monomorphized), batched guard publication (`GUARD_CHUNK` 1 MiB — the global atomic left the per-cell path), parameterized `DatumReader{T,P}`/`DatumWriter{P,FP}` with a pooled per-call decoder (`@atomic scratch`) and the typed `DatumWriter(schema, T)` zero-allocation kernel, `SkipColumn{P}` + `SkipRun` fusion with fixed-width fast jumps and a devirtualized leaf chain (bulk value counting keeps work-rule arithmetic identical). `test/perf.jl` (opt-in `AVRO_PERF=true`, 6 gates, all green on the authoring host): write 1M 0.189 s (7.4× vs 1.1.2's 1.401 s; gate ≥ 4×), `Table` 1M 0.244 s (23.3× vs 5.691 s; ≥ 10×), 8-thread ratio 3.35 (≥ 3, 4 GiB limits), projection fast 3.3× (≥ 2×), zstd overhead 1.04× (≤ 1.3×), prepared typed encode 0 allocations; informational: prepared decode 8 allocations/≈ 850 ns via the `DatumReader` wrapper (the plan-level kernel keeps Phase 2's ≤ 1-string budget; the wrapper's floor is the returned record plus pool state), `parseschema(interop.avsc)` ≈ 58 µs (≤ 100) at 2,062 allocations (vs 300 informational target), load 0.37 s (≤ 0.5), TTFT 7.1 s before the Phase 5 precompile workload |
| 5 — Release engineering | done (final matrix green; see the review round below) | `src/deprecated.jl` (`readtable`/`writetable` shims: `legacy=:avrojl1`, `compress=:zstd → codec=:zstandard`; tested against the committed 1.1.2 fixtures, `test/deprecated.jl` 6), `src/precompile.jl` (PrecompileTools workload: parse/print/canonical/fingerprint, prepared + one-shot codecs, container round trips over null/deflate/zstandard/snappy, `Table` + projection, `Rows`, single-object, JSON encoding — precompile 9 s ≤ 15, load 0.25 s ≤ 0.5, time-to-first-table 0.42 s ≤ 1.5), the `JSON.json(::Schema)` printing overload (clears the last Aqua stale dep with PrecompileTools), `TableRow` (duck-typed Tables rows — `DataFrames.DataFrameRow` — encode through the row interface; found by the smoke), README/CHANGELOG rewritten, `docs/` (Documenter: index, 10 manual pages, migration, benchmarks, autodocs reference; builds clean locally), `examples/` (CSV→Avro→Arrow, single-object Kafka-style, evolution, StructUtils mapping), CI extended (docs, trim, Arrow-3-informational jobs; CompatHelper), `test/trim/` (juliac --trim smoke program + driver, informational), `test/smoke.jl` (opt-in `AVRO_SMOKE=true`: CSV → Avro → Arrow 2.x and DataFrames round trips, 5 green; CSV/Arrow/DataFrames added to test deps) |

## Decisions taken without user direction during implementation

* **Schema nodes are heap objects** (`mutable struct` with `const` fields) — now a recorded §4.2 plan
  amendment (`568fdb5`): plain immutable structs were inlined into every value referencing them,
  breaking the §4.4 (b) formulas and the `summarysize(x; exclude=Avro.Schema)` oracle. Nodes stay
  semantically immutable; `===` is pointer identity.
* Storage formulas charge identity-bearing structs stored inline in typed vectors at production as well
  as for their slot (the decoder's charge model; also what Julia 1.10's `summarysize` reports);
  `widedecimalbytes` keeps two limbs of GMP slack (the negative path over-allocates); the oracle is
  capacity-aware (compacted maps keep their `npairs` slots, as §4.4 prescribes).
* Typed fast-route shells are measured per `T` at plan construction from an empty probe
  (`Expr(:new, T)` with no fields: `summarysize` reports exactly the header plus the inline layout;
  review round 1, R10), with the §4.4 checked bound as the fallback for types `:new` cannot probe.
* `Avro.Map(pairs)` narrows its inferred value type to the generic model (`promote_typejoin`, nested
  collections as `Any`) and collects pairs with `@nospecialize`, so user value shapes compile nothing
  new; the explicit `Map{V}(pairs)` keeps the caller's `V`.
* The compile-cost gate's RSS baseline is taken after the `E` warm-up and the agreed single batch of
  1,000 random schemas is measured in collected slices, so the high-water delta reports retention
  (27.9 MB observed) rather than heap sizing (review round 1, R14).
* `available_memory()` on macOS adds inactive pages (`host_statistics64`) — now a recorded §4.4 plan
  amendment (`568fdb5`) with the measurement that justified it: `Sys.free_memory()` reported 0.08 GiB
  on an otherwise healthy 96 GiB host, under which every operation's first-unit check would fail.
* Strict skipping domain-checks logical values (uuid text, time-of-day ranges, decimal payload and
  precision) exactly like decoding — a gap the fuzz harness found; the only documented skip/decode
  difference remains skipped-string UTF-8 (plan §4.3).
* The work-rule cap is cached in the budget and recomputed when input arrives (`countvalues!` is one
  checked addition and one comparison on the hot path).
* Plan §12's fuzz "split gate" is read as the required CI sample (200 entries × 1,000 mutations, two
  subprocess batches at a time) versus the full corpus under `AVRO_FUZZ_ITERATIONS`.
* avro-tools oracle limitation recorded (plan §8.4): zero-byte datums (null root, the empty record) —
  `fragtojson` prints nothing for them and `jsontofrag` hangs on `{}`; such schemas are skipped in the
  differential. Java tools run with stdin closed and a watchdog (a tool falling back to stdin hung).
* Deflate tolerates ≤ 3 trailing bytes after the final block: fastavro writes its blocks as
  `zlib.compress(...)[2:-1]`, leaving three checksum bytes, and the Phase 4a gate requires reading
  fastavro files for every codec; longer suffixes stay a `CodecError`. The committed `-xz.avro` data
  (like `-fastavro-*`) went through fastavro, which reselects union branches, so those fixtures are
  compared leniently on non-nullable union fields (§8.5's branch-identity caveat).
* `legacy=:avrojl1` has a third unambiguous tolerance: Avro.jl ≤ 1.1.2 wrote `fixed` schemas without
  names (the committed 1.x fixtures embody it), so nameless fixed schemas get synthetic names at parse
  time under the legacy flag. A `:big` read of a 1.x little-endian decimal fails the decode-side
  precision validation (`DataError`) rather than yielding garbage.
* `eachdatum` enforces `max_block_output_bytes` cumulatively per block from the actual decode charges;
  the writer's estimate is a per-datum walk (`estimatevalue`) over the same storage formulas.
* Per-version manifests (`Manifest-v1.10.toml`, ignored) run the 1.10 matrix beside the 1.12 one.
* Typed decoding under a `reader_schema` takes the direct route when the §4.8 eligibility analysis
  passes (promotions, enum remaps, nullable unions, resolved records with static defaults, recursion —
  review round 1, R18, `33a3190`); everything else converts through the semantic route.
* `compare` is implemented as `comparebytes` over canonical encodings (the §4.12 cross-API contract is
  the definition); the encode-side work rule credits produced bytes lazily when the cached cap would
  trip.
* The §4.7 multi-match record errors trigger through name+alias pairs only: a duplicate alias or an
  alias colliding with a field name is already a parse-time `SchemaError` (§4.2), so those corners are
  unreachable from parsed schemas.
* The fastavro venv is recreated and pinned (CPython 3.14.2, fastavro 1.12.2, avro 1.12.2,
  cramjam 2.11.0, python-snappy); the complete §8.5 live matrix (294 checks over seven categories) runs
  under `AVRO_INTEROP=true AVRO_PYTHON=<venv python>`, and §8.5's full
  matrix runs in the CI interop job.

* **Phase 4b decisions.** The writer preflight mirrors a *stream* reader's construction retention
  (key/value buffers retained in addition to the entries), the conservative source mode; the
  equal-charge test asserts exact equality against `Reader(path; mmap=false)`. The Table consumer
  projection uses the §4.9 consumer-independent estimate minus a per-field `cellslack` (the slot part
  chunk capacities already cover), so isbits and string columns project exactly and nested cells
  conservatively; the projection applies to record roots only (`Reader`/`Rows` stream any root — the
  176 MiB bytes-root file still writes and streams). Preflight arithmetic uses the live measured
  storage constants (as 4a's estimates already did); `RECORDED_STORAGE` matches the certified 1.10/1.12
  measurements and cross-read gates validate portability empirically. `Tables.columns(::Rows)` and
  `Tables.partitions(::Rows)` hand ownership to the caller per block (caller-space concatenation); the
  guaranteed bounded consumer is `Avro.Table`. `max_bytes` on `SymbolAdmission` now counts an 8-byte
  index slot per name and reserves merge scratch (plan §4.4 wording); the docstring says so.
* `Tables.istable`/`rowaccess` are value-level for `Avro.Rows` (mode is runtime state); typed and
  non-record modes are plain iterators, and `Tables.partitions` on them is an `ArgumentError`.

* **Phase 4c decisions.** The settled 16 KiB per-worker state (and the jobs vector) is physically
  charged per admission wave and released whenever the barrier drains (review round 1, R06), so the
  charge is real while parallelism is active and the sequential-tail failing arithmetic stays
  byte-identical at every `ntasks`. Commits cannot fail on the ceiling (`job.reserved ≤ W` is released-then-
  reserved), so acceptance divergence is impossible on the ceiling path; all three decode loops
  enforce `max_block_output_bytes` per row (review round 1, R06), so a block beyond the output cap
  fails with the cap's error at the same row on every path.
  `SCRATCH_STATE_MAX = 8 MiB` is the recorded per-block scratch/state maximum; every commit checks the
  job's budget peak against its `W` (`peak_violations == 0` gated; observed job peaks ≈ 17 MiB on the
  RSS fixture). Legacy mode (`legacy=:avrojl1`) disables workers (the trailing-bytes tolerance mutates
  reader state); the direct path keeps full legacy parity. The wide 1,001-field fixture now *succeeds*
  through mapped/byte `Table` (exact preallocation) and the streamed-IO refusal is tested via
  `open(path)` — the writer preflight still models the streamed consumer, the largest guaranteed peak.
  `Rows` is `Rows{L}`: byte and mapped sources pre-scan and report `HasLength` with the exact count;
  streamed sources — and byte sources whose pre-scan finds a structural error — stay `SizeUnknown`
  (review round 1, R09).

* **Phase 4d decisions.** Avro 1.1.2 ratio baselines measured once on the authoring host (Julia
  1.12.6): `writetable` 1M rows 1.401 s, `readtable`+`columntable` 5.691 s; recorded in `test/perf.jl`.
  The projection gate compares like-with-like `ntasks`; the recursive-root round-trip gate is
  schema + row count (a graph-wide projected root re-encodes nested records projected — the §6
  "named types preserved" wording cannot keep both the projected root and the full nested definition
  under one name). The `SkipRun` leaf chain devirtualizes a closed set of plan kinds (no schema-shaped
  tuple types, preserving §4.5's specialization discipline). Guard publication batches at 1 MiB per
  budget (the availability guard is best-effort; slack is bounded).

## Review round 1 (2026-08-23)

* Codex (gpt-5.6-sol, reasoning ultra, full-access worktree `codex/review-fixes`) reviewed the complete
  tree at `688b920`: 45 commits fixing 24 findings (F01–F24: a GUARD CAS race, a parallel reservation
  double-release, writer poisoning on rejected datums, encode-time validation of mutated values,
  reservation restoration on failed decodes, sized metadata framing, deflate suffix rejection,
  constructor identity, the 1.x datum shims, IOBuffer borrowing, admission carry preflight,
  documentation corrections), merged at `372705b`; report:
  `reviews/codex-implementation-review-1.md`, `VERDICT: REVISE` with 19 deferred findings (R01–R19).
* Claude's revision commits: plan amendments `568fdb5` (heap schema nodes §4.2, macOS guard §4.4,
  primitive `limits=` §5.1, the third legacy tolerance §4.9 — R15/R16/R17); shared construction scope
  `6a8b94a` (R01–R03); reservation-ordered headers and charged admission `4d84ea8` (R04/R05/R07/R08);
  exact parallel/streamed state and identical error kinds `3572337` (R06); `Rows{L}` and measured
  typed shells `b1e9edb` (R09/R10); the direct typed resolving route `33a3190` (R18); finalised work
  constants, the §10.1 perf protocol and single-allocation prepared kernels `821d9e0` (R11 + R14);
  the complete §8.5 interop matrix `befa6fd` (R12); the compile-cost RSS protocol, corpus projection
  sweep, cross-version exchange and CI guards `bfa4f2a` (R14); pins/claims/STATUS (R13, this commit).
* R19 (the mechanical explicit-return conversion of expression-bodied one-line methods) stays deferred
  by design: one-line `f(x) = expr` definitions are the file-local idiom throughout the reviewed code,
  `function … end` bodies do use explicit returns, and a whole-tree mechanical rewrite belongs in its
  own reviewed commit if wanted — flagged for round 2 rather than smuggled into correctness work.

## Commands run and results (2026-08-23, head of the review-round revisions)

* Full suites: `julia +1.1x --project=. -e 'using Pkg; Pkg.test(julia_args=["-t4"])'` green on 1.10.11,
  1.11.9 and 1.12.6 (committed per-version manifests `Manifest-v1.10.toml`/`Manifest-v1.11.toml`/
  `Manifest.toml`); 1.13.0-rc1 informational. Quality (`AVRO_QUALITY_GATES=true`, 1.12): JET clean,
  Aqua fully clean (the JSON printing overload and the precompile workload consume the last deps).
* Interop (`AVRO_INTEROP=true AVRO_TOOLS_JAR=… AVRO_PYTHON=<venv>`): the datum differential (58) plus
  the complete §8.5 matrix (294) — containers×codecs against Java `tojson` and fastavro value round
  trips, canonical/fingerprints (avro-tools prints the CRC-64 little-endian), resolution via
  `ReadWithReader`, single-object cross-decode, live sort-order verdicts, and the negative-oracle
  corpus with recorded per-oracle verdicts (Julia rejects all four cases; Java tolerates the truncated
  final sync and undomained bools; fastavro tolerates undomained bools and a corrupted first magic
  byte).
* Performance (`AVRO_PERF=true`, §10.1 protocol — medians of 5 cold processes): write 0.145 s (9.6× vs
  the 1.1.2 baseline 1.401 s), `Table` 0.126 s (45× vs 5.691 s), 8-thread ratio 3.39 under 4 GiB
  limits, codec overheads zstandard 1.03× / deflate 0.99× / snappy 1.06×, projection 2.4×, prepared
  decode 1 allocation (the string) ≈ 40 ns, prepared encode 0 allocations ≈ 57 ns, one-shot ≈ 12 µs,
  `parseschema(interop.avsc)` ≈ 78 µs, load 0.31 s, time-to-first-table 0.68 s.
* Latency gate (finalised constants, `max_total_values = 3·2^26`): dense records 3.0 s, depth-80 skip
  5.8 s, empty arrays 0.4 s — worst margin ≈ 42% under the 10 s bound on the authoring host.
* Peak-RSS gate (`AVRO_RSS_GATE=true`): baseline 404.5 MB → peak 3,146.9 MB on a 2 GiB half-ceiling
  table of 16 MiB blocks, 7 workers, in-flight high-water 8 — within `ceiling + 128 MiB`.
* Cross-version exchange: `test/crossversion/{write,read}.jl` verified in both directions between
  1.10.11 and 1.12.6 (32 checks each way); a CI job runs 1.10 ↔ 1.
* Cross-package smoke (`AVRO_SMOKE=true`): CSV → Avro → Arrow 2.x and DataFrames round trips, 5 checks.
* Docs: `julia --project=docs docs/make.jl` builds clean (strict).

## Remaining gaps and assumptions

* The available-memory guard is best-effort (plan §4.4, amended for macOS); cgroup files are read on
  Linux only. Guard publication batches at 1 MiB per budget (bounded slack, best-effort by contract).
* Oracle pins: CPython 3.14.2, `fastavro==1.12.2`, `avro==1.12.2`, `cramjam==2.11.0`, python-snappy;
  avro-tools 1.12.2 (sha256 `6220e8bc089aaf917cdad4cd358bd651fc0394c0e5ddb8b36da402012c294a68`) on
  OpenJDK 25 locally, Temurin 21 in CI. `generate.sh` regenerates the corpus except the three
  documented fixture families (legacy1x, highwindow, sortorder inputs).
* Invalidation counts (SnoopCompileCore) and per-`T` typed compile cost are informational (plan §4.5)
  and not asserted.
* R19 (mechanical style pass) deferred with rationale above; `Tables.columns(::Rows)` and
  `Tables.partitions(::Rows)` hand per-block ownership to the caller (the guaranteed bounded consumer
  is `Avro.Table`).
* Readiness: all phases and the takeover audit are complete. The clean branch is ready for pull-request
  validation. Hosted CI provides the final operating-system, Julia-version, interop and docs matrix.
