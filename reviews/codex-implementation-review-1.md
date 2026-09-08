# Codex implementation review 1

## Scope and disposition

I reviewed the complete tree at `688b920`, then reviewed every fix through code head `59a1e85`, against
`AVRO_REWRITE_PLAN.md` v24, the Avro 1.12 specification, `STATUS.md`, the repository conventions, and
the promised phase gates. I treated the settled plan as the contract. I did not edit the plan or any
remote state.

The review found and fixed correctness, corruption, resource-accounting, concurrency, compatibility,
and documentation defects. Material budget and release-gate work remains. The branch is not PR-ready.

## Findings

### Fixed in this review

| ID | Severity | Location | Finding | Disposition |
|---|---|---|---|---|
| F01 | Critical | `src/limits.jl:275` | GUARD publication used separate atomic load/store operations. Concurrent reservations lost updates. | Fixed by CAS publication in `63f58a6`; concurrent stress regression added. |
| F02 | High | `src/prepared.jl:51` | A failed pooled `DatumReader` call retained its GUARD reservation and pinned caller input. | Fixed with unconditional cleanup in `b71aa31`. |
| F03 | High | `src/deprecated.jl:30`, `src/container.jl:38`, `src/limits.jl:160` | Required 1.x datum shims were absent. The first shim implementation shadowed internal stream/cgroup reads; importing `Base.read` then caused type piracy and an ambiguity. | Shims added in `b188e09`; internal dispatch repaired in `27e56ba`; the package-owned `Avro.read` generic and explicit `Base.read` calls finalized in `febd03f`. |
| F04 | High | `src/plan_write.jl:180`, `src/plan_write.jl:360`, `src/plan_write.jl:479` | Mutated `Fixed`/`Record` values could emit malformed data or leak `BoundsError`; map keys that collided after string conversion silently lost data. | Fixed with encode-time validation and charged duplicate detection in `b8b6623`. |
| F05 | High | `src/plan_write.jl:331`, `src/plan_write.jl:360` | Array and map writers emitted collection blocks larger than `max_block_count`, which identical-limit readers rejected. | Fixed in `a303884`. |
| F06 | High | `src/compare.jl:16` | Strict `comparebytes` accepted invalid UTF-8 strings that strict decode rejected. | Fixed in `de8c239`. |
| F07 | High | `src/schema.jl:1098`, `src/schema.jl:447`, `src/logical.jl:100`, `src/values.jl:82` | Public primitive constructors retained invalid logical annotations; aliases were wrongly name-validated; fixed-decimal precision was one too high at a large boundary; timestamp subtraction could wrap. | Fixed in `2c7c3a9`, `cd31a0c`, `588754a`, and `84a582b`. |
| F08 | High | `src/container.jl:299`, `src/container.jl:760`, `src/parallel.jl:180`, `src/tables.jl:200` | Strict streamed/container paths decoded values twice, and Writer/Reader paths did not consistently enforce cumulative rows and blocks. Acceptance differed by consumer and `ntasks`. | Fixed in `6ac7061` and `132e7a4`. |
| F09 | High | `src/container.jl:83`, `src/container.jl:715`, `src/container.jl:897` | `Reader(IOBuffer)` copied caller storage before budgeting. Abandoned caller-IO writers leaked GUARD reservations. | Borrowed bounded IOBuffer storage in `030c401`; installed non-closing cleanup for caller IO in `2daac34`; finalizer regression stabilized in `8dcebc6`. |
| F10 | Medium | `src/singleobject.jl:111`, `src/schema.jl:1458` | Single-object decode could not request the valid typed target `Nothing`; `minsize` could overflow to a negative value. | Fixed in `0904c3d` and `860b3c5`. |
| F11 | High | `src/container.jl:97`, `src/generic.jl:77` | Sized metadata-map framing was ignored. Duplicate metadata detection did uncharged quadratic work. | Exact sized-block exhaustion added in `f671b6d`; duplicates moved to the charged sorted-map pass in `20c5912`. |
| F12 | Critical | `src/parallel.jl:278`, `src/tables.jl:105` | Invalid `ntasks` values were accepted. A failure after parallel reservation transfer released the job reservation twice and could erase unrelated reservations. | Fixed in `1c7a106` and `9a9ce89`. |
| F13 | High | `src/tables.jl:200`, `src/tables.jl:441`, `src/tables.jl:483` | `Tables.partitions(Rows)` skipped the per-block output cap. `Tables.columns(Rows)` retained released chunks and assembled unreserved final columns above the ceiling. | Fixed in `63d2dac` and `d6808b6`. |
| F14 | High | `src/codecs.jl:179`, `ext/AvroCodecXzExt.jl:16`, `ext/AvroCodecBzip2Ext.jl:12` | Empty/padding-only compressed payloads were accepted. Member work was charged before decompressed input credit, rejecting valid many-member blocks. Deflate accepted 1–3 arbitrary suffix bytes after `BFINAL`. | Fixed in `45005dd`, `ac402b9`, and `93d67e5`; fastavro suffix fixtures are now explicit rejection fixtures in `3292804`. |
| F15 | High | `src/admission.jl:119` | A failed carry/cascade reservation mutated the symbol-admission table and made the failed name visible on retry. | Preflighted the carry merge in `13498d6`. |
| F16 | High | `src/schema.jl:314` | Guarded IO normalization allocated/grow-copied before complete reservations and reported the schema byte limit for datum IO. | Replaced it with exact, pre-reserved growth and the correct limit identity in `9e1a4fe`. |
| F17 | High | `src/container.jl:937` | The datum shim guard rejected valid `Tables.Partitioner` sources. | Fixed in `387c9eb`. |
| F18 | High | `src/schema.jl:1175`, `src/schema.jl:1398` | Public constructor imports silently merged distinct named schemas with one fullname. In-builder duplicates could then print as a reference to the wrong definition while retaining a different write plan. | Frozen-child identity fixed in `4bf92aa`; graph-wide fullname uniqueness fixed in `491f97e`. |
| F19 | Medium | `README.md:49`, `CHANGELOG.md:12`, `docs/src/manual/container.md:16`, `docs/src/manual/encoding.md:24`, `docs/src/manual/limits-and-security.md:70`, `src/prepared.jl:182` | Documentation overstated logical-type completeness and DatumWriter task safety, misstated atomic-write/fsync scope, and left `encode!` without an attached docstring so strict Documenter failed. | Corrected and regression-tested in `35dd2ba` and `f7ccc59`; the Writer docstring was aligned with the settled failure rule by F20. |
| F20 | High | `src/container.jl:750` | Rejected datums and pre-encode limit errors did not poison `Writer`, contrary to §4.9. A failed encode also left budget counters changed, so later valid data could fail under a limit. | The complete `push!` operation now poisons once and preserves the original cause in `897fd31`; pending output is discarded and docs/tests match the plan. This supersedes the incorrect recoverable-datum wording introduced by `ff4d206`. |
| F21 | Medium | `src/compare.jl:58` | `comparebytes` charged a pair of decoded values as one. Two root integers were accepted with `max_total_values=1`. | Fixed by charging both inputs in `573fa60`; boundary regression added. |
| F22 | High | `src/codecs.jl:179`, `src/container.jl:299`, `ext/AvroCodecXzExt.jl:16`, `ext/AvroCodecBzip2Ext.jl:12` | Failed decompression and block validation retained output, workspace, or streamed-payload reservations instead of restoring the Reader baseline. | Reservation checkpoints now restore only failed-call transients in `fb83c5f`; all five compressed codecs, byte/stream sources, Reader reuse, and idempotent close are covered. |
| F23 | High | `src/tables.jl:445`, `src/tables.jl:483` | `Tables.partitions(Rows)` released only vector shells when output ownership transferred, so referenced payload charges ratcheted across blocks. `Tables.columns(Rows)` had the same incomplete transfer boundary. | Fixed in `ca3664d`; regressions cover per-yield baselines, retained assembly headroom, error rollback, and close. |
| F24 | High | `README.md:51`, `README.md:78`, `README.md:82`, `CHANGELOG.md:17`, `CHANGELOG.md:40`, `CHANGELOG.md:71`, `docs/src/manual/evolution.md:3`, `docs/src/migration.md:50`, `docs/src/manual/container.md:71`, `docs/src/manual/container.md:79` | The migration recipes omitted little-endian decimal handling; `Avro.inspect` was said to detect payload defects that it does not inspect; `reader_schema=` was claimed on every read path although block-level `Reader` has no such option. | README/CHANGELOG fixed in `ecb01ee`; manual/migration claims and regressions completed in `59a1e85`. |

Every fix commit includes a regression and the required `Co-Authored-By` trailer.

### Deferred material findings

| ID | Severity | Location | Finding | Disposition and reason |
|---|---|---|---|---|
| R01 | High | `src/schema.jl:157`, `src/schema.jl:1098`, `src/schema.jl:1175`, `src/types.jl:101`, `README.md:10`, `README.md:64`, `CHANGELOG.md:9`, `CHANGELOG.md:32`, `docs/src/manual/limits-and-security.md:10` | Public schema constructors and `schema(T)` do not own the required complete budget or enforce all graph limits. Zero-node, zero-depth, zero-name-byte, zero-named-type, derived zero-node, and derived zero-field probes succeed. Primitive wrappers do not accept `limits`. The public fixed-limit claims are therefore false. | Deferred. This needs one shared schema-construction budget and uniform graph validation across every construction/derivation path, not another local check. |
| R02 | High | `src/schema.jl:860`, `src/canonical.jl:11`, `src/canonical.jl:107` | `json` has no `limits` keyword. `canonical` accepts but ignores it. Printer buffers and seen tables are unbounded; `fingerprint` inherits the defect. | Deferred. Implement the §4.4 category-(e) printer/canonical scope once and pass it through all nested operations. |
| R03 | High | `src/singleobject.jl:48`, `src/singleobject.jl:91`, `src/singleobject.jl:111` | Single-object and SchemaCache operations stack fresh budgets or ignore limits. Final/intermediate buffers and cache vector growth/moves are unreserved. Cache byte accounting omits retained vector capacity. | Deferred. These APIs require one operation budget, transactional cache insertion, exact capacity charges, and charged comparison/insertion work. |
| R04 | High | `src/container.jl:640`, `src/container.jl:841`, `src/encoder.jl:131`, `src/codecs.jl:307`, `README.md:64`, `CHANGELOG.md:32`, `CHANGELOG.md:36`, `docs/src/manual/container.md:32`, `docs/src/manual/limits-and-security.md:10`, `docs/src/manual/limits-and-security.md:35` | Writer creates schema JSON, entry vectors, and complete metadata copies before its `Budget` exists. It later copies the pending encoder and allocates compressor output before reservation. The public allocation/preflight claims are therefore false. | Deferred. Construct the budget first. Reserve every retained header copy, then use a compression API that reserves the exact overlap before copy or codec entry. |
| R05 | High | `src/container.jl:121`, `src/container.jl:128`, `src/container.jl:129`, `CHANGELOG.md:36`, `docs/src/manual/container.md:32`, `docs/src/manual/limits-and-security.md:35` | Streamed metadata key/value copies are reserved after allocation. Temporary stream-payload reservations are not released after ownership moves, so byte and streamed sources can have different acceptance at one ceiling. | Deferred. Rework header ownership transitions and add byte/stream charge-equivalence gates. |
| R06 | High | `src/container.jl:505`, `src/tables.jl:200`, `src/parallel.jl:31`, `src/parallel.jl:353`, `src/parallel.jl:369`, `README.md:61`, `CHANGELOG.md:30`, `docs/src/manual/tables.md:46` | Parallel pre-scan grows `BlockEntry[]` before reservation, charges 32 bytes for a 40-byte entry, and allocates the jobs vector and worker state without reservation. Streamed Table state (`counts`, `chunkcols`, per-block `keep`) also grows before exact capacity charges. The implementation also substitutes unreserved 64 KiB worker state for the settled charged 16 KiB term and permits error-kind divergence, contrary to public identical-error claims. | Deferred. Use exact-capacity incremental tables, reserve coordinator/stream state before allocation, restore the settled worker charge, and enforce identical lowest-index errors. |
| R07 | High | `src/admission.jl:54`, `src/admission.jl:81`, `src/admission.jl:136`, `src/typed.jl:347` | Admission lookup, sorting, and merging have no operation `Budget`; typed Symbol decoding can perform prefix comparison work with `compare_bytes == 0` and mutate admission state. | Deferred. Thread the caller budget through lookup/maintenance and preflight mutation after charged work. |
| R08 | Medium | `src/plan_write.jl:128`, `src/container.jl:857`, `CHANGELOG.md:36`, `docs/src/manual/container.md:32`, `docs/src/manual/limits-and-security.md:35` | Writer work accounting omits collection/container block framing, so writer and reader can make different work-rule decisions under identical limits. | Deferred. The writer and reader need the same decompressed-payload-plus-framing arithmetic and boundary tests. |
| R09 | Medium | `src/tables.jl:265`, `src/tables.jl:336` | `Rows` always reports `SizeUnknown`; byte-backed rows have no `length` although block pre-scan is possible. | Deferred. The settled contract requires source-specific iterator types or wrappers. |
| R10 | Medium | `src/typed.jl:458`, `STATUS.md:43` | Typed fast-route shells use `16 + sizeof(T)` rather than the settled measured-per-`T` probe and exactness oracle. | Deferred. Replace the admitted simplification with the plan's probe, cache, and generated layout matrix. |
| R11 | High | `test/perf.jl:1`, `benchmarks/logs/avro112.log:2` | The perf gate uses one warm process and best-of-three, hard-coded baselines that differ from the committed log, and mismatched 1.x/v2 data. It omits deflate/snappy, one-shot, load, TTFT, and required assertions. Current prepared decode is 8 allocations (target ≤1); warmed schema parse is about 2,062 allocations (target ≤300). | Deferred. Rebuild the gate around five cold processes and fail every numeric target. |
| R12 | High | `test/interop.jl:45`, `.github/workflows/CI.yml:64`, `test/fixtures/generate.sh:6`, `test/fixtures/generate.sh:9`, `test/fixtures/generate.sh:28` | Live interop executes only Java raw-datum checks and skips zero-byte deterministic datums. Python is installed but unused. Container/codec/root/resolution/canonical/single-object/sort/negative matrices are absent. The generator targets missing directories, assumes an unbuilt Java harness, and asks fastavro for only two codecs. `README.md:68` therefore makes a false CI claim. | Deferred. Rebuild and verify the reproducible oracle corpus and complete §8.5 matrix, including zero-byte roots through a Java harness. |
| R13 | High | `.gitignore:4`, `test/Project.toml:25`, `STATUS.md:30`, `STATUS.md:64`, `STATUS.md:120` | No supported-version Manifest or exact test pin set is tracked. An ignored Manifest resolved by Julia 1.12 made Julia 1.10 select incompatible PrecompileTools until a separate 1.10 resolution was generated. `STATUS.md` is materially stale about phases, deflate, admission, dependency use, results, and readiness. | Deferred. Commit reproducible supported-version test pins and rewrite STATUS only after all remaining fixes and gates. |
| R14 | High | `test/gates.jl:85`, `test/invariant.jl:57`, `test/latency.jl:14`, `test/projection.jl:9`, `src/precompile.jl:7`, `.github/workflows/CI.yml:10`, `docs/src/manual/limits-and-security.md:19` | The compile-cost RSS protocol measures a second 1,000-schema batch instead of the agreed post-`E` batch. Phase 4a container calibration is still provisional, and its 10-second latency gate crossed the threshold in loaded-host review attempts even though the final isolated 1.11/1.12 runs passed at 9.40/7.99 seconds. Projection does not cover every fixture or a Rows matrix. CI has no cross-version file-artifact exchange. Precompile omits a representative struct and load/TTFT gates. The duplicate-run guard covers only one job. | Deferred. Complete the exact phase and release-engineering gates, including cross-version producer/consumer artifacts, then calibrate on every supported runtime and record the host protocol. |
| R15 | High | `src/limits.jl:202`, `STATUS.md:52` | On macOS the available-memory guard adds inactive pages instead of using the settled `Sys.free_memory()` formula. This silently makes the guard less conservative on the main development platform. | Deferred. Revert to the settled formula or obtain a plan amendment with measured safety evidence and portable tests. |
| R16 | Medium | `src/schema.jl:63`, `STATUS.md:35` | Every schema node is a `mutable struct` with `const` fields, while §4.2 explicitly requires immutable `struct` nodes and transitive immutability. The change is tied to an alternate storage model and was taken without agreement. | Deferred. Restore the settled representation, or amend §4.2 and re-certify identity, storage, hashing, recursion, and cross-version behavior. |
| R17 | Medium | `src/schema.jl:486`, `STATUS.md:69`, `CHANGELOG.md:72`, `docs/src/manual/container.md:74`, `docs/src/migration.md:49` | `legacy=:avrojl1` accepts nameless fixed schemas as a third tolerance, but §4.9 settles exactly two tolerances. The committed 1.1.2 fixture is useful evidence, but it does not amend the contract. | Deferred. Either remove the extra acceptance or amend the plan and document its exact framing boundary. |
| R18 | Medium | `src/typed.jl:114`, `STATUS.md:76` | Every resolving read plan is forced onto the semantic typed route before the settled eligibility analysis. Eligible resolved DTOs therefore miss the required direct typed route. | Deferred. Build the typed resolving plan family and run the constructor/eligibility/layout gates. |
| R19 | Low | `src/canonical.jl:100` and many peers | The codebase still contains hundreds of expression-bodied methods and other first-party convention violations despite the repository's explicit-return and typed-empty-literal rules. | Deferred under the user's material-first direction. Apply a separately reviewed mechanical cleanup after correctness/resource work. |

## Review assumptions and decisions

* I treated v24 and its settled §14 decisions as authoritative, including where the implementation or
  `STATUS.md` chose a different contract.
* I used isolated ignored manifests for each Julia runtime. I did not treat those local resolutions as
  the tracked exact pins required for PR readiness.
* I fixed defects that had a bounded local ownership or validation repair. I deferred changes that need
  a shared budget architecture, representation redesign, regenerated oracle corpus, or new performance
  protocol. This was a review-scope decision, not approval of those deviations.
* I classified the fastavro deflate suffix files as negative fixtures because the settled exact-exhaustion
  rule is explicit. A different compatibility choice needs the plan amendment proposed below.

## Verification

Commands are shown exactly as run from the review worktree.

| Command | Result |
|---|---|
| `julia +1.12 --project=. -e 'using Pkg; Pkg.test(julia_args=["-t4"])'` at `688b920` | 8,240/8,240 passed in 8m56.3s. |
| `AVRO_QUALITY_GATES=true julia +1.12 --project=. -e 'using Pkg; Pkg.test(julia_args=["-t4"])'` at `59a1e85` | 8,469/8,469 passed in 9m41.4s, including Aqua and JET; the depth-80 latency case was 7.99s. |
| `rg '^julia_version' Manifest.toml && julia +1.10 --project=. -e 'using Pkg; Pkg.test(julia_args=["-t4"])'` with the isolated 1.10 resolution at `59a1e85` | `julia_version = "1.10.11"`; 8,458/8,458 passed in 8m02.6s. |
| `JULIA_DEPOT_PATH=/tmp/avro-review-depot11.61JHtG:/Users/jacob.quinn/.julia JULIA_LOAD_PATH=/tmp/avro-review-testenv11.9vSmLw:/Users/jacob.quinn/.julia/dev/Avro-codex-review:@stdlib julia +1.11 --compiled-modules=no -t4 --project=/tmp/avro-review-testenv11.9vSmLw -e 'include("test/runtests.jl")'` with the isolated 1.11 resolution at `59a1e85` | 8,448/8,448 passed in 9m40.7s; the depth-80 latency case was 9.40s. |
| `julia +1.13 --project=. -e 'using Avro; println(VERSION); s=Avro.IntSchema(); @assert Avro.decode(s, Avro.encode(s, Int32(1))) == 1'` at `59a1e85` | Passed on Julia 1.13.0-rc1 (informational). |
| `AVRO_INTEROP=true AVRO_TOOLS_JAR=/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/0203e6d6-c082-4e1d-8728-87312a161fb7/scratchpad/tools/avro-tools-1.12.2.jar julia +1.12 --project=. -e 'using Test, Avro; const FIXTURES=joinpath(pwd(), "test", "fixtures"); include("test/interop.jl")' </dev/null` at `59a1e85` | 58/58 interop assertions passed in 30.0s. Live oracle calls covered Java raw datums and skipped zero-byte roots; this is the incomplete matrix in R12. |
| `AVRO_PERF=true julia +1.12 -t8 --project=. -e 'using Test, Avro; include("test/perf.jl")'` at `59a1e85` | 6/6 implemented assertions passed in 8.5s. It reported 8 prepared-decode allocations and the incomplete measurements described in R11. |
| `julia +1.12 --startup-file=no --project=. -e 'using Avro; avsc=read(joinpath("test", "fixtures", "apache", "interop.avsc"), String); Avro.parseschema(avsc); println("parseschema_allocations=", @allocations Avro.parseschema(avsc))'` at `59a1e85` | `parseschema_allocations=2062`; the §10.2 target is at most 300. |
| `julia +1.12 --project=. -e 'using Avro; outcome(f)=try f(); :accepted catch e; nameof(typeof(e)) end; nt=NamedTuple{(:x,),Tuple{Int64}}; s=Avro.IntSchema(); L=Avro.Limits(max_schema_bytes=0, max_schema_nodes=0); println((array=outcome(() -> Avro.ArraySchema(Avro.NullSchema(); limits=Avro.Limits(max_schema_nodes=0))), derived_fields=outcome(() -> Avro.schema(nt; limits=Avro.Limits(max_fields=0))), json=outcome(() -> Avro.json(s; limits=L)), canonical=outcome(() -> Avro.canonical(s; limits=L)), single=outcome(() -> Avro.encodesingle(s, Int32(1); limits=L))))'` at `59a1e85` | `array`, `derived_fields`, `canonical`, and `single` were accepted; `json` raised `MethodError`. This reproduces R01–R03. |
| `julia +1.12 --project=. -e 'using Avro; println((entry_bytes=sizeof(Avro.BlockEntry), charged_bytes=Avro.blocktablecharge(1)-Avro.blocktablecharge(0), worker_state=Avro.WORKER_STATE))'` at `59a1e85` | `entry_bytes=40`, `charged_bytes=32`, `worker_state=65536`. This reproduces the R06 accounting mismatch. |
| `AVRO_RSS_GATE=true julia +1.12 -t8 --project=. -e 'using Test, Avro; include("test/rssgate.jl")'` at `59a1e85` | 6/6 passed in 17.6s; baseline 317.6 MiB, peak 3,089.7 MiB, ceiling 4,096 MiB. |
| `AVRO_SMOKE=true julia +1.12 --project=. -e 'push!(LOAD_PATH, joinpath(pwd(), "test")); using Test, Avro, Dates; include("test/smoke.jl")'` at `59a1e85` | 5/5 CSV → Avro → Arrow/DataFrames checks passed in 17.1s. |
| `julia +1.12 --project=docs -e 'using Pkg; Pkg.develop(path=pwd()); Pkg.instantiate()'` then `julia +1.12 --project=docs docs/make.jl` at `59a1e85` | Instantiation was unchanged; strict doctest, cross-reference, document, and HTML checks passed. |
| `git diff --check 688b920..HEAD && for c in $(git rev-list 688b920..HEAD); do test "$(git show -s --format=%B "$c" \| rg -c '^Co-Authored-By: Codex <codex@openai.com>$')" = 1 \|\| exit 1; done` | Passed after this report commit: 45/45 review commits have exactly one required trailer; no merge, empty, binary, or unrelated-path commit was found. |

The final supported-version totals above are the authoritative suite results. Julia 1.11 cache-writing
deadlocked in compiler shutdown both with parallel and serial precompile. The recorded full run uses a
fresh resolved test environment with compiled modules disabled, so it tests the package without touching
the other worktree's long-running process or cache locks. Earlier loaded-host latency attempts crossed
the 10-second threshold. The final 1.11 and 1.12 runs passed it; R14 records that the calibration and host
protocol are still provisional, not a current suite failure.

## Disagreements with `STATUS.md`

| STATUS decision or claim | Review disposition |
|---|---|
| Lines 64–68 allow up to three deflate suffix bytes to accept fastavro output. | Rejected. Settled §4.9/§14 requires exact `BFINAL` exhaustion. `93d67e5` restores that contract. The affected fastavro files are nonconforming rejection fixtures unless the plan changes. |
| Lines 97–100 call `Tables.columns(::Rows)` concatenation caller space and limit the bounded guarantee to `Avro.Table`. | Rejected. The package methods perform the materialization. `d6808b6` and `ca3664d` now keep columns and partitions under guarded ownership. |
| Lines 104–107 keep worker state only in admission arithmetic. | Rejected. The plan requires complete reservations before package-owned allocation. Coordinator/jobs/pool storage remains R06. |
| Lines 107–110 allow a different parallel error kind when the ceiling intervenes. | Rejected. The settled contract requires the same lowest-index exception at every `ntasks`. |
| Lines 117–118 call source-sensitive `Rows` length not expressible. | Rejected. It is expressible by separate iterator types/wrappers and is an explicit §6 contract. |
| Lines 43–45 replace measured typed shells with a formula. | Rejected as an unagreed simplification; R10 remains. |
| Lines 52–53 replace `Sys.free_memory()` with free plus inactive macOS pages. | Rejected. This weakens the settled available-memory guard without an amendment or a portable safety gate; R15 remains. |
| Lines 35–38 replace immutable schema structs with mutable heap objects carrying `const` fields. | Rejected as an unagreed representation change. The rationale may support a plan amendment, but the implementation cannot silently replace §4.2; R16 remains. |
| Lines 69–72 add a third legacy tolerance for nameless fixed schemas. | Rejected under the current “exactly two” decision. The real 1.1.2 fixture supports a focused amendment; R17 remains meanwhile. |
| Lines 76–78 defer every resolved typed target to the semantic route. | Rejected. The settled eligibility rule also applies to resolving typed plans; R18 remains. |
| Line 75 relies on ignored per-version manifests. | Rejected for PR readiness. The plan requires tracked exact test pins. |
| Lines 85–87 imply CI supplies the full fastavro/Java matrix. | Rejected. Current CI installs Python but the test never invokes it. |
| Lines 23–31 mark phases 0–4c done while their acceptance gates remain incomplete; lines 30–31, 120–127, 150–164 also contradict the current tree. | “Done” describes landed files, not accepted phases. The record is not current and fails the PR-ready status gate. |
| Lines 89–97 claim exact streamed-reader preflight and cross-version portability gates. | Rejected. R05 disproves exact streamed ownership, and the tests perform no cross-version artifact exchange. |
| Lines 49–51 change the compile-cost RSS protocol to a second thousand schemas. | Rejected. The settled gate measures the first 1,000 heterogeneous schemas after the `E` warm-up. |

## Suggested plan amendments

1. Resolve the internal fastavro-deflate conflict. Keep the settled strict suffix rule and explicitly
   classify fastavro 1.12.2 files produced by `zlib.compress(...)[2:-1]` as nonconforming rejection
   fixtures; do not also require Julia to accept those exact files.
2. Add `limits=Limits()` to the primitive public-constructor synopsis in §5.1. The global §4.4 scope
   already requires it, but the synopsis currently omits it.
3. If mutable heap schema nodes remain necessary, replace §4.2's immutable-struct representation with
   that design explicitly and re-state its identity/storage guarantees. Do not treat `const` fields as
   an implicit amendment.
4. Add the narrowly evidenced nameless-fixed `legacy=:avrojl1` tolerance for the committed real 1.1.2
   fixture, including its exact framing boundary, or remove the tolerance from the implementation.

The macOS available-memory change needs evidence before it merits a plan amendment. The other remaining
work implements contracts that are already clear.

## Required revision work

Claude must still do exactly the following before another implementation review:

1. Implement one complete, shared, pre-allocation budget for every schema constructor/deriver,
   printer/canonical/fingerprint operation, single-object operation, and SchemaCache operation. Enforce
   every schema/work/capacity limit transactionally.
2. Move Writer header construction and compression, streamed metadata ownership, and SymbolAdmission
   work under exact pre-allocation reservations. Correct writer framing work arithmetic. Add boundary and
   acceptance-equivalence regressions.
3. Reserve the exact parallel and streamed-Table block tables, capacities, jobs, and coordinator state
   before allocation. Restore the settled worker-state charge and identical error semantics.
4. Implement source-specific `Rows` length/`IteratorSize` behavior. Replace the typed-shell formula with
   the measured-per-`T` design, and implement eligible typed resolving plans with the layout oracle.
5. Resolve the unagreed schema-node, macOS available-memory, and nameless-fixed legacy deviations against
   the current plan or the explicit amendments proposed above.
6. Replace the fixture generator and live interop job with the complete reproducible Java and Python
   matrix. If the user approves suggested amendment 1, apply that classification; otherwise preserve
   the settled strict suffix rule and treat the affected fastavro files as negative fixtures.
7. Replace the performance gate with the five-cold-process protocol. Enforce all §10.2 targets, including
   decode and parse allocations, all three codec-overhead rows, one-shot timings, package load, and TTFT.
8. Finish the exact compile-cost and container latency protocols, every-fixture Table/Rows projection
   tests, cross-version artifact exchange, representative-struct precompile workload, examples/docs/CI
   coverage, and same-repository CI duplicate guards.
9. Commit exact supported-version test dependency pins, correct remaining README, CHANGELOG, manual,
   and migration safety/interoperability claims, update `STATUS.md` from the final exact head,
   then rerun the complete 1.10/1.11/1.12, quality, interop, performance, RSS, smoke, docs, and clean-source
   regeneration matrix.
10. After material correctness is complete, bring first-party code into the documented explicit-return
   convention with a mechanical, separately reviewed change.

VERDICT: REVISE
