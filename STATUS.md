# Avro.jl 2.0 rewrite — status record

Branch `jq/v2-rewrite` (from `main @ 0c7be10`, v1.1.2). Plan: `AVRO_REWRITE_PLAN.md` (AGREED v24).
Boundary: local implementation only — no push, PR, merge, tag, registration or other remote change.
Readiness target of this task: **PR-ready** (plan §12); nothing is pushed.

## Plan review loop (closed)

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
| 5 — Release engineering | in progress (final matrix running) | `src/deprecated.jl` (`readtable`/`writetable` shims: `legacy=:avrojl1`, `compress=:zstd → codec=:zstandard`; tested against the committed 1.1.2 fixtures, `test/deprecated.jl` 6), `src/precompile.jl` (PrecompileTools workload: parse/print/canonical/fingerprint, prepared + one-shot codecs, container round trips over null/deflate/zstandard/snappy, `Table` + projection, `Rows`, single-object, JSON encoding — precompile 9 s ≤ 15, load 0.25 s ≤ 0.5, time-to-first-table 0.42 s ≤ 1.5), the `JSON.json(::Schema)` printing overload (clears the last Aqua stale dep with PrecompileTools), `TableRow` (duck-typed Tables rows — `DataFrames.DataFrameRow` — encode through the row interface; found by the smoke), README/CHANGELOG rewritten, `docs/` (Documenter: index, 10 manual pages, migration, benchmarks, autodocs reference; builds clean locally), `examples/` (CSV→Avro→Arrow, single-object Kafka-style, evolution, StructUtils mapping), CI extended (docs, trim, Arrow-3-informational jobs; CompatHelper), `test/trim/` (juliac --trim smoke program + driver, informational), `test/smoke.jl` (opt-in `AVRO_SMOKE=true`: CSV → Avro → Arrow 2.x and DataFrames round trips, 5 green; CSV/Arrow/DataFrames added to test deps) |

## Decisions taken without user direction during implementation

* **Schema nodes are heap objects** (`mutable struct` with `const` fields): as plain immutable structs
  they were inlined into every value referencing them (`sizeof(Avro.Record)` was 104 bytes, a
  `Vector{EnumValue}` slot 104 bytes), which broke the §4.4 (b) formulas and the
  `summarysize(x; exclude=Avro.Schema)` oracle. Nodes stay immutable; `===` is pointer identity.
* Storage formulas charge identity-bearing structs stored inline in typed vectors at production as well
  as for their slot (the decoder's charge model; also what Julia 1.10's `summarysize` reports);
  `widedecimalbytes` keeps two limbs of GMP slack (the negative path over-allocates); the oracle is
  capacity-aware (compacted maps keep their `npairs` slots, as §4.4 prescribes).
* Typed fast-route shells are charged by formula (`16 + sizeof(T)` for non-isbits targets) rather than
  measured per `T` from a probe instance (plan §4.4) — a simplification to revisit with the Phase 4b
  storage oracle over typed layouts.
* `Avro.Map(pairs)` narrows its inferred value type to the generic model (`promote_typejoin`, nested
  collections as `Any`) and collects pairs with `@nospecialize`, so user value shapes compile nothing
  new; the explicit `Map{V}(pairs)` keeps the caller's `V`.
* The compile-cost gate's RSS bound is measured over a second thousand schemas: `Sys.maxrss` is a
  high-water mark and the first thousand brings the GC heap to its steady-state peak (periodic
  collections made the delta larger, not smaller).
* `available_memory()` on macOS adds inactive pages (`host_statistics64`): `Sys.free_memory()` counts
  only free pages, which fall to a few hundred MB on a busy host and tripped the guard under load.
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
* Typed decoding under a `reader_schema` takes the semantic route (generic reader values, then
  `StructUtils.make` in caller space); a fast route over resolving plans is deferred to the Phase 4d
  performance work.
* `compare` is implemented as `comparebytes` over canonical encodings (the §4.12 cross-API contract is
  the definition); the encode-side work rule credits produced bytes lazily when the cached cap would
  trip.
* The §4.7 multi-match record errors trigger through name+alias pairs only: a duplicate alias or an
  alias colliding with a field name is already a parse-time `SchemaError` (§4.2), so those corners are
  unreachable from parsed schemas.
* fastavro was not re-run for the resolution matrix in this session (the authoring venv was not
  recreated); the Java fixtures (`ReadWithReader`, both readers) are the live oracle, and §8.5's full
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

* **Phase 4c decisions.** The worker-pool state term (`WORKER_STATE` × workers) lives in the admission
  arithmetic only, not as a main-budget reservation — charging it polluted the failing `LimitError`'s
  `observed` and broke identical acceptance by construction; pool memory (~64 KiB/worker) sits inside
  the scratch allowance. Commits cannot fail on the ceiling (`job.reserved ≤ W` is released-then-
  reserved), so acceptance divergence is impossible on the ceiling path; for blocks beyond the
  per-block caps (malformed) the error kind may be `:max_total_bytes` instead of
  `:max_block_output_bytes` when the ceiling intervenes first — same failing block either way.
  `SCRATCH_STATE_MAX = 8 MiB` is the recorded per-block scratch/state maximum; every commit checks the
  job's budget peak against its `W` (`peak_violations == 0` gated; observed job peaks ≈ 17 MiB on the
  RSS fixture). Legacy mode (`legacy=:avrojl1`) disables workers (the trailing-bytes tolerance mutates
  reader state); the direct path keeps full legacy parity. The wide 1,001-field fixture now *succeeds*
  through mapped/byte `Table` (exact preallocation) and the streamed-IO refusal is tested via
  `open(path)` — the writer preflight still models the streamed consumer, the largest guaranteed peak.
  `Rows` keeps `IteratorSize` `SizeUnknown` at the type level (Base's protocol is type-level; a
  per-instance `HasLength` for pre-scanned byte sources is not expressible without splitting the type).

* **Phase 4d decisions.** Avro 1.1.2 ratio baselines measured once on the authoring host (Julia
  1.12.6): `writetable` 1M rows 1.401 s, `readtable`+`columntable` 5.691 s; recorded in `test/perf.jl`.
  The projection gate compares like-with-like `ntasks`; the recursive-root round-trip gate is
  schema + row count (a graph-wide projected root re-encodes nested records projected — the §6
  "named types preserved" wording cannot keep both the projected root and the full nested definition
  under one name). The `SkipRun` leaf chain devirtualizes a closed set of plan kinds (no schema-shaped
  tuple types, preserving §4.5's specialization discipline). Guard publication batches at 1 MiB per
  budget (the availability guard is best-effort; slack is bounded).

## Commands run and results (2026-08-22)

* `julia +1.12 --project=. -e 'using Pkg; Pkg.test()'` — 6,435 tests pass (Julia 1.12.6; ~4.5 min:
  latency gate ≈ 28 s, fuzz sample ≈ 1.5 min). `julia +1.10 --project=. -e 'using Pkg; Pkg.test()'` —
  6,435 tests pass (Julia 1.10.11; ~3.5 min). (After Phase 3.)
* `AVRO_QUALITY_GATES=true … Pkg.test()` (1.12.6): JET `report_package` clean; Aqua clean except
  `stale_deps` — CodecZlib, CodecZstd, Snappy, TranscodingStreams, Zstd_jll, Mmap, JSON and
  PrecompileTools are declared for Phases 4a/5 and unused so far.
* `AVRO_INTEROP=true AVRO_TOOLS_JAR=… Pkg.test()` (1.12.6, OpenJDK 25, avro-tools 1.12.2 with the
  pinned sha256): 58 checks pass — byte-exact `jsontofrag` agreement for every deterministic root and
  record schema, semantic agreement in both directions for arrays and maps.
* Compile-cost gate: 0 new method instances for Avro's functions after the `E` warm-up over 1,000 random
  heterogeneous schemas; retained RSS growth over a second thousand 4–7 MB (both versions).
* Storage oracle: `heldbytes(x) ≥ summarysize(x; exclude=Avro.Schema)` and decoder reservation ≥ formula
  for every member of `E` (both union choices), wide records, nested collections, duplicate/prefix-heavy/
  reversed maps and wide decimals of both signs (804 tests); layout probes equal the recorded constants
  on 1.10.11 and 1.12.6.
* Latency gate (worst-density legal inputs, single thread, Apple M-series): skip 16M one-boolean/14-null
  records (256M values) 2.6 s / 2.5 s (1.12 / 1.10); column decode of 16M such rows 2.5 s / 2.2 s;
  skip 2.77M depth-80 records at `max_total_values` 6.4 s / 6.9 s; 16M empty arrays 0.3 s / 0.2 s;
  `fromjson` of a 60 MB object with 1 KB-prefix keys 0.4–0.7 s; a 600K-key reversed prefix map
  0.1–0.2 s. All ≤ 10 s; constants remain provisional (plan §4.4).
* Fuzz: the recorded sample (`test/fuzz/sample.tsv`, 200 × 1,000 = 200,000 cases) runs clean in 8
  sandboxed batches; its first run found the two defects fixed in `Domain-check skipped logical
  values…` (skip/decode equivalence for logical types; `escapename` byte slicing).
* Allocation budgets: primitive decode 0, typed isbits record 0, column decode per isbits row 0, one
  String per string cell.

## Remaining gaps and assumptions

* The symbol-admission table merges runs eagerly at carry time; the deamortised fixed-step merge of
  plan §4.4 is scheduled with its Phase 4b gate.
* The available-memory guard is best-effort (plan §4.4); cgroup files are read on Linux only.
* Python oracle pin: CPython 3.14 (3.14.2 in the authoring venv), `fastavro==1.12.2`, `avro==1.12.2`,
  `cramjam==2.11.0`; Java oracle: avro-tools 1.12.2 (sha256
  `6220e8bc089aaf917cdad4cd358bd651fc0394c0e5ddb8b36da402012c294a68`), run on OpenJDK 25 locally and
  Temurin 21 in CI.
* The latency gate's worst shape (deep-record skip, 6.4–6.9 s) leaves limited margin on slower CI hosts;
  the plan finalises the provisional work constants at the Phase 4a calibration (lowering
  `max_total_values` halves that time).
* Invalidation counts (SnoopCompileCore) and per-`T` typed compile cost are not measured (informational
  in plan §4.5).
* Aqua `stale_deps` stays red until Phases 4a/5 consume the codec/JSON/Mmap/PrecompileTools dependencies.
* Readiness: Phases 0–2 locally validated on both supported versions; not yet review-ready (Phases 3–5
  pending).
