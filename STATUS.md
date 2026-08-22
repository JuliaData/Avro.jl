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
| 4a–4d — Containers, Tables, parallel, projection | not started | |
| 5 — Release engineering | not started | |

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
