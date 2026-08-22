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
| 1 — Schema model | not started | |
| 2 — Binary core | not started | |
| 3 — Resolution and order | not started | |
| 4a–4d — Containers, Tables, parallel, projection | not started | |
| 5 — Release engineering | not started | |

## Commands run and results

See the git log for per-commit test results; the latest local validation is recorded below.

* Julia 1.12.6 / 1.10.11: `Pkg.test()` — 264 tests pass (Phase 0 skeleton + names + JSON reader, 2026-08-22).

## Remaining gaps and assumptions

* The symbol-admission table merges runs eagerly at carry time; the deamortised fixed-step merge of
  plan §4.4 is scheduled with its Phase 4b gate.
* The available-memory guard is best-effort (plan §4.4); cgroup files are read on Linux only.
* Python oracle pin: CPython 3.14 (3.14.2 in the authoring venv), `fastavro==1.12.2`, `avro==1.12.2`,
  `cramjam==2.11.0`; Java oracle: avro-tools 1.12.2 (sha256
  `6220e8bc089aaf917cdad4cd358bd651fc0394c0e5ddb8b36da402012c294a68`), run on OpenJDK 25 locally and
  Temurin 21 in CI.
* Readiness: not yet review-ready (Phase 0 in progress).
