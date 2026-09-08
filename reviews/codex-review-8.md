# Codex review round 8

## Disposition check (round-7 items)

I include every row carried in round 7, including its round-6 findings table.

### Primary carried rows

| Item | Status | Evidence |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | Fixed defaults, the memory guard, and ordered reservations are materially better ([plan L379](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:379), [plan L741](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:741)). Nullable-column storage, zstd accounting, and retry work remain defective; see findings 1–3. |
| R1-9 — Freezing and hashing | **RESOLVED** | Transitive freezing, freeze-time hashes, and fresh default materialisation remain explicit ([plan L240](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:240), [plan L278](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:278)). |
| R1-10 — Schema-driven compilation | **RESOLVED** | Generic plans remain schema-independent, and compile growth has a numerical gate ([plan L497](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:497), [plan L523](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:523)). |
| R1-18 — Writer lifecycle | **RESOLVED** | Option validation, atomic replacement, poisoning, cleanup, abort, and idempotence are specified ([plan L793](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:793)). |
| R1-20 — Parallel determinism | **RESOLVED for error selection** | Ordered commits and lowest-index failure selection across failure kinds are explicit ([plan L771](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:771)). |
| R1-25 — Union oracle | **RESOLVED** | Direct branch-index/result-type expectations and Java-policy fixtures remain ([plan L584](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:584), [plan L1183](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1183)). |
| R2-new-6 — Symbol admission | **RESOLVED** | All typed binary and JSON paths use admission objects ([plan L665](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:665), [plan L862](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:862), [plan L1068](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1068)). |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowed contract** | Schema-free encoding is explicitly conventional and not representation-preserving ([plan L651](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:651)). I continue to accept this amendment. |

### Other carried rows

| Item | Status | Evidence |
|---|---|---|
| R4-1 — Codec memory | **PARTIALLY RESOLVED** | Xz thresholds and zstd log clamping are present, but `max_codec_memory` alternates between a window cap and a complete decoder-requirement cap ([plan L393](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:393), [plan L697](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:697)). |
| R4-2 — Work/allocation budget | **PARTIALLY RESOLVED** | The compressed-size work precheck is gone, but final vector capacity and eviction work are not correctly accounted ([plan L445](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:445), [plan L744](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:744)). |
| R5-1 — Writer/Reader invariant | **PARTIALLY RESOLVED** | All dimensions are now listed, but the zstd construction does not satisfy the invariant for every valid custom limit ([plan L460](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:460), [plan L804](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:804)). |
| R5-2 — Parallel transient memory | **PARTIALLY RESOLVED** | Actual reservations and preallocation replace the prior deadlock, but nullable isbits-union arrays are undercharged. |
| R5-3 — Work amplification | **PARTIALLY RESOLVED** | The original density issue has a latency gate, but v8 introduces discarded and retried speculative work without a compatible accounting rule ([plan L470](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:470), [plan L758](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:758)). |
| R5-4 — Codec-cap contract | **PARTIALLY RESOLVED** | Earlier xz and zstd threshold errors are fixed, but the zstd 128 KiB deduction is not a valid general decoder-memory calculation. |
| R5-9 — Julia-derived names | **RESOLVED** | The complete name policy, hooks, and collision errors are explicit ([plan L627](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:627)). |
| R5-10 — Typed Symbol admission | **RESOLVED** | Binary and JSON typed paths are covered, including caller-owned admission objects. |

### Round-6 findings carried by round 7

| # | Status | Evidence |
|---:|---|---|
| 1 — Semaphore memory/liveness | **PARTIALLY RESOLVED** | Lowest-block liveness is specified, but final nullable-vector capacity is not exact ([plan L741](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:741)). |
| 2 — Writer/Reader invariant | **PARTIALLY RESOLVED** | Writer checks were added, but the zstd custom-cap case remains false. |
| 3 — Unsafe default memory | **RESOLVED** | Default total memory is 256 MiB, with smaller blocks and an available-memory guard ([plan L384](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:384), [plan L417](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:417)). |
| 4 — Compressed-size work precheck | **RESOLVED** | The plan explicitly forbids that precheck and evaluates work after bounded decompression ([plan L445](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:445)). |
| 5 — Whole-file `mmap=false` allocation | **RESOLVED** | `mmap=false` uses the sequential streaming reader ([plan L724](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:724)). |
| 6 — Self-alias canonical gate | **RESOLVED** | Self-aliases are idempotent and the complete Apache fixture remains gated ([plan L298](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:298)). |
| 7 — `fromjson` admission | **RESOLVED** | `names=` reaches typed JSON decoding ([plan L862](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:862)). |
| 8 — Julia-derived name policy | **RESOLVED** | All derived name categories, remedies, and collision rules are present. |
| 9 — Codec-cap cases | **PARTIALLY RESOLVED** | Decoder thresholds are improved, but the cap still has conflicting meanings and incorrect zstd arithmetic. |
| 10 — Unmeasured work ceilings | **PARTIALLY RESOLVED** | Value-density work is measured, but eviction retries are outside that measurement. |
| 11 — JSON depth asymmetry | **RESOLVED** | Both directions use `max_json_depth`, with boundary round trips ([plan L860](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:860)). |
| 12 — Duplicate named union branches | **RESOLVED** | Branch identity and definition-plus-reference duplication are explicit ([plan L298](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:298)). |
| 13 — Attribute wording | **RESOLVED** | Structural attributes and optional metadata are now distinguished contextually ([plan L263](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:263)). |

### Round-7 new findings 1–10 and follow-up

| # | Status | Evidence |
|---:|---|---|
| 1 — Assembly copy/reallocation | **PARTIALLY RESOLVED** | One-time preallocation removes vector growth, but the stated capacity formula is not exact for nullable isbits-union arrays ([plan L741](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:741)). |
| 2 — Writer codec workspace/frame requirement | **PARTIALLY RESOLVED** | Checks were added, but the zstd fit calculation is false for valid caps ([plan L804](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:804)). |
| 3 — Fixed worst-case permit/`ntasks` behavior | **RESOLVED for the reported fixed-`W` mechanism** | Actual reservations and priority eviction replace fixed worst-case permits ([plan L748](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:748)). Eviction work is a new defect below. |
| 4 — Fixed 1 GiB default | **RESOLVED** | Replaced by 256 MiB plus an available-memory guard. |
| 5 — Constructor/runtime `W` mismatch | **RESOLVED for the reported mechanism** | Fixed `W` was removed; checked constructor relations replace it ([plan L434](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:434)). |
| 6 — Empty-union `minsize` | **RESOLVED** | Empty union parses, has `minsize = ∞`, and fails datum operations cleanly ([plan L252](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:252)). |
| 7 — Peak-RSS method | **PARTIALLY RESOLVED** | The numerical threshold is fixed, but its streamed input selects the sequential path, so it cannot test parallel memory behavior. |
| 8 — Alias normalization | **RESOLVED** | Relative aliases normalize to fullnames while raw spelling is retained ([plan L248](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:248)). |
| 9 — Streamed compressed-input ownership | **RESOLVED** | Compressed and decompressed buffers are both reserved ([plan L724](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:724), [plan L750](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:750)). |
| 10 — `Nothing` schema | **RESOLVED** | `Nothing → null`; duplicate null mappings are rejected ([plan L620](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:620)). |
| Follow-up — Exact sanitization | **RESOLVED** | The algorithm, truncation, SHA suffix, hooks, and collision behavior are specified and gated ([plan L627](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:627)). |

## New findings

1. **[major] Nullable columns invalidate the exact memory ceiling.**

   **Claim:** The preallocation formula undercharges Julia isbits-union arrays.

   **Evidence:** Generic nullable schemas produce `Union{Missing,T}` columns ([plan L531](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:531)). The parallel plan charges either `rows × sizeof(e)` or `rows × 8` ([plan L744](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:744)). Julia stores an additional one-byte type tag for each isbits-union element. A read-only Julia 1.12 probe reported:

   ```text
   Vector{Union{Missing,Float64}}, 1,000,000 rows:
   Base.elsize = 8
   Base.summarysize = 9,000,040

   Vector{Union{Missing,UUID}}, 1,000,000 rows:
   sizeof(element payload) = 16
   Base.summarysize = 17,000,040
   ```

   Either stated branch undercharges `Union{Missing,UUID}`. This defeats the physical ceiling and peak claims at [plan L748](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:748) and [plan L949](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:949).

   **Recommendation:** Charge actual array storage: element payload, the isbits-union tag byte, vector header, checked rounding, and capacity. Add near-ceiling nullable `Int64`, `Float64`, `Date`, and `UUID` tests.

2. **[major] The zstd decoder-requirement guarantee is false for valid custom limits.**

   **Claim:** Subtracting 128 KiB from `max_codec_memory` does not prove that `ZSTD_estimateDStreamSize(window)` fits.

   **Evidence:** The writer uses that subtraction at [plan L804](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:804). CodecZstd exposes `windowLog` as a power-of-two parameter ([CodecZstd compression source L70](/Users/jacob.quinn/.julia/packages/CodecZstd/dNeRy/src/compression.jl:70)). A read-only probe generated a valid level-20/window-log-25 frame and measured:

   ```text
   max_codec_memory          = 33,816,576
   frame window              = 33,554,432
   ZSTD_estimateDStreamSize  = 34,043,696
   excess over cap           = 227,120
   ```

   The frame decoded successfully with `windowLogMax=25`. A 1 GiB total ceiling admits the corresponding compressor workspace, so this is a valid configuration. The plan also calls the field a decoder-requirement cap at [plan L393](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:393), but a window-only cap plus separately reserved context at [plan L697](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:697).

   **Recommendation:** Give the field one meaning. If it caps the complete decoder requirement, select the largest supported `windowLog` for which the actual estimate fits, and verify each emitted frame with `ZSTD_estimateDStreamSize_fromFrame`. Test just below, at, and above every power-of-two transition. Update decision 6 to match decision 28.

3. **[major] Eviction cannot preserve both acceptance and the work guarantee as written.**

   **Claim:** Retried speculative decoding is either uncharged CPU work or changes parallel acceptance.

   **Evidence:** The work rule counts every value encountered and has one operation-wide allowance ([plan L445](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:445)). The coordinator abandons partial work and retries an evicted block after a later commit ([plan L758](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:758)).

   If discarded attempts consume the work budget, parallel decoding can reject a file that sequential decoding accepts. If their counters are rolled back, repeated decompression and value traversal are outside the input-proportional work bound. `ntasks` is only capped by block count, so no fixed retry factor is stated.

   **Recommendation:** After one eviction, make that block serial-only and retry it only when it becomes the lowest block, or preserve its partial result. Otherwise add a bounded retry-work mechanism that falls back to sequential execution without changing acceptance. Gate maximum attempts and CPU time under forced eviction schedules.

4. **[major] The parallel peak-RSS gate executes the sequential path.**

   **Claim:** The fixed gate cannot verify parallel reservations, eviction, or liveness.

   **Evidence:** Parallel `Avro.Table` is limited to mmap and byte sources; streamed/`IO` sources decode sequentially ([plan L741](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:741)). The RSS protocol then requires streamed input with no mapping and eight workers ([plan L786](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:786)). Phase 4c repeats this gate ([plan L1455](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1455)).

   **Recommendation:** Load and fault a caller-owned byte buffer or mapping before the warmed baseline, retain it, then run `ntasks=8`. Assert actual overlap of multiple worker blocks. Use the normative 16 MiB blocks; §9’s 64 MiB wording is stale and exceeds the default block cap.

5. **[major] The fastavro readability guarantee excludes a valid OCF case but does not say so.**

   **Claim:** Avro permits arbitrary metadata bytes, while fastavro rejects non-UTF-8 values. The plan promises fastavro readability for every file written from an oracle-accepted schema.

   **Evidence:** The specification says files may contain arbitrary user metadata and defines metadata as `map<string,bytes>` ([spec L450](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:450), [spec L463](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:463)). The Writer correctly accepts `Dict{String,Vector{UInt8}}` metadata ([plan L793](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:793)). However, fastavro decodes every metadata value as UTF-8 ([fastavro source L967](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/venv/lib/python3.14/site-packages/fastavro/_read_py.py:967)). A valid header with `x => [0xff]` produced:

   ```text
   UnicodeDecodeError: 'utf-8' codec can't decode byte 0xff
   ```

   This contradicts the unconditional promise at [plan L1150](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1150).

   **Recommendation:** Keep byte-valued metadata for spec compliance. Record non-UTF-8 metadata as a fastavro capability exception, narrow the oracle-readability promise, and add a fixture where Julia and Java accept while fastavro is expected to reject.

6. **[major] Filtered Scan cannot use the stated exact preallocation algorithm.**

   **Claim:** Header counts cannot reveal the final row count of a row-dependent filter.

   **Evidence:** The parallel path pre-scans headers without decompression and then allocates exact final-column capacity ([plan L741](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:741)). Scan instead discovers qualifying rows during its per-block filter pass ([plan L1101](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1101)). Header counts describe source rows, not filtered rows.

   **Recommendation:** Specify a global filter/count pass before allocating result columns, followed by selected-column decoding; alternatively define a separately charged chunked Scan path or exclude filtered Scan from parallel decoding. Gate low-selectivity filters, offset/limit, near-ceiling files, and `ntasks=1/2/8`.

7. **[minor] `fixed(0)` makes the decimal precision formula undefined.**

   **Claim:** The plan permits `fixed.size == 0` but does not define logical validation for decimal on that size.

   **Evidence:** Fixed size permits zero at [plan L263](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:263). Decimal validation applies `floor(log10(2^(8n−1)−1))` ([plan L273](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:273)); at `n=0` this is not a usable precision bound. The specification requires invalid logical types to be ignored in favor of the underlying schema ([spec L787](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:787)).

   **Recommendation:** State that no positive decimal precision is valid for `fixed(0)`, drop the logical annotation, retain raw properties, and test that parsing never leaks `DomainError`.

8. **[minor] Codec memory estimates rely on unlisted, non-public APIs.**

   **Claim:** The dependency plan does not expose a supported route to the estimator functions that the resource contract requires.

   **Evidence:** Production CodecZstd 0.8.7 does not wrap the `ZSTD_estimate*` static APIs, and CodecXz does not wrap the liblzma memory estimators. They are currently reachable only through internal library handles and direct `ccall`, while the dependency list names only CodecZstd/CodecXz ([plan L1403](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1403)).

   **Recommendation:** Add direct JLL dependencies/weak dependencies with an explicit ABI contract, or upstream supported wrappers and pin their first available versions. Add a startup capability check that fails clearly if the required symbols are absent.

## Non-blocking follow-ups (if any)

The following can be settled during implementation:

- Change the stale bzip2 “8 MiB constructor floor” wording at [plan L706](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:706) to 16 MiB.
- Change the stale 64 MiB peak-RSS workload at [plan L1335](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1335) to the normative 16 MiB.
- Define cooperative codec cancellation before saying eviction has physically released a worker’s memory.
- State an effective `max_inflight_blocks`/`Threads.nthreads()` task cap.
- Distinguish preallocated vector storage from committed referenced payload to prevent accidental double charging.
- Pin the Python minor version used by the interop job.
- Keep the accepted deferrals: RPC, `big-decimal`, inference, append, borrowed views, and parallel compression remain reasonable 2.x work.

## Milestone and gate assessment

The schema grammar, names, defaults, unions, binary and JSON encodings, single-object encoding, sort order, OCF framing/codecs, resolution, canonical form, fingerprints, logical types, audit table, and generated interop corpus are otherwise strong. The zero-export API, recorded compatibility breaks, and RPC/IDL deferrals remain appropriate.

The gates are not executable as a complete production-safety argument yet:

- Phase 0 must establish supported codec estimator dependencies and one exact codec-cap meaning.
- Phase 2 must include speculative retry work or prevent repeated retries.
- Phase 4a must repair zstd identical-limit checks and add arbitrary-byte metadata interoperability expectations.
- Phase 4b/4c must use real Julia column storage sizes.
- Phase 4c must run its RSS gate through an actual parallel-capable source.
- Phase 4d must define filtered-result sizing before exact preallocation.
- PR-ready, merge-ready, RC-ready, and release-ready remain correctly distinct. The exact RC-archive rerun remains the correct release gate.

Assumptions: the pinned specification and tools remain authoritative; schemas, payloads, metadata, and codec frames are untrusted; declared artifacts are future deliverables; default resource failures must occur as Avro errors before process OOM.

Decisions without user direction: I marked replaced mechanisms resolved when their exact old defect is gone, but kept broad resource rows partial where their promised guarantee still fails. I classified filtered Scan as major because its conditional public API cannot satisfy both recorded contracts if it ships. I accepted all existing feature deferrals and the schema-free identity narrowing.

Validation: I inspected DRAFT v8, `response-7.md`, the pinned specification, the codec package sources, the API, tests, benchmarks, milestones, and audit source state. I ran read-only Julia 1.12 isbits-union probes, zstd frame/estimator probes, and a fastavro non-UTF-8 metadata probe. I created or modified no files.

DRAFT v8 is materially closer, chief, but six major guarantees still fail.

## Verdict

VERDICT: REVISE
