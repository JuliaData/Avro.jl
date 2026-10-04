# Codex review round 9

## Disposition check (round-8 items)

### Primary carried rows

| Item | Status | Evidence |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | Fixed defaults, nullable-column storage, and bounded retries are present ([v9:395](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:395), [v9:758](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:758), [v9:786](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:786)). The XZ and generic-value reservations remain unsafe; see findings 1–2. |
| R1-9 — Freezing and hashing | **RESOLVED** | Transitive freezing, stored hashes, and fresh default materialisation remain explicit ([v9:240](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:240), [v9:280](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:280)). |
| R1-10 — Schema-driven compilation | **RESOLVED** | Generic plans remain schema-independent, with a numerical compilation gate ([v9:500](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:500)). |
| R1-18 — Writer lifecycle | **RESOLVED** | Validation, atomic replacement, poisoning, cleanup, abort, and idempotent close are specified ([v9:827](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:827)). |
| R1-20 — Parallel error determinism | **RESOLVED for error selection** | Ordered cumulative failures and lowest-index error selection remain explicit ([v9:803](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:803)). |
| R1-25 — Union oracle | **RESOLVED** | Direct branch-index and output-representation expectations remain specified ([v9:574](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:574)). |
| R2-new-6 — Symbol admission | **RESOLVED** | All typed binary and JSON paths use admission objects ([v9:503](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:503), [v9:899](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:899)). |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowed contract** | Schema-free encoding remains explicitly conventional, not representation-preserving ([v9:656](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:656)). I continue to accept this amendment. |

### Other carried rows

| Item | Status | Evidence |
|---|---|---|
| R4-1 — Codec memory | **PARTIALLY RESOLVED** | Zstandard now uses a complete reported requirement, but the XZ reservation is neither exact nor complete ([v9:702](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:702)). |
| R4-2 — Work/allocation budget | **PARTIALLY RESOLVED** | Nullable shell sizing and retry bounds improved. Generic bytes/fixed values remain undercharged ([v9:483](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:483)). |
| R5-1 — Writer/Reader invariant | **RESOLVED for Writer-emitted frames** | Writer codec workspace and emitted-frame checks are explicit ([v9:839](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:839)). |
| R5-2 — Parallel transient memory | **PARTIALLY RESOLVED** | Exact column shells are charged, but XZ and generic payload reservations can still be too small. |
| R5-3 — Work amplification | **PARTIALLY RESOLVED** | Base `Table` retries are bounded. Filtered parallel Scan can still attempt one block four times; see finding 4. |
| R5-4 — Codec-cap contract | **RESOLVED for the prior zstd arithmetic defect** | The writer selects a zstd window by the estimator and verifies every emitted frame ([v9:843](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:843)). |
| R5-9 — Julia-derived names | **RESOLVED** | The complete name policy, hooks, and collision handling remain ([v9:632](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:632)). |
| R5-10 — Typed Symbol admission | **RESOLVED** | Binary and JSON typed-value paths are covered. |

### Round-6 carried findings

| # | Status | Evidence |
|---:|---|---|
| 1 — Semaphore memory/liveness | **PARTIALLY RESOLVED** | The ordered reservation algorithm is defined, but findings 1–2 still invalidate exact physical accounting. |
| 2 — Writer/Reader invariant | **RESOLVED** | Reader limits and Writer frame checks are paired at [v9:465](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:465) and [v9:839](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:839). |
| 3 — Unsafe default memory | **RESOLVED for the reported 1 GiB default** | The default is now 256 MiB with an available-memory guard ([v9:395](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:395), [v9:419](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:419)). |
| 4 — Compressed-size work precheck | **RESOLVED** | It is expressly forbidden ([v9:450](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:450)). |
| 5 — Whole-file `mmap=false` allocation | **RESOLVED** | The path streams sequentially ([v9:735](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:735)). |
| 6 — Self-alias canonical gate | **RESOLVED** | Self-aliases are idempotent ([v9:298](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:298)). |
| 7 — `fromjson` admission | **RESOLVED** | `names=` reaches typed JSON decoding ([v9:899](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:899)). |
| 8 — Julia-derived name policy | **RESOLVED** | The full policy remains at [v9:632](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:632). |
| 9 — Codec-cap cases | **PARTIALLY RESOLVED** | Zstandard is fixed; XZ estimation still needs per-filter-chain library estimates. |
| 10 — Unmeasured work ceilings | **PARTIALLY RESOLVED** | Base retries are bounded, but the Scan and exact timing gates conflict; see findings 4–5. |
| 11 — JSON depth asymmetry | **RESOLVED** | Both directions use `max_json_depth`. |
| 12 — Duplicate named union branches | **RESOLVED** | Definition plus reference counts as a duplicate ([v9:298](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:298)). |
| 13 — Attribute wording | **RESOLVED** | Structural and logical/custom attributes are separated contextually ([v9:263](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:263)). |

### Round-7 findings 1–10 and follow-up

| Item | Status | Evidence |
|---|---|---|
| 1 — Assembly copy/reallocation | **RESOLVED for column shells** | One-time exact shell preallocation and allocation-free assembly are specified ([v9:758](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:758), [v9:803](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:803)). |
| 2 — Writer codec workspace/frame requirement | **RESOLVED** | Construction and emitted-frame checks are explicit ([v9:839](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:839)). |
| 3 — Fixed worst-case permit mechanism | **RESOLVED** | Actual reservations and priority eviction replaced fixed `W` ([v9:768](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:768)). |
| 4 — Fixed 1 GiB default | **RESOLVED** | It is now 256 MiB. |
| 5 — Constructor/runtime `W` mismatch | **RESOLVED** | Fixed `W` is gone. |
| 6 — Empty-union `minsize` | **RESOLVED** | Empty unions have `minsize = ∞` and clean datum errors ([v9:252](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:252)). |
| 7 — Peak-RSS path | **RESOLVED for source selection** | The gate now uses a faulted caller-owned byte source and asserts concurrent blocks ([v9:818](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:818)). |
| 8 — Alias normalization | **RESOLVED** | Relative aliases normalize to fullnames ([v9:248](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:248)). |
| 9 — Streamed compressed-input ownership | **RESOLVED** | Both compressed and decompressed buffers are reserved ([v9:735](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:735), [v9:770](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:770)). |
| 10 — `Nothing` mapping | **RESOLVED** | `Nothing → null` is specified. |
| Sanitisation follow-up | **RESOLVED** | The exact algorithm and hooks remain specified ([v9:632](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:632)). |

### Round-8 new findings 1–8

| # | Status | Evidence |
|---:|---|---|
| 1 — Nullable-column storage | **RESOLVED** | Payload size, isbits-union tags, array header, checked arithmetic, and near-ceiling tests are explicit ([v9:758](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:758), [v9:1397](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1397)). |
| 2 — Zstd decoder requirement | **RESOLVED for each individual frame** | Reads use `fromFrame`; writes select by estimate and verify emitted frames ([v9:711](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:711), [v9:843](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:843)). |
| 3 — Eviction versus work guarantee | **PARTIALLY RESOLVED** | Base blocks are limited to two attempts ([v9:786](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:786)). Filtered Scan and the CPU gate remain inconsistent. |
| 4 — Peak-RSS gate used a sequential source | **RESOLVED** | The byte-buffer path, eight tasks, and high-water assertion are explicit ([v9:818](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:818)). |
| 5 — Fastavro non-UTF-8 metadata | **PARTIALLY RESOLVED** | The exception and fixture exist ([v9:1201](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1201), [v9:1254](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1254)), but §13 retains the old unconditional promise ([v9:1545](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1545)). |
| 6 — Filtered Scan preallocation | **PARTIALLY RESOLVED** | The global count pass fixes result sizing ([v9:1143](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1143)). Its interaction with retries is unresolved. |
| 7 — `fixed(0)` decimal | **RESOLVED** | The annotation is dropped without evaluating the formula ([v9:273](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:273)). |
| 8 — Estimator APIs | **PARTIALLY RESOLVED** | Direct JLL access and symbol checks are present, but only the easy liblzma estimators are listed; the reader needs `lzma_raw_decoder_memusage` for arbitrary filter chains ([v9:1464](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1464)). |

### Round-8 follow-ups

| Follow-up | Status | Evidence |
|---|---|---|
| Change stale bzip2 floor to 16 MiB | **RESOLVED** | [v9:716](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:716) |
| Use 16 MiB in the RSS workload | **RESOLVED** | [v9:823](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:823), [v9:1393](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1393) |
| Cooperative, acknowledged cancellation | **RESOLVED** | [v9:786](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:786) |
| Safe task/in-flight cap | **PARTIALLY RESOLVED** | In-flight blocks are bounded, but actual tasks remain `min(ntasks,nblocks)` ([v9:752](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:752)). |
| Separate column storage from referenced payload | **PARTIALLY RESOLVED** | The local rule says “never double-charged,” but stage 2 still invokes the general per-value estimate for all values ([v9:758](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:758), [v9:775](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:775)). |
| Pin Python minor | **RESOLVED** | CPython 3.14 is pinned ([v9:1290](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1290)). |
| Preserve accepted deferrals | **RESOLVED** | RPC, `big-decimal`, inference, append, parallel compression, and borrowed views remain explicitly deferred ([v9:1595](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1595)). |

## New findings

1. **[major] The XZ reservation is not the complete library-reported requirement.**

   **Claim:** The in-house XZ formula under-reserves even simple presets and cannot cover every Block in a valid XZ Stream.

   **Evidence:** The plan computes dictionary size plus 64 KiB, with another 64 KiB per BCJ/delta filter, and claims equality with liblzma ([v9:707](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:707)). Read-only calls to the pinned liblzma returned:

   ```text
   preset 0: actual 328,312; formula 327,680
   preset 6: actual 8,454,776; formula 8,454,144
   preset 9: actual 67,175,032; formula 67,174,400
   ```

   The formula is low by 632 bytes for each case. More importantly, liblzma warns that different Blocks in one XZ Stream can have different memory requirements ([base.h:681](/Users/jacob.quinn/.julia/artifacts/8497848586ec3c8e66c36a5b01fea1e48dd2f62f/include/lzma/base.h:681)). Its supported estimator for an arbitrary decoded filter chain is `lzma_raw_decoder_memusage` ([filter.h:180](/Users/jacob.quinn/.julia/artifacts/8497848586ec3c8e66c36a5b01fea1e48dd2f62f/include/lzma/filter.h:180)). The dependency contract lists only the easy estimators ([v9:1472](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1472)).

   The liblzma `memlimit` still protects `max_codec_memory`. It does not protect the shared `max_total_bytes` ceiling from an understated reservation.

   **Recommendation:** Either reserve the full configured `max_codec_memory` for every XZ decoder, or parse every XZ Stream and Block header and call `lzma_raw_decoder_memusage` for each filter chain before allocation. Add the symbol check and mixed-filter/multi-Block fixtures.

2. **[major] The generic output estimator undercharges bytes, fixed, and wide decimals.**

   **Claim:** `length + 32` is not an actual-size reservation for the promised Julia representations.

   **Evidence:** The estimator uses `length + 32` for strings, bytes, and fixed ([v9:483](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:483)). Generic bytes are `Vector{UInt8}` and fixed wraps such a vector plus schema identity ([v9:540](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:540), [v9:553](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:553)). A read-only Julia 1.12 probe reported:

   ```text
   Base.summarysize(Vector{UInt8}(undef, n)) = n + 40
   ```

   This held for `n = 0, 1, 10, 1_000_000`. `Avro.Fixed` needs more storage than the vector alone. `Avro.WideDecimal` contains a variable-size `BigInt`, but the estimator gives no limb rule ([v9:561](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:561)). Millions of small byte values can therefore exceed the physical ceiling before the budget reaches its limit.

   The column rule also says storage is never double-charged, while stage 2 charges the general estimator for every produced value ([v9:764](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:764), [v9:775](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:775)). For isbits columns, this can instead charge payload already included in column capacity.

   **Recommendation:** Define tested, representation-specific storage formulas. Include vector capacity/header, `Avro.Fixed`, record/union wrappers, BigInt limbs, nested containers, and transfer of reference payload from in-flight to committed ownership. Add near-ceiling empty-bytes, fixed, wide-decimal, and nullable-isbits tests.

3. **[major] Valid concatenated codec members are rejected as suffixes.**

   **Claim:** A second valid codec stream or frame is treated as invalid trailing data.

   **Evidence:** The plan rejects “a valid stream followed by extra bytes” for every codec ([v9:719](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:719)). The Avro specification only requires that XZ and Zstandard use their named compression libraries; it does not add a one-member restriction ([spec:522](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:522)). Zstandard’s API explicitly accepts an exact sequence of multiple frames and concatenates their outputs ([zstd.h:164](/Users/jacob.quinn/.julia/artifacts/8da603395acfbdbef8c5de3b7223aeb9276ecbdb/include/zstd.h:164)). Avro-python explicitly restarts its bzip2 decoder for concatenated streams ([codecs.py:251](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/venv/lib/python3.14/site-packages/avro/codecs.py:251)).

   A read-only in-memory probe created one OCF Zstandard block from two valid frames. Fastavro 1.12.2 decoded it as:

   ```text
   22 ['a', 'b']
   ```

   Java acceptance was not verified because the read-only environment prevented zstd-jni extraction.

   **Recommendation:** Consume valid codec members until exact payload exhaustion. Apply memory checks per member and output/work limits cumulatively. Reject only malformed or truncated suffixes. Add concatenated Zstandard, XZ, and bzip2 fixtures against each capable oracle.

4. **[major] Filtered parallel Scan breaks the two-attempt work contract.**

   **Claim:** A filtered block can be decompressed four times, not at most twice.

   **Evidence:** Base parallel decoding permits one speculative attempt plus one serial-only retry ([v9:790](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:790)). Filtered Scan separately performs a global filter pass and a selection pass, and says each block is decompressed at most twice ([v9:1143](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1143)). Both passes use the parallel reservation rules. Either pass can therefore be evicted and retried: two filter attempts plus two selection attempts.

   This contradicts decision 22 and the Phase 4c gate ([v9:1624](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1624), [v9:1522](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1522)).

   **Recommendation:** Define one Scan-specific attempt state. For example, make the global filter pass non-speculative, or retain its results so it is never repeated. Otherwise state and gate the larger four-attempt bound and its corresponding CPU limit. Add forced-eviction tests with row-dependent filters.

5. **[major] The exact `CPU ≤ 2 × sequential` timing gate is not executable.**

   **Claim:** The plan acknowledges scheduling overhead but gives the test no allowance for it.

   **Evidence:** The algorithm says CPU is at most twice sequential “plus scheduling overhead” ([v9:793](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:793)). The test and Phase 4c require literal `CPU time ≤ 2 × sequential` ([v9:1396](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1396), [v9:1522](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1522)). A discarded attempt can finish almost all decode work. Its retry then approaches 2× before task, atomic, coordinator, and copy costs are added.

   **Recommendation:** Gate deterministic work units and attempt counts at `≤ 2×`. Give measured CPU a predeclared tolerance on a named host, or make CPU timing informational.

6. **[minor] The summary still overstates fastavro readability.**

   **Claim:** §7 records the non-UTF-8 metadata exception, but §13 again promises that fastavro reads every file written from oracle-accepted schemas.

   **Evidence:** Compare [v9:1201](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1201) with [v9:1545](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1545).

   **Recommendation:** Add “subject to the §7 recorded exceptions” to §13. The detailed implementation contract is already clear, so this is non-blocking.

7. **[minor] Worker task objects are not bounded by the effective in-flight limit.**

   **Claim:** The plan can create one parked Julia `Task` per block even when only a few blocks may be in flight.

   **Evidence:** Actual tasks are `min(ntasks,nblocks)`; only in-flight blocks are reduced by `max_inflight_blocks` and the ceiling ([v9:752](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:752)). `ntasks` is validated only as non-negative. Task objects and stacks are not charged.

   **Recommendation:** Use a fixed worker pool capped by `min(ntasks, Threads.nthreads(), effective_inflight, nblocks)` or a documented bounded oversubscription factor. Charge coordinator and task state.

8. **[minor] The peak-RSS measurement primitive remains unspecified.**

   **Claim:** The source and threshold are fixed, but the definition of `peak_rss` is not.

   **Evidence:** The method specifies the baseline, byte-buffer source, tasks, and threshold, but not the OS counter or sampler ([v9:818](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:818)). A `Sys.maxrss` baseline is a lifetime high-water mark. A large warm-up peak can hide later operation growth.

   **Recommendation:** Specify current-RSS sampling in a child process or an external OS peak measurement. Define sample frequency, baseline subtraction, and treatment of caller-owned input.

## Non-blocking follow-ups (if any)

The minor findings above can be handled during implementation:

- Qualify the §13 fastavro summary.
- Cap the worker pool and charge its state.
- Make column-capacity and payload ownership categories disjoint.
- Fix the peak-RSS sampling method.

The existing deferrals remain appropriate: RPC, `big-decimal`, inference, append, borrowed views, and parallel compression. They do not need to enter 2.0.

## Milestone and gate assessment

The schema grammar, names, aliases, defaults, unions, binary and JSON datum encoding, single-object encoding, sort order, resolution, canonical form, fingerprints, logical types, strict OCF framing, and the audit table are otherwise strong. I found no new §2.2 audit inaccuracy. The Apache and generated corpora are broad. RPC remains explicitly and credibly deferred.

The remaining changes affect executable gates:

- Phase 0 must add `lzma_raw_decoder_memusage` or reserve the full XZ cap.
- Phase 2 must replace the generic output estimate with representation-specific accounting.
- Phase 4a must accept concatenated valid codec members and cap every member.
- Phase 4c must use a measurable retry-work gate with timing tolerance.
- Phase 4d must define filtered-Scan retry accounting.

PR-ready, merge-ready, RC-ready, and release-ready remain correctly distinct. The exact RC-archive rerun remains the right final release gate. DRAFT v9 is not implementation-ready because five major contracts still fail.

Assumptions: the pinned specification and tools are authoritative. Schemas, metadata, payloads, and codec streams are untrusted. Declared fixtures remain future implementation deliverables.

Decisions without user direction: I marked replaced mechanisms resolved when their original defect was removed. I treated stale summary wording and pathological caller task counts as minor because the detailed contracts are implementable and the defaults are bounded.

Validation: I inspected DRAFT v9, `response-8.md`, the pinned specification, local codec headers, dependencies, gates, milestones, and prior dispositions. I ran read-only Julia 1.12 allocation and liblzma probes and the in-memory fastavro concatenated-frame probe. I created or modified no files, big dawg.

## Verdict

VERDICT: REVISE
