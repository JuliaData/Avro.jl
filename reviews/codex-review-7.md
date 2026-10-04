# Codex review round 7

## Disposition check (round-6 items)

### Primary carried rows

| Item | Status | Evidence |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | The work rule and ceiling are much clearer at [plan L364](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:364). Parallel assembly, codec symmetry, and the 1 GiB default remain defective. See findings 1–5. |
| R1-9 — Freezing and hashing | **RESOLVED** | Recursive freezing, freeze-time hashes, fresh defaults, and semantic equality remain explicit at [plan L236](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:236). |
| R1-10 — Schema-driven compilation | **RESOLVED** | Generic and column plans stay schema-independent, with a numerical compile-cost gate at [plan L471](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:471). |
| R1-18 — Writer lifecycle | **RESOLVED** | Writer option validation, atomic replacement, poisoning, cleanup, and failure behavior remain specified at [plan L743](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:743). |
| R1-20 — Parallel determinism | **RESOLVED for error selection** | Ordered commits and lowest-index failure selection are explicit at [plan L734](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:734). Memory accounting is a separate unresolved issue. |
| R1-25 — Union oracle | **RESOLVED** | Direct branch-index and result-type expectations remain at [plan L558](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:558) and [plan L1122](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1122). |
| R2-new-6 — Symbol admission | **RESOLVED** | Every typed path, including JSON, now accepts an admission object at [plan L477](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:477), [plan L632](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:632), and [plan L800](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:800). |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowed contract** | Schema-free encoding is explicitly conventional rather than representation-preserving at [plan L618](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:618). I continue to accept this amendment. |

### Other carry-forward rows that were still open in round 6

| Item | Status | Evidence |
|---|---|---|
| R4-1 — Codec memory | **RESOLVED for the reported decoder-threshold defects** | Liblzma-reported memory and clamped zstd bounds are now correct at [plan L664](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:664). The new writer-side contradiction is finding 2. |
| R4-2 — Work/allocation budget | **PARTIALLY RESOLVED** | The compressed-size precheck is gone at [plan L419](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:419). Parallel allocation accounting remains incomplete. |
| R5-1 — Writer/default Reader invariant | **PARTIALLY RESOLVED** | Schema, metadata, payload, and output limits are listed at [plan L434](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:434). Codec output and parallel materialization still violate the broad promise. |
| R5-2 — Parallel transient memory | **PARTIALLY RESOLVED** | Ordered permits replace the old deadlocking design at [plan L708](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:708), but final assembly is not fully reserved. |
| R5-3 — Work amplification | **RESOLVED as an implementation gate** | Phase 2 must measure and fix the provisional constants against a 10-second ceiling at [plan L444](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:444). |
| R5-4 — Codec-cap contract | **RESOLVED for the reported xz/zstd threshold cases** | The corrected thresholds and fixtures are at [plan L664](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:664) and [plan L1131](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1131). |
| R5-9 — Julia-derived names | **RESOLVED** | Every derived name now validates or fails with actionable hooks at [plan L599](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:599). |
| R5-10 — Typed Symbol admission | **RESOLVED** | `fromjson(...; names=...)` and repeated-input tests are explicit at [plan L800](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:800) and [plan L1007](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1007). |

All other R4/R5 rows already marked resolved in round 6 remain resolved.

### Round-6 new findings 1–13

| # | Status | Evidence |
|---:|---|---|
| 1 — Semaphore memory/liveness | **PARTIALLY RESOLVED** | Full reservations and ordered progress exist at [plan L708](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:708). Assembly still duplicates output outside the reservation. |
| 2 — Writer/Reader invariant | **PARTIALLY RESOLVED** | Cumulative fields are listed at [plan L434](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:434), but `max_codec_memory` remains decode-only at [plan L664](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:664). |
| 3 — Unsafe default memory | **PARTIALLY RESOLVED** | Fixed defaults remove host-to-host drift, but a fixed 1 GiB ceiling with no available-memory check remains unsafe in constrained processes at [plan L402](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:402). |
| 4 — Compressed-size work precheck | **RESOLVED** | Only hard bounds run before decompression; exact work uses decompressed bytes at [plan L425](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:425). |
| 5 — Whole-file `mmap=false` allocation | **RESOLVED** | Paths now use the sequential streaming reader at [plan L691](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:691). |
| 6 — Self-alias canonical gate | **RESOLVED** | Self-aliases are idempotent at [plan L285](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:285), preserving the 100% Apache gate. |
| 7 — `fromjson` admission | **RESOLVED** | [Plan L800](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:800) and [plan L963](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:963). |
| 8 — Julia-derived name policy | **RESOLVED** | [Plan L599](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:599). |
| 9 — Codec-cap cases | **RESOLVED for decoder thresholds** | [Plan L664](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:664). Writer-side enforcement remains a separate new finding. |
| 10 — Unmeasured work ceilings | **RESOLVED as an implementation gate** | [Plan L444](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:444), [plan L1324](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1324). |
| 11 — JSON depth asymmetry | **RESOLVED** | Both directions use `max_json_depth` at [plan L369](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:369) and [plan L800](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:800). |
| 12 — Duplicate named union branches | **RESOLVED** | Branch identity is explicit at [plan L159](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:159) and [plan L287](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:287). |
| 13 — Attribute wording | **RESOLVED** | Structural attributes and optional metadata are distinguished at [plan L250](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:250). |

The five round-6 non-blocking follow-ups are also resolved: shared allowance accounting, failure-kind precedence, the orderable-schema comparison property, decompressed `eachblock` ownership, and explicit decode-only codec wording.

## New findings

1. **[blocker] Parallel assembly still exceeds the operation memory ceiling.**

   **Claim:** `W` reserves one decoded-output buffer. Ordered assembly then creates the final-column copy before freeing that buffer. Both copies coexist outside the stated accounting.

   **Evidence:** `W` includes one `max_block_output_bytes` term at [plan L715](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:715). The coordinator then appends retained chunks to final vectors and only afterward frees the chunks at [plan L723](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:723). Therefore the physical peak is at least:

   ```text
   old final columns + W + newly appended destination
   ```

   A Julia `Vector` also has no guarantee that growth happens in place. Reallocation may temporarily retain the old vector, the enlarged vector, and the source chunk. This contradicts the ceiling guarantee at [plan L889](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:889).

   **Recommendation:** Decode directly into fully reserved destination storage, retain chunked columns without copying, or reserve the complete source-plus-destination copy peak before assembly. Charge vector capacity, not logical length. Add a near-ceiling forced-reallocation test.

2. **[major] Codec writing breaks both the identical-limits invariant and the operation-memory ceiling.**

   **Claim:** A successful Writer can emit an xz or zstd frame that an identical-limits Reader rejects. Compressor workspace can also exceed `max_total_bytes`.

   **Evidence:** The plan promises identical-limit readability at [plan L434](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:434), but declares `max_codec_memory` decode-only at [plan L664](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:664) and does not validate the emitted frame’s decoder requirement at [plan L743](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:743).

   Read-only liblzma measurements in the pinned API environment produced:

   ```text
   preset 6 decoder memory:   8,454,776 bytes
   preset 9 encoder memory: 705,794,571 bytes
   ```

   The constructor permits `max_codec_memory = 8 MiB` at [plan L411](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:411). A default `XzCompressor` stream failed to decode at both 8,388,608 and 8,454,144 byte limits with `lzma error: code = 6`; it succeeded at 9,437,184. A valid custom ceiling of about 448 MiB can also permit xz level 9 even though its compressor alone needs about 706 MB.

   **Recommendation:** Before writing, determine both the compressor workspace and the emitted frame’s decoder requirement. Reject or lower a level that exceeds `max_total_bytes` or `max_codec_memory`. Use liblzma’s memusage functions and equivalent codec-specific checks. Gate every codec at minimum, default, high, and raised limits.

3. **[major] The fixed worst-case permit makes parallel behavior hardware- and `ntasks`-dependent and conflicts with the performance gate.**

   **Claim:** Default limits permit at most two decode workers. They eventually prevent any further parallel block even when the exact final table would fit.

   **Evidence:** From [plan L715](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:715):

   ```text
   W = 64 MiB + 128 MiB + 0.125 MiB + 256 MiB = 448.125 MiB
   ```

   Under the 1 GiB default:

   ```text
   initial permits:                     2
   two permits after committed output: ≤127.750 MiB
   one permit after committed output:  ≤575.875 MiB
   ```

   After that point, [plan L731](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:731) requires `LimitError`. Thus `ntasks=1` can accept a table that default parallel `Avro.Table` rejects. The plan nevertheless requires identical results for `ntasks ∈ {1,2,8}` at [plan L1262](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1262) and at least 3× eight-thread speedup at [plan L1319](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1319). Two decode permits plus serial assembly provide no credible path to that gate under the stated defaults.

   **Recommendation:** Use codec-, block-, and schema-specific reservations or incremental permits. Add a sequential low-headroom fallback that preserves acceptance. Test the thresholds around 127.75 MiB and 575.875 MiB with `ntasks=1/2/8`. If the performance gate requires raised limits, state the exact limits in the benchmark protocol.

4. **[blocker] A fixed 1 GiB ceiling is not a safe portable default.**

   **Claim:** The number is portable, but the allocation is not safe on a process with less available memory.

   **Evidence:** The plan performs no RAM, cgroup, or resource-limit check and permits each operation to approach 1 GiB plus Julia runtime and allocator overhead at [plan L402](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:402). One valid default block may reserve about 448 MiB before those excluded costs. A process limited to 512 MiB can therefore be killed before Avro can raise `LimitError`. Concurrent operations each receive another 1 GiB allowance.

   Documentation and a suggested caller-side governor do not make an untrusted default safe.

   **Recommendation:** Use a substantially smaller fixed default with smaller per-block limits, or cap the effective limit against cgroup/RLIMIT-aware available memory. Keep larger limits explicit. A process-wide governor can remain optional, but the single-operation default must fail safely by itself.

5. **[major] `Limits` accepts configurations that the parallel path cannot start.**

   **Claim:** Constructor validation omits the fixed 128 KiB zstd overhead included in runtime `W`.

   **Evidence:** The constructor accepts:

   ```text
   max_block_bytes + max_codec_memory + max_block_output_bytes
       ≤ max_total_bytes
   ```

   at [plan L411](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:411). Runtime adds another 128 KiB at [plan L715](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:715). A configuration at exact equality passes construction but deterministically rejects block zero.

   **Recommendation:** Validate the exact checked runtime reservation, including every fixed overhead and overflow. Prefer codec-specific overhead rather than charging zstd context memory to every codec.

6. **[minor] Empty-union `minsize` is undefined.**

   **Claim:** The plan takes the minimum over union branches without defining the zero-branch case.

   **Evidence:** Apache `schema-tests.txt` case 016 accepts and canonicalizes `[]` at [schema-tests.txt L95](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/apache-avro/share/test/data/schema-tests.txt:95). The plan says unions take the minimum branch size at [plan L459](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:459).

   **Recommendation:** Define a no-finite-datum or infinity sentinel. Test `minsize([])`, `juliatype([])`, canonicalization, and clean encode failure.

7. **[minor] The peak-RSS gate has no fixed pass threshold.**

   **Claim:** “Ceiling plus documented runtime overhead” can be selected after observing the implementation.

   **Evidence:** [Plan L740](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:740) and [plan L1266](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1266) provide no numerical allowance or measurement method. RSS includes the warmed Julia runtime, JIT code, allocator slack, loaded codec libraries, and mapped input.

   **Recommendation:** Before Phase 4c, define a fresh-process warmed baseline, whether mapped inputs are subtracted, and a fixed additive or ratio tolerance. Record both baseline and peak.

8. **[minor] Type-alias normalization is still implicit.**

   **Claim:** Relative and qualified aliases should have one semantic representation before equality and resolution.

   **Evidence:** The specification says a type alias may be fully qualified or relative to the namespace of its type ([spec L267](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:267)). The plan compares aliases structurally at [plan L244](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:244), but does not state when relative aliases become fullnames.

   **Recommendation:** Normalize type aliases to fullnames during parsing, while retaining raw text separately only if exact re-emission requires it. Test `Bar` and `a.Bar` for a type in namespace `a`.

9. **[minor] Streamed compressed-input ownership is not included in the memory proof.**

   **Claim:** The plan does not say whether an owned compressed block coexists with its decompressed buffer.

   **Evidence:** The container layer reads known-size blocks into owned buffers at [plan L348](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:348). Streamed sources keep one block resident at [plan L691](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:691). `W` reserves only one `max_block_bytes` term at [plan L715](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:715).

   **Recommendation:** Decompress directly from a bounded source substream, or reserve both compressed and decompressed buffers before allocation. Test incompressible maximum-size blocks through `IO` and `mmap=false`.

10. **[minor] Schema derivation for `Nothing` is missing.**

   **Claim:** `nothing` is accepted as null, but schema-free encoding derives a schema from `typeof(x)` and no `Nothing` mapping is stated.

   **Evidence:** The value table says `nothing` encodes as null at [plan L518](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:518). Schema-free encoding uses `Avro.schema(typeof(x))` at [plan L618](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:618), while the type mapping only names the `Missing` nullable convention at [plan L586](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:586).

   **Recommendation:** Define `Avro.schema(Nothing) == NullSchema`, including collision behavior for unions containing both `Missing` and `Nothing`, or narrow the schema-free `nothing` promise.

## Non-blocking follow-ups (if any)

Findings 6–10 are non-blocking. Also define the exact cross-version parameter-name sanitization algorithm rather than only examples and a hash suffix.

No further scope expansion is required. RPC, `big-decimal`, schema inference, append, borrowed views, and writer-side parallel compression remain acceptable 2.x deferrals. The canonical logical-comparison amendment and the zero-export API remain sound.

## Milestone and gate assessment

DRAFT v7 now covers schema grammar, defaults, names, binary and JSON encoding, single-object encoding, sorting, OCF/codecs, resolution, canonical form, fingerprints, and all non-deferred logical types well. The 13-root, six-codec corpus and 51 normalized sort cases are credible conformance inputs. The Java and fastavro differential design is sufficient once Writer codec requirements are enforced.

The phase order still needs these changes:

- Phase 0 must fix the default ceiling, exact constructor arithmetic, codec encoder/decoder requirements, and measurable RSS rule.
- Phase 1 can settle empty-union `minsize` and alias normalization.
- Phase 2’s latency gate is now executable and correctly precedes container release work.
- Phase 4a must enforce codec readability under identical limits.
- Phase 4c must replace the assembly accounting and permit policy.
- Phase 4d cannot enforce the eight-thread ratio until Phase 4c permits real parallelism.
- PR-ready, merge-ready, RC-ready, and release-ready remain correctly distinct. The exact RC-archive rerun remains the correct release gate.

Assumptions made:

- The pinned specification and commits remain authoritative.
- Schemas, data, JSON, metadata, and codec payloads can be untrusted.
- Declared fixtures and implementation gates are future deliverables, not review preconditions.
- Default limits must fail with an Avro error before process-level OOM.

Decisions made without user direction:

- I treated the repaired xz/zstd decoder thresholds as resolved and reported the writer-side consequence separately.
- I accepted the recorded feature deferrals and prior canonical-comparison amendment.
- I classified the fixed 1 GiB default and unreserved assembly copy as blockers because they defeat the safety contract.
- I used the package-review readiness rubric to keep implementation, PR, RC, and release gates separate.

Validation performed:

- I inspected DRAFT v7, `response-6.md`, the pinned specification, Apache fixtures, generated corpus descriptions, APIs, tests, benchmarks, milestones, and codec package behavior.
- I measured liblzma decoder and encoder memory and reproduced the 8 MiB xz decode failure.
- I calculated the default permit thresholds from the normative `W` formula.
- `git diff --exit-code 0c7be10db6d83fd20806a8eceec9276a7aa8e21d -- src test` exited 0, so the audited 1.1.2 source and tests remain unchanged.
- I created or modified no files.

This is close, big dawg, but the resource model is still not safe or internally executable.

## Verdict

VERDICT: REVISE
