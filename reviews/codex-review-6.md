# Codex review round 6

## Disposition check (round-5 items)

### Carried rows

| Item | Status | Evidence |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | The common work rule and constructor checks are specified at [plan L357](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:357). Writer flushing is work-driven at [plan L683](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:683). The semaphore, Writer/Reader byte symmetry, source-size bound, and default memory policy remain defective. See findings 1–5. |
| R1-9 — Freezing and hashing | **RESOLVED** | Recursive freezing, freeze-time hashes, fresh defaults, and structural equality are explicit at [plan L233](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:233). |
| R1-10 — Schema-driven compilation | **RESOLVED** | Generic plans remain dynamic. Column storage is schema-independent. The numerical specialization gate is defined at [plan L442](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:442). |
| R1-18 — Writer lifecycle | **RESOLVED** | Atomic replacement, poisoning, cleanup, abort, partial-sink behavior, and close semantics are covered at [plan L683](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:683). |
| R1-20 — Parallel determinism | **RESOLVED for semantic error selection** | Ordered budget commits and lowest-index content errors are specified at [plan L671](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:671). The separate memory/liveness design is not resolved. |
| R1-25 — Union oracle | **RESOLVED** | Direct branch-index and result-type expectations are required at [plan L529](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:529) and [plan L1049](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1049). |
| R2-new-6 — Symbol interning | **PARTIALLY RESOLVED** | Binary typed decoding and Tables names use admission objects at [plan L451](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:451) and [plan L938](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:938). Typed `fromjson` still cannot accept a caller-owned admission object. |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowed contract** | [Plan L578](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:578) now describes deterministic conventions and disclaims representation-preserving round trips. I accept this amendment. |

### Round-4 findings carried through round 5

| # | Status | Evidence |
|---:|---|---|
| 1 — Codec memory | **PARTIALLY RESOLVED** | Decoder window controls exist at [plan L625](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:625), but the xz threshold and large zstd limits remain wrong. |
| 2 — Work/allocation budget | **PARTIALLY RESOLVED** | The work rule is improved at [plan L404](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:404), but the compressed-size precheck, transient accounting, and default ceilings remain defective. |
| 3 — Fast skipping versus strict validation | **RESOLVED** | [Plan L322](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:322). |
| 4 — Enum-default repair / inspect | **RESOLVED** | [Plan L261](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:261), [plan L914](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:914). |
| 5 — Bounded `tojson` and comparison | **RESOLVED** | [Plan L737](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:737), [plan L767](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:767). |
| 6 — Tuple-unrolled column builders | **RESOLVED** | [Plan L453](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:453). |
| 7 — Parallel budget determinism | **RESOLVED for ordered semantic accounting** | [Plan L671](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:671). |
| 8 — Resolution output representation | **RESOLVED** | [Plan L537](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:537). |
| 9 — Non-decimal logical resolution | **RESOLVED** | [Plan L544](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:544). |
| 10 — Logical sort order | **RESOLVED by narrowed canonical-encoding contract** | [Plan L779](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:779). I accept the amendment. |
| 11 — Schema inference | **RESOLVED BY DEFERRAL** | [Plan L172](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:172). I accept this scope decision. |
| 12 — Prepared datum gates | **RESOLVED** | [Plan L459](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:459), [plan L1222](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1222). |
| 13 — JSON union ambiguity | **RESOLVED** | [Plan L296](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:296), [plan L759](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:759). |
| 14 — Sort normalization/block forms | **RESOLVED** | [Plan L1051](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1051). |
| 15 — Missing root schemas | **RESOLVED** | All 13 kinds are listed at [plan L1044](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1044). |
| 16 — Option validation | **RESOLVED for the original checks** | Cross-field, task-count, and block-size checks are at [plan L397](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:397). The new zstd upper-range defect is finding 9. |
| 17 — Atomic replacement | **RESOLVED** | [Plan L693](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:693). |
| 18 — Multi-tenant field-name quota | **RESOLVED for its original Tables scope** | [Plan L938](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:938). |
| 19 — Coverage gate | **RESOLVED by explicit classification** | Coverage is expressly informational at [plan L1194](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1194). I accept that decision. |
| 20 — Conventional-schema wording | **RESOLVED** | [Plan L578](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:578). |

### Round-5 new findings 1–15

| # | Status | Evidence |
|---:|---|---|
| 1 — Default Writer output rejected by Reader | **PARTIALLY RESOLVED** | Work-driven flushing fixes zero-size nested values at [plan L404](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:404). Symmetric lifetime byte, schema, and metadata limits are still absent. |
| 2 — Parallel transient memory | **PARTIALLY RESOLVED** | A semaphore was added at [plan L665](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:665), but it under-reserves decompression and can deadlock. |
| 3 — Work-rule amplification | **PARTIALLY RESOLVED** | The allowance is now per operation, not per block. However, defaults still permit about 43 million datum iterations from 655 KiB and up to `2^31` values. The constants remain unmeasured guesses at [plan L1416](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1416). |
| 4 — Codec-memory contract | **PARTIALLY RESOLVED** | The cap is correctly narrowed to a decoder window/dictionary cap, but the xz success threshold and large zstd-cap conversion are wrong. |
| 5 — Contextual numeric attributes | **RESOLVED** | [Plan L247](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:247). |
| 6 — Decimal scale width | **RESOLVED** | Runtime scale is `Int` at [plan L500](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:500). |
| 7 — Logical comparison agreement | **RESOLVED by narrowed contract** | Canonical native encoding versus raw noncanonical bytes is explicit at [plan L781](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:781). I accept this amendment. |
| 8 — Resolution work | **RESOLVED** | [Plan L377](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:377), [plan L515](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:515). |
| 9 — Julia-derived naming collisions | **PARTIALLY RESOLVED** | The reported parameter and union collisions are detected at [plan L557](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:557). Other invalid Julia-derived names and the unusable nested `name=` remedy remain. |
| 10 — Typed Symbol admission | **PARTIALLY RESOLVED** | Binary APIs expose admission. Typed `fromjson` does not. |
| 11 — Decimal scale equality | **RESOLVED** | [Plan L353](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:353). |
| 12 — Filter-aware Scan offset/limit | **RESOLVED** | [Plan L972](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:972). |
| 13 — Ignored namespace | **RESOLVED** | [Plan L276](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:276). |
| 14 — Oracle-readability scope | **RESOLVED** | [Plan L1018](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1018). |
| 15 — Strict-mode principle wording | **RESOLVED** | [Plan L203](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:203). |

## New findings

1. **[blocker] The transient-memory semaphore neither bounds memory nor guarantees progress.**

   **Claim:** It under-reserves decompressed buffers. Retained chunks can then deadlock stage 2 or stage 3.

   **Evidence:** OCF `size` is the compressed size after codec application ([spec L483–488](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:483)). The plan reserves a decompressed buffer as `min(max_block_bytes, declared size)` ([plan L665–670](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:665)). A small compressed block can expand toward 64 MiB while reserving only kilobytes.

   Per-block chunks retain reservations until their buffers are freed. Assembly starts only in stage 3 ([plan L668–681](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:668)). Chunks can fill the semaphore before all workers start. If stage 2 completes, an equally large final-column allocation can still wait forever while the source chunks occupy the capacity. Releasing permits without freeing the chunks would instead falsify the memory bound. Zstd’s stated fixed context overhead is also outside the reservation.

   **Recommendation:** Reserve `max_block_bytes` before decompression, or acquire permits incrementally before every buffer growth. Include fixed codec overhead. Pre-reserve final columns and copy/release chunks incrementally, or provide another proof that no task waits while holding memory needed for progress. Test highly compressed blocks and tables whose chunks exceed half the semaphore capacity.

2. **[blocker] The default Writer/default Reader guarantee is still false.**

   **Claim:** The Writer applies the common work rule, but it does not apply all cumulative limits that the Reader applies.

   **Evidence:** Reader budgets charge decompressed bytes, decoded output, defaults, and internal structures ([plan L371–377](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:371), [plan L422–434](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:422)). Writer behavior names per-datum/per-block bytes and lifetime rows/values, but not lifetime payload bytes or predicted decoded output ([plan L683–692](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:683)). It can therefore emit more than 1 GiB of ordinary payload while every individual block and the work rule pass. A default Reader on a floor-budget host then rejects it.

   Writer validation also does not apply `max_metadata_bytes`, `max_metadata_entries`, or serialized-schema limits. Reader header parsing always applies them at [plan L616–624](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:616). Defaults vary by machine at [plan L392](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:392), so output accepted on a large host can fail under defaults on a smaller host. This contradicts [plan L418–420](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:418) and decision 12 at [plan L1369–1373](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1369).

   **Recommendation:** Apply the same cumulative uncompressed payload, predicted decoded-output, work, schema, and metadata limits before writing. Scope the invariant to identical explicit `Limits`, or use portable fixed defaults. Add multi-block, large-metadata, and large-schema Writer-to-Reader invariant tests.

3. **[blocker] The default memory policy is unsafe even if the semaphore algorithm is repaired.**

   **Claim:** A single default operation can approach all physical memory. Concurrent operations can exceed it.

   **Evidence:** `default_total_bytes()` is half physical RAM with a minimum of 1 GiB, even on hosts with less than 2 GiB ([plan L392–394](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:392)). Parallel memory is defined as one `max_total_bytes` semaphore capacity plus committed output ([plan L823–824](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:823)). One operation can therefore approach physical RAM before Julia, source buffers, and allocator overhead. Every operation owns a separate budget ([plan L436–438](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:436)); two concurrent calls can each claim half of physical RAM. The 1 GiB floor can exceed available memory outright.

   **Recommendation:** Use a conservative fixed default or a cgroup/RLIMIT-aware available-memory limit. Do not set a floor above measured available memory. Bound transient plus committed package-owned memory under one per-operation ceiling. If large concurrent calls are supported, use a shared process-level governor or require explicit caller budgets.

4. **[major] The compressed-size work precheck rejects safe, valid oracle files.**

   **Claim:** The plan defines work from decompressed bytes, then substitutes compressed size before decompression.

   **Evidence:** The normative rule uses decompressed payload plus framing ([plan L404–408](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:404)). The early check instead uses compressed `size` ([plan L411–412](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:411)).

   A read-only fastavro probe wrote one million empty-string root datums in one block:

   ```text
   deflate complete OCF:     1,071 bytes
   zstandard complete OCF:     134 bytes
   ```

   Both have about 1 MiB of decompressed data and one million values. They pass `max_block_count`, `max_block_bytes`, and the exact decompressed work rule. The compressed-size precheck rejects both.

   **Recommendation:** Before decompression, enforce only hard count, compressed-size, remaining-input, and codec-window limits. Decompress under the output cap. Then apply the exact work rule before datum iteration where the count supplies a safe lower bound.

5. **[major] `mmap=false` can allocate an unbounded source file before block limits run.**

   **Claim:** The documented alternative to mmap reads the whole path into memory without a source-size budget.

   **Evidence:** Path sources with `mmap=false` are read into memory at [plan L647–648](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:647). The plan recommends that mode for files that may change at [plan L659–660](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:659). `Limits` has no source-file limit. Its byte accounting excludes the owned compressed input buffer ([plan L371–377](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:371)).

   **Recommendation:** Add `max_source_bytes` and check it before allocation, or make `mmap=false` paths use the sequential block reader. Charge any package-owned input copy to the operation budget.

6. **[major] The Phase 1 canonical-form gate conflicts with alias validation.**

   **Claim:** The parser rejects official test inputs that the plan requires to pass.

   **Evidence:** The parser requires alias uniqueness against names at [plan L276–285](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:276). Apache `schema-tests.txt` cases 023 and 024 use `name:"foo", aliases:["foo","bar"]` ([fixture L130–138](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/apache-avro/share/test/data/schema-tests.txt:130)). The plan says this file is parsed and executed at [plan L1031–1034](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1031), with a 100% Phase 1 gate at [plan L1296](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1296).

   A read-only Java 1.12.2 probe accepted that schema and returned:

   ```json
   {"name":"foo","type":"record","fields":[]}
   ```

   **Recommendation:** Treat a self-alias as idempotent while still rejecting aliases that collide across distinct names. Alternatively, record a deliberate divergence and stop claiming that strict parsing passes 100% of the file.

7. **[major] Caller-owned Symbol admission cannot cover typed JSON decoding.**

   **Claim:** `fromjson` accepts a typed target but exposes no `names=` keyword.

   **Evidence:** Typed targets can contain `Symbol`, and the plan says every such value passes through the operation’s admission object ([plan L586–596](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:586)). Section 6 says callers can provide their own admission object with `names=` ([plan L938–946](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:938)). `fromjson(schema, json, T=…)` omits it in both the contract and public API ([plan L737–742](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:737), [plan L894](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:894)).

   **Recommendation:** Add `names=Avro.DEFAULT_ADMISSION` to `fromjson`. Pass it through both typed construction paths. Test repeated JSON inputs with default, caller-owned, exhausted, and `:trusted` admission.

8. **[major] Julia-derived schemas still have no complete valid-name policy.**

   **Claim:** Valid Julia types and Tables schemas can produce names that are invalid in Avro, or collision errors whose suggested repair is unavailable.

   **Evidence:** Avro requires ASCII names matching `[A-Za-z_][A-Za-z0-9_]*`, including named types, fields, and enum symbols ([spec L182–192](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:182)). The plan uses `nameof(T)`, `string(parentmodule(T))`, Julia field names, enum symbols, and Tables names. It sanitizes only parameter spelling ([plan L557–577](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:557)). Julia permits Unicode and quoted identifiers such as `Å` and `var"bad-name"`.

   A collision between two nested sanitized parameter names raises an error asking for `name=`, but that keyword only overrides the root schema ([plan L569–577](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:569), [plan L868–871](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:868)). It cannot rename either nested type.

   **Recommendation:** Define one policy for every fullname component, field, enum symbol, and Tables column. Either use deterministic collision-resistant mapping, or reject with a field/type path and an actionable explicit-schema or rename remedy. Add a nested-name override map if `name=` is to remain the documented collision remedy.

9. **[major] The codec-cap contract still contains two unexecutable cases.**

   **Claim:** The xz fixture does not succeed at the stated threshold, and large valid limits produce invalid zstd options.

   **Evidence:** The plan says the 1 GiB-dictionary xz fixture succeeds at any cap of at least 1 GiB ([plan L635–637](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:635), [plan L1058–1062](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1058)). A read-only probe found:

   ```text
   memlimit = 1,073,741,824  -> LZMAError: Memory usage limit exceeded
   memlimit = 1,073,807,408  -> success
   ```

   Liblzma includes decoder state in `memlimit`.

   The zstd conversion can return values through 62 ([plan L627–628](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:627)). CodecZstd 0.8.7 requires `windowLogMax` within its reported bounds ([CodecZstd L38–51](/Users/jacob.quinn/.julia/packages/CodecZstd/dNeRy/src/decompression.jl:38)). A read-only Julia probe returned:

   ```text
   bounds: (10, 31)
   31: ok
   32: ArgumentError
   ```

   **Recommendation:** Gate xz against liblzma’s reported memory requirement, not dictionary size alone. Keep its threshold separate from zstd. Clamp zstd to the library’s supported maximum so a larger byte cap permits every supported frame. Test configured caps above 2 GiB.

10. **[major] The default work ceilings are still unproven as production-safe.**

    **Claim:** Input proportionality alone does not make the selected amplification factor safe.

    **Evidence:** Defaults allow 64 values per input byte, `2^31` total values, and `2^31` rows ([plan L371–376](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:371)). The plan explicitly permits about 43.1 million datum iterations from a 655 KiB file ([plan L409–410](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:409)). A roughly 32 MiB adversarial input can purchase the full two billion value operations. The constants are described as “conservative guesses” validated only against the functional corpora ([plan L1416–1418](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1416)). No worst-density latency gate exists.

    **Recommendation:** Benchmark the slowest legal zero-byte/nested value shape on each supported Julia version. Set `max_total_values`, `max_rows`, the allowance, and the byte multiplier from a documented worst-case latency target. Keep explicit overrides for trusted bulk workloads.

11. **[minor] JSON output and input use different default nesting ceilings.**

    **Claim:** Default `tojson` can emit text that default `fromjson` rejects.

    **Evidence:** `tojson` uses `max_depth=1024`, while `fromjson` applies the lexical `max_schema_depth=256` limit ([plan L362–380](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:362), [plan L737–742](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:737)). A recursive schema can encode a datum deeper than 256 without requiring a deeply nested schema document.

    **Recommendation:** Add a distinct `max_json_depth`, or use one common effective datum-JSON depth on both operations. Add boundary round trips at 256 and 257 levels.

12. **[minor] Duplicate named union branches are not explicit.**

    **Claim:** The plan explicitly limits only unnamed union kinds.

    **Evidence:** The specification permits several named branches because their fullnames distinguish them, but still forbids repeating the same schema type ([spec L159–167](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:159)). The plan’s scope table says only “one unnamed type per kind” ([plan L159](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:159)); “union rules” later is not explicit.

    **Recommendation:** Define union branch identity as primitive/complex kind for unnamed schemas and fullname for named schemas. Reject a definition plus a reference to that same fullname. Add a negative fixture.

13. **[nit] The attribute wording is broader than its intended context.**

    **Claim:** “Only `fixed.size` is schema syntax; every other attribute is metadata” literally includes required attributes such as `type`, `name`, and `fields`.

    **Evidence:** [Plan L247–250](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:247).

    **Recommendation:** Limit this statement to `size`, `precision`, `scale`, `logicalType`, and unknown/custom properties.

## Non-blocking follow-ups (if any)

Findings 11–13 are non-blocking. Also:

- Define one shared work-allowance deficit counter. The current prose applies the allowance per datum/block/cumulative, but Writer block flushing omits it at [plan L407–416](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:407).
- Define precedence when an earlier block has a budget failure and another block has a content failure.
- Scope the generated comparison property to orderable schemas and call `encode(schema, value)` explicitly. Maps otherwise raise by design.
- Clarify whether `eachblock` returns compressed or decompressed bytes.
- Account for writer-side native compression memory, or state explicitly that `max_codec_memory` is decode-only throughout the security summary.

RPC, `big-decimal`, inference, append, borrowed views, and writer-side parallel compression remain acceptable deferrals. The canonical logical comparison amendment remains acceptable.

## Milestone and gate assessment

DRAFT v6 has a strong conformance matrix. Schema grammar, defaults, binary and JSON encoding, single-object encoding, sorting, resolution, canonical form, fingerprints, non-deferred logical types, codec interop, and root-schema coverage are otherwise well specified. The audit table also remains accurate: `git diff --exit-code 0c7be10db6d83fd20806a8eceec9276a7aa8e21d -- src test` exited 0.

The gates are not executable in their current order:

- **Phase 0** must correct the xz threshold and zstd upper-cap conversion.
- **Phase 1** cannot reach `schema-tests.txt` 100% until self-alias handling is settled. Julia-derived name handling also belongs here.
- **Phase 2** must add typed JSON admission and establish measured work ceilings.
- **Phase 4a** must remove the compressed-size work precheck, enforce symmetric Writer limits, and bound `mmap=false`.
- **Phase 4c** must replace the semaphore algorithm and repair the default memory policy. Its current peak-RSS and liveness gates cannot pass for near-limit tables.
- **Phase 4d and Phase 5** are correctly ordered after those corrections.
- The PR-ready, merge-ready, RC-ready, and release-ready distinctions remain sound. The exact RC-archive rerun is still the correct release gate.

Assumptions made:

- The pinned specification remains authoritative.
- Schemas, files, JSON, and metadata can be untrusted.
- Default limits must fail safely and predictably.
- Declared fixtures and gates are future implementation deliverables.
- The recorded feature deferrals remain allowed.

Decisions made without user direction:

- I accepted the schema-inference deferral and canonical logical-comparison amendment.
- I retained the documented skipped-string UTF-8 exception.
- I classified unexecutable gates and unsafe defaults as major or blocker findings.
- I used the package-review checklist to keep PR readiness separate from RC and release readiness.

Validation performed:

- I inspected DRAFT v6, `response-5.md`, the prior review, the pinned specification, Apache fixtures, generated fixtures, CodecZstd/CodecXz sources, APIs, tests, benchmarks, and milestones.
- I ran read-only Java canonical-form verification for the self-alias schema.
- I checked fastavro compressed empty-string blocks, xz memory thresholds, and CodecZstd option bounds.
- I confirmed that `src/` and `test/` are unchanged from the audited `0c7be10` source.
- I created or modified no files.

DRAFT v6 is materially better, big dawg. It is not implementation-ready. The central Writer/Reader invariant and transient-memory guarantee still fail. Several Phase 0–4 gates are also unexecutable.

## Verdict

VERDICT: REVISE
