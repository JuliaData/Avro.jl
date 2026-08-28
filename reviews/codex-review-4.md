# Codex review round 4

## Disposition check (round-3 items)

Round 3 has a bookkeeping inconsistency. Its tables contain eight `PARTIALLY RESOLVED` rows. Round-1 finding 25 and round-3 new finding 8 describe the same oracle defect, so there are seven unique themes. I checked all eight rows.

### Carried findings

| Item | Status | Evidence and assessment |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | Decode work improved, but encode work, codec working memory, block-index allocations, and the compressed-size precheck remain defective. See new findings 1–2. |
| R1-9 — Freezing and hashing | **RESOLVED** | Props/defaults are recursively frozen, hashes are stored at freeze time, defaults are copied per use, and the side cache is gone ([plan:225](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:225)). |
| R1-10 — Schema-driven compilation | **PARTIALLY RESOLVED** | `E` and union representations are finite, but tuple-unrolled heterogeneous column builders can still specialize on schema shape ([plan:389](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:389)). |
| R1-18 — Writer lifecycle | **RESOLVED** | Compression, write, flush, close, rename, poisoning, abort, and atomic cleanup now have contracts and failure-injection gates ([plan:566](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:566)). |
| R1-20 — Parallel determinism | **PARTIALLY RESOLVED** | The lower-index cancellation defect is fixed. Concurrent shared-budget reservations still make the set of failing blocks schedule-dependent. See new finding 7. |
| R1-25 — Union oracle | **RESOLVED** | Direct branch-index/type expectations replace fastavro where Python erases branch identity ([plan:457](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:457), [plan:920](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:920)). |
| R2-new-6 — Symbol interning | **RESOLVED for the original objection** | A locked process-wide admission table now bounds permanent growth across repeated files ([plan:789](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:789)). A smaller multi-tenant concern remains below. |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowing the contract** | Tuple/fixed and UUID use documented deterministic conventions, while schema-free encoding is explicitly not a decoded-data round-trip mechanism ([plan:475](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:475), [plan:489](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:489)). I accept this amendment. |

### Round-3 new findings 1–19

| # | Status | Evidence and assessment |
|---:|---|---|
| 1 — Invalid OCF repair | **PARTIALLY RESOLVED** | Reader/Table/Rows have the flags, but invalid enum defaults cannot be represented and `inspect` omits the flags. See new finding 4. |
| 2 — Reachable limits | **PARTIALLY RESOLVED** | Every binary API named in round 3 now accepts limits. `tojson` remains unbounded despite the adopted “all encode-side APIs” and bounded-JSON claims. |
| 3 — Reserved metadata | **RESOLVED** | All user `avro.*` keys are rejected ([plan:566](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:566)). |
| 4 — Codec input consumption | **RESOLVED** | Clean EOS and complete compressed-input consumption are required and tested ([plan:529](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:529)). |
| 5 — Recursive `minsize` | **RESOLVED** | A memoized cycle-safe computation and named tests now exist ([plan:371](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:371)). |
| 6 — Dormant Scan activation | **RESOLVED** | Scan ships only against a required registered Tables version, or is removed from the 2.0 archive before the RC rerun ([plan:802](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:802)). |
| 7 — Codec test dependencies | **RESOLVED** | CodecBzip2 and CodecXz are explicit test dependencies ([plan:1054](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1054)). |
| 8 — Spec-union oracle | **RESOLVED** | Direct expectations are authoritative; fastavro is limited to observable results. |
| 9 — One-shot inference consumption | **RESOLVED for ownership** | Inference returns the materialized rows, and `write(...; infer=true)` owns that materialization ([plan:583](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:583)). Its inference rules remain unspecified; see new finding 11. |
| 10 — Error hierarchy/fuzz gate | **RESOLVED** | Shared errors are direct `AvroError` subtypes and the raw-datum/whole-file fuzz contracts are separated ([plan:650](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:650)). |
| 11 — Exact RC validation | **RESOLVED** | The complete matrix reruns from the final source archive after pin removal and the Scan decision ([plan:1112](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1112)). |
| 12 — JSON strict mode | **RESOLVED** | Strict and permissive non-finite forms are distinct ([plan:620](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:620)). |
| 13 — `comparebytes` consumption | **RESOLVED** | Both buffers must contain exactly one complete datum ([plan:639](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:639)). |
| 14 — DataAPI | **RESOLVED** | The complete read-only interface is specified ([plan:782](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:782)). |
| 15 — Decimal edges | **RESOLVED** | Scale zero, empty payload, sign extension, precision, and Java’s decode deviation are recorded ([plan:503](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:503)). |
| 16 — Timestamp conversion | **RESOLVED** | Native range failures use `ConversionError`, not `DataError` ([plan:435](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:435)). |
| 17 — `eachblock` ownership | **RESOLVED** | Returned block bytes are owned copies ([plan:536](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:536)). |
| 18 — Writer validation | **RESOLVED** | Sync, block size, level, and reserved metadata are validated ([plan:566](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:566)). |
| 19 — Measurement protocol | **RESOLVED for the prior objection** | Cold processes, repetitions, version-specific counters, and unavailable-counter behavior are defined ([plan:1026](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1026)). |

I continue to accept the exact timestamp wrappers, exact union representation, fixed-value contract narrowing, unsigned bytes/fixed sort order, decode-side decimal precision check, and the narrow “green after each phase” definition. The new opaque-skip problem below is separate from the accepted skipped-string UTF-8 exception.

## New findings

1. **[blocker] Codec output caps do not bound native decoder memory.**

   **Claim:** A small xz or zstandard block can request a large dictionary/window before producing capped output.

   **Evidence:** V4 limits compressed and decompressed bytes, but has no codec-memory limit ([plan:325](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:325), [plan:529](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:529)). CodecXz 0.7.4 defaults to `typemax(UInt64)` memory ([CodecXz:14](/Users/jacob.quinn/.julia/packages/CodecXz/80q1V/src/decompression.jl:14), [CodecXz:26](/Users/jacob.quinn/.julia/packages/CodecXz/80q1V/src/decompression.jl:26)). CodecZstd 0.8.0 exposes no window limit in `ZstdDecompressor` ([CodecZstd:4](/Users/jacob.quinn/.julia/packages/CodecZstd/HXJf5/src/decompression.jl:4)), although its binding contains `ZSTD_d_windowLogMax` ([binding:212](/Users/jacob.quinn/.julia/packages/CodecZstd/HXJf5/src/LibZstd_clang.jl:212)). Parallel workers multiply this exposure.

   **Recommendation:** Add `max_codec_memory`. Pass it to xz, enforce zstd’s maximum window through a supported upstream API or safe adapter, and reserve worst-case codec memory per in-flight worker. Add tiny-output/high-dictionary xz and zstd fixtures under an RSS-limited subprocess.

2. **[blocker] The work and allocation budget remains unsafe and internally inconsistent.**

   **Claim:** Encode work, valid compressed blocks, and internal indexing structures are not covered correctly.

   **Evidence:**

   - Encoding is limited by bytes and depth only ([plan:315](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:315)). A billion-element `Vector{Missing}` or repeated empty record requires a billion iterations but encodes to only a count and terminator.
   - A default Writer can accumulate 65,537 zero-byte records because it flushes by bytes. Its default Reader rejects the resulting size-zero block because the precheck permits only `work_allowance == 65_536`.
   - The precheck uses OCF `size`, which is compressed size ([plan:364](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:364), [plan:526](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:526)). The actual rule also credits decompressed bytes. Valid highly compressible blocks can therefore fail before decompression.
   - `max_blocks = 2^31`, but the parallel block table, scan offsets, and filter masks are not charged to `max_total_bytes` ([plan:336](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:336), [plan:555](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:555)).

   **Recommendation:** Add encode-side value/work counters to binary and container writers. Make Writer output readable under the same default limits. Count all relevant on-wire bytes or define a safe zero-size-value policy. Decompress under codec/output caps before applying the stated compressed-plus-decompressed rule. Charge every internal index/table allocation and lower the default block/value ceilings substantially.

3. **[major] Fast skipping contradicts the declared strict-validation policy.**

   **Claim:** Projection and Scan can accept malformed structures beyond the one documented UTF-8 exception.

   **Evidence:** V4 says skipped values validate boolean bytes, enum/union indexes, and all structure ([plan:299](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:299)). It also skips sized array/map blocks directly by byte size and skips complete OCF blocks for offset/limit ([plan:815](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:815), [plan:820](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:820)). Those paths cannot validate malformed values inside the skipped bytes. The specification says block size permits fast skipping ([spec:362](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:362)); it does not make that skip equivalent to validation. The acceptance gate permits only invalid UTF-8 to differ ([plan:975](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:975)).

   **Recommendation:** Choose explicitly. Strict mode must descend into skipped sized blocks and validate skipped OCF payloads. A fast mode may jump them, but must document every resulting acceptance difference. Test invalid booleans, enums, union tags, nested block framing, and corrupt compressed blocks in skipped regions.

4. **[major] Invalid-default repair is still incomplete.**

   **Claim:** V4 cannot retain an invalid enum default and cannot invoke repair policy through `inspect`.

   **Evidence:** `EnumSchema.default` stores only `nothing` or a valid integer index ([plan:216](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:216)). Raw JSON plus `valid=false` exists only for record-field defaults ([plan:247](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:247)). Enum membership is still always validated ([plan:264](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:264)), despite the general `allow_invalid_defaults` promise ([plan:157](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:157)). Enum defaults affect resolution ([spec:120](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:120)). `Avro.inspect(src; limits)` also omits both repair flags ([plan:767](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:767)).

   **Recommendation:** Give enum defaults raw frozen JSON, source span, optional valid index, and validity. Preserve but never use an invalid writer default. Add both repair options to `inspect`, or define `inspect` as always performing a bounded permissive diagnostic parse.

5. **[major] JSON encoding and in-memory comparison remain unbounded.**

   **Claim:** Two recursive public operations lack the limit contract applied to binary paths.

   **Evidence:** `fromjson` is bounded, but `tojson` has no `limits` keyword or output/depth/value budget ([plan:614](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:614), [plan:747](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:747)). Phase 2 nevertheless promises bounded JSON encoding ([plan:1097](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1097)). `Avro.compare` also has no limits, unlike `comparebytes`; cyclic in-memory records can recurse indefinitely ([plan:639](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:639)).

   **Recommendation:** Add `limits=Limits()` to both operations. Charge actual JSON output, values, and depth. Reject cyclic values deterministically with `EncodeError` or `LimitError`.

6. **[major] Tuple-unrolled columns can still cause schema-derived compiler growth.**

   **Claim:** A finite element-type set does not bound specializations for ordered heterogeneous builder tuples in a practical way.

   **Evidence:** The column path is tuple-unrolled through 32 fields using `ColumnBuilder{e}` ([plan:389](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:389)). Warm-up covers members of `E`, not every width and ordered element-type combination, yet the gate then demands zero new method instances ([plan:398](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:398)).

   **Recommendation:** Store builders in a schema-independent runtime container behind a function barrier. If only width is specialized, state that precisely and warm every width. Do not specialize on ordered element-type combinations.

7. **[major] Shared atomic budget reservations keep parallel errors schedule-dependent.**

   **Claim:** The minimum-index cancellation rule does not make cumulative `LimitError`s deterministic.

   **Evidence:** Workers reserve one shared budget concurrently before allocation ([plan:362](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:362), [plan:555](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:555)). If only one of two blocks fits the remaining budget, whichever reserves first succeeds. A later corrupt block can also consume the allowance and cause an earlier valid block to fail, masking the corruption.

   **Recommendation:** Track local block usage and commit cumulative usage in block-index order, or explicitly make limit failure operation-global while ensuring it cannot hide an earlier content error. Force opposite reservation schedules in tests.

8. **[major] Resolution output representation is internally inconsistent.**

   **Claim:** Writer union tags cannot always be “preserved,” and the public decode default selects the wrong schema’s type.

   **Evidence:** V4 says tags survive resolution ([plan:464](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:464)). A nullable reader union is represented as a bare value ([plan:429](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:429)). The specification requires a selected writer-union branch to resolve recursively against a non-union reader ([spec:722](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:722)). The API also defaults `T` from `writer_schema` even when `reader_schema` is supplied ([plan:744](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:744)).

   **Recommendation:** Define output from the reader schema: unwrap for non-union and nullable readers; add or remap `UnionValue` to the reader branch index for tagged reader unions. Default `T` from the effective reader schema. Test all writer-union/reader-union and union/non-union directions.

9. **[major] Resolution between different recognized non-decimal logical types is undefined.**

   **Claim:** V4 covers decimal pairs and logical/plain pairs, but not different logical annotations on matching underlying schemas.

   **Evidence:** The rule stops after those cases ([plan:466](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:466)). Logical types retain the underlying encoding ([spec:785](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:785)). Decimal alone has an extra precision/scale matching rule ([spec:813](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:813)).

   **Recommendation:** Resolve different non-decimal logical types through their underlying schemas and apply the reader interpretation. Test timestamp-millis→timestamp-micros, global→local timestamp, date→time-millis, and promotion cases. UUID string→fixed must still fail because the underlying kinds differ.

10. **[major] Logical-type sort order is unspecified and untested.**

    **Claim:** Native Julia value order can differ from the required underlying Avro order.

    **Evidence:** Logical values use the underlying schema encoding ([spec:785](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:785)); bytes/fixed sort unsigned lexicographically ([spec:436](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:436)). Decimal `-1` (`ff`) therefore sorts after `0` (`00`), unlike numeric decimal order. Duration also sorts by its 12 little-endian bytes, not numerically by its fields. V4 does not state this ([plan:637](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:637)); a read-only `rg logicalType fixtures/sortorder` found no logical sort vectors.

    **Recommendation:** Define both comparison APIs over the underlying Avro schema. Add every logical type, especially decimal and duration, to both value and encoded comparator matrices.

11. **[major] `inferschema` still has no deterministic inference contract.**

    **Claim:** Returning consumed rows fixes ownership but not schema meaning.

    **Evidence:** V4 only says inference is bounded and materializing ([plan:583](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:583), [plan:729](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:729)). It defines no rules for empty sources, absent or reordered fields, null-only values, numeric joins, heterogeneous arrays/maps, named-type conflicts, union branch order, or unknown Tables types.

    **Recommendation:** Define a deterministic field-set and type-join lattice with explicit ambiguity failures and order-independent tests. Otherwise defer inference and require an explicit schema for schema-less sources.

12. **[major] The single-record performance gates cannot measure the public API as written.**

    **Claim:** The target excludes work that every public call must perform.

    **Evidence:** Plans take microseconds to construct and are operation-owned with no cache ([plan:396](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:396)). Public encode/decode accepts a schema, not a prepared plan ([plan:743](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:743)). Typed decode is nevertheless required to finish in 150 ns with only the string allocation, and `encode!` must allocate zero ([plan:1040](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1040)).

    **Recommendation:** Expose a reusable prepared reader/writer or plan-based API. Apply the kernel target to that API. Add a separate one-shot benchmark that includes plan construction.

13. **[minor] The JSON union contract still has two ambiguities.**

    **Claim:** Default validation wording is wrong for unions, and wrapper-label collisions lack a policy.

    **Evidence:** V4 applies the datum JSON table to defaults ([plan:634](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:634)), but union defaults are bare JSON selected by first match ([spec:81](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:81)). The spec also permits complex keywords as named-type names ([spec:258](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:258)); a record named `map` plus a map branch has two `"map"` JSON labels. Java rejects this schema while fastavro accepts it. V4 lists collisions without a decision ([plan:895](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:895)).

    **Recommendation:** State the union-default exception. Accept the binary schema but reject ambiguous JSON conversion, or record a deliberate Java-compatible parse restriction.

14. **[minor] The encoded sort gate needs normalization and alternate block forms.**

    **Claim:** The current oracle is not fully machine-stable and does not test representation-independent array comparison.

    **Evidence:** [verdicts.tsv:31](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/fixtures/sortorder/verdicts.tsv:31>) contains truncated Java stack frames rather than a normalized map-error verdict. Arrays may use positive or negative-count sized blocks ([spec:359](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:359)), but cross-form comparison is not a gate.

    **Recommendation:** Normalize to `-1`, `0`, `1`, or `ERROR:<class>`. Compare equal and unequal arrays across positive/positive, sized/sized, and positive/sized encodings.

15. **[minor] “Every root kind” is not yet the declared corpus.**

    **Claim:** Four primitive roots are omitted.

    **Evidence:** The corpus lists int, string, double, bytes, enum, fixed, array, map, and union ([plan:881](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:881)). It omits null, boolean, long, and float, while the summary promises every root kind ([plan:1124](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1124)). Null is materially important because any datum count has zero payload bytes.

    **Recommendation:** Add all four for every codec and both producers.

16. **[minor] Limit and concurrency options lack construction-time validation.**

    **Claim:** Negative and contradictory settings have undefined behavior.

    **Evidence:** `Limits` is a raw `@kwdef` structure ([plan:327](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:327)). `Table` accepts arbitrary `ntasks`; Writer checks only `block_bytes > 0`, not its relation to `max_block_bytes` ([plan:566](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:566), [plan:756](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:756)).

    **Recommendation:** Validate every limit and checked cross-field relation once. Require `ntasks ≥ 1`; allow only zero or positive `max_inflight_blocks`; cap actual tasks to available blocks.

17. **[minor] Atomic file replacement lacks a portable filesystem contract.**

    **Claim:** “Atomic” is too broad for existing destinations and crash behavior.

    **Evidence:** V4 only says a sibling temp is renamed and that failures leave the destination untouched ([plan:573](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:573)).

    **Recommendation:** Define secure temp creation, symlink handling, existing-destination replacement, mode preservation, same-filesystem requirements, Windows behavior, and whether file/directory `fsync` is excluded. Test replacement and rename failure on every supported OS.

18. **[minor] The global symbol quota remains a multi-tenant availability boundary.**

    **Claim:** One tenant can exhaust the quota for all later untrusted Tables users in the process.

    **Evidence:** The admission set is permanent, process-wide, and only bypassed with `names=:trusted` ([plan:789](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:789)).

    **Recommendation:** Permit a caller-owned configurable admission object. Delay Rows symbol admission until a Tables name/schema API is actually requested.

19. **[minor] CI has no executable source-coverage gate.**

    **Claim:** Docstring coverage is present, but the rewrite has no source coverage report or regression threshold.

    **Evidence:** The CI and quality lists include platform tests, interop, Aqua, JET, docs, and docstring coverage, but no process-coverage or Codecov job ([plan:1000](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1000), [plan:1073](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1073)).

    **Recommendation:** Add Julia process-coverage aggregation and a project/patch threshold or ratchet. Treat it as supporting evidence, not a substitute for the conformance gates.

20. **[nit] “Unambiguous” misdescribes schema-free encoding.**

    **Claim:** The accepted contract uses deterministic conventions, not unique Avro representations.

    **Evidence:** UUID can be string or fixed, and a tuple can appear under many named fixed schemas, yet the API and decision log say schema-free encoding exists only where the schema is unambiguous ([plan:726](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:726), [plan:1156](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1156)).

    **Recommendation:** Say “uses the documented conventional schema.”

## Non-blocking follow-ups (if any)

Findings 13–20 are non-blocking. The unsigned byte/fixed decision is correct and is supported by the new Java evidence. The decimal and timestamp vectors now cover the earlier gaps.

RPC, big-decimal, append mode, borrowed views, and writer-side parallel compression may remain deferred. `inferschema` should also be deferred unless its full lattice is specified before implementation.

## Milestone and gate assessment

| Phase | Required correction |
|---|---|
| 0 | Add high-window codec fixtures, the four missing roots, normalized sort errors, coverage plumbing, and explicit codec-memory feasibility. |
| 1 | Repair invalid enum defaults and the `inspect` policy. Validate every numeric schema attribute before converting to `Int`. |
| 2 | Fix encode work, JSON/value-comparison limits, opaque-skip semantics, internal allocation accounting, column specialization, and the prepared-plan API. |
| 3 | Define reader-directed union output, all non-decimal logical resolution pairs, underlying logical sort order, and alternate array-block comparator tests. |
| 4a | Enforce codec working-memory caps. Either define inference completely or defer it. Ensure default-written zero-size values are default-readable. |
| 4c | Make budget failures deterministic, not only content-error cancellation. |
| 4d | Test malformed data inside projected fields and skipped OCF blocks under the chosen validation policy. |
| 5 / RC | The Scan release rule and exact final-archive rerun are now correct. Keep them. Add the corrected coverage, resource, and interop gates to that complete rerun. |

The bidirectional Java/fastavro strategy is much stronger. It is not yet sufficient because it misses codec dictionary bombs, four primitive roots, logical sort semantics, and alternate encoded array block forms. The compressed-size precheck can also reject valid Java or fastavro files before the stated work rule is evaluated.

The audit table remains accurate. `git diff --exit-code 0c7be10 -- src test` returned exit 0, so the audited 1.1.2 source and tests remain unchanged. I found no new section 2.2 discrepancy.

The ratio benchmarks are fairer and measurable. The single-datum targets are not executable until the plan states whether plan construction is included and exposes a reusable prepared codec when it is excluded.

The PR-ready, merge-ready, RC-ready, and release-ready distinctions are now sound. The implementation-phase gates are not yet executable because blocker and major contracts remain unresolved.

## Verdict

DRAFT v4 is not implementation-ready, big dawg. It fixes most round-3 findings, but codec dictionary memory and encode/internal work remain blocker-level. Union resolution, logical resolution/order, strict skipping, compilation bounds, parallel budgets, inference, and performance APIs still have major specification gaps.

Assumptions made:

- The pinned 1.13-SNAPSHOT specification is authoritative.
- The Tables `df4e68c…` source is the development target.
- Configurable limits may reject valid data, but defaults must be safe, internally consistent, and able to read default Writer output.
- Declared fixtures and gates remain phase deliverables.
- RPC and big-decimal remain acceptable deferrals.

Decisions made without user direction:

- I accepted the narrowed schema-free encoding contract.
- I treated codec dictionary/window memory as part of the untrusted-input budget.
- I treated inference as optional scope that can be deferred.
- I used the package-review checklist to keep phase, PR, RC, and release evidence separate.

Validation performed:

- I inspected DRAFT v4, response-3, the pinned specification, Tables.Scan source, codec sources, generated roots, sort verdicts, and the existing Java/Python evidence.
- I confirmed CodecXz’s unlimited default and CodecZstd’s missing public window cap.
- I confirmed the sort corpus has no logical schemas and that its map verdict is malformed.
- I confirmed `src/` and `test/` have no changes from `0c7be10`.
- I created or modified no files.

VERDICT: REVISE
