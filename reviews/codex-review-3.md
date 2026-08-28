# Codex review round 3

## Disposition check (round-2 items)

Round 2 had 17 open round-1 entries: 16 partial and one not resolved. I checked all 17.

### Open round-1 findings

| Item | Status | Evidence and assessment |
|---:|---|---|
| 1 — Resource limits | **PARTIALLY RESOLVED — blocker remains** | The new limits cover more categories ([plan:317](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:317)), but the defaults remain unsafe. A roughly 23-byte OCF block with a zero-field record root, `count = 2^30`, and `size = 0` passes `max_block_count`, `max_rows`, and `max_total_values`, then causes one billion iterations. The byte budget does not constrain that work. The 1 GiB floor and 64 GiB ceiling also do not establish the claimed half-RAM bound. There is no `max_blocks` or cumulative compressed/source-byte counter. |
| 2 — Datum JSON limits | **RESOLVED** | `fromjson` now has lexical size/depth checks, duplicate-key rejection, lazy traversal, and operation budgets ([plan:563](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:563)). |
| 5 — Union branch recovery | **RESOLVED** | Exact generic representation now precedes coercive acceptance ([plan:399](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:399)). |
| 6 — Local timestamp millis | **RESOLVED** | All global and local timestamp precisions use exact wrappers. `DateTime` conversion is explicit and range-checked ([plan:407](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:407)). |
| 9 — Freezing and hashing | **PARTIALLY RESOLVED — blocker remains** | Nodes and outer schema containers freeze, but `FrozenDict{String,Any}` properties and `DefaultValue.value` can contain mutable nested arrays, maps, and records ([plan:203](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:203), [plan:246](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:246)). Mutation can invalidate cached hashes. The unspecified identity side table also contradicts “no global mutable state” and lacks lock, ownership, weak-key, and lifetime rules ([plan:197](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:197), [plan:229](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:229)). |
| 10 — Schema-driven compilation | **PARTIALLY RESOLVED — major remains** | Runtime-scale decimals and `Any` fallbacks help. However, arbitrary injective unions still create schema-derived Julia unions, `Vector{Union{…}}`, `Dict{String,Union{…}}`, and `ColumnBuilder{E}` specializations ([plan:373](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:373), [plan:399](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:399)). The combinatorial set cannot support the promised finite warm-up and zero-new-method gate. |
| 15 — Logical types | **RESOLVED** | Decimal precision is checked during encode and decode; all timestamp wrappers are exact; big-decimal is explicitly deferred ([plan:171](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:171), [plan:403](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:403)). |
| 16 — Schema store | **RESOLVED** | Structural ambiguity, CRC collisions, and untrusted external-store results are handled ([plan:549](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:549)). |
| 17 — Comparison API | **RESOLVED** | Values and encoded datums now use separate APIs, and encoded comparison is budgeted ([plan:588](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:588)). A smaller exact-consumption omission remains below. |
| 18 — Legacy and writer lifecycle | **PARTIALLY RESOLVED — major remains** | Decimal reinterpretation is explicit-only, and normal poison/abort behavior is described ([plan:504](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:504), [plan:525](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:525)). Failures during final block compression, close, sink flush, temp-file close, or rename still have no cleanup/state contract. |
| 19 — Close ownership | **RESOLVED** | Caller-owned IO is never closed; readers only release their reference ([plan:511](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:511)). |
| 20 — Parallel error handling | **PARTIALLY RESOLVED — major remains** | Atomic reservation was added, but any error cancels all workers before selecting the lowest observed block ([plan:517](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:517)). A higher block can fail first and cancel a lower corrupt block before it reaches its error. The result remains scheduler-dependent. |
| 22 — Tables.Scan API | **RESOLVED against the declared pin** | The authoritative `df4e68c…` source exposes `Tables.resolve`, `BoundScan`, `All()`, and `filtermask(::BoundScan, ...)`, which v3 now uses correctly ([plan:742](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:742)). The older local Tables checkout is not counterevidence. A separate release-activation problem remains below. |
| 23 — Enum limits and identity | **RESOLVED** | Name/symbol limits exist, equality uses fullname plus symbol, and encoding remaps by symbol ([plan:333](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:333), [plan:398](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:398)). |
| 25 — Interoperability oracles | **PARTIALLY RESOLVED — major remains** | Codec capabilities, pins, reverse files, and collection comparison are corrected ([plan:812](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:812)). Fastavro still cannot prove the selected primitive reader-union branch; see new finding 8. |
| 27 — Benchmarks | **RESOLVED** | Cross-language figures are informational. Required gates compare 2.0 with 1.1.2 on the same host and define the thread and codec kernels ([plan:930](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:930)). |
| 28 — Decode schema roles | **RESOLVED** | The positional schema is now the writer schema, with `reader_schema` named separately ([plan:688](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:688)). |

### Round-2 new findings 1–16

| # | Status | Evidence and assessment |
|---:|---|---|
| 1 — Normative union selection | **RESOLVED** | Spec-first matching is the default; Java exact-first behavior is opt-in ([plan:418](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:418)). |
| 2 — Automatic legacy decimal | **RESOLVED** | Little-endian reinterpretation requires an explicit option ([plan:504](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:504)). |
| 3 — Non-record OCF roots | **RESOLVED** | Positional datum decoding, `eachdatum`, non-record `Rows`, and clear `Table` rejection are present ([plan:303](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:303), [plan:497](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:497)). |
| 4 — Negative block headers | **RESOLVED** | Count and size require nonnegative values before arithmetic ([plan:325](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:325), [plan:491](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:491)). |
| 5 — Skip policy | **RESOLVED** | Structural checks and the skipped-string UTF-8 exception are explicit and tested ([plan:295](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:295), [plan:900](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:900)). I accept this disposition because round 2 asked for a precise policy and acceptance-equivalence tests. Documentation must not describe projection as full-file UTF-8 validation. |
| 6 — Tables symbols and specialization | **PARTIALLY RESOLVED — major remains** | Stored `Tables.Schema{nothing,nothing}` prevents name/type parameters, but Tables still converts every field name to `Symbol` ([Tables.jl:480](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/tables-scan/src/Tables.jl:480>), [plan:729](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:729)). Per-schema limits do not stop permanent symbol growth across repeated untrusted files. |
| 7 — Enum equality | **RESOLVED** | Equality and remapping use the symbol string ([plan:398](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:398)). |
| 8 — Error guarantee | **RESOLVED for the original objection** | System, IO, interruption, OOM, mmap, and user-hook exceptions now propagate unchanged ([plan:599](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:599)). The hierarchy and fuzz gate have a separate new defect below. |
| 9 — Raw-byte oracle | **RESOLVED** | Exact equality is limited to deterministic datums; arrays/maps use semantic comparison and independent block-form tests ([plan:855](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:855)). |
| 10 — Codec capability and reverse coverage | **RESOLVED** | The measured matrix and exact Python pins are correct, and fastavro fixtures cover all six standard codecs ([plan:817](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:817), [plan:840](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:840)). |
| 11 — Schema-free encoding | **PARTIALLY RESOLVED — major remains** | `Record`, `EnumValue`, and generic `Fixed` now retain schemas. However, fixed UUID decodes to plain `UUID`, then schema-free encoding infers string UUID; the fixed representation is lost ([plan:404](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:404), [plan:450](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:450)). A bare `NTuple{N,UInt8}` also cannot supply the required fixed fullname, yet type-level inference maps it to fixed ([plan:445](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:445)); fixed names are required by the specification ([spec:169](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:169>)). |
| 12 — Single-object cache | **RESOLVED** | Cache ambiguity and external-store verification are explicit ([plan:549](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:549)). |
| 13 — Schema-less writing | **RESOLVED for the original implicit-inference objection** | Writing now requires a schema; inference is explicit and bounded ([plan:536](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:536)). The returned inference artifact has a separate API problem below. |
| 14 — Big-decimal | **RESOLVED** | It is deferred with the exact unresolved semantics and future vectors recorded ([plan:171](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:171)). |
| 15 — Audit narrative | **RESOLVED** | The counts, stored-schema trigger, null-codec scope, and allocation units are corrected ([plan:31](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:31), [plan:83](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:83)). |
| 16 — Header-only OCF | **RESOLVED** | The Java-compatible extension is recorded and tested ([plan:495](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:495), [plan:1088](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1088)). |

### Amendment decisions

- Finding 5: I accept the withdrawn amendment. Exact representation now wins before conversion.
- Finding 6: I accept the withdrawn amendment. Every timestamp precision now has an exact wrapper.
- Finding 22: I accept the withdrawn API amendment. V3 matches `df4e68c…`. The future activation rule is a separate issue.
- Fixed-value identity: I accept `Avro.Fixed` as the generic fixed representation. Bare tuple and fixed-UUID inference still need correction.
- Green at every phase: I accept the narrow definition at [plan:140](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:140) and [plan:1005](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1005).

## New findings

1. **[major] The promised invalid-schema repair path is not usable for OCF files.**

   **Claim:** Section 3 marks the whole invalid-schema repair section as required, but embedded writer schemas cannot use it.

   **Evidence:** The specification permits repair of invalid simple names, namespaces, fullnames, and defaults through resolution ([spec:271](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:271)). Only `parseschema` has `allow_invalid_names` ([plan:252](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:252)). `Reader`, `Rows`, `Table`, and `inspect` cannot pass that policy while parsing `avro.schema` ([plan:487](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:487), [plan:703](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:703)). Namespace grammar also remains separately strict, and every default is always validated.

   **Recommendation:** Add a narrow writer-schema repair policy to all OCF entry points. Cover invalid simple names, namespaces, fullnames, and field names. Add a separate option for invalid writer defaults, or narrow the section 3 scope claim.

2. **[major] Encode and single-object limits are not reachable through the public API.**

   **Claim:** Internal limits exist, but callers cannot configure them consistently.

   **Evidence:** `max_encode_bytes` and `max_datum_bytes` cover encoding and single-object payloads ([plan:324](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:324), [plan:332](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:332)). Nevertheless, `encode`, `encode!`, `encodesingle`, `Writer`, `write`, and `tobuffer` expose no `limits` keyword ([plan:690](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:690), [plan:709](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:709)). `decodesingle` also omits `union_resolution` and `instants`.

   **Recommendation:** Forward the normal encode/decode keyword contract through all these APIs. Define separate per-datum, per-block, and writer-lifetime output limits. Do not make a streaming writer silently inherit a 64 GiB lifetime ceiling.

3. **[major] User metadata can override reserved OCF metadata.**

   **Claim:** The writer has two authorities for `avro.schema` and `avro.codec`.

   **Evidence:** The specification reserves every `avro.*` property and defines `avro.schema` and `avro.codec` ([spec:467](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:467)). V3 rejects reserved keys except those two ([plan:525](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:525)). No precedence or equality rule exists.

   **Recommendation:** Reject every user-supplied `avro.*` key. Generate schema and codec metadata only from the authoritative constructor arguments.

4. **[major] Strict OCF validation does not require complete codec-input consumption.**

   **Claim:** A valid compressed prefix followed by extra compressed bytes can pass if the adapter stops at the first end marker.

   **Evidence:** The declared block data is entirely the codec-compressed serialized objects ([spec:483](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:483)). V3 checks decompressed size and datum exhaustion, but does not require clean codec EOS plus full consumption of the compressed payload ([plan:487](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:487), [plan:543](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:543)).

   **Recommendation:** Make every adapter report clean EOS and consumed input. Add truncated-stream and valid-stream-plus-suffix cases for deflate, snappy, bzip2, xz, and zstandard.

5. **[major] `minsize(schema)` is undefined for recursive schemas.**

   **Claim:** The count-bound optimization can recurse forever or reject a valid recursive schema.

   **Evidence:** Collection checks depend on `remaining_bytes ÷ minsize(item_schema)` ([plan:350](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:350)). The same plan requires recursive schemas and `LongList` through `PlanRef` ([plan:364](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:364)). No fixed-point or active-cycle rule is given.

   **Recommendation:** Specify a memoized, cycle-safe lower bound. Returning zero on an active recursion edge is safe. Test direct recursion, mutual recursion, a nullable recursive union, and recursion with no finite datum.

6. **[major] The Scan variant can activate later against an API it was never tested with.**

   **Claim:** An existing Avro 2.0 installation can gain a new and incompatible public keyword solely because Tables is upgraded.

   **Evidence:** Avro accepts Tables 1.13-compatible future releases and includes Scan code whenever `isdefined(Tables, :Scan)` is true ([plan:742](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:742), [plan:967](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:967), [plan:1020](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1020)). The code is validated only against `df4e68c…`. The cross-package gate uses registered Arrow 2.x, not the sibling Arrow 3 rewrite ([plan:923](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:923)); that checkout still calls `Tables.bind` ([Arrow table.jl:445](/Users/jacob.quinn/.julia/dev/Arrow/src/table.jl:445)).

   **Recommendation:** If Scan is unregistered at RC, do not ship dormant Scan code in 2.0. Add it in a later Avro release with a minimum known-compatible Tables version. Add an exact Tables-plus-Arrow-3 candidate smoke test.

7. **[major] The all-codec test environment omits both weak codec dependencies.**

   **Claim:** The required bzip2/xz gates cannot run in the declared test environment.

   **Evidence:** `CodecBzip2` and `CodecXz` are weak dependencies, but the explicit `test/Project.toml` list omits both ([plan:967](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:967)). Phase 4a requires Java and fastavro interchange for every codec ([plan:1014](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1014)).

   **Recommendation:** Put both packages in the test/interop environment with exact compatible versions. Explicitly load each extension before its codec matrix.

8. **[major] The spec-union oracle cannot observe the fact it is meant to verify.**

   **Claim:** Fastavro does not expose whether `long` or `int` was selected for a primitive reader union.

   **Evidence:** V3 assigns spec-policy expectations to fastavro where Java differs ([plan:822](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:822)). The normative rule selects the first matching reader branch ([spec:716](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:716)). The pinned fastavro probe produced:

   ```text
   writer="int", reader=["long","int"] => 7 int
   ```

   Python’s one `int` type erases the selected branch.

   **Recommendation:** Commit spec-derived expectations that assert Julia branch index and result type directly. Use fastavro only where its result preserves branch identity.

9. **[major] `inferschema` consumes a one-shot source and discards the materialized data.**

   **Claim:** The recommended fallback cannot then write the source it inspected.

   **Evidence:** `Avro.inferschema(table)` is explicitly materializing but returns only `Schema` ([plan:536](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:536), [plan:677](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:677)). Tables sources are not required to be replayable.

   **Recommendation:** Return a bounded replayable artifact such as `(schema, materialized_table)`, or provide an explicit `write(...; infer=true)` operation that owns and writes the bounded materialization. State source-consumption semantics.

10. **[major] The error hierarchy and fuzz gate are internally inconsistent.**

    **Claim:** Writer failures are classified as decode failures, while valid schema failures cannot pass the fuzz gate.

    **Evidence:** `LimitError`, `CodecError`, and `UnsupportedCodecError` subtype `DecodeError`, although encoders and writers also use limits and codecs ([plan:599](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:599)). The mutation gate requires every fixture to yield `DecodeError` or success ([plan:893](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:893)), but mutation of an OCF’s embedded schema correctly yields `SchemaError`.

    **Recommendation:** Make shared limit/codec errors direct `AvroError` subtypes, or split encode and decode variants. Require `AvroError` for whole-file/schema/JSON fuzzing. Reserve `DecodeError` for raw datums under a fixed valid schema.

11. **[major] RC-ready does not require full validation of the exact unpinned candidate.**

    **Claim:** The plan can remove temporary pins and change the Scan surface after the last full matrix.

    **Evidence:** Phase 5 validates development variants ([plan:1018](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1018)). RC-ready then removes `[sources]` and applies the Scan decision, but does not rerun the complete suite from the resulting source archive ([plan:1025](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1025)).

    **Recommendation:** After all pin and compat changes, instantiate a clean environment from the exact RC archive. Rerun supported Julia versions, codecs, interop, fuzz, docs, and cross-package tests. Require the release tag tree to equal that tested tree.

12. **[minor] `fromjson(strict=true)` accepts non-JSON tokens while `strict=false` is undefined.**

    **Claim:** The public mode flag has no coherent contract.

    **Evidence:** The API defaults to `strict=true`, but accepts bare `NaN`, `Infinity`, and `-Infinity` ([plan:565](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:565)). V3 emits the quoted Java forms and does not define what `strict=false` changes.

    **Recommendation:** Accept only quoted forms in strict mode. Put bare tokens behind permissive mode, record the extension, or remove the unused keyword.

13. **[minor] `comparebytes` does not require exact consumption of either datum.**

    **Claim:** The comparator can ignore trailing bytes.

    **Evidence:** Top-level decode and SOE explicitly require exact consumption ([plan:303](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:303), [plan:551](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:551)); `comparebytes` does not ([plan:590](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:590)).

    **Recommendation:** Require exactly one complete datum in each buffer. Add trailing-byte tests on both sides.

14. **[minor] The DataAPI metadata contract is incomplete.**

    **Claim:** Naming only `DataAPI.metadata` does not implement the read interface.

    **Evidence:** DataAPI requires `metadatasupport`, `metadatakeys`, and keyed `metadata` methods ([DataAPI.jl:312](/Users/jacob.quinn/.julia/packages/DataAPI/atdEM/src/DataAPI.jl:312)). V3 only promises `DataAPI.metadata` ([plan:729](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:729)). Arrow implements the full set ([Arrow table.jl:95](/Users/jacob.quinn/.julia/dev/Arrow/src/table.jl:95)).

    **Recommendation:** Specify and test the complete read-only DataAPI interface.

15. **[minor] Decimal edge behavior is incomplete.**

    **Claim:** The plan omits the default scale and empty-byte rule.

    **Evidence:** The specification defaults an absent decimal scale to zero ([spec:798](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:798)). V3 does not state this or say whether an empty bytes payload is rejected.

    **Recommendation:** State `scale=0` when absent. Add empty payload, `00`, sign-extension, maximum-precision, and one-digit-overflow vectors.

16. **[minor] A valid timestamp can be reported as malformed input.**

    **Claim:** Failure of an optional native conversion is not an invalid Avro datum.

    **Evidence:** Every `Int64` timestamp is valid, but `instants=:datetime` range failure becomes `DataError` ([plan:411](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:411)); `DataError` is defined as malformed input ([plan:610](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:610)).

    **Recommendation:** Raise a distinct conversion/range error, or retain the exact wrapper.

17. **[minor] `eachblock` does not define byte ownership.**

    **Claim:** Callers cannot know whether retained block bytes remain valid after iteration advances.

    **Evidence:** `eachblock` returns `(count, bytes)`, while streaming keeps one block buffer resident and borrowed views are deferred ([plan:498](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:498), [plan:1078](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1078)).

    **Recommendation:** State that returned bytes are owned copies, or expose an explicitly ephemeral block view with a precise lifetime.

18. **[minor] Writer option validation is not specified.**

    **Claim:** Invalid sync markers and block sizes have undefined behavior.

    **Evidence:** The writer accepts `sync`, `block_bytes`, and codec `level` without stated validation ([plan:525](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:525)). The OCF sync marker is exactly 16 randomly generated bytes ([spec:457](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:457)).

    **Recommendation:** Require a 16-byte copied sync value, `block_bytes > 0`, codec-specific level validation, and random generation when `sync=nothing`.

19. **[minor] Some benchmark and compile gates use unstable measurement interfaces.**

    **Claim:** The gates are not yet reproducible across Julia 1.10–1.12.

    **Evidence:** Method/native-code accounting uses `Core.Compiler` caches or `jl_native_code`, and allocation counts use `Base.gc_num` ([plan:383](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:383), [plan:914](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:914)). Load-time limits use one unspecified `@time` measurement ([plan:960](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:960)).

    **Recommendation:** Define version-specific supported measurement scripts, cold-process repetition, aggregation, and fallback behavior. Keep unavailable internal counters informational rather than silently passing.

## Non-blocking follow-ups (if any)

New findings 12–19 are individually non-blocking. They should be handled during implementation. They do not change the verdict because blocker and major issues remain.

The header-only extension, skipped-string policy, RPC deferral, big-decimal deferral, append deferral, borrowed-view deferral, and writer-side parallel-compression deferral are acceptable as recorded.

## Milestone and gate assessment

| Phase | Assessment |
|---|---|
| 0 | Add CodecBzip2 and CodecXz to the test environment. Keep the exact Tables pin. Record a real Arrow 3 candidate smoke, not only registered Arrow 2. |
| 1 | Require transitive freezing. Give hash/canonical caches explicit ownership and locking. Add reachable invalid-writer-schema repair. |
| 2 | Replace the unsafe work defaults; add `max_blocks`, source-byte accounting, and encode work limits. Close the collection type set. Define cycle-safe `minsize`. Repair schema-free UUID/fixed inference and the error hierarchy. |
| 3 | Use direct spec-derived union-branch expectations. Require exact input consumption in `comparebytes`. |
| 4a | Add codec EOS/input-consumption tests, reserved-metadata rejection, close-time failure cleanup, writer option validation, and usable inference output. |
| 4b | Either require trusted-name opt-in for `Avro.Table` or accept a caller-owned cumulative symbol-admission budget. Per-file limits do not stop process-lifetime symbol growth. |
| 4c | Use an atomic minimum failure index. Continue all lower-index blocks and cancel only higher ones. Peak-memory tests must include per-block chunks plus final assembly copies. |
| 4d | If Scan is not registered, defer the integration instead of dormant future activation. If it is registered, set a minimum Tables version and run exact Arrow 3 compatibility tests. |
| 5 / RC | Rebuild and rerun the complete matrix after removing pins and applying the final Scan decision. Validate the exact source archive and tag tree. |

Specification coverage is now strong for normal schema grammar, names, defaults, unions, binary encoding, JSON encoding, single-object encoding, order, resolution, canonical form, fingerprints, OCF roots/codecs, and all non-deferred logical types. Invalid-schema repair and several strict OCF boundaries remain incomplete. Protocol declaration, handshake, framing, call format, and HTTP transport are explicitly deferred with credible future gates; I accept that scope as a data-format-focused 2.0, provided the package does not claim full Avro RPC support.

Section 2.2 is now accurate. `src/` and `test/` remain unchanged from the pinned `0c7be10` source, and the four round-2 audit corrections are reflected correctly. I found no new audit-table discrepancy.

The benchmark ratios are now substantially fairer and measurable. The unstable internal counters need the minor protocol correction above. The larger problem is that the compile gate cannot pass until the generic value set is truly closed.

## Verdict

DRAFT v3 is substantially better, big dawg, but it is not implementation-ready. Unsafe default work limits, shallow immutability, schema-derived union specialization, nondeterministic parallel errors, cumulative symbol interning, incomplete schema-free identity, and non-executable release/conformance gates remain.

Assumptions made:

- The pinned 1.13-SNAPSHOT specification is authoritative.
- `df4e68c…` is the authoritative Tables.Scan revision for this review.
- Declared fixtures and harnesses are implementation deliverables, not review preconditions.
- RPC and big-decimal can remain deferred.

Decisions made without user direction:

- I accepted the documented skipped-string UTF-8 policy.
- I treated untrusted-input safety as a repeated-operation guarantee unless an API explicitly requires trusted input.
- I used the package-review checklist to keep PR-ready, RC-ready, and release-ready evidence separate.

Validation performed:

- I inspected DRAFT v3, both prior reviews and responses, the pinned specification, unchanged 1.1.2 source/tests, the authoritative Tables pin, Arrow 3, JSON.jl, StructUtils, DataAPI, the generated corpus, and Java/Python harness material.
- I confirmed the pinned fastavro reader returns `7 int` for writer `int` and reader `["long","int"]`, which cannot identify the selected branch.
- I confirmed `src/` and `test/` have no diff from `0c7be10`.
- I created or modified no files.

VERDICT: REVISE
