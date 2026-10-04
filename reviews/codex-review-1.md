# Codex review round 1

## Findings

1. **[blocker] The limits do not provide the promised denial-of-service protection.**

   **Claim:** The default limits permit very large allocations and unbounded cumulative work.

   **Evidence:** The plan permits `max_collection_length = 2^31-1`, calls `sizehint!` from an untrusted count, permits 1 GiB per decompressed block, and lets `decode(schema, io)` read to EOF ([plan:257](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:257), [plan:268](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:268)). An array of nulls or empty records can describe billions of values in very few bytes. Parallel tasks multiply the per-block limit. No limits cover total rows, total values, total allocations, total metadata bytes, in-flight task memory, schema nodes, protocol frames, or encoding. The spec explicitly says blocked arrays permit processing values larger than memory ([spec:373](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:373>)); materializing attacker-sized counts defeats that design.

   **Recommendation:** Add cumulative per-operation budgets for allocated bytes, values, rows, total decompressed bytes, metadata bytes, schema nodes, names, fields, union branches, enum symbols, protocol frames, and active worker memory. Use checked arithmetic. Cap initial `sizehint!` to a small value. Add encode-side limits and a bounded datum-from-IO API. Use conservative defaults.

2. **[blocker] Schema and protocol depth checks occur after the dangerous parse.**

   **Claim:** `max_schema_depth` does not protect the JSON parser.

   **Evidence:** The plan first calls eager `JSON.parse` and only then walks the result with `ParseContext.depth` ([plan:215](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:215)). A deeply nested 16 MiB document can exhaust parser stack or memory before Avro applies a limit. JSON.jl also overwrites duplicate keys unless configured otherwise. That can erase duplicate protocol messages, which must be rejected ([spec:539](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:539>)). Eager parsing also cannot retain the “original JSON text” promised for defaults ([plan:211](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:211)).

   **Recommendation:** Use bounded token or lazy traversal, or run a safe lexical depth and token-count scan before parsing. Set `duplicate_keys=:error`. Apply the same policy to schemas, protocols, and datum JSON. Either retain source spans or drop the exact-text preservation promise.

3. **[blocker] Recursive schemas cannot use the proposed plan-tree and generic-type architecture.**

   **Claim:** The architecture cannot terminate for a valid recursive record.

   **Evidence:** The spec’s `LongList` refers to itself ([spec:99](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:99>)). The plan creates a cyclic schema graph but calls codec plans immutable trees and maps small records to `NamedTuple` ([plan:221](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:221), [plan:275](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:275), [plan:314](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:314)). A recursive `NamedTuple` type is not finite. A resolving plan can also recurse forever while constructing a writer-reader pair.

   **Recommendation:** Add two-pass `PlanRef` or fixpoint nodes keyed by schema identity. Memoize writer-reader pairs before descending. Use a stable dynamic `Avro.Record` for generic recursive records. Reserve static record plans for an explicit target type whose recursion is representable.

4. **[blocker] The union-default rule is wrong.**

   **Claim:** The plan incorrectly requires a union default to match branch zero.

   **Evidence:** The plan says “union-first-branch rule” ([plan:136](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:136)). The pinned spec says the default corresponds to “the first schema that matches in the union” ([spec:81](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:81>)). Apache Python 1.12.2 accepted `"ok"` as the default for `["null","string"]`. The spec also says a default does not make the field optional during encoding.

   **Recommendation:** Test branches in declaration order and retain the selected branch index. Store original semantic JSON separately from the converted value. Require every record field during encoding. Create a fresh array, map, or record default for each resolved record; do not reuse one mutable object.

5. **[blocker] Generic union decoding loses the encoded branch.**

   **Claim:** Decoding a general union to a bare Julia value is not reversible.

   **Evidence:** Binary unions carry an explicit branch index ([spec:382](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:382)). JSON encoding must identify the selected branch ([spec:399](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:399)). Sort order compares union branch first ([spec:440](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:440)). The plan instead returns `Union{juliatype.(branches)...}` and uses `Avro.Branch` only when a caller encodes ([plan:312](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:312)). Two named records, enums, or fixed types can map to identical Julia values.

   **Recommendation:** Return a tagged `Avro.UnionValue(index, value)` whenever branch mappings are not injective. Preserve tags through defaults, resolution, JSON conversion, sorting, and re-encoding. Reserve bare `Union{Missing,T}` for cases where identity is provably unambiguous.

6. **[blocker] The time and timestamp model intentionally loses valid data.**

   **Claim:** `Dates.DateTime` cannot represent the required microsecond and nanosecond values.

   **Evidence:** The spec defines exact microsecond time values and exact millisecond, microsecond, and nanosecond timestamp ticks ([spec:865](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:865), [spec:875](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:875), [spec:885](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:885)). The plan maps all timestamps to millisecond-resolution `DateTime` and documents truncation ([plan:319](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:319)). It also maps global instants and local timestamps to the same timezone-naive type. This makes the promised Java decode/re-encode gate impossible for non-aligned values.

   **Recommendation:** Use exact `Int64`-backed, unit-tagged `Timestamp`, `LocalTimestamp`, and time values. Make conversion to `DateTime` explicit and lossy. Reject non-aligned `Dates.Time` writes unless the caller selects a rounding policy. Replace decision 14.4; do not infer a global instant from a naive `DateTime`.

7. **[blocker] The RPC fixtures do not test RPC wire interoperability.**

   **Claim:** The Phase 6 acceptance gate is based on ordinary container files, not handshake or call frames.

   **Evidence:** The named `interop/rpc` files start with `4f 62 6a 01`, the OCF magic. Apache’s `test_rpc_interop.sh:60-72` uses them as application request and expected-response datum files for live RPC tools. They are not wire captures. The proposed gate therefore tests none of framing ([spec:602](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:602)), handshake states ([spec:620](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:620)), or call metadata/error responses ([spec:664](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:664)).

   **Recommendation:** Defer RPC from 2.0, or add real wire tests. Cover `BOTH`, `CLIENT`, and `NONE`; handshake retry; multiple frames; invalid frame lengths; metadata; declared and system errors; empty-message ping; one-way calls; and protocol evolution. Require at least one live Apache client/server interchange.

8. **[blocker] Two core Julia declarations cannot work on the stated compatibility floor.**

   **Claim:** The planned error hierarchy and public declarations are invalid.

   **Evidence:** The plan calls `DecodeError` concrete and then declares `LimitError <: DecodeError` ([plan:420](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:420)). Julia cannot subtype a concrete type. It also plans literal `public` declarations while supporting Julia 1.10 ([plan:436](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:436), [plan:556](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:556)). Julia 1.10 cannot parse that syntax.

   **Recommendation:** Use an abstract decode-error category or sibling concrete errors with shared context. Emit `public` through a version-gated `Core.eval(Expr(:public, ...))`. Add a Julia 1.10 parse/load test in Phase 0.

9. **[major] PCF equality is not semantic schema equality, and the graph is not immutable.**

   **Claim:** The proposed `==` and `hash` can report materially different schemas as equal.

   **Evidence:** PCF strips defaults, aliases, logical types, docs, and custom properties ([spec:739](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:739)). Plain `int` and logical `date` would therefore compare equal although `juliatype` differs. Reader schemas with different defaults would also compare equal. The proposed graph contains mutable vectors, dictionaries, and `RecordSchema`, while its PCF hash is cached ([plan:180](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:180), [plan:222](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:222)).

   **Recommendation:** Provide an explicit `parsing_equivalent` operation. Make `==` structural and semantic, or use identity. Freeze schemas after construction and keep mutable parser state private. Do not expose mutable objects that can invalidate hashes or plans.

10. **[major] Untrusted schemas can cause excessive Julia compilation.**

    **Claim:** The generic path specializes on runtime schema structure.

    **Evidence:** `RecordPlan{names,Ps}`, generated `NamedTuple` types, tuple-unrolled field plans, and `juliatype(schema)` create distinct compiler work for each schema ([plan:277](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:277), [plan:295](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:295)). A 32-field threshold also changes the decoded public value type when a schema evolves to 33 fields.

    **Recommendation:** Use stable dynamic plans and `Avro.Record` for generic or untrusted schemas at every width. Specialize only when the caller supplies a trusted target `T`. Add 100–1000 heterogeneous-schema tests for cold compile time, method instances, invalidations, native-code size, and memory.

11. **[major] The binary decoder contract omits required validation and overstates buffer safety.**

    **Claim:** Bounds checks alone do not make the decoder conforming or memory-safe.

    **Evidence:** Avro booleans must be exactly byte 0 or 1 ([spec:302](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:302)). Strings must be UTF-8 ([spec:320](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:320)). The plan uses `unsafe_string` without UTF-8 validation and accepts `AbstractVector{UInt8}` while assuming contiguous, one-based, stable memory ([plan:235](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:235), [plan:244](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:244)). Sized array/map blocks must contain exactly the declared bytes ([spec:360](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:360)); the plan does not require exact child-buffer exhaustion or address `typemin(Int64)` count negation.

    **Recommendation:** Restrict buffers to validated one-based, unit-stride storage or copy them. Validate UTF-8 for strings and map keys. Reject non-0/1 booleans. Decode sized blocks through a bounded child decoder and require exact exhaustion. Add checked negation and addition. Define whether top-level datum decode rejects trailing bytes; strict rejection should be the default.

12. **[major] Name, namespace, and alias handling is too broad in legacy mode.**

    **Claim:** `validate_names=false` can be read as disabling more validation than the spec permits.

    **Evidence:** Names must follow the grammar and be defined before use ([spec:182](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:182), [spec:260](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:260)). Aliases may contain invalid name syntax, but remain subject to uniqueness constraints ([spec:269](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:269)). The plan does not state that duplicate fields, symbols, fullnames, primitive redefinitions, and forward references remain errors in relaxed mode.

    **Recommendation:** Rename the option to `allow_invalid_name_syntax`. Continue to enforce all structural and uniqueness rules. Store invalid raw aliases without passing them through a valid-name constructor. Test qualified names, inherited namespaces, alias collisions, primitive names, and define-before-use.

13. **[major] The JSON datum contract is incomplete.**

    **Claim:** “Union wrapping by type name” is not precise enough for Java interoperability.

    **Evidence:** Apache Java emitted `{"u":{"a.Foo":{"x":1}}}` for a namespaced record union, so the key is the fullname. The spec requires a selected union type name ([spec:399](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:399)). The plan does not define missing, extra, or duplicate record fields; exact numeric range checks; one-key union wrappers; byte/fixed code points above 255; or permissive versus strict handling of non-finite floats ([plan:401](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:401)).

    **Recommendation:** Add a complete JSON rule table and negative matrix. Use fullnames for named union branches. Require exactly one wrapper member. Reject missing and extra record fields during encoding. Reject code points above 255 for bytes/fixed. Match Java’s string forms for non-finite floats by default and document any permissive fastavro input mode.

14. **[major] Schema resolution contains a direct decimal contradiction and incomplete conversion rules.**

    **Claim:** The plan both rejects and permits mismatched recognized decimals.

    **Evidence:** It says decimal precision and scale must match, then says nonmatching logical types fall back to underlying-type resolution ([plan:331](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:331)). The spec says two decimals match only when both precision and scale match ([spec:813](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:813)). Resolution must also preserve union tags, allocate a fresh mutable reader default, validate UTF-8 during `bytes → string`, and memoize recursive writer-reader pairs.

    **Recommendation:** Make two recognized decimals with different scale or precision a resolution error. Define recognized-logical versus plain-underlying behavior separately. Specify exact conversion errors and paths. Add cyclic, ambiguous-union, mutable-default, and invalid-UTF-8 resolution cases.

15. **[major] The logical-type plan is incomplete beyond timestamps.**

    **Claim:** Several normative rules have no executable design.

    **Evidence:** The spec requires:

   - Decimal: positive precision, nonnegative scale, big-endian two’s-complement bytes, fixed-size precision bounds, and equal precision/scale for resolution ([spec:789](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:789)). The plan omits explicit negative-scale and encoded-value precision checks.
   - Big-decimal: an inner Avro byte array followed by an Avro int inside the outer bytes ([spec:827](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:827)). The plan gives only a Julia type.
   - UUID: string or named fixed-16 values must conform to RFC-4122 ([spec:829](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:829)). Canonical validation is not specified.
   - Date and time: checked native ranges and time-of-day bounds are absent.
   - Duration: the proposed three `UInt32` fields and little-endian layout agree with the spec ([spec:895](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:895)).

   **Recommendation:** Add a per-logical-type contract covering schema validation, binary boundaries, JSON/default behavior, resolution, native conversion, and Java/fastavro capability. Defer big-decimal unless its nested binary format has independent Java-backed vectors.

16. **[major] Single-object schema lookup trusts a probabilistic fingerprint as identity.**

    **Claim:** A `Dict{UInt64,Schema}`-style cache can return the wrong schema after a collision.

    **Evidence:** The spec states CRC-64 has a collision probability and gives no security guarantee ([spec:751](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:751), [spec:754](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:754)). The proposed `SchemaCache` has no collision or size policy ([plan:390](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:390)).

    **Recommendation:** Retain PCF bytes in each fingerprint bucket. Reject a second non-equivalent registration rather than overwrite. Bound cache entries and bytes. Document that fingerprints are identifiers, not authentication. Require exact payload consumption after single-object decoding.

17. **[major] Sort order is listed but has no executable scope.**

    **Claim:** The plan calls sort order optional but gives it no API, phase, or gate.

    **Evidence:** The spec defines ordering for every type, record `order`, union branch order, and an error for maps ([spec:426](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:426)). The public API does not list `Avro.compare`, and no milestone owns it.

    **Recommendation:** Include sort order in a named phase for a “leading implementation.” Define separate value and encoded-datum APIs. Test every type, ignored record fields, nested maps, unions, recursion limits, NaNs, and signed zero against a recorded Java policy.

18. **[major] OCF compatibility is unsafe by default, and writer lifecycle is under-specified.**

    **Claim:** The new API accepts arbitrary trailing block bytes unless `strict=true`.

    **Evidence:** OCF data blocks consist of count, encoded size, encoded objects, and sync ([spec:483](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:483)). Apache Java rejects the old Avro.jl padding. The plan nevertheless defaults `strict=false` ([plan:366](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:366), [plan:495](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:495)). Append mode also lacks locking, partial-tail truncation policy, atomicity, and crash recovery.

    **Recommendation:** Make strict OCF validation the new default. Put exact Avro.jl 1.x null-codec tolerance behind `legacy=:avrojl1`, optionally enabled by the deprecated shim. Use atomic path replacement for new files. Defer append until locking, truncation, abort, and crash semantics are specified.

19. **[major] Borrowed bytes and mmap ownership have no safe lifetime contract.**

    **Claim:** `bytes=:view` conflicts with “one decompressed block resident” and deterministic close.

    **Evidence:** A view can retain a whole mmap or decompressed block for one small value ([plan:306](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:306)). The plan does not state whether path-owned files, caller-owned IO, mappings, or decompressed buffers remain alive after `Table`, `Rows`, or `Reader` close. Concurrent truncation of a mapped file can also fault outside Julia’s error model.

    **Recommendation:** Defer borrowed bytes from 2.0, or introduce explicit owner regions and revocation rules. Define path-owned versus caller-owned resources, idempotent close, use-after-close behavior, and when a copied table releases its mapping.

20. **[major] Parallel decoding has no complete correctness algorithm.**

    **Claim:** `errormonitor` does not join tasks, propagate failures, cancel siblings, or protect partially initialized columns.

    **Evidence:** The plan preallocates from block counts and starts tasks that write disjoint ranges ([plan:369](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:369)). Filtered output ranges are not known before qualifying counts are computed. An error can leave reference arrays or isbits-union tag arrays partly uninitialized. `ColumnBuilder.push!` also conflicts with direct disjoint-range writes.

    **Recommendation:** Specify a structured two-stage algorithm with checked prefix sums, bounded worker reservations, `@sync`/fetch-based error propagation, cooperative cancellation, and deterministic first-cause selection. Prefer per-block builders followed by ordered assembly until direct writes are proved correct under GC stress and worker failure.

21. **[major] The StructUtils fast path cannot honor the promised customization semantics.**

    **Claim:** Static direct construction and arbitrary `make`, `choosetype`, `lift`, defaults, and custom styles cannot both always win.

    **Evidence:** The plan promises both ([plan:339](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:339)). Current StructUtils gives custom `make` dispatch precedence at `StructUtils/src/StructUtils.jl:1051-1094`. Value-dependent construction and dynamic defaults cannot be compiled into a fixed field constructor safely.

    **Recommendation:** Define an `AvroStyle` namespace, similar to JSON’s style wrapper. Add a narrow eligibility check for a plain-DTO fast route. Send custom hooks, dynamic defaults, broad unions, and complex constructors through a semantic fallback. Test against the exact supported StructUtils head.

22. **[major] The Tables.Scan design targets a stale and internally mixed API.**

    **Claim:** The plan matches neither the pinned Scan revision nor the supplied current branch.

    **Evidence:** The plan names `df4e68c…` ([plan:532](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:532)); local `jq/scan` is `6be83de366499e7a428ecea1f6adab1c4f8eeff0`. Current Scan rejects column-to-column comparisons and rejects `OpNode` during bind (`Tables/src/scan.jl:106-117`, `213-228`, `407-419`). They cannot simply become residuals. Scan type conversions have actual-value and no-op subtype rules (`scan.jl:569-575`). `select=()` also needs an authoritative row count because generic zero-column Tables fallbacks report zero rows. The plan uses `DataAPI.metadata` but omits DataAPI from dependencies ([plan:524](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:524), [plan:674](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:674)).

    **Recommendation:** Pin the exact current Scan SHA. Reject unsupported nodes before decode. Differential-test names, order, eltypes, schema, and row count, including `select=()`, empty records, `validate=false`, missing-only filters, offset overflow, and supertype overrides. Add DataAPI as a direct dependency with compat.

23. **[major] `Symbol` is an unsafe and lossy generic enum representation.**

    **Claim:** Untrusted schemas can permanently intern attacker-selected symbols, and the value loses its named-schema identity.

    **Evidence:** The plan selects `Symbol` as the default ([plan:309](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:309)). Two named enum branches with the same symbol then become indistinguishable. Schema-controlled symbols also grow Julia’s process symbol table.

    **Recommendation:** Use `Avro.EnumValue(schema-or-index, symbol-index)` or `String` for generic decoding. Keep `Symbol` and `Base.Enum` as trusted typed targets. Replace decision 14.2 and add enum-count/name-byte limits.

24. **[major] Protocol declarations and MD5 hashing are incomplete even before transport.**

    **Claim:** The planned protocol model drops information used by Apache Java.

    **Evidence:** `RecordSchema` requires a fullname, but message requests are anonymous-record-equivalent ([plan:196](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:196), [spec:552](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:552)). The proposed `Protocol` and `Message` shapes omit custom properties present in pinned `simple.avpr`. Apache Java hashes the UTF-8 bytes of its deterministic protocol JSON printer. The plan only says “hash of the protocol JSON text” ([plan:408](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:408)). It also omits the one-way constraints, empty-name ping, and stateless one-way response rule.

    **Recommendation:** Add a distinct request-schema type. Preserve protocol, message, type, and field properties. Define a deterministic Java-compatible printer and hash exactly those transmitted bytes. Validate one-way declarations and test ping, errors, metadata, and request/response resolution.

25. **[major] The interoperability gates contain factual and methodological errors.**

    **Claim:** Passing the proposed harness would not prove bidirectional interoperability.

    **Evidence:**

   - The plan says Java avro-tools lacks xz ([plan:584](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:584)). The jar contains `XZCodec`; `recodec --codec xz --level 6 ... | count` returned 5.
   - The CI installs only `fastavro`. In the supplied environment, snappy requires `cramjam` or another snappy extra.
   - Two Java `recodec --codec null` runs produced different SHA-256 values because OCF sync and framing are regenerated. Byte comparison after recodec is therefore invalid ([plan:592](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:592)).
   - The spec snapshot is 1.13.0-SNAPSHOT while Java, Python avro, and fastavro are 1.12.2. Their logical-type capabilities differ.
   - The matrix lacks explicit empty files, zero-row blocks, many small blocks, user metadata, unknown codecs, named-branch collisions, negative collection blocks, codec bombs, corrupt checksums, and boundary logical values.

   **Recommendation:** Pin `avro==1.12.2`, `fastavro==1.12.2`, `cramjam`, and a checksummed jar. Exercise Java xz. Compare decoded datum sequences, schema, metadata, counts, and codec invariants, not rewritten OCF bytes. Maintain a versioned feature-capability matrix. Use direct spec vectors or a Java build from the pinned Apache SHA for post-1.12 features.

26. **[major] The property and fuzz gates can pass after data loss or fail to contain a hang.**

    **Claim:** Several tests use the implementation under test as its own oracle.

    **Evidence:** `parseschema(json(s)) == s` uses the proposed PCF equality, so lost defaults, aliases, properties, and logical types remain invisible. `decode(encode(x))` cannot detect a matching encoder/decoder bug. `isequal` does not verify a NaN payload. A Julia task timeout cannot safely terminate a hung decoder or native codec. `@allocated` does not include codec-native memory or RSS ([plan:610](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:610), [plan:618](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:618)).

    **Recommendation:** Assert all schema attributes directly. Compare float bit patterns. Add independent Java/Python vectors and negative accept/reject oracles. Run mutations and bombs in subprocesses with wall, CPU, and RSS limits. Use per-case stable RNG objects, persist failing inputs and seeds, and add shrinking.

27. **[major] The benchmark baseline is wrong, and several targets compare different work.**

    **Claim:** The acceptance targets are not reproducible or portable.

    **Evidence:** `probe/bench.jl:19` uses `@allocated`; the reported 1,376 value is bytes, not allocation count. A warmed rerun found 1,376 bytes and 38 allocations. Current code produced a 32,279,364-byte null-codec output, not the claimed 23.1 MB. `bench2.avro` is 23,100,363 bytes and valid, so it does not match the current null writer. Avro 1.x and 2.x share a package UUID and cannot be loaded side by side in one Julia session. Fastavro dict rows, Julia columns, Java `count`, and Java `tojson` perform different work. Absolute 8-thread and subsecond requirements are hardware-sensitive ([plan:643](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:643)).

    **Recommendation:** Preserve raw logs and exact commits. Use separate pinned processes driven by one harness. Compare the same source layout and semantic result. Separate encode, file IO, decode, indexing, and materialization. Record cold/warm results, CPU, threads, block size/count, codec level, bytes, and peak RSS. Make absolute numbers tracked targets on a named host; gate regressions by ratios and tolerances.

28. **[major] The migration claims omit existing nonconforming data and unstable API changes.**

    **Claim:** “Nothing else 1.x wrote is non-conformant” is false.

    **Evidence:** Current decimal schema regeneration always uses fixed size 16 and current logical encoding uses native-endian behavior (`src/types/logical.jl:9-23`). Duration uses signed/native representation (`src/types/logical.jl:233-260`). The current container path discards the writer schema and regenerates a lossy schema from Julia types (`src/tables.jl:198`, `src/types/binary.jl:74-75`). The plan also changes nested record type at 33 fields and gives `decode(schema, ...; writer=...)` an ambiguous writer/reader-schema contract.

    **Recommendation:** Document every known 1.x data defect. Add a scanner and rewrite recipe. Keep strict new APIs and place tolerance in deprecated shims. Use stable generic record types. Rename datum arguments to `writer_schema` and `reader_schema`. Test all shims against actual 1.x files, not only new round trips.

29. **[minor] The plan contains a literal NUL byte.**

    **Claim:** The Markdown file is treated as binary by standard tools.

    **Evidence:** The bytes/fixed JSON range at [plan:403](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:403) contains a literal NUL. `file` reports the plan as data and `rg` reports a binary match.

    **Recommendation:** Replace it with textual `U+0000–U+00FF` or escaped `\\u0000–\\u00ff`.

### Specification coverage ledger

| Spec area | Assessment |
|---|---|
| Schema grammar and metadata | Partial; eager parsing, duplicate keys, limits, and exact default JSON are unresolved. Findings 1, 2. |
| Names, namespaces, aliases | Mostly identified, but relaxed validation is over-broad. Finding 12. |
| Defaults | Incorrect union rule and unsafe mutable-value reuse. Finding 4. |
| Unions | Generic branch identity is lost. Finding 5. |
| Binary encoding | Missing UTF-8, strict boolean, exact sized-block, buffer, and trailing-data contracts. Finding 11. |
| JSON encoding | Under-specified named wrappers, fields, ranges, bytes/fixed, and non-finite floats. Finding 13. |
| Single-object encoding | Marker/layout are planned; cache collision and consumption rules are not. Finding 16. |
| Sort order | Not executable as scoped. Finding 17. |
| Object container files and codecs | Broad coverage, but unsafe compatibility default, lifecycle, and interop gates remain. Findings 18–20, 25. |
| Protocol declaration | Anonymous request shape, properties, one-way validation, and MD5 text are incomplete. Finding 24. |
| Framing, handshake, and calls | The cited fixtures do not test them. Finding 7. |
| Schema resolution | Broad list, but decimals, recursive pairs, tags, defaults, and UTF-8 conversions remain wrong or ambiguous. Finding 14. |
| Parsing Canonical Form | Algorithm scope is plausible; use as general equality is wrong. Finding 9. |
| Fingerprints | Algorithms are named; collision and security handling are missing. Finding 16. |
| Decimal | Partial and internally contradictory. Findings 14, 15. |
| Big-decimal | Binary layout and gate are missing. Finding 15. |
| UUID string/fixed | Intended mapping is plausible; validation and version-skew gates are missing. Findings 15, 25. |
| Date | Native conversion needs checked range behavior. Finding 15. |
| Time millis/micros | Range and precision-loss policy are wrong or absent. Findings 6, 15. |
| Timestamp millis/micros/nanos | Millis is representable; micros/nanos are intentionally lossy. Finding 6. |
| Local timestamp millis/micros/nanos | Local/global semantics are collapsed; micros/nanos are lossy. Finding 6. |
| Duration | The proposed unsigned little-endian representation is correct; boundary vectors are still needed. Finding 15. |

## Audit verification

The source and tests have no diff from `0c7be10`; the current worktree head only adds the plan.

| # | Section 2.2 row | Verdict | Evidence |
|---:|---|---|---|
| 1 | Five Apache uncompressed/deflate files read correctly | **confirmed** | Full-value comparisons matched fastavro or Java: 5, 5, 5, 6001, and 3 rows. Reader path: `src/tables.jl:170-200`. |
| 2 | `weather-zstd.avro` segfaults | **confirmed** | Isolated process exited 139 at `src/types/binary.jl:171`. Metadata uses `zstandard`; only `zstd` is registered at `src/Avro.jl:128-134`; unknown codec becomes `nothing` at `src/tables.jl:173-174`. |
| 3 | `weather-snappy.avro` returns garbage | **confirmed** | Five rows were returned with compressed bytes interpreted as data. Snappy is disabled at `src/Avro.jl:125,133`. |
| 4 | `withUnion` fails because Julia union order differs | **confirmed** | `MethodError` reproduced. Schema and Julia union orders are combined independently at `src/types/unions.jl:54-58`. |
| 5 | Logical record has wrong UUID offset because decimal defaults to size 16 in `skipvalue` | **wrong** | The symptom is confirmed, but the cause is different. `Avro.Table` discards the writer schema at `src/tables.jl:198`; row decoding regenerates a lossy schema via `src/types/binary.jl:74-75`. Decimal regeneration uses fixed-16 at `src/types/logical.jl:9-19`. |
| 6 | Julia null-codec file is rejected by Java | **confirmed** | Java reported `Block read partially`. The writer emits the entire sizing cushion at `src/tables.jl:83-116`. |
| 7 | Julia `zstd` file is rejected by Java and fastavro | **confirmed** | Both reported unrecognized codec `zstd`. Metadata comes from `String(compress)` at `src/tables.jl:60-65`. |
| 8 | Julia deflate file reads correctly | **confirmed** | Java and fastavro returned both expected rows. |
| 9 | Negative string length causes OOM | **confirmed** | Probe threw `OutOfMemoryError`; negative length reaches allocation through `src/types/binary.jl:244-247`. |
| 10 | Truncated varint returns zero | **confirmed** | `[0x80,0x80] => 0`; loop returns the accumulator at buffer end in `src/types/binary.jl:155-166`. |
| 11 | Eleven-byte varint returns zero | **confirmed** | Probe returned zero; there is no byte-count or terminal-bit overflow check at `src/types/binary.jl:155-166`. |
| 12 | Fixed-4 from two bytes reads out of bounds | **confirmed** | Probe returned `(1,2,0,0)`. `src/types/fixed.jl:34-43` performs unchecked reads. The observed zeros are not guaranteed. |
| 13 | Invalid union index produces `BoundsError` | **confirmed** | Decoder indexes directly at `src/types/unions.jl:54-58`. |
| 14 | Invalid enum index fails only during display | **confirmed** | Decode accepts the invalid datum at `src/types/enums.jl:45-47`; display later fails at `src/types/enums.jl:23-25`. The table understates the defect. |
| 15 | Array count `2^40` attempted 8 TB and was killed after ten minutes | **unverified** | Source confirms a direct request for about 8 TiB at `src/types/arrays.jl:61-72`. I did not rerun the destructive probe, and the historical duration/kill output was not preserved. |
| 16 | Five invalid schemas are accepted | **confirmed** | All five were accepted. Parsing delegates without validation at `src/utils.jl:68-70`; record and enum constructors enforce no constraints. |
| 17 | Nested union produces a low-level JSON error | **confirmed** | `ExpectedOpeningQuoteChar` reproduced. Nested unions are spec-invalid; the defect is the pathless error, not rejection. |
| 18 | Nanos, big-decimal, and fixed UUID are unsupported as described | **wrong** | Nanos and big-decimal fall back to underlying types. Fixed UUID is structurally parsed as `UUIDType("fixed","uuid")`, but its read/write code incorrectly uses string encoding at `src/types/logical.jl:53-73`. |
| 19 | The supplied greater-than-10k-column schema fails | **wrong** | The 10,001-column probe succeeded with `620082`. The real failure is `Tables.Schema{nothing,nothing}`, obtainable with `stored=true`; `src/types/rows.jl:20-22` calls `fieldcount(Nothing)`. |
| 20 | Benchmark figures and “1,376 allocations” | **wrong** | `@allocated` reports bytes. Rerun: 1,376 bytes and 38 allocations. Current source produced 32,279,364 bytes, 1.209 s write, 3.901 s indexing, and 3.888 s materialization. The supplied 23.1 MB file does not match the current null writer. |

## Too broad / too narrow

Too broad for 2.0:

- Defer the full RPC stack. Its current tests do not exercise the wire protocol.
- Defer append mode, writer-side parallel compression, and borrowed `bytes=:view`.
- Defer big-decimal unless Phase 2 gains exact Java-backed binary vectors.
- Implement ordinary Tables behavior before the full Scan optimizer. Keep Scan behind a precise dependency and equivalence gate.
- Do not require the full OS × Julia × thread cross-product for prerelease and nightly Julia. Run required supported releases broadly; run prerelease/nightly on one OS as nonblocking.
- Remove a mutable global mapping registry unless its mutation, invalidation, cache, and thread semantics are specified.

Too narrow for a production-leading implementation:

- Add stable generic representations for recursive records, enums, fixed values, ambiguous unions, and exact timestamps.
- Add cumulative decode, encode, and in-flight concurrency budgets.
- Add deterministic ownership and close behavior for files, IO, mmap regions, buffers, and views.
- Include sort order in a real phase and gate.
- Add collision handling for schema stores.
- Add atomic path writes and failure cleanup.
- Add zero-column and zero-row Tables behavior.
- Add many-schema compile, invalidation, and native-code-size tests.
- Add independent accept/reject corpora for malformed schemas, raw datums, OCF blocks, codecs, and JSON.
- Add downstream tests for the exact Tables, StructUtils, JSON, and Arrow conventions used by this rewrite.

For §14, I would replace decisions 2, 3, 4, 7, 9, 11, and 12:

- Generic enums should use an indexed wrapper, not `Symbol`.
- Generic nested records should always use a stable `Avro.Record`.
- Timestamps should use exact unit-tagged wrappers.
- Strict OCF reading should be the new default.
- The exact Tables.Scan dependency must be decided and pinned in Phase 0.
- RPC should be deferred from 2.0 unless real wire interoperability is added.
- Limits must be cumulative and conservative.

## Milestone and gate assessment

| Phase | Assessment | Required change |
|---|---|---|
| 0 — Foundation | Not executable as written. The baseline is wrong, `[sources]` does not solve Julia 1.10, and tools are not fully pinned. | Correct the benchmark record. Pin exact Tables, Python packages, extras, and the Java jar checksum. Add the Julia 1.10 `public` workaround. |
| 1 — Schema model | Ordered too early for its dependencies. Default validation needs schema-directed JSON handling. Recursive equality and freezing are unresolved. | Move minimum default-JSON support here. Add duplicate-key, depth, recursion, semantic equality, and immutable-graph gates. |
| 2 — Binary core | Missing its hardest architectural gates. | Require recursive `PlanRef`, tagged unions, stable generic values, exact logical values, cumulative budgets, strict binary validation, and many-schema compile tests. |
| 3 — Resolution | The listed matrix is not enough. | Add recursive writer-reader pairs, mutable-default isolation, logical-underlying cases, decimal mismatch, UTF-8 conversion, alias collision, and branch-preservation gates. |
| 4 — Containers and Tables | Too large for one phase and mixes correctness with optimization. | Split into sequential strict OCF/codecs; basic Tables/ownership; parallel decode; then Scan/performance. Run Java and fastavro interoperability after each writer codec is added. |
| 5 — Release engineering | Contains `protocols.md` before optional Phase 6. Local-only status is mislabeled PR-ready. | Remove deferred API/docs. Define review-ready, PR-ready, merge-ready, RC-ready, and release-ready separately. |
| 6 — Protocols | Its acceptance gate does not test protocols. | Defer it, or replace the gate with live client/server and real framed-wire tests. |

The “branch stays green at every phase” rule is also incompatible with replacing dependencies and schema types before the old binary implementation can use the new model. Build the v2 core beside the legacy implementation. Switch public APIs and remove legacy dependencies only after the replacement paths pass their gates.

PR-ready versus release-ready also needs correction:

- **Review-ready:** clean exact local head, scoped diff, local supported-version tests, docs, licensing, and interop artifacts complete.
- **PR-ready:** review-ready plus a reproducible branch and exact dependency pins.
- **Merge-ready:** hosted exact-head CI is green and review issues are resolved.
- **RC-ready:** no temporary `[sources]`, lower compat bounds resolve, registered dependencies exist, reverse dependencies and PkgEval are checked, and the source archive is validated.
- **Release-ready:** the exact version/tag state is approved. Registration and docs deployment are release actions, not evidence that the source was ready beforehand.

## Verdict

Big dawg, the plan is not implementation-ready. It has blocker-level errors in default semantics, recursive plans, union identity, timestamp precision, resource limits, RPC validation, and Julia syntax/type declarations.

Assumptions made:

- The pinned specification is authoritative when 1.12.2 tools lack a newer feature.
- The supplied local ecosystem heads are the intended integration targets.
- Phase 6 may be deferred without blocking a core data-format 2.0 release.

Decisions made without user direction:

- I selected strict, owned, and exact generic behavior as the default.
- I recommend stable dynamic generic values and opt-in typed specialization.
- I recommend including sort order but deferring RPC, append, parallel writing, borrowed views, and possibly big-decimal.

Validation performed:

- I confirmed that `src/` and `test/` match the stated `0c7be10` audit base.
- I inspected the plan, specification, package source/tests, Tables.Scan, Arrow, JSON, and StructUtils.
- I ran read-only Julia, Java avro-tools, avro Python, and fastavro checks.
- I did not run the destructive 8 TiB allocation probe.
- I created or modified no files.

VERDICT: REVISE
