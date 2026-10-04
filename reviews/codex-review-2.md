# Codex review round 2

## Disposition check (round-1 findings 1-29)

I count 12 resolved, 16 partially resolved, and 1 not resolved.

| # | Status | Evidence and assessment |
|---:|---|---|
| 1 | **PARTIALLY RESOLVED** | Cumulative budgets and capped `sizehint!` are present ([plan:299](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:299)). Defaults still permit 2^31−1 values/rows and 4 GiB. Output allocations, schema nodes, fields, names, branches, symbols, block count, and encoder output are unbudgeted. Encoding is depth-only ([plan:329](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:329)). This remains blocker-level. |
| 2 | **PARTIALLY RESOLVED** | Schema JSON now gets a lexical pre-scan, lazy traversal, duplicate-key rejection, and source spans ([plan:240](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:240)). `fromjson` has no equivalent limits, pre-scan, duplicate-key policy, or cumulative value budget ([plan:510](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:510), [plan:625](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:625)). This contradicts [response-1.md:20](/Users/jacob.quinn/.julia/dev/Avro/reviews/response-1.md:20). |
| 3 | **RESOLVED** | Dynamic generic plans, two-pass `PlanRef` construction, writer-reader pair memoization, and stable `Avro.Record` are specified ([plan:333](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:333)). |
| 4 | **RESOLVED** | Defaults use the first matching branch, retain that branch, retain the JSON span, make fresh copies, and do not make fields optional ([plan:234](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:234)). |
| 5 | **PARTIALLY RESOLVED — amendment accepted in principle** | Non-injective unions are tagged ([plan:371](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:371)). Bare injective unions are reasonable. The stated “first-accepting branch” is still insufficient: for `["long","int"]`, an `Int32` identifies the `int` representation but can also be accepted by a permissive `long` encoder. Branch recovery must use the exact generic representation before conversions. |
| 6 | **PARTIALLY RESOLVED — amendment partly rejected** | Global and micro/nanosecond local timestamps now have exact wrappers ([plan:377](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:377)). Bare `DateTime` is not semantically exact across the full local-millis `Int64` domain; the maximum positive tick wraps to a negative calendar year. |
| 7 | **RESOLVED** | Protocol declarations and RPC are consistently deferred, with real future wire gates recorded ([plan:165](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:165)). |
| 8 | **RESOLVED** | `DecodeError` is abstract, and `public` uses a Julia 1.10-compatible mechanism ([plan:541](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:541), [plan:871](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:871)). |
| 9 | **PARTIALLY RESOLVED** | Semantic equality and parsing equivalence are separated ([plan:222](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:222)). The graph is not frozen: `RecordSchema.fields`, `fieldindex`, and `EnumSchema.symbolindex` remain mutable ([plan:207](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:207)). Exact default spelling also conflicts with a hash derived from full printed JSON. |
| 10 | **PARTIALLY RESOLVED** | Generic plans are dynamic, but column element types, `Vector{juliatype(...)}`, `Dict{String,juliatype(...)}`, and `Decimal{P,S}` remain schema-derived ([plan:345](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:345), [plan:369](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:369)). The plan explicitly leaves nested specialization unresolved ([plan:975](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:975)). |
| 11 | **RESOLVED** | Buffer restrictions, boolean and UTF-8 validation, bounded varints, checked arithmetic, sized-block exhaustion, and top-level trailing-byte rejection are specified ([plan:272](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:272)). Skip semantics remain a new issue below. |
| 12 | **RESOLVED** | `allow_invalid_names` relaxes syntax only; structural and uniqueness rules remain enforced ([plan:240](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:240)). |
| 13 | **RESOLVED** | The datum JSON rule table now covers ranges, fields, fullname union keys, bytes/fixed, and non-finite floats ([plan:510](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:510)). Resource handling remains under finding 2. |
| 14 | **RESOLVED** | The original decimal-resolution contradiction, recursive pairs, defaults, UTF-8 conversion, and union-tag preservation are corrected ([plan:387](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:387)). A new union-selection violation remains below. |
| 15 | **PARTIALLY RESOLVED** | The logical-type table is substantially improved ([plan:436](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:436)). Decimal decoding intentionally ignores precision ([plan:373](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:373)); local-millis and big-decimal also remain incomplete. |
| 16 | **PARTIALLY RESOLVED** | The store is bounded and detects different-PCF CRC collisions ([plan:499](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:499)). It still accepts semantically different but parsing-equivalent schemas under one fingerprint. |
| 17 | **PARTIALLY RESOLVED** | Sort order now has a phase and Java vectors ([plan:531](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:531)). One `compare` signature cannot distinguish a bytes value from encoded datum bytes, and encoded comparison has no budget. |
| 18 | **PARTIALLY RESOLVED** | Strict reading, atomic path intent, and append deferral are correct ([plan:452](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:452)). Automatic legacy decimal reinterpretation creates a new silent-corruption path. Writer abort, poisoned-state, caller-IO ownership, and failure cleanup remain incomplete. |
| 19 | **PARTIALLY RESOLVED** | Borrowed values are removed, copied tables release resources, and close is idempotent ([plan:470](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:470)). The plan does not say whether closing `Rows` or `Reader` closes caller-owned IO or only releases its reference. |
| 20 | **PARTIALLY RESOLVED** | Per-block chunks, structured joins, cancellation, and ordered assembly are specified ([plan:475](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:475)). Shared budget reservations are not synchronized. “First error” remains scheduler-dependent. |
| 21 | **RESOLVED** | `AvroStyle`, a narrow DTO fast route, and semantic StructUtils fallback are explicit ([plan:428](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:428)). |
| 22 | **NOT RESOLVED — SHA amendment accepted, API amendment rejected** | `df4e68c…` is the correct public head. That commit exposes `Tables.resolve`, uses `Tables.All()` as identity, and documents `select=All()` residuals. The plan instead calls nonexistent `Tables.bind` and uses invalid `select=nothing` ([plan:668](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:668)). |
| 23 | **PARTIALLY RESOLVED** | `EnumValue` avoids enum-symbol interning and retains a schema ([plan:368](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:368)). Name and enum-symbol count/byte limits are absent, and the chosen equality is wrong. |
| 24 | **RESOLVED** | Protocol declaration details are properly deferred with requirements ([plan:165](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:165)). |
| 25 | **PARTIALLY RESOLVED** | Java xz, semantic OCF comparison, negative cases, and most pins are corrected ([plan:729](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:729)). `cramjam` is not version-pinned, the avro-python codec row is false, reverse fastavro coverage is incomplete, and the raw byte-equality gate is invalid for collections. |
| 26 | **RESOLVED** | Independent assertions, float-bit checks, subprocess limits, stable seeds, persistence, and shrinking are present ([plan:781](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:781)). |
| 27 | **PARTIALLY RESOLVED** | Units, separate processes, logs, and same-host ratios are corrected ([plan:831](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:831)). Julia columns are still gated against fastavro dictionary rows, and the 8-thread and codec ratios lack a controlled host/kernel definition ([plan:844](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:844)). |
| 28 | **PARTIALLY RESOLVED** | The defect catalogue, inspection path, rewrite recipe, and actual 1.x fixtures are present ([plan:696](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:696)). `decode(schema, ...; writer_schema=...)` still leaves the positional schema’s role ambiguous ([plan:621](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:621)). |
| 29 | **RESOLVED** | The literal NUL was replaced by textual `U+0000–U+00FF` ([plan:521](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:521)). |

## New findings

1. **[blocker] Reader-union selection deliberately violates the pinned specification.**

   **Claim:** The plan chooses an exact branch before an earlier promotable branch.

   **Evidence:** The plan requires Java’s exact-first algorithm ([plan:401](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:401), [plan:969](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:969)). The specification says to use the first reader branch that matches ([spec:716](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:716>)), and matching includes promotions ([spec:695](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:695>)). For writer `int` and reader `["long","int"]`, the spec selects `long`; Java selects the later exact `int`. This is not a case where Java may act as a tie-breaker under [plan:175](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:175).

   **Recommendation:** Make normative first-match behavior the default. If Java compatibility is also required, expose an explicit policy such as `union_resolution=:spec|:java`. Test `["long","int"]` and `["double","int"]` under both policies.

2. **[blocker] Automatic legacy decimal handling can silently corrupt conforming files.**

   **Claim:** Deprecated `readtable` can reinterpret a valid Java decimal as little-endian Avro.jl 1.x data.

   **Evidence:** Legacy mode reinterprets every fixed-16 decimal as native-endian ([plan:465](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:465)). The deprecated shim enables that mode for every source ([plan:649](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:649)). A conforming Java fixed-16 decimal has the same OCF and schema structure. There is no reliable provenance bit that distinguishes it from an Avro.jl 1.x file.

   **Recommendation:** Do not auto-enable endian reinterpretation. Keep unambiguous padding and `zstd` tolerances separate. Require an explicit option such as `legacy_decimal_endian=:little` before changing decimal bytes.

3. **[major] The OCF API cannot decode valid non-record container files.**

   **Claim:** OCF supports any Avro schema, but the public decoded iterators assume records.

   **Evidence:** The specification defines an OCF as a file with a schema and objects; it does not restrict the schema to records ([spec:447](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:447>)). The pinned Java tools successfully produced and counted a top-level `int` OCF:

   ```text
   avro-tools random --schema '"int"' --count 3 ... | avro-tools count -
   3
   ```

   `Avro.Rows` promises `Avro.Record` or `T`, and `Reader.eachblock` exposes only raw bytes ([plan:636](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:636)). `decode` rejects trailing data and exposes no consumed position.

   **Recommendation:** Add `Avro.Values` or schema-directed datum iteration on `Reader`. Make `Table` reject non-record roots clearly. Add primitive, enum, fixed, array, map, and union root OCF fixtures for every codec.

4. **[major] Strict OCF parsing omits lower bounds for block count and size.**

   **Claim:** Negative header values pass the stated checks.

   **Evidence:** The plan checks only `count ≤ max_block_count` and `size ≤ max_block_bytes` ([plan:458](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:458)). The specification defines these as object count and byte size ([spec:483](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:483>)). Negative values can bypass limits and poison prefix sums or ranges.

   **Recommendation:** Require `0 ≤ count ≤ max_block_count` and `0 ≤ size ≤ max_block_bytes` before arithmetic. Test −1 and `typemin(Int64)` for both fields.

5. **[major] Projection and resolution skips have no validation policy.**

   **Claim:** Selecting fewer columns can change whether the same malformed file is accepted.

   **Evidence:** The decoder only promises unspecified “skip counterparts” ([plan:281](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:281)). Projection uses skip plans for unselected fields ([plan:671](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:671)). A length-only skip can bypass invalid UTF-8, a non-0/1 boolean, or invalid enum/union indexes that full decoding rejects.

   **Recommendation:** Define this policy explicitly. If strict mode validates the whole file, skip plans must validate semantic primitives. If projection deliberately validates only traversed values, document that narrower guarantee and add acceptance-equivalence tests for full decode, projection, resolution skips, and encoded comparison.

6. **[major] Tables integration reintroduces untrusted symbol interning and compiler specialization.**

   **Claim:** Untrusted field names and shapes can create permanent symbols and compiler-visible types.

   **Evidence:** The plan says untrusted schemas never drive compilation ([plan:181](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:181)). `Avro.Table` exposes a `Tables.Schema` ([plan:659](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:659)). Tables converts names to `Symbol` and places names/types in `Schema` parameters for up to 65,535 columns unless `stored=true` ([Tables.jl:485](/Users/jacob.quinn/.julia/dev/Tables/src/Tables.jl:485)). The plan has no field, name-count, or name-byte limit.

   **Recommendation:** Always return `Tables.Schema{nothing,nothing}` for file-derived schemas. Add conservative field/name limits. Define the symbol-interning trust boundary. Ensure the compile gate proves that names and nested shapes do not enter method-instance keys.

7. **[major] `EnumValue` equality can equate different symbols.**

   **Claim:** Fullname plus ordinal is not semantic value identity across schema versions.

   **Evidence:** Equality uses fullname and index ([plan:368](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:368)). Enum `E` with `["A","B"]` and another `E` with `["B","A"]` give different symbols at index zero, but the proposed values compare equal.

   **Recommendation:** Compare structural schema identity plus index, or fullname plus resolved symbol. When encoding under a different reader enum, remap by symbol rather than reusing an index.

8. **[major] The error guarantee is impossible and unsafe.**

   **Claim:** “Nothing else escapes the decoders” would require hiding exceptions that callers must see.

   **Evidence:** The promise appears at [plan:557](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:557), and fuzzing requires only `DecodeError` or success ([plan:802](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:802)). IO failures, `InterruptException`, `OutOfMemoryError`, user StructUtils hooks, and system/mmap errors are not malformed Avro data. They must not be recast as `DataError`.

   **Recommendation:** Guarantee that package-detected malformed content becomes an Avro decode error. Preserve IO, interruption, system-resource, and user-hook exceptions. Restrict the “no raw exception” fuzz gate to owned byte buffers and built-in target types.

9. **[major] Raw datum byte equality is not a valid general interoperability oracle.**

   **Claim:** Equivalent arrays and maps need not re-encode identically.

   **Evidence:** The gate requires Java data decoded and re-encoded by Julia to compare byte-for-byte ([plan:772](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:772)). Arrays and maps permit different legal block segmentation ([spec:359](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:359>)). Map entry order is not semantic, and generic maps use `Dict` ([plan:370](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:370)). JSON conversion can also lose NaN payload bits.

   **Recommendation:** Use byte-exact vectors only for deterministic primitives, fixed values, unions, and restricted records. Compare decoded semantics for arrays and maps. Test positive and sized block forms independently.

10. **[major] The oracle capability matrix and reverse-producer coverage are incorrect.**

   **Claim:** Passing the planned job would not establish the claimed all-codec matrix.

   **Evidence:** The plan says avro-python supports all six codecs ([plan:758](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:758)). In the supplied environment, `avro.codecs.KNOWN_CODECS` reports only:

   ```text
   ['bzip2', 'deflate', 'null']
   ```

   Avro-python 1.12.2 has optional snappy/zstandard support but no xz implementation. Generated fastavro files cover deflate and xz only ([plan:735](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:735)), while the summary claims reverse coverage for all six ([plan:936](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:936)). `cramjam` is installed as 2.11.0 but remains unpinned.

   **Recommendation:** Correct the capability matrix. Pin `cramjam==2.11.0`. Generate fastavro→Julia fixtures for each supported codec. Limit avro-python claims to its measured dependency set.

11. **[major] Schema-free `encode(x)` cannot implement its stated inference rule.**

   **Claim:** Several generic decoded values carry their schema in the instance, not in `typeof(x)`.

   **Evidence:** The API defines schema-free encoding as `Avro.schema(typeof(x))` ([plan:622](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:622)). `Avro.Record` and `EnumValue` have stable types and instance schema references. `Vector{UInt8}` cannot distinguish bytes from named fixed. `UUID` cannot retain string-versus-fixed representation. `UnionValue` does not contain its enclosing union schema.

   **Recommendation:** Remove unrestricted schema-free encoding, or define instance-aware `schema(x)` only for identity-bearing values and reject ambiguous values. Rename raw decoding to `decode(writer_schema, src; reader_schema=...)`.

12. **[major] Single-object cache handling confuses parsing identity with logical interpretation.**

   **Claim:** One fingerprint can silently select different Julia meanings.

   **Evidence:** The cache rejects only non-parsing-equivalent registration ([plan:505](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:505)). PCF strips logical types, defaults, aliases, and custom attributes ([spec:737](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:737>)). Plain `int` and logical `date` share a fingerprint but generically decode as `Int32` and `Date`.

   **Recommendation:** Treat registration as idempotent only for structural semantic equality. Reject other same-fingerprint schemas as ambiguous. Recompute the requested fingerprint for every schema returned by an external `SchemaStore`.

13. **[major] Schema-less table writing is not streaming or bounded.**

   **Claim:** The fallback can materialize an unbounded, one-shot source before writing anything.

   **Evidence:** The plan uses `Tables.dictrowtable` when a source has no schema ([plan:489](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:489)). Tables documents and implements it as a materialized vector of dictionaries while unioning columns across every row ([dicts.jl:142](/Users/jacob.quinn/.julia/dev/Tables/src/dicts.jl:142), [dicts.jl:157](/Users/jacob.quinn/.julia/dev/Tables/src/dicts.jl:157)). This contradicts “stream by default” ([plan:185](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:185)).

   **Recommendation:** Require an explicit schema for size-unknown or streaming schema-less sources. If inference remains, expose it as an explicitly materializing operation with row and allocation limits.

14. **[major] The optional big-decimal gate does not cover its unresolved semantics.**

   **Claim:** Three layout vectors can pass without selecting spec or Java behavior for scale.

   **Evidence:** The plan stores an unrestricted `Int32` scale ([plan:374](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:374)). The pinned specification says scale is nonnegative but refers to a precision that big-decimal does not declare ([spec:815](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:815)). Java accepts negative scale; the supplied harness encoded `1E+3` with scale −3. The current three positive vectors do not expose this conflict.

   **Recommendation:** Defer big-decimal, or record a precise policy and add negative-scale, malformed-inner-payload, scale-bound, and exact-inner-consumption cases.

15. **[minor] The revised audit still contains inaccurate summary claims.**

   **Claim:** The narrative overstates what round 1 verified and misstates the wide-schema trigger.

   **Evidence:** The executive summary says 16 of 20 rows were confirmed and four corrected ([plan:32](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:32)). Round 1 recorded 15 confirmed, four wrong, and one destructive row unverified ([review-1:280](/Users/jacob.quinn/.julia/dev/Avro/reviews/codex-review-1.md:280)). The revised table says DataFrames produces stored schemas above 10,000 columns ([plan:99](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:99)); DataFrames calls the ordinary constructor ([tables.jl:24](/Users/jacob.quinn/.julia/packages/DataFrames/0Y1g5/src/other/tables.jl:24)), whose automatic threshold is 65,535. “Every uncompressed container” at [plan:34](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:34) is also broader than the demonstrated affected non-empty files.

   **Recommendation:** State “15 confirmed, four corrected, one source-supported but not rerun.” Use `Tables.Schema{nothing,nothing}` as the exact defect trigger. Limit the null-codec claim to files containing affected blocks.

16. **[minor] Header-only OCF acceptance is an unrecorded spec deviation.**

   **Claim:** The plan follows Java behavior without recording the conflict with its spec-first rule.

   **Evidence:** The plan declares a header-only file valid ([plan:461](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:461)). The pinned specification says a header is followed by one or more data blocks ([spec:452](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:452)). Java creates and accepts header-only empty files.

   **Recommendation:** Continue accepting Java-compatible header-only files, but record this as a deliberate interoperability extension in §14 and test both reading and writing behavior.

## Remaining objections to amendments (if any)

- **Finding 5:** I accept bare values for provably disjoint generic representations. I still object to “first accepting branch.” Exact generic representation matching must occur before coercive branch acceptance.
- **Finding 6:** I accept exact timestamp wrappers and the global/local split. I reject bare `DateTime` for the full local-millis domain. `DateTime(1970) + Millisecond(typemax(Int64))` produces `-292275055-05-16T16:47:04.191`, not the mathematical future local time.
- **Finding 22:** I accept `df4e68c…` as the public pin. I reject the integration text. That commit requires `Tables.resolve`, `All()`, and a residual that retains unhandled type overrides.
- **Fixed-value identity:** I do not accept the amendment as written. Named fixed identity is observable through equality, display, schema-free encoding, and consistency with `GenericFixed`. Either use an identity-bearing generic fixed value or remove schema-free encoding and explicitly define fixed as schema-directed only. A shared-schema column wrapper avoids one schema reference per cell.
- **Green at every phase:** I accept the amendment narrowly. It means only that the new phase-scoped suite passes. It must not imply preserved 1.x behavior, feature completeness, merge readiness, or release readiness.

## Milestone and gate assessment

| Phase | Assessment and required change |
|---|---|
| **0 — Foundation** | Not executable on Julia 1.10 through `[sources]` alone. Arrow’s local precedent records that Julia 1.10 ignores `[sources]` and explicitly adds Tables ([Arrow Project.toml:34](/Users/jacob.quinn/.julia/dev/Arrow/Project.toml:34)). Add `Pkg.add(PackageSpec(url=..., rev=full_SHA))` to 1.10 setup. Pin `cramjam==2.11.0`. |
| **1 — Schema** | Add node, field, branch, symbol, name-byte, and default-allocation limits. Replace convention-only freezing with enforceable immutability. Normalize semantic defaults and property maps for `==` and `hash`. |
| **2 — Binary core** | Decide the stable collection/decimal fallback now. Add exact local-millis wrappers, decimal precision validation on decode, bounded datum JSON, schema-store ambiguity handling, and a coherent schema-free encoding rule. Compile gates need numeric limits for RSS/native code as well as methods and time. |
| **3 — Resolution/order** | Follow the normative union-first-match rule or expose an explicit Java mode. Split value and encoded comparison APIs. Give encoded comparison full budgets. |
| **4a — OCF/codecs** | Reject negative counts and sizes. Remove automatic endian reinterpretation. Define Writer abort, poisoned state, caller-IO ownership, and all failure paths. Add producer-by-codec tests from Java and fastavro. |
| **4b — Tables/streaming** | Add decoded iteration for non-record OCF schemas. Always use stored Tables schemas for file-derived inputs. Test name limits and symbol growth. |
| **4c — Parallel** | Define atomic reserve-before-allocation budget operations and deterministic error selection, preferably the lowest failing block index. |
| **4d — Scan/performance** | Rewrite against `df4e68c` before implementation. Use `Tables.resolve`, `All()`, and a correct residual. Cross-language performance must be informational unless result layouts match. Define a named ≥8-physical-core host for the 8-thread gate and a codec-only kernel for the 1.3× gate. |
| **5 — Release engineering** | Add an exact Tables/Arrow cross-package smoke test. Keep the full CI, documentation, interoperability, migration, and archive gates. |

The scope deferrals for RPC, append, borrowed views, and parallel writing are sound. Big-decimal should remain deferred until its semantics are resolved. Non-record OCF iteration, strict resource limits, and safe legacy handling are too narrow today and are required for 2.0.

The readiness levels are mostly correct. However, PR-ready is not reproducible on Julia 1.10 until the dependency bootstrap is explicit. The Scan/no-Scan release choice also occurs too late at RC. Decide the 2.0 surface earlier, or define and test a complete no-Scan variant.

## Verdict

DRAFT v2 is materially better, big dawg, but it is not implementation-ready. A normative union-resolution violation, silent legacy decimal corruption, unsafe default budgets, and several major API and conformance gaps remain.

Assumptions made:

- The pinned 1.13-SNAPSHOT specification is authoritative.
- RPC can remain deferred without blocking a production data-format implementation.
- The declared fixture and gate artifacts are implementation deliverables, not current review preconditions.

Decisions made without user direction:

- I require normative union selection by default.
- I require explicit opt-in before ambiguous legacy decimal reinterpretation.
- I treat decoded iteration for non-record OCF schemas as required 2.0 scope.
- I used the package-review checklist to keep PR, RC, and release gates separate.

Validation performed:

- I inspected DRAFT v2, both round-1 review files, the pinned specification, package source/tests, generated fixtures, Java harnesses, JSON lazy parsing, Tables at the pinned and local revisions, Arrow conventions, StructUtils, and DataFrames’ Tables adapter.
- I ran read-only Java, Julia, and Python checks for primitive-root OCF, local-millis extremes, codec capabilities, and the pinned Scan API.
- I did not rerun the destructive 8 TiB allocation probe.
- I created or modified no files.

VERDICT: REVISE
