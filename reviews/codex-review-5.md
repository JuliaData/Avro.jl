# Codex review round 5

## Disposition check (round-4 items)

### Carried rows

| Item | Status | Evidence and assessment |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | Encode counting, post-decompression checks, internal-allocation charging, and lower ceilings exist ([plan:330](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:330), [plan:341](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:341)). Default Writer readability, work amplification, and aggregate transient memory remain defective. See findings 1–4. |
| R1-9 — Freezing and hashing | **RESOLVED** | Props/defaults are recursively frozen, hashes are stored at freeze time, and defaults are freshly materialized ([plan:229](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:229)). |
| R1-10 — Schema-driven compilation | **RESOLVED** | Generic columns use schema-independent `Vector{ColumnBuilder}` storage with no width/order specialization ([plan:412](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:412)). I accept the measured dispatch risk and fallback. |
| R1-18 — Writer lifecycle | **RESOLVED** | Atomic replacement, poisoning, abort, partial-sink behavior, cleanup, and failure injection are specified ([plan:622](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:622)). |
| R1-20 — Parallel determinism | **RESOLVED for error selection** | Local accounting and ordered cumulative commits remove schedule-dependent semantic failures ([plan:606](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:606)). Aggregate transient memory remains separate; see finding 2. |
| R1-25 — Union oracle | **RESOLVED** | Direct branch-index/type expectations and reader-directed output are authoritative ([plan:492](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:492), [plan:974](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:974)). |
| R2-new-6 — Symbol interning | **RESOLVED for Tables field names** | Caller-owned admission tables and lazy `Rows` admission are specified ([plan:873](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:873)). Typed datum values still have an uncovered Symbol path; see finding 10. |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowed contract** | Schema-free encoding now uses documented conventions and explicitly disclaims decoded-data identity ([plan:534](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:534)). I accept this amendment. |

### Round-4 new findings 1–20

| # | Status | Evidence and assessment |
|---:|---|---|
| 1 — Codec memory | **PARTIALLY RESOLVED** | Xz and zstd controls were added, but the total-memory claim, bzip2 handling, aggregate reservation, and zstd fixture remain wrong. See findings 2 and 4. |
| 2 — Work/allocation budget | **PARTIALLY RESOLVED** | Counters and charging exist, but default Writer output can still be default-unreadable and a small file can buy billions of iterations. See findings 1 and 3. |
| 3 — Fast skipping versus strict validation | **RESOLVED** | Strict walking, opt-in fast jumping, documented blind spots, and equivalence gates exist ([plan:310](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:310)). I continue to accept the previously recorded skipped-string exception. |
| 4 — Enum-default repair / inspect | **RESOLVED** | Enum and field defaults share the frozen validity-bearing representation; `inspect` is bounded and permissive ([plan:220](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:220), [plan:251](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:251)). |
| 5 — Bounded `tojson` and `compare` | **RESOLVED** | Both APIs now expose depth/value limits; JSON also bounds output bytes ([plan:676](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:676), [plan:705](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:705)). |
| 6 — Tuple-unrolled builders | **RESOLVED** | Column builders no longer specialize on schema width or order ([plan:421](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:421)). |
| 7 — Parallel budget determinism | **RESOLVED** | Cumulative commits occur in block order and tests force opposite schedules ([plan:612](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:612)). |
| 8 — Resolution output representation | **RESOLVED** | Non-union and nullable readers unwrap; tagged readers use the reader branch index; `T` follows the effective reader schema ([plan:500](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:500)). |
| 9 — Non-decimal logical pairings | **RESOLVED** | Pairings resolve through compatible underlying schemas with reader interpretation and no unit conversion ([plan:507](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:507)). |
| 10 — Logical sort order | **PARTIALLY RESOLVED** | Underlying ordering is stated, but the native value model cannot always reproduce encoded ordering. See finding 7. |
| 11 — Inference contract | **RESOLVED BY DEFERRAL** | The missing lattice is recorded, and 2.0 requires an explicit or Tables-provided schema ([plan:170](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:170)). I accept this scope decision. |
| 12 — Prepared single-record gates | **RESOLVED** | Reusable `DatumReader`/`DatumWriter` are public; kernel and one-shot measurements are separated ([plan:427](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:427), [plan:1134](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1134)). |
| 13 — JSON union ambiguities | **RESOLVED** | Bare union defaults and the branch-label collision policy are explicit ([plan:284](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:284), [plan:698](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:698)). |
| 14 — Sort normalization/block forms | **RESOLVED** | Normalized oracle verdicts and positive/sized cross-form arrays are deliverables ([plan:976](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:976)). |
| 15 — Missing roots | **RESOLVED** | Every primitive and complex root is listed for all codecs and both producers ([plan:969](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:969)). |
| 16 — Option validation | **PARTIALLY RESOLVED** | Named checks exist, but contradictory per-block/aggregate defaults remain legal; for example, `max_block_output_bytes` may exceed `max_total_bytes` ([plan:352](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:352), [plan:380](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:380)). |
| 17 — Atomic replacement | **RESOLVED** | Same-directory temporary creation, replacement, symlink, Windows, and `fsync` behavior are stated and gated ([plan:632](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:632)). |
| 18 — Multi-tenant symbol quota | **RESOLVED for Tables names** | Per-tenant admission objects and lazy admission exist ([plan:873](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:873)). |
| 19 — Coverage gate | **PARTIALLY RESOLVED — minor** | Process coverage and Codecov now run, but the ratchet is expressly informational, not an enforced regression gate ([plan:1106](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1106), [plan:1184](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1184)). |
| 20 — Conventional-schema wording | **RESOLVED** | The plan now consistently describes deterministic conventions, not unique representations ([plan:534](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:534)). |

## New findings

1. **[blocker] Default Writer output can still be rejected by the default Reader.**

   **Claim:** `block_datums` limits root datums, while the work budget counts nested values.

   **Evidence:** Writer flushes after 65,536 datums ([plan:389](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:389), [plan:623](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:623)). The budget counts every encoded or decoded value ([plan:334](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:334), [plan:355](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:355)). A record with three `null` fields produces a zero-byte block containing at least 196,608 nested values. For one zero-byte block, the cumulative rule permits only `65,536 × (1 + 1) = 131,072` values. The declared test covers null, empty-record, and empty-fixed roots, not non-empty zero-byte records ([plan:391](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:391)).

   **Recommendation:** Flush on encoded-value work, not only datum count. Give each container Writer a lifetime row/value budget and a per-block encoded-value counter. Add records and nested arrays containing several zero-byte values to the default-writer/default-reader invariant.

2. **[blocker] Parallel memory can exceed the declared total before any `LimitError`.**

   **Claim:** Ordered semantic commits do not provide an aggregate transient-memory bound.

   **Evidence:** One worker may locally allocate 256 MiB decompressed data, 256 MiB codec memory, and 1 GiB decoded output ([plan:350](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:350)). The default cumulative budget can be only 256 MiB, while `max_inflight_blocks` defaults to `ntasks` ([plan:373](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:373)). Workers create chunks before the coordinator commits them against `max_total_bytes` ([plan:606](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:606)). One worker can therefore allocate roughly 1.5 GiB, and eight workers roughly 12 GiB, before the ordered check rejects anything.

   **Recommendation:** Add a weighted transient-memory semaphore separate from deterministic semantic accounting. Reserve decompressed buffers, codec memory, chunks, offsets, and assembly overlap before allocation. Waiting must not itself become a semantic failure. Validate per-block bounds against the aggregate bound.

3. **[major] The default work rule permits billions of iterations from a sub-megabyte file.**

   **Claim:** The per-block free allowance defeats the claimed input-proportional safety property.

   **Evidence:** Defaults permit `2^31` values and grant 65,536 free values per block ([plan:357](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:357)). An OCF with 32,768 null-root blocks, each declaring 65,536 datums and zero payload bytes, is about 655 KiB of block headers and sync markers yet admits `2^31` datum iterations. The proof says `blocks ≤ B/18` ([plan:394](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:394)), but `B` is defined to exclude compressed bytes and OCF framing ([plan:359](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:359)); for this file its defined `B` is zero.

   **Recommendation:** Charge block headers and sync markers as consumed input. Use one small operation allowance rather than 65,536 free values per block, or price each allowance against actual framing bytes. Lower the safe default value/row ceiling substantially.

4. **[major] The codec-memory contract and its zstd gate are not executable as written.**

   **Claim:** The limit is neither a true codec-memory cap nor correctly mapped to the declared fixtures.

   **Evidence:**

   - Bzip2 is described as bounded by its 900 KiB block format ([plan:578](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:578)). CodecBzip2 defaults to `small=false` ([CodecBzip2:15](/Users/jacob.quinn/.julia/packages/CodecBzip2/gnqO5/src/decompression.jl:15)); its bundled documentation says a 900 KiB block needs about 3.7 MiB normally or 2.35 MiB in small mode ([bzip2 manual:292](/Users/jacob.quinn/.julia/artifacts/c1600fa286afe4bf3616780a19b65285c63968ca/man/man1/bzip2.1:292)). `Limits` nevertheless accepts a 1 MiB cap.
   - CodecZstd’s option limits the streaming window buffer, not all decoder/context memory ([CodecZstd:38](/Users/jacob.quinn/.julia/packages/CodecZstd/dNeRy/src/decompression.jl:38)).
   - The default 256 MiB maps to window log 28, yet the plan says a window-log-27 frame must fail ([plan:982](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:982)). A read-only probe of `zstd-window1g.avro` failed at limits 27–29 and succeeded at 30–31.
   - The literal `floor(log2(max_codec_memory))` call returns `Float64`; CodecZstd requires `Int32`. A Julia 1.12 probe returned `TypeError(... windowLogMax, Int32, 28.0)`.

   **Recommendation:** Define whether this is a total-memory cap or a codec window/dictionary cap. Account for fixed context overhead separately. Enforce bzip2’s block-dependent minimum or reject impossible configured caps. Use a checked integer-log-to-`Int32` conversion. Correct the fixture description to an actually advertised window above 28.

5. **[major] Global numeric-attribute validation rejects valid schemas.**

   **Claim:** `size`, `precision`, and `scale` are being reserved globally instead of interpreted in context.

   **Evidence:** The plan requires every such property to be a non-negative `Int` ([plan:153](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:153), [plan:266](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:266)). The specification says unknown attributes are permitted metadata ([spec:43](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:43)) and invalid logical types must be ignored in favor of their underlying type ([spec:785](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:785)). Thus these are valid underlying schemas:

   - `{"type":"string","size":"large"}`
   - `{"type":"bytes","logicalType":"vendor","precision":"opaque"}`
   - `{"type":"bytes","logicalType":"decimal","precision":4,"scale":-1}`

   Java 1.12.2 canonical-form probes accepted the metadata cases and warned before reducing the invalid decimal to `"bytes"`.

   **Recommendation:** Validate `fixed.size` as schema syntax. Interpret precision and scale only within recognized decimal handling. Malformed, negative, or out-of-range logical attributes must cause the logical annotation to be ignored while preserving the raw properties.

6. **[major] `Int32` cannot represent every valid decimal scale accepted by the parser.**

   **Claim:** A small valid datum can fail solely because the Julia value type narrows the schema scale.

   **Evidence:** `Avro.Decimal` and `Avro.WideDecimal` store `scale::Int32` ([plan:466](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:466)). Schema attributes are accepted throughout Julia `Int` range ([plan:270](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:270)). The specification imposes `0 ≤ scale ≤ precision`, but no 32-bit bound ([spec:798](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:798)). A bytes-decimal with precision and scale `2147483648` and datum `00` is valid but cannot construct either promised value type.

   **Recommendation:** Store scale as `Int`, or introduce an explicit configurable decimal-attribute limit with a controlled `LimitError`. Test both sides of `typemax(Int32)`.

7. **[major] The two logical comparison APIs cannot always agree.**

   **Claim:** Native logical values discard bytes that the plan says determine ordering.

   **Evidence:** Bytes decimals decode to only `(unscaled, scale)` and UUID strings to `UUID` ([plan:466](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:466)). The plan then requires logical values to sort by underlying encoding and says both APIs agree ([plan:716](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:716)). The specification orders bytes unsigned lexicographically ([spec:436](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:436)). Decimal payloads `00` and `00 00` are both valid encodings of zero: `comparebytes` orders the shorter first, while `compare` sees identical `Decimal` values. Mixed-case valid UUID strings have the same information-loss problem.

   **Recommendation:** Preserve raw representation, or restrict cross-API agreement to canonical encodings produced by Avro.jl. Otherwise define distinct native-value semantics. Add non-minimal decimal and mixed-case UUID vectors.

8. **[major] Schema resolution has no work or pair-count limit.**

   **Claim:** Individually bounded schemas can cause unbounded cross-product work during resolution.

   **Evidence:** `resolve` memoizes `(writer, reader)` pairs but exposes no `limits` keyword ([plan:479](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:479), [plan:812](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:812)). Each schema may contain one million nodes and 1,024 union branches ([plan:364](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:364)). Repeated large writer/reader unions can force millions to hundreds of millions of match attempts and a large pair cache before datum decoding begins. `DatumReader.limits` has no corresponding resolution-pair counter.

   **Recommendation:** Add `limits=` to `resolve` and a `max_resolution_pairs` or equivalent work budget. Charge every match attempt, memo entry, and resolving-plan node.

9. **[major] Julia type-derived schemas have unresolved fullname and union collisions.**

   **Claim:** Valid Julia types can generate invalid or semantically wrong Avro schemas.

   **Evidence:** Struct names use only `parentmodule(T)` and `nameof(T)` ([plan:520](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:520)). Julia 1.12 reports the same name and parent module for `Box{Int}` and `Box{String}`: `(:Box, :Box, Main.M, Main.M)`. A containing struct therefore generates two incompatible definitions of the same fullname. The specification forbids repeated fullname definitions ([spec:260](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:260)). Similarly, `Union{String,Symbol}` maps both branches to `string`, violating the duplicate-union rule ([spec:165](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:165)).

   **Recommendation:** Define parameter-aware stable naming or require explicit nested names where collisions occur. Detect mapped union collisions and reject them with an actionable error unless lossless deduplication is defined.

10. **[major] Typed decoding can permanently intern untrusted datum values without admission control.**

    **Claim:** The symbol budget protects field names, but not string or enum values decoded into `Symbol`.

    **Evidence:** `Symbol` maps to Avro string, and typed enums accept `Symbol` ([plan:461](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:461), [plan:520](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:520)). Datum APIs have no symbol-admission option ([plan:821](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:821)). Repeated untrusted inputs can therefore permanently intern distinct values across operations, bypassing the admission boundary in §6.

    **Recommendation:** Make `Symbol` encode-only by default, or require a bounded caller-owned admission object for typed Symbol decoding. Document this as a trust boundary and test repeated files, not only one operation.

11. **[minor] Decimal encoding does not define scale mismatch behavior.**

    **Claim:** Precision validation alone is insufficient when scale is a runtime field.

    **Evidence:** A decimal value represents `unscaled × 10^-scale`, while the schema fixes scale ([spec:792](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:792)). The encoder checks precision but does not require `value.scale == schema.scale` ([plan:333](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:333), [plan:466](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:466)).

    **Recommendation:** Require exact scale equality on encode. Provide a separate checked rescaling helper if desired.

12. **[minor] Scan’s count-based block skipping is invalid when a filter is active.**

    **Claim:** Header counts describe source rows, not rows surviving the filter.

    **Evidence:** Tables applies filter, then offset/limit, then projection ([pinned Tables.Scan:647](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/tables-scan/src/scan.jl:647)). The plan says whole blocks outside the row window are skipped using header counts ([plan:903](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:903)). That optimization is valid only without a row-dependent filter. “Decoding stops at limit” also conflicts with strict-mode validation of remaining blocks.

    **Recommendation:** Restrict count-only block skipping to no-filter or constant-filter cases. With a filter, count qualifying rows. In strict mode, stop materialization at the limit but continue validation.

13. **[minor] Fullname construction must precede validation of an ignored namespace.**

    **Claim:** The parser can reject an attribute that the fullname algorithm says to ignore.

    **Evidence:** The plan blanket-validates namespace grammar ([plan:266](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:266)). The specification says that when `name` already contains a dot, any supplied namespace is ignored ([spec:250](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:250)). Java accepted `name:"a.R", namespace:"bad-ns"` and even a numeric ignored namespace.

    **Recommendation:** Apply the fullname algorithm first. Do not validate an ignored namespace. Add both cases.

14. **[minor] The blanket Java-readability promise contradicts a recorded compatibility decision.**

    **Claim:** Some files intentionally supported by V5 cannot be read by Java.

    **Evidence:** V5 accepts binary unions whose JSON labels collide, although Java rejects those schemas ([plan:284](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:284)). It nevertheless promises that Java and fastavro read every written file ([plan:946](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:946), [plan:1236](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1236)).

    **Recommendation:** Scope the promise to the supported oracle matrix and list spec-over-oracle exceptions, including repaired invalid schemas.

15. **[nit] The top-level strict-mode principle contradicts its explicit exception.**

    **Claim:** “Never accepts bytes it has not validated” is broader than the actual contract.

    **Evidence:** The principle makes that absolute claim ([plan:200](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:200)), while skipped strings are deliberately not UTF-8 validated ([plan:310](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:310)).

    **Recommendation:** Qualify the principle with the recorded skipped-string exception. I do not reopen the accepted policy itself.

## Non-blocking follow-ups (if any)

Findings 11–15 are non-blocking. Also:

- Enforce a real coverage ratchet, or call it an informational coverage report.
- State that a container Writer’s row/value counters cover its full lifetime, while each prepared datum call gets a fresh operation budget.
- Clarify that `DatumReader` is shareable with per-call decoder state, while `DatumWriter` is mutable and single-owner. State whether `writer(x)` returns an owned buffer.
- Record quoted non-finite float defaults as a deliberate Java-compatible extension in §14.

Schema inference, RPC, big-decimal, append, borrowed views, and writer-side parallel compression remain acceptable deferrals.

## Milestone and gate assessment

| Phase | Required correction |
|---|---|
| 0 | Correct the zstd high-window description and generator. Record the real advertised window and normalize the typed `Int32` option. |
| 1 | Make numeric-property validation contextual; widen or explicitly limit decimal scale; define parameterized-type names and union-collision errors. |
| 2 | Fix Writer value-based flushing, lifetime encode budgets, typed Symbol admission, and exact decimal-scale encoding. |
| 3 | Bound resolution work and correct the logical comparator contract and vectors. |
| 4a | Enforce real per-codec memory limits and prove default Writer output is default-readable for nested zero-byte schemas. |
| 4c | Add aggregate transient reservations and cross-field limit validation before parallel allocations. |
| 4d | Restrict count-based offset/limit skipping when filters are active. |
| 5 / RC | Scope the Java/fastavro promise and decide whether coverage is enforced or informational. Rerun the complete matrix from the final archive as already planned. |

The bidirectional codec/root interoperability matrix is otherwise strong. The prepared-codec benchmark design now measures what the public API can perform. The ratio baselines, PR/merge/RC distinctions, inference deferral, and exact final-archive rerun are sound.

The §2.2 audit remains accurate. `git diff --exit-code 0c7be10 -- src test` returned exit 0, so the audited source and tests are unchanged. I found no new audit-table discrepancy.

## Verdict

DRAFT v5 is not implementation-ready, big dawg. The default Writer/Reader invariant and parallel memory safety remain blocker-level. The work rule, codec cap, logical-property handling, decimal representation, sorting, resolution budget, type mapping, and typed Symbol path still have major defects.

Assumptions made:

- The pinned specification is authoritative.
- Embedded schemas and datum bytes are untrusted.
- Default limits must be safe and must read default Writer output.
- Declared fixtures and gates remain implementation deliverables.
- The recorded feature deferrals remain allowed.

Decisions made without user direction:

- I accepted schema-inference deferral and the narrowed schema-free contract.
- I retained the previously accepted skipped-string policy.
- I treated typed Symbol conversion and resolving-plan construction as part of the untrusted-input boundary.
- I used the package-review checklist to keep phase, PR, RC, and release evidence separate.

Validation performed:

- I inspected DRAFT v5, response-4, the pinned specification, pinned Tables.Scan, codec sources, fixtures, APIs, tests, benchmarks, and milestones.
- I ran read-only Java probes for metadata, invalid logical annotations, ignored namespaces, and decimal boundaries.
- I ran read-only Julia probes for CodecZstd option typing and parameterized-type names.
- I checked the high-window fixture thresholds and bzip2 memory documentation.
- I confirmed `src/` and `test/` remain unchanged from `0c7be10`.
- I created or modified no files.

VERDICT: REVISE
