# Codex review round 13

DRAFT v13 is still not implementation-ready, big dawg. Six major defects remain. The specification coverage is otherwise strong. Line references below are to [DRAFT v13](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md).

## Disposition check (round-12 items)

A finding is marked resolved only for its original claim. A replacement mechanism that breaks the same guarantee is marked partially resolved.

### Primary carried rows

| Item | Status | Evidence |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | The five accounting categories are defined at lines 538–607. Seed-dependent map memory, admission-table work, and typed-shell undercharging still violate the ceiling. |
| R1-9 — Freezing and hashing | **RESOLVED** | Transitive freezing and stored hashes: lines 244–251. Fresh defaults: lines 286–296. |
| R1-10 — Schema-driven compilation | **RESOLVED** | Schema-independent generic plans and the finite value set: lines 618–665. |
| R1-18 — Writer lifecycle | **RESOLVED** | Validation, poison state, atomic replacement, cleanup, and abort: §4.9, lines 990–1031. |
| R1-20 — Parallel error determinism | **RESOLVED** | Lowest-block priority and deterministic failure selection: lines 925–949. |
| R1-25 — Union oracle | **RESOLVED** | Generic representation and reader-directed resolution: lines 677–717. |
| R2-new-6 — Symbol admission | **PARTIALLY RESOLVED** | All typed paths use admission, but the admission structure contradicts itself about fixed versus growing capacity at lines 474–477 and 581–583. |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowed contract** | The conventional, non-round-trip contract is explicit at lines 774–781. |

### Other carried rows

| Item | Status | Evidence |
|---|---|---|
| R4-1 — Codec memory | **RESOLVED** | Complete xz and zstd caps and multi-member handling: lines 831–870. |
| R4-2 — Work/allocation budget | **PARTIALLY RESOLVED** | The accounting model is strong, but map/admission work and typed DTO shells remain incorrect. |
| R5-1 — Writer/Reader invariant | **PARTIALLY RESOLVED** | The peak preflight is specified at lines 508–523, but it omits the comparison rule and cannot predict random-seed overflow capacity. |
| R5-2 — Parallel transient memory | **RESOLVED for the prior mechanism** | Preallocation, reservations, ordered commits, and eviction are specified at lines 897–962. |
| R5-3 — Work amplification | **PARTIALLY RESOLVED** | Value and comparison rules exist at lines 474–506. Incremental admission overflow movement is not bounded. |
| R5-4 — Codec-cap contract | **RESOLVED** | Reader and emitted-frame checks: lines 831–870 and 1001–1017. |
| R5-9 — Julia-derived names | **RESOLVED** | Complete naming policy and hooks: §4.8. |
| R5-10 — Typed Symbol paths | **RESOLVED for path coverage** | Every typed binary and JSON path is listed at lines 630–631 and in §6. |

### Round-6 carried findings

| # | Item | Status |
|---:|---|---|
| 1 | Semaphore memory and liveness | **RESOLVED for the parallel mechanism** — lines 897–962. |
| 2 | Writer/Reader invariant | **PARTIALLY RESOLVED** — comparison work and seed-dependent overflow remain outside preflight, lines 508–523. |
| 3 | Unsafe default memory | **RESOLVED** — fixed 256 MiB default and available-memory guard, lines 443–458. |
| 4 | Compressed-size work precheck | **RESOLVED** — expressly prohibited, lines 492–503. |
| 5 | Whole-file `mmap=false` allocation | **RESOLVED** — streamed path handling in §4.9. |
| 6 | Self-alias gate | **RESOLVED** — lines 323–325. |
| 7 | `fromjson` admission | **RESOLVED** — lines 1062–1070 and §6. |
| 8 | Julia name policy | **RESOLVED** — §4.8. |
| 9 | Codec-cap cases | **RESOLVED** — lines 831–870. |
| 10 | Work ceilings | **PARTIALLY RESOLVED** — the datum rules are bounded, but admission movement remains uncharged. |
| 11 | JSON-depth symmetry | **RESOLVED** — lines 409–412 and 1062–1068. |
| 12 | Duplicate named union branches | **RESOLVED** — lines 325–327. |
| 13 | Attribute wording | **RESOLVED** — lines 269–285. |

### Round-7 items

All eleven reported items are **RESOLVED**:

- Final-column allocation: lines 897–915.
- Writer codec workspace and frame requirements: lines 1001–1017.
- Actual reservations and priority: lines 916–962.
- Portable memory defaults: lines 420 and 443–458.
- Constructor relations without fixed `W`: lines 460–472.
- Empty-union `minsize`: lines 256–259.
- Peak-RSS method: lines 969–989.
- Alias normalization: lines 252–255.
- Streamed compressed input: §4.9.
- `Nothing → null`: §4.8.
- Julia-name sanitization: §4.8.

### Round-8 items and follow-ups

All reported Round-8 items are **RESOLVED**:

- Nullable-column storage and tag bytes: lines 538–573 and 905–915.
- Complete Zstandard requirement: lines 839–844.
- Eviction protocol: lines 925–949.
- Parallel RSS source and sampler: lines 969–989.
- Non-UTF-8 metadata oracle exception: §7.
- Filtered Scan sizing and per-pass attempts: lines 1308–1321.
- `fixed(0)` decimal handling: lines 281–285.
- Estimator dependencies and symbol checks: §11.
- Bzip2 floor, cooperative cancellation, charged worker pool, storage transfer, CPython pin, and accepted deferrals are also resolved.

### Round-9 items and follow-ups

| Item | Status | Evidence |
|---|---|---|
| XZ cap reservation | **RESOLVED** | Lines 831–839. |
| Representation storage formulas | **PARTIALLY RESOLVED** | Generic formulas improved, but `sizeof(T)` undercharges some approved DTOs at lines 560–562. |
| Concatenated codec members | **RESOLVED** | Lines 847–870. |
| Filtered Scan attempts | **RESOLVED** | Lines 1308–1321. |
| Deterministic work versus CPU timing | **RESOLVED** | Lines 937–949 and Phase 4c. |
| Interoperability-summary qualification | **RESOLVED** | §7 and §13. |
| Fixed worker pool | **RESOLVED** | Parallel worker-pool contract in §4.9. |
| RSS primitive | **RESOLVED** | Lines 969–989. |
| Ownership, task, RSS, and deferral follow-ups | **RESOLVED** | §§4.4, 4.9, and 14. |

### Round-10 findings 1–9

| # | Item | Status |
|---:|---|---|
| 1 | `Dict` capacity/collision rehash | **PARTIALLY RESOLVED** — `Base.Dict` is gone, but the replacement still has seed-dependent overflow and an inconsistent admission design. |
| 2 | Schema/plan graph accounting | **PARTIALLY RESOLVED** — category (e) exists at lines 584–592, but source normalization and one budget-scope entry remain unclear. |
| 3 | Identity-bearing storage oracle | **RESOLVED** — schema references are excluded and separately charged, lines 563–566. |
| 4 | XZ Stream Padding | **RESOLVED** — lines 847–856. |
| 5 | Stale clauses | **PARTIALLY RESOLVED** — the original clauses are fixed, but the mandatory map test at lines 1571–1573 now contradicts the normative design. |
| 6 | Zstandard empty/skippable frames | **RESOLVED for decoding** — lines 847–853. One stale work-charge sentence remains. |
| 7 | Output/yield ownership | **RESOLVED** — lines 573–597. |
| 8 | RSS sampler | **RESOLVED** — §4.9. |
| 9 | Runtime layout guard | **RESOLVED for the original shell constants** — typed DTO accounting has a separate defect. |

### Round-11 findings 1–9

| # | Item | Status |
|---:|---|---|
| 1 | Exact `Avro.Map` formula | **RESOLVED for retained-capacity charging** — actual capacities and the measured shell are specified at lines 551–560. Seed-dependent acceptance remains separate. |
| 2 | JSON.jl unbudgeted `Set` | **RESOLVED** — JSON.jl duplicate tracking is disabled; Avro sorts decoded keys, lines 310–317. |
| 3 | Typed conversion boundary | **PARTIALLY RESOLVED** — user constructors are bypassed, but the specified mechanism cannot construct ordinary immutable structs. |
| 4 | Schema-operation limits | **PARTIALLY RESOLVED** — `schema(x; limits=…)` exists at line 1204, but it is absent from the authoritative budget-scope list at lines 609–616. |
| 5 | Writer/Reader category-(e) peak | **RESOLVED for category (e)** — lines 508–523. Map comparison preflight is a separate omission. |
| 6 | Map-probe work | **PARTIALLY RESOLVED** — finite lookup probes exist, but admission overflow movement and seed-dependent compared-byte work remain. |
| 7 | Map capacity terms | **RESOLVED for actual capacity accounting** — lines 551–560. |
| 8 | Stale summaries | **RESOLVED for the reported summaries** — Decision 30 and the global-state summary are corrected. The old map test is separate. |
| 9 | Layout-state exception | **RESOLVED** — both exceptions are recorded at lines 207–213 and 1770–1771. |

### Round-12 new findings 1–12

| # | Item | Status | Evidence |
|---:|---|---|---|
| 1 | Rebuilds versus exact map accounting | **PARTIALLY RESOLVED** | Rebuilds were removed at lines 474–483, but the required test still demands them at lines 1571–1573. |
| 2 | Seed-dependent map rejection | **PARTIALLY RESOLVED** | Probe rejection is gone. Actual overflow capacity and comparison work still depend on the random seed, lines 474–483 and 676. |
| 3 | Bounded `parseschema(::IO)` | **RESOLVED** | Incremental `max_schema_bytes + 1` buffering: lines 299–303; boundary gate: line 1577. |
| 4 | Invalid UTF-8 and escapes | **PARTIALLY RESOLVED** | Invalid raw UTF-8 and malformed escapes are covered, but the global paired-surrogate rule rejects valid metadata accepted by the oracles. |
| 5 | Sorted duplicate-key work | **RESOLVED for the original JSON sort** | Comparison limit and counter: lines 425, 485–490, and 533–536. |
| 6 | `schema(x)` budget | **PARTIALLY RESOLVED** | The public keyword exists at line 1204; the budget-scope list at lines 609–616 still omits it. |
| 7 | User constructors on the fast route | **PARTIALLY RESOLVED** | User constructors are bypassed, but `jl_new_struct_uninit` plus field stores is invalid for immutable structs, lines 782–792. |
| 8 | Decoded-key equality | **RESOLVED for Unicode-scalar keys** | Lines 313–317 and the gate at lines 1578–1580. |
| 9 | Codec-member work | **PARTIALLY RESOLVED** | Members count as values at lines 492–496, but skippable frames are still “charged nothing” at lines 852–853. |
| 10 | Source-span ownership | **RESOLVED** | Owned immutable spans: lines 286–293. |
| 11 | Typed-shell accounting | **NOT RESOLVED** | Lines 560–562 still use `sizeof(T)`, which does not cover every heap object. |
| 12 | Stale category/global-state summaries | **RESOLVED** | Lines 1755–1772 and Decision 30, lines 1855–1859. |

The Round-12 follow-ups have these dispositions:

- Decoded-key equality: **RESOLVED**.
- Codec-member traversal: **PARTIALLY RESOLVED** because of lines 852–853.
- Owned source spans: **RESOLVED**.
- Typed-shell oracle: **NOT RESOLVED**.
- Category and global-state summaries: **RESOLVED**.
- Schema Base-operation scratch treatment: **PARTIALLY RESOLVED**; caller-owned `Avro.Map` lookup cannot consume a retained operation budget.
- Accepted deferrals: **RESOLVED**.

## New findings

1. **[major] Random map seeds still decide memory and comparison acceptance.**

   **Claim:** The plan says acceptance and memory are independent of the random seed. Its actual overflow policy does not deliver that guarantee.

   **Evidence:** [Lines 474–483](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:474) allocate the sorted overflow index only for keys whose seeded probe sequence exceeds 64. Lines 551–560 charge its actual capacity. Line 676 confirms that each map has a random seed. The number of overflow entries, retained capacity, and compared bytes therefore vary with the seed. Near the ceiling, one seed can pass while another fails. The Writer preflight at lines 508–523 also omits `max_compare_bytes_per_byte`, so it cannot guarantee that an identical-limit Reader accepts the map.

   **Recommendation:** Use a deterministic representation, or reserve and charge a seed-independent worst case such as `nunique` overflow entries and worst-case comparison work. Add the comparison rule to Writer preflight. Test many seeds immediately below and above memory and comparison thresholds for `ntasks ∈ {1,2,8}`.

2. **[major] The symbol-admission map has incompatible capacity and work contracts.**

   **Claim:** The plan cannot implement the admission table as both a never-growing map and a dynamically growing process-wide table. Its incremental sorted overflow can also perform unbounded movement work.

   **Evidence:** Lines 474–477 say the admission hash index is never grown. Lines 581–583 say the same admission table grows by doubling. The table admits names incrementally up to its configured quota. Keeping an overflow permutation sorted after each insertion can shift `O(n)` entries per admission, producing `O(n²)` movement without a counter. The comparison rule counts compared key bytes, not moved index entries.

   **Recommendation:** Give admission a separate algorithm. Charge all capacity growth and entry movement. Use bounded chunked runs, periodic budgeted rebuilds, or another deterministic structure with bounded amortized work. Add a long-lived incremental-admission latency test and a capacity-boundary test.

3. **[major] The typed fast route cannot construct ordinary immutable Julia structs as written.**

   **Claim:** `jl_new_struct_uninit` followed by field stores only works for mutable structs. Julia structs are immutable by default.

   **Evidence:** [Plan lines 782–792](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:782) claim this is how `Serialization.deserialize` builds all structs. Julia’s implementation uses `jl_new_struct_uninit` plus `jl_set_nth_field` only for mutable structs. It uses a field vector and `jl_new_structv` for immutable structs ([Serialization.jl:1635](/Users/jacob.quinn/.julia/juliaup/julia-1.12.6+0.aarch64.apple.darwin14/Julia-1.12.app/Contents/Resources/julia/share/julia/stdlib/v1.12/Serialization/src/Serialization.jl:1635)). Read-only probes on Julia 1.10.11 and 1.12.6 failed with `setfield!: immutable struct ... cannot be changed`.

   **Recommendation:** Split construction by mutability. Use `jl_new_structv` or generated `Expr(:new, …)` for immutable structs, and charge any temporary field vector and boxing. Test mutable, immutable, zero-field, isbits, reference-bearing, and partially undefined layouts on every supported Julia version.

4. **[major] `sizeof(T)` undercharges approved mutable DTO instances.**

   **Claim:** The typed-shell formula does not cover heap-object storage for all admitted fast-route types.

   **Evidence:** [Lines 560–562](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:560) charge `sizeof(T)` for each struct. On Julia 1.10.11 and 1.12.6, an empty mutable struct has `sizeof(T) == 0` but `Base.summarysize(T()) == 8`. For 1,000 instances, the reference vector used 8,040 bytes and the populated vector used 16,040 bytes. The plan undercharges eight bytes per object, and the mandatory typed-shell oracle cannot pass.

   **Recommendation:** Measure and charge the actual object shell separately from inline `sizeof(T)`. Include zero-field mutable structs and arrays of them in the oracle. Keep inline immutable and heap-object formulas distinct.

5. **[major] The mandatory map gate requires behavior that v13 removed.**

   **Claim:** Phase 2 cannot become green under the current plan.

   **Evidence:** [Lines 1571–1573](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1571) require a map rebuild and a third-exceedance `LimitError`. The normative contract at lines 474–483 says maps never rebuild and probe overflow never fails. Lines 1564–1566 also expect collision memory to remain unchanged even though an overflow index can allocate storage.

   **Recommendation:** Replace the gate with forced-overflow tests that assert no rebuild, no probe error, correct last-wins semantics, actual overflow-capacity charging, and seed-independent memory/work acceptance.

6. **[major] Global lone-surrogate rejection violates the metadata and interoperability contracts.**

   **Claim:** The pre-scan rejects valid JSON escape syntax in `doc` and custom properties that the plan promises to preserve without validation.

   **Evidence:** Lines 269–273 say unknown and custom attributes are preserved verbatim and never validated. [Lines 303–309](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:303) globally require paired surrogate escapes. The Avro specification permits undefined attributes as metadata ([spec:35](/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:35)). RFC 8259 also notes that its JSON ABNF permits an unpaired escape such as `\uDEAD`. [RFC 8259 §8.2](https://www.rfc-editor.org/rfc/rfc8259#section-8.2)

   Read-only probes showed that Java avro-tools 1.12.2, avro-py 1.12.2, and fastavro 1.12.2 accept a record schema whose `doc` is `"\uD800"`. Java canonicalization succeeded. The Python implementations retained the property.

   **Recommendation:** Validate `\uXXXX` syntax lexically, but apply Unicode-scalar requirements contextually to Avro datum strings, names, map keys, and union labels. Preserve custom metadata in a raw or code-unit-safe frozen representation. Add schema and OCF fixtures for lone-surrogate metadata. If rejection is intentional, record an oracle exception and narrow the “never validated” and interoperability promises.

7. **[minor] The comparison budget cannot govern caller lookups after ownership transfer.**

   **Claim:** The plan includes every map lookup in a cumulative operation budget, but returned `Avro.Map` values no longer have such an operation.

   **Evidence:** Lines 485–487 include map lookups. Lines 593–597 transfer streamed values to the caller, and line 676 exposes `Avro.Map` as an `AbstractDict`.

   **Recommendation:** Charge construction and package-internal pre-transfer lookups only. Document a fixed per-call bound for caller `getindex` operations. Do not retain mutable operation budgets inside returned maps.

8. **[minor] Non-contiguous JSON inputs need an explicit normalization step.**

   **Claim:** `parseschema` accepts any `AbstractVector{UInt8}` and says byte vectors are used in place, but JSON.jl copies non-unit-stride inputs.

   **Evidence:** Lines 299–303 and the public API at line 1200 admit such sources. JSON.jl copies a source without pointer support or with non-unit stride at [lazy.jl:95](/Users/jacob.quinn/.julia/dev/JSON/src/lazy.jl:95). The accounting authority already includes owned copies of non-conforming sources at lines 573–580, so this is implementable but under-specified.

   **Recommendation:** Detect contiguous, one-based byte storage before `JSON.lazy`. Reserve and make an Avro-owned copy otherwise. Apply the same rule to `fromjson`. Add stepped-view and custom-vector boundary tests.

9. **[minor] Skippable-frame charging still contradicts the work rule.**

   **Claim:** A skippable Zstandard frame cannot both count as one value and be “charged nothing.”

   **Evidence:** Lines 492–496 count every codec member. Lines 852–853 say a skippable frame is charged nothing.

   **Recommendation:** Say it has no output or codec-workspace charge but consumes one member/value charge.

10. **[minor] Two authoritative summaries retain old terminology.**

   **Claim:** The budget-scope list omits `schema(x)`, and the JSON section still says “sorted-span” duplicate detection.

   **Evidence:** Lines 609–616 omit the public operation added at line 1204. Line 1068 conflicts with decoded-key comparison at lines 313–317.

   **Recommendation:** Add `schema(x)` to the budget-scope list and change “sorted-span” to “sorted decoded-key.”

11. **[minor] The fixture and latency summaries are broader than their executable gates.**

   **Claim:** “All Apache fixtures pass” is broader than the selected data-format corpus, and the Phase 2 constant list does not explicitly include `max_compare_bytes_per_byte`.

   **Evidence:** Line 1750 makes the broad fixture claim, while §8.1 excludes RPC and other out-of-scope material. Lines 525–531 list the provisional constants but omit the comparison constant.

   **Recommendation:** Say “all in-scope vendored Apache fixtures pass.” Explicitly calibrate and record `max_compare_bytes_per_byte` in the Phase 2 latency gate.

## Non-blocking follow-ups (if any)

Findings 7–11 are non-blocking implementation follow-ups. They do not change the chosen architecture.

The current scope remains credible. RPC/protocols, IDL, `big-decimal`, schema inference, append mode, borrowed views, and parallel compression remain acceptable deferrals.

## Milestone and gate assessment

- Phase 1 is blocked by the global lone-surrogate policy. It also needs the non-contiguous-source normalization text.
- Phase 2 is blocked by the impossible legacy map gate, immutable struct construction, typed-shell undercharging, and admission work.
- Phase 4a cannot prove the Writer/Reader invariant until comparison preflight and seed-independent map acceptance are defined.
- Phase 4b cannot safely ship the process-wide admission table under the current contradictory growth model.
- Phase 4c cannot prove identical acceptance across task counts while random seeds affect overflow memory and comparison work.
- The remaining schema, binary, JSON datum, single-object, sort, OCF, codec, resolution, canonical-form, fingerprint, logical-type, test, and benchmark coverage is strong.
- The §2.2 audit table is unchanged. I found no new inaccurate audit row.
- The PR-ready, merge-ready, RC-ready, and release-ready distinctions remain correct and in the right order.

Assumptions: I reviewed the checked-in DRAFT v13 as authoritative. I treated declared fixtures and gates as implementation deliverables, not review preconditions.

Decisions made without user direction: I treated a replacement as partially resolved when it still failed the prior guarantee. I kept the stated deferrals. I classified normalization and stale wording as minor because the authoritative accounting contract already permits an implementation-safe resolution.

Validation: I inspected DRAFT v13, response-12, the pinned Avro specification, JSON.jl, and Julia Serialization. Read-only probes covered Julia 1.10.11 and 1.12.6, non-contiguous JSON sources, typed struct construction and storage, and Java/avro-py/fastavro surrogate handling. I did not modify any file.

## Verdict

VERDICT: REVISE
