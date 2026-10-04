# Codex review round 12

All line references are to [DRAFT v12](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1), big dawg. Broad resource rows remain partial when a new defect still breaks the same guarantee.

## Disposition check (round-11 items)

### Primary carried rows

| Item | Status | Evidence |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | The five-category model is much stronger at lines 509–584. Map rebuild capacity, schema-IO buffering, JSON work, typed constructors, and `schema(x)` still violate it. |
| R1-9 — Freezing and hashing | **RESOLVED** | Transitive freezing and stored hashes: lines 244–251. Fresh default materialisation: lines 284–292. |
| R1-10 — Schema-driven compilation | **RESOLVED** | Generic and column plans remain schema-independent at lines 588–620. |
| R1-18 — Writer lifecycle | **RESOLVED** | Validation, atomic replacement, poisoning, cleanup, and abort: lines 953–994. |
| R1-20 — Parallel error determinism | **RESOLVED** | Ordered cumulative failures and lowest-index selection: lines 926–931. |
| R1-25 — Union oracle | **RESOLVED** | Generic union representation: lines 645–658. Resolution expectations: lines 675–689. |
| R2-new-6 — Symbol admission | **RESOLVED** | All typed paths are covered at lines 594–599, 762–766, and 1235–1246. |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowed contract** | The conventional, non-round-trip contract is explicit at lines 742–749. |

### Other carried rows

| Item | Status | Evidence |
|---|---|---|
| R4-1 — Codec memory | **RESOLVED** | Complete per-member requirements and reservations: lines 794–833. |
| R4-2 — Work/allocation budget | **PARTIALLY RESOLVED** | Lines 509–584 add the required categories, but findings 1, 3–5, and 7 still bypass or contradict them. |
| R5-1 — Writer/Reader invariant | **PARTIALLY RESOLVED** | The category-(e) peak defect is fixed at lines 479–494. Seed-dependent map failure can still make an identical-limit Reader reject Writer output. |
| R5-2 — Parallel transient memory | **PARTIALLY RESOLVED** | The parallel algorithm at lines 860–925 is coherent, but committed maps can retain uncharged rebuilt indexes. |
| R5-3 — Work amplification | **PARTIALLY RESOLVED** | Datum and map-probe bounds exist at lines 457–477. Sorted JSON-key comparison work is not counted. |
| R5-4 — Codec-cap contract | **RESOLVED** | Reader: lines 794–833. Writer: lines 965–980. |
| R5-9 — Julia-derived names | **RESOLVED** | Lines 718–741. |
| R5-10 — Typed Symbol admission | **RESOLVED** | Lines 594–599, 762–766, and 1235–1246. |

### Round-6 carried findings

| # | Item | Status | Evidence |
|---:|---|---|---|
| 1 | Semaphore memory/liveness | **PARTIALLY RESOLVED** | Core liveness is fixed at lines 860–925; rebuilt map storage remains undercharged. |
| 2 | Writer/Reader invariant | **PARTIALLY RESOLVED** | Reader peak preflight is fixed at lines 479–494; random map rejection remains. |
| 3 | Unsafe default memory | **RESOLVED** | Fixed defaults and guard: lines 426–455. |
| 4 | Compressed-size work precheck | **RESOLVED** | Explicitly prohibited at lines 464–477. |
| 5 | Whole-file `mmap=false` allocation | **RESOLVED** | Sequential streaming: lines 843–846. |
| 6 | Self-alias gate | **RESOLVED** | Lines 307–310. |
| 7 | `fromjson` admission | **RESOLVED** | Lines 1025–1034 and 1190–1192. |
| 8 | Julia name policy | **RESOLVED** | Lines 718–741. |
| 9 | Codec-cap cases | **RESOLVED** | Lines 794–833. |
| 10 | Work ceilings | **PARTIALLY RESOLVED** | Datum and probe work are bounded, but duplicate-key sorting has no counter. |
| 11 | JSON-depth symmetry | **RESOLVED** | Lines 396 and 1025–1031. |
| 12 | Duplicate named union branches | **RESOLVED** | Lines 309–310. |
| 13 | Attribute wording | **RESOLVED** | Lines 267–283. |

### Round-7 items

All Round-7 items are **RESOLVED**:

- Final-column allocation: lines 867–876.
- Writer codec workspace and frame requirements: lines 965–980.
- Actual reservations and priority mechanism: lines 879–925.
- Portable memory default: lines 404 and 426–441.
- Constructor relations without fixed `W`: lines 443–455.
- Empty-union `minsize`: lines 256–259.
- Peak-RSS method: lines 934–952.
- Alias normalisation: lines 252–255.
- Streamed compressed input: lines 843–846.
- `Nothing → null`: lines 711–713.
- Sanitisation policy: lines 718–741.

### Round-8 items and follow-ups

All are **RESOLVED**:

- Nullable-column storage: lines 509–541 and 867–876.
- Complete Zstandard requirement: lines 802–807.
- Eviction/work bound: lines 900–912.
- Parallel RSS source and sampler: lines 934–952.
- Non-UTF-8 OCF metadata exception: lines 1332–1342.
- Filtered Scan sizing and per-pass attempts: lines 1271–1284.
- `fixed(0)` decimal handling: lines 279–283.
- Estimator dependencies: lines 1620–1625.
- Bzip2 floor: lines 443–448.
- Cooperative cancellation: lines 896–912.
- Fixed and charged worker pool: lines 860–866.
- Storage/payload transfer: lines 509–570 and 918–925.
- CPython pin and accepted deferrals: lines 1428, 1650–1654, and 1758–1759.

### Round-9 items and follow-ups

| Item | Status | Evidence |
|---|---|---|
| XZ cap reservation | **RESOLVED** | Lines 797–802. |
| Representation storage formulas | **PARTIALLY RESOLVED** | Ordinary storage is measured, but rebuilt map index capacity is absent. |
| Concatenated codec members | **RESOLVED** | Lines 810–835. |
| Filtered Scan attempts | **RESOLVED** | Lines 1271–1284 and 1550–1552. |
| Deterministic work versus CPU time | **RESOLVED** | Lines 900–912. |
| §13 interoperability qualification | **RESOLVED** | Lines 1332–1342 and 1706–1710. |
| Fixed worker pool | **RESOLVED** | Lines 860–866. |
| RSS primitive | **RESOLVED** | Lines 934–952. |
| Ownership, task charging, RSS, and deferral follow-ups | **RESOLVED** | Lines 509–570, 548–552, 934–952, and 1758–1759. |

### Round-10 findings 1–9

| # | Status | Evidence |
|---:|---|---|
| 1 — `Dict` capacity/collision rehash | **PARTIALLY RESOLVED** | `Base.Dict` is gone from the bounded generic path. The replacement map has contradictory rebuild accounting; see findings 1–2. |
| 2 — Schema/plan graph accounting | **PARTIALLY RESOLVED** | Category (e) exists at lines 552–560. `schema(x)` and the streaming schema parser remain incomplete. |
| 3 — Identity-bearing storage oracle | **RESOLVED** | Schema references are excluded and charged once at lines 531–534. |
| 4 — XZ Stream Padding | **RESOLVED** | Lines 817–829. |
| 5 — Stale clauses | **PARTIALLY RESOLVED** | Codec and retry text is fixed. Decision 30 still omits category (e), lines 1810–1814. |
| 6 — Zstandard empty/skippable frames | **RESOLVED** | Lines 802–816 and 826–833. |
| 7 — Output/yield ownership | **RESOLVED** | Lines 541–570 and 918–925. |
| 8 — RSS sampler | **RESOLVED** | Lines 934–952. |
| 9 — Runtime layout guard | **RESOLVED for the original defect** | Shell constants are measured and checked at lines 509–534 and 1533–1534. |

The Round-10 output, RSS, Zstandard, and accepted-deferral follow-ups are resolved. The stale-accounting follow-up is **PARTIALLY RESOLVED** because of decision 30.

### Round-11 new findings 1–9

| # | Status | Evidence |
|---:|---|---|
| 1 — Exact `Avro.Map` formula | **PARTIALLY RESOLVED** | The shell is measured, but lines 457–460 permit doubled indexes while lines 523–529 and 644 charge only `tablesz(npairs)`. |
| 2 — JSON.jl unbudgeted `Set` | **RESOLVED for the reported `Set` defect** | JSON.jl’s duplicate tracker is disabled; Avro-owned spans are used at lines 293–301 and 552–560. The replacement has separate defects below. |
| 3 — Typed conversion boundary | **PARTIALLY RESOLVED** | The semantic route is outside the ceiling, but the fast-route eligibility at lines 750–761 still permits arbitrary Julia constructors. |
| 4 — Schema-operation limits | **PARTIALLY RESOLVED** | Most APIs gained `limits=`, but value-level `schema(x)` at line 1167 did not. |
| 5 — Writer/Reader category-(e) peak | **RESOLVED for the original defect** | Lines 479–494 and Phase 4a line 1681. |
| 6 — Map-probe work | **RESOLVED for the finite work bound** | Lines 457–462 cap probes and retries. Seed-dependent acceptance and the unraiseable failure are separate defects. |
| 7 — Map capacity terms | **PARTIALLY RESOLVED** | `npairs` and `nunique` are defined; retained rebuilt index capacity is not. |
| 8 — Stale summaries | **PARTIALLY RESOLVED** | Decision 30, lines 1810–1814, says five categories but lists four. |
| 9 — Layout `Ref` exception | **PARTIALLY RESOLVED** | Principle 5 records both exceptions at lines 207–213, but §13 line 1726 still names only admission state. |

Round-11 map-term, stale-text, and layout-state follow-ups are **PARTIALLY RESOLVED**. The accepted deferrals remain **RESOLVED** at lines 1758–1759.

## New findings

1. **[major] Collision rebuilds invalidate exact `Avro.Map` accounting.**

   **Claim:** A successful rebuild can retain an index two or four times larger than the charged capacity.

   **Evidence:** Lines 457–460 require a fresh seed and double capacity after a 64-probe exceedance. Lines 523–529 charge exactly `4 × tablesz(npairs)` and say the index is never grown. Line 644 repeats the fixed capacity. The gate at lines 1527–1535 also says collision memory is unchanged while requiring rebuild tests.

   **Recommendation:** Store and charge the actual `index_capacity`. Reserve the old and new indexes during a rebuild. Release the old charge only after replacement. Update Writer preflight, the storage oracle, and collision tests.

2. **[major] Random map retries make acceptance nondeterministic and produce an unraiseable `LimitError`.**

   **Claim:** The same valid map can succeed or fail according to random seeds or task assignment.

   **Evidence:** Lines 457–462 use fresh random seeds and fail after a third exceedance. Parallel acceptance must match sequential acceptance at lines 894–905 and 1539–1542. `Limits` at lines 391–423 has no probe-distance or rebuild field. The failure therefore cannot name a keyword that the caller can raise, contrary to lines 439–441 and 504–507.

   **Recommendation:** Use a deterministic, non-failing fallback after bounded probing. A sorted overflow index is one option. If rejection remains possible, add explicit raisable limits and deterministic seed derivation. Random retries must not decide whether valid input is accepted.

3. **[major] `parseschema(::IO)` cannot execute its stated safe two-pass algorithm.**

   **Claim:** A non-seekable input is either consumed by the pre-scan or fully allocated before `max_schema_bytes` is enforced.

   **Evidence:** The API accepts `IO` at line 1163. Lines 293–301 require a no-allocation lexical pass followed by `JSON.lazy`. The pinned JSON implementation documents that `lazy(io)` fully reads the stream and implements it as `lazy(Base.read(io))` at [lazy.jl](/Users/jacob.quinn/.julia/dev/JSON/src/lazy.jl:88).

   **Recommendation:** Incrementally read at most `max_schema_bytes + 1` into an Avro-owned, incrementally reserved buffer. Perform depth and UTF-8 checks while filling it. Then call `JSON.lazy` on that buffer. Test a non-seekable over-limit stream and both exact byte boundaries.

4. **[major] Schema and datum JSON can accept invalid UTF-8.**

   **Claim:** The selected parser accepts invalid byte sequences as Julia strings, and v12 does not require an earlier validation pass.

   **Evidence:** The specification represents schemas as JSON and defines `string` as a “unicode character sequence” (spec lines 35–56); binary strings must contain UTF-8 data (spec line 320). The v12 pre-scan at lines 295–301 does not require UTF-8 validation. JSON.jl scans bytes without UTF-8 validation and uses `unsafe_string` for unescaped values at [lazy.jl](/Users/jacob.quinn/.julia/dev/JSON/src/lazy.jl:446).

   Read-only probes returned invalid Julia strings rather than errors for both a top-level JSON string containing `0xff` and an object whose key contained `0xff`.

   **Recommendation:** Require strict UTF-8 validation before lazy traversal. Validate malformed escapes and unpaired surrogates too. Add negative cases for schema keys, names, properties, defaults, datum strings, map keys, and union labels. Return `SchemaError` or `DataError`.

5. **[major] Sorted duplicate-key detection has no executable work bound.**

   **Claim:** Wide objects with long common-prefix keys can consume comparison work not represented by any limit.

   **Evidence:** Lines 300–301 say the `O(k log k)` sort is counted by “the work rule.” Lines 464–477 only constrain semantic value count relative to input bytes. `Budget` at lines 504–507 has no key-comparison or compared-byte counter. The Phase-2 latency gate does not cover wide schema or datum JSON objects.

   **Recommendation:** Add a raisable JSON/schema work limit that charges comparisons and decoded key bytes. Alternatively, use a bounded radix algorithm. Add wide-object and long-common-prefix latency fixtures.

6. **[major] Value-level `Avro.schema(x)` still has no reachable budget.**

   **Claim:** A caller cannot raise the ceiling for a legitimate value-derived schema.

   **Evidence:** Lines 742–749 delegate plain values to type-derived schema construction. Budget scopes at lines 577–584 require schema-building operations to own a budget. The public signature at line 1167 has no `limits=`. Phase 1 nevertheless says every schema operation honours `limits=`.

   **Recommendation:** Add `Avro.schema(x; limits=Limits())`. Pass one shared internal `Budget` when it delegates to `schema(typeof(x))` or nested derivation.

7. **[major] The typed fast route can invoke arbitrary user constructors inside the exact ceiling.**

   **Claim:** Absence of StructUtils hooks does not make positional construction allocation-free or package-controlled.

   **Evidence:** Lines 750–761 admit concrete structs when no StructUtils hook applies and then use direct positional construction. Julia inner and outer constructors are arbitrary methods. They can allocate, mutate state, or return errors without any StructUtils customization.

   **Recommendation:** Restrict the exact route to `NamedTuple` and representations constructed without user dispatch. Otherwise require an explicit trusted-constructor trait. Send ordinary structs through the post-transfer semantic route. Add a custom positional-constructor regression test.

8. **[minor] Duplicate-key equality is undefined after JSON unescaping.**

   **Claim:** Sorting raw spans would miss equal keys with different source spellings, such as `"type"` and `"\u0074ype"`.

   **Evidence:** Lines 300–301 and decision 39 only say “key spans.” JSON.jl’s former duplicate mode compared decoded values.

   **Recommendation:** Define ordering and equality over decoded Unicode scalar sequences. Test simple escapes, BMP escapes, surrogate pairs, and mixed literal/escaped spellings.

9. **[minor] Codec-member traversal is not charged as work.**

   **Claim:** A compressed block can contain many empty or skippable Zstandard frames that produce no values or decompressed bytes.

   **Evidence:** Lines 810–829 accept such frames. Lines 464–477 do not count codec members. The hard compressed-block cap bounds the total input, but not the number of member operations.

   **Recommendation:** Count members and member-header bytes. Add a dense empty/skippable-frame latency case.

10. **[minor] Exact source-span ownership is implicit.**

    **Claim:** Exact invalid-default re-emission cannot safely retain slices into a caller-owned mutable buffer.

    **Evidence:** `DefaultValue` records a source span at lines 284–287, and lines 290–292 require verbatim re-emission. The ownership of the source bytes is not defined.

    **Recommendation:** Copy retained source slices into immutable, category-(e)-charged storage. Test mutation of the caller’s input after parsing.

11. **[minor] Exact typed shells lack a stated formula and oracle.**

    **Claim:** The plan claims exact charging for approved structs and `NamedTuple`s without stating their shell charge.

    **Evidence:** Lines 536–541 include these representations. The oracle at lines 1523–1525 covers generic `E`, not generated fast-route layouts.

    **Recommendation:** Charge `sizeof(T)` for the typed shell plus referenced payload. Add generated nested, padded, mutable, reference-bearing, and nullable layouts to the supported-version oracle.

12. **[minor] Two summaries still contradict the normative model.**

    **Claim:** Decision 30 omits category (e), and §13 omits the write-once layout-state exception.

    **Evidence:** Lines 1810–1814 list only four categories after saying five. Line 1726 says only admission state is global, contrary to lines 207–213.

    **Recommendation:** Correct both summaries. Also state whether schema `==` is excluded from the limits contract or uses a bounded visited-pair table.

## Non-blocking follow-ups (if any)

Findings 8–12 are non-blocking. They can be completed during implementation:

- Define decoded JSON-key equality.
- Charge dense codec-member traversal.
- Own exact source spans.
- Add the typed-shell oracle.
- Correct decision 30 and the concurrency summary.
- Clarify budget treatment for schema `==`, `show`, and similar Base operations.

The explicit scope remains sound. RPC, IDL, `big-decimal`, schema inference, append mode, borrowed views, and parallel compression remain acceptable deferrals.

## Milestone and gate assessment

Phase 1 is not executable as written. It requires bounded non-seekable IO buffering, UTF-8 validation, decoded duplicate-key equality, JSON sort-work accounting, and `limits=` on `schema(x)`.

Phase 2 is blocked by the contradictory map capacity model, seed-dependent acceptance, and unsafe typed-constructor eligibility. Its storage and collision gates cannot establish the stated guarantee yet.

Phase 4a’s category-(e) Writer preflight is now correctly placed and materially stronger. Phase 4c’s reservation, liveness, error-ordering, and per-pass attempt gates are otherwise executable, but random map construction can still violate its identical-acceptance gate.

The specification surface outside the JSON defects is strong. I found no new problem in schema names, aliases, defaults, unions, binary encoding, single-object encoding, sort order, schema resolution, canonical form, fingerprints, OCF framing, codec semantics, or the implemented logical types. The audit table has no new incorrect row. The Java and fastavro gates remain well designed.

Assumptions: I treated declared fixtures and gates as future deliverables, not current preconditions. I accepted the explicit deferrals. I treated an exact previous defect as resolved when its mechanism was removed, even if its replacement created a separately numbered defect.

Decisions without user direction: I classified the IO, UTF-8, JSON-work, map, schema API, and constructor defects as major because they cause unsafe defaults, nondeterministic rejection, a specification violation, or an unexecutable stated gate. I treated the remaining contract details as minor.

Validation: I reviewed DRAFT v12, `response-11.md`, the Round-11 review, the pinned Avro specification, JSON.jl’s lazy parser, the API, resource model, tests, decisions, and milestones. I ran only read-only in-memory Julia/CPython probes. I did not modify any file.

## Verdict

VERDICT: REVISE
