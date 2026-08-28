# Codex review round 15

DRAFT v15 closes most Round-14 defects, big dawg. It is still not implementation-ready. Five major objections remain.

All line references are to DRAFT v15.

## Disposition check (round-14 items)

### Primary carried rows

| Item | Status | Evidence |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | The accounting model is substantially complete at lines 427–670. JSON string materialization, category-(e) identity/work, and the Writer/Reader invariant remain unsound. |
| R1-9 — Freezing and hashing | **RESOLVED** | Transitive freezing and stored hashes: 244–251. Fresh defaults: 289–299. |
| R1-10 — Schema-driven compilation | **RESOLVED** | Dynamic plans and finite generic representations: 672–719. |
| R1-18 — Writer lifecycle | **RESOLVED** | Validation, poisoning, cleanup, abort, and atomic replacement: 1049–1090. |
| R1-20 — Parallel error determinism | **RESOLVED** | Ordered commit and lowest-index failure: 1014–1027. |
| R1-25 — Union representation/resolution | **RESOLVED** | Generic union identity and reader-directed resolution: 731, 770–776. |
| R2-new-6 — Symbol admission | **PARTIALLY RESOLVED** | Carry merges are deamortized at 508–521. The recent-buffer lookup and initial sort do not match the stated comparison allowance. |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowed contract** | Conventional, non-representation-preserving behavior is explicit at 829–836. |

### Other carried rows

| Item | Status | Evidence |
|---|---|---|
| R4-1 — Codec memory | **RESOLVED** | Complete member requirements and concatenated-member handling: 890–929. |
| R4-2 — Work/allocation budget | **PARTIALLY RESOLVED** | Core rules are at 497–670. JSON conversion and dense memo scans still escape the declared accounting. |
| R5-1 — Writer/Reader invariant | **PARTIALLY RESOLVED** | The preflight at 550–565 covers codec, graph, map, and final-output costs, but not all reader modes or repaired schemas. |
| R5-2 — Parallel transient memory | **RESOLVED** | Reservations, ordered commit, eviction, and peak gates: 956–1048. |
| R5-3 — Work amplification | **PARTIALLY RESOLVED** | Datum, comparison, and parallel work are bounded at 523–548 and 997–1008. Dense partner scans and admission lookup/sort remain incomplete. |
| R5-4 — Codec-cap contract | **RESOLVED** | 890–929 and 1060–1076. |
| R5-9 — Julia-derived names | **RESOLVED** | 805–828. |
| R5-10 — Typed Symbol paths | **RESOLVED** | 837–862 and 1331–1343. |

### Earlier numbered carried sets

- **Round 6:** 1 RESOLVED; 2 PARTIALLY RESOLVED because of the invariant below; 3–9 RESOLVED; 10 PARTIALLY RESOLVED; 11–13 RESOLVED. Relevant text: 430–565, 663–670, 890–1048.
- **Round 7:** all 11 items remain RESOLVED. Relevant text: 252–259, 466–495, 798–828, 956–1076.
- **Round 8:** all original items and follow-ups remain RESOLVED. Relevant text: 580–617, 890–1048, 1366–1381, 1482–1525.
- **Round 9:** XZ reservation, codec members, Scan attempts, parallel work, interoperability qualification, worker pool, RSS, and ownership are RESOLVED. Representation accounting remains PARTIALLY RESOLVED because of the probe-shell and final-capacity details at 593–607.
- **Round 10:** findings 1 and 3–8 are RESOLVED. Finding 2 is PARTIALLY RESOLVED because dense IDs have no complete representation/creation contract. Finding 9 is PARTIALLY RESOLVED because the fixed 4 KiB pre-probe reservation is not conservative.
- **Round 11:** findings 1–4 and 6–9 are RESOLVED for their original claims. Finding 5 is PARTIALLY RESOLVED because the complete reader peak is still not guaranteed for all advertised consumers.
- **Round 12:** findings 1–3, 5–7, 9–10, and 12 are RESOLVED. Findings 4 and 8 are PARTIALLY RESOLVED because the selected JSON decoder corrupts surrogate sequences. Finding 11 is PARTIALLY RESOLVED because of the 4 KiB probe reserve.
- **Round 13:** findings 1, 3, 5, and 7–11 are RESOLVED. Finding 2 remains PARTIALLY RESOLVED for admission lookup/sort. Finding 4 is PARTIALLY RESOLVED for the probe reserve. Finding 6 remains PARTIALLY RESOLVED for surrogate decoding and alias repair.

### Round-14 findings 1–6

| # | Status | Evidence |
|---:|---|---|
| 1 — Duplicate-heavy `Avro.Map` construction | **RESOLVED for the major defect** | The plan reserves an `npairs` permutation and `cld(npairs,2)` scratch, sorts every pair, and compacts last-wins in place at 497–504. A minor final-capacity contradiction remains at 593–600. |
| 2 — Surrogate contexts | **PARTIALLY RESOLVED** | Contextual policy is present at 315–328. JSON.jl does not provide the representation claimed there, and surrogate aliases are intentionally prevented from matching. |
| 3 — Pointer-incompatible strings | **RESOLVED** | Unsupported strings and byte vectors are normalized before `JSON.lazy` at 302–310; boundary gate at 1648. |
| 4 — Incremental category-(e) tables | **PARTIALLY RESOLVED** | Dense-ID arrays and partner vectors are specified at 264–268 and 628–638. ID ownership and scan-cost accounting remain incomplete. |
| 5 — Admission carry merges | **PARTIALLY RESOLVED** | Merges are deamortized and transactional at 508–521. Recent-buffer lookup and initial sorting are not covered by the claimed one-symbol bound. |
| 6 — Implicit vector growth | **RESOLVED** | Exact-capacity replacement with old-plus-new charging is mandatory at 645–656 and used by streamed columns at 956–958. |

### Round-14 follow-ups

| Follow-up | Status | Evidence |
|---|---|---|
| Check `npairs ≤ typemax(Int32)` | **RESOLVED** | 497–504. |
| Reserve before the per-`T` probe | **NOT RESOLVED** | 601–607 reserves only 4 KiB. Valid wide types can exceed that before measurement. This is minor under the requested severity rule. |
| Clarify the `Avro.Table` typed path | **RESOLVED** | 1335–1337 states that `Table` admits column names only and has no `T`. |
| Define `==`/`show` budget source | **PARTIALLY RESOLVED** | `==` uses recorded root limits at 264–268. The representation of those limits and `show` behavior remain unspecified. |
| Remove stale hashing wording | **RESOLVED** | The guarded path is consistently non-hashing at 497–521 and Decision 35. |
| Order Decisions 43 and 44 | **RESOLVED** | 1965–1971. |

## New findings

1. **[major] The pinned JSON decoder corrupts the surrogate representation required by v15.**

   **Claim:** The mechanism at lines 315–319 cannot preserve accepted metadata or reliably enforce Unicode-scalar rules.

   **Evidence:** The plan says JSON.jl converts `"\uD800"` to exactly `ed a0 80`. Read-only probes against pinned JSON.jl 1.7.1 instead produced:

   ```text
   "\uD800"       -> ed a0 80 30
   "\uD800\uD800" -> ef b0 80
   "\uFC00"       -> ef b0 80
   "\uDC00\uD800" -> f4 8f b0 80
   "\uDBFF\uDC00" -> f4 8f b0 80
   ```

   Thus distinct JSON strings collide, and invalid high/high or low/high sequences can become valid scalar strings. The defect is in the pinned [JSON.jl decoder](/Users/jacob.quinn/.julia/dev/JSON/src/utils.jl:175). Materializing long escaped strings through `JSON.PtrString` also performs allocations not covered by the source-buffer reservation.

   **Recommendation:** Decode guarded JSON strings directly from raw spans with an Avro-owned decoder. Pair only a high surrogate followed by a low surrogate. Preserve unmatched code units only in metadata contexts, using a code-unit-safe representation. Reject them in Avro string contexts. Test lone high/low, high/high, low/low, low/high, valid pairs, separated surrogates, and both demonstrated collision pairs in keys and values.

2. **[major] The alias rule does not deliver invalid-name repair.**

   **Claim:** Lines 323–324 permit arbitrary aliases syntactically but declare that a surrogate-bearing alias never matches. The same pre-scalar check prevents a corresponding legacy writer name from entering repair mode.

   **Evidence:** The pinned specification permits aliases that are not valid names and describes aliases as a mechanism for repairing illegal historical names at [spec lines 262–279](/private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:262). Avro-py and fastavro accept surrogate-bearing aliases. The plan promises `allow_invalid_names` and corresponding repair fixtures at 300–350, but its contextual rule makes this case unrepresentable.

   **Recommendation:** Store alias and repair-mode name text with code-unit-safe equality. Allow an invalid writer fullname to be parsed under `allow_invalid_names=true`, and permit the reader alias to match that exact text. Add direct writer-invalid/reader-alias resolution fixtures. If this support is intentionally refused, record a specification exception and narrow the repair guarantee.

3. **[major] Category-(e) dense identity and memo work are not executable as specified.**

   **Claim:** Equality and plans require persistent dense IDs, but schema nodes have no ID or recorded-limits field, no identity side table is allowed, and only parsed nodes are said to receive IDs. Partner-vector scans also charge one work unit regardless of the number of IDs inspected.

   **Evidence:** The schema layouts at 221–241 contain neither field. Lines 244–251 prohibit an identity side table. Lines 264–268 and 631–638 require dense IDs and recorded parse limits. Public constructors and `Avro.schema(T)` do not originate in the parser. A per-node partner vector can contain many reader IDs; charging a whole linear scan as one `max_resolution_work` unit does not bound its actual work.

   **Recommendation:** Give every graph root/node an explicit graph-local identity and recorded limits on every creation path, or assign operation-local IDs through a separately charged deterministic structure. Charge every inspected partner ID, not merely each scan call, or use a bounded direct/sorted index. Gate parser-created, constructor-created, and type-derived cyclic graphs, including one node paired with many partners near `max_resolution_work`.

4. **[major] The Writer/Reader invariant still has counterexamples under identical `Limits`.**

   **Claim:** Lines 550–565 promise that everything a Writer emits is accepted by a Reader, but the promised consumer and its external state are not fully defined.

   **Evidence:**

   - Streamed `Avro.Table` materialization uses old-plus-new replacement growth at 645–656 and 956–958. A final table can fit under the ceiling while its late replacement peak does not. The same file can therefore pass exact preallocation from a path/byte source and fail from non-seekable `IO`.
   - `Writer` accepts any `Schema` at 1049–1056. It has no repair-policy option and does not reject a schema parsed with `allow_invalid_names/defaults=true`. A default Reader reparses the embedded schema with both options false at 882–885.
   - Materialized table access also depends on symbol-admission state. Codec availability is separate from `Limits`.

   **Recommendation:** Name the guaranteed consumer precisely. Include the deterministic streamed-growth peak when materialization is covered, or use a two-pass/spooled representation. Reject repaired schemas by default at Writer construction or require an explicit matching repair policy. Qualify the invariant by codec availability and fresh/sufficient admission state. Gate one near-ceiling file through bytes, mapped path, `mmap=false`, and non-seekable `IO`, plus repaired-schema and exhausted-admission cases.

5. **[major] `Avro.write(dst, Avro.Rows(src))` cannot preserve schema identity under the stated precedence.**

   **Claim:** The documented round-trip path and the write API select incompatible schemas.

   **Evidence:** Lines 829–836 promise schema-preserving copies through `Avro.Rows`. Lines 1091–1095 derive the output schema only from explicit `schema=` or `Tables.schema(table)`. `Tables.Schema` cannot preserve named record, enum, fixed, general-union, or logical identity; decimal cannot be derived at all. Line 1265 also exposes only the file writer schema, although a source with `reader_schema` yields values governed by the effective reader schema.

   **Recommendation:** Define schema precedence as:

   1. Explicit `schema=`.
   2. Effective retained schema from an Avro source.
   3. Conventional `Tables.schema` derivation.

   Expose separate writer-schema and effective-reader-schema accessors. Gate direct copies containing enum, fixed, decimal, logical values, and general unions, plus evolved copies that add, drop, reorder, or promote fields.

## Non-blocking follow-ups (if any)

The following are minor or nit items. They can be settled during implementation after the major contracts above are revised.

- Make the retained `Avro.Map` permutation charge match reality: either retain `npairs` capacity after in-place compaction or reserve an exact `nunique` replacement plus overlap.
- Replace the fixed 4 KiB typed-probe reservation with a checked bound derived from `sizeof(T)` and layout information.
- Include pointer-incompatible `AbstractString` copies explicitly in accounting category (c), not only byte sources.
- Calibrate admission’s recent-buffer scan and sort against the Phase-2 comparison constants. Count every compared byte and moved entry. Add maximum-length common-prefix names at every flush/carry boundary.
- Include permutation/reference moves from map and JSON sorting, and fullname comparisons/moves from the named-type table, in the authoritative comparison-work definition.
- Correct `minsize`: a required recursive SCC can have no finite datum, and a union branch index is not always one byte.
- Define the test allocation hook as package allocation wrappers plus explicit dependency/native exceptions. `Profile.Allocs` alone cannot prove reserve-before-allocate.
- Replace “physically freed” after cancellation with a precise logical-unreachability or reusable-buffer contract. Julia reference release does not guarantee immediate RSS reduction.
- Qualify the 64 KiB cancellation interval for Snappy, whose pinned decoder performs one whole-block native call.
- Define whether duplicate map keys retain their first iteration position or move to their last position under last-wins.
- Either make frozen backing unreachable or narrow “transitively immutable” to supported public mutators; `getfield` can expose mutable backing in Julia.
- Add a do-block/open pattern or documented finalizer fallback for `Rows` and `Reader`.
- Add `decimal_byteorder` to `Reader`, or state that legacy decimal iteration requires `Rows`/`Table`. Add public `SchemaCache.register!` and `lookup` signatures used by Appendix B.
- Clarify invalid surrogate-bearing defaults under `allow_invalid_defaults=true`: retain them as `valid=false` rather than rejecting them during contextual validation.
- Replace “verbatim” metadata round-tripping with “code-unit-preserving” unless raw lexical spans are retained for every metadata string.
- Define ambiguous field-alias resolution when one reader field aliases multiple writer fields.
- Add duplicate OCF `avro.schema` and `avro.codec` keys to the malformed corpus and specify rejection or last-wins.
- Define standalone `{"type":"error"}` behavior while RPC remains deferred.
- State whether `UnionValue` and `EnumValue` indices are Julia one-based or Avro wire zero-based.
- Add identity-bearing union branch-recovery fixtures for multiple records, enums, and fixed branches.
- Define schema-default equality/hash semantics for signed zero and quoted non-finite values.
- Require a JSON-string type for a supplied but fullname-ignored `namespace`, or record the Java-compatible broader acceptance as a deliberate spec exception.
- Bound the “10k mutations for every fixture and generated datum” gate with a fixed sampled case count. Separate required CI fuzzing from extended fuzzing.
- Rename the ≤10-second latency statement as a worst-density gate, not a guarantee for every admitted streamed file.
- State `Base.show` scratch behavior for schemas created with raised limits.
- Extend the deferred RPC checklist with effective error unions, big-endian framing and terminator rules, and HTTP behavior.
- Correct the review-log ordering if Round 14 still precedes Round 13.

## Milestone and gate assessment

Assumptions:

- DRAFT v15 is authoritative.
- Declared fixtures, manifests, and gates are deliverables.
- The fixed compatibility pins and accepted deferrals remain in scope.
- Caller-owned memory remains outside the package ceiling as documented.

Decisions made without user direction:

- I treated bounded constants, scratch-size formulas, and wording corrections as minor, per the requested severity rule.
- I did not reopen RPC, IDL, inference, append, borrowed views, `big-decimal`, or parallel-compression deferrals.
- I treated schema preservation and the Writer/Reader promise as public API guarantees, not aspirational test goals.

Assessment:

- Phase 1 is blocked by the surrogate decoder, alias-repair contract, and incomplete dense-ID model.
- Phase 2 cannot close the schema/equality work gates until partner scans are truly bounded.
- Phase 4a is blocked by repaired-schema output and schema provenance.
- Phase 4b cannot prove identical source-mode acceptance until streamed growth is included or the invariant is narrowed.
- The remaining binary encoding, logical types, canonical/fingerprint behavior, codec framing, interoperability corpus, parallel scheduling, benchmark, CI, and release-order gates are otherwise strong and correctly sequenced.

Validation was read-only. I inspected DRAFT v15, response-14, Round-14 review, the pinned specification, JSON.jl 1.7.1, Snappy.jl, Tables.Scan, and Julia Base. Read-only probes covered JSON surrogate collisions and storage/layout behavior on Julia 1.10.11 and 1.12.6. No file was created or modified.

## Verdict

Five major findings remain. DRAFT v15 requires revision.

VERDICT: REVISE
