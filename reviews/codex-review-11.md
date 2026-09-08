# Codex review round 11

## Disposition check (round-10 items)

All line references are to [DRAFT v11](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1). Broad resource rows remain partial when a new defect still breaks the same guarantee.

### Primary carried rows

| Item | Status | Evidence |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | The five-category model is at L488–554. The map formula, parser allocations, typed-target exception, and work accounting remain incomplete; see findings 1–6. |
| R1-9 — Freezing and hashing | **RESOLVED** | Transitive freezing and stored hashes are at L240–247; defaults are materialised freshly at L280–288. |
| R1-10 — Schema-driven compilation | **RESOLVED** | Generic plans remain schema-independent at L558–590. |
| R1-18 — Writer lifecycle | **RESOLVED** | Validation, atomic replacement, poisoning, cleanup, and abort are specified at L917–958. |
| R1-20 — Parallel error determinism | **RESOLVED** | Ordered failures and lowest-index selection are specified at L890–895. |
| R1-25 — Union oracle | **RESOLVED** | Direct branch and output expectations remain at L645–659. |
| R2-new-6 — Symbol admission | **RESOLVED** | Binary, JSON, Tables, and typed routes are covered at L567–569, L697–700, L991–996, and L1146–1155. Collision work in the shared map is a separate new finding. |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowed contract** | L712–719 retains the accepted conventional-schema contract. |

### Other carried rows

| Item | Status | Evidence |
|---|---|---|
| R4-1 — Codec memory | **RESOLVED** | XZ reserves the configured cap and Zstandard uses complete reported requirements at L758–771. |
| R4-2 — Work/allocation budget | **PARTIALLY RESOLVED** | Categories are substantially complete at L488–554, but findings 1–3 and 6 still permit unreserved memory or work. |
| R5-1 — Writer/Reader invariant | **PARTIALLY RESOLVED** | L465–473 still omits the reader’s retained schema/header graph and parser peak; see finding 5. |
| R5-2 — Parallel transient memory | **PARTIALLY RESOLVED** | L824–889 is coherent, but the `Avro.Map` formula under-reserves committed output. |
| R5-3 — Work amplification | **PARTIALLY RESOLVED** | The per-pass rule is correct at L864–876 and L1234–1247. Phase 4c and the risk list still say “per block,” and map probes are uncharged. |
| R5-4 — Codec-cap contract | **RESOLVED** | L758–797 and L928–944. |
| R5-9 — Julia-derived names | **RESOLVED** | L688–711. |
| R5-10 — Typed Symbol admission | **RESOLVED** | All typed binary and JSON routes retain admission. |

### Round-6 carried findings

| # | Item | Status | Evidence |
|---:|---|---|---|
| 1 | Semaphore memory/liveness | **PARTIALLY RESOLVED** | The parallel algorithm is fixed at L824–889, but map and parser allocations still bypass exact accounting. |
| 2 | Writer/Reader invariant | **PARTIALLY RESOLVED** | L465–473 omits category-(e) reader memory. |
| 3 | Unsafe default memory | **RESOLVED** | Fixed 256 MiB default and available-memory guard at L395–434. |
| 4 | Compressed-size work precheck | **RESOLVED** | Explicitly prohibited at L450–463. |
| 5 | Whole-file `mmap=false` allocation | **RESOLVED** | Sequential streaming at L807–810. |
| 6 | Self-alias gate | **RESOLVED** | L295–304. |
| 7 | `fromjson` admission | **RESOLVED** | L947–954 and L1146–1155. |
| 8 | Julia-derived name policy | **RESOLVED** | L688–711. |
| 9 | Codec-cap boundary cases | **RESOLVED** | L758–797. |
| 10 | Work ceilings | **PARTIALLY RESOLVED** | Datum work is bounded at L450–480, but map probing is not counted. |
| 11 | JSON-depth symmetry | **RESOLVED** | L386–389 and L947–954. |
| 12 | Duplicate named union branches | **RESOLVED** | L295–304. |
| 13 | Attribute wording | **RESOLVED** | Contextual attribute treatment at L263–279. |

### Round-7 findings and sanitisation follow-up

| Item | Status | Evidence |
|---|---|---|
| 1 — Final-column allocation | **RESOLVED** | Exact column capacity is specified at L832–840. |
| 2 — Writer codec workspace/frame requirement | **RESOLVED** | L928–944. |
| 3 — Fixed worst-case permit mechanism | **RESOLVED** | Replaced by actual reservations and priority eviction at L843–876. |
| 4 — Fixed 1 GiB default | **RESOLVED** | The default is now 256 MiB at L395–434. |
| 5 — Constructor/runtime `W` mismatch | **RESOLVED** | Fixed `W` is gone; constructor relations are at L436–448. |
| 6 — Empty-union `minsize` | **RESOLVED** | L252–255. |
| 7 — Peak-RSS method | **RESOLVED** | L896–916. |
| 8 — Alias normalisation | **RESOLVED** | L248–251. |
| 9 — Streamed compressed-input ownership | **RESOLVED** | L807–810 and L843–851. |
| 10 — `Nothing` mapping | **RESOLVED** | L643–653. |
| Sanitisation policy | **RESOLVED** | L688–711. |

### Round-8 findings and follow-ups

| Item | Status | Evidence |
|---|---|---|
| 1 — Nullable-column storage | **RESOLVED** | Isbits-union tag storage is counted at L488–511 and L832–840. |
| 2 — Zstandard complete requirement | **RESOLVED** | L766–771. |
| 3 — Eviction/work bound | **PARTIALLY RESOLVED** | Detailed per-pass accounting is correct; Phase 4c and the risk text remain unqualified. |
| 4 — RSS gate used a sequential source | **RESOLVED** | L896–916 uses a caller-owned faulted byte source and confirms parallel activity. |
| 5 — Non-UTF-8 metadata exception | **RESOLVED** | The interoperability exception remains recorded and the summary is qualified. |
| 6 — Filtered Scan sizing/work | **PARTIALLY RESOLVED** | The global filter pass and four-attempt absolute bound are correct at L1234–1247; stale summaries remain. |
| 7 — `fixed(0)` decimal | **RESOLVED** | L268–279. |
| 8 — Estimator dependencies | **RESOLVED** | Direct checked symbols are specified at L1577–1589. |
| Bzip2 floor | **RESOLVED** | L436–443. |
| 16 MiB RSS workload | **RESOLVED** | L913–916. |
| Cooperative cancellation | **RESOLVED** | L864–876. |
| Safe task cap | **RESOLVED** | L824–830. |
| Storage/payload separation | **RESOLVED** | The ownership categories and transfer rule are at L488–537. |
| CPython pin | **RESOLVED** | L1608–1611. |
| Accepted deferrals | **RESOLVED** | L1716–1717. |

### Round-9 findings and follow-ups

| # | Item | Status | Evidence |
|---:|---|---|---|
| 1 | XZ cap reservation | **RESOLVED** | L761–765. |
| 2 | Representation storage formulas | **PARTIALLY RESOLVED** | The framework is improved, but the literal `Avro.Map` formula is false; see finding 1. |
| 3 | Concatenated codec members | **RESOLVED** | L774–797 covers members, XZ padding, and Zstandard skippable frames. |
| 4 | Filtered Scan attempts | **PARTIALLY RESOLVED** | L1234–1247 is correct; L1641 and L1807–1809 remain stale. |
| 5 | CPU timing gate | **RESOLVED** | Deterministic work is gated; measured time is informational. |
| 6 | §13 interoperability qualification | **RESOLVED** | L1664–1668 refers to recorded exceptions. |
| 7 | Worker pool | **RESOLVED** | L824–830. |
| 8 | RSS primitive | **RESOLVED** | L896–916 uses reservation hooks, current RSS, `Sys.maxrss`, and GC live-byte observations. |
| Qualify §13 | **RESOLVED** | L1664–1668. |
| Cap and charge worker tasks | **RESOLVED** | L824–830. |
| Disjoint storage/payload ownership | **RESOLVED** | L488–537. |
| Define RSS sampler | **RESOLVED** | L896–916. |
| Preserve deferrals | **RESOLVED** | L1716–1717. |

### Round-10 findings 1–9

| # | Item | Status | Evidence |
|---:|---|---|---|
| 1 | `Dict` capacity and collision rehash | **PARTIALLY RESOLVED** | Generic maps no longer use `Dict` (L601–614), but the replacement formula is too small, its probe work is unbounded, and typed `Dict` remains approximate. |
| 2 | Schema/plan graph accounting | **PARTIALLY RESOLVED** | Category (e) and scopes were added at L525–554. Parser dependency allocations, public limit reachability, and the Writer invariant remain incomplete. |
| 3 | Identity-bearing `summarysize` oracle | **PARTIALLY RESOLVED** | `exclude=Avro.Schema` at L506–510 fixes the identity issue, but L1485 still states the raw oracle and the map formula makes the complete gate fail. |
| 4 | XZ Stream Padding | **RESOLVED** | L781–797, including positive and negative fixtures. |
| 5 | Stale clauses | **PARTIALLY RESOLVED** | The old bytes and XZ rules were removed. Decision 30, Phase 4c, and the risk list remain stale. |
| 6 | Short/skippable Zstandard frames | **RESOLVED** | L766–780, L792–797, and required symbol checks at L1579–1583. |
| 7 | Output-buffer and yield ownership | **RESOLVED** | L514–521 and L533–537. |
| 8 | RSS sampler | **RESOLVED** | L896–916. |
| 9 | Runtime layout guard | **PARTIALLY RESOLVED** | L493–505 and L1619–1622 add runtime probes, but the hard-coded `Avro.Map` base is neither measured nor correct. |

### Round-10 follow-ups

| Follow-up | Status | Evidence |
|---|---|---|
| Remove stale accounting/retry text | **PARTIALLY RESOLVED** | See L1485, L1641, L1768–1772, and L1807–1809. |
| Add short/skippable Zstandard fixtures | **RESOLVED** | L774–797. |
| State output and yield ownership | **RESOLVED** | L514–537. |
| Harden RSS checks | **RESOLVED** | L896–916. |
| Harden runtime-layout checks | **PARTIALLY RESOLVED** | The map shell is still hard-coded incorrectly. |
| Keep accepted deferrals | **RESOLVED** | L1716–1717. |

## New findings

1. **[major] The exact `Avro.Map` formula under-reserves its stated layout.**

   **Claim:** The replacement for `Dict` cannot satisfy its own storage oracle or reserve-before-allocation rule.

   **Evidence:** L501–505 specifies three vectors plus a seed and charges:

   ```text
   128 + n × (8 + slotbytes(T)) + 4 × tablesz(n)
   ```

   I modelled that exact layout and ran `Base.summarysize(...; exclude=String)` on Julia 1.10.11 and 1.12.6:

   ```text
   n=0,    capacity=16:   actual=216,   formula=192,   deficit=24
   n=1,    capacity=16:   actual=232,   formula=208,   deficit=24
   n=11,   capacity=32:   actual=456,   formula=432,   deficit=24
   n=1024, capacity=2048: actual=24728, formula=24704, deficit=24
   ```

   Three array headers and the map wrapper require a 152-byte base, not 128. Even a three-vector variant without the seed needs more than 128. The mandatory oracle at L1485–1496 therefore cannot pass as written.

   **Recommendation:** Measure the complete shell from an actual empty `Avro.Map` during `__init__`. Charge actual allocated capacities, not logical lengths. Gate every supported Julia version at all declared boundary counts and map element types.

2. **[major] `JSON.lazy(...; duplicate_keys=:error)` allocates an unbudgeted `Set` from untrusted keys.**

   **Claim:** The schema parser cannot implement category-(e)’s “reserve before every allocation” contract while using the required JSON path.

   **Evidence:** L289–307 mandates `JSON.lazy(...; duplicate_keys=:error)`. The pinned [JSON lazy parser](/Users/jacob.quinn/.julia/dev/JSON/src/lazy.jl:271) creates `Set{String}()` for every object and inserts each decoded key at lines 276–294. `Set` growth and collision rehashing occur inside JSON.jl. Avro cannot reserve the actual allocation first. A wide or deeply repeated untrusted property/default tree can therefore allocate outside the operation budget before category-(e) charges it.

   **Recommendation:** Disable JSON.jl’s internal duplicate tracker. Detect decoded duplicate keys with an Avro-owned, budgeted, fixed-capacity set. Apply the same no-`Dict`/`Set` rule to `ParseContext`, frozen-object construction, and plan memo tables. Add large-object and adversarial-collision allocation-hook gates.

3. **[major] Typed decoding remains an explicit escape from the universal memory ceiling.**

   **Claim:** The public typed path can allocate beyond `max_total_bytes`, despite the plan saying every decode path is bounded.

   **Evidence:** L188–196 promises one ceiling for package-owned allocation. L512–514 then says caller-chosen `T` is only approximately charged. L614 says typed `Dict` is built outside exact accounting. Public typed decoding is available through `DatumReader`, `decode`, `Rows`, and `Table` at L1146–1155. A `Dict` can collision-rehash, and a StructUtils constructor can allocate arbitrary storage that the generic estimate does not cover.

   **Recommendation:** Make this an explicit trusted-only boundary, not a silent approximation. For untrusted decoding, construct the bounded generic representation and require caller-side conversion after ownership transfer. Alternatively, provide exact accounting only for a closed set of approved typed representations and reject other typed targets under guarded mode.

4. **[major] The newly budgeted schema operations do not expose the limit that callers must raise.**

   **Claim:** The public API cannot deliver its category-(e) budget contract for large legitimate schemas.

   **Evidence:** L549–554 makes `Avro.schema(T)`, schema JSON, canonical form, fingerprints, and prepared-codec construction independent budget scopes. L432–434 says `LimitError` identifies the keyword to raise. Yet the public signatures at L1127–1136 omit `limits=` from `Avro.schema(T)`, `Avro.schema(::Tables.Schema)`, `Avro.json`, `canonical`, `fingerprint`, and `parsingequivalent`.

   **Recommendation:** Add `limits=Limits()` to every allocation-bearing schema operation. Specify that nested operations share one `Budget` instead of starting stacked default budgets. Define how `JSON.json(schema)` selects or inherits that budget.

5. **[major] The Writer/Reader invariant still omits the reader’s retained schema and header graph.**

   **Claim:** A Writer can succeed under a limit that an identical-limit Reader exceeds while parsing the header, before or while it materialises the data.

   **Evidence:** Category (e), L525–532, now charges parsed schema nodes, names, frozen defaults/properties, parser state, and plan graphs. The invariant at L465–473 charges the writer for reader value output, decompressed bytes, the block table, metadata sizes, and schema syntactic limits. It does not charge the retained parsed schema graph, materialised metadata representation, parser temporary peak, or their overlap with near-ceiling output.

   **Recommendation:** Make Writer preflight the complete reader peak under the same budget: header metadata representation, parser temporaries, retained schema graph, plan graph, block table, codec progress, and decoded output. Add combined schema-plus-near-ceiling data tests. Testing a maximal schema and near-ceiling data separately is insufficient.

6. **[major] Seeded `Avro.Map` probing has no deterministic work bound.**

   **Claim:** Fixed capacity bounds memory, but it does not bound CPU. Inserting or looking up colliding keys can take quadratic total work.

   **Evidence:** L614 explicitly admits worst-case `O(n)` lookup. The work rule at L450–463 counts values and bytes, not hash-table probes. The equal-hash gate at L1489–1491 checks memory and correctness only. Julia’s [string hash](/Users/jacob.quinn/.julia/juliaup/julia-1.12.6+0.aarch64.apple.darwin14/Julia-1.12.app/Contents/Resources/julia/share/julia/base/hashing.jl:198) passes only the low `UInt32` seed to MurmurHash. A random seed reduces attack practicality; it is not the deterministic input-proportional bound promised by L188–196. The symbol-admission table uses the same mechanism.

   **Recommendation:** Charge each probe to a bounded map-work counter. Set a maximum probe distance and use a deterministic fallback or fail with `LimitError`. Gate forced collisions for construction, lookup, admission, and latency.

7. **[minor] The map capacity variable is ambiguous for duplicate keys.**

   **Claim:** `n` can mean decoded pair count, unique key count, vector length, or allocated capacity.

   **Evidence:** L501–505 writes `tablesz(n)`. L540–542 says construction is sized from decoded pair count but duplicate keys use last-wins semantics. An `AbstractDict` cannot expose duplicate entries, so retained vector length may differ from the pair count.

   **Recommendation:** Name separate `npairs`, `nunique`, and capacity values. Charge actual allocated vector and index capacities. Add a duplicate-heavy storage-oracle case.

8. **[minor] Several summaries still contradict the authoritative rules.**

   **Claim:** Independent implementers can select different contracts.

   **Evidence:** L1485 states the raw `Base.summarysize` oracle, while L1492–1493 excludes schemas. Decision 30 at L1768–1772 still says four categories and omits category (e). Phase 4c at L1641 and the risk at L1807–1809 say two attempts per block rather than per pass. L395 says codec “frame” although XZ and bzip2 use streams/members.

   **Recommendation:** Make these statements match L488–554, L758–797, and decision 22.

9. **[minor] The runtime-layout `Ref` is a second global mutable-state exception.**

   **Claim:** The plan says the symbol-admission table is the only global mutable state, but layout constants are stored in another mutable object.

   **Evidence:** L204–209 declares one exception. L1619–1622 adds a module-level `const Ref` written during `__init__`.

   **Recommendation:** Record it as write-once initialization state and freeze it before public operations, or use immutable runtime-specific dispatch.

## Non-blocking follow-ups (if any)

Findings 7–9 are non-blocking:

- Define map pair, unique-key, and capacity terms.
- Remove the stale accounting, codec, and retry text.
- Document the write-once layout state.

The accepted scope remains sound. RPC, `big-decimal`, schema inference, append, borrowed views, and parallel compression can remain deferred.

## Milestone and gate assessment

The schema grammar, fullname and alias rules, defaults, unions, binary and JSON encodings, single-object encoding, sort order, OCF framing, codec members, schema resolution, canonical form, fingerprints, and all non-deferred logical types remain strong. I found no new §2.2 audit-table error. The updated XZ padding and Zstandard frame corpus is coherent with the recorded oracle evidence.

The phase order is still reasonable, but these gates cannot pass yet:

- Phase 1 needs a budget-controlled duplicate-key tracker and reachable `limits` on schema operations.
- Phase 2 needs a correct `Avro.Map` formula, a deterministic probe-work bound, and a clear typed-target trust boundary.
- Phase 4a needs a combined reader-side header/schema/data peak check for the Writer/Reader invariant.
- Phase 4b must apply the same probe-work rule to symbol admission.
- Phase 4c is executable after its stale “per block” text is corrected.

The benchmark methodology, interoperability corpus, readiness levels, and PR-ready versus release-ready distinction remain suitable. DRAFT v11 is not implementation-ready because several public paths can still exceed the promised memory or work ceiling, and one mandatory storage gate fails numerically as written.

Assumptions: All schema documents, values, metadata, and codec payloads are untrusted. The pinned specification and dependency revisions are authoritative. Declared fixtures remain implementation deliverables, not review preconditions.

Decisions without user direction: I treated detailed authoritative clauses as controlling over stale summaries. I still marked the affected broad rows partial. I treated typed decoding as untrusted because the public API does not declare it a trusted boundary.

Validation: I inspected DRAFT v11, `response-10.md`, the pinned specification, JSON.jl’s lazy parser, Julia’s hashing implementation, the tests, milestones, and review log. I ran the map-layout probe read-only on Julia 1.10.11 and 1.12.6. I modified no files, big dawg.

## Verdict

VERDICT: REVISE
