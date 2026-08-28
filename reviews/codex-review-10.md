# Codex review round 10

## Disposition check (round-9 items)

Line references below are to [DRAFT v10](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:1).

### Primary carried rows

| Item | Status | Evidence |
|---|---|---|
| R1-1 — Resource limits | **PARTIALLY RESOLVED** | The new accounting model is at L488–520. Map storage, schema/plan allocations, and the literal storage oracle remain unsafe or unexecutable; see findings 1–3. |
| R1-9 — Freezing and hashing | **RESOLVED** | Transitive freezing and stored hashes remain at L240–247; fresh defaults at L280–288. |
| R1-10 — Schema-driven compilation | **RESOLVED** | Schema-independent generic plans and the compilation gate remain at L528–560. |
| R1-18 — Writer lifecycle | **RESOLVED** | Validation, atomic replacement, poisoning, cleanup, and abort are specified at L875–916. |
| R1-20 — Parallel error determinism | **RESOLVED** | Ordered budget failures and lowest-block failure selection are at L852–857. |
| R1-25 — Union oracle | **RESOLVED** | Direct branch-index and reader-result expectations remain at L615–629. |
| R2-new-6 — Symbol admission | **RESOLVED** | Binary, JSON, Tables, and typed paths are covered at L534–539, L697–700, L947–954, and L1156–1166. |
| R2-new-11 — Schema-free identity | **RESOLVED by narrowed contract** | L682–689 clearly defines a conventional schema, not representation preservation. I continue to accept this amendment. |

### Other carried rows

| Item | Status | Evidence |
|---|---|---|
| R4-1 — Codec memory | **PARTIALLY RESOLVED** | The authoritative XZ cap rule is correct at L507–510 and L731–736, but L804–807 retains the obsolete per-frame-estimate rule. |
| R4-2 — Work/allocation budget | **PARTIALLY RESOLVED** | The general model improved, but the map formula and schema/plan omissions invalidate the complete-memory claim. |
| R5-1 — Writer/Reader invariant | **RESOLVED** | Symmetric limits and emitted-frame checks are specified at L465–473 and L887–902. |
| R5-2 — Parallel transient memory | **PARTIALLY RESOLVED** | The reservation algorithm is coherent, but materialised maps can allocate beyond their reservation. |
| R5-3 — Work amplification | **PARTIALLY RESOLVED** | The detailed per-pass rule is correct at L826–838 and L1191–1204. Several summaries still incorrectly say “twice per block.” |
| R5-4 — Codec-cap contract | **RESOLVED** | Complete decoder requirements and writer verification are specified at L728–757 and L887–902. |
| R5-9 — Julia-derived names | **RESOLVED** | The complete name policy and hooks remain at L658–681. |
| R5-10 — Typed Symbol admission | **RESOLVED** | All typed binary and JSON routes remain covered. |

### Round-6 carried findings

| # | Status | Evidence |
|---:|---|---|
| 1 — Semaphore memory/liveness | **PARTIALLY RESOLVED** | L784–851 fixes the algorithm and liveness. Findings 1–3 still invalidate complete accounting. |
| 2 — Writer/Reader invariant | **RESOLVED** | L465–473 and L887–902. |
| 3 — Unsafe default memory | **RESOLVED** | Fixed 256 MiB default and available-memory guard at L395–428. |
| 4 — Compressed-size work precheck | **RESOLVED** | Explicitly prohibited at L450–463. |
| 5 — Whole-file `mmap=false` allocation | **RESOLVED** | Sequential streaming at L767–770. |
| 6 — Self-alias gate | **RESOLVED** | Idempotent self-alias handling at L295–304. |
| 7 — `fromjson` admission | **RESOLVED** | `names=` is present at L947–954 and L1109–1113. |
| 8 — Julia name policy | **RESOLVED** | L658–681. |
| 9 — Codec-cap cases | **PARTIALLY RESOLVED** | The authoritative mechanism is fixed, but L804–807 still contradicts it. |
| 10 — Work ceilings | **PARTIALLY RESOLVED** | Detailed deterministic gates are executable; summaries still omit the per-pass qualification. |
| 11 — JSON-depth symmetry | **RESOLVED** | Common `max_json_depth` at L386–389 and L947–954. |
| 12 — Duplicate named union branches | **RESOLVED** | L295–304. |
| 13 — Attribute wording | **RESOLVED** | Contextual structural/logical attributes at L263–279. |

### Round-7 findings 1–10 and sanitisation follow-up

| Item | Status | Evidence |
|---|---|---|
| 1 — Final-column allocation | **RESOLVED for column shells** | Exact capacity and no reallocation at L488–505 and L792–803. |
| 2 — Writer codec workspace/frame requirement | **RESOLVED** | L887–902. |
| 3 — Fixed worst-case permit mechanism | **RESOLVED** | Replaced by actual reservations and priority eviction at L804–838. |
| 4 — Fixed 1 GiB default | **RESOLVED** | Default is 256 MiB at L395–428. |
| 5 — Constructor/runtime `W` mismatch | **RESOLVED** | Fixed `W` no longer exists; relations are at L436–448. |
| 6 — Empty-union `minsize` | **RESOLVED** | L252–255. |
| 7 — Peak-RSS source/method | **RESOLVED for the prior defect** | Caller-owned faulted input and current-RSS sampling are specified at L858–874. |
| 8 — Alias normalisation | **RESOLVED** | L248–251. |
| 9 — Streamed compressed-input ownership | **RESOLVED** | L767–770 and L804–813. |
| 10 — `Nothing` mapping | **RESOLVED** | L643–653. |
| Sanitisation follow-up | **RESOLVED** | Exact algorithm, hooks, and collision handling at L658–681. |

### Round-8 findings and follow-ups

| Item | Status | Evidence |
|---|---|---|
| 1 — Nullable-column storage | **RESOLVED** | Exact isbits-union shell accounting at L488–505 and L792–801. |
| 2 — Zstd decoder requirement | **RESOLVED** | `fromFrame` estimates and writer verification at L736–741 and L887–902. |
| 3 — Eviction/work bound | **PARTIALLY RESOLVED** | The detailed per-pass algorithm is fixed; summaries remain unqualified. |
| 4 — RSS gate used a sequential source | **RESOLVED** | L858–874 now exercises the parallel byte-source path. |
| 5 — Non-UTF-8 metadata exception | **RESOLVED** | The exception is recorded at L1252–1259 and the summary is qualified at L1606–1610. |
| 6 — Filtered Scan sizing/work | **PARTIALLY RESOLVED** | The global count pass and four-attempt bound are correct at L1191–1204; plan-wide summaries still contradict them. |
| 7 — `fixed(0)` decimal | **RESOLVED** | L268–279. |
| 8 — Estimator APIs | **RESOLVED for the prior issue** | V10 deliberately reserves the XZ cap and uses direct, checked JLL symbols at L1523–1535. |
| Bzip2 floor | **RESOLVED** | L436–443. |
| 16 MiB RSS workload | **RESOLVED** | L871–874. |
| Cooperative cancellation | **RESOLVED** | L822–828. |
| Safe task cap | **RESOLVED** | Fixed pool at L787–790. |
| Storage/payload separation | **PARTIALLY RESOLVED** | Authoritative ownership rules are correct, but L798–800 retains the obsolete bytes formula. |
| CPython pin | **RESOLVED** | Pinned in the interoperability environment. |
| Accepted deferrals | **RESOLVED** | L1658–1659. |

### Round-9 findings 1–8 and follow-ups

| # | Status | Evidence |
|---:|---|---|
| 1 — XZ reservation | **PARTIALLY RESOLVED** | Reserving the configured cap is correct at L507–510 and L731–736. L804–807 still says liblzma reports a frame requirement before allocation. |
| 2 — Generic storage estimator | **PARTIALLY RESOLVED** | Representation formulas were added at L488–520, but the `Dict` formula is false and the raw `Base.summarysize` oracle cannot handle schema-retaining values. |
| 3 — Concatenated codec members | **PARTIALLY RESOLVED** | Ordinary members are accepted at L744–757, but valid XZ Stream Padding is rejected; see finding 4. |
| 4 — Filtered Scan attempts | **PARTIALLY RESOLVED** | L826–838 and L1191–1204 correctly permit two attempts per pass. L1036–1040, L1611–1616, L1687–1692, and L1739–1741 still state two per block. |
| 5 — CPU timing gate | **RESOLVED** | Deterministic units are gated; measured CPU is informational with a 2.5× tolerance at L829–838 and L1453–1456. |
| 6 — §13 oracle summary | **RESOLVED** | L1606–1610 refers to the recorded exceptions. |
| 7 — Worker pool | **RESOLVED** | Fixed and charged at L787–790 and L511–513. |
| 8 — Peak-RSS primitive | **RESOLVED for the missing definition** | Child-process current RSS sampling is specified at L858–874. |
| Qualify §13 | **RESOLVED** | L1606–1610. |
| Cap and charge worker tasks | **RESOLVED** | L787–790. |
| Make storage/payload categories disjoint | **PARTIALLY RESOLVED** | The authoritative transfer rule is correct; stale L798–800 remains. |
| Define RSS sampler | **RESOLVED** | L858–874. |
| Preserve accepted deferrals | **RESOLVED** | L1658–1659. |

## New findings

1. **[major] The `Dict` capacity formula under-reserves maps and cannot prevent collision-driven rehashing.**

   **Claim:** The formula is wrong at ordinary boundary counts. It is also not a function of count alone. An attacker can force `Dict` to allocate a larger table inside `setindex!`, before Avro reserves it.

   **Evidence:** V10 defines `tablesz(n)` as the next power of two above `max(16, 3n ÷ 2)` and relies on one `sizehint!` ([v10:499](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:499)). Julia uses `cld(3n,2)` ([Julia 1.12 `dict.jl`:193](/Users/jacob.quinn/.julia/juliaup/julia-1.12.6+0.aarch64.apple.darwin14/Julia-1.12.app/Contents/Resources/julia/share/julia/base/dict.jl:193)) and performs a 4× rehash when probe limits are exceeded ([`dict.jl`:314](/Users/jacob.quinn/.julia/juliaup/julia-1.12.6+0.aarch64.apple.darwin14/Julia-1.12.app/Contents/Resources/julia/share/julia/base/dict.jl:314)). Read-only probes produced:

   ```text
   Julia 1.10.11: n=11, plan=16 slots, actual=32, summarysize=728
   Julia 1.12.6:  n=11, plan=16 slots, actual=32, summarysize=656
   17 colliding keys after sizehint!(d,17): 32 slots → 128 slots
   ```

   This breaks both reserve-before-allocation and `storagebytes ≥ Base.summarysize`.

   **Recommendation:** Do not base the security ceiling on private `Dict` layout. Use a package-owned bounded map with deterministic capacity, or preflight into a charged collision-safe representation and expose `Dict` conversion outside the guarded generic path. Add `n=11`, `n=43`, duplicate-key, and adversarial-collision boundary gates on every supported Julia version.

2. **[major] The authoritative ceiling omits schema, default, property, parser, and plan graphs.**

   **Claim:** Untrusted schema parsing and prepared-plan construction allocate objects that are not in any accounting category or budget scope.

   **Evidence:** The security principle promises one ceiling for package-owned memory ([v10:188](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:188)). The exhaustive categories at L488–513 omit `Schema`, `Field`, `FullName`, recursively frozen properties/defaults, `ParseContext`, and read/write plan nodes. Budget scopes at [v10:522](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:522) omit standalone `parseschema` and prepared-codec construction. Those allocations are required at L240–247, L280–311, and L528–533.

   **Recommendation:** Add schema graphs, frozen JSON, parser state, canonical/printer buffers, and plan graphs to the authoritative categories. Give `parseschema`, schema printing/fingerprinting, and prepared-codec construction explicit budgets. Add shallow large-schema, large-default/property, and large-plan tests near the ceiling.

3. **[major] The literal `storagebytes ≥ Base.summarysize` gate is unexecutable for identity-bearing values.**

   **Claim:** `Fixed`, `EnumValue`, and `Record` retain schema references. Their fixed formulas cannot exceed `Base.summarysize` of an arbitrarily large reachable schema graph.

   **Evidence:** The gate applies raw `Base.summarysize` to every member of `E` ([v10:493](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:493)). Julia defines it as the memory of all unique reachable objects ([Julia `summarysize.jl`:14](/Users/jacob.quinn/.julia/juliaup/julia-1.12.6+0.aarch64.apple.darwin14/Julia-1.12.app/Contents/Resources/julia/share/julia/base/summarysize.jl:14)). The value model says `Fixed`, `EnumValue`, and `Record` carry schema references ([v10:581](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:581)). Yet the formula is only `56+n` for `Fixed`, and no explicit `EnumValue` formula is stated.

   **Recommendation:** Charge shared schema/plan graphs once. Define an ownership-aware oracle using `Base.summarysize(...; exclude=...)` for separately charged types. Specify identity deduplication for nested/shared values. Test shared and distinct schema identities.

4. **[major] The XZ member loop rejects valid Stream Padding.**

   **Claim:** A decoder that supports concatenated XZ Streams must accept zero-byte Stream Padding in multiples of four, between streams and at EOF. V10 treats it as garbage.

   **Evidence:** Avro delegates the codec to the XZ library ([Avro spec:522](</private/tmp/claude-501/-Users-jacob-quinn--julia-dev/a562222a-cba9-4dfa-89bb-7a3c76e3ffb8/scratchpad/spec/avro-spec.md:522)). The pinned XZ format says concatenation-capable decoders “MUST support Stream Padding,” which must contain null bytes in a multiple-of-four length and may appear at EOF ([XZ format:455](/Users/jacob.quinn/.julia/artifacts/8497848586ec3c8e66c36a5b01fea1e48dd2f62f/share/doc/xz/xz-file-format.txt:455)). V10 accepts concatenated streams but rejects bytes that do not start another member ([v10:744](/Users/jacob.quinn/.julia/dev/Avro/AVRO_REWRITE_PLAN.md:744)).

   Read-only verification produced:

   ```text
   XZ stream + 4 zero bytes  -> accepted
   XZ stream + 8 zero bytes  -> accepted
   XZ stream + 1 zero byte   -> LZMA error
   stream + padding + stream + padding -> accepted
   Apache avro-tools on a padded XZ OCF block -> decoded datum 1
   ```

   Fastavro rejects this padding, so it is another recorded oracle exception.

   **Recommendation:** Accept only all-zero XZ padding whose length is a multiple of four. Permit it between streams and at EOF. Add positive 4/8-byte cases and negative nonzero/non-multiple cases. Record Java/CodecXz acceptance and fastavro rejection.

5. **[minor] Three obsolete clauses still conflict with the adopted authoritative rules.**

   **Claim:** These clauses can send independent implementers down different paths.

   **Evidence:** L798–800 says bytes use `length+32`, instead of `40+n`. L804–807 says XZ has a reported pre-allocation requirement, instead of reserving the cap. The concurrency summary and decision 22 say two attempts per block, instead of per pass.

   **Recommendation:** Delete the obsolete storage and codec text. Qualify every retry summary as “per block per pass”; state the four-attempt filtered absolute bound.

6. **[minor] The Zstandard corpus omits short and skippable frames.**

   **Claim:** The member contract is implementable, but two ordinary frames do not cover all legal member forms.

   **Evidence:** Zstandard accepts an exact sequence of compressed or skippable frames ([zstd.h:164](/Users/jacob.quinn/.julia/artifacts/8da603395acfbdbef8c5de3b7223aeb9276ecbdb/include/zstd.h:164)). `ZSTD_findFrameCompressedSize` handles both forms ([zstd.h:216](/Users/jacob.quinn/.julia/artifacts/8da603395acfbdbef8c5de3b7223aeb9276ecbdb/include/zstd.h:216)), but it is absent from the required symbols at L1525–1529. A valid empty frame can be only nine bytes, while L736 says “first 18 bytes.”

   **Recommendation:** Define “up to the available 18-byte header,” require a bounded member-boundary API, and add empty, skippable-before/between/after, skippable-only, and truncated-skippable cases.

7. **[minor] Output-buffer and streaming-yield ownership transitions remain implicit.**

   **Claim:** The architecture can satisfy its guarantees, but the authoritative accounting list does not explicitly place encoder/compressor/JSON output buffers. It also does not say when `Rows`, `eachdatum`, or `eachblock` transfers yielded memory to the caller and releases its budget charge.

   **Evidence:** Encoder growth is specified at L366–373, output APIs at L762–770 and L947–954, and lifetime budgets at L522–524. Only Table chunk-to-column transfer is explicit at L504–505.

   **Recommendation:** Add output buffers to category (c), reserve replacement capacity before growth, and define yield-time transfer. Keep cumulative work/value/row counters after memory ownership transfers. Test streaming more than the ceiling with retained and unretained outputs.

8. **[minor] The 10 ms RSS sampler can miss short allocation spikes.**

   **Claim:** The method now has a defined counter, but it measures sampled RSS, not a true peak.

   **Evidence:** L865–871 polls `ps` every 10 ms. A buffer allocation and release between samples is invisible.

   **Recommendation:** Name it “sampled high-water,” flush the start/done markers, and supplement it with allocator instrumentation or an OS high-water measurement. Keep the deterministic reservation oracle as the primary safety gate.

9. **[minor] Hard-coded Julia layout formulas need a runtime compatibility guard.**

   **Claim:** The formulas were measured on Julia 1.10 and 1.12, while `julia = "1.10"` accepts future 1.x releases whose internal layouts may change.

   **Evidence:** The measurements are recorded at L493–503; compat is unbounded above at L1535.

   **Recommendation:** Validate representation constants during package initialization or derive them from stable runtime APIs. Fail safely if an unsupported layout is detected. Run the storage oracle on every Julia version admitted by release CI.

## Non-blocking follow-ups (if any)

Findings 5–9 are non-blocking implementation follow-ups. They should be fixed before the associated phase gate is declared green:

- Remove all stale accounting and retry text.
- Add short/skippable Zstandard fixtures.
- State output and yield ownership transitions.
- Harden RSS and runtime-layout checks.

The accepted deferrals remain appropriate: RPC, `big-decimal`, schema inference, append, borrowed views, and parallel compression.

## Milestone and gate assessment

The schema grammar, names, aliases, defaults, unions, binary and JSON encoding, single-object encoding, sorting, resolution, canonical form, fingerprints, logical types, strict OCF framing, and the section 2.2 audit are otherwise strong. I found no new audit-table inaccuracy. RPC remains explicitly deferred.

The phase order remains sound, but four gates need revision:

- Phase 1 must budget schema/default/property/parser/plan graphs and define the shared-object oracle.
- Phase 2 and Phase 4b must replace `Dict` layout assumptions with a bounded map design.
- Phase 4a must accept and test XZ Stream Padding.
- Phase 4c must remove stale retry statements; the detailed deterministic work gate itself is executable.

PR-ready, merge-ready, RC-ready, and release-ready remain correctly distinct. DRAFT v10 is not implementation-ready because its map and schema accounting can exceed the promised ceiling, one storage gate cannot pass as written, and its XZ reader rejects valid input.

Assumptions: the pinned Avro and codec specifications are authoritative. All schema documents, values, metadata, and codec payloads are untrusted. Declared fixtures remain implementation deliverables.

Decisions without user direction: I treated detailed authoritative clauses as controlling when stale summaries disagreed, but marked the affected prior findings only partially resolved. I treated missing output/yield wording, RSS sampling, and future-layout guards as minor because an implementer can settle them within the recorded contracts.

Validation: I used the package-review checklist. I inspected DRAFT v10, `response-9.md`, the pinned specification, codec formats and headers, Julia Base implementation, gates, milestones, and prior dispositions. I ran read-only Julia 1.10/1.12 `Dict`, `summarysize`, XZ, and codec probes. I created or modified no files, schema sheriff.

## Verdict

VERDICT: REVISE
