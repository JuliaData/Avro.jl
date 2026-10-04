# Response to codex-review-2 (round 2) — disposition of all items

The plan was revised in place to DRAFT v3. Every round-1 item marked partial/not resolved and every new
finding was adopted; the amendments Codex still objected to were withdrawn except where noted. Section
references are to v3.

## Round-1 items marked PARTIALLY/NOT RESOLVED

* **1 (budgets) — Adopted.** §4.4: RAM-aware cumulative byte budget (`clamp(total_memory ÷ 2, 1 GiB,
  64 GiB)`, deterministic per machine, pinned explicitly in CI), output-byte estimation rules, encoder
  budget, schema limits (nodes, fields, union branches, enum symbols, name bytes, named types), block
  header lower bounds, atomic reserve-before-allocate; the corpus must decode under a 1 GiB floor.
* **2 (`fromjson`) — Adopted.** §4.11: same pre-scan, lazy traversal with `duplicate_keys=:error`, and
  operation budget as schema parsing.
* **5 (branch recovery) — Adopted.** §4.6: exact-representation match first, coercive acceptance second.
* **6 (local-millis) — Adopted; amendment withdrawn.** §4.6: uniform exact wrappers for *all* timestamp
  kinds including `local-timestamp-millis`; `DateTime` conversion is explicit and range-checked (error,
  never wrap) via `instants=:datetime`; `DateTime` still derives `local-timestamp-millis` on encode.
* **9 (freezing/hash) — Adopted.** §4.2: every node an immutable struct; `FrozenVector`/`FrozenDict`
  with `freeze!` enforce immutability; hash/canonical caches live in an identity-keyed side table;
  `==`/`hash` normalised (defaults by value, props as unordered maps, JSON spelling excluded).
* **10 (schema-derived types) — Adopted.** §4.5/§4.6: closed value set; collections typed one level
  deep and `Any`-typed beyond; `Decimal`/`WideDecimal` with runtime scale instead of type parameters;
  numeric compile gate (zero new method instances after warm-up, zero invalidations, RSS and native-code
  bounds).
* **15 (decimal precision on decode; local-millis; big-decimal) — Adopted.** Decode-side digit check
  (§4.6/§4.8); local-millis via wrappers; big-decimal deferred (§3).
* **16 (store semantics) — Adopted.** §4.10: idempotent only for structural `==`; same-fingerprint
  different-semantics registration is `AmbiguousSchemaError`; fingerprints of schemas returned by
  external stores are recomputed and checked.
* **17 (compare API) — Adopted.** §4.12: `compare` (values) and `comparebytes` (encoded, budgeted).
* **18 (legacy decimal; writer lifecycle) — Adopted.** §4.9: legacy mode covers only padding and the
  `zstd` name; decimal reinterpretation requires `decimal_byteorder=:little`; writer poison/abort/cleanup
  and caller-IO semantics specified.
* **19 (close semantics) — Adopted.** §4.9: caller-owned IO is never closed, only dereferenced.
* **20 (parallel) — Adopted.** §4.9: atomic budget reservations; lowest failing block index is the
  deterministic first cause after all tasks settle.
* **22 (Scan API) — Adopted; amendment withdrawn.** §6 rewritten against the pinned revision (cloned and
  read): `Tables.resolve` → `BoundScan`, `All()` identity, `filtermask(::BoundScan, subset)`, residual
  carrying unhandled overrides over output names, `Tables.scan`'s conversion rules; plus the load-time
  Scan/no-Scan variant rule (§6, §12) so the 2.0 surface is decided now.
* **23 (enum limits/equality) — Adopted.** §4.4 limits; `EnumValue` equality by fullname + symbol;
  remap by symbol on encode (§4.6).
* **25 (oracles) — Adopted.** §8.4 matrix measured (`avro-py KNOWN_CODECS = null/deflate/bzip2`;
  fastavro all six via cramjam 2.11.0, pinned); fastavro fixtures for every codec; byte-exact oracle
  restricted to deterministic datums, collections compared semantically (§8.5).
* **27 (benchmarks) — Adopted.** §10: cross-language numbers informational; gates are same-host ratios
  vs 1.1.2; named ≥ 8-physical-core host for the thread gate; codec kernel defined; Java warm in-JVM
  number (49 ms) recorded.
* **28 (`decode` roles) — Adopted.** §5.2: `decode(writer_schema, src; reader_schema=…)`.

## New findings 1–16

1. **[blocker] Union selection violates the spec — Adopted.** §4.7: `union_resolution=:spec` (first
   match including promotion) is the default; `:java` is an explicit option; both tested on
   `["long","int"]`/`["double","int"]`; Java fixtures are labelled `:java`, spec-policy expectations
   come from fastavro where they differ (§8.2).
2. **[blocker] Automatic legacy decimal reinterpretation — Adopted.** Explicit-only
   `decimal_byteorder=:little`; `legacy=:avrojl1` limited to the two unambiguous tolerances (§4.9, §7).
3. **[major] Non-record OCF roots — Adopted.** §4.9: any root schema; `Reader.eachdatum`, `Avro.Rows`
   for all roots (Tables interface only for records), `Avro.Table` errors clearly; fixtures for
   primitive/enum/fixed/array/map/union roots in every codec (§8.2); `decode(schema, bytes, pos)` returns
   the consumed position (§4.3).
4. **[major] Block header lower bounds — Adopted.** §4.4/§4.9: `0 ≤ count`, `0 ≤ size` before any
   arithmetic; −1 and `typemin(Int64)` tests.
5. **[major] Skip validation policy — Adopted.** §4.3: skips validate everything structural (varints,
   booleans, lengths, enum/union indexes, sized-block exhaustion, depth, budgets); the single documented
   exception is UTF-8 of skipped strings; acceptance-equivalence tests (§9.8).
6. **[major] Tables symbol interning / specialisation — Adopted.** §6: file-derived schemas are always
   stored `Tables.Schema{nothing,nothing}`; names bounded by `max_fields`/`max_name_bytes`; the trust
   boundary is documented; the compile gate covers names and shapes.
7. **[major] `EnumValue` equality — Adopted.** By fullname + symbol; remap by symbol (§4.6).
8. **[major] Error guarantee — Adopted.** §4.13 restated: package-detected defects → `AvroError`; IO,
   interrupt, OOM, stack overflow, user-hook and mmap errors propagate unchanged; fuzz gate scoped to
   owned buffers and built-in targets.
9. **[major] Raw byte-equality oracle — Adopted.** §8.5 (2): byte-exact only for deterministic datums;
   collections semantic; both block forms tested; NaN-payload JSON limitation documented (§4.11).
10. **[major] Capability matrix / reverse coverage — Adopted.** §8.4 corrected from measurements;
    fastavro→Julia fixtures for all six codecs; `cramjam==2.11.0` pinned.
11. **[major] Schema-free `encode(x)` — Adopted.** §4.8: value-level `Avro.schema(x)` defined for
    identity-bearing generic values, type-derived for plain values, `ArgumentError` for ambiguous ones
    (`UnionValue`, `Decimal`); `decode(writer_schema, src; reader_schema)` naming.
12. **[major] Single-object cache semantics — Adopted.** §4.10 (see round-1 item 16 above).
13. **[major] Schema-less writing — Adopted.** §4.9: `Avro.write` requires a `Tables.schema` or
    `schema=`; `Avro.inferschema(table; max_rows, limits)` is the explicit, bounded, materialising
    operation; never implicit.
14. **[major] big-decimal semantics — Adopted.** Deferred to 2.x with the Java-vs-spec scale conflict and
    the required vectors recorded (§3); carried as `UnknownLogical` bytes in 2.0.
15. **[minor] Audit narrative — Adopted.** §1/§2.2: "15 confirmed, 4 corrected, 1 source-supported but
    not re-run"; stored `Tables.Schema{nothing,nothing}` as the exact trigger; "every non-empty
    uncompressed container file (one with at least one data block)".
16. **[minor] Header-only OCF — Adopted.** Recorded as decision 16 in §14; read and write tests (§9.9).

## Remaining objections to amendments

* Finding 5: resolved by exact-representation-first recovery (no remaining amendment).
* Finding 6: amendment withdrawn (uniform wrappers).
* Finding 22: amendment withdrawn (rewritten against the pin).
* Fixed-value identity: amendment withdrawn — `Avro.Fixed` (schema reference + bytes) is the generic
  value (§4.6); the shared-schema column wrapper suggestion is noted as a later optimisation.
* Green at every phase: accepted in Codex's narrow reading, now stated verbatim in §2.3 and §12.

## Milestone and gate changes

Phase 0 adds the Julia 1.10 `Pkg.add(PackageSpec(url, rev=SHA))` bootstrap and cramjam pin; Phase 1 adds
schema/name limits, enforced immutability and normalised equality; Phase 2 adds the closed value set,
decode-side decimal checks, bounded datum JSON, store ambiguity rules, value-level `schema(x)`, numeric
compile gates; Phase 3 adds both union policies and the compare API split; Phase 4a adds lower bounds,
writer failure semantics, producer-by-codec tests and any-root iteration; Phase 4b adds stored schemas
and name-limit tests; Phase 4c adds atomic reservations and deterministic error selection; Phase 4d is
rewritten against the pin; Phase 5 adds the cross-package smoke test. The Scan decision rule is fixed now
(§12).

No material objection to any round-2 finding remains on Claude's side; DRAFT v3 is submitted for round 3.
