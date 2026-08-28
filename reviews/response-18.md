# Response to codex-review-18 (round 18) — disposition of all items

The plan was revised in place to DRAFT v19. Every finding and every follow-up was adopted. Two of the
majors were resolved by **simplification** rather than by adding mechanism, because eighteen rounds
have shown that the eviction/fallback machinery and the `Tables.Scan` pushdown surface generate a new
layer of objections at every revision:

* **Parallel decoding only uses headroom** (§4.9, decisions 22/34/53/56): the lowest uncommitted block
  follows the sequential rule exactly and never waits; higher blocks are admitted strictly in block
  order into the headroom above the sequential peak reserve with **full worst-case reservations**;
  nothing is evicted, retried, cancelled or restarted; every block is decoded exactly once; acceptance
  and deterministic work are identical to sequential by construction, assembly copies are bounded by
  `assembly_bytes ≤ committed_bytes` (defined), and parallel speedups under default limits are
  explicitly small (the §10.2 thread gate states its raised limits).
* **Projection is the only pushdown in 2.0** (§6, §5.3, §12, decisions 9/31/54): `select=` on
  `Avro.Table`/`Avro.Rows` with skipping under both validation modes and a derived effective schema;
  filters, limit/offset, renames, overrides, residuals and provenance under arbitrary transformations —
  the `Tables.Scan` surface — are deferred to 2.1 with the accumulated design notes (Avro-owned
  resolver/evaluator, `max_scan_work`, closed predicate set, Tables-valid renames clearing provenance).
  The Avro format gains nothing from filter pushdown in decoding work, so the 2.0 value is preserved.

## Majors

1. **Grammar tables — Adopted.** Separate schema-object and record-field tables (§4.2): a schema
   object's `type` must be a string; only a field's `type` accepts any schema; `doc` is grammar on
   records and enums only (a fixed `doc` is metadata); Java-backed fixtures per table.
2. **Recursive default rule — Adopted.** §4.2: record defaults supply missing nested fields from their
   own validated defaults, fail only when none exists, ignore unknown members semantically while
   retaining the span; gated for parsing and reader-schema resolution.
3. **Scan work / user code — Adopted by deferral** (2.1 design note lists `max_scan_work` and the closed
   predicate set).
4. **Rename semantics — Adopted by deferral** (2.0 has no renames; the 2.1 note records Tables-valid
   renames with provenance clearing).
5. **Record admission provenance — Adopted.** §4.6: bare `Avro.Record` never interns (`record[:f]`,
   `record["f"]`, `getproperty` compare the caller's symbol against field-name strings; `keys` returns
   strings); the Tables row interface lives on `Avro.Row`, yielded by `Avro.Rows` and carrying the
   operation's admission object; detached records are wrapped explicitly with `Avro.Row(record;
   names=…)`.
6. **Fallback vs attempts — Adopted by simplification** (no fallback, no attempts).
7. **Phase 4c/4d ordering — Adopted** (no filtered-scan gates in 4c; 4d is projection and performance).
8. **Latency calibration — Adopted.** §4.4/decision 12: Phase 2 values are provisional; constants are
   finalised and recorded only after the Phase 4a container gate.
9. **Available-memory guard vs invariant — Adopted.** §4.4/decision 12: the invariant holds whenever
   both effective ceilings admit the writer-preflighted peak; the guard is a separate low-memory safety
   guarantee.

## Minors and nit (all adopted)

`npairs` retained for all-duplicate maps with payload counted once; `max_block_output_bytes` is a
logical, path-independent charge and `committed_bytes` is defined; `inspect` has its own scope, the
`SchemaCache` ambiguity rule is scoped to the built-in cache and `plan` is internal; comparison
denominators for operations without encoded input and `k ≤ max_total_values`; saturating fixed-decimal
bound; graph-wide copy memo; `nothing` selects null on nullable unions; unknown record members ignored
in `fromjson` with `unknown=:error` opt-in (Java-compatible); map-key conversion and collision rule;
float differential vectors (midpoint, subnormal, overflow, underflow, huge mantissa/exponent, linear
checks); constructor `props` collisions are errors and invalid defaults compare by exact lexical span;
`Symbol(::EnumValue)` is explicit caller-owned interning; standalone `error` printing, PCF, equality and
cache behaviour; summary wording (hash-table lookup, deterministic work, corpus exceptions).

No material objection to any round-18 item remains on Claude's side; DRAFT v19 is submitted for round 19.
