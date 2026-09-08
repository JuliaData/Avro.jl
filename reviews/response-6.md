# Response to codex-review-6 (round 6) — disposition of all items

The plan was revised in place to DRAFT v7. Every carried item and every new finding was adopted; the
resource model was simplified rather than extended. Section references are to v7.

## Carried items

* **R1-1 / R4-1 / R4-2 / R5-1 / R5-2 / R5-3 / R5-4 (limits, codec memory, work budget, invariant,
  semaphore, amplification) — Adopted.** See new findings 1–5 and 9–10 below.
* **R2-new-6 / R5-10 (Symbol admission for typed JSON) — Adopted.** `fromjson(...; names=)` and the
  admission boundary stated for every typed path (§4.1, §4.5, §4.8, §4.11, §5.2, §6).
* **R5-9 (Julia-derived names) — Adopted.** See new finding 8.

## New findings 1–13

1. **[blocker] Semaphore under-reserves / can deadlock — Adopted (replaced).** §4.9: one accumulator —
   the operation ceiling — shared by committed output and in-flight reservations; the single coordinator
   issues permits **in block-index order** only when `committed + inflight + W ≤ max_total_bytes` with
   `W = max_block_bytes + max_codec_memory + 128 KiB + max_block_output_bytes` (the full decompressed cap
   and the codec window plus its fixed overhead are reserved before decompression, never the compressed
   size); **streaming ordered assembly** appends each lowest-index block's chunks to the final columns,
   commits its exact totals and releases its reservation; the liveness argument is written out (the
   lowest uncommitted block always holds a permit; the coordinator waits only on it; every commit
   releases a permit); failure when even one `W` cannot fit is deterministic at that index; tests
   include highly compressible 64 MiB blocks, half-ceiling tables and liveness (§9.11).
2. **[blocker] Writer/Reader guarantee false — Adopted.** §4.4 "Writer/Reader invariant": the writer
   enforces every per-datum, per-block and cumulative reader limit, including `max_total_bytes` charged
   with the reader's output estimate plus payload and block-table bytes, `max_metadata_*` on the header
   and the schema limits on the serialised schema; the invariant is stated under identical limits — and
   the defaults are identical everywhere because they are now fixed (finding 3); tests cover multi-block
   near-ceiling files, maximal metadata and a maximal schema.
3. **[blocker] Unsafe default memory policy — Adopted.** §4.4 (decision 12): fixed, portable defaults —
   a 1 GiB ceiling with no RAM probing; the manual states that concurrent operations each own a ceiling
   and recommends a caller-side governor; the ceiling is documented as an estimate of package-owned
   memory; raising limits is an explicit both-sides decision.
4. **[major] Compressed-size pre-check rejects valid files — Adopted.** §4.4/§4.9: no compressed-size
   work pre-check; only hard count/size/remaining/window limits before decompression; the exact work
   rule runs on the decompressed size before datum iteration; the million-empty-string block (134 bytes
   zstandard / 1,071 bytes deflate) is a fixture that must read under defaults (§8.2, §12).
5. **[major] `mmap=false` reads the whole file — Adopted.** §4.9 (decision 27): `mmap=false` uses the
   sequential streaming block reader; path sources are never read whole; owned input copies of
   non-conforming byte vectors are charged; caller-owned byte sources are not (§4.3); test: a file larger
   than the ceiling streams (§9.10).
6. **[major] Self-alias vs `schema-tests.txt` — Adopted.** §3/§4.2 (decision 26): an alias equal to its
   own name is idempotent and ignored (Java-compatible); collisions across distinct names remain errors;
   the Phase 1 gate keeps 100% of the file including cases 023/024 (§12).
7. **[major] `fromjson` admission — Adopted.** `names=` on `fromjson`, passed through both typed
   construction paths; repeated-JSON admission tests (§4.11, §5.2, §6, §9.10).
8. **[major] Julia-derived name policy — Adopted.** §4.8 (decision 24): every component (type names,
   module path, field names, enum symbols, Tables columns, sanitised parameters) must already be a valid
   Avro name; no transliteration; actionable `ArgumentError` with the Julia path; remedies are StructUtils
   field tags, the overridable `Avro.avroname(::Type)`/`Avro.avrosymbol` hooks (the documented remedy for
   nested collisions instead of the root-only `name=`), `names=` renames for Tables columns, or an
   explicit schema.
9. **[major] Codec-cap unexecutable cases — Adopted.** §4.9/§8.2: the xz threshold is liblzma's reported
   requirement (dictionary + ≈ 64 KiB; the fixture is documented accordingly); zstd's `windowLogMax` is
   clamped to the library's bounds (10…31) so caps ≥ 2 GiB permit every supported frame; caps above
   2 GiB are tested.
10. **[major] Unmeasured work ceilings — Adopted.** §4.4/§10.2/§12: the work constants are provisional;
    Phase 2's **latency gate** measures the slowest legal zero-byte shapes on every supported Julia
    version and fixes `max_values_per_byte`, `work_allowance`, `max_total_values`, `max_rows` so the
    worst case admitted by defaults decodes in ≤ 10 s single-threaded; starting values lowered
    (16 values/byte, 2^28 values/rows).
11. **[minor] JSON depth asymmetry — Adopted.** `max_json_depth` (1024) shared by `tojson`/`fromjson`;
    boundary round trips at 1024/1025 (§4.4, §4.11, §9.4).
12. **[minor] Duplicate named union branches — Adopted.** §3/§4.2: branch identity = kind for unnamed and
    fullname for named schemas; a definition plus a reference to the same fullname is a duplicate; negative
    fixture.
13. **[nit] Attribute wording — Adopted.** §4.2 scopes the statement to `size`/`precision`/`scale`/
    `logicalType`/custom properties.

## Non-blocking follow-ups (all adopted)

* One shared allowance deficit counter per operation, consumed identically by the writer's flush decision
  (§4.4).
* Failure-kind precedence: the lowest block index wins regardless of kind, with both orderings spelled
  out (§4.9, decision 22).
* The comparison property is scoped to orderable schemas and calls `encode(schema, value)` explicitly
  (§4.12, §9.4).
* `eachblock` returns owned **decompressed** bytes (§4.9, §5.3).
* `max_codec_memory` is decode-only; writer-side compression memory is bounded by the codec level and
  stated as such (§4.9, §13, decision 6).

No material objection to any round-6 finding remains on Claude's side; DRAFT v7 is submitted for round 7.
