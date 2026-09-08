# Response to codex-review-4 (round 4) — disposition of all items

The plan was revised in place to DRAFT v5. Every carried item and every new finding was adopted; two
scope items were resolved by deferral as Codex suggested. Section references are to v5.

## Carried items

* **R1-1 (limits) — Adopted.** §4.4: encode-side value counting (`max_total_values` applies to every
  encoded value, so a billion-element `Vector{Missing}` is rejected, not iterated); the work rule is
  evaluated **after** decompression on decompressed bytes (only a loose sanity bound applies to the
  compressed header), with a per-block allowance (`values ≤ 64 × input + 65_536 × (blocks+1)`); the
  writer flushes at `block_datums = work_allowance` so default-written zero-size-datum blocks are
  readable under default limits (invariant tested); `max_blocks` lowered to 2^28 and `max_block_count`
  to 2^24; internal tables (block table, offsets, masks, chunks) are charged to `max_total_bytes`;
  `max_block_output_bytes` bounds in-flight transients; the `Limits` constructor validates fields.
* **R1-10 (compilation) — Adopted.** §4.5: column builders live in a schema-independent
  `Vector{ColumnBuilder}` driven by one loop with a per-column function barrier — no tuple unrolling,
  no specialisation on width or order; the compile gate runs over random widths/orders after a
  per-`E` warm-up. The per-cell dispatch cost is recorded as an accepted risk with a pre-agreed fallback.
* **R1-20 (parallel budgets) — Adopted.** §4.9: per-block local budgets; cumulative totals committed in
  block-index order by the coordinator, so `LimitError`s are raised at a deterministic block index and a
  later corrupt block cannot make an earlier valid block fail; tests force opposite schedules.
* **R3-1 (repair) — Adopted.** §4.2: `EnumSchema.default` is a `Default` (frozen JSON, span, resolved
  index, validity); invalid enum defaults are preserved verbatim and never used; `Avro.inspect` always
  performs a bounded permissive diagnostic parse (§3, §5.3).
* **R3-2 (limits reachability) — Adopted.** `tojson(...; limits)` bounded by output bytes, values and
  depth (§4.11).
* **R3-9 (inference) — Adopted by deferral.** Schema inference is deferred to 2.x with the required
  lattice recorded (§3); `Avro.write` requires `schema=` or a `Tables.schema` and the error message shows
  the `Tables.dictrowtable` recipe (§4.9, §5.4).

## New findings 1–20

1. **[blocker] Codec memory — Adopted.** §4.4/§4.9: `max_codec_memory` (256 MiB) passed to
   `CodecXz.XzDecompressor(; memlimit)` and `CodecZstd.ZstdDecompressor(; windowLogMax)` (CodecZstd ≥
   0.8.7; both verified in the authoring session to reject over-limit frames: "Window size larger than
   maximum" / `LZMA_MEMLIMIT_ERROR`); deflate/bzip2/snappy bounds stated; per-worker reservation in the
   in-flight bound; high-window xz/zstd fixtures with tiny output under an RSS-limited subprocess (§8.2,
   §9.9).
2. **[blocker] Work/allocation budget — Adopted.** See R1-1 above (encode counters, post-decompression
   rule, `block_datums`, internal tables, lowered ceilings).
3. **[major] Fast skipping vs strict — Adopted.** §4.3/§4.1: `validate=:strict` (default) walks every
   skipped value including sized blocks and decompresses/walks skipped OCF blocks; `validate=:fast`
   (opt-in) jumps, with its blind spots documented precisely; acceptance-equivalence tests in both modes
   (§9.8); Scan gate covers malformed data in projected-away fields and skipped blocks (§6).
4. **[major] Enum-default repair / inspect — Adopted.** See R3-1.
5. **[major] `tojson`/`compare` unbounded — Adopted.** Both take `limits`; cyclic values fail with
   `LimitError` via depth/value budgets (§4.11, §4.12).
6. **[major] Tuple-unrolled builders — Adopted.** See R1-10.
7. **[major] Parallel budget determinism — Adopted.** See R1-20.
8. **[major] Resolution output representation — Adopted.** §4.7: values follow the reader schema
   (unwrap for non-union and nullable readers; `UnionValue` with the reader branch index for tagged
   reader unions); `T` defaults from the effective reader schema (§5.2); all directions tested.
9. **[major] Non-decimal logical pairings — Adopted.** §4.7: resolved through the underlying schemas
   with the reader's interpretation, no unit conversion (documented hazard), kind mismatches fail;
   decision 20.
10. **[major] Logical sort order — Adopted.** §4.12: both comparators order logical types by their
    underlying encoding (decimal by bytes, duration by LE bytes); vectors for every logical type in both
    matrices (§8.2).
11. **[major] Inference contract — Adopted by deferral.** See R3-9.
12. **[major] Single-record gates — Adopted.** §4.5/§5.2: prepared `Avro.DatumReader`/`Avro.DatumWriter`
    own plans and are reusable; kernel gates apply to them; the one-shot API is reported separately
    (§10.1 protocol, §10.2).
13. **[minor] JSON union ambiguities — Adopted.** §4.11 states the union-default exception (bare JSON,
    first match); §4.2/§4.11 record the label-collision policy (schema accepted, JSON conversion raises;
    decision 21).
14. **[minor] Sort oracle normalisation / block forms — Adopted.** Verdicts normalised to
    `-1`/`0`/`1`/`ERROR:<class>`; cross-form array pairs (positive/sized) gated (§8.2, §4.12).
15. **[minor] Missing roots — Adopted.** `null`, `boolean`, `long`, `float` roots added for every codec
    and both producers (§8.2).
16. **[minor] Option validation — Adopted.** `Limits` constructor validation; `ntasks ≥ 1`; actual tasks
    capped at block count; `block_bytes ≤ max_block_bytes`; `block_datums ≤ work_allowance` (§4.4, §4.9).
17. **[minor] Atomic replacement contract — Adopted.** §4.9: `mktemp` in the destination directory (same
    filesystem, umask permissions), rename replaces existing files and symlinks-as-paths, directories are
    errors, Windows open-destination failure cleans up the temp, optional `fsync` (file only; directory
    not synced, documented), tests on every OS.
18. **[minor] Multi-tenant symbol quota — Adopted.** §6: `Avro.SymbolAdmission` caller-owned objects via
    `names=`; `Rows` admits lazily only when a Tables name/schema API is requested.
19. **[minor] Coverage gate — Adopted.** `julia-processcoverage` + Codecov with an informational ratchet
    (§11, §9.14).
20. **[nit] Wording — Adopted.** "documented conventional schema" (§4.8, decision 8).

## Milestone changes

Phase 0 adds high-window codec fixtures, the four roots, normalised sort errors, coverage plumbing, and
the codec-memory feasibility check (already verified); Phase 1 adds enum-default repair, `inspect`'s
permissive parse, and numeric-attribute validation; Phase 2 adds encode work, JSON/value-comparison
limits, both validation modes, internal allocation accounting, schema-independent builders, and the
prepared API; Phase 3 adds reader-directed union output, all logical pairings, underlying-encoding sort
order, and cross-form array tests; Phase 4a enforces codec memory caps, requires explicit schemas, and
asserts default-writer readability; Phase 4c makes budget failures deterministic; Phase 4d tests
malformed data in skipped regions under both modes; the RC rerun keeps the corrected gates.

No material objection to any round-4 finding remains on Claude's side; DRAFT v5 is submitted for round 5.
