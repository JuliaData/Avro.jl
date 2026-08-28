# Response to codex-review-22 (round 22) — disposition of all items

The plan was revised in place to DRAFT v23. The single major and every minor, nit and follow-up were
adopted. Section references are to v23.

## Major

1. **Non-waiting head vs the higher-failure bound — Adopted (admission-wave barrier).** §4.9 rule 1: after
   its direct block the head never starts a block beyond an unresolved lower worker block; it waits for
   that fully reserved worker block (which needs nothing and cannot deadlock), commits its chunk, and
   only then takes the next unassigned block. The head never waits for memory, only for the barrier;
   rule 3's liveness statement is updated. The failure bounds (`inflight` on head failure, `inflight −
   1` on higher-block failure) now hold for every schedule.

## Minors and nit (all adopted)

2. One `reader_block_peak` function (buffers, codec workspace, output, map-sort scratch, replacement
   overlap, WTF-8 scratch, per-block state) is used by the writer preflight, sequential decoding and `W`
   (§4.4).
3. The admission equation includes `live_base` — the Budget's complete live total of category-(d)/(e)
   charges and the final `Avro.Table` shells (wrapper, column-reference container, stored
   `Tables.Schema`, metadata/partition state, row count), all in the storage oracle; `W` is charged
   atomically against it (§4.9).
4. The RSS child waits for the parent's acknowledgement before decoding; forced-interruption gates
   during decompression and assembly assert task joining, one-time reservation restoration, no
   post-return writes and propagation of the original interruption (§4.9, §9.11).
5. Phase 4a finalises codec and value constants; concrete `W` is measured after Phase 4b and recorded
   before the Phase 4c gate (§4.9).
6. Public constructors accept acyclic graphs directly and a builder form `Avro.RecordSchema(f, name)`
   for recursion (register-before-fill); logical annotations are distinct objects (`DecimalLogical`,
   `UUIDLogical`, …); `Avro.nodefault` is the no-default sentinel (`nothing`/`missing` mean a JSON
   `null` default); `props` collisions are rejected only for the keys the same constructor emits; the
   signatures carry defaults and `limits=Limits()` (§4.2, §5.1).
7. Typed conversions: narrow integers range-checked, `Float16` rounds to nearest (documented lossy),
   `Char` requires a one-character string, `ZonedDateTime` targets decode as UTC instants — all
   failures `ConversionError` (§4.6, §4.8).
8. `UnresolvableBranch` nodes defer the `ResolutionError` to datum selection, gated with
   `["int","string"]` → `"long"` (§4.7).
9. Primitive names are reserved only in the null namespace (`a.int` valid, both cases gated); the
   pre-scan rejects every RFC 8259 syntax error with negative fixtures; property collisions are
   per-context (§4.2).
10. The metadata default is `Dict{String,Vector{UInt8}}()`, with any abstract byte-vector dict accepted
    and copied (§4.9).
11. `SchemaCache` equality uses an internal routine charged to the caller's shared budget (§4.10).
12. Fast-validation wording: "malformed decompressed datum bytes" (§4.3).

Follow-ups: the remaining 2.1 Scan wording in the RC-ready and risk sections is removed or recast, and
Appendix B uses `first_row` throughout.

No material objection to any round-22 item remains on Claude's side; DRAFT v23 is submitted for round 23.
