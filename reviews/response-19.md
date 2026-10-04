# Response to codex-review-19 (round 19) — disposition of all items

The plan was revised in place to DRAFT v20. Every finding and every follow-up was adopted. Section
references are to v20.

## Majors

1. **Schema-object `type` — Adopted.** §4.2: a schema object's `type` is a string naming a primitive or
   one of `record`/`error`/`enum`/`array`/`map`/`fixed`; named references are schema strings and unions
   are arrays only (`{"type":"E"}` and `{"type":"union"}` are `SchemaError`s, as in Java); positive and
   negative fixtures for every form (decision 58).
2. **Parallel-only physical memory — Adopted.** §4.9: the lowest uncommitted block decodes directly into
   its final-column slice exactly as the `ntasks = 1` path does (no chunks, no parallel-only
   allocation); `W` is the complete per-block peak (compressed and decompressed buffers, codec
   workspace, chunk shells and payload, every transient scratch maximum, per-block state); the worker
   pool is created only from headroom — with none, the operation *is* the direct path (decision 56).
3. **Work on failures — Adopted.** §4.9: work equality is stated for successful operations; a failing
   parallel operation may additionally have decoded at most `inflight − 1` higher blocks, each under its
   own caps, which the failure gates assert; a stage-1 structural failure is carried as a pending indexed
   failure so a lower content failure still wins.
4. **Identity-bearing union recovery — Adopted.** §4.6: `Record`/`EnumValue`/`Fixed` select the branch
   whose named-schema identity they carry (fullname; size for fixed) before any representation-type
   rule; gated with several named branches of each kind (decision 58).

## Minors and nit (all adopted)

Signatures synchronised (`register!`/`lookup` with `limits=`, `fromjson(...; unknown=)`,
`Rows(...; select=)`, `Avro.Row`, validated copying public value constructors; `juliatype` needs no
budget); projection rules for duplicate/unknown names, typed `T`, non-record roots, preserved root
attributes and recursive-root substitution with a memo; the `Avro.Row` shell and yield-time transfer in
the storage oracle; all-duplicate maps keep `npairs` capacity charged; the Scan development pin, 1.10
bootstrap, Scan precompile entry and fast-mode offset/window language removed; `FixedSchema.doc`
removed (a fixed `doc` is metadata); empty enums defined; a missing `avro.schema` is a `DataError`;
strict `eachblock` walks `count` datums before yielding; decimal matching is additional to the
structural rules; GMP allocations listed and `WideDecimal` peaks reserved; non-encodable StructUtils
defaults are `ArgumentError`s with the tag remedy, and the TimeZones contract (UTC at schema precision,
zone not carried) is stated; fullname-first alias matching; the Phase 1 ignored-namespace gate
qualified; a finite certified Julia/JLL matrix for the portable invariant; the review log is in
chronological order.

No material objection to any round-19 item remains on Claude's side; DRAFT v20 is submitted for round 20.
