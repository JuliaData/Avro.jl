# Response to codex-review-21 (round 21) — disposition of all items

The plan was revised in place to DRAFT v22. Every finding and every follow-up was adopted. Section
references are to v22.

## Majors

1. **`instants=:datetime` vs the closed set `E` — Adopted (option removed).** §4.6/§5/§7/decision 4:
   there is no `instants=` option; timestamps always decode to the exact wrappers, and `DateTime` is
   reached by the explicit range-checked `DateTime(x)` conversion or by a typed `T` with `DateTime`
   fields. `E`, the column builders and the Table option list are unchanged and consistent.
2. **Typed `T` vs `instants` — Adopted (moot; `T` authoritative).** With the option gone, an explicit `T`
   is always authoritative and the typed plan performs the `DateTime` conversion itself.
3. **Failure-work bound — Adopted.** §4.9, §9.11, Phase 4c, decision 22: at most `inflight` surplus
   blocks when the direct head is the authoritative failure and `inflight − 1` when a higher block is;
   both schedules are forced gates; the CPU tolerance applies to successful runs and CPU is only
   recorded on failures.

## Minors and nits (all adopted)

Guard wording in §4.1 (best-effort, "usually"); the pending-reservation counter is the third
global-state exception with checked, clamped arithmetic and `finally`/finalizer restoration, and only
reserved-but-unallocated bytes are subtracted (no double counting with cgroup-resident memory); the
`Rows` synopsis is qualified by mode, typed `Rows` is writable through its retained schema but not a
Tables source, and `Avro.schema(::Avro.Row)` is defined; the direct head is a role (head + workers form
one admission wave; a chunk that becomes lowest is committed by the head) and `max_inflight_blocks`
counts higher blocks (`0 = ntasks − 1`); `committed_payload` and `committed_bytes` are defined without
double counting capacity; one consumer-independent `max_block_output_bytes` formula (generic
representation; no row/chunk/column headers); the `Avro.Row` oracle excludes the record and the
admission object; the default `W` scratch maximum is measured in Phase 4a; the comparison multiplier is
finalised from comparisons and moves and the "admits every legitimate input" claim is replaced; security
wording says "no unchecked or unreserved allocation"; §5 lists `Avro.Limits`, `SchemaCache`, the error
types and the public schema-constructor signatures with consistent `limits=Limits()`; `props` colliding
with structural keys are errors; `UnionValue` is a non-copying wrapper; `Avro.schema(ZonedDateTime)` is
`timestamp-millis` and float overflow/underflow/signed-zero follow IEEE 754 as Java does; invalid UTF-8
in `avro.codec` fails before lookup and compressed blocks are checked against `max_block_bytes` before
emission (incompressible boundary fixture); metadata values accept and copy `AbstractVector{UInt8}`;
dense IDs are "assigned by `freeze!`"; projection gates include direct and mutual recursive roots; the
PR-ready pins drop the `[sources]`/bootstrap residue; "eight workers" is `ntasks = 8`; Appendix B is
executable as written (no `instants`, no `scan=`, `row` scoped) and the duplicate heading check found
one §8.2 heading.

No material objection to any round-21 item remains on Claude's side; DRAFT v22 is submitted for round 22.
