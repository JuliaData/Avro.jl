# Response to codex-review-20 (round 20) — disposition of all items

The plan was revised in place to DRAFT v21. Every finding and every follow-up was adopted. Section
references are to v21.

## Majors

1. **Writer and `max_block_output_bytes` — Adopted.** §4.9/§4.4: the writer tracks the exact reader-side
   output estimate of each pending block and flushes before `max_block_output_bytes` or
   `max_block_count` would be exceeded; a single datum whose estimate exceeds the output cap is a
   `LimitError`; the four-million-empty-strings `array<string>` fixture (≈ 4 MiB wire, ≈ 96 MiB reader
   charges) is refused under defaults and accepted with a raised cap on both sides; the invariant
   enumeration and §4.3 list the cap.
2. **Typed `Rows` contract — Adopted.** §6/§4.9/§12: generic record mode (`T=nothing`, record root)
   yields `Avro.Row` with the Tables interface and partitions; typed mode yields a plain iterator of
   `T` (no Tables interface, partitions or lazy name admission; `Symbol` values still pass admission);
   non-record mode is a plain iterator; `select=` applies to generic record mode only.
3. **Available-memory guard — Adopted (best-effort, improved).** §4.4: `available` also takes the
   cgroup's remaining memory (v2 `memory.max`/`memory.current`, v1 equivalents, when readable) and
   subtracts a package-wide `@atomic` counter of live reservations; the no-OOM promise is replaced by a
   documented best-effort statement; injected total/current/live-reservation boundary tests.
4. **Failure-work gates — Adopted.** §9.11, Phase 4c, §13 and decisions 22/34 state counter equality for
   successful operations only and a separate mandatory failure bound (same error, no retry, at most
   `inflight − 1` speculative higher blocks within their caps).

## Minors and nit (all adopted)

`SchemaStore` and `fromjson` detailed signatures synchronised; `limits=` and budget scopes for public
schema and copying value constructors, with `UnionValue(index, x)` validating only `index ≥ 1`;
`Avro.Time{P}` removed (alignment via `Avro.truncate`/`Avro.round`); schema-free rules for collections
of identity-bearing values; the remaining Scan artifacts removed (filter masks, development-pin note,
Phase 0 Tables SHA/bootstrap, skipped-block language); the parallel prose distinguishes the direct
lowest block from chunked higher blocks; `ntasks` counts the caller's task plus at most `ntasks − 1`
workers bounded by threads, `inflight` and headroom (`ntasks = 1` creates none); the default `W`
statement lists its declared terms; the constructor claim is narrowed to per-component fractions with
run-time enforcement of the combined peak; the no-hashing claim is narrowed to package-owned hash-table
lookups and hash-dependent charges; the invariant applies to a successfully closed writer's output; the
open risk says Phase 4a; decisions are in numeric order.

No material objection to any round-20 item remains on Claude's side; DRAFT v21 is submitted for round 21.
