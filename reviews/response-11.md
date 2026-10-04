# Response to codex-review-11 (round 11) — disposition of all items

The plan was revised in place to DRAFT v12. Every finding and every follow-up was adopted. Section
references are to v12. Verified before adoption: JSON.jl 1.7.1 `lazy.jl:276` allocates `Set{String}()`
per object only under `duplicate_keys=:error` (the default `:overwrite` allocates nothing); Java
avro-tools 1.12.2 reads a single xz stream followed by 4, 8 or even 1 byte of padding because it decodes
`count` datums and never verifies payload exhaustion, while fastavro raises `LZMAError` on any padding;
Java and fastavro both read zstandard blocks with a skippable frame or an empty frame between members.

## Carried rows

* **R1-1 / R4-2 / R5-1 / R5-2 / R5-3 and the round-6/7/8/9/10 partial rows — Adopted.** Each remaining
  defect is one of findings 1–6 below; the mechanisms marked resolved are unchanged.

## New findings 1–9

1. **[major] `Avro.Map` formula — Adopted.** The shell constant is measured at `__init__` from an empty
   `Avro.Map` like every other constant, and the charge is the actual allocated capacities: key/value
   vectors at `npairs`, the `Int32` index at `tablesz(npairs)`; the literal `128 + …` is gone; the oracle
   gate includes duplicate-heavy maps and every boundary count on every supported Julia version (§4.4,
   §9.10, decision 35).
2. **[major] JSON.jl's unbudgeted `Set` — Adopted.** §4.2/§4.11: Avro never enables JSON.jl's tracker
   (default `:overwrite`) and detects duplicate keys per object by sorting the object's key spans in a
   charged vector (no hashing, no `Dict`/`Set`; O(k log k) counted by the work rule); `ParseContext`,
   frozen objects and plan memo tables use the seeded deterministic-capacity map or sorted vectors,
   never `Base.Dict`/`Set`; an allocation-hook gate on wide objects and adversarial keys (§9.10;
   decision 39).
3. **[major] Typed decoding outside the ceiling — Adopted (explicit boundary).** §4.1 principle 2,
   §4.4 and §4.8: exactness covers `E`, the column path and the typed fast route's approved
   representations (isbits, `String`, `Vector{UInt8}`, `Vector{T}`, `Union{Missing,T}`, NamedTuples and
   plain structs of those, `Base.Enum`, `Symbol` via admission, `Avro.Map`); every other typed target
   takes the semantic route, whose conversion runs after ownership transfer in caller space — exactly
   as if the caller had called `StructUtils.make` — so `Dict` fields and custom hooks are outside the
   ceiling by construction and documented as such (decision 38).
4. **[major] Unreachable limits on schema operations — Adopted.** §5.1: `limits=Limits()` on
   `Avro.schema(T)`, `Avro.schema(::Tables.Schema)`, `Avro.json`, `canonical`, `fingerprint`,
   `parsingequivalent`; `JSON.json(schema)` uses `Limits()`; nested operations pass the caller's `Budget`
   through an internal `budget` argument and never stack fresh defaults (§4.4 scopes).
5. **[major] Writer/Reader invariant vs category (e) — Adopted.** §4.4: the writer preflights the
   reader's complete peak under its own budget — header/metadata representation, its parsed schema
   graph (asserted equal to the reader's charge), a generic read plan built at construction, the block
   table, one block's buffers and codec workspace, and the committed output estimate — and the
   invariant tests add the combinations (maximal schema + near-ceiling data, maximal metadata + large
   plan) (§12 4a, decision 40).
6. **[major] Map probe work — Adopted.** §4.4 "Map-probe bound": every probe is counted by the work
   counter; probe distance per key is capped at 64; exceeding it rebuilds with a fresh seed and double
   capacity (reserved first) at most twice; a third exceedance is a `LimitError`; so map and admission
   work is deterministic (≤ 3 × 64 probes per key); forced-collision gates for construction, lookup,
   admission and latency (§4.6, §9.10).
7. **[minor] Map capacity terms — Adopted.** `npairs`, `nunique`, capacity defined (§4.4, §4.6).
8. **[minor] Stale summaries — Adopted.** §9 oracle now `summarysize(x; exclude=Avro.Schema)`; decision
   30 says five categories; Phase 4c and the risk list say per block per pass; the `max_codec_memory`
   comment says "codec member".
9. **[minor] Layout `Ref` — Adopted.** §4.1 principle 5 records the write-once constants as the second
   documented exception, frozen before any public operation.

## Non-blocking follow-ups (all adopted)

* Map terms defined; stale text removed; write-once layout state documented. Deferrals unchanged.

No material objection to any round-11 finding remains on Claude's side; DRAFT v12 is submitted for round 12.
