# Response to codex-review-3 (round 3) — disposition of all items

The plan was revised in place to DRAFT v4. Every item Codex marked partially/not resolved and every new
finding was adopted. Section references are to v4.

## Open round-1 items

* **1 (limits; blocker remained) — Adopted.** §4.4 adds an input-proportional **work rule**
  (`max_values_per_byte = 64`, `work_allowance = 65_536`: values decoded ≤ 64 × input bytes consumed +
  allowance, with the declared block count pre-checked against it), `max_blocks`, input-byte accounting,
  per-datum/per-block encode limits reachable from every API (`limits=` on `encode`/`encode!`/
  `encodesingle`/`Writer`/`write`/`tobuffer`, §5), no writer-lifetime ceiling, and a budget floor clamp
  of 256 MiB with the small-RAM caveat documented. The 23-byte/2^30-count block now fails at datum
  ~67k instead of running a billion iterations (tested, §9.10).
* **9 (freezing; blocker remained) — Adopted.** §4.2: props and defaults are stored as recursively
  frozen JSON trees (`FrozenJSON`); `RecordSchema` uses frozen containers plus a `FrozenRef` hash filled
  once; hashes are computed bottom-up at freeze time and stored in the nodes; `canonical` is computed on
  demand and not cached; there is no identity side table (principle 5 holds). Julia default values are
  materialised fresh per use from the frozen tree.
* **10 (schema-derived specialisation; major remained) — Adopted.** §4.6 defines the finite enumerated
  value set `E` and §4.5 defines the compile gate over it: only the two-branch nullable union decodes
  bare; every other union (three-or-more branches, no-null, ambiguous) decodes as `Avro.UnionValue`,
  which removes schema-derived `Union{…}` element types; collections are typed one level deep over
  `E`; `ColumnBuilder{e}` exists only for `e ∈ E`; an enumeration test asserts `E`.
* **18 (writer lifecycle; major remained) — Adopted.** §4.9 failure contract covers final-block
  compression, sink write/flush, temp-file close, and rename; temp deletion in atomic mode; sink state
  documented for non-atomic/caller-IO; idempotent `close`; failure-injection tests at every phase (§9.9).
* **20 (parallel determinism; major remained) — Adopted.** §4.9: a failing block records an `@atomic`
  minimum index; workers abandon only blocks with a *higher* index, so lower blocks always run to
  completion or their own failure, and the lowest failing index is reported regardless of scheduling
  (tested over repeated schedules, §9.11). Peak-memory tests include chunks plus the assembly copy.
* **25 (fastavro union oracle; major remained) — Adopted.** §4.7/§8.2/§8.4: spec-policy expectations are
  written directly (branch index + result type); fastavro's non-observability (`7 int`) is recorded; Java
  witnesses branches through the JSON union wrapper under the `:java` policy.

## New findings 1–19

1. **[major] Invalid-schema repair for OCF — Adopted.** `allow_invalid_names` (names, namespaces,
   fullnames, field names, symbols — syntax only) and `allow_invalid_defaults` are accepted by
   `parseschema` and every container entry point (`Reader`/`Rows`/`Table`/`inspect`); invalid defaults
   are kept with `valid=false` and using one during resolution is an error unless the reader supplies a
   valid default (§3, §4.2, §4.9, §5).
2. **[major] Limits unreachable — Adopted.** `limits=` on all encode-side APIs; `decodesingle` gains
   `union_resolution`, `instants`, `limits`; per-datum/per-block/no-lifetime encode limits (§4.3, §5).
3. **[major] Reserved metadata — Adopted.** Every user-supplied `avro.*` key is rejected; `avro.schema`/
   `avro.codec` come only from constructor arguments (§4.9).
4. **[major] Codec input consumption — Adopted.** Every adapter must report clean EOS and full
   consumption; truncated and valid-plus-suffix cases for all five codecs (§4.9, §9.9).
5. **[major] `minsize` on recursive schemas — Adopted.** Memoised, cycle-safe fixed point (active edge
   contributes 0; unions = min over branches + index byte; records sum; arrays/maps 1) with the four
   named tests (§4.4).
6. **[major] Dormant Scan activation — Adopted.** §6 release rule: Scan ships only in a release whose
   `[compat]` requires a registered Tables with `Scan`, included unconditionally there; otherwise
   `src/scan.jl` and the keyword are removed from the 2.0.0 archive and ship later with a minimum Tables
   version; no runtime `isdefined` gating. Arrow 3 candidate smoke added as informational alongside the
   required registered-Arrow-2 smoke.
7. **[major] Weak codec deps in the test environment — Adopted.** `CodecBzip2`/`CodecXz` added to
   `test/Project.toml` with recorded versions (§11, §3).
8. **[major] Spec-union oracle — Adopted.** Direct expectations; fastavro only where it preserves branch
   identity (§4.7, §8.4 row added).
9. **[major] `inferschema` consumes the source — Adopted.** `Avro.inferschema(table; limits) ->
   (schema, table)` returns the bounded materialisation; `Avro.write(dst, table; infer=true)` owns and
   writes it; source-consumption semantics stated (§4.9, §5.1).
10. **[major] Error hierarchy / fuzz gate — Adopted.** `LimitError`, `CodecError`,
    `UnsupportedCodecError`, `ConversionError` are direct `AvroError` subtypes; `DecodeError` is
    reserved for `DataError`; the fuzz gate is split: raw datums under a fixed valid schema →
    `DataError`/`LimitError` or success; whole files/schemas/JSON → any `AvroError` or success (§4.13,
    §9.7).
11. **[major] RC validation — Adopted.** RC-ready requires instantiating a clean environment from the
    exact RC source archive after the Scan decision and pin removal, re-running the complete matrix
    (Julia versions, codecs, interop, fuzz, docs, cross-package), and the tag tree equal to the tested
    tree (§12).
12. **[minor] `fromjson` strict — Adopted.** Strict accepts only the quoted non-finite forms; `strict=false`
    additionally accepts the bare tokens (documented extension, §4.11).
13. **[minor] `comparebytes` consumption — Adopted.** Each buffer must contain exactly one datum (§4.12).
14. **[minor] DataAPI interface — Adopted.** `metadatasupport`, `metadatakeys`, keyed `metadata`,
    tested like Arrow's (§6).
15. **[minor] Decimal edges — Adopted.** Absent scale = 0; empty payload rejected (Java raises
    `NumberFormatException` — verified with a `DecEdge` harness); vectors for `00`, `ff`, sign extension,
    maximum precision, one-digit overflow; Java's lack of a decode-side precision check recorded as a
    deviation in favour of the spec (§4.6, §4.8, §8.4).
16. **[minor] Timestamp conversion error — Adopted.** `Avro.ConversionError` (not `DataError`) for
    `instants=:datetime` range failures and `DateTime`-sourced encodes (§4.6, §4.13).
17. **[minor] `eachblock` ownership — Adopted.** Returned bytes are owned copies valid after iteration
    advances (§4.9).
18. **[minor] Writer option validation — Adopted.** 16-byte copied sync (random from `RandomDevice` when
    `nothing`), `block_bytes > 0`, codec-specific level validation (§4.9).
19. **[minor] Measurement stability — Adopted.** §10.1 measurement protocol (cold processes, medians,
    version-gated scripts, "unavailable" never passes silently); compile gate uses `Base.specializations`
    counts with SnoopCompileCore/native-code size informational (§4.5, §9.12–13).

## Amendment decisions acknowledged

Findings 5, 6, 22 (round 1) and the fixed-value identity amendment are closed as accepted by Codex; the
"green at every phase" narrow reading is stated verbatim in §2.3/§12. Two new spec-vs-Java deviations
discovered while building vectors are recorded as decisions 18 (unsigned byte order) and 5 (decode-side
decimal precision), and the package documents itself as a data-format implementation, not Avro RPC (§3,
§7).

No material objection to any round-3 finding remains on Claude's side; DRAFT v4 is submitted for round 4.
