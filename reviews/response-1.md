# Response to codex-review-1 (round 1) — disposition of all 29 findings

Every finding was dispositioned and the plan revised in place (`AVRO_REWRITE_PLAN.md`, DRAFT v2).
"Adopted" = incorporated as recommended; "Adopted, amended" = incorporated with a stated modification;
nothing was countered outright. Section references are to the revised plan.

## Blockers

1. **Limits are not DoS protection — Adopted.** §4.4 now defines per-value, per-block, cumulative
   per-operation (`max_total_bytes`, `max_total_values`, `max_rows`), schema/metadata, and concurrency
   (`max_inflight_blocks`) limits with conservative defaults (256 MiB blocks/values, 4 GiB cumulative),
   a `Budget` accumulator consulted by every collection/string/block/row decode, checked arithmetic,
   `sizehint!` capped at 1024, declared counts bounded by remaining bytes ÷ item minsize, zero-size items
   governed by the value budget, encode-side depth limiting, and a bounded `decode(schema, io; max_bytes)`
   (§4.3). Every corpus file must decode under the defaults (gate in §4.4/§12).
2. **Depth checks after the dangerous parse; duplicate keys; exact default text — Adopted.** §4.2 parsing
   now: (1) a lexical pre-scan rejects over-size/over-depth JSON before any parser recursion; (2) traversal
   uses `JSON.lazy(...; duplicate_keys=:error)` with `applyobject`/`applyarray`, so recursion depth is
   ours and value byte spans are available; (3) defaults keep the exact source span (`DefaultValue.json`).
   The same policy applies to datum JSON (`fromjson`). Protocols are deferred (finding 7).
3. **Recursive schemas vs plan trees and NamedTuple — Adopted.** §4.5: plans are built in two passes with
   `PlanRef` nodes keyed by schema identity (and `(writer, reader)` identity pairs for resolving plans);
   the generic path is dynamic and produces a stable `Avro.Record` at every width and for recursive
   records; static record plans exist only for a caller-supplied `T` (recursion through the user's own
   recursive structs). The 32-field NamedTuple threshold is gone from the value model (it survives only
   as a code-shape detail for column-builder iteration, §4.5).
4. **Union default rule — Adopted.** §4.2/§3: the default is matched against branches in declaration
   order and the *first branch that accepts it* is retained (`DefaultValue.branch`); the original JSON
   span is stored separately from the converted value; defaults are copied on every use; encoding
   requires every field (defaults never make fields optional, §4.2).
5. **Generic union decoding loses the branch — Adopted, amended.** §4.6: values of unions whose branch →
   Julia-type mapping is injective decode bare (`["null","string"]` → `Union{Missing,String}`), which
   keeps the dominant Tables case ergonomic and is exactly reversible by construction; every value of a
   non-injective union (two records, two enums, two fixed, bytes+fixed, …) decodes as
   `Avro.UnionValue(index, value)`, preserved through defaults, resolution, JSON, sorting and re-encoding.
   Amendment rationale: tagging *all* unions would make every nullable column a wrapper type, which is
   the one case where identity is provably unambiguous.
6. **Time/timestamp model loses data — Adopted, amended.** §4.6: exact `Int64` unit-tagged
   `Avro.Timestamp{Millisecond|Microsecond|Nanosecond}` (global instants) and `Avro.LocalTimestamp{Micro|Nano}`;
   `DateTime(x)` is an explicit, documented, lossy conversion; `Avro.Timestamp{P}(::DateTime)` exact;
   `ZonedDateTime` via a TimeZones extension; `Dates.Time` is exact for time-millis/micros (ns resolution)
   and non-aligned writes are rejected unless `Avro.truncate`/`Avro.round`/`Avro.Time{P}` is used;
   decision 14.4 replaced: a naive `DateTime` derives `local-timestamp-millis` and decodes from it
   exactly, global instants are never inferred from a naive `DateTime`. Amendment: `local-timestamp-millis`
   ↔ `DateTime` stays a bare mapping because it is exact in both directions; `Avro.Table(...;
   instants=:datetime)` is an explicit opt-in for the lossy convenience conversion.
7. **RPC fixtures do not test the wire — Adopted.** RPC (protocol declarations, MD5, handshake, framing,
   calls, transports) is deferred to 2.x (§3) with the wire-test requirements recorded (`BOTH`/`CLIENT`/
   `NONE`, retry, multi-frame, invalid lengths, metadata, declared/system errors, ping, one-way,
   protocol evolution, live `rpcsend`/`rpcreceive` interchange). The `interop/rpc` OCF files are excluded
   from the fixtures (§8.1) and `manual/protocols.md` removed from Phase 5.
8. **Invalid Julia declarations — Adopted.** §4.13: `DecodeError` is abstract with concrete `DataError`,
   `LimitError`, `CodecError`, `UnsupportedCodecError`; §11: `public` names are declared through a
   version-gated `Core.eval(Expr(:public, ...))`; Phase 0 adds a Julia 1.10 load/parse test.

## Majors

9. **PCF equality is not semantic equality; graph not immutable — Adopted.** §4.2: `==`/`hash` are
   structural and semantic (cycle-safe visited set; cached digest of the full printing);
   `Avro.parsingequivalent` is the PCF comparison; schemas are frozen (`FrozenVector`, frozen props,
   `RecordSchema` with `const` fields and a once-assigned `fields`), parser state is private, no setters.
10. **Untrusted schemas drive compilation — Adopted.** §4.1/§4.5: generic path fully dynamic; column
    builders specialise only per element type (closed set); typed specialisation only for caller-supplied
    `T`; a 1,000-schema compile-cost gate (method instances, cold compile time, invalidations) in Phase 2.
    Nested-shape open-endedness is recorded as an explicit risk (§14).
11. **Decoder validation and buffer assumptions — Adopted.** §4.3: booleans must be 0/1; UTF-8 validated
    for strings and map keys; buffers restricted to `Vector{UInt8}`/mmap vectors and unit-stride
    `SubArray`s thereof (others copied); sized blocks decoded through a bounded child range with exact
    exhaustion; checked arithmetic and `typemin` count rejection; trailing bytes rejected by default for
    top-level datums and single objects; blocks must be exactly consumed.
12. **Relaxed name validation too broad — Adopted.** Renamed `allow_invalid_names` (syntax only);
    structural and uniqueness rules (fields, symbols, fullnames, primitive names, define-before-use,
    aliases) always enforced; raw aliases stored as strings (§4.2, §3).
13. **JSON datum contract incomplete — Adopted.** §4.11 rule table: fullname wrapper keys for named
    branches, exactly one wrapper member, every record field exactly once and no extras, integer range
    checks, code points ≤ U+00FF for bytes/fixed, Java's quoted `"NaN"`/`"Infinity"`/`"-Infinity"` output
    with both quoted and bare forms accepted (verified against `fragtojson`/`jsontofrag`).
14. **Decimal resolution contradiction — Adopted.** §4.7: two recognised decimals with different
    precision/scale → `ResolutionError`; recognised-vs-plain resolves as the underlying type with the
    reader's interpretation winning; tags preserved; fresh defaults; UTF-8 validated for `bytes→string`;
    `(writer, reader)` pairs memoised (recursive pairs terminate). Test cases added in §9.5.
15. **Logical types incomplete — Adopted.** §4.8 per-type contract table (validation, bounds, JSON,
    oracle); decimal checks (precision > 0, 0 ≤ scale ≤ precision, fixed-size bound, encode digit check);
    big-decimal layout specified and kept optional, gated on the Java `BigDecimalConversion` vectors that
    were captured (`12.345 → 04 30 39 06`, `-1.5 → 02 f1 02`, `0 → 02 00 00`); UUID RFC-4122 validation;
    time-of-day ranges; duration unsigned LE confirmed.
16. **Fingerprint cache collisions — Adopted.** §4.10: `SchemaCache` keeps the PCF per entry, rejects
    non-equivalent registration under an existing fingerprint (`FingerprintCollisionError`), bounds
    entries/bytes, documents "identifier, not authentication", requires exact payload consumption.
17. **Sort order has no executable scope — Adopted.** §4.12 + Phase 3: `Avro.compare` on values and on
    encoded bytes; Java `Double.compare` NaN/−0.0 policy recorded and verified with the `Compare.java`
    harness; maps error unless `ignore`; depth-limited; vectors in §9.
18. **OCF compatibility unsafe by default; append under-specified — Adopted.** §4.9: strict by default;
    1.x tolerances only under `legacy=:avrojl1` (auto-enabled by the deprecated `readtable` shim); atomic
    sibling-temp + rename for path writes with failure cleanup; append deferred to 2.x with its
    requirements listed.
19. **Borrowed bytes/mmap lifetime — Adopted.** `bytes=:view` removed from 2.0; §4.9 ownership contract
    (Table copies and closes within the call; Rows/Reader hold until idempotent `close`; use-after-close
    throws; mmap truncation hazard documented with `mmap=false` as the safe option).
20. **Parallel decoding algorithm — Adopted.** §4.9: two-stage algorithm with checked prefix sums,
    bounded in-flight blocks, per-block chunks, `@sync`-style joins, first-cause propagation, cooperative
    cancellation via an `@atomic` flag, ordered assembly; direct disjoint writes deferred until proven;
    GC-stress and failure-propagation tests in §9.10.
21. **StructUtils fast path vs customisation — Adopted.** §4.8: `Avro.AvroStyle`; plain-DTO eligibility
    check for the direct route; semantic route through `StructUtils.make(AvroStyle(), T, generic)` for
    custom hooks/dynamic defaults/`choosetype`/abstract fields; tests against the pinned StructUtils
    head including "fast route never taken when a hook exists".
22. **Tables.Scan stale pin and mixed API — Adopted, amended.** §6 pins `df4e68c15c874079521d9d4ce4be67dce4345a31`
    (the commit the public `jq/scan` branch points at; the local checkout's head is an ancestor of it,
    so there is no newer public revision to pin) and states that `OpNode`/column-to-column nodes are
    rejected by `Tables.bind`/`Scan` before decoding; type overrides beyond exact widenings go through
    `Tables.scan`'s own rules; `select=()` keeps an authoritative row count; DataAPI added as a direct
    dependency (§11). Equivalence gate extended (§6).
23. **`Symbol` enums unsafe/lossy — Adopted.** §4.6: `Avro.EnumValue` (schema reference + index); typed
    targets may still choose `Base.Enum`/`Symbol`/`String`; decision 14.2 replaced.
24. **Protocol model incomplete — Adopted (by deferral).** Requirements recorded in §3 for 2.x.
25. **Interop gate errors — Adopted.** §8: Java xz used (`recodec --codec xz --level 6` verified);
    `cramjam` pinned for fastavro snappy; `avro==1.12.2`, `fastavro==1.12.2`, jar SHA-256 pinned;
    semantic comparison of decoded sequences/schema/metadata/counts instead of OCF bytes (raw datums
    compared byte-exactly via `jsontofrag`/`fragtojson`); versioned capability matrix (§8.4, measured
    with a `LogicalCaps` harness: Java recognises all 16 logical types, fastavro lacks nanos/fixed-uuid/
    big-decimal/duration, avro-py covers four); extra matrix rows (empty files, zero-row blocks, many
    small blocks, user metadata, unknown codecs, named-branch collisions, negative blocks, bombs, bad CRC,
    boundary logical values, legacy files).
26. **Property/fuzz oracles — Adopted.** §9: per-attribute assertions on schema round trips, bit-exact
    float comparison, Java/Python vectors and negative accept/reject corpora, sandboxed subprocess batches
    with wall/CPU/RSS limits, stable per-case RNG, persisted failing inputs, shrinking.
27. **Benchmark baseline errors — Adopted.** §10.1 corrected (bytes vs allocations; file sizes explained;
    workloads labelled; 1.1.2 benchmarked in a separate pinned process; in-JVM Java decode harness
    added); §10.2 uses ratio gates on the same host plus tracked absolute numbers per named host.
28. **Migration claims omit 1.x defects — Adopted.** §7 catalogues the five known 1.x defects (padding,
    `zstd` name, native-endian fixed-16 decimals, signed duration, hash names), `legacy=:avrojl1`
    handles them, `Avro.inspect` reports them, a rewrite recipe is given, shims are tested against real
    1.1.2-written fixtures; `Avro.decode(...; writer_schema=)` and `reader_schema=` naming adopted.

## Minor

29. **Literal NUL byte — Adopted.** Replaced with `U+0000–U+00FF`.

## Too broad / too narrow and milestones

* Deferred as recommended: RPC, append, writer-side parallel compression, borrowed views; big-decimal
  stays optional with Java vectors now in hand; Scan kept behind the pinned dependency and an
  equivalence gate; pre/nightly CI reduced to one OS non-blocking; the mutable logical-type registry
  removed (closed set in 2.0).
* Added as recommended: stable generic representations, cumulative budgets, ownership/close semantics,
  sort order phase, collision handling, atomic writes, zero-column/zero-row behaviour, many-schema
  compile tests, independent accept/reject corpora, downstream convention pins (Tables/StructUtils/JSON
  exact versions in the test environment; Arrow conventions by review).
* Phases restructured (§12): 0 corrected (baseline logs, pins, 1.10 gating), 1 gains minimal default-JSON
  handling and equality/freeze gates, 2 gains the hard architectural gates, 3 adds order, 4 split into
  4a–4d with interop checked after each codec, 5 without protocol docs. Readiness levels defined
  (review/PR/merge/RC/release); this task ends at PR-ready. "Green at every phase" is now defined as the
  new suite passing, with the legacy implementation removed at Phase 0 (§2.3).
* Fixed-value identity: `fixed` values remain `Vector{UInt8}` in the generic model because named identity
  is only observable inside unions, where `UnionValue` tagging now preserves it; a wrapper type for every
  fixed value would cost every column of fixed data for no observable gain. Open to a counter-argument.

No material objection to any finding remains on Claude's side; the revised plan is submitted for round 2.
