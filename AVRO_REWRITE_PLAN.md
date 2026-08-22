# Avro.jl 2.0 — Audit and Rewrite Plan

Status: DRAFT v2 (revised after Codex review round 1; see §14 review log and `reviews/response-1.md`).
Date: 2026-08-21/22. Repository: `JuliaData/Avro.jl`, local checkout `/Users/jacob.quinn/.julia/dev/Avro`,
branch `jq/v2-rewrite` forked from `main` @ `0c7be10db6d83fd20806a8eceec9276a7aa8e21d` (v1.1.2, registered).
Specification source pinned for this plan: `apache/avro` `main` @ `326950f40c1172f7564c757b0e51c39883721083`
(`doc/content/en/docs/++version++/Specification/_index.md`, 1.13.0-SNAPSHOT text, 2026-08-18), plus the
Apache shared test data under `share/test/` at the same commit. Reference implementations pinned for
differential testing: Apache Java `avro-tools` 1.12.2 (Maven Central jar, SHA-256
`6220e8bc089aaf917cdad4cd358bd651fc0394c0e5ddb8b36da402012c294a68`), Python `fastavro==1.12.2` (+ `cramjam`
for snappy) and `avro==1.12.2`. Where the 1.13-SNAPSHOT text describes behaviour the 1.12.2 tools lack,
the spec text is authoritative and the capability matrix in §8.4 records which oracle covers what.

Review protocol: this file is the single text both reviewers operate on. Each review round produces
`reviews/codex-review-N.md` (Codex, read-only sandbox) and `reviews/response-N.md` (Claude disposition);
the plan is revised in place and the decision log in §14 is appended. The loop ends only when Codex's
file ends with `VERDICT: AGREE` and Claude's response states no remaining material objections.
Declared artifacts, fixtures, benchmarks, and gates in this plan are **deliverables of the implementation
phases, not preconditions of the review**.

---

## 1. Executive summary

Avro.jl 1.1.2 is a ~1,600-line package that covers a useful slice of Avro (all types, the logical types it
knows, container files with four codecs, a Tables.jl sink/source) but it is neither spec-correct nor safe
on untrusted input, and its architecture (two-pass `nbytes` sizing, per-value dynamic dispatch through
StructTypes closures, `Vector{UInt8}`-only decoding with `@inbounds` and no bounds discipline,
Julia-`Union`-ordered unions, global codec state, writer-schema discarded on read) cannot be
incrementally fixed into a leading implementation. The audit in §2 was done empirically against the
Apache corpus, the Apache Java tools, fastavro, and hand-built malformed inputs, and was independently
re-verified by the round-1 reviewer (16 of 20 rows confirmed, 4 corrected in §2.2). Headline findings:

* **Interop is broken in both directions.** Every uncompressed container file 1.1.2 writes is rejected by
  Apache Java ("Block read partially"): the null-codec block carries the uninitialised 5% sizing cushion.
  Its zstd files name the codec `zstd` (spec: `zstandard`) and are rejected by Java and fastavro. Its
  decimals are written native-endian into a 16-byte fixed (spec: big-endian two's complement). Reading:
  the official `weather-zstd.avro` **segfaults** the process; `weather-snappy.avro` returns garbage
  silently; logical types written by Java decode at wrong offsets because the container reader discards
  the writer schema and regenerates a lossy one from Julia types; a `["string","int"]` union (Apache
  `withUnion/data.avro`) throws a `MethodError` because union branches are mapped through Julia's
  canonical `Union` order instead of the schema's branch order.
* **Untrusted input is unsafe.** Truncated or over-long varints silently decode as `0`; `fixed` reads past
  the buffer through `@inbounds`; a negative length → `OutOfMemoryError`; a 7-byte array header requests
  a ~8 TiB allocation.
* **Schema handling is permissive and incomplete.** Duplicate field names, invalid names and enum symbols,
  duplicate union branches, and mismatched defaults are accepted; nested unions fail with a pathless JSON
  error; no canonical form, fingerprints, schema resolution, single-object encoding, JSON encoding, sort
  order; `timestamp-nanos`/`big-decimal` silently fall back to their underlying types and fixed `uuid`
  is decoded with the string path.
* **Performance is an order of magnitude off.** ~20 MB/s write and ~7 s to materialise a 1M-row,
  4-column file versus fastavro's 0.65 s write / 0.50 s read (same machine, single thread; §10.1).

The plan (§4–§12) replaces the core with a schema-compiled encoder/decoder over bounds-checked byte
buffers with cumulative resource budgets, a faithful schema model (names, aliases, defaults, logical
types, canonical form, fingerprints, resolution, sort order), a strict, streaming, parallel container
reader/writer, a columnar `Avro.Table` with `Tables.Scan` pushdown, a row-streaming `Avro.Rows`, a
StructUtils-based typed API, single-object encoding, and JSON encoding, gated by conformance against
Apache Java, fastavro, and avro-python. RPC, append mode, writer-side parallel compression, and borrowed
byte views are deferred to 2.x with their requirements recorded. The release is **Avro.jl 2.0.0**
(breaking; §7 gives the migration policy and the catalogue of 1.x data defects).

---

## 2. Evidence-based audit of 1.1.2

### 2.1 Method

* Full read of `src/` (12 files) and `test/runtests.jl`; baseline suite on Julia 1.12.6: 65,635 passes.
* Apache corpus read with `Avro.readtable` (each in an isolated process after the first segfault).
* Julia-written files read with `avro-tools tojson/getschema` and `fastavro.reader`.
* Java-written records (`fromjson` with decimal/uuid/timestamp-micros/enum/union-default schema) read back.
* Hand-built malformed inputs for every decoder primitive; schema-parser probes for spec constraints.
* Throughput/allocation baseline (§10.1). Round-1 reviewer re-ran the non-destructive probes.

### 2.2 Results (source citations are to 1.1.2 at `0c7be10`)

| Probe | 1.1.2 result | Cause | Severity |
|---|---|---|---|
| `weather.avro`, `weather-deflate.avro`, `weather-sorted.avro`, `syncInMeta.avro`, `schemas/simple/data.avro` | read correctly (values verified against fastavro/Java) | — | — |
| `weather-zstd.avro` (codec `zstandard`) | **segfault** (exit 139 at `src/types/binary.jl:171`) | only `zstd` registered (`src/Avro.jl:128-134`); unknown codec → `nothing` → compressed bytes parsed as records under `@inbounds` (`src/tables.jl:173-174`) | memory safety |
| `weather-snappy.avro` | **silent garbage** (5 rows of compressed bytes) | snappy disabled (`src/Avro.jl:125,133`) | silent corruption |
| `schemas/withUnion/data.avro` (`["string","int"]`) | `MethodError` | schema order and Julia `Union` order combined independently (`src/types/unions.jl:54-58`) | correctness |
| Java-written `decimal(bytes)`, `decimal(fixed 8)`, `uuid`, `timestamp-micros`, enum, `["null","string"]` | UUID decoded from a wrong offset | **corrected by review:** `readtable` discards the writer schema (`src/tables.jl:198`) and row decoding regenerates a lossy schema from Julia types (`src/types/binary.jl:74-75`); decimal regeneration is always fixed-16 (`src/types/logical.jl:9-19`) | correctness |
| Julia-written `jl.avro` (null codec) → Java `tojson` | **"Block read partially, the data may be corrupt"** | whole sizing cushion emitted as block data (`src/tables.jl:83-116`) | interop |
| Julia-written `jl-zstd.avro` → Java / fastavro | "Unrecognized codec: zstd" | `String(compress)` written as codec name (`src/tables.jl:60-65`) | interop |
| Julia-written `jl-deflate.avro` → Java / fastavro | read correctly | — | — |
| `Avro.read([0x01,0x61], String)` (length −1) | `OutOfMemoryError` | negative length reaches allocation (`src/types/binary.jl:244-247`) | DoS |
| `Avro.read([0x80,0x80], Int64)` (truncated varint) | returns `0` silently | loop returns accumulator at buffer end (`src/types/binary.jl:155-166`) | silent corruption |
| 11-byte varint | returns `0` silently | no byte-count/terminal-bit check (same) | silent corruption |
| `Avro.read([1,2], NTuple{4,UInt8})` | returned `(1,2,0,0)`; out-of-bounds read (values not guaranteed) | unchecked `@inbounds` (`src/types/fixed.jl:34-43`) | memory safety |
| union index 4 of a 2-branch union | `BoundsError` | direct indexing (`src/types/unions.jl:54-58`) | ungraceful |
| enum index out of range | datum accepted; `BoundsError` only at `show` | no validation (`src/types/enums.jl:45-47`, `:23-25`) | silent corruption |
| array count 2^40 (7 bytes of input) | direct request for ~8 TiB (`src/types/arrays.jl:61-72`); in the authoring session the process thrashed until killed after 10 min (not re-run by the reviewer) | `Vector{Int}(undef, len)` from untrusted count | DoS |
| duplicate field names / `"9R"` name / `"a-b"` enum symbol / `["int","int"]` / `"default":"oops"` for int | all accepted | parsing delegates to JSON3 without validation (`src/utils.jl:68-70`) | spec |
| nested union `["null",["int","string"]]` | JSON3 parse error (`ExpectedOpeningQuoteChar`) | spec-invalid input rejected with a pathless low-level error | ungraceful |
| `timestamp-nanos`, `big-decimal` | silently fall back to `long`/`bytes` | no logical-type entries | spec gap |
| fixed `uuid` | parsed as `UUIDType("fixed","uuid")` but read/written with the **string** encoding | `src/types/logical.jl:53-73` | correctness |
| Wide `Tables.Schema` (issue #18) | **corrected by review:** a 10,001-column NamedTuple source works; the failing case is `Tables.Schema{nothing,nothing}` (names/types stored in fields, as DataFrames produce for >10k columns) | `src/types/rows.jl:20-22` calls `fieldcount(Nothing)` | bug |
| Throughput (1M rows × {long,double,string,boolean}) | write 1.02–1.21 s; read-index 2.9–3.9 s + materialise 3.9 s; single-record read 1,376 **bytes** / 38 allocations (the draft-v1 figure mislabelled bytes as allocations) | two-pass sizing, closure dispatch, boxed positions | performance |

Open upstream issues map onto these: #15 (row-at-a-time read/write → §5.3 `Avro.Writer`/`Avro.Rows`),
#17 (buffer-too-small with string columns → two-pass sizing removed), #18 (wide `Tables.Schema`),
#6 (pre-written object files → §8 corpus), #13 (`signed(UInt8)` on Julia < 1.5 → moot, floor is 1.10),
PR #16 (row-wise API prototype → superseded by §5.3).

### 2.3 What stays, what goes

Keep (as ideas, re-implemented under the new core, with their tests ported):

* The public *shape* of the container API (`writetable`/`readtable`/`tobuffer`/`parseschema`) survives as
  deprecated shims for one major cycle (§7).
* `missing` as the Julia value of Avro `null` (ecosystem convention; `nothing` also encodes as null).
* `Date`/`Time`/`UUID` mappings; `Avro.Decimal` and `Avro.Duration` names (both redesigned: spec byte
  order, `UInt32` duration fields).
* The zigzag/varint arithmetic (correct as written), the `Tables.partitions` → blocks idea, the
  `dictrowtable` fallback for schema-less sources, the block pre-scan idea.
* The test corpus of Julia round-trip cases in `test/runtests.jl` (ported to the new API).

Replace:

* StructTypes/JSON3 schema model (`Schema = Union{String, LogicalType, SchemaType, UnionType}`,
  `Base.@pure eachunion`, mutable schema structs with `type::String` fields) → frozen schema graph with a
  real parser/validator (§4.2).
* `nbytes` two-pass writer and `Vector{UInt8}`-only, `@inbounds`, position-tuple decoders → growable
  encoder, bounds-checked decoder with budgets, compiled read/write plans (§4.3–§4.5).
* `Avro.Record{names,types,N}` lazy-field record, `Avro.Enum{names}`, `Avro.Array{T}` lazy vector →
  typed columnar `Avro.Table`, stable generic `Avro.Record`/`Avro.EnumValue`, plain `Vector`s (§4.6).
* Global per-thread codec arrays (`COMPRESSORS[Threads.threadid()]`, unsafe under task migration) →
  per-task codec instances (§4.9).
* CI (Julia 1.5/1/nightly, ubuntu only, actions v1) → §11 matrix.

The legacy implementation is **removed at Phase 0** (clean break on the branch); the new test suite is
the suite from Phase 0 on, and the 1.x cases are ported as each feature lands (Phase 5 ports the rest
through the shims). "Green at every phase" means `Pkg.test()` of the new suite passes at the end of
every phase on Julia 1.10 and 1.12.

---

## 3. Specification surface assessment

"Required" ships in 2.0.0; "Optional" ships in 2.0.0 if its phase gate passes, else 2.x; "Deferred" is
intentionally out of scope for 2.0.0 with the reason and the requirements for later inclusion recorded.

| Spec area | Scope | Notes |
|---|---|---|
| Schema declaration, primitive & complex types, attributes as metadata | Required | unknown attributes preserved (`props`) and re-emitted; stripped by canonical form |
| Names, namespaces, fullname algorithm, define-before-use, uniqueness, reserved primitive names | Required | §4.2; the spec's `Example`/`Simple`/`a.full.Name` example is a test |
| Aliases (type and field; relative/qualified; any string) | Required | used in resolution; subject to uniqueness |
| Record field `default` (all types; union default = first **matching** branch; bytes/fixed code points 0–255), `order`, `doc` | Required | defaults validated at parse time; selected union branch retained |
| Enum `default` | Required | used in resolution |
| Union constraints (no immediate nesting, one unnamed type per kind) | Required | |
| "Fixing an invalid, but previously accepted, schema" | Required | `allow_invalid_names=true` relaxes name *syntax* only; all structural and uniqueness rules stay |
| Binary encoding, all types, blocked arrays/maps with negative counts and sizes | Required | reader accepts both block forms with exact size exhaustion; writer emits positive-count blocks |
| JSON encoding of datums | Required | full rule table §4.11; used by defaults, differential tests, debugging |
| Single-object encoding (`C3 01` + CRC-64-AVRO LE + payload) | Required | with a collision-safe `SchemaStore` (§4.10) |
| Sort order (values and encoded datums, record `order`, map error) | Required (Phase 3) | `Avro.compare`; Java-backed vectors via the `Compare` harness |
| Object container files: header, metadata, blocks, sync, codecs `null`/`deflate` | Required | strict by default (§4.9) |
| Codecs `snappy` (with CRC32), `zstandard` | Required | hard dependencies (Snappy.jl, CodecZstd) |
| Codecs `bzip2`, `xz` | Required, via package extensions | CodecBzip2/CodecXz as weak deps; actionable error when absent |
| Schema resolution (all rules incl. promotions, reorder, defaults, enum default, unions, aliases, decimals) | Required | §4.7 |
| Parsing Canonical Form, fingerprints (CRC-64-AVRO, MD5, SHA-256) | Required | conformance file `schema-tests.txt` |
| Logical types: decimal (bytes/fixed), uuid (string/fixed), date, time-millis/micros, timestamp-millis/micros/nanos, local-timestamp-millis/micros/nanos, duration | Required | per-type contract §4.8; invalid logical types ignored per spec |
| Logical type `big-decimal` | Optional | Java `BigDecimalConversion` vectors captured (`12.345 → 04 30 39 06`, `-1.5 → 02 f1 02`, `0 → 02 00 00`); ships if the Phase 2 gate passes against them |
| Protocol declaration (`.avpr`), messages, errors, MD5 | **Deferred to 2.x** | requires: distinct anonymous request-schema type, property preservation, a deterministic Java-compatible printer whose bytes are what MD5 hashes, one-way validation, ping |
| Handshake, framing, call format; HTTP transport | **Deferred to 2.x** | requires real wire tests: `BOTH`/`CLIENT`/`NONE` handshakes and retry, multi-frame messages, invalid frame lengths, metadata, declared and system errors, ping, one-way, protocol evolution, and a live interchange with `avro-tools rpcsend/rpcreceive`. The `share/test/interop/rpc` files are OCF datum files, not wire captures, and are not a gate |
| Avro IDL (`.avdl`), Trevni, tethered MapReduce | Deferred | outside the data format |

---

## 4. Architecture

### 4.1 Principles

1. **Spec first, Java as tie-breaker.** Where the spec is silent or ambiguous, behave like Apache Java
   1.12 and record the choice in §14. Where the spec is explicit and Java is lax (e.g. boolean bytes
   other than 0/1), follow the spec.
2. **Never trust bytes.** Every decode path is bounds-checked against an explicit end position with checked
   arithmetic; every length/count is validated before allocation; cumulative budgets cap total work; the
   only failure modes on malformed input are `Avro.DecodeError` subtypes.
3. **Stable generic values, opt-in specialisation.** Untrusted schemas never drive Julia compilation:
   the generic path uses dynamic plans and stable value types (`Avro.Record`, `Avro.EnumValue`,
   `Avro.UnionValue`, exact timestamp wrappers). Specialised, unrolled code is generated only for a
   caller-supplied target type `T` or for the closed set of column element types in `Avro.Table`.
4. **Stream by default, materialise on request.** Container reading is block-at-a-time with bounded
   in-flight memory; `Avro.Table` is the explicit materialisation and owns copies of everything.
5. **No global mutable state.** No runtime registries; codec instances, buffers, and plans are owned by
   reader/writer objects or tasks. The logical-type set is closed in 2.0 (unknown names are preserved as
   `UnknownLogical`).
6. **Qualified API, zero exports.**

### 4.2 Schema model (`src/schema.jl`, `src/names.jl`, `src/logical.jl`, `src/canonical.jl`)

```julia
abstract type Schema end
struct NullSchema <: Schema; props::Props; end             # Props = frozen ordered map of custom attributes
struct BooleanSchema <: Schema; props::Props; end
struct IntSchema <: Schema; logical::Union{Nothing,LogicalType}; props::Props; end
struct LongSchema <: Schema; logical; props; end
struct FloatSchema <: Schema; props; end
struct DoubleSchema <: Schema; props; end
struct BytesSchema <: Schema; logical; props; end
struct StringSchema <: Schema; logical; props; end
struct ArraySchema <: Schema; items::Schema; props; end
struct MapSchema <: Schema; values::Schema; props; end
struct UnionSchema <: Schema; branches::FrozenVector{Schema}; end
struct FixedSchema <: Schema; name::FullName; aliases::FrozenVector{String}; doc; size::Int; logical; props; end
struct EnumSchema <: Schema; name::FullName; aliases; doc; symbols::FrozenVector{String}; default::Union{Nothing,Int}; symbolindex::Dict{String,Int}; props; end
struct Field; name::String; schema::Schema; doc; default::Default; order::Order; aliases::FrozenVector{String}; props; end
mutable struct RecordSchema <: Schema      # the only mutable node: its `fields` are bound once, after the
    const name::FullName; const aliases; const doc; const iserror::Bool; const props   # record is registered,
    fields::FrozenVector{Field}; fieldindex::Dict{String,Int}                           # so self-references resolve
end
struct FullName; name::String; namespace::String; end       # fullname(x) = isempty(ns) ? name : ns*"."*name
```

* **Immutability contract.** Schemas are frozen after construction: vectors are `FrozenVector`s
  (private `AbstractVector` with `setindex!`/`push!` throwing), `props` is a frozen ordered map, and
  `RecordSchema.fields` is assigned exactly once by the parser (or the public constructors) and never
  reassigned; there are no public setters. Hashes and canonical forms are computed after freezing and
  cached. The parser's mutable state (`ParseContext`) is private.
* **Equality.** `==` is structural and semantic: type kind, fullname, fields (name, schema, default,
  order, aliases, doc, props), symbols, enum default, size, logical type and its attributes, and custom
  props, compared recursively with a visited set of `(a,b)` pairs so cyclic graphs terminate; `hash` is
  consistent (cached digest of the full spec-JSON printing). `Avro.parsingequivalent(a, b)` compares
  Parsing Canonical Forms (the spec's notion of "the same schema for readers").
* Logical types are *attributes* of the underlying schema (`logical`), a closed set:
  `Decimal(precision, scale)`, `BigDecimal`, `UUIDLogical`, `DateLogical`, `TimeMillis`, `TimeMicros`,
  `TimestampMillis/Micros/Nanos`, `LocalTimestampMillis/Micros/Nanos`, `DurationLogical`, and
  `UnknownLogical(name)` (kept so the schema re-serialises faithfully). Invalid logical types (wrong
  underlying type, `precision ≤ 0`, `scale < 0`, `scale > precision`, precision exceeding
  `floor(log10(2^(8n−1) − 1))` for fixed size n, duration size ≠ 12, uuid fixed size ≠ 16) are dropped
  to the underlying type per spec with their attributes preserved in `props`.
* `Default` is `nothing` (absent) or `Some(DefaultValue(value, branch, json))`: `value` is the default
  already validated and converted to the field schema's Julia value (so `null` defaults are
  representable), `branch` is the selected union branch (first branch that accepts the default, in
  declaration order), and `json` is the exact source text span of the default (from the lazy parser)
  for faithful re-serialisation. Defaults are **copied on every use** (fresh array/map/record per
  decoded record); they never make a field optional when encoding.
* **Parsing.** `Avro.parseschema(src; allow_invalid_names=false, limits=Limits())`:
  1. A lexical pre-scan (no allocation; handles strings/escapes) rejects inputs over `max_schema_bytes`
     or deeper than `max_schema_depth` *before* any parser recursion.
  2. The JSON is traversed with `JSON.lazy(...; duplicate_keys=:error)` (`applyobject`/`applyarray`),
     so recursion depth is ours, duplicate keys are errors, and value byte spans are available.
  3. A `ParseContext{namespace stack, named-type table, depth}` validates every rule: name grammar
     (unless `allow_invalid_names`), namespace grammar, fullname uniqueness, primitive names never
     redefined, define-before-use (depth-first, left-to-right), field/symbol uniqueness within scope,
     alias uniqueness against names and other aliases, union rules, enum default membership, fixed
     size ≥ 0, `order` values, default validity (including union branch selection and integer range
     checks: JSON integers that do not fit `Int32`/`Int64`, e.g. `BigInt`, are invalid defaults).
     Errors are `SchemaError`s carrying a JSON path (`record R > field next > union[1]`).
  4. Named-type references resolve by fullname (qualified, or relative to the enclosing namespace);
     recursive references bind to the already-registered (field-less) record and are closed when its
     `fields` are assigned.
* **Printing.** `Avro.json(schema; pretty=false)` / `JSON.json(schema)` emits spec JSON (first occurrence
  of a named type in full, later references by fullname, namespace attribute only when it differs from
  the enclosing one, custom `props` and exact default text included). `Avro.canonical(schema)`
  implements the seven PCF transformations; `Avro.fingerprint(schema; algorithm=:crc64avro|:md5|:sha256)`
  hashes the PCF bytes (CRC-64-AVRO in-house from the spec pseudo-code; MD5 via `MD5.jl`; SHA-256 via
  `SHA`).
* Julia type → schema (`Avro.schema(T)`, `Avro.schema(::Tables.Schema)`): §4.8.

### 4.3 Byte-level decoder and encoder (`src/decoder.jl`, `src/encoder.jl`)

```julia
mutable struct Decoder
    buf::Vector{UInt8}; pos::Int; stop::Int        # next byte; last valid byte (inclusive)
    depth::Int; budget::Budget; limits::Limits
end
```

* **Buffers.** The decoder works on `Vector{UInt8}` and on `Mmap`-backed `Vector{UInt8}` (both one-based,
  unit-stride, stable). Other `AbstractVector{UInt8}` inputs (views, `Memory`, custom arrays) are
  accepted at the public API and copied into a `Vector{UInt8}` unless they are `SubArray`s of a
  `Vector{UInt8}` with unit stride, which are decoded through `(parent, offset, length)`. Nothing else
  is assumed about memory.
* **Primitives**: `readbool` (byte must be 0 or 1), `readint` (≤ 5 bytes, value range-checked into
  `Int32`), `readlong` (≤ 10 bytes, bit 64 overflow rejected), `readfloat`/`readdouble` (little-endian
  `reinterpret`; NaN payloads preserved bit-for-bit), `readlen` (non-negative, `≤ stop − pos + 1`),
  `readbytes` → copy, `readstring` → `String` validated as UTF-8 (`DataError` otherwise; also map keys),
  `readfixed(n)`, and `skip*` counterparts. All arithmetic on positions/sizes uses checked operations;
  block counts equal to `typemin(Int64)` are rejected (negation overflow). Every failure throws a
  `DataError(msg, pos)`. No `@inbounds` anywhere that is not immediately preceded by an explicit range
  check on the same values.
* **Sized blocks.** A negative array/map count is followed by a byte size; the block is decoded through a
  bounded child range `[pos, pos+size)` and must be **exactly exhausted** (`DataError` otherwise).
* **Trailing bytes.** Top-level datum decoding (`Avro.decode`, single-object) rejects trailing bytes by
  default (`allow_trailing=false`); a container block must be exactly consumed by its `count` records.
* **IO sources.** The container layer reads blocks of known size into owned buffers. `Avro.decode(schema,
  io; max_bytes=limits.max_datum_bytes)` reads at most `max_bytes + 1` bytes and errors if the datum is
  larger or if bytes remain (no unbounded read-to-EOF).
* **Encoder** is a growable `Vector{UInt8}` with `pos`, `ensureroom!` (checked growth), and write
  primitives (`writelong` = 10-byte unrolled varint, `writebytes`, `writestring`, `writefloat`, …).
  `Encoder` is re-usable (`reset!`) and its buffer can be handed to a codec or `IO` without copying. No
  pre-sizing pass. Encoding is depth-limited (`max_depth`, so self-referential Julia values fail cleanly)
  and validates values against the schema (union branch acceptance, enum membership, fixed length,
  integer ranges, UTF-8 of strings produced from `codeunits`, decimal precision, time-of-day ranges).

### 4.4 Limits and budgets (`src/limits.jl`)

```julia
Base.@kwdef struct Limits
    # per value
    max_depth::Int            = 1024          # nesting depth of values (recursive schemas); encode and decode
    max_bytes::Int            = 256 << 20     # one bytes/string/fixed value (256 MiB)
    max_datum_bytes::Int      = 256 << 20     # Avro.decode from IO; single-object payloads
    # per container block
    max_block_bytes::Int      = 256 << 20     # compressed and decompressed size of one block
    max_block_count::Int      = 2^31 - 1      # declared records per block (also bounded by bytes)
    # cumulative per top-level operation (decode/Table/Rows/Reader); checked arithmetic throughout
    max_total_bytes::Int      = 4 << 30       # sum of decoded value bytes (strings, bytes, fixed) + decompressed block bytes
    max_total_values::Int     = 2^31 - 1      # sum of decoded values (including null and empty-record items)
    max_rows::Int             = 2^31 - 1      # container rows per operation
    # schema / metadata
    max_schema_bytes::Int     = 16 << 20
    max_schema_depth::Int     = 256           # JSON nesting depth of a schema document
    max_metadata_bytes::Int   = 16 << 20      # total OCF metadata (keys + values, including avro.schema)
    max_metadata_entries::Int = 10_000
    # concurrency
    max_inflight_blocks::Int  = 0             # 0 = ntasks; bounds decompressed buffers to max_inflight_blocks × max_block_bytes
end
```

`Budget` is a mutable per-operation accumulator (values, bytes, rows) consulted by every collection,
string, block, and row decode; exceeding any field throws `LimitError` naming the limit and how to
raise it. Collections are decoded incrementally: `sizehint!` is capped at `min(count, 1024)` (never the
declared count), growth is by `push!`, and the declared count is additionally bounded by
`remaining_bytes ÷ minsize(item schema)` when `minsize > 0`. Items of zero encoded size (null, empty
record, empty fixed) are governed by `max_total_values`. Encoding is depth-limited only (values are
trusted). Defaults are deliberately conservative; every corpus file in §8 must decode under them, and
every `LimitError` message states the limit name and the keyword to raise it.

### 4.5 Plans: schema-directed codecs (`src/plan_read.jl`, `src/plan_write.jl`)

* A **read plan** is a tree of plan nodes, one per schema node. The **generic** plan family is dynamic
  (`Vector{ReadPlan}` children, one dynamic dispatch per child behind a function barrier) and produces
  the stable generic values of §4.6; it never specialises on schema shape, so untrusted schemas cost no
  compilation beyond the first use of each node kind. Named-type recursion is represented by `PlanRef`
  nodes: plans are built in two passes keyed by schema object identity (and by `(writer, reader)`
  identity pairs for resolving plans), so `LongList`-style schemas and recursive writer/reader pairs
  terminate.
* The **typed** plan family (`Avro.decode(schema, bytes, T)`, `Avro.Rows(src; T)`) specialises on a
  caller-supplied `T`: `RecordPlan{names, Ps<:Tuple}` unrolled via `ntuple`/`Val` (any width the caller
  chose), leaf plans per Julia type, recursion through the user's own recursive struct types.
* The **column** plan family (`Avro.Table`) specialises per column *element type* (a closed set: the
  §4.6 leaf types, `Union{Missing, leaf}`, and `Vector`/`Dict`/`Avro.Record`-valued cells), never per
  schema: each selected field owns a `ColumnBuilder{E}` holding a `Vector{E}` chunk; a row is decoded by
  iterating builders (tuple-unrolled for ≤ 32 columns, dynamic loop beyond — the threshold affects only
  code shape, not the result types). Unselected fields use `skip`.
* Write plans mirror read plans with value-extraction strategies for NamedTuples, StructUtils structs
  (§4.8), `Tables.AbstractRow`s (by column index), `AbstractDict`s, and iterables.
* Plans are built by `Avro.plan(schema[, T])`; construction is pure and cheap (microseconds for the
  interop schema) and plans are owned by reader/writer objects — no global caches.
* **Compile-cost gate** (Phase 2): decoding 1,000 random heterogeneous schemas through the generic and
  column paths must stay within a fixed budget of new method instances and cold compile time (recorded
  numbers), with zero invalidations of Base/Tables methods measured by `SnoopCompile`-style checks in
  the test suite.

### 4.6 Julia value model (generic decoding, `Avro.juliatype(schema)`)

| Avro | Julia (generic decode) | Notes |
|---|---|---|
| null | `Missing` (`missing`) | `nothing` also encodes as null |
| boolean / int / long / float / double | `Bool` / `Int32` / `Int64` / `Float32` / `Float64` | |
| bytes | `Vector{UInt8}` | always copied (no borrowed views in 2.0) |
| string | `String` | UTF-8 validated |
| fixed(N) | `Vector{UInt8}` of length N | typed API accepts `NTuple{N,UInt8}`; named identity is preserved through `UnionValue` tagging when a union needs it |
| enum | `Avro.EnumValue` (enum schema reference + `Int32` index; `String(x)`, `Symbol(x)`, `Int(x)`; `==` by fullname and index; `show` prints the symbol) | typed API accepts `Base.Enum` subtypes, `Symbol`, `String` (validated); no `Symbol` interning from untrusted schemas |
| array | `Vector{juliatype(items)}` | |
| map | `Dict{String, juliatype(values)}` | |
| union | **bare value** when the branch → Julia-type mapping is injective (e.g. `["null","string"]` → `Union{Missing,String}`, `["int","string"]` → `Union{Int32,String}`); **`Avro.UnionValue(index, value)`** for every value of a union whose mapping is not injective (two records, two enums, two fixed, `bytes`+`fixed`, …) | the branch is always recoverable: exactly (tagged) or by the first-accepting-branch rule which is exact by construction for injective unions; `UnionValue` is accepted by the encoder for any union and preserved through defaults, resolution, JSON, and sorting |
| record | `Avro.Record` (schema reference + `Vector{Any}` values; `Tables.AbstractRow`; property access by name; `==` structural) | stable at every width and for recursive records; `Avro.Table` top-level rows are columns, so this concerns nested records and `Avro.Rows` |
| decimal(bytes/fixed) | `Avro.Decimal{P,S}` (`Int128` unscaled; big-endian two's complement; minimal bytes for `bytes`, sign-extended to size for `fixed`) | precision ≤ 38 required for the `Int128` path; `precision > 38` decodes to `Avro.Decimal{P,S,BigInt}`; encode validates digit count ≤ P; decode validates byte length only (Java does not re-validate digits) |
| big-decimal (optional) | `Avro.BigDecimal` (`BigInt` unscaled, `Int32` scale) | inner bytes = Avro `bytes` (length-prefixed, big-endian two's complement) followed by an Avro `int` scale, inside the outer `bytes` |
| uuid (string / fixed 16) | `UUIDs.UUID` | string form must be RFC-4122 `8-4-4-4-12` hex (`DataError` otherwise); fixed form is the 16 big-endian bytes |
| date | `Dates.Date` | any `Int32` day count is representable |
| time-millis / time-micros | `Dates.Time` | exact (ns-resolution `Time`); decode range-checked to one day; **encode rejects non-aligned values** (sub-ms for millis, sub-µs for micros) unless the caller passes `Avro.truncate`/`Avro.round` wrappers or `Avro.Time{Millisecond|Microsecond}` |
| timestamp-millis / micros / nanos | `Avro.Timestamp{Millisecond|Microsecond|Nanosecond}` — exact `Int64` ticks since the Unix epoch, a global instant | `DateTime(x)` is an explicit, documented, lossy conversion (UTC-naive, floor to ms); `Avro.Timestamp{P}(::DateTime)` is exact; `ZonedDateTime` via the TimeZones extension |
| local-timestamp-millis | `Dates.DateTime` | exact both ways (naive ↔ local) |
| local-timestamp-micros / nanos | `Avro.LocalTimestamp{Microsecond|Nanosecond}` — exact `Int64` ticks | `DateTime(x)` explicit lossy conversion; exact constructor from `DateTime` |
| duration | `Avro.Duration(months::UInt32, days::UInt32, millis::UInt32)` | little-endian unsigned |

`Avro.Table(src; instants=:exact)` keeps the wrappers; `instants=:datetime` converts global and local
micro/nano values to `DateTime` (documented floor-to-ms loss) for convenience. The value model is
identical for `Avro.decode`, `Avro.Rows`, `Avro.Table` cells, defaults, JSON conversion, and sorting.

### 4.7 Schema resolution (`src/resolution.jl`)

`Avro.resolve(writer::Schema, reader::Schema) -> ResolvedSchema` implements every rule of the "Schema
Resolution" section, memoised on `(writer, reader)` identity pairs (recursive pairs terminate):

* Match by kind: arrays/maps recursively; enums, fixed (plus size), records by unqualified name *after*
  applying the reader's type aliases (a type alias makes the writer's name count as the reader's);
  primitives equal or promotable `int→long/float/double`, `long→float/double`, `float→double`,
  `string↔bytes` (`bytes→string` validates UTF-8 at decode time).
* Records: fields matched by name after reader field aliases; **a reader alias consumes the writer
  field** (Java behaviour: a reader that also has a field with the writer's original name gets no value
  for it and must have a default); writer-only fields are skipped; reader-only fields require a default
  (fresh copy per record); reader field order wins.
* Enums: symbol remap by name; writer symbols absent in the reader use the reader `default` or error.
* Unions: for a writer branch, the reader branch is chosen by **Java's algorithm** — first an exact
  match (same kind, and same unqualified name for named types), then the first branch reachable by
  promotion — so `["null","double","long"]` reading a writer `int` selects `double`. Reader-union-only
  and writer-union-only cases follow the same selection. Tags (`UnionValue`) are preserved through
  resolution.
* Logical types: two recognised `decimal`s match only if both precision and scale match
  (`ResolutionError` otherwise); a recognised logical type on one side and a plain underlying type on
  the other resolves as the underlying type and the **reader's interpretation wins** (reader `date`
  over writer `int` yields dates; reader `int` over writer `date` yields ints).
* Failures are `ResolutionError`s carrying both paths. The result is consumed by `Avro.plan` to produce a
  resolving read plan (writer-driven field order with `SkipPlan`/`DefaultPlan`/`PromotePlan`/
  `EnumRemapPlan`/`UnionRemapPlan` nodes).

### 4.8 Typed API and Julia type mapping (`src/types.jl`, StructUtils)

* `Avro.schema(T)` derives a schema from a Julia type: the §4.6 table inverted, plus `Int8/16/UInt8/16 →
  int`, `UInt32/UInt64 → long` (range-checked on write), `Float16 → float`, `AbstractString`/`Symbol`/
  `Char → string`, `NTuple{N,UInt8} → fixed`, `Union{Missing,T} → ["null", T]`, other `Union`s → union in
  Julia's member order (documented), `Base.Enum` subtypes → enum, `DateTime → local-timestamp-millis`
  (a naive `DateTime` is a local timestamp; global instants are `Avro.Timestamp{P}` or `ZonedDateTime`),
  `Date → date`, `Time → time-micros`, `UUID → string uuid`, structs → records via StructUtils
  (`fieldnames`/`fieldtypes`; `@kwarg`/`@defaults` field defaults become Avro defaults when
  JSON-encodable), `Tables.Schema` → record (through the `names`/`types` accessors, which also serve
  `Tables.Schema{nothing,nothing}` — fixes #18). Named-type naming: structs use `nameof(T)` with
  `namespace = string(parentmodule(T))`; `NamedTuple`/`Tables.Schema` records are named `Record` with
  nested anonymous records `Record_1`, `Record_2`, … in depth-first order, the same Julia type reusing
  its first definition; `name=`/`namespace=` keywords override.
* **Typed decoding** goes through a dedicated `Avro.AvroStyle <: StructUtils.StructStyle`. A plain-DTO
  fast route (direct positional construction from a typed plan) is used only when an eligibility check
  passes: `T` is a concrete struct or `NamedTuple`, no custom `StructUtils.make`/`lift`/`choosetype`
  methods apply for `AvroStyle`, no field tags affect construction, every field has a mapped schema, and
  any field defaults are static. Everything else (custom hooks, dynamic defaults, `choosetype`, abstract
  fields, broad unions) takes the semantic route: generic decode then `StructUtils.make(AvroStyle(), T,
  generic)`. Both routes are tested against the pinned StructUtils version, and a test asserts the fast
  route is never taken when a custom hook exists.
* Logical-type contract (each row has tests for schema validation, binary boundaries, JSON/default form,
  resolution, native conversion, and oracle coverage):

| Logical type | Schema validation | Binary / value bounds | JSON & defaults | Oracle (Java 1.12.2 / fastavro / avro-py) |
|---|---|---|---|---|
| decimal | precision > 0; 0 ≤ scale ≤ precision; fixed-size bound | big-endian two's complement; encode checks digits ≤ precision | bytes/fixed string form | Java `fromjson`/`tojson` vectors; fastavro `bytes-decimal`/`fixed-decimal`; avro-py decimal |
| big-decimal | bytes only | inner bytes + int scale | bytes string | Java `BigDecimalConversion` vectors only |
| uuid | string, or fixed size 16 | RFC-4122 text; 16 bytes | string / fixed string | Java both; fastavro string only |
| date | int | any Int32 | integer | all three |
| time-millis / micros | int / long | `0 ≤ v < 86_400_000` / `< 86_400_000_000` | integer | Java; fastavro both; avro-py none |
| timestamp-millis/micros | long | any Int64 | integer | Java; fastavro; avro-py |
| timestamp-nanos | long | any Int64 | integer | Java only (spec text) |
| local-timestamp-millis/micros | long | any Int64 | integer | Java; fastavro |
| local-timestamp-nanos | long | any Int64 | integer | Java only (spec text) |
| duration | fixed size 12 | three LE UInt32 | fixed string | Java (raw bytes) |

### 4.9 Container files (`src/container.jl`, `src/codecs.jl`)

* `Avro.Reader(src; limits, legacy=nothing, mmap=true)`: parses the header (magic; metadata map decoded
  with the spec's `{"type":"map","values":"bytes"}` schema under `max_metadata_bytes`/`entries`; sync
  marker; `avro.schema` parsed with §4.2 under `max_schema_bytes`), selects the codec from `avro.codec`
  (`null`/absent, `deflate`, `snappy`, `bzip2`, `xz`, `zstandard`; unknown → `UnsupportedCodecError`),
  and iterates blocks **strictly**: `count ≤ max_block_count`, `size ≤ max_block_bytes` and ≤ remaining
  bytes, data, sync (mismatch → `DataError` with block index); decompressed size capped at
  `max_block_bytes` (streamed with a cap); exactly `count` records must consume exactly the block
  (`DataError` otherwise); a partial trailing block → `DataError("truncated file")`; an empty file
  (header only) is a valid zero-row file; zero-count blocks are valid. Sources: file path (mmap by
  default, `mmap=false` reads into memory), `Vector{UInt8}`/views, `IO` (streaming: header + one
  block buffer resident), `IOBuffer` (its written bytes only).
* **Legacy mode.** `legacy=:avrojl1` enables the tolerances needed for files written by Avro.jl ≤ 1.1.2
  and nothing else: `avro.codec == "zstd"` read as zstandard; null-codec blocks with trailing bytes
  after `count` records accepted (one `@warn` per source); `decimal` fixed-16 values interpreted
  native-endian (1.x wrote `Int128` bytes little-endian). The deprecated `readtable` shim sets
  `legacy=:avrojl1` automatically. Strict mode is the default for every new API.
* **Ownership and lifetime.** `Avro.Table` copies all data: path sources are opened, mapped, decoded, and
  closed within the call; caller-owned `IO`/byte sources are left untouched and unreferenced. `Rows` and
  `Reader` hold the mapping/IO until `close` (idempotent; iteration after close throws). Truncating a
  memory-mapped file while it is being read is undefined at the OS level (SIGBUS); `mmap=false` is the
  safe option for files that may change and is documented as such.
* **Parallel decoding** (`Avro.Table` only, in-memory/mmap sources, `ntasks > 1`): stage 1 pre-scans block
  headers (no decompression) into a block table with checked prefix sums under `max_rows`; stage 2
  decodes blocks on worker tasks (bounded to `max_inflight_blocks`, `Threads.@spawn` + `errormonitor`,
  joined with `@sync`-style fetch) into **per-block column chunks**; the first error cancels siblings via
  an `@atomic` flag checked between rows and is rethrown as the first cause; stage 3 assembles chunks
  into final columns in block order (one copy). Direct disjoint-range writes into preallocated columns
  are a later, measured optimisation. `IO` sources decode sequentially.
* `Avro.Writer(io_or_path, schema; codec=:null, level=nothing, metadata=Dict{String,Vector{UInt8}}(),
  sync=nothing, block_bytes=64*1024)`: writes the header, buffers encoded rows in an `Encoder`, emits a
  block when `block_bytes` is reached or on `flush`/`close`; `push!(w, row)` / `write(w, rows)`;
  `close` writes the final block. Metadata keys starting with `avro.` other than `schema`/`codec` are
  rejected. Path targets are written atomically (sibling temp file + `rename`; temp removed on failure;
  `atomic=false` opt-out). Append mode is deferred (requires locking, tail-truncation, abort, and crash
  semantics).
* `Avro.write(dst, table; schema=nothing, codec, …)`: schema inferred from `Tables.schema` (or from
  `Tables.dictrowtable` when absent); `Tables.partitions` become block boundaries (each partition ≥ 1
  block); row encoding through write plans with column-major extraction for column-accessible
  partitions. The block payload is exactly the encoded bytes.
* Codecs: `deflate` = raw RFC 1951 via CodecZlib; `snappy` = Snappy.jl block format followed by the
  4-byte big-endian CRC32 of the *uncompressed* data (verified on read; in-house table CRC32 tested
  against `Zlib_jll`'s `crc32`); `zstandard` via CodecZstd; `bzip2`/`xz` through extensions
  `AvroCodecBzip2Ext`/`AvroCodecXzExt` (weak deps) with the error message naming the package to load.
  Codec objects are allocated per task (never shared) and finalised deterministically.

### 4.10 Single-object encoding and schema stores (`src/singleobject.jl`)

`Avro.encodesingle(schema, x) -> Vector{UInt8}` (marker `C3 01`, little-endian CRC-64-AVRO of the PCF,
payload). `Avro.decodesingle(bytes, store; reader_schema=nothing, T=…)` validates the marker, looks up
the writer schema by fingerprint, resolves against `reader_schema` when given, decodes, and requires
exact payload consumption. `SchemaStore` interface: `Avro.lookup(store, fp::UInt64)`; the built-in
`Avro.SchemaCache(; max_entries=10_000, max_bytes=64 << 20)` keeps the PCF string with each entry,
**rejects registration of a non-parsing-equivalent schema under an existing fingerprint**
(`FingerprintCollisionError`) instead of overwriting, and bounds entries and bytes. Fingerprints are
identifiers, not authentication (documented). Unknown fingerprints raise `UnknownSchemaError(fp)`.

### 4.11 JSON encoding (`src/jsonencoding.jl`)

`Avro.tojson(schema, x; pretty=false)` and `Avro.fromjson(schema, json, T=…; strict=true)` implement
the JSON encoding with this rule table (positive and negative tests for each row):

| Avro | JSON output (Java-compatible) | Accepted input |
|---|---|---|
| null | `null` | `null` |
| boolean | `true`/`false` | booleans only |
| int / long | integer | integers within `Int32`/`Int64` range (no floats, no strings) |
| float / double | number; non-finite as the strings `"NaN"`, `"Infinity"`, `"-Infinity"` (as Java emits) | numbers, those strings, and the bare tokens `NaN`/`Infinity`/`-Infinity` |
| bytes / fixed | string with code points U+0000–U+00FF per byte | strings whose code points are all ≤ U+00FF; fixed length must match |
| string | string | string |
| enum | symbol string | member symbol only |
| array | array | array |
| map | object | object (keys are map keys) |
| record | object in field order | object: every field present exactly once, no unknown fields |
| union | `null`, or a one-member object keyed by the branch's **fullname** for named types and the type name otherwise | exactly one member whose key names a branch; `null` |

The same rules validate and convert defaults. `tojson` uses the §4.6 branch recovery for unions.

### 4.12 Sort order (`src/compare.jl`, Phase 3)

`Avro.compare(schema, a, b)` on Julia values and `Avro.compare(schema, bytes_a, bytes_b)` on encoded
datums (without materialising) implement the spec order: null equal; booleans; numerics ascending with
Java's `Double.compare` policy for `NaN` (greater than everything, equal to itself) and `-0.0 < 0.0`
(recorded policy, verified against the `Compare.java` harness); bytes/fixed unsigned lexicographic;
strings by code point (byte order of UTF-8); arrays lexicographic; enums by index; unions by branch then
value; records by fields honouring `ascending`/`descending`/`ignore`; maps are an error unless inside
an `ignore` field; depth-limited by `max_depth`.

### 4.13 Errors

```
abstract type AvroError <: Exception end
struct SchemaError <: AvroError            # message, JSON path
struct EncodeError <: AvroError            # message, value path, expected schema
struct ResolutionError <: AvroError        # message, writer path, reader path
struct UnknownSchemaError <: AvroError     # fingerprint
struct FingerprintCollisionError <: AvroError
abstract type DecodeError <: AvroError end
struct DataError <: DecodeError            # message, byte position, optional value path (malformed input)
struct LimitError <: DecodeError           # limit name, observed value, limit value, keyword to raise it
struct CodecError <: DecodeError           # codec name, cause
struct UnsupportedCodecError <: DecodeError # codec name, extension package to load (if any)
```

Every error type has a `showerror` with actionable text. Nothing else escapes the decoders.

### 4.14 Concurrency contract

* `Schema`, plans, `Limits`, and `SchemaCache` (lock-protected) are safe to share across tasks.
* `Decoder`/`Encoder`/`Reader`/`Writer`/codec instances are single-owner; concurrent use is a bug.
* `Avro.Table` columns are plain vectors (concurrent reads safe). `Avro.Rows` is a single-consumer iterator.
* Parallel decode memory is bounded by `max_inflight_blocks × max_block_bytes` plus the output.
* Writer-side parallel compression is deferred to 2.x.

### 4.15 Module layout

```
src/Avro.jl            module, includes, public API docstrings, version-gated `public` declaration
src/errors.jl          error types
src/limits.jl          Limits, Budget
src/frozen.jl          FrozenVector, frozen props map
src/names.jl           FullName, name validation, namespace resolution
src/jsonscan.jl        lexical pre-scan (size/depth) and lazy JSON traversal helpers
src/schema.jl          Schema types, Field/Default, parser, validator, printer, equality
src/logical.jl         LogicalType structs, validation, value types (Decimal, BigDecimal, Timestamp, LocalTimestamp, Duration)
src/canonical.jl       Parsing Canonical Form, CRC-64-AVRO, fingerprints
src/values.jl          Record, EnumValue, UnionValue
src/decoder.jl         Decoder + primitive reads/skips
src/encoder.jl         Encoder + primitive writes
src/plan_read.jl       read plans (generic dynamic, typed, resolving, PlanRef)
src/plan_write.jl      write plans (NamedTuple/struct/row/dict/iterable extraction)
src/columns.jl         column builders and column plans
src/types.jl           Julia type <-> schema mapping, AvroStyle, StructUtils integration
src/resolution.jl      resolve(writer, reader)
src/jsonencoding.jl    tojson/fromjson
src/compare.jl         sort order
src/codecs.jl          codec registry, CRC32, deflate/snappy/zstandard
src/container.jl       Reader/Writer, header/block parsing, parallel decode, Avro.write, atomic paths
src/tables.jl          Avro.Table, Avro.Rows, Tables.jl interface, Scan pushdown
src/singleobject.jl    single-object encoding, SchemaStore, SchemaCache
src/deprecated.jl      1.x shims (legacy mode)
src/precompile.jl      PrecompileTools workload
ext/AvroCodecBzip2Ext.jl, ext/AvroCodecXzExt.jl, ext/AvroTimeZonesExt.jl
```

---

## 5. Public API (all qualified; nothing exported)

### 5.1 Schemas

```julia
Avro.parseschema(src::Union{AbstractString, AbstractVector{UInt8}, IO}; allow_invalid_names=false, limits=Limits()) -> Schema
Avro.schema(T::Type; name=nothing, namespace=nothing) -> Schema      # Julia type → schema
Avro.schema(::Tables.Schema; name="Record", namespace="")            # table schema → record
Avro.schema(x::Avro.Table / Avro.Rows / Reader)                      # the file's writer schema
Avro.json(schema; pretty=false) / JSON.json(schema)                  # spec JSON text
Avro.canonical(schema) -> String                                      # Parsing Canonical Form
Avro.fingerprint(schema; algorithm=:crc64avro) -> UInt64 | Vector{UInt8} (md5/sha256)
Avro.parsingequivalent(a, b) -> Bool
Avro.resolve(writer, reader) -> ResolvedSchema
Avro.juliatype(schema) -> Type
Avro.fullname(named_schema) -> String
Base.:(==), Base.hash, Base.show on schemas
```

### 5.2 Datums

```julia
Avro.encode(schema, x) -> Vector{UInt8};  Avro.encode!(enc_or_io, schema, x);  Avro.encode(x)  # schema = Avro.schema(typeof(x))
Avro.decode(schema, src, T=juliatype(schema); writer_schema=nothing, limits=Limits(), allow_trailing=false, max_bytes=…)
Avro.encodesingle(schema, x); Avro.decodesingle(src, store; reader_schema=nothing, T=…)
Avro.tojson(schema, x; pretty=false) -> String;  Avro.fromjson(schema, json, T=…; strict=true)
Avro.compare(schema, a, b) -> Int                      # values or encoded bytes
Avro.UnionValue(i, x), Avro.EnumValue, Avro.Record, Avro.Decimal{P,S}, Avro.BigDecimal, Avro.Duration,
Avro.Timestamp{P}, Avro.LocalTimestamp{P}, Avro.Time{P}, Avro.truncate(x, P), Avro.round(x, P)
```

### 5.3 Container files and tables

```julia
Avro.Table(src; scan=nothing, reader_schema=nothing, ntasks=Threads.nthreads(), limits=Limits(), legacy=nothing, mmap=true, instants=:exact)
    # Tables.jl columns table; `Avro.metadata(t)`, `Avro.schema(t)`, `Avro.codec(t)`, `Avro.sync(t)`, `length`, `Tables.partitions` (per block)
Avro.Rows(src; T=nothing, reader_schema=nothing, limits, legacy, mmap, instants)
    # streaming row iterator (one decompressed block resident); Tables.rows/schema; rows are Avro.Record or T; `close`
Avro.Reader(src; limits, legacy, mmap)       # block-level: header, `eachblock(r)` → (count, bytes), `close`
Avro.Writer(io_or_path, schema; codec=:null, level=nothing, metadata=Dict(), sync=nothing, block_bytes=64*1024, atomic=true)
    push!(w, row); Base.write(w, rows); flush(w); close(w)
Avro.write(dst, table; schema=nothing, codec=:null, level, metadata, block_bytes, name, namespace, atomic=true) -> dst
Avro.tobuffer(table; kw...) -> IOBuffer
Avro.codecs() -> available codec names
Avro.inspect(src) -> diagnostic report (codec, block count/sizes, padding/legacy issues, schema)
```

### 5.4 Deprecated 1.x shims (`src/deprecated.jl`, removed in 3.0)

`Avro.readtable(src; kw...) → Avro.Table(src; legacy=:avrojl1)`; `Avro.writetable(dst, tbl;
compress=:zstd → codec=:zstandard)`; `Avro.read(src, T_or_schema) → Avro.decode`; `Avro.write(x; schema) →
Avro.encode` (only the one-positional-argument datum form; `Avro.write(dst, table)` is the new container
writer, and `Avro.write(io, x)` where `x` is not a table raises a clear error pointing at `encode!`);
`Avro.parseschema` and `Avro.tobuffer` keep their names. All shims call `Base.depwarn`.

---

## 6. Tables.jl integration and `Tables.Scan` pushdown

* `Avro.Table` is a column table: `Tables.columnaccess`, `Tables.columns`, `Tables.schema` (names and
  Julia eltypes from §4.6), `Tables.partitions` (one `Avro.Table` per block, materialised lazily),
  `DataAPI.metadata` for file metadata (`avro.schema`, `avro.codec`, user keys; DataAPI is a direct
  dependency). Columns are plain `Vector`s. Zero-column and zero-row tables keep an authoritative row
  count (`length`).
* `Avro.Rows` is a row table (`Tables.rowaccess`, `Tables.rows`, `Tables.schema`, `Base.IteratorSize`
  `SizeUnknown` for `IO`, `HasLength` when the block pre-scan is possible); `Tables.partitions(rows)`
  yields per-block `Avro.Table`s so `Avro.write(dst, Avro.Rows(src))` streams with bounded memory.
* `Avro.write` accepts any Tables.jl source (row or column access) and honours `Tables.partitions`.
* `Tables.Scan` pushdown (`Avro.Table(src; scan=Tables.Scan(...))`) targets **Tables.jl `jq/scan` @
  `df4e68c15c874079521d9d4ce4be67dce4345a31`** (the commit the public branch points at; pinned in
  `[sources]` by SHA, to be replaced by a registered release before RC):
  * **select / Not / All / Regex / rename**: bound with `Tables.bind`; unselected fields are skipped
    (`skip` plans; sized array/map blocks skipped by byte size); selection order = output order;
    `select=()` yields a zero-column table with the correct row count.
  * **filter**: per block, pass 1 decodes only the filter columns and records each row's byte offset;
    `Tables.filtermask` on those columns (exact `Tables.scan` semantics incl. `missing`, `validate=false`,
    missing-only columns); pass 2 decodes the selected columns for qualifying rows only. `OpNode`s and
    column-to-column comparisons are rejected by `Tables.bind`/`Scan` before any decoding, with the same
    errors `Tables.scan` raises.
  * **offset / limit**: whole blocks are skipped by header `count` before decoding when no filter is
    present; decoding stops at `limit` (the parallel path bounds the block range first); offset beyond
    the row count yields an empty table.
  * **type overrides** (`ref => Type`): decoded directly only for exact widenings (`int→Int64`,
    `float→Float64`, `int/long→Float64`); everything else is applied by `Tables.scan`'s own conversion
    rules (including its no-op subtype rule) via the residual.
  * Residual: `Tables.scan(table, Tables.Scan(scan; select=nothing, filter=residual, limit=nothing,
    offset=0))`; `Tables.describe(scan, residual)` works.
  * Gate: names, order, eltypes, `Tables.schema`, row count, and values identical to
    `Tables.scan(Avro.Table(src), scan)` for a generated matrix of scans (selection × filter × limit/
    offset × validate × overrides incl. supertypes), including multi-block files, empty records,
    `select=()`, and missing-only filters.
* Registration of 2.0.0 is gated on a Tables.jl release containing `Scan` (or the `scan` keyword moves
  to 2.1 and the `[sources]` entry is removed) — decided at RC time (§12).

---

## 7. Compatibility and migration policy

* Version: **2.0.0** (SemVer-breaking). Julia ≥ 1.10 (LTS). Registered reverse dependencies: none.
* Breaking changes, each with a migration line in `CHANGELOG.md` and `docs/src/migration.md`:
  StructTypes → StructUtils customisation; `Avro.Record{names,T,N}`/`Avro.Enum{names}`/`Avro.Array`
  removed; enums decode as `Avro.EnumValue`; nested records as `Avro.Record`; `Avro.Duration` fields are
  `UInt32`; `Avro.Decimal` byte order and parameters; global timestamps decode as `Avro.Timestamp{P}`;
  `compress=:zstd` → `codec=:zstandard` (shim maps it); `Avro.write(io, x)` with a non-table `x` is no
  longer the datum writer; `readtable` returns columns, not lazy records; strict container validation.
* **Known 1.x data defects** (files written by Avro.jl ≤ 1.1.2), all handled by `legacy=:avrojl1` and
  reported by `Avro.inspect`: (1) null-codec blocks padded with uninitialised bytes after the last
  record; (2) codec name `zstd`; (3) `decimal` values written native-endian (little-endian on all
  supported platforms) as fixed-16 regardless of the schema's size; (4) `Duration` fields signed
  (identical bytes for values < 2^31); (5) record names `Record_<hash>`. Rewrite recipe:
  `Avro.write(dst, Avro.Rows(src; legacy=:avrojl1); codec=...)`. The shims are tested against real 1.x
  files (generated with the pinned 1.1.2 in a separate environment and committed as fixtures).
* Deprecation shims for one major cycle; removal in 3.0.
* Behavioural guarantees stated in docs: decode never crashes on malformed input (bounded by `Limits`
  and budgets), written files are readable by Apache Java 1.12 and fastavro (CI-enforced), thread-safety
  contract (§4.14), ownership contract (§4.9).

---

## 8. Interoperability and conformance strategy

### 8.1 Vendored Apache fixtures (`test/fixtures/apache/`, Apache-2.0 with LICENSE/NOTICE, commit pinned)

`schema-tests.txt` (PCF + CRC-64 fingerprints — parsed and executed), `interop.avsc`,
`weather{,-deflate,-snappy,-zstd,-sorted}.avro`, `weather.avsc`, `weather.json`, `syncInMeta.avro`,
`schemas/simple`, `schemas/withUnion`, `messageV1` (single-object bytes + schema), `reserved.avsc`,
`TestRecordWithLogicalTypes.avsc`. `test.avro12` is an obsolete pre-1.3 format (Java: "Not an Avro data
file") and is excluded. The `interop/rpc` files are excluded (not wire captures).

### 8.2 Generated corpus (`test/fixtures/generated/`, ~1.5 MB, committed; generator `test/fixtures/generate.sh` checked in)

Already produced in the authoring session and to be regenerated by the script: schemas `bench`,
`everything` (all kinds, recursive `LongList`, nested `Inner`, 13-branch union, every default kind incl.
bytes/fixed `\u00XX` strings), `wide` (100 fields), `empty` (zero-field record), `interop`, `logical`
(all 16 logical-type fields); data from Java `random --seed 7` and hand-built JSON via `fromjson`
(boundary logical values), recodec'd by Java to `deflate`, `snappy`, `bzip2`, `xz` (`--level 6`),
`zstandard`, and written by fastavro (`deflate`, `xz`); Java `tojson` expectations; schema-evolution
pairs with Java `ReadWithReader` expectations (`everything_readerA`: promotions, reorder, defaults,
enum-default fallback, union reorder; `everything_readerB`: type/field aliases and nested renames;
`weather_reader`, `weather_reader_fail`); single-object bytes from Java `BinaryMessageEncoder`; blocking-
encoder datums (negative-count sized blocks at every level); Java `canonical` forms and CRC-64/MD5/
SHA-256 fingerprints for every schema; Java big-decimal vectors; Java sort-order verdicts.

Additional matrix rows (Phase 4): empty file (header only), zero-row blocks, many small blocks (1-row
blocks), user metadata, unknown codec, named-branch collisions in unions, negative collection blocks,
codec bombs (zeros compressed with every codec), corrupt snappy CRC, boundary logical values, 1.x
legacy files.

### 8.3 Java harness (`test/interop/java/`, compiled in CI against the pinned jar)

`ReadWithReader` (reader-schema resolution → JSON), `SingleObject` (encode/decode), `BlockingEncode`
(sized blocks), `Compare` (sort order), `LogicalCaps` (capability probe), `BigDec` (big-decimal vectors),
plus `TimeConversions`-based vectors for every time/timestamp logical type.

### 8.4 Oracle capability matrix (versioned; updated when tools move)

| Feature | Java 1.12.2 | fastavro 1.12.2 | avro-py 1.12.2 | Spec 1.13-SNAPSHOT |
|---|---|---|---|---|
| codecs null/deflate/snappy/bzip2/xz/zstandard | all | all (snappy via cramjam, xz via lzma) | null/deflate/snappy/bzip2/xz/zstandard | all |
| decimal bytes/fixed | yes | yes | yes | yes |
| uuid string / fixed | yes / yes | yes / no | yes / no | yes / yes |
| date, time-millis/micros | yes | yes | date only | yes |
| timestamp-millis/micros, local-* | yes | yes | timestamp only | yes |
| timestamp-nanos, local-timestamp-nanos | recognised | no | no | yes (spec vectors + Java raw) |
| big-decimal | conversion class | no | no | yes |
| duration | recognised (raw) | no | no | yes |

### 8.5 Live differential tests (`test/interop/`, run when `java`/`AVRO_TOOLS_JAR` and the pinned Python venv are available; a CI job on ubuntu installs Temurin 21, the checksummed jar, `avro==1.12.2`, `fastavro==1.12.2`, `cramjam`)

For the schema matrix: (1) Julia-written container files for every codec read by Java `tojson` and
fastavro, compared **semantically** to Julia's `tojson` (decoded datum sequences, schema, metadata,
row counts, codec name — never OCF bytes, since sync markers and framing are regenerated);
(2) Java `random` data decoded by Julia and re-encoded as raw datums compared byte-exactly through
`jsontofrag`/`fragtojson`; (3) `canonical` and `fingerprint` outputs compared for every schema;
(4) schema-resolution pairs read by Julia, Java (`ReadWithReader`) and fastavro (`reader_schema`);
(5) single-object bytes cross-decoded; (6) sort-order verdicts; (7) negative oracles: a corpus of
malformed schemas, datums, blocks, and JSON with each oracle's accept/reject verdict recorded, which
Julia must match (spec-justified deviations recorded in the fixture).

---

## 9. Testing strategy (deterministic; per-case `Xoshiro(seed)`; failing inputs and seeds persisted)

1. **Unit**: every primitive read/write/skip incl. boundary values (`typemin/typemax`, −0.0, NaN payload
   bit patterns, 10-byte varints, `typemin` counts), every schema constraint in §3 (positive and
   negative), every §4.6 mapping, every §4.8 logical-type row, every §4.11 JSON row.
2. **Spec examples**: the encoding tables and examples from the specification text are literal tests
   (`36 06 66 6f 6f`, `04 06 36 00`, `02 02 61`, the `Example` fullname schema, the LongList schema, the
   Helsinki timestamp example, the `Suit` enum default).
3. **Conformance**: §8.1/§8.2 fixtures; `schema-tests.txt` harness; messageV1; blocking-encoder datums.
4. **Round-trip properties with independent assertions**: a bounded random schema generator (all kinds,
   recursion, logical types, unions, aliases/defaults, props) with a matching random value generator;
   `decode(encode(x))` compared with `isequal` **and** bit-exact float comparison; `parseschema(json(s))`
   compared with structural `==` and per-attribute assertions (defaults, aliases, props, logical types,
   docs, order); `canonical(parseschema(canonical(s))) == canonical(s)`; resolved decode with
   `reader == writer` equals plain decode; columnar `Avro.Table` equals `Tables.columntable(Avro.Rows)`.
   Encoder/decoder symmetric bugs are caught by the Java/Python oracles of §8.5 on the same generators.
5. **Schema evolution**: a table-driven matrix of (writer, reader) pairs for every resolution rule and
   failure rule, including recursive pairs, ambiguous unions, alias collisions, mutable-default isolation
   (two decoded records never share a default object), invalid UTF-8 under `bytes→string`, decimal
   mismatch, logical-vs-underlying cases; Java and fastavro expectations for the positive cases.
6. **Differential**: §8.5.
7. **Mutation / fuzz**: for every fixture and generated datum, 10k seeded mutations (bit flips, byte
   insert/delete, truncation at every offset for small inputs, varint lengthening, count/size
   substitution with extreme values) must yield a `DecodeError` subtype or a valid decode — never another
   exception type; each case runs under `Limits`; the harness runs cases in batches inside a subprocess
   with wall-clock, CPU, and RSS limits (`ulimit`/`setrlimit` where available, watchdog kill otherwise)
   so a hang or native-codec runaway fails the batch instead of the session; failing inputs are shrunk
   (byte-level delta debugging) and persisted. A longer loop runs under `AVRO_FUZZ_ITERATIONS`.
8. **Truncation / corruption of containers**: header truncation at every byte, sync mismatch, block size
   larger than the file, wrong snappy CRC, decompression bombs for every codec against
   `max_block_bytes`/`max_total_bytes`, trailing garbage, 1.x-style cushion blocks (strict vs legacy).
9. **Resource limits and budgets**: each `Limits` field has a test that trips it quickly (< 10 ms,
   < 1 MB allocated) and one that passes just under it; `max_depth` verified inside `Threads.@spawn`;
   cumulative budgets verified across blocks; parallel in-flight bound verified by counting live buffers.
10. **Concurrency**: multi-block files decoded with `ntasks ∈ {1,2,8}` produce identical tables; a block
    that fails mid-file cancels siblings and surfaces the first cause; GC-stress runs (`GC.gc()` between
    rows under `JULIA_GC_STRESS`-style loops) on the parallel path; writer flush/close ordering.
11. **Allocation budgets**: `@allocated` (bytes, after warm-up) and allocation counts via
    `Base.gc_num` deltas for primitive decode (0), typed record decode of isbits fields (0), column
    decode per row of isbits fields (0), string row (1 allocation per string).
12. **Compile-cost gate**: 1,000 heterogeneous random schemas through generic, column, and typed paths;
    recorded method-instance counts, cold compile time, and zero invalidations.
13. **Quality gates**: Aqua (ambiguities, unbound type parameters, undefined exports, piracy, stale deps,
    compat bounds, `project_extras`), JET (`report_package` zero errors; `@test_opt` on hot paths),
    docstring coverage for every public name, Documenter doctests, a Julia 1.10 load/parse test.
14. **1.x regression port**: the existing `runtests.jl` cases re-expressed through the shims and the new
    API, plus real 1.1.2-written fixtures decoded through `legacy=:avrojl1`.

---

## 10. Benchmarks and performance targets

### 10.1 Baselines (Apple M-series, Julia 1.12.6, 1 thread, 1M rows `{id:long, x:double, name:string, flag:boolean}`)

Measured in the authoring session and re-run by the reviewer; the Phase 0 harness re-measures all of
them with raw logs, exact commits, CPU/threads/codec level/bytes/peak RSS recorded.

| Implementation | Write (null) | Write (zstd) | Read |
|---|---|---|---|
| Avro.jl 1.1.2 (file via `writetable`: 23,100,363 bytes, of which ~2.2 MB is cushion padding; `IOBuffer` capacity 32.3 MB is not the file size) | 1.02–1.21 s | 1.24 s | index 2.9–3.9 s + materialise 3.9 s (lazy records → columns) |
| fastavro 1.12.2 (Cython, dict rows) | 0.65 s | 0.78 s | 0.50 s |
| Java avro-tools 1.12.2 | — | — | `count` 0.67 s, `tojson` 0.91 s (JVM start-up included; an in-JVM `GenericDatumReader` harness is added in Phase 0 to time decoding alone) |

The workloads differ (Julia columns vs Python dicts vs Java generic records); the harness reports each
as "read to the implementation's natural in-memory rows/columns" and states the caveat. 1.x and 2.x share
a package UUID, so 1.1.2 is benchmarked in a separate pinned environment/process driven by the same
harness script.

### 10.2 Targets

Ratios are gates (measured on the same host in the same session); absolute numbers are tracked targets
recorded per named host in `docs/src/benchmarks.md`, not CI gates.

| Metric | Gate (ratio) | Tracked absolute (authoring host) |
|---|---|---|
| `Avro.write` 1M rows null codec, 1 thread | ≥ 4× faster than 1.1.2 | ≤ 0.25 s (≥ 80 MB/s) |
| `Avro.Table` 1M rows null codec, 1 thread | ≥ 10× faster than 1.1.2 read+materialise; ≥ fastavro | ≤ 0.30 s |
| `Avro.Table` 1M rows, 8 threads | ≥ 3× single-thread | — |
| zstandard/deflate/snappy read overhead | within 1.3× of raw `transcode` of the same bytes | — |
| Typed single record decode (3 isbits + 1 string field) | ≤ 1 allocation (the string) | ≤ 150 ns |
| Typed single record encode with `encode!` | 0 allocations | — |
| `parseschema(interop.avsc)` | — | ≤ 100 µs, ≤ 300 allocations |
| Projection `select=(:id,)` on the 4-column file | ≥ 2× faster than full decode | — |
| Package load time (`@time using Avro`) | — | ≤ 0.5 s |
| Time-to-first-table on a fresh session | — | ≤ 1.5 s |

---

## 11. Engineering deliverables

* **Package metadata**: `Project.toml` version `2.0.0-DEV`; deps `JSON` (1.7), `StructUtils` (2.8),
  `Tables` (pinned SHA via `[sources]` until released), `DataAPI` (1), `CodecZlib` (0.7), `CodecZstd`
  (0.8), `Snappy` (0.4), `TranscodingStreams` (0.11), `MD5` (0.2), stdlibs `Dates`, `UUIDs`, `Mmap`,
  `SHA`, `Random`; `PrecompileTools` (1); weak deps `CodecBzip2` (0.8), `CodecXz` (0.7), `TimeZones`
  (1); `[compat]` bounds for everything incl. `julia = "1.10"`; `test/Project.toml` with `Aqua`, `JET`,
  `Test`, `Tables`, `TimeZones`. `SentinelArrays`, `JSON3`, `StructTypes` dropped. `public` names are
  declared through a version-gated `Core.eval(Expr(:public, ...))` so Julia 1.10 still parses the module.
* **Licensing**: MIT (unchanged) for the package; `test/fixtures/apache/LICENSE` (Apache-2.0) + `NOTICE`
  attribution in `README.md`; the specification text is referenced, not vendored.
* **Docs** (Documenter): `index.md` (quick start), `manual/schemas.md`, `manual/encoding.md`,
  `manual/container.md`, `manual/tables.md`, `manual/evolution.md`, `manual/logicaltypes.md`,
  `manual/singleobject.md`, `manual/sortorder.md`, `manual/limits-and-security.md`,
  `manual/performance.md`, `migration.md`, `benchmarks.md`, `reference.md` (autodocs). `examples/`:
  CSV→Avro→Arrow pipeline, Kafka-style single-object producer/consumer with a `SchemaCache`, schema
  evolution walkthrough, struct mapping with StructUtils.
* **CHANGELOG.md** (Keep-a-Changelog style) with the 2.0.0 entry.
* **CI** (`.github/workflows/ci.yml`): required jobs — Julia `1.10`, `1.11`, `1` on ubuntu/macos/windows
  with `JULIA_NUM_THREADS ∈ {1, 4}`; docs build; Aqua+JET; interop (ubuntu: Temurin 21 + checksummed jar
  + pinned Python packages). Non-blocking jobs — `pre` and `nightly` on ubuntu only; `juliac --trim`
  smoke compile of a reader program. `TagBot`, `CompatHelper`. Trigger on `push: branches: ['**']` plus
  `pull_request` with a same-repo duplicate guard.
* **Precompile**: PrecompileTools workload covering schema parse/print/canonical/fingerprint, encode/
  decode of a representative NamedTuple and struct, container round trip with null/deflate/zstandard/
  snappy, `Avro.Table` with a `Scan`, `Avro.Rows`, single-object, JSON encoding; budget ≤ 15 s
  precompile, ≤ 0.5 s load.
* **Trim**: no `eval`/`@generated` on runtime data, no `Symbol`-to-function lookups; `test/trim/` smoke.
* **Code style**: AGENTS.md rules (explicit `return`, guard clauses, `T[]`, `@atomic`, `errormonitor`,
  small functions, whitespace discipline); no formatter enforcement.

---

## 12. Phased milestones and gates

Each phase lands as small local commits with tests; `Pkg.test()` of the new suite passes on Julia 1.10
and 1.12 at the end of every phase.

| Phase | Scope | Acceptance gate |
|---|---|---|
| **0 — Foundation** | branch; legacy code removed; `Project.toml` 2.0.0-DEV + pinned deps (Tables SHA, jar checksum, Python pins); `test/Project.toml`; CI skeleton; vendored + generated fixtures with licence and generator script; Java harness; `errors.jl`, `limits.jl`, `frozen.jl`; benchmark harness with raw-logged 1.1.2/fastavro/Java baselines; Julia 1.10 load test; `public` gating | `Pkg.test` skeleton green on 1.10 and 1.12; fixtures licensed and regenerable; baseline logs committed |
| **1 — Schema model** | JSON pre-scan + lazy traversal, names, parser/validator (all §3 rules), defaults (with the minimal JSON datum rules needed to validate/convert them), freezing, structural `==`/`hash`, printer, canonical form, fingerprints, logical-type validation, Julia-type mapping | `schema-tests.txt` 100%; every §3 constraint tested positive/negative; duplicate-key, depth, recursion, equality-on-cycles and immutability gates; `canonical`/`fingerprint` equal to Java for all 19 fixture schemas; Aqua+JET clean |
| **2 — Binary core** | Decoder/Encoder with checked arithmetic and validation, budgets, generic dynamic plans with `PlanRef`, stable generic values (`Record`, `EnumValue`, `UnionValue`, exact timestamps), typed plans with `AvroStyle` eligibility, column builders, logical values, full JSON datum encoding, single-object + `SchemaCache` | spec-example tests; round-trip properties with independent assertions; 1.x round-trip cases ported; Java `fragtojson`/`jsontofrag` differential on generated datums; blocking-encoder fixtures; fuzz 100k mutations clean in sandboxed batches; limit/budget tests; allocation budgets; messageV1; many-schema compile gate |
| **3 — Resolution and order** | `resolve`, resolving plans (memoised pairs), aliases, defaults, enum defaults, Java union-branch selection, decimal rule, `Avro.compare` (values and bytes) | resolution matrix (positive/negative, recursive, ambiguous, UTF-8, mutable-default isolation) with Java and fastavro expectations; sort-order vectors from `Compare.java` incl. NaN/−0.0/ignore/maps |
| **4a — Strict containers and codecs** | Reader/Writer/`Avro.write` with strict validation, atomic path writes, codecs (+ extensions), legacy mode, `Avro.inspect` | Apache corpus reads; Java **and** fastavro read Julia files — checked after each codec lands; 1.x fixtures read under legacy mode and rejected under strict; truncation/corruption/bomb/empty-file/zero-row/metadata tests |
| **4b — Tables basics and ownership** | `Avro.Table` (sequential), `Avro.Rows`, partitions, metadata, zero-column/zero-row behaviour, close semantics | `Table == columntable(Rows)` property; ownership/close tests; Tables interface tests |
| **4c — Parallel decode** | block pre-scan, bounded worker pool, per-block chunks, cancellation, assembly | identical results for `ntasks ∈ {1,2,8}`; failure propagation; GC-stress; in-flight bound test |
| **4d — Scan and performance** | `Tables.Scan` pushdown, projection skipping, performance work | Scan equivalence matrix; §10.2 ratio gates |
| **5 — Release engineering** | shims, docs (no protocol pages), examples, changelog, precompile workload, trim smoke, benchmarks doc, README, CI matrix live | docs build without warnings; load-time budget; full local matrix green on 1.10/1.11/1.12 (+1.13-rc if installed); Aqua/JET; interop job green locally |

Readiness levels (this task stops at **PR-ready**; nothing is pushed):

* **Review-ready**: clean exact local head; scoped diff; local supported-version tests, docs, licensing,
  and interop artifacts complete.
* **PR-ready**: review-ready plus a reproducible branch and exact dependency pins (`[sources]` SHA,
  jar checksum, Python pins) and the status record (§15) up to date.
* **Merge-ready**: hosted exact-head CI green and review issues resolved.
* **RC-ready**: no temporary `[sources]`; lower compat bounds resolve; registered dependencies exist
  (Tables with `Scan`, or the `scan` keyword moved to 2.1); reverse dependencies/PkgEval checked; source
  archive validated.
* **Release-ready**: exact version/tag state approved; registration and docs deployment are release
  actions, not evidence of readiness.

---

## 13. Correctness, security, performance, allocation, streaming, concurrency goals (summary)

* Correctness: every spec rule in §3 has a test; all Apache fixtures pass; Java and fastavro read every
  file we write; we read every file they write (all six codecs); oracle-verified resolution and sort order.
* Security: checked arithmetic, no `@inbounds` without a proven bound, no allocation sized by untrusted
  counts, cumulative budgets, bounded recursion/decompression/schema/metadata, strict validation, all
  failures are `DecodeError` subtypes; fuzz-clean under sandboxed batches.
* Performance: §10.2; plans compiled per (schema, T) only on request; zero-allocation primitives;
  column-major materialisation; projection skips; block-parallel decode with bounded memory.
* Streaming: `Rows`, `Writer`, `IO` sources with one block resident; partitions for pipelines.
* Concurrency: §4.14 contract; no global mutable state; structured task lifecycle with cancellation.

---

## 14. Decisions, open risks, and review log

Decisions (each with rationale; reviewers may challenge any):

1. `missing` is the Julia value of `null`; `nothing` encodes as null.
2. Generic enums are `Avro.EnumValue` (identity-preserving, no interning); typed targets may choose
   `Base.Enum`/`Symbol`/`String`.
3. Generic records are always `Avro.Record`; typed `T` is opt-in specialisation.
4. Naive `DateTime` derives `local-timestamp-millis`; global instants are `Avro.Timestamp{P}` (or
   `ZonedDateTime` via the extension); micro/nano values are exact wrappers; `DateTime` conversion is
   explicit and lossy (`instants=:datetime` opt-in on `Avro.Table`).
5. Decimal values are `Int128`-backed up to precision 38 with a `BigInt` slow path beyond.
6. Codec dependency split: `deflate`/`snappy`/`zstandard` hard, `bzip2`/`xz` extensions.
7. Strict container validation is the default; Avro.jl 1.x tolerances live behind `legacy=:avrojl1`
   (auto-enabled by the deprecated `readtable` shim).
8. `Avro.write(dst, table)` is the container writer; datum writing is `encode`/`encode!`.
9. `Tables.Scan` support is developed against the pinned `jq/scan` SHA; RC requires a registered release.
10. Default block size 64 KiB; positive-count array/map blocks written; both forms read.
11. RPC, append, writer-side parallel compression, and borrowed byte views are deferred to 2.x with their
    requirements recorded (§3, §4.9).
12. Limits and cumulative budgets default conservatively (§4.4) and every corpus file decodes under them.
13. Union-branch selection during resolution follows Java (exact match first, then promotion).
14. Unions decode bare only when the branch mapping is injective; otherwise `Avro.UnionValue`.
15. Booleans other than 0/1 and invalid UTF-8 strings are rejected (spec over Java leniency).

Intentionally unresolved risks:

* Column-builder specialisation per element type is bounded by a closed type set but nested shapes
  (`Vector{Vector{…}}`) are open-ended; the Phase 2 compile-cost gate decides whether deeper nesting
  falls back to `Vector{Any}`-backed cells.
* The `Avro.Record` generic row boxes isbits fields; the typed path and `Avro.Table` are the fast paths.
  If `Rows` throughput for generic rows proves inadequate, an unboxed layout is a 2.x addition.
* Tables.jl `Scan` release timing is outside this repository.
* Worker-task stack depth vs `max_depth=1024`: 500k frames were measured on macOS; Linux/Windows are
  measured in Phase 2 and the default lowered if needed.

Review log:

* **Round 1** (`reviews/codex-review-1.md`, 29 findings: 8 blockers, 20 majors, 1 minor; `VERDICT:
  REVISE`). All 29 adopted, 5 with amendments; dispositions in `reviews/response-1.md`. Major changes:
  cumulative budgets and conservative limits (§4.4); lexical pre-scan + lazy JSON traversal with
  duplicate-key errors and default text spans (§4.2); `PlanRef` recursion and dynamic generic plans
  (§4.5); union default = first matching branch with retained branch and fresh copies (§4.2);
  `UnionValue` tagging for non-injective unions (§4.6); exact `Timestamp`/`LocalTimestamp`/`Time`
  wrappers (§4.6); RPC deferred with wire-test requirements (§3); error hierarchy and `public` gating
  (§4.13, §11); structural `==` + `parsingequivalent` + freezing (§4.2); decoder validation rules
  (§4.3); `allow_invalid_names` scope (§4.2); JSON rule table (§4.11); decimal resolution rule and
  Java union-branch selection (§4.7); per-logical-type contract (§4.8); collision-safe `SchemaCache`
  (§4.10); sort order in Phase 3 (§4.12); strict containers + legacy mode + atomic writes + append
  deferred (§4.9); ownership contract, views deferred (§4.9); structured parallel algorithm (§4.9);
  `AvroStyle` eligibility (§4.8); Scan pin/rules/DataAPI (§6); `EnumValue` (§4.6); interop corrections
  and capability matrix (§8); independent oracles and sandboxed fuzzing (§9); corrected baselines and
  ratio gates (§10); 1.x defect catalogue and `writer_schema` naming (§7, §5.2); NUL byte removed;
  phases split and readiness levels defined (§12); legacy removed at Phase 0 with the new suite as the
  green gate (§2.3).

---

## 15. Status record (maintained through implementation)

* Assumptions: local-only work; no pushes/PRs/tags/registration; CSV/Arrow/Parquet checkouts untouched;
  network used only for specs, dependencies, and interop tools.
* Commands and results: recorded in `STATUS.md` in the worktree as phases complete (exact commands,
  Julia versions, pass/fail counts, benchmark numbers).
* Current state: **plan under review (round 2); no production code changed yet.**

---

## Appendix A — Probe scripts and raw results

Kept in the scratchpad of the authoring session (`probe/`, `fixtures/`, `javah/`) and summarised in
§2.2, §8.2, §8.3; the scripts are re-created as tests and harness sources in Phases 0, 2 and 4.

## Appendix B — API sketch

```julia
using Avro, Tables

sch = Avro.parseschema("""{"type":"record","name":"Weather","namespace":"test","fields":[
  {"name":"station","type":"string"},{"name":"time","type":"long"},{"name":"temp","type":"int"}]}""")
Avro.fingerprint(sch)                       # CRC-64-AVRO of the canonical form (UInt64)
bytes = Avro.encode(sch, (station="011990-99999", time=-619524000000, temp=0))
r = Avro.decode(sch, bytes)                 # Avro.Record; r.station == "011990-99999"
Avro.decode(sch, bytes, MyWeather)          # typed, via StructUtils (AvroStyle)

Avro.write("w.avro", table; codec=:zstandard, metadata=Dict("source"=>b"sensor"))
t = Avro.Table("w.avro"; scan=Tables.Scan(select=(:station, :temp), filter=Tables.col(:temp) > 0))
for row in Avro.Rows("w.avro")              # streaming, one block resident
    row.temp
end
w = Avro.Writer("out.avro", sch; codec=:snappy); push!(w, row); close(w)

new = Avro.parseschema(...)                 # reader schema with a new defaulted field
Avro.Table("w.avro"; reader_schema=new)     # resolved decode
store = Avro.SchemaCache(); Avro.register!(store, sch)
msg = Avro.encodesingle(sch, row); Avro.decodesingle(msg, store)
Avro.Table("old.avro"; legacy=:avrojl1)     # file written by Avro.jl 1.x
```
