# Avro.jl 2.0 — Audit and Rewrite Plan

Status: DRAFT v1 for adversarial review (Claude Fable 5 author; Codex gpt-5.6-sol reviewer).
Date: 2026-08-21. Repository: `JuliaData/Avro.jl`, local checkout `/Users/jacob.quinn/.julia/dev/Avro`,
branch `jq/v2-rewrite` forked from `main` @ `0c7be10db6d83fd20806a8eceec9276a7aa8e21d` (v1.1.2, registered).
Specification source pinned for this plan: `apache/avro` `main` @ `326950f40c1172f7564c757b0e51c39883721083`
(`doc/content/en/docs/++version++/Specification/_index.md`, 2026-08-18), plus the Apache shared test data
under `share/test/` at the same commit. Reference implementations available locally for differential testing:
Apache Java `avro-tools` 1.12.2 (Maven Central jar) and Python `fastavro` 1.12.2 / `avro` 1.12.2 (venv).

Review protocol: this file is the single text both reviewers operate on. Each review round produces
`reviews/codex-review-N.md` (Codex, read-only sandbox) and `reviews/response-N.md` (Claude disposition);
the plan is revised in place and the decision log in §14 is appended. The loop ends only when Codex's
file ends with `VERDICT: AGREE` and Claude's response states no remaining material objections.
Declared artifacts, fixtures, benchmarks, and gates in this plan are **deliverables of the implementation
phases, not preconditions of the review**.

---

## 1. Executive summary

Avro.jl 1.1.2 is a ~1,600-line package that covers a useful slice of Avro (all types, the five logical
types it knows, container files with four codecs, a Tables.jl sink/source) but it is neither spec-correct
nor safe on untrusted input, and its architecture (two-pass `nbytes` sizing, per-value dynamic dispatch
through StructTypes closures, `Vector{UInt8}`-only decoding with `@inbounds` and no bounds discipline,
Julia-`Union`-ordered unions, global codec state) cannot be incrementally fixed into a leading
implementation. The audit in §2 was done empirically against the Apache corpus, the Apache Java tools,
fastavro, and hand-built malformed inputs. Headline findings:

* **Interop is broken in both directions.** Every uncompressed container file 1.1.2 writes is rejected by
  Apache Java ("Block read partially"): the null-codec block carries the uninitialised 5% sizing cushion.
  Its zstd files name the codec `zstd` (spec: `zstandard`) and are rejected by Java and fastavro.
  Reading: the official `weather-zstd.avro` **segfaults** the process; `weather-snappy.avro` returns
  garbage silently; a Java-written record with `decimal`/`uuid` logical types decodes at wrong offsets;
  a `["string","int"]` union (Apache `withUnion/data.avro`) throws a `MethodError` because union branches
  are mapped through Julia's canonical `Union` order instead of the schema's branch order.
* **Untrusted input is unsafe.** Truncated or over-long varints silently decode as `0`; `fixed` reads past
  the buffer through `@inbounds`; a negative length → `OutOfMemoryError`; a 7-byte array header made the
  reader attempt a multi-terabyte allocation (the probe had to be killed after 10 minutes).
* **Schema handling is permissive and incomplete.** Duplicate field names, invalid names and enum symbols,
  duplicate union branches, and mismatched defaults are accepted; nested unions fail with a JSON parser
  error; no canonical form, fingerprints, schema resolution, single-object encoding, JSON encoding,
  `timestamp-nanos`, fixed-`uuid`, or protocols.
* **Performance is an order of magnitude off.** 23 MB/s write and ~6.9 s to materialise a 1M-row,
  4-column file versus fastavro's 0.65 s write / 0.50 s read (same machine, single thread).

The plan (§4–§12) replaces the core with a schema-compiled encoder/decoder over bounds-checked
byte buffers, a faithful schema model (names, aliases, defaults, logical types, canonical form,
fingerprints, resolution), a streaming, parallel, resource-bounded container reader/writer, a columnar
`Avro.Table` with `Tables.Scan` pushdown, a row-streaming `Avro.Rows`, a StructUtils-based typed API,
single-object encoding, JSON encoding, and protocol/RPC core support, with conformance gates against
Apache Java and fastavro. The release is **Avro.jl 2.0.0** (breaking; §7 gives the migration policy).

---

## 2. Evidence-based audit of 1.1.2

### 2.1 Method

* Full read of `src/` (12 files) and `test/runtests.jl`; baseline suite run on Julia 1.12.6: 65,635 passes.
* Apache corpus read with `Avro.readtable` (each in an isolated process after the first segfault).
* Julia-written files read with `avro-tools tojson/getschema` and `fastavro.reader`.
* Java-written records (`fromjson` with decimal/uuid/timestamp-micros/enum/union-default schema) read back.
* Hand-built malformed inputs for every decoder primitive; schema-parser probes for spec constraints.
* Throughput/allocation baseline (§10.1).

### 2.2 Results

| Probe | 1.1.2 result | Severity |
|---|---|---|
| `weather.avro`, `weather-deflate.avro`, `weather-sorted.avro`, `syncInMeta.avro`, `schemas/simple/data.avro` | read correctly | — |
| `weather-zstd.avro` (codec `zstandard`) | **segfault** (unknown codec → compressed bytes parsed as records under `@inbounds`) | memory safety |
| `weather-snappy.avro` | **silent garbage** (codec ignored, data parsed uncompressed) | silent corruption |
| `schemas/withUnion/data.avro` (`["string","int"]`) | `MethodError` (Julia `Union{String,Int32}` member order ≠ schema branch order) | correctness |
| Java-written `decimal(bytes)`, `decimal(fixed 8)`, `uuid`, `timestamp-micros`, enum, `["null","string"]` | UUID decoded from wrong offset (decimal size defaulted to 16 in `skipvalue`) | correctness |
| Julia-written `jl.avro` (null codec) → Java `tojson` | **"Block read partially, the data may be corrupt"** (block bytes include sizing cushion) | interop |
| Julia-written `jl-zstd.avro` → Java / fastavro | "Unrecognized codec: zstd" | interop |
| Julia-written `jl-deflate.avro` → Java / fastavro | read correctly | — |
| `Avro.read([0x01,0x61], String)` (length −1) | `OutOfMemoryError` | DoS |
| `Avro.read([0x80,0x80], Int64)` (truncated varint) | returns `0` silently | silent corruption |
| 11-byte varint | returns `0` silently (no overflow check) | silent corruption |
| `Avro.read([1,2], NTuple{4,UInt8})` | returns `(1,2,0,0)` — out-of-bounds read | memory safety |
| union index 4 of a 2-branch union | `BoundsError` | ungraceful |
| enum index out of range | `BoundsError` at `show` | ungraceful |
| array count 2^40 (7 bytes of input) | attempted ~8 TB allocation; process killed after 10 min | DoS |
| duplicate field names / `"9R"` name / `"a-b"` enum symbol / `["int","int"]` / `"default":"oops"` for int | all accepted | spec |
| nested union `["null",["int","string"]]` | JSON3 parse error (`ExpectedOpeningQuoteChar`) | ungraceful |
| `timestamp-nanos`, `big-decimal`, fixed `uuid` | unsupported (nanos → plain long; fixed uuid parsed as string uuid) | spec gap |
| `Tables.Schema` with >10k columns stored in fields (issue #18) | `schematype` reads type parameters → fails | bug |
| Throughput (1M rows × {long,double,string,boolean}) | write 1.02 s (22.7 MB/s); read-index 2.9 s + materialise 3.9 s; 1,376 allocs per single-record read | performance |

Open upstream issues map onto these: #15 (row-at-a-time read/write → §5.3 `Avro.Writer`/`Avro.Rows`),
#17 (buffer-too-small with string columns → two-pass sizing removed), #18 (wide `Tables.Schema`),
#6 (pre-written object files → §8 corpus), #13 (`signed(UInt8)` on Julia < 1.5 → moot, floor is 1.10),
PR #16 (row-wise API prototype → superseded by §5.3).

### 2.3 What stays, what goes

Keep (as ideas, re-implemented under the new core, with their tests ported):

* The public *shape* of the container API (`writetable`/`readtable`/`tobuffer`/`parseschema`) survives as
  deprecated shims for one major cycle (§7).
* `missing` as the Julia value of Avro `null` (ecosystem convention; `nothing` also encodes as null).
* Logical-type value types: `Avro.Decimal{P,S}` (redesigned: spec byte order), `Avro.Duration`
  (fields become `UInt32` per spec), `Date`/`Time`/`DateTime`/`UUID` mappings.
* The zigzag/varint arithmetic (correct as written), the `Tables.partitions` → blocks idea, the
  `dictrowtable` fallback for schema-less sources, the per-block index idea (becomes the parallel
  block pre-scan).
* The test corpus of Julia round-trip cases in `test/runtests.jl` (ported to the new API).

Replace:

* StructTypes/JSON3 schema model (`Schema = Union{String, LogicalType, SchemaType, UnionType}`,
  `Base.@pure eachunion`, mutable schema structs with `type::String` fields) → immutable schema tree
  with a real parser/validator (§4.2).
* `nbytes` two-pass writer and `Vector{UInt8}`-only, `@inbounds`, position-tuple decoders → growable
  encoder, bounds-checked decoder, compiled read/write plans (§4.3–§4.5).
* `Avro.Record{names,types,N}` lazy-field record, `Avro.Enum{names}`, `Avro.Array{T}` lazy vector →
  typed columnar `Avro.Table`, generic `Avro.Record`, `Symbol` enums, plain `Vector`s (§4.6).
* Global per-thread codec arrays (`COMPRESSORS[Threads.threadid()]`, unsafe under task migration) →
  per-task codec instances (§4.9).
* CI (Julia 1.5/1/nightly, ubuntu only, actions v1) → §11 matrix.

---

## 3. Specification surface assessment

Spec sections with the scope decision. "Required" ships in 2.0.0; "Optional" ships in 2.0.0 if its
phase gate passes, else 2.x; "Deferred" is intentionally out of scope with the reason recorded.

| Spec area | Scope | Notes |
|---|---|---|
| Schema declaration, primitive & complex types, attributes as metadata | Required | unknown attributes preserved (`props`) and re-emitted; ignored by canonical form |
| Names, namespaces, fullname algorithm, define-before-use, uniqueness, reserved primitive names | Required | §4.2; the spec's `Example`/`Simple`/`a.full.Name` example is a test |
| Aliases (type and field; relative/qualified) | Required | used in resolution; any string accepted as alias |
| Record field `default` (all types, union-first-branch rule, bytes/fixed `\u00XX` strings), `order`, `doc` | Required | defaults validated at parse time against the field schema |
| Enum `default` | Required | used in resolution |
| Union constraints (no immediate nesting, one unnamed type per kind) | Required | |
| "Fixing an invalid, but previously accepted, schema" (aliases for invalid names) | Required | parser option `validate_names=false` to load legacy schemas, resolution renames them |
| Binary encoding, all types, blocked arrays/maps with negative counts and sizes | Required | reader accepts both block forms; writer emits positive-count blocks, optional sized blocks |
| JSON encoding of datums (union wrapping by type name) | Required | needed for defaults, `avro-tools` differential tests, debugging |
| Single-object encoding (`C3 01` + CRC-64-AVRO LE + payload) | Required | with a `SchemaStore` lookup interface |
| Sort order | Optional | `Avro.compare(schema, a, b)` on encoded bytes; small, isolated |
| Object container files: header, metadata, blocks, sync, codecs `null`/`deflate` | Required | |
| Codecs `snappy` (with CRC32), `zstandard` | Required | hard dependencies (Snappy.jl, CodecZstd) |
| Codecs `bzip2`, `xz` | Required, via package extensions | CodecBzip2/CodecXz as weak deps; actionable error when absent |
| Protocol declaration (`.avpr`: types incl. `error`, messages, request/response/errors/one-way) | Optional (Phase 6) | pure JSON parsing + protocol MD5 |
| Handshake records, message framing, call/response format | Optional (Phase 6) | transport-agnostic core, in-process tests against Apache `share/test/interop/rpc` fixtures |
| HTTP transport (`POST`, `avro/binary`) | Deferred (2.x) | would be an extension on HTTP.jl; no demand evidence yet |
| Stateful transports (sockets), Netty framing | Deferred | non-spec / Java-specific |
| Schema resolution (all rules incl. promotions, reorder, defaults, enum default, unions, aliases) | Required | §4.7 |
| Parsing Canonical Form, fingerprints (CRC-64-AVRO, MD5, SHA-256) | Required | conformance file `schema-tests.txt` |
| Logical types: decimal (bytes/fixed), uuid (string/fixed), date, time-millis/micros, timestamp-millis/micros/nanos, local-timestamp-millis/micros/nanos, duration | Required | invalid logical types ignored per spec |
| Logical type `big-decimal` | Optional | reads as `(BigInt unscaled, scale)`; Java/C++/Rust only today |
| Avro IDL (`.avdl`) | Deferred | separate grammar; separate package if ever |
| Trevni, tethered MapReduce | Deferred | not part of the data format |

---

## 4. Architecture

### 4.1 Principles

1. **Spec first, Java as tie-breaker.** Where the spec is silent or ambiguous, behave like Apache Java
   1.12 (the reference everyone interoperates with), and record the choice in §14.
2. **Never trust bytes.** Every decode path is bounds-checked against an explicit end position, every
   length/count is validated before allocation, recursion depth is capped, decompression is capped;
   the only failure modes on malformed input are `Avro.DecodeError`/`Avro.LimitError`.
3. **Compile the schema, not the value.** Encoding/decoding is driven by plan trees built once per
   (schema, Julia type) pair; the per-value hot path has no dynamic dispatch for records of ≤ 32 fields
   and at most one dispatch per field beyond that; primitives allocate nothing.
4. **Stream by default, materialise on request.** Container reading is block-at-a-time with one
   decompressed block resident per task; `Avro.Table` is the explicit materialisation.
5. **No global mutable state.** Codec instances, buffers, and plans are owned by reader/writer objects or
   tasks. The only module-level mutable is a lock-protected logical-type/Julia-type mapping registry.
6. **Qualified API, zero exports.**

### 4.2 Schema model (`src/schema.jl`, `src/names.jl`, `src/logical.jl`, `src/canonical.jl`)

```julia
abstract type Schema end
struct NullSchema <: Schema; props::Props; end             # Props = Dict{String,Any} (custom attributes), empty by default
struct BooleanSchema <: Schema; props::Props; end
struct IntSchema <: Schema; logical::Union{Nothing,LogicalType}; props::Props; end
struct LongSchema <: Schema; logical; props; end
struct FloatSchema <: Schema; props; end
struct DoubleSchema <: Schema; props; end
struct BytesSchema <: Schema; logical; props; end
struct StringSchema <: Schema; logical; props; end
struct ArraySchema <: Schema; items::Schema; props; end
struct MapSchema <: Schema; values::Schema; props; end
struct UnionSchema <: Schema; branches::Vector{Schema}; end
struct FixedSchema <: Schema; name::FullName; aliases::Vector{FullName}; doc; size::Int; logical; props; end
struct EnumSchema <: Schema; name::FullName; aliases; doc; symbols::Vector{String}; default::Union{Nothing,Int}; props; end
struct Field; name::String; schema::Schema; doc; default::Default; order::Order; aliases::Vector{String}; props; end
mutable struct RecordSchema <: Schema   # mutable only so recursive references can be closed after parsing
    name::FullName; aliases; doc; fields::Vector{Field}; iserror::Bool; props
    fieldindex::Dict{String,Int}        # name → position, built once
end
struct FullName; name::String; namespace::String; end       # fullname(x) = isempty(ns) ? name : ns*"."*name
```

* Logical types are *attributes* of the underlying schema (`logical` field), matching the spec ("a
  logical type is always serialised using its underlying type"). `LogicalType` is a small closed set:
  `Decimal(precision, scale)`, `BigDecimal`, `UUIDLogical`, `DateLogical`, `TimeMillis`, `TimeMicros`,
  `TimestampMillis`, `TimestampMicros`, `TimestampNanos`, `LocalTimestampMillis/Micros/Nanos`,
  `DurationLogical`, and `UnknownLogical(name)` (kept so the schema re-serialises faithfully). Invalid
  logical types (wrong underlying type, decimal scale > precision, precision ≤ 0, precision too large for
  the fixed size, duration size ≠ 12, uuid fixed size ≠ 16) are dropped to the underlying type per spec,
  with their attributes preserved in `props`. Unknown logical-type names are not an error.
* `Default` is `nothing` (absent) or `Some(value)` where `value` is the JSON default *already validated and
  converted* to the field schema's Julia value (so `null` defaults are representable and decoders can
  emit defaults without re-parsing JSON). A `DefaultJSON` copy of the original JSON text is kept for
  faithful re-serialisation.
* Parsing: `Avro.parseschema(src; validate_names=true, validate_defaults=true, max_depth=..)` walks
  `JSON.parse` output (`JSON.Object`, ordered) with a `ParseContext{namespace stack, named-type table,
  depth}`. Named types are defined before use in depth-first/left-to-right order; references resolve by
  fullname (qualified or relative to the enclosing namespace); redefinition and forward references are
  `SchemaError`s. All spec constraints in §3 are enforced; each error carries a JSON path
  (`record R > field next > union[1]`).
* Recursive schemas form a cyclic object graph (`LongList.fields[2].schema.branches[2] === LongList`).
  `==`/`hash` on schemas are defined as equality of Parsing Canonical Form (cached per schema), which is
  well-defined on cycles because canonical printing emits later occurrences as name references.
* Printing: `JSON.json(schema)` / `Avro.json(schema; canonical=false)` emits the spec JSON (first
  occurrence of a named type in full, later references by fullname, namespace attribute only when it
  differs from the enclosing one, custom `props` included). `Avro.canonical(schema)` implements the seven
  PCF transformations; `Avro.fingerprint(schema; algorithm=:crc64avro|:md5|:sha256)` hashes the PCF
  bytes. CRC-64-AVRO is in-house (table from the spec pseudo-code); MD5 via `MD5.jl`; SHA-256 via `SHA`.
* Julia type → schema (`Avro.schema(T)`, `Avro.schema(::Tables.Schema)`): §4.8.
* `Avro.Protocol` (Phase 6) reuses the same parser with a protocol-level namespace.

### 4.3 Byte-level decoder and encoder (`src/decoder.jl`, `src/encoder.jl`)

```julia
mutable struct Decoder{B <: AbstractVector{UInt8}}
    buf::B; pos::Int; stop::Int       # next byte, last valid byte (inclusive)
    depth::Int; limits::Limits
end
```

* Primitives: `readbool`, `readint` (≤ 5 bytes, value range-checked into `Int32`), `readlong` (≤ 10 bytes,
  bit 64 overflow rejected), `readfloat`/`readdouble` (little-endian, `reinterpret`, NaN payload
  preserved), `readlen` (non-negative, `≤ stop - pos + 1`), `readbytes` → `SubArray` view,
  `readstring` → `String` (copies; `unsafe_string` of the checked range), `readfixed(n)`, plus `skip*`
  counterparts. Every check failure throws `DecodeError(msg, pos)`. No `@inbounds` anywhere that is not
  immediately preceded by an explicit range check on the same values.
* Buffers are `AbstractVector{UInt8}` with contiguous memory (`Vector`, `Mmap.Array`, views of them);
  `IO` sources are read into owned buffers by the container layer (block size is known) or by
  `Avro.decode(schema, io)` (reads to end; documented).
* `Encoder` is a growable `Vector{UInt8}` with `pos`, `ensureroom!`, and write primitives
  (`writelong` is the 10-byte unrolled varint, `writebytes`, `writestring`, `writefloat`, …). `Encoder`
  can be re-used (`reset!`) and its buffer handed to a codec or `IO` without copying. No pre-sizing pass.

### 4.4 Limits (`src/limits.jl`)

```julia
Base.@kwdef struct Limits
    max_depth::Int = 256                  # nesting depth of values (recursive schemas)
    max_bytes::Int = 2^31 - 1             # one bytes/string/fixed value
    max_collection_length::Int = 2^31 - 1 # one array/map block count (also bounded by remaining bytes / min item size)
    max_block_bytes::Int = 1 << 30        # container block, compressed and decompressed
    max_schema_bytes::Int = 16 << 20      # avro.schema metadata, and parseschema input
    max_schema_depth::Int = 256
    max_metadata_entries::Int = 10_000
end
```

Collections are decoded incrementally (`push!` with a `sizehint!` of `min(count, remaining ÷ minsize(item
schema), max_collection_length)`), never `Vector(undef, count)` from an untrusted count. The null-item
and empty-record special cases (`minsize == 0`) are bounded by `max_collection_length` alone. Recursion
depth is checked on every record/array/map/union entry and the default is sized so that the deepest
decode frame chain fits comfortably in a worker task stack (verified by a test that decodes at
`max_depth` inside `Threads.@spawn` on every CI platform).

### 4.5 Plans: schema-directed codecs (`src/plan_read.jl`, `src/plan_write.jl`)

A **read plan** is a tree of immutable functor structs, one per schema node, parameterised by the target
Julia type: `LongPlan <: ReadPlan{Int64}`, `StringPlan`, `ArrayPlan{P}`, `MapPlan{P}`, `UnionPlan{Ps}`,
`EnumPlan`, `FixedPlan{N}`, logical plans (`DatePlan`, `DecimalPlan{P,S}`…), and record plans in two
flavours: `RecordPlan{names, Ps<:Tuple}` (fully unrolled via `ntuple`/`Val` for ≤ 32 fields; per-field
code is static) and `WideRecordPlan` (`Vector{ReadPlan}`, one dynamic dispatch per field behind a
function barrier). `read(plan, dec)` returns the Julia value; `skip(plan, dec)` skips it. Write plans
mirror this (`write(plan, enc, value)`), with value extraction strategies for NamedTuples, StructUtils
structs, `Tables.AbstractRow`s (by column index), `AbstractDict`s, and iterables.

**Resolving plans** (§4.7) are the same trees with writer-driven field order and `SkipPlan`/
`DefaultPlan`/`PromotePlan`/`EnumRemapPlan`/`UnionRemapPlan` nodes, so resolution costs nothing when
schemas match and is a plain plan when they do not.

**Column plans** are the columnar variant used by `Avro.Table`: each selected field owns a typed
`ColumnBuilder{T}` (a `Vector{T}` or `Vector{Union{Missing,T}}` plus the leaf plan) and a row is
decoded by `decodepush!` across builders (unrolled tuple ≤ 32 columns, dynamic loop beyond); unselected
fields use `skip`. Column element types follow §4.6.

Plans are built by `Avro.plan(schema, T=juliatype(schema))`; construction is pure and cheap (microseconds
for the interop schema), so plans are built per reader/writer object and cached nowhere globally.
Per-type compile cost is paid once per distinct (schema, T) like any Julia data tool; wide records
(> 32 fields) never generate per-schema unrolled code.

### 4.6 Julia value model (generic decoding, `Avro.juliatype(schema)`)

| Avro | Julia (decode) | Notes |
|---|---|---|
| null | `Missing` (`missing`) | `nothing` also encodes as null |
| boolean / int / long / float / double | `Bool` / `Int32` / `Int64` / `Float32` / `Float64` | |
| bytes | `Vector{UInt8}` | copied; `Avro.Table(...; bytes=:view)` opt-in returns views into the block buffer (lifetime documented) |
| string | `String` | |
| fixed(N) | `Vector{UInt8}` of length N | typed API accepts `NTuple{N,UInt8}`; logical fixed types map as below |
| enum | `Symbol` | typed API accepts `Base.Enum` subtypes, `Symbol`, `String` (validated against `symbols`) |
| array | `Vector{juliatype(items)}` | |
| map | `Dict{String, juliatype(values)}` | insertion order is not preserved by `Dict` (spec imposes none) |
| union `["null", T]` / `[T, "null"]` | `Union{Missing, juliatype(T)}` | the dominant case, columnar-friendly |
| other union | `Union{juliatype.(branches)...}` | encoding picks the first branch that accepts the value; `Avro.Branch(i, v)` forces a branch for ambiguous unions (two branches with the same Julia type) |
| record | `NamedTuple` when `≤ 32` fields, else `Avro.Record` (schema + `Vector{Any}`, `Tables.AbstractRow`, property access by name) | threshold is a documented constant; `Avro.Table` top-level rows are columns, so this only concerns nested records and `Avro.Rows` |
| decimal(bytes/fixed) | `Avro.Decimal{P,S}` (`Int128` unscaled; big-endian two's complement; minimal bytes for `bytes`, sign-extended to size for `fixed`) | precision ≤ 38 enforced when mapping to Julia; larger precision decodes as `Avro.Decimal{P,S,BigInt}` (slow path) |
| big-decimal (optional) | `Avro.BigDecimal` (`BigInt` unscaled, `Int32` scale) | |
| uuid (string / fixed 16) | `UUIDs.UUID` | fixed form is the RFC-4122 big-endian 16 bytes |
| date | `Dates.Date` | |
| time-millis / time-micros | `Dates.Time` | write truncates sub-unit precision (documented) |
| timestamp-millis/micros/nanos, local-timestamp-* | `Dates.DateTime` | micros/nanos use floor division on read (negative instants correct); precision loss documented |
| duration | `Avro.Duration(months::UInt32, days::UInt32, millis::UInt32)` | |

### 4.7 Schema resolution (`src/resolution.jl`)

`Avro.resolve(writer::Schema, reader::Schema) -> ResolvedSchema` implements every rule of the
"Schema Resolution" section: match by kind (arrays/maps recursively; enums/fixed/records by unqualified
name *after* applying the reader's type aliases; fixed sizes; primitives equal or promotable
`int→long/float/double`, `long→float/double`, `float→double`, `string↔bytes`), records (fields matched by
name after reader field aliases; writer-only fields skipped; reader-only fields require a default; reader
field order wins), enums (symbol remap, reader `default` for unknown symbols), unions (writer branch →
first matching reader branch; reader-union-only; writer-union-only), and logical decimals (scale and
precision must match). Decimal/logical attributes are compared on the reader side; non-matching logical
types fall back to the underlying-type rule. Failures are `ResolutionError` with both paths. The result
is consumed by `Avro.plan` to produce a resolving read plan (§4.5) and is cached inside reader objects
keyed by writer fingerprint (single-object decoding with a `SchemaStore`).

### 4.8 Typed API and Julia type mapping (`src/types.jl`, StructUtils)

* `Avro.schema(T)` derives a schema from a Julia type: the §4.6 table inverted, plus `Int8/16/UInt8/16 →
  int`, `UInt32/UInt64 → long` (range-checked on write), `Float16 → float`, `AbstractString`/`Symbol`/
  `Char → string`, `NTuple{N,UInt8} → fixed`, `Union{Missing,T} → ["null", T]`, other `Union`s → union in
  Julia's member order (documented), `Base.Enum` subtypes → enum, structs → records via StructUtils
  (`fieldnames`/`fieldtypes`, `@kwarg`/`@defaults` field defaults become Avro defaults when JSON-encodable,
  `StructUtils.lower`/`lift` hooks honoured), `Tables.Schema` → record (reads `names`/`types` through
  the public accessors, so wide schemas stored in fields work — fixes #18). Named-type naming: structs use
  `nameof(T)` with `namespace = string(parentmodule(T))`; `NamedTuple`/`Tables.Schema` records are named
  `Record` by default with nested anonymous records `Record_1`, `Record_2`, … in depth-first order, the
  same Julia type always reusing its first definition; `name=`/`namespace=` keywords override.
* `Avro.decode(schema, bytes, T)` constructs `T` through StructUtils (`make`-style construction honouring
  `@kwarg`, `choosetype`, `lift`) from a typed read plan; `Avro.encode(schema, x)` writes any value the
  plan's extraction strategies accept. `DateTime` defaults to `timestamp-millis` when deriving a schema
  (interoperability with Java/Spark/fastavro defaults; 1.x used `local-timestamp-millis` — breaking,
  documented); both flavours decode to `DateTime`.

### 4.9 Container files (`src/container.jl`, `src/codecs.jl`)

* `Avro.Reader(src; limits)`: parses the header (magic, metadata map decoded with the spec's
  `{"type":"map","values":"bytes"}` schema, sync marker, `avro.schema` parsed with the §4.2 parser under
  `max_schema_bytes`), selects the codec from `avro.codec` (`null`/absent, `deflate`, `snappy`, `bzip2`,
  `xz`, `zstandard`; `zstd` accepted as a read-only alias for files written by Avro.jl ≤ 1.1.2), and
  iterates blocks: `count`, `size ≤ max_block_bytes`, data, sync (mismatch → `DecodeError` with block
  index; a partial trailing block → `DecodeError("truncated file")`). Decompression is streamed with a
  cap of `max_block_bytes` on the output. Sources: file path (mmap by default, `mmap=false` to read),
  `AbstractVector{UInt8}`, `IO` (streaming: header + one block buffer resident; block size bounded first),
  `IOBuffer` (its written bytes only).
* Trailing bytes inside a block after `count` records: 1.1.2 produced such files for five years, Java
  rejects them, fastavro accepts them. 2.0 **accepts them with a one-time `@warn` per source** and rejects
  them under `strict=true`. (Decision open for review; §14.)
* Parallel decoding: a cheap pre-scan of block headers (no decompression) yields block offsets and row
  counts; `Avro.Table` preallocates columns of `sum(counts)` rows and decodes blocks on
  `Threads.@spawn` tasks (wrapped in `errormonitor`), each with its own `Decoder`, codec instance, and
  builders writing disjoint row ranges, so no concatenation or `ChainedVector` is needed. `ntasks=1`
  (or `Threads.nthreads()==1`) runs inline. `IO` sources decode sequentially (no random access).
* `Avro.Writer(io, schema; codec=:null, level=nothing, metadata=Dict{String,Vector{UInt8}}(),
  sync=rand(16 bytes), block_bytes=64 KiB)`: writes the header, buffers encoded rows in an `Encoder`,
  emits a block when `block_bytes` is reached or on `flush`/`close`; `push!(w, row)` / `write(w, rows)`;
  `Base.close` writes the final block. Metadata keys starting with `avro.` other than `schema`/`codec`
  are rejected. Append mode (`Avro.Writer(path; append=true)`) re-reads the existing header (schema,
  codec, sync) and continues after the last complete block — optional, gated in Phase 4.
* `Avro.write(dst, table; schema=nothing, codec, …)`: schema inferred from `Tables.schema` (or from
  `Tables.dictrowtable` when absent); `Tables.partitions` become block boundaries (each partition ≥ 1
  block); row encoding through write plans with column-major extraction for column-accessible
  partitions. The block payload is exactly the encoded bytes (fixes the 1.1.2 cushion bug).
* Codecs: `deflate` = raw RFC 1951 via CodecZlib; `snappy` = Snappy.jl block format followed by the
  4-byte big-endian CRC32 of the *uncompressed* data (verified on read; in-house table CRC32 tested
  against `Zlib_jll`); `zstandard` via CodecZstd; `bzip2`/`xz` through extensions `AvroCodecBzip2Ext`/
  `AvroCodecXzExt` (weak deps) with the error message naming the package to load. Codec objects are
  allocated per task (never shared), `TranscodingStreams.finalize`d deterministically.

### 4.10 Single-object encoding and schema stores (`src/singleobject.jl`)

`Avro.encodesingle(schema, x) -> Vector{UInt8}` (marker `C3 01`, little-endian CRC-64-AVRO of the PCF,
payload). `Avro.decodesingle(bytes, store; reader=nothing, T=…)` validates the marker, looks up the
writer schema by fingerprint in a `SchemaStore` (`Avro.lookup(store, fp::UInt64)`; built-in
`Avro.SchemaCache` with `register!`), resolves against `reader` when given, and decodes. Unknown
fingerprints raise `Avro.UnknownSchemaError(fp)`. (Confluent's wire format is a different, non-Apache
format and stays out; users can strip its 5-byte prefix and call `Avro.decode` with the registry schema.)

### 4.11 JSON encoding (`src/jsonencoding.jl`)

`Avro.tojson(schema, x; pretty)` and `Avro.fromjson(schema, json, T=…)` implement the JSON encoding
(field defaults rules for all types, union values wrapped by type name except `null`, `bytes`/`fixed`
as ` –ÿ` strings, map vs record disambiguated by schema, NaN/Infinity as Java emits them).
Used by defaults validation, `avro-tools fromjson/tojson` differential tests, and the docs.

### 4.12 Protocols and RPC core (`src/protocol.jl`, Phase 6, optional)

`Avro.parseprotocol(json) -> Protocol(name, namespace, doc, types, messages)`; `Message(name, doc,
request::RecordSchema (anonymous), response, errors::UnionSchema (effective: `"string"` prepended),
oneway)`; `Avro.md5(protocol)` (hash of the protocol JSON text); handshake request/response schemas
built in; `Avro.frame`/`Avro.unframe` (4-byte big-endian lengths, zero-length terminator);
`Avro.encoderequest`/`decoderequest`/`encoderesponse`/`decoderesponse` (metadata map, message name,
parameters as the request record; error flag + response or error union); `Avro.Responder(protocol,
handlers)` / `Avro.Requestor(protocol, send::Function)` implementing the stateless-transport handshake
state machine over byte payloads with a protocol cache. Verified against
`share/test/interop/rpc/{echo,add,hello}` request/response bytes and an in-process echo loop.

### 4.13 Errors

`abstract type AvroError <: Exception`; concrete `SchemaError` (with JSON path), `DecodeError` (message,
byte position, optional value path), `LimitError <: DecodeError`, `EncodeError` (value path, expected
schema), `ResolutionError` (writer/reader paths), `CodecError`, `UnsupportedCodecError` (names the
extension), `UnknownSchemaError`. Every error type has a `showerror` with actionable text.

### 4.14 Concurrency contract

* `Schema`, plans, `Limits`, `SchemaCache` (lock-protected) are safe to share across tasks.
* `Decoder`/`Encoder`/`Reader`/`Writer`/codec instances are single-owner; concurrent use is a bug.
* `Avro.Table` columns are plain vectors (concurrent reads safe). `Avro.Rows` is a single-consumer iterator.
* Writer-side parallel compression (encode on the caller task, compress blocks on a worker pool with
  ordered emission) is a Phase 4 stretch item behind `ntasks=`; correctness first.

### 4.15 Module layout

```
src/Avro.jl            module, includes, public API docstrings, `public` declarations (1.11+)
src/errors.jl          error types
src/names.jl           FullName, name validation, namespace resolution
src/schema.jl          Schema types, Field/Default, parser, validator, printer, equality
src/logical.jl         LogicalType structs, validation, Julia value types (Decimal, Duration, BigDecimal)
src/canonical.jl       Parsing Canonical Form, CRC-64-AVRO, fingerprints
src/limits.jl          Limits
src/decoder.jl         Decoder + primitive reads/skips
src/encoder.jl         Encoder + primitive writes
src/plan_read.jl       read plans (generic + typed + resolving)
src/plan_write.jl      write plans (NamedTuple/struct/row/dict/iterable extraction)
src/columns.jl         column builders and column plans
src/types.jl           Julia type <-> schema mapping, StructUtils integration
src/resolution.jl      resolve(writer, reader)
src/jsonencoding.jl    tojson/fromjson
src/codecs.jl          codec registry, CRC32, deflate/snappy/zstandard
src/container.jl       Reader/Writer, header/block parsing, parallel decode, Avro.write
src/tables.jl          Avro.Table, Avro.Rows, Tables.jl interface, Scan pushdown
src/singleobject.jl    single-object encoding, SchemaStore
src/protocol.jl        protocols, handshake, framing, calls (Phase 6)
src/deprecated.jl      1.x shims
src/precompile.jl      PrecompileTools workload
ext/AvroCodecBzip2Ext.jl, ext/AvroCodecXzExt.jl
```

---

## 5. Public API (all qualified; nothing exported)

### 5.1 Schemas

```julia
Avro.parseschema(src::Union{AbstractString, AbstractVector{UInt8}, IO}; validate_names=true, validate_defaults=true, limits=Limits()) -> Schema
Avro.schema(T::Type; name=nothing, namespace=nothing) -> Schema      # Julia type → schema
Avro.schema(::Tables.Schema; name="Record", namespace="")            # table schema → record
Avro.schema(x::Avro.Table / Avro.Rows / Reader)                      # the file's writer schema
Avro.json(schema; pretty=false) / JSON.json(schema)                  # spec JSON text
Avro.canonical(schema) -> String                                      # Parsing Canonical Form
Avro.fingerprint(schema; algorithm=:crc64avro) -> UInt64 | Vector{UInt8} (md5/sha256)
Avro.resolve(writer, reader) -> ResolvedSchema
Avro.juliatype(schema) -> Type
Avro.fullname(named_schema) -> String
Base.:(==), Base.hash, Base.show on schemas
```

### 5.2 Datums

```julia
Avro.encode(schema, x) -> Vector{UInt8};  Avro.encode!(enc_or_io, schema, x);  Avro.encode(x)  # schema = Avro.schema(typeof(x))
Avro.decode(schema, src, T=juliatype(schema); writer=nothing, limits=Limits())      # src: bytes/IO; writer → resolution
Avro.encodesingle(schema, x); Avro.decodesingle(src, store; reader=nothing, T=…)
Avro.tojson(schema, x; pretty=false) -> String;  Avro.fromjson(schema, json, T=…)
Avro.Branch(i, x)                                   # explicit union branch
Avro.Decimal{P,S}, Avro.Duration, Avro.BigDecimal, Avro.Record
```

### 5.3 Container files and tables

```julia
Avro.Table(src; scan=nothing, reader=nothing, ntasks=Threads.nthreads(), limits=Limits(), strict=false, mmap=true, bytes=:copy)
    # Tables.jl columns table; `Avro.metadata(t)`, `Avro.schema(t)`, `Avro.codec(t)`, `Avro.sync(t)`, `length`, `Tables.partitions` (per block)
Avro.Rows(src; T=nothing, reader=nothing, limits, strict, mmap)
    # streaming row iterator (one decompressed block resident); Tables.rows/schema; rows are NamedTuple/Avro.Record/T
Avro.Reader(src; limits, strict, mmap)      # block-level: header, `eachblock(r)` → (count, bytes), `close`
Avro.Writer(io_or_path, schema; codec=:null, level=nothing, metadata=Dict(), sync=nothing, block_bytes=64*1024, append=false)
    push!(w, row); Base.write(w, rows); flush(w); close(w)
Avro.write(dst, table; schema=nothing, codec=:null, level, metadata, block_bytes, name, namespace) -> dst
Avro.tobuffer(table; kw...) -> IOBuffer
Avro.codecs() -> available codec names
```

### 5.4 Protocols (Phase 6)

`Avro.parseprotocol`, `Avro.Protocol`, `Avro.Message`, `Avro.md5`, `Avro.frame`/`unframe`,
`Avro.encoderequest`/`decoderequest`/`encoderesponse`/`decoderesponse`, `Avro.Requestor`, `Avro.Responder`.

### 5.5 Deprecated 1.x shims (`src/deprecated.jl`, removed in 3.0)

`Avro.readtable(src; kw...) → Avro.Table`; `Avro.writetable(dst, tbl; compress=:zstd → codec=:zstandard)`;
`Avro.read(src, T_or_schema) → Avro.decode`; `Avro.write(x; schema) → Avro.encode`
(only the one-positional-argument datum form; `Avro.write(dst, table)` is the new container writer,
and `Avro.write(io, x)` where `x` is not a table raises a clear error pointing at `encode!`);
`Avro.parseschema` and `Avro.tobuffer` keep their names. All shims call `Base.depwarn`.

---

## 6. Tables.jl integration and `Tables.Scan` pushdown

* `Avro.Table` is a column table: `Tables.columnaccess`, `Tables.columns`, `Tables.schema` (names and
  Julia eltypes from §4.6), `Tables.partitions` (one `Avro.Table` per block, materialised lazily),
  `DataAPI.metadata` for file metadata (`avro.schema`, `avro.codec`, user keys). Columns are plain
  `Vector`s so downstream copies are not needed.
* `Avro.Rows` is a row table (`Tables.rowaccess`, `Tables.rows`, `Tables.schema`, `Base.IteratorSize`
  `SizeUnknown` for `IO`, `HasLength` when the block pre-scan is possible); `Tables.partitions(rows)`
  yields per-block `Avro.Table`s so `Avro.write(dst, Avro.Rows(src))` streams with bounded memory.
* `Avro.write` accepts any Tables.jl source (row or column access) and honours `Tables.partitions`.
* `Tables.Scan` pushdown (`Avro.Table(src; scan=Tables.Scan(...))`, against Tables.jl `jq/scan` @
  `df4e68c15c874079521d9d4ce4be67dce4345a31`, the branch Arrow.jl 3.0 already targets):
  * **select / Not / All / Regex / rename**: bound with `Tables.bind`; unselected fields are skipped
    (`skip` plans; sized array/map blocks are skipped by byte size); selection order = output order.
  * **filter**: per block, pass 1 decodes only the filter columns and records each row's byte offset;
    `Tables.filtermask` on those columns (exact `Tables.scan` semantics incl. `missing` and
    `validate=false`); pass 2 decodes the selected columns for qualifying rows only.
    `OpNode`s and column-to-column comparisons go to the residual.
  * **offset / limit**: whole blocks are skipped by header `count` before decoding when no filter is
    present; decoding stops at `limit` (the parallel path bounds the block range first).
  * **type overrides** (`ref => Type`): decoded directly when the override is a known widening
    (`int→Int64`, `float→Float64`, …), otherwise applied by the residual with `Tables.scan`'s exact rules.
  * Residual: `Tables.scan(table, Tables.Scan(scan; select=nothing, filter=residual, limit=nothing,
    offset=0))`; `Tables.describe(scan, residual)` works.
  * Gate: identical output to `Tables.scan(Avro.Table(src), scan)` for a generated matrix of scans
    (selection × filter × limit/offset × validate), including on multi-block files.
* Because `Tables.Scan` is unreleased, `Project.toml` uses `[sources]` for Tables exactly as Arrow 3.0
  does during development; **registration of 2.0.0 is gated on a Tables.jl release containing `Scan`**
  (or the `scan` keyword ships in 2.1 — decision recorded at release time, §12).

---

## 7. Compatibility and migration policy

* Version: **2.0.0** (SemVer-breaking). Julia ≥ 1.10 (LTS). Registered reverse dependencies: none
  (checked in General). Open issues/PRs are closed by this release (listed in §2.2).
* Breaking changes, each with a migration line in `CHANGELOG.md` and `docs/src/migration.md`:
  StructTypes → StructUtils customisation; `Avro.Record{names,T,N}`/`Avro.Enum{names}`/`Avro.Array`
  removed; enums decode as `Symbol`; `Avro.Duration` fields are `UInt32`; `Avro.Decimal` byte order and
  parameters; `compress=:zstd` → `codec=:zstandard` (shim maps it); `DateTime` derives
  `timestamp-millis` instead of `local-timestamp-millis`; `Avro.write(io, x)` with a non-table `x` is
  no longer the datum writer; `readtable` returns columns, not lazy records.
* File compatibility with 1.x writers: `zstd` codec name read (never written); null-codec trailing
  bytes tolerated with a warning (§4.9); nothing else 1.x wrote is non-conformant.
* Deprecation shims for one major cycle; removal in 3.0.
* Behavioural guarantees stated in docs: decode never crashes on malformed input (bounded by `Limits`),
  written files are readable by Apache Java 1.12 and fastavro (CI-enforced), thread-safety contract
  (§4.14).

---

## 8. Interoperability and conformance strategy

* **Vendored Apache fixtures** (`test/fixtures/apache/`, Apache-2.0 with LICENSE/NOTICE, provenance
  pinned to the commit above): `schema-tests.txt` (PCF + CRC-64 fingerprints, 30+ cases — parsed and
  executed), `interop.avsc`, `weather{,-deflate,-snappy,-zstd,-sorted}.avro`, `weather.avsc`,
  `weather.json`, `syncInMeta.avro`, `schemas/simple`, `schemas/withUnion`, `messageV1`
  (single-object bytes + schema), `interop/rpc/*` request/response bytes with `simple.avpr`,
  `reserved.avsc`, `specialtypes`/`schemaevolution` derived schemas, `TestRecordWithLogicalTypes.avsc`.
  `test.avro12` is an obsolete pre-1.3 format (Java: "Not an Avro data file") and is excluded.
* **Generated multi-codec corpus** (`test/fixtures/generated/`): the bench schema and the interop schema
  written by Java `random`/`fromjson` and recodec'd by Java (`deflate`, `snappy`, `bzip2`, `zstandard`)
  and by fastavro (`xz`, which the Java tools jar lacks), plus Java-written logical-type records
  (decimal bytes/fixed, uuid string/fixed, date, time-*, timestamp-*, local-timestamp-*, duration) and
  schema-evolution pairs. Generation script `test/fixtures/generate.sh` is checked in; outputs are small
  (< 1 MB total) and committed so CI needs no Java for the core suite.
* **Live differential tests** (`test/interop/`, run when `java` or `AVRO_TOOLS_JAR` and/or the Python
  venv with fastavro are available; a dedicated CI job on ubuntu installs both): for a schema matrix
  (primitives, nested records, recursive LongList, all logical types, unions, maps/arrays of everything,
  wide record, empty record): (1) Julia-written container files read by Java `tojson` and fastavro and
  compared against Julia's `tojson`; (2) Java `random` data decoded by Julia, re-encoded, and byte-compared
  after Java `recodec`-normalisation; (3) `fragtojson`/`jsontofrag` for raw datums; (4) `canonical` and
  `fingerprint` outputs compared for every schema; (5) schema-resolution pairs read by both.
* **Real-world corpora**: representative public Avro files (Kafka-Connect style envelopes, Hive/Spark
  exports with decimals and timestamps, Debezium-like nested unions) are reproduced from schemas in the
  generated corpus rather than vendored wholesale; any additional corpus the maintainer supplies is
  added to `test/fixtures/external/` behind an environment variable.

---

## 9. Testing strategy (deterministic; `Random.seed!` fixed per file)

1. **Unit**: every primitive read/write/skip incl. boundary values (`typemin/typemax`, −0.0, NaN payloads,
   10-byte varints), every schema constraint in §3 (positive and negative), every §4.6 mapping.
2. **Spec examples**: the encoding tables and examples from the specification text are literal tests
   (`36 06 66 6f 6f`, `04 06 36 00`, `02 02 61`, the `Example` fullname schema, the LongList schema, the
   Helsinki timestamp example).
3. **Conformance**: §8 fixtures; `schema-tests.txt` harness; messageV1; rpc interop bytes.
4. **Round-trip properties**: a bounded random schema generator (all kinds, recursion, logical types,
   unions, aliases/defaults) with a matching random value generator; `decode(encode(x)) == x`
   (with `isequal` for `missing`/NaN), `fromjson(tojson(x)) == x`, `parseschema(json(s)) == s`,
   `canonical(parseschema(canonical(s))) == canonical(s)`, resolved decode with `reader == writer`
   equals plain decode; columnar `Avro.Table` equals `Tables.columntable(Avro.Rows)`.
5. **Schema evolution**: a table-driven matrix of (writer, reader) pairs for every resolution rule and
   every failure rule; Java-generated expected outputs for the positive cases.
6. **Differential**: §8 live tests.
7. **Mutation / fuzz**: for every fixture and generated datum, 10k seeded mutations (bit flips, byte
   insert/delete, truncation at every offset for small inputs, varint lengthening, count/size
   substitution with extreme values) must yield `AvroError` or a valid decode — never another exception
   type, crash, hang (per-case time budget), or allocation above a bound. A longer loop runs under
   `AVRO_FUZZ_ITERATIONS`.
8. **Truncation / corruption of containers**: header truncation at every byte, sync mismatch, block size
   larger than the file, wrong CRC for snappy, decompression bombs (deflate/zstd of zeros) against
   `max_block_bytes`, trailing garbage, 1.x-style cushion blocks (warn vs strict).
9. **Resource limits**: each `Limits` field has a test that trips it quickly (< 10 ms, < 1 MB allocated)
   and one that passes just under it; `max_depth` verified inside `Threads.@spawn`.
10. **Concurrency**: multi-block files decoded with `ntasks ∈ {1,2,8}` produce identical tables; writer
    flush/close ordering; `Rows` consumed across tasks by the partitions API.
11. **Allocation budgets**: `@allocated` tests (after warm-up) for primitive decode (0), record decode
    into a NamedTuple of isbits fields (0), column decode per row of isbits fields (0), string row
    (1 per string).
12. **Quality gates**: Aqua (ambiguities, unbound type parameters, undefined exports, piracy, stale
    deps, compat bounds, `project_extras`), JET (`report_package` zero errors; `@test_opt` on hot
    decode/encode paths), docstring coverage for every public name, Documenter doctests.
13. **1.x regression port**: the existing `runtests.jl` cases (all round-trip `cases`, the compression
    loop, the dictcolumntable case) re-expressed through the shims and the new API.

---

## 10. Benchmarks and performance targets

### 10.1 Baselines (Apple Silicon, Julia 1.12.6, 1 thread, 1M rows `{id:long, x:double, name:string, flag:boolean}`, 23.1 MB uncompressed)

| Implementation | Write (null) | Write (zstd) | Read to materialised rows/columns |
|---|---|---|---|
| Avro.jl 1.1.2 | 1.02 s | 1.24 s | 2.90 s index + 3.95 s columns = 6.85 s |
| fastavro 1.12.2 (Cython) | 0.65 s | 0.78 s | 0.50 s (dict rows) |
| Java avro-tools 1.12.2 | — | — | `count` 0.67 s, `tojson` 0.91 s (JVM startup included) |

### 10.2 Targets (acceptance for Phase 4/5; measured by `benchmarks/` with Chairmarks; numbers recorded in `docs/src/benchmarks.md`)

| Metric | Must | Stretch |
|---|---|---|
| `Avro.write` 1M rows null codec, 1 thread | ≥ 4× 1.1.2 (≤ 0.25 s, ≥ 90 MB/s) | ≤ 0.15 s |
| `Avro.Table` 1M rows null codec, 1 thread | ≤ 0.30 s (≥ 3M rows/s; ≥ fastavro) | ≤ 0.20 s |
| `Avro.Table` 1M rows, 8 threads | ≥ 4× single-thread | ≥ 6× |
| zstandard/deflate/snappy read overhead | codec-bound (within 1.3× of raw `transcode` of the same bytes) | — |
| Single record decode into NamedTuple (3 fields) | ≤ 3 allocations (strings only), ≤ 150 ns | — |
| Single record encode | ≤ 1 allocation (result vector) | 0 with `encode!` |
| `parseschema(interop.avsc)` | ≤ 100 µs, ≤ 200 allocations | — |
| Projection `select=(:id,)` on the 4-column file | ≥ 2× faster than full decode | — |
| Package load time (`@time using Avro`) | ≤ 0.5 s (precompiled) | ≤ 0.3 s |
| Time-to-first-table on a fresh session | ≤ 1.5 s | — |

Baselines are re-measured on the same machine in the same session as the candidate (fairness: same
Julia, same data, same codec levels; fastavro timed in-process excluding interpreter start; Java timed
with `count`/`tojson` and JVM start-up stated separately).

---

## 11. Engineering deliverables

* **Package metadata**: `Project.toml` version `2.0.0-DEV`; deps `JSON` (1.7), `StructUtils` (2.8),
  `Tables` (1.13 via `[sources]` until released), `CodecZlib` (0.7), `CodecZstd` (0.8), `Snappy` (0.4),
  `TranscodingStreams` (0.11), `MD5` (0.2), stdlibs `Dates`, `UUIDs`, `Mmap`, `SHA`, `Random`;
  `PrecompileTools` (1); weak deps `CodecBzip2` (0.8), `CodecXz` (0.7); `[compat]` bounds for everything
  incl. `julia = "1.10"`; `[extras]`/`test/Project.toml` with `Aqua`, `JET`, `Test`, `DataFrames`-free
  (Tables only). `SentinelArrays`, `JSON3`, `StructTypes` dropped.
* **Licensing**: MIT (unchanged) for the package; `test/fixtures/apache/LICENSE` (Apache-2.0) + `NOTICE`
  attribution in `README.md`; the specification text is referenced, not vendored.
* **Docs** (Documenter): `index.md` (quick start: write a table, read it back, Scan, streaming rows),
  `manual/schemas.md`, `manual/encoding.md`, `manual/container.md`, `manual/tables.md`,
  `manual/evolution.md`, `manual/logicaltypes.md`, `manual/singleobject.md`, `manual/protocols.md`,
  `manual/limits-and-security.md`, `manual/performance.md`, `migration.md`, `benchmarks.md`,
  `reference.md` (autodocs). `examples/`: CSV→Avro→Arrow pipeline, Kafka-style single-object producer/
  consumer with a `SchemaCache`, schema evolution walkthrough, struct mapping with StructUtils.
* **CHANGELOG.md** (Keep-a-Changelog style) with the 2.0.0 entry.
* **CI** (`.github/workflows/ci.yml`): Julia `1.10`, `1.11`, `1`, `pre` on ubuntu/macos/windows with
  `JULIA_NUM_THREADS ∈ {1, 4}`; `nightly` allowed-to-fail; jobs for docs build, Aqua+JET, interop
  (ubuntu: Temurin 21 + `avro-tools` jar from Maven + `pip install fastavro`), and a non-blocking
  `juliac --trim` smoke compile of a reader program on the newest release; `TagBot`, `CompatHelper`.
  Trigger on `push: branches: ['**']` plus `pull_request` with a same-repo duplicate guard.
* **Precompile**: PrecompileTools workload covering schema parse/print/canonical/fingerprint, encode/
  decode of a representative NamedTuple and struct, container round trip with null/deflate/zstandard/
  snappy, `Avro.Table` with a `Scan`, `Avro.Rows`, single-object, JSON encoding; budget ≤ 15 s
  precompile, ≤ 0.5 s load.
* **Trim**: no `eval`/`@generated` on runtime data, no `Symbol`-to-function lookups; `test/trim/` smoke.
* **Code style**: AGENTS.md rules (explicit `return`, guard clauses, `T[]`, `@atomic`, `errormonitor`,
  small functions, whitespace discipline); no formatter enforcement.

---

## 12. Phased milestones and gates

Each phase lands as one or more small local commits with tests; the branch stays green at every phase.

| Phase | Scope | Acceptance gate |
|---|---|---|
| **0 — Foundation** | branch, `Project.toml` 2.0.0-DEV + deps, `test/Project.toml`, CI skeleton, vendored fixtures + licence, `errors.jl`, `limits.jl`, benchmark harness with 1.1.2/fastavro/Java baselines recorded | `Pkg.test` skeleton green on 1.10 and 1.12; fixtures licensed; baseline numbers committed |
| **1 — Schema model** | names, parser, validator, printer, equality, canonical form, fingerprints, logical types, Julia-type mapping (`Avro.schema`/`juliatype`) | `schema-tests.txt` 100%; all §3 constraints tested; `canonical`/`fingerprint` equal to Java for ≥ 50 schemas; Aqua+JET clean |
| **2 — Binary core** | Decoder/Encoder, read/write plans (generic + typed + columnar), logical values, limits, JSON encoding, single-object | spec-example tests; round-trip properties; 1.x round-trip cases ported; fuzz 100k mutations clean; allocation budgets met; messageV1 decodes; Java `fragtojson/jsontofrag` differential |
| **3 — Resolution** | `resolve`, resolving plans, aliases, defaults, enum defaults, union rules, decimal rule | resolution matrix (positive/negative) with Java-generated expectations; single-object + `SchemaStore` with reader schema |
| **4 — Containers & Tables** | Reader/Writer/`Avro.write`, codecs (+ extensions), parallel decode, `Avro.Table`, `Avro.Rows`, partitions, metadata, 1.x-file tolerance, `Tables.Scan` pushdown | Apache corpus reads (incl. snappy/zstd); Java + fastavro read Julia files for every codec; 1.x files read; truncation/corruption/bomb tests; threads tests; Scan equivalence matrix; performance "must" targets |
| **5 — Release engineering** | shims, docs, examples, changelog, precompile workload, trim smoke, benchmarks doc, README, CI matrix live | docs build without warnings; `@time using Avro` budget; full matrix green locally on 1.10/1.11/1.12 (+1.13-rc if installed); Aqua/JET; interop job green |
| **6 — Protocols (optional)** | `parseprotocol`, handshake, framing, calls, Requestor/Responder | rpc interop fixtures decode/encode byte-exact; in-process echo/add/hello round trips |

**PR-ready** (the state this work will stop in; nothing is pushed): phases 0–5 complete, all gates met
locally, status record (§15) up to date, commit history of small logical commits on `jq/v2-rewrite`.
**Release-ready** (requires maintainer actions outside this task): CI matrix green on GitHub; Tables.jl
release with `Scan` (or the `scan` keyword moved to 2.1 and the `[sources]` entry removed); docs deployed;
`CHANGELOG` dated; version set to `2.0.0`; registration.

---

## 13. Correctness, security, performance, allocation, streaming, concurrency goals (summary)

* Correctness: every spec rule in §3 has a test; all Apache fixtures pass; Java and fastavro read every
  file we write; we read every file they write (all six codecs).
* Security: no `@inbounds` without a proven bound; no allocation sized by untrusted counts; bounded
  recursion, decompression, schema size; all failures are `AvroError`s; fuzz-clean.
* Performance: §10.2 targets; plans compiled per schema; zero-allocation primitives; column-major
  materialisation; projection skips; block-parallel decode.
* Streaming: `Rows`, `Writer`, `IO` sources with one block resident; partitions for pipelines.
* Concurrency: §4.14 contract; no global mutable state; `errormonitor`ed tasks; ordered results.

---

## 14. Decisions made without user direction, and open risks

Decisions (reviewers may challenge any; each has the rationale):

1. `missing` remains the Julia value of `null` (Tables/DataFrames convention); `nothing` encodes as null.
2. Enums decode to `Symbol` (cheap, interned, readable; typed API supports `Base.Enum`).
3. Nested records decode to `NamedTuple` up to 32 fields and `Avro.Record` beyond (compile-cost bound).
4. `DateTime` derives `timestamp-millis` (interop) — a change from 1.x's `local-timestamp-millis`.
5. Decimal values are `Int128`-backed up to precision 38 with a `BigInt` slow path beyond.
6. Codec dependency split: `deflate`/`snappy`/`zstandard` hard, `bzip2`/`xz` extensions.
7. 1.x null-codec files with trailing block bytes are accepted with a warning; `strict=true` rejects.
8. `Avro.write(dst, table)` is the container writer; datum writing moves to `encode`/`encode!`.
9. `Tables.Scan` support developed against the `jq/scan` branch with registration gated on its release.
10. Default block size 64 KiB (Java's `syncInterval` default), positive-count array/map blocks by default.
11. Protocol/RPC core is Phase 6 (optional for 2.0.0); HTTP transport deferred to 2.x.
12. `Limits` defaults (§4.4) chosen to reject obvious bombs while admitting every file in the corpora.

Intentionally unresolved risks:

* The 32-field unrolling threshold and the resulting compile latency on many-schema workloads are to be
  measured in Phase 2; the fallback (lower threshold, or dynamic-only plans) is pre-agreed.
* Worker-task stack depth vs `max_depth=256` is to be measured on all three OSes in Phase 2.
* Tables.jl `Scan` release timing is outside this repository.
* `Avro.Writer(append=true)` and writer-side parallel compression are stretch items that may slip to 2.x
  without affecting the release gates.

Review log: (appended per round)

---

## 15. Status record (maintained through implementation)

* Assumptions: local-only work; no pushes/PRs/tags/registration; CSV/Arrow/Parquet checkouts untouched;
  network used only for specs, dependencies, and interop tools.
* Commands and results: recorded in `STATUS.md` in the worktree as phases complete (exact commands,
  Julia versions, pass/fail counts, benchmark numbers).
* Current state: **plan under review; no production code changed yet.**

---

## Appendix A — Probe scripts and raw results

Kept in the scratchpad of the authoring session and summarised in §2.2; the scripts are re-created as
tests in Phases 2 and 4 (`test/malformed.jl`, `test/interop/`).

## Appendix B — API sketch

```julia
using Avro, Tables

sch = Avro.parseschema("""{"type":"record","name":"Weather","namespace":"test","fields":[
  {"name":"station","type":"string"},{"name":"time","type":"long"},{"name":"temp","type":"int"}]}""")
Avro.fingerprint(sch)                       # 0x… CRC-64-AVRO of the canonical form
bytes = Avro.encode(sch, (station="011990-99999", time=-619524000000, temp=0))
Avro.decode(sch, bytes)                     # (station = "011990-99999", time = -619524000000, temp = 0)
Avro.decode(sch, bytes, MyWeather)          # typed, via StructUtils

Avro.write("w.avro", table; codec=:zstandard, metadata=Dict("source"=>b"sensor"))
t = Avro.Table("w.avro"; scan=Tables.Scan(select=(:station, :temp), filter=Tables.col(:temp) > 0))
for row in Avro.Rows("w.avro")              # streaming, one block resident
    row.temp
end
w = Avro.Writer("out.avro", sch; codec=:snappy); push!(w, row); close(w)

new = Avro.parseschema(...)                 # reader schema with a new defaulted field
Avro.Table("w.avro"; reader=new)            # resolved decode
store = Avro.SchemaCache(); Avro.register!(store, sch)
msg = Avro.encodesingle(sch, row); Avro.decodesingle(msg, store)
```
