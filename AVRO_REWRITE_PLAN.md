# Avro.jl 2.0 — Audit and Rewrite Plan

Status: DRAFT v4 (revised after Codex review rounds 1–3; see §14 review log and `reviews/response-N.md`).
Date: 2026-08-21/22. Repository: `JuliaData/Avro.jl`, local checkout `/Users/jacob.quinn/.julia/dev/Avro`,
branch `jq/v2-rewrite` forked from `main` @ `0c7be10db6d83fd20806a8eceec9276a7aa8e21d` (v1.1.2, registered).
Specification source pinned for this plan: `apache/avro` `main` @ `326950f40c1172f7564c757b0e51c39883721083`
(`doc/content/en/docs/++version++/Specification/_index.md`, 1.13.0-SNAPSHOT text, 2026-08-18), plus the
Apache shared test data under `share/test/` at the same commit. Reference implementations pinned for
differential testing: Apache Java `avro-tools` 1.12.2 (Maven Central jar, SHA-256
`6220e8bc089aaf917cdad4cd358bd651fc0394c0e5ddb8b36da402012c294a68`), Python `fastavro==1.12.2`,
`cramjam==2.11.0`, `avro==1.12.2`. Where the 1.13-SNAPSHOT text describes behaviour the 1.12.2 tools
lack, the spec text is authoritative and the capability matrix in §8.4 records which oracle covers what.

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
Apache corpus, the Apache Java tools, fastavro, and hand-built malformed inputs; the round-1 reviewer
re-verified it (15 rows confirmed, 4 corrected in §2.2, 1 destructive row source-supported but not
re-run). Headline findings:

* **Interop is broken in both directions.** Every non-empty uncompressed container file 1.1.2 writes
  (one with at least one data block) is rejected by Apache Java ("Block read partially"): the null-codec
  block carries the uninitialised 5% sizing cushion. Its zstd files name the codec `zstd` (spec:
  `zstandard`) and are rejected by Java and fastavro. Its decimals are written native-endian into a
  16-byte fixed (spec: big-endian two's complement). Reading: the official `weather-zstd.avro`
  **segfaults** the process; `weather-snappy.avro` returns garbage silently; logical types written by
  Java decode at wrong offsets because the container reader discards the writer schema and regenerates a
  lossy one from Julia types; a `["string","int"]` union (Apache `withUnion/data.avro`) throws a
  `MethodError` because union branches are mapped through Julia's canonical `Union` order instead of the
  schema's branch order.
* **Untrusted input is unsafe.** Truncated or over-long varints silently decode as `0`; `fixed` reads past
  the buffer through `@inbounds`; a negative length → `OutOfMemoryError`; a 7-byte array header requests
  a ~8 TiB allocation.
* **Schema handling is permissive and incomplete.** Duplicate field names, invalid names and enum symbols,
  duplicate union branches, and mismatched defaults are accepted; nested unions fail with a pathless JSON
  error; no canonical form, fingerprints, schema resolution, single-object encoding, JSON encoding, sort
  order; `timestamp-nanos`/`big-decimal` silently fall back to their underlying types and fixed `uuid`
  is decoded with the string path.
* **Performance is an order of magnitude off.** ~20 MB/s write and ~7 s to materialise a 1M-row,
  4-column file versus fastavro's 0.65 s write / 0.50 s read and Java's 49 ms warm in-JVM decode (same
  machine, single thread; §10.1).

The plan (§4–§12) replaces the core with a schema-compiled encoder/decoder over bounds-checked byte
buffers with cumulative, input-proportional resource budgets, a faithful, recursively frozen schema model
(names, aliases, defaults, logical types, canonical form, fingerprints, resolution, sort order), a
strict, streaming, parallel container reader/writer for any root schema, a columnar `Avro.Table`, a
row/datum-streaming `Avro.Rows`, a StructUtils-based typed API, single-object encoding, JSON encoding,
and `Tables.Scan` pushdown that ships only against a registered Tables release, gated by conformance
against Apache Java, fastavro, and avro-python. RPC, big-decimal, append mode, writer-side parallel
compression, and borrowed byte views are deferred to 2.x with their requirements recorded. The release
is **Avro.jl 2.0.0** (breaking; §7 gives the migration policy and the catalogue of 1.x data defects).

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
| Julia-written `jl.avro` (null codec, one block) → Java `tojson` | **"Block read partially, the data may be corrupt"** | whole sizing cushion emitted as block data (`src/tables.jl:83-116`) | interop |
| Julia-written `jl-zstd.avro` → Java / fastavro | "Unrecognized codec: zstd" | `String(compress)` written as codec name (`src/tables.jl:60-65`) | interop |
| Julia-written `jl-deflate.avro` → Java / fastavro | read correctly | — | — |
| `Avro.read([0x01,0x61], String)` (length −1) | `OutOfMemoryError` | negative length reaches allocation (`src/types/binary.jl:244-247`) | DoS |
| `Avro.read([0x80,0x80], Int64)` (truncated varint) | returns `0` silently | loop returns accumulator at buffer end (`src/types/binary.jl:155-166`) | silent corruption |
| 11-byte varint | returns `0` silently | no byte-count/terminal-bit check (same) | silent corruption |
| `Avro.read([1,2], NTuple{4,UInt8})` | returned `(1,2,0,0)`; out-of-bounds read (values not guaranteed) | unchecked `@inbounds` (`src/types/fixed.jl:34-43`) | memory safety |
| union index 4 of a 2-branch union | `BoundsError` | direct indexing (`src/types/unions.jl:54-58`) | ungraceful |
| enum index out of range | datum accepted; `BoundsError` only at `show` | no validation (`src/types/enums.jl:45-47`, `:23-25`) | silent corruption |
| array count 2^40 (7 bytes of input) | direct request for ~8 TiB (`src/types/arrays.jl:61-72`); in the authoring session the process thrashed until killed after 10 min (source-supported; not re-run by the reviewer) | `Vector{Int}(undef, len)` from untrusted count | DoS |
| duplicate field names / `"9R"` name / `"a-b"` enum symbol / `["int","int"]` / `"default":"oops"` for int | all accepted | parsing delegates to JSON3 without validation (`src/utils.jl:68-70`) | spec |
| nested union `["null",["int","string"]]` | JSON3 parse error (`ExpectedOpeningQuoteChar`) | spec-invalid input rejected with a pathless low-level error | ungraceful |
| `timestamp-nanos`, `big-decimal` | silently fall back to `long`/`bytes` | no logical-type entries | spec gap |
| fixed `uuid` | parsed as `UUIDType("fixed","uuid")` but read/written with the **string** encoding | `src/types/logical.jl:53-73` | correctness |
| Wide `Tables.Schema` (issue #18) | **corrected by review:** a 10,001-column NamedTuple source works; the exact trigger is a `Tables.Schema{nothing,nothing}` (names/types stored in fields, which Tables produces above its type-parameter threshold or with `stored=true`) | `src/types/rows.jl:20-22` calls `fieldcount(Nothing)` | bug |
| Throughput (1M rows × {long,double,string,boolean}) | write 1.02–1.21 s; read-index 2.9–3.9 s + materialise 3.9 s; single-record read 1,376 **bytes** / 38 allocations (the draft-v1 figure mislabelled bytes as allocations) | two-pass sizing, closure dispatch, boxed positions | performance |

Open upstream issues map onto these: #15 (row-at-a-time read/write → §5.3 `Avro.Writer`/`Avro.Rows`),
#17 (buffer-too-small with string columns → two-pass sizing removed), #18 (stored `Tables.Schema`),
#6 (pre-written object files → §8 corpus), #13 (`signed(UInt8)` on Julia < 1.5 → moot, floor is 1.10),
PR #16 (row-wise API prototype → superseded by §5.3).

### 2.3 What stays, what goes

Keep (as ideas, re-implemented under the new core, with their tests ported):

* The public *shape* of the container API (`writetable`/`readtable`/`tobuffer`/`parseschema`) survives as
  deprecated shims for one major cycle (§7).
* `missing` as the Julia value of Avro `null` (ecosystem convention; `nothing` also encodes as null).
* `Date`/`Time`/`UUID` mappings; `Avro.Decimal` and `Avro.Duration` names (both redesigned: spec byte
  order, runtime scale, `UInt32` duration fields).
* The zigzag/varint arithmetic (correct as written), the `Tables.partitions` → blocks idea, the block
  pre-scan idea.
* The test corpus of Julia round-trip cases in `test/runtests.jl` (ported to the new API).

Replace:

* StructTypes/JSON3 schema model → recursively frozen schema graph with a real parser/validator (§4.2).
* `nbytes` two-pass writer and `Vector{UInt8}`-only, `@inbounds`, position-tuple decoders → growable
  encoder, bounds-checked decoder with budgets, compiled read/write plans (§4.3–§4.5).
* `Avro.Record{names,types,N}` lazy-field record, `Avro.Enum{names}`, `Avro.Array{T}` lazy vector →
  typed columnar `Avro.Table`, stable generic `Avro.Record`/`Avro.EnumValue`/`Avro.Fixed`/`Avro.UnionValue`,
  plain `Vector`s (§4.6).
* Global per-thread codec arrays (unsafe under task migration) → per-task codec instances (§4.9).
* `Tables.dictrowtable` implicit schema inference → explicit `Avro.inferschema` (§4.9).
* CI (Julia 1.5/1/nightly, ubuntu only, actions v1) → §11 matrix.

The legacy implementation is **removed at Phase 0** (clean break on the branch); the new test suite is
the suite from Phase 0 on, and the 1.x cases are ported as each feature lands (Phase 5 ports the rest
through the shims). "Green at every phase" means exactly that `Pkg.test()` of the new, phase-scoped suite
passes at the end of every phase on Julia 1.10 and 1.12 — it implies nothing about preserved 1.x
behaviour, feature completeness, merge readiness, or release readiness.

---

## 3. Specification surface assessment

"Required" ships in 2.0.0; "Deferred" is intentionally out of scope for 2.0.0 with the reason and the
requirements for later inclusion recorded.

| Spec area | Scope | Notes |
|---|---|---|
| Schema declaration, primitive & complex types, attributes as metadata | Required | unknown attributes preserved (`props`) and re-emitted; stripped by canonical form |
| Names, namespaces, fullname algorithm, define-before-use, uniqueness, reserved primitive names | Required | §4.2; the spec's `Example`/`Simple`/`a.full.Name` example is a test |
| Aliases (type and field; relative/qualified; any string) | Required | used in resolution; subject to uniqueness |
| Record field `default` (all types; union default = first **matching** branch; bytes/fixed code points 0–255), `order`, `doc` | Required | defaults validated at parse time; selected union branch retained |
| Enum `default` | Required | used in resolution |
| Union constraints (no immediate nesting, one unnamed type per kind) | Required | |
| "Fixing an invalid, but previously accepted, schema" | Required | `allow_invalid_names=true` (invalid simple names, namespaces, fullnames, field names, symbols — syntax only) and `allow_invalid_defaults=true` (keep invalid defaults as raw JSON) are accepted by `parseschema` **and every container entry point** (`Reader`/`Rows`/`Table`/`inspect`), so embedded writer schemas can be repaired by a reader schema with aliases/corrected defaults; structural and uniqueness rules always stay |
| Binary encoding, all types, blocked arrays/maps with negative counts and sizes | Required | reader accepts both block forms with exact size exhaustion; writer emits positive-count blocks |
| JSON encoding of datums | Required | full rule table §4.11; bounded like schema JSON |
| Single-object encoding (`C3 01` + CRC-64-AVRO LE + payload) | Required | with an ambiguity-safe `SchemaStore` (§4.10) |
| Sort order (values and encoded datums, record `order`, map error) | Required (Phase 3) | `Avro.compare` / `Avro.comparebytes`; Java-backed vectors (§8.2) |
| Object container files: header, metadata, blocks, sync, codecs `null`/`deflate`; **any root schema** | Required | strict by default (§4.9); `Avro.Rows`/`eachdatum` decode non-record roots |
| Codecs `snappy` (with CRC32), `zstandard` | Required | hard dependencies (Snappy.jl, CodecZstd) |
| Codecs `bzip2`, `xz` | Required, via package extensions | CodecBzip2/CodecXz as weak deps (present in the test environment); actionable error when absent |
| Schema resolution (all rules incl. promotions, reorder, defaults, enum default, unions, aliases, decimals) | Required | §4.7; spec-normative union first-match by default, Java mode opt-in |
| Parsing Canonical Form, fingerprints (CRC-64-AVRO, MD5, SHA-256) | Required | conformance file `schema-tests.txt` |
| Logical types: decimal (bytes/fixed), uuid (string/fixed), date, time-millis/micros, timestamp-millis/micros/nanos, local-timestamp-millis/micros/nanos, duration | Required | per-type contract §4.8; invalid logical types ignored per spec |
| Logical type `big-decimal` | **Deferred to 2.x** | the spec says scale is non-negative and refers to an undeclared precision, while Java 1.12.2 accepts negative scale (`1E+3` → scale −3); needs a recorded policy plus negative-scale, malformed-inner-payload, scale-bound and exact-inner-consumption vectors. Decoded as the underlying `bytes` with the logical attribute preserved |
| Protocol declaration (`.avpr`), messages, errors, MD5 | **Deferred to 2.x** | requires: distinct anonymous request-schema type, property preservation, a deterministic Java-compatible printer whose bytes are what MD5 hashes, one-way validation, ping. The package documents that it implements the Avro data format, not Avro RPC |
| Handshake, framing, call format; HTTP transport | **Deferred to 2.x** | requires real wire tests: `BOTH`/`CLIENT`/`NONE` handshakes and retry, multi-frame messages, invalid frame lengths, metadata, declared and system errors, ping, one-way, protocol evolution, and a live interchange with `avro-tools rpcsend/rpcreceive`. The `share/test/interop/rpc` files are OCF datum files, not wire captures, and are not a gate |
| Avro IDL (`.avdl`), Trevni, tethered MapReduce | Deferred | outside the data format |

---

## 4. Architecture

### 4.1 Principles

1. **Spec first, Java as tie-breaker only where the spec is silent.** Normative spec rules win over Java
   behaviour (union first-match, boolean bytes 0/1, unsigned byte order, decimal precision on decode);
   every deliberate interoperability extension beyond the spec (e.g. header-only files) is recorded in §14.
2. **Never trust bytes.** Every decode path is bounds-checked against an explicit end position with checked
   arithmetic; every length/count is validated before allocation; cumulative budgets bound both memory
   and **work as a function of input bytes consumed**; package-detected malformed content always surfaces
   as an `AvroError`.
3. **Stable generic values, opt-in specialisation.** Untrusted schemas never drive Julia compilation:
   the generic path uses dynamic plans and a finite, enumerated set of value types (§4.6). Specialised,
   unrolled code is generated only for a caller-supplied target type `T` or for that finite set.
4. **Stream by default, materialise on request.** Container reading is block-at-a-time with bounded
   in-flight memory; `Avro.Table` is the explicit materialisation and owns copies of everything; schema
   inference over a schema-less source is an explicit, bounded, materialising operation that returns the
   materialised data.
5. **No global mutable state**, with one documented exception: a process-wide atomic counter of field
   names the package has admitted into Julia's symbol table (§6), because `Tables.columnnames` must return
   `Symbol`s. Codec instances, buffers, budgets, and plans are owned by reader/writer objects or
   operations; hashes are computed at freeze time and stored in the immutable nodes. The logical-type
   set is closed in 2.0.
6. **Qualified API, zero exports.**

### 4.2 Schema model (`src/schema.jl`, `src/names.jl`, `src/logical.jl`, `src/canonical.jl`, `src/frozen.jl`)

```julia
abstract type Schema end
struct NullSchema <: Schema; props::Props; hash::UInt64; end        # Props = FrozenDict{String,FrozenJSON}; FrozenJSON = recursively
struct BooleanSchema <: Schema; props; hash; end                     # frozen JSON tree (FrozenDict / FrozenVector / immutable scalars)
struct IntSchema <: Schema; logical::Union{Nothing,LogicalType}; props; hash; end
struct LongSchema <: Schema; logical; props; hash; end
struct FloatSchema <: Schema; props; hash; end
struct DoubleSchema <: Schema; props; hash; end
struct BytesSchema <: Schema; logical; props; hash; end
struct StringSchema <: Schema; logical; props; hash; end
struct ArraySchema <: Schema; items::Schema; props; hash; end
struct MapSchema <: Schema; values::Schema; props; hash; end
struct UnionSchema <: Schema; branches::FrozenVector{Schema}; hash; end
struct FixedSchema <: Schema; name::FullName; aliases::FrozenVector{String}; doc; size::Int; logical; props; hash; end
struct EnumSchema <: Schema; name::FullName; aliases; doc; symbols::FrozenVector{String}; default::Union{Nothing,Int}; symbolindex::FrozenDict{String,Int}; props; hash; end
struct Field; name::String; schema::Schema; doc; default::Default; order::Order; aliases::FrozenVector{String}; props; end
struct RecordSchema <: Schema       # immutable struct; `fields`/`fieldindex`/`hash` are frozen containers / a Ref that the parser
    name::FullName; aliases; doc; iserror::Bool; props           # fills exactly once after registering the record (so
    fields::FrozenVector{Field}; fieldindex::FrozenDict{String,Int}; hash::FrozenRef{UInt64}   # self-references resolve), then freezes
end
struct FullName; name::String; namespace::String; end       # fullname(x) = isempty(ns) ? name : ns*"."*name
```

* **Enforced, transitive immutability.** Every schema node is an immutable `struct`. `FrozenVector`,
  `FrozenDict`, and `FrozenRef` are private containers with a `frozen` flag: the parser fills them and
  calls `freeze!` once (public constructors freeze immediately); after that every mutating method throws.
  `props` and default values are stored as **recursively frozen JSON trees** (`FrozenJSON`: frozen
  containers of immutable scalars), so no reachable object from a frozen schema is mutable. There is no
  identity side table: each node's `hash` is computed bottom-up at freeze time (recursive named-type
  references hash by fullname only) and stored in the node; `canonical(schema)` is computed on demand
  and not cached. The parser's mutable state (`ParseContext`) is private.
* **Equality and hashing.** `==` is structural and semantic: kind, fullname, fields (name, schema,
  default *value* and selected branch — not the JSON spelling — `order`, aliases, doc), symbols, enum
  default, size, logical type and attributes, and `props` compared as unordered maps of JSON values;
  recursion uses a visited set of `(a,b)` pairs so cyclic graphs terminate. `hash` is the stored digest
  of the same normalised content (props sorted by key, defaults hashed by value), so `==` and `hash`
  agree. `Avro.parsingequivalent(a, b)` compares Parsing Canonical Forms.
* Logical types are *attributes* of the underlying schema (`logical`), a closed set:
  `Decimal(precision, scale)` (scale defaults to 0 when absent), `UUIDLogical`, `DateLogical`,
  `TimeMillis`, `TimeMicros`, `TimestampMillis/Micros/Nanos`, `LocalTimestampMillis/Micros/Nanos`,
  `DurationLogical`, and `UnknownLogical(name)` (kept so the schema re-serialises faithfully;
  `big-decimal` is carried this way in 2.0). Invalid logical types (wrong underlying type,
  `precision ≤ 0`, `scale < 0`, `scale > precision`, precision exceeding `floor(log10(2^(8n−1) − 1))` for
  fixed size n, duration size ≠ 12, uuid fixed size ≠ 16) are dropped to the underlying type per spec
  with their attributes preserved in `props`.
* `Default` is `nothing` (absent) or `Some(DefaultValue(json::FrozenJSON, branch, span, valid::Bool))`:
  the frozen JSON tree of the default, the selected union branch (first branch that accepts the default,
  in declaration order), the exact source text span (for faithful re-serialisation), and whether it
  validated. Plans materialise the Julia default **fresh on every use** from the frozen tree (a plan may
  hold a private immutable prototype), charged to the operation's budget. Defaults never make a field
  optional when encoding. With `allow_invalid_defaults=true` an invalid default is kept with
  `valid=false`; using such a default during resolution is a `ResolutionError` unless the reader schema
  supplies a valid one.
* **Parsing.** `Avro.parseschema(src; allow_invalid_names=false, allow_invalid_defaults=false,
  limits=Limits())`:
  1. A lexical pre-scan (no allocation; handles strings/escapes) rejects inputs over `max_schema_bytes`
     or deeper than `max_schema_depth` *before* any parser recursion.
  2. The JSON is traversed with `JSON.lazy(...; duplicate_keys=:error)` (`applyobject`/`applyarray`),
     so recursion depth is ours, duplicate keys are errors, and value byte spans are available.
  3. A `ParseContext{namespace stack, named-type table, depth, counters}` validates every rule: name and
     namespace grammar (unless `allow_invalid_names`), fullname uniqueness, primitive names never
     redefined, define-before-use (depth-first, left-to-right), field/symbol uniqueness within scope,
     alias uniqueness against names and other aliases, union rules, enum default membership, fixed size
     ≥ 0, `order` values, default validity (including union branch selection and integer range checks:
     JSON integers that do not fit `Int32`/`Int64`, e.g. `BigInt`, are invalid defaults), and the schema
     limits `max_schema_nodes`, `max_fields`, `max_union_branches`, `max_enum_symbols`,
     `max_name_bytes`, `max_named_types`. Errors are `SchemaError`s carrying a JSON path.
  4. Named-type references resolve by fullname (qualified, or relative to the enclosing namespace);
     recursive references bind to the already-registered (field-less) record and are closed when its
     `fields` are filled and frozen.
* **Printing.** `Avro.json(schema; pretty=false)` / `JSON.json(schema)` emits spec JSON (first occurrence
  of a named type in full, later references by fullname, namespace attribute only when it differs from
  the enclosing one, custom `props` and exact default text included). `Avro.canonical(schema)`
  implements the seven PCF transformations; `Avro.fingerprint(schema; algorithm=:crc64avro|:md5|:sha256)`
  hashes the PCF bytes (CRC-64-AVRO in-house from the spec pseudo-code; MD5 via `MD5.jl`; SHA-256 via
  `SHA`).

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
  `Vector{UInt8}` with unit stride, which are decoded through `(parent, offset, length)`.
* **Primitives**: `readbool` (byte must be 0 or 1), `readint` (≤ 5 bytes, value range-checked into
  `Int32`), `readlong` (≤ 10 bytes, bit 64 overflow rejected), `readfloat`/`readdouble` (little-endian
  `reinterpret`; NaN payloads preserved bit-for-bit), `readlen` (non-negative, `≤ stop − pos + 1`),
  `readbytes` → copy, `readstring` → `String` validated as UTF-8 (`DataError` otherwise; also map keys),
  `readfixed(n)`. All arithmetic on positions/sizes uses checked operations; block counts equal to
  `typemin(Int64)` are rejected (negation overflow). Every failure throws a `DataError(msg, pos)`. No
  `@inbounds` anywhere that is not immediately preceded by an explicit range check on the same values.
* **Skip policy (documented guarantee).** `skip*` counterparts validate everything structural: varint
  length/overflow, boolean bytes, length bounds, enum and union indexes against the schema, sized-block
  exhaustion, nesting depth, and budgets. The single exception is that **skipped strings are not
  UTF-8-validated** (length-only skip); projection and resolution therefore accept a file whose only
  defect is invalid UTF-8 in a skipped string, and the documentation never describes projection as
  full-file validation. Acceptance-equivalence tests compare full decode, projection, resolution skips,
  and `comparebytes` on the malformed corpus with exactly this exception.
* **Sized blocks.** A negative array/map count is followed by a byte size; the block is decoded through a
  bounded child range `[pos, pos+size)` and must be **exactly exhausted** (`DataError` otherwise).
* **Trailing bytes and positions.** `Avro.decode(schema, bytes)` rejects trailing bytes; `Avro.decode(schema,
  bytes, pos::Int) -> (value, nextpos)` decodes one datum starting at `pos` and returns the consumed
  position (for concatenated datums and container iteration); single objects and `comparebytes` require
  exact consumption of their inputs.
* **IO sources.** The container layer reads blocks of known size into owned buffers. `Avro.decode(schema,
  io; limits)` reads at most `max_datum_bytes + 1` bytes and errors if the datum is larger or if bytes
  remain.
* **Encoder** is a growable `Vector{UInt8}` with `pos`, `ensureroom!` (checked growth), and write
  primitives (`writelong` = 10-byte unrolled varint, `writebytes`, `writestring`, `writefloat`, …).
  `Encoder` is re-usable (`reset!`) and its buffer can be handed to a codec or `IO` without copying. No
  pre-sizing pass. Encoding is bounded per datum by `max_datum_bytes` and per container block by
  `max_block_bytes` (never by a writer-lifetime ceiling), depth-limited (`max_depth`, so self-referential
  Julia values fail cleanly), and validates values against the schema (union branch acceptance, enum
  membership, fixed length, integer ranges, UTF-8 of strings produced from `codeunits`, decimal
  precision, time-of-day ranges, timestamp range for `DateTime` sources — Java raises on out-of-range
  nanosecond conversions, and so do we).

### 4.4 Limits and budgets (`src/limits.jl`)

```julia
Base.@kwdef struct Limits
    # per value
    max_depth::Int            = 1024          # nesting depth of values (recursive schemas); encode and decode
    max_bytes::Int            = 256 << 20     # one bytes/string/fixed value (256 MiB)
    max_datum_bytes::Int      = 256 << 20     # one encoded datum: Avro.decode from IO, single-object payloads, encode output per datum
    # per container block (both bounds enforced before any arithmetic: 0 ≤ count, 0 ≤ size)
    max_block_bytes::Int      = 256 << 20     # compressed and decompressed size of one block; encode output per block
    max_block_count::Int      = 1 << 30       # declared datums per block (also bounded by the work rule below)
    max_blocks::Int           = 1 << 31       # blocks per container operation
    # cumulative per top-level operation (decode/Table/Rows/Reader/comparebytes/fromjson); checked arithmetic
    max_total_bytes::Int      = default_total_bytes()   # estimated decoded output bytes + decompressed block bytes + materialised defaults
    max_total_values::Int     = 1 << 31       # decoded values, including zero-size ones (null, empty record, empty fixed)
    max_rows::Int             = 1 << 31       # container datums per operation
    max_values_per_byte::Int  = 64            # WORK RULE: values decoded so far ≤ max_values_per_byte × input bytes consumed so far + work_allowance
    work_allowance::Int       = 65_536        #   (input bytes = compressed block bytes read + decompressed bytes + raw datum bytes); bounds CPU time by input size
    # schema / metadata (enforced by the parser before allocation of the corresponding structure)
    max_schema_bytes::Int     = 16 << 20
    max_schema_depth::Int     = 256           # JSON nesting depth of a schema or datum JSON document
    max_schema_nodes::Int     = 1_000_000
    max_fields::Int           = 65_535        # per record
    max_union_branches::Int   = 1_024
    max_enum_symbols::Int     = 65_535
    max_name_bytes::Int       = 1_024         # any name, namespace, symbol, alias
    max_named_types::Int      = 10_000
    max_metadata_bytes::Int   = 16 << 20      # total OCF metadata (keys + values, including avro.schema)
    max_metadata_entries::Int = 10_000
    # concurrency
    max_inflight_blocks::Int  = 0             # 0 = ntasks; bounds decompressed buffers to max_inflight_blocks × max_block_bytes
end
default_total_bytes() = clamp(Sys.total_memory() ÷ 2, 256 << 20, 64 << 30)
# half of physical RAM, clamped to [256 MiB, 64 GiB]; deterministic per machine; on machines with < 512 MiB the
# floor exceeds half of RAM (documented). CI pins an explicit value in every test.
```

`Budget` is a mutable per-operation accumulator (values, output bytes, input bytes, rows, blocks) with
`@atomic` counters so parallel block decoders reserve **before** allocating; exceeding any field throws
`LimitError` naming the limit, the observed value, and the keyword to raise it. The **work rule** makes
CPU time proportional to input: a 23-byte block declaring `count = 2^30` zero-size datums fails at datum
`64 × 23 + 65_536` instead of running a billion iterations; a block's declared `count` is additionally
pre-checked against `max_values_per_byte × size + work_allowance` before decoding starts. The
output-bytes estimate charges `sizeof` for isbits values, `length + 32` for strings/bytes/fixed, and 64 +
element charges for containers and records. Collections are decoded incrementally: `sizehint!` is capped
at `min(count, 1024)`, growth is by `push!`, and the declared count is additionally bounded by
`remaining_bytes ÷ minsize(item schema)` when `minsize > 0`. `minsize(schema)` is the minimal encoded
size of a datum: computed by a memoised, cycle-safe fixed-point (an active recursion edge contributes 0;
unions take the minimum over branches plus the index byte; records sum fields; arrays/maps contribute 1;
tested on direct recursion, mutual recursion, nullable recursive unions, and a recursive schema with no
finite datum). Every corpus file in §8 decodes under the defaults, and CI runs the corpus once with
`max_total_bytes = 256 MiB` to prove the floor suffices.

### 4.5 Plans: schema-directed codecs (`src/plan_read.jl`, `src/plan_write.jl`)

* A **read plan** is a tree of plan nodes, one per schema node. The **generic** plan family is dynamic
  (`Vector{ReadPlan}` children, one dynamic dispatch per child behind a function barrier) and produces
  the finite value set of §4.6; it never specialises on schema shape. Named-type recursion is represented
  by `PlanRef` nodes: plans are built in two passes keyed by schema object identity (and by `(writer,
  reader)` identity pairs for resolving plans), so `LongList`-style schemas and recursive writer/reader
  pairs terminate.
* The **typed** plan family (`Avro.decode(schema, bytes, T)`, `Avro.Rows(src; T)`) specialises on a
  caller-supplied `T`: `RecordPlan{names, Ps<:Tuple}` unrolled via `ntuple`/`Val` (any width the caller
  chose), leaf plans per Julia type, recursion through the user's own recursive struct types.
* The **column** plan family (`Avro.Table`) specialises per column *element type* drawn from the
  enumerated set `E` of §4.6 (|E| is fixed at compile time of the package): each selected field owns a
  `ColumnBuilder{e}` for some `e ∈ E` holding a `Vector{e}` chunk; a row is decoded by iterating builders
  (tuple-unrolled for ≤ 32 columns, dynamic loop beyond — the threshold affects only code shape, not the
  result types). Unselected fields use `skip`.
* Write plans mirror read plans with value-extraction strategies for NamedTuples, StructUtils structs
  (§4.8), `Tables.AbstractRow`s (by column index), `AbstractDict`s, iterables, and the generic values.
* Plans are built by `Avro.plan(schema[, T])`; construction is pure and cheap (microseconds for the
  interop schema) and plans are owned by reader/writer objects — no global caches.
* **Compile-cost gate** (Phase 2, numeric): a warm-up decodes one schema per member of `E` (and per
  `ColumnBuilder{e}`); afterwards decoding 1,000 random heterogeneous schemas through the generic and
  column paths must create **zero new method instances** for Avro's decode functions (measured with
  `Base.specializations`/`methods` counts per Julia version, with a documented script), zero
  invalidations (SnoopCompileCore when available, otherwise informational), and bounded RSS growth
  (< 50 MB, `Sys.maxrss` delta). Native-code size is reported as informational only. The typed path is
  measured separately per `T`.

### 4.6 Julia value model (generic decoding, `Avro.juliatype(schema)`)

The **finite value set** `E` (closed; enumerated in `src/values.jl` and asserted by a test):

* Leaves `L` = {`Missing`, `Bool`, `Int32`, `Int64`, `Float32`, `Float64`, `Vector{UInt8}`, `String`,
  `Avro.Fixed`, `Avro.EnumValue`, `Avro.Decimal`, `Avro.WideDecimal`, `UUID`, `Date`, `Time`,
  `Avro.Timestamp{Millisecond}`, `Avro.Timestamp{Microsecond}`, `Avro.Timestamp{Nanosecond}`,
  `Avro.LocalTimestamp{Millisecond}`, `Avro.LocalTimestamp{Microsecond}`, `Avro.LocalTimestamp{Nanosecond}`,
  `Avro.Duration`}.
* Composites `C` = {`Avro.Record`, `Avro.UnionValue`} ∪ {`Vector{x}`, `Dict{String,x}` : x ∈ L ∪ {`Record`,
  `UnionValue`} ∪ {`Union{Missing,y}` : y ∈ L∖{Missing} ∪ {`Record`}}} ∪ {`Vector{Any}`, `Dict{String,Any}`}.
* `E` = L ∪ C ∪ {`Union{Missing, x}` : x ∈ (L ∪ C)∖{Missing}}.

| Avro | Julia (generic decode) | Notes |
|---|---|---|
| null | `Missing` (`missing`) | `nothing` also encodes as null |
| boolean / int / long / float / double | `Bool` / `Int32` / `Int64` / `Float32` / `Float64` | |
| bytes | `Vector{UInt8}` | always copied (no borrowed views in 2.0) |
| string | `String` | UTF-8 validated |
| fixed(N) | `Avro.Fixed` (named fixed schema reference + `Vector{UInt8}` of length N; `==` by fullname, size and bytes; `show` prints name and hex) | typed API accepts `NTuple{N,UInt8}`/`Vector{UInt8}` |
| enum | `Avro.EnumValue` (enum schema reference + `Int32` index; `String(x)`, `Symbol(x)`, `Int(x)`; `==` by fullname and symbol string; `show` prints the symbol) | typed API accepts `Base.Enum` subtypes, `Symbol`, `String` (validated); encoding under a different enum schema remaps by symbol, never by index |
| array | `Vector{e}` with `e = juliatype(items)` when that is in L ∪ {`Record`, `UnionValue`} ∪ {`Union{Missing,…}`} of those; `Vector{Any}` when items are themselves arrays or maps | one level of typed nesting |
| map | `Dict{String, e}` with the same rule | |
| union | **bare value** only for the two-branch nullable form `["null", T]` / `[T, "null"]`: `Union{Missing, juliatype(T)}`; **every other union** (including any three-or-more-branch union, unions without null, and ambiguous unions) decodes every value — the null branch included — as `Avro.UnionValue(index, value)` | closed set by construction; branch recovery on encode: (1) for bare nullable values, `missing` → the null branch, anything else → the other branch; (2) `UnionValue` is exact; (3) for plain Julia values encoded against any union, a branch whose generic representation type equals `typeof(value)` exactly, else the first branch that accepts the value by conversion |
| record | `Avro.Record` (schema reference + `Vector{Any}` values; `Tables.AbstractRow`; property access by name; `==` structural) | stable at every width and for recursive records |
| decimal(bytes/fixed) | `Avro.Decimal` (`Int128` unscaled, `Int32` scale — runtime fields) for precision ≤ 38; `Avro.WideDecimal` (`BigInt` unscaled, `Int32` scale) for larger precision (chosen per schema at plan time) | big-endian two's complement; minimal bytes for `bytes`, sign-extended to size for `fixed`; an empty `bytes` payload is rejected (Java raises `NumberFormatException`); absent `scale` = 0; **both encode and decode validate `digits(unscaled) ≤ precision`** (`EncodeError` / `DataError`; Java validates only on encode — recorded deviation in favour of the spec's "maximum precision") |
| uuid (string / fixed 16) | `UUIDs.UUID` | string form must be RFC-4122 `8-4-4-4-12` hex (`DataError` otherwise); fixed form is the 16 big-endian bytes; the representation (string vs fixed) is **not** carried by the value — round trips use the writer schema (§4.8) |
| date | `Dates.Date` | any `Int32` day count is representable |
| time-millis / time-micros | `Dates.Time` | exact (ns-resolution `Time`); decode range-checked to one day; **encode rejects non-aligned values** unless `Avro.truncate(x, P)`/`Avro.round(x, P)` or `Avro.Time{P}` is used (Java truncates silently — recorded deviation) |
| timestamp-millis / micros / nanos | `Avro.Timestamp{Millisecond|Microsecond|Nanosecond}` — exact `Int64` ticks since the Unix epoch, a global instant | `DateTime(x)` is an explicit, **range-checked** conversion raising `Avro.ConversionError` (never `DataError`, never wrap), lossy (floor) for sub-ms units; `Avro.Timestamp{P}(::DateTime)` exact and range-checked; `ZonedDateTime` via the TimeZones extension |
| local-timestamp-millis / micros / nanos | `Avro.LocalTimestamp{Millisecond|Microsecond|Nanosecond}` — exact `Int64` ticks | same conversion rules |
| duration | `Avro.Duration(months::UInt32, days::UInt32, millis::UInt32)` | little-endian unsigned |

`instants=:exact` (default, all of `Avro.decode`/`Rows`/`Table`/`decodesingle`) keeps the wrappers;
`instants=:datetime` converts every timestamp kind to `DateTime` (a `ConversionError` on out-of-range
values) with documented floor-to-ms loss for sub-ms units. The value model is identical for
`Avro.decode`, `Avro.Rows`, `Avro.Table` cells, defaults, JSON conversion, and sorting.

### 4.7 Schema resolution (`src/resolution.jl`)

`Avro.resolve(writer::Schema, reader::Schema; union_resolution=:spec) -> ResolvedSchema` implements
every rule of the "Schema Resolution" section, memoised on `(writer, reader)` identity pairs:

* Match by kind: arrays/maps recursively; enums, fixed (plus size), records by unqualified name *after*
  applying the reader's type aliases; primitives equal or promotable `int→long/float/double`,
  `long→float/double`, `float→double`, `string↔bytes` (`bytes→string` validates UTF-8 at decode time).
* Records: fields matched by name after reader field aliases; **a reader alias consumes the writer
  field** (a reader that also has a field with the writer's original name gets no value for it and must
  have a default); writer-only fields are skipped (skip policy §4.3); reader-only fields require a valid
  default (fresh copy per record); reader field order wins.
* Enums: symbol remap by name; writer symbols absent in the reader use the reader `default` or error.
* Unions: `union_resolution=:spec` (default, normative) selects **the first reader branch that matches
  the writer branch, where matching includes promotion** (writer `int` against reader `["long","int"]`
  selects `long`; against `["double","int"]` selects `double`). `union_resolution=:java` reproduces
  Apache Java's exact-match-first-then-promotion selection. Both policies are tested on
  `["long","int"]` and `["double","int"]` with **directly written expectations (branch index and result
  type)**; Java fixture expectations are labelled `:java`; fastavro is used as an oracle only where its
  result preserves branch identity (it returns `7 int` for either branch, so it cannot witness primitive
  branch selection). Reader-union-only and writer-union-only cases follow the same selection. Tags
  (`UnionValue`) are preserved through resolution.
* Logical types: two recognised `decimal`s match only if both precision and scale match
  (`ResolutionError` otherwise); a recognised logical type on one side and a plain underlying type on
  the other resolves as the underlying type and the **reader's interpretation wins**.
* Failures are `ResolutionError`s carrying both paths. The result is consumed by `Avro.plan` to produce a
  resolving read plan (writer-driven field order with `SkipPlan`/`DefaultPlan`/`PromotePlan`/
  `EnumRemapPlan`/`UnionRemapPlan` nodes).

### 4.8 Typed API and Julia type mapping (`src/types.jl`, StructUtils)

* `Avro.schema(T::Type)` derives a schema from a Julia type: the §4.6 table inverted, plus `Int8/16/
  UInt8/16 → int`, `UInt32/UInt64 → long` (range-checked on write), `Float16 → float`, `AbstractString`/
  `Symbol`/`Char → string`, `NTuple{N,UInt8} → fixed` named `fixed<N>` by default (a valid name; the
  enclosing record's namespace applies; override with `name=`), `Union{Missing,T} → ["null", T]`, other
  `Union`s → union in Julia's member order (documented), `Base.Enum` subtypes → enum, `DateTime →
  local-timestamp-millis` (a naive `DateTime` is a local timestamp; global instants are
  `Avro.Timestamp{P}` or `ZonedDateTime`), `Date → date`, `Time → time-micros`, `UUID → string uuid`,
  `Avro.Decimal` → no type-level schema (a decimal needs precision/scale: `schema=` is required), structs
  → records via StructUtils (`fieldnames`/`fieldtypes`; `@kwarg`/`@defaults` field defaults become Avro
  defaults when JSON-encodable), `Tables.Schema` → record (through the `names`/`types` accessors, which
  also serve stored schemas — fixes #18). Named-type naming: structs use `nameof(T)` with `namespace =
  string(parentmodule(T))`; `NamedTuple`/`Tables.Schema` records are named `Record` with nested
  anonymous records `Record_1`, `Record_2`, … in depth-first order, the same Julia type reusing its
  first definition; `name=`/`namespace=` keywords override.
* **Value-level `Avro.schema(x)`** (used by `Avro.encode(x)`): identity-bearing generic values return
  their instance schema (`Record`, `EnumValue`, `Fixed`); plain Julia values use `Avro.schema(typeof(x))`;
  a bare `UnionValue` (no enclosing union) and a `Decimal` raise an `ArgumentError` directing the caller
  to `Avro.encode(schema, x)`. Schema-free encoding is a convenience for *constructing* datums from
  Julia values; it is **not** a round-trip mechanism for decoded data — `UUID` (string vs fixed) and
  `Vector{UInt8}` (bytes) lose their source representation — and the documentation says so: round trips
  go through the writer schema (`Avro.write(dst, Avro.Rows(src))`, `Avro.schema(table)`).
* **Typed decoding** goes through a dedicated `Avro.AvroStyle <: StructUtils.StructStyle`. A plain-DTO
  fast route (direct positional construction from a typed plan) is used only when an eligibility check
  passes: `T` is a concrete struct or `NamedTuple`, no custom `StructUtils.make`/`lift`/`choosetype`
  methods apply for `AvroStyle`, no field tags affect construction, every field has a mapped schema, and
  any field defaults are static. Everything else takes the semantic route: generic decode then
  `StructUtils.make(AvroStyle(), T, generic)`. Both routes are tested against the pinned StructUtils
  version, and a test asserts the fast route is never taken when a custom hook exists.
* Logical-type contract (each row has tests for schema validation, binary boundaries, JSON/default form,
  resolution, native conversion, and oracle coverage; decimal vectors include empty payload (reject),
  `00`, `ff`, sign extension, maximum precision, one-digit overflow, absent scale):

| Logical type | Schema validation | Binary / value bounds | JSON & defaults | Oracle (Java 1.12.2 / fastavro / avro-py) |
|---|---|---|---|---|
| decimal | precision > 0; 0 ≤ scale ≤ precision (scale default 0); fixed-size bound | big-endian two's complement; non-empty; digits ≤ precision on encode and decode | bytes/fixed string form | Java `fromjson`/`tojson` and `DecimalConversion` vectors (Java: no decode-side precision check); fastavro `bytes-decimal`/`fixed-decimal`; avro-py decimal |
| uuid | string, or fixed size 16 | RFC-4122 text; 16 bytes | string / fixed string | Java both; fastavro string only |
| date | int | any Int32 | integer | all three |
| time-millis / micros | int / long | `0 ≤ v < 86_400_000` / `< 86_400_000_000` | integer | Java `TimeConversions` vectors (Java truncates sub-unit input); fastavro both; avro-py none |
| timestamp-millis/micros | long | any Int64 (exact wrapper) | integer | Java `TimeConversions` vectors; fastavro; avro-py |
| timestamp-nanos | long | any Int64; Java raises on out-of-range conversion | integer | Java vectors + spec vectors |
| local-timestamp-millis/micros | long | any Int64 | integer | Java vectors; fastavro |
| local-timestamp-nanos | long | any Int64 | integer | Java vectors + spec vectors |
| duration | fixed size 12 | three LE UInt32 | fixed string | Java (raw bytes) |

### 4.9 Container files (`src/container.jl`, `src/codecs.jl`)

* `Avro.Reader(src; limits, legacy=nothing, allow_invalid_names=false, allow_invalid_defaults=false,
  mmap=true)`: parses the header (magic; metadata map decoded with the spec's
  `{"type":"map","values":"bytes"}` schema under `max_metadata_bytes`/`entries`; sync marker;
  `avro.schema` parsed with §4.2 under `max_schema_bytes` and the repair options), selects the codec from
  `avro.codec` (`null`/absent, `deflate`, `snappy`, `bzip2`, `xz`, `zstandard`; unknown →
  `UnsupportedCodecError`), and iterates blocks **strictly**: `0 ≤ count ≤ max_block_count`, pre-checked
  against the work rule; `0 ≤ size ≤ max_block_bytes` and `size ≤` remaining bytes — all checked before
  any arithmetic (tests use −1 and `typemin(Int64)` for both); block index ≤ `max_blocks`; data; sync
  (mismatch → `DataError` with block index). **Codec contract:** every adapter must report a clean
  end-of-stream **and** full consumption of the compressed payload (a valid stream followed by extra
  bytes, or a truncated stream, is a `CodecError`; tests cover both cases for all five codecs);
  decompressed size is capped at `max_block_bytes` (streamed with a cap); exactly `count` datums must
  consume exactly the decompressed block (`DataError` otherwise); a partial trailing block →
  `DataError("truncated file")`. A header-only file is accepted and written as the zero-datum file (a
  deliberate Java-compatible extension of the spec's "one or more data blocks", recorded in §14);
  zero-count blocks are valid. **Any root schema** is supported: `eachblock(r)` → `(count, bytes)` where
  `bytes` is an **owned copy** that remains valid after iteration advances, and `eachdatum(r)` →
  schema-directed generic values; `Avro.Rows` yields `Avro.Record`s for record roots and §4.6 values
  otherwise (the Tables.jl row interface is provided only for record roots); `Avro.Table` requires a
  record root and raises a clear `ArgumentError` otherwise. Sources: file path (mmap by default,
  `mmap=false` reads into memory), `Vector{UInt8}`/views, `IO` (streaming: header + one block buffer
  resident), `IOBuffer` (its written bytes only).
* **Legacy mode.** `legacy=:avrojl1` enables exactly two *unambiguous* tolerances for files written by
  Avro.jl ≤ 1.1.2: `avro.codec == "zstd"` read as zstandard, and null-codec blocks with trailing bytes
  after `count` datums accepted (one `@warn` per source). The 1.x native-endian decimal defect is **never
  inferred**: reinterpreting fixed/bytes decimals little-endian requires the explicit keyword
  `decimal_byteorder=:little` (default `:big`). The deprecated `readtable` shim sets `legacy=:avrojl1`
  only. Strict mode is the default for every new API.
* **Ownership and lifetime.** `Avro.Table` copies all data: path sources are opened, mapped, decoded, and
  closed within the call; caller-owned `IO`/byte sources are left open and unreferenced afterwards.
  `Rows` and `Reader` hold a path-owned mapping/handle until `close` (idempotent; iteration after close
  throws); for caller-owned `IO` they only drop their reference on `close` and never close the caller's
  stream. Truncating a memory-mapped file while it is being read is undefined at the OS level (SIGBUS);
  `mmap=false` is the safe option for files that may change and is documented as such.
* **Parallel decoding** (`Avro.Table` only, in-memory/mmap sources, `ntasks > 1`): stage 1 pre-scans block
  headers (no decompression) into a block table with checked prefix sums under `max_rows`/`max_blocks`;
  stage 2 decodes blocks on worker tasks (bounded to `max_inflight_blocks`, `Threads.@spawn` +
  `errormonitor`, joined with `@sync`-style fetch) into **per-block column chunks**, reserving budget
  atomically before each allocation. **Deterministic failure:** a failing block records its index in an
  `@atomic` minimum; workers check the minimum between datums and abandon only blocks with a **higher**
  index — blocks with lower indices always run to completion or their own failure — so after all tasks
  settle the error of the lowest failing block index is rethrown regardless of scheduling. Stage 3
  assembles chunks into final columns in block order (one copy; the peak-memory test accounts for chunks
  plus the assembly copy). Direct disjoint-range writes are a later, measured optimisation. `IO` sources
  decode sequentially.
* `Avro.Writer(io_or_path, schema; codec=:null, level=nothing, metadata=Dict{String,Vector{UInt8}}(),
  sync=nothing, block_bytes=64*1024, atomic=true, limits=Limits())`: **option validation** — `sync` must
  be exactly 16 bytes and is copied (`nothing` → 16 bytes from `RandomDevice`); `block_bytes > 0`;
  `level` validated against the codec's documented range (`nothing` → codec default); **every
  user-supplied `avro.*` metadata key is rejected** (`avro.schema` and `avro.codec` come only from the
  constructor arguments). It writes the header, buffers encoded datums in an `Encoder` (per-datum
  `max_datum_bytes`, per-block `max_block_bytes`), emits a block when `block_bytes` is reached or on
  `flush`/`close`; `push!(w, datum)` / `write(w, datums)`; `close(w)` writes the final block and, for path
  targets, renames the sibling temp file into place (`atomic=true`) — caller-owned `IO` is flushed but
  never closed. **Failure contract (all phases, including final-block compression, sink write/flush,
  temp-file close, and rename):** the first exception poisons the writer (further operations throw
  `WriterClosedError` carrying the original cause); for path targets in atomic mode the temp file is
  deleted and the destination is untouched; in non-atomic mode or for caller-owned `IO` the sink keeps
  every block whose bytes were completely written plus possibly a partial final block (documented; no
  repair is promised); `close(w; abort=true)` discards the buffered block and performs the same cleanup;
  `close` is idempotent. Append mode is deferred (requires locking, tail-truncation, abort, and crash
  semantics).
* `Avro.write(dst, table; schema=nothing, infer=false, codec, limits, …)`: schema from `schema=` or from
  `Tables.schema(table)`; a source without a `Tables.schema` raises an `ArgumentError` unless
  `infer=true`, in which case `Avro.inferschema(table; limits) -> (schema, table)` runs first — an
  explicit, bounded (`max_rows`, `max_total_bytes`), materialising operation that **returns the
  materialised rows it inspected** (the source is consumed once; `Avro.write` then writes the returned
  table). `Tables.partitions` become block boundaries (each partition ≥ 1 block); row encoding through
  write plans with column-major extraction for column-accessible partitions. The block payload is
  exactly the encoded bytes.
* Codecs: `deflate` = raw RFC 1951 via CodecZlib; `snappy` = Snappy.jl block format followed by the
  4-byte big-endian CRC32 of the *uncompressed* data (verified on read; in-house table CRC32 tested
  against `Zlib_jll`'s `crc32`); `zstandard` via CodecZstd; `bzip2`/`xz` through extensions
  `AvroCodecBzip2Ext`/`AvroCodecXzExt` (weak deps, both present in the test environment so the
  all-codec gates run) with the error message naming the package to load. Codec objects are allocated
  per task (never shared) and finalised deterministically.

### 4.10 Single-object encoding and schema stores (`src/singleobject.jl`)

`Avro.encodesingle(schema, x; limits) -> Vector{UInt8}` (marker `C3 01`, little-endian CRC-64-AVRO of
the PCF, payload). `Avro.decodesingle(bytes, store; reader_schema=nothing, union_resolution=:spec,
instants=:exact, limits=Limits(), T=…)` validates the marker, looks up the writer schema by
fingerprint, **recomputes the fingerprint of the returned schema and rejects a mismatch**, resolves
against `reader_schema` when given, decodes, and requires exact payload consumption. `SchemaStore`
interface: `Avro.lookup(store, fp::UInt64)`; the built-in `Avro.SchemaCache(; max_entries=10_000,
max_bytes=64 << 20)`: registration is idempotent only for a schema that is structurally `==` to the
stored one; any other schema under an existing fingerprint — whether a different PCF (CRC collision) or a
parsing-equivalent schema with different logical types/defaults/props — is rejected as
`AmbiguousSchemaError`; entries and bytes are bounded. Fingerprints are identifiers, not authentication
(documented). Unknown fingerprints raise `UnknownSchemaError(fp)`.

### 4.11 JSON encoding (`src/jsonencoding.jl`)

`Avro.tojson(schema, x; pretty=false)` and `Avro.fromjson(schema, json, T=…; strict=true,
limits=Limits())` implement the JSON encoding. `fromjson` applies the same protections as schema
parsing — lexical pre-scan against `max_datum_bytes`/`max_schema_depth`, lazy traversal with
`duplicate_keys=:error`, and the operation budget for produced values — with this rule table (positive
and negative tests for each row):

| Avro | JSON output (Java-compatible) | Accepted input (`strict=true`) | Additionally accepted with `strict=false` |
|---|---|---|---|
| null | `null` | `null` | — |
| boolean | `true`/`false` | booleans only | — |
| int / long | integer | integers within `Int32`/`Int64` range (no floats, no strings) | — |
| float / double | number; non-finite as the strings `"NaN"`, `"Infinity"`, `"-Infinity"` (as Java emits) | numbers and exactly those quoted strings | the bare non-JSON tokens `NaN`/`Infinity`/`-Infinity` (Java's decoder accepts them) |
| bytes / fixed | string with code points U+0000–U+00FF per byte | strings whose code points are all ≤ U+00FF; fixed length must match | — |
| string | string | string | — |
| enum | symbol string | member symbol only | — |
| array | array | array | — |
| map | object | object (keys are map keys) | — |
| record | object in field order | object: every field present exactly once, no unknown fields | — |
| union | `null`, or a one-member object keyed by the branch's **fullname** for named types and the type name otherwise | exactly one member whose key names a branch; `null` | — |

The same rules (strict) validate and convert defaults. `tojson` uses the §4.6 branch recovery for
unions. NaN payload bits are not representable in JSON (documented; the binary path preserves them).

### 4.12 Sort order (`src/compare.jl`, Phase 3)

`Avro.compare(schema, a, b) -> Int` on Julia values and `Avro.comparebytes(schema, abytes, bbytes;
limits=Limits()) -> Int` on encoded datums (without materialising; budgeted and depth-limited like a
decode, with the §4.3 skip policy; **each buffer must contain exactly one complete datum** — trailing
bytes on either side are a `DataError`) implement the spec order: null equal; booleans; numerics
ascending with Java's `Double.compare` policy for `NaN` (greater than everything, equal to itself) and
`-0.0 < 0.0` (verified against the `Compare`/`CompareBytes` harnesses); bytes/fixed **unsigned**
lexicographic (the spec rule; Java's object-level `GenericData.compare` orders bytes signed while its
encoded `BinaryData.compare` is unsigned — recorded deviation, spec wins); strings by code point (byte
order of UTF-8); arrays lexicographic; enums by index; unions by branch then value; records by fields
honouring `ascending`/`descending`/`ignore`; maps are an error unless inside an `ignore` field.

### 4.13 Errors and the error guarantee

```
abstract type AvroError <: Exception end
struct SchemaError <: AvroError            # message, JSON path
struct EncodeError <: AvroError            # message, value path, expected schema
struct ResolutionError <: AvroError        # message, writer path, reader path
struct LimitError <: AvroError             # limit name, observed value, limit value, keyword to raise it (encode or decode)
struct CodecError <: AvroError             # codec name, direction, cause (compress or decompress)
struct UnsupportedCodecError <: AvroError  # codec name, extension package to load (if any)
struct ConversionError <: AvroError        # lossy/native conversion failure (e.g. timestamp → DateTime range)
struct UnknownSchemaError <: AvroError     # fingerprint
struct AmbiguousSchemaError <: AvroError   # fingerprint, existing and offered schema
struct WriterClosedError <: AvroError      # original cause, if poisoned
abstract type DecodeError <: AvroError end
struct DataError <: DecodeError            # message, byte position, optional value path (malformed input)
```

Guarantee: every defect the package itself detects in the input (malformed bytes, limit violations,
codec payload errors, schema/resolution/JSON errors) surfaces as the corresponding `AvroError`.
Exceptions that are not about the content — `IOError`/`SystemError` from sources and sinks,
`InterruptException`, `OutOfMemoryError`, `StackOverflowError`, errors thrown by user StructUtils hooks
or user iterators, mmap faults — propagate unchanged and are never recast. Fuzz gates (§9.7): raw
datums mutated under a fixed valid schema must yield `DataError`/`LimitError` or a valid decode;
whole-file, schema, and JSON mutations must yield any `AvroError` or a valid result.

### 4.14 Concurrency contract

* `Schema`, plans, `Limits`, and `SchemaCache` (lock-protected) are safe to share across tasks.
* `Decoder`/`Encoder`/`Reader`/`Writer`/codec instances are single-owner; concurrent use is a bug.
* `Avro.Table` columns are plain vectors (concurrent reads safe). `Avro.Rows` is a single-consumer iterator.
* Parallel decode memory is bounded by `max_inflight_blocks × max_block_bytes` plus the output; the
  shared `Budget` is reserved atomically; the symbol-admission counter is atomic.
* Writer-side parallel compression is deferred to 2.x.

### 4.15 Module layout

```
src/Avro.jl            module, includes, public API docstrings, version-gated `public` declaration
src/errors.jl          error types
src/limits.jl          Limits, Budget, minsize
src/frozen.jl          FrozenVector, FrozenDict, FrozenRef, FrozenJSON, freeze!
src/names.jl           FullName, name validation, namespace resolution, symbol admission
src/jsonscan.jl        lexical pre-scan (size/depth) and lazy JSON traversal helpers
src/schema.jl          Schema types, Field/Default, parser, validator, printer, equality
src/logical.jl         LogicalType structs, validation, value types (Decimal, WideDecimal, Timestamp, LocalTimestamp, Duration)
src/canonical.jl       Parsing Canonical Form, CRC-64-AVRO, fingerprints
src/values.jl          Record, EnumValue, Fixed, UnionValue, the enumerated value set E
src/decoder.jl         Decoder + primitive reads/skips
src/encoder.jl         Encoder + primitive writes
src/plan_read.jl       read plans (generic dynamic, typed, resolving, PlanRef)
src/plan_write.jl      write plans (NamedTuple/struct/row/dict/iterable/generic extraction)
src/columns.jl         column builders and column plans
src/types.jl           Julia type <-> schema mapping, AvroStyle, StructUtils integration, inferschema
src/resolution.jl      resolve(writer, reader)
src/jsonencoding.jl    tojson/fromjson
src/compare.jl         sort order
src/codecs.jl          codec registry, CRC32, deflate/snappy/zstandard, EOS/consumption contract
src/container.jl       Reader/Writer, header/block parsing, parallel decode, Avro.write, atomic paths
src/tables.jl          Avro.Table, Avro.Rows, Tables.jl interface, DataAPI metadata
src/scan.jl            Tables.Scan pushdown (present only in a release that requires a Tables with Scan; §6)
src/singleobject.jl    single-object encoding, SchemaStore, SchemaCache
src/deprecated.jl      1.x shims (legacy mode)
src/precompile.jl      PrecompileTools workload
ext/AvroCodecBzip2Ext.jl, ext/AvroCodecXzExt.jl, ext/AvroTimeZonesExt.jl
```

---

## 5. Public API (all qualified; nothing exported)

### 5.1 Schemas

```julia
Avro.parseschema(src::Union{AbstractString, AbstractVector{UInt8}, IO}; allow_invalid_names=false, allow_invalid_defaults=false, limits=Limits()) -> Schema
Avro.schema(T::Type; name=nothing, namespace=nothing) -> Schema      # Julia type → schema
Avro.schema(x)                                                       # value-level (identity-bearing generic values; ambiguous → ArgumentError)
Avro.schema(::Tables.Schema; name="Record", namespace="")            # table schema → record
Avro.schema(x::Avro.Table / Avro.Rows / Reader)                      # the file's writer schema
Avro.inferschema(table; limits=Limits()) -> (schema::Schema, table)  # explicit, bounded, materialising; returns the materialised rows
Avro.json(schema; pretty=false) / JSON.json(schema)                  # spec JSON text
Avro.canonical(schema) -> String                                      # Parsing Canonical Form
Avro.fingerprint(schema; algorithm=:crc64avro) -> UInt64 | Vector{UInt8} (md5/sha256)
Avro.parsingequivalent(a, b) -> Bool
Avro.resolve(writer, reader; union_resolution=:spec) -> ResolvedSchema
Avro.juliatype(schema) -> Type
Avro.fullname(named_schema) -> String
Base.:(==), Base.hash, Base.show on schemas
```

### 5.2 Datums

```julia
Avro.encode(schema, x; limits=Limits()) -> Vector{UInt8};  Avro.encode!(enc_or_io, schema, x; limits);  Avro.encode(x; limits)  # schema = Avro.schema(x)
Avro.decode(writer_schema, src, T=juliatype(writer_schema); reader_schema=nothing, union_resolution=:spec, limits=Limits(), instants=:exact)
Avro.decode(writer_schema, bytes, pos::Int, T=…; kw...) -> (value, nextpos)
Avro.encodesingle(schema, x; limits); Avro.decodesingle(src, store; reader_schema=nothing, union_resolution=:spec, instants=:exact, limits=Limits(), T=…)
Avro.tojson(schema, x; pretty=false) -> String;  Avro.fromjson(schema, json, T=…; strict=true, limits=Limits())
Avro.compare(schema, a, b) -> Int;  Avro.comparebytes(schema, abytes, bbytes; limits=Limits()) -> Int
Avro.UnionValue(i, x), Avro.EnumValue, Avro.Fixed, Avro.Record, Avro.Decimal, Avro.WideDecimal, Avro.Duration,
Avro.Timestamp{P}, Avro.LocalTimestamp{P}, Avro.Time{P}, Avro.truncate(x, P), Avro.round(x, P)
```

### 5.3 Container files and tables

```julia
Avro.Table(src; reader_schema=nothing, union_resolution=:spec, ntasks=Threads.nthreads(), limits=Limits(), legacy=nothing, decimal_byteorder=:big,
           allow_invalid_names=false, allow_invalid_defaults=false, mmap=true, instants=:exact, names=:admit)   # + `scan=` in a Scan-enabled release
    # Tables.jl columns table (record roots only); `Avro.metadata(t)`, `Avro.schema(t)`, `Avro.codec(t)`, `Avro.sync(t)`, `length`, `Tables.partitions` (per block)
Avro.Rows(src; T=nothing, reader_schema=nothing, union_resolution, limits, legacy, decimal_byteorder, allow_invalid_names, allow_invalid_defaults, mmap, instants, names)
    # streaming datum iterator for any root schema (one decompressed block resident); Tables.rows/schema for record roots; `close`
Avro.Reader(src; limits, legacy, allow_invalid_names, allow_invalid_defaults, mmap)   # block-level: header, `eachblock(r)` → (count, owned bytes), `eachdatum(r)`, `close`
Avro.Writer(io_or_path, schema; codec=:null, level=nothing, metadata=Dict(), sync=nothing, block_bytes=64*1024, atomic=true, limits=Limits())
    push!(w, datum); Base.write(w, datums); flush(w); close(w; abort=false)
Avro.write(dst, table; schema=nothing, infer=false, codec=:null, level, metadata, block_bytes, name, namespace, atomic=true, limits=Limits()) -> dst
Avro.tobuffer(table; kw...) -> IOBuffer
Avro.codecs() -> available codec names
Avro.inspect(src; limits) -> diagnostic report (codec, block count/sizes, padding/legacy issues, root schema, invalid-name/default findings)
```

### 5.4 Deprecated 1.x shims (`src/deprecated.jl`, removed in 3.0)

`Avro.readtable(src; kw...) → Avro.Table(src; legacy=:avrojl1)`; `Avro.writetable(dst, tbl;
compress=:zstd → codec=:zstandard)`; `Avro.read(src, T_or_schema) → Avro.decode`; `Avro.write(x; schema) →
Avro.encode` (only the one-positional-argument datum form; `Avro.write(dst, table)` is the new container
writer, and `Avro.write(io, x)` where `x` is not a table raises a clear error pointing at `encode!`);
`Avro.parseschema` and `Avro.tobuffer` keep their names. All shims call `Base.depwarn`.

---

## 6. Tables.jl integration and `Tables.Scan` pushdown

* `Avro.Table` is a column table: `Tables.columnaccess`, `Tables.columns`, `Tables.schema` — **always a
  stored `Tables.Schema{nothing,nothing}`** for file-derived schemas, so untrusted field names and
  element types never become type parameters — `Tables.partitions` (one `Avro.Table` per block,
  materialised lazily), and the complete read-only DataAPI metadata interface
  (`DataAPI.metadatasupport`, `DataAPI.metadatakeys`, `DataAPI.metadata(t, key; style)`) for file
  metadata (DataAPI is a direct dependency; tested like Arrow's implementation). Columns are plain
  `Vector`s. Zero-column and zero-row tables keep an authoritative row count (`length`).
* **Symbol admission (the trust boundary).** `Tables.columnnames` must return `Symbol`s, which Julia
  interns permanently. Avro keeps a process-wide `@atomic` counter of *distinct* field names it has
  interned (a `Set{String}`-backed admission table under a lock, bounded in bytes); `Avro.Table`/`Rows`
  with the default `names=:admit` intern names while the counter is below `Avro.SYMBOL_BUDGET` (default
  1,000,000 names / 64 MiB of name bytes) and raise `LimitError` beyond it, with the message naming
  `names=:trusted` (bypasses the budget for sources the caller trusts) — repeated untrusted files
  therefore cannot grow the symbol table without bound. Names already interned do not count twice.
* `Avro.Rows` is a row table for record roots (`Tables.rowaccess`, `Tables.rows`, stored
  `Tables.schema`, `Base.IteratorSize` `SizeUnknown` for `IO`, `HasLength` when the block pre-scan is
  possible); `Tables.partitions(rows)` yields per-block `Avro.Table`s so `Avro.write(dst, Avro.Rows(src))`
  streams with bounded memory. For non-record roots it is a plain iterator of §4.6 values.
* `Avro.write` accepts any Tables.jl source with a `Tables.schema` (row or column access) and honours
  `Tables.partitions`.
* **`Tables.Scan` pushdown — release rule (no dormant code):** the Scan integration (`src/scan.jl`, the
  `scan=` keyword) is developed and tested on this branch against Tables.jl `jq/scan` @
  `df4e68c15c874079521d9d4ce4be67dce4345a31` (pinned in `[sources]`, with the Julia 1.10 bootstrap of §11).
  It ships **only** in an Avro release whose `[compat]` requires a registered Tables version that
  contains `Scan` (`src/scan.jl` is included unconditionally in that release, never behind a runtime
  `isdefined` check). If no such Tables release exists when 2.0.0 reaches RC, `src/scan.jl` and the
  `scan` keyword are removed from the 2.0.0 archive (kept on a branch) and ship in the first later
  release that can require the minimum Tables version; the RC rerun (§12) validates whichever archive is
  released. Semantics, against that exact revision:
  * `Tables.resolve(scan, names) -> Tables.BoundScan` binds selection (`Tables.All()` identity, `Not`,
    `Regex`, renames, type overrides; `()` selects zero columns), filter (rewritten to source names),
    `limit`, `offset`; `OpNode`s and column-to-column comparisons are rejected by `Scan`/`resolve` before
    any decoding, with Tables' own errors. An identity scan returns the table unchanged.
  * **Projection**: only `b.columns` ∪ `b.filtercols` are decoded; unselected fields are skipped (skip
    policy §4.3; sized array/map blocks skipped by byte size); output order = `b.columns` order;
    `select=()` yields a zero-column table with the correct row count.
  * **Filter**: per block, pass 1 decodes only `b.filtercols` and records each datum's byte offset;
    `Tables.filtermask(b, subset_table)`; pass 2 decodes the selected columns for qualifying rows only.
  * **offset / limit**: whole blocks are skipped by header `count` before decoding when no filter is
    present; decoding stops at `limit`; offset beyond the row count yields an empty table.
  * **Type overrides**: exact widenings (`int→Int64`, `float→Float64`, `int/long→Float64`) are decoded
    directly; every other override is left to the residual.
  * **Residual**: filter, limit and offset are always pushed, so the residual carries only the unhandled
    overrides over the output names (`Tables.Scan(; select=…, validate=scan.validate)`, or the identity
    `Tables.Scan()`), applied with `Tables.scan(table, residual)` so the conversion rules are Tables' own.
    `Tables.describe(scan, residual)` works.
  * Gate: names, order, eltypes, `Tables.schema`, row count, and values identical to
    `Tables.scan(Avro.Table(src), scan)` for a generated matrix of scans (selection × filter × limit/
    offset × validate × overrides incl. supertypes and no-op subtype overrides), including multi-block
    files, empty records, `select=()`, and missing-only filters.
* **Cross-package smoke** (Phase 5): CSV → Avro → Arrow round trip with the registered Arrow 2.x and a
  `DataFrames` round trip with a stored schema (required); the same pipeline against the local Arrow 3
  candidate checkout (`/Users/jacob.quinn/.julia/dev/Arrow`, which still targets an older Scan revision)
  is run and reported as **informational** until Arrow 3 is released.

---

## 7. Compatibility and migration policy

* Version: **2.0.0** (SemVer-breaking). Julia ≥ 1.10 (LTS). Registered reverse dependencies: none.
* Breaking changes, each with a migration line in `CHANGELOG.md` and `docs/src/migration.md`:
  StructTypes → StructUtils customisation; `Avro.Record{names,T,N}`/`Avro.Enum{names}`/`Avro.Array`
  removed; enums decode as `Avro.EnumValue`, fixed as `Avro.Fixed`, nested records as `Avro.Record`,
  non-nullable unions as `Avro.UnionValue`; `Avro.Duration` fields are `UInt32`; `Avro.Decimal` byte
  order and runtime scale; all timestamps decode as exact wrappers unless `instants=:datetime`;
  `compress=:zstd` → `codec=:zstandard` (shim maps it); `Avro.write(io, x)` with a non-table `x` is no
  longer the datum writer; `readtable` returns columns, not lazy records; strict container validation;
  schema inference is explicit.
* **Known 1.x data defects** (files written by Avro.jl ≤ 1.1.2), reported by `Avro.inspect`: (1)
  null-codec blocks padded with uninitialised bytes after the last datum and (2) codec name `zstd` —
  both handled by `legacy=:avrojl1`; (3) `decimal` values written native-endian (little-endian on all
  supported platforms) as fixed-16 regardless of the schema's size — handled only by the explicit
  `decimal_byteorder=:little`; (4) `Duration` fields signed (identical bytes for values < 2^31); (5)
  record names `Record_<hash>`. Rewrite recipe: `Avro.write(dst, Avro.Rows(src; legacy=:avrojl1,
  decimal_byteorder=...); codec=...)`. The shims are tested against real 1.x files (generated with the
  pinned 1.1.2 in a separate environment and committed as fixtures).
* Deprecation shims for one major cycle; removal in 3.0.
* Behavioural guarantees stated in docs: the error guarantee (§4.13), written files readable by Apache
  Java 1.12 and fastavro (CI-enforced), thread-safety contract (§4.14), ownership contract (§4.9), the
  symbol-admission boundary (§6). The package documents itself as an implementation of the Avro data
  format (schemas, encodings, container files), not of Avro RPC.

---

## 8. Interoperability and conformance strategy

### 8.1 Vendored Apache fixtures (`test/fixtures/apache/`, Apache-2.0 with LICENSE/NOTICE, commit pinned)

`schema-tests.txt` (PCF + CRC-64 fingerprints — parsed and executed), `interop.avsc`,
`weather{,-deflate,-snappy,-zstd,-sorted}.avro`, `weather.avsc`, `weather.json`, `syncInMeta.avro`,
`schemas/simple`, `schemas/withUnion`, `messageV1` (single-object bytes + schema), `reserved.avsc`,
`TestRecordWithLogicalTypes.avsc`. `test.avro12` is an obsolete pre-1.3 format (Java: "Not an Avro data
file") and is excluded. The `interop/rpc` files are excluded (not wire captures).

### 8.2 Generated corpus (`test/fixtures/generated/`, ~3 MB, committed; generator `test/fixtures/generate.sh` checked in)

Already produced in the authoring session and to be regenerated by the script: schemas `bench`,
`everything` (all kinds, recursive `LongList`, nested `Inner`, 13-branch union, every default kind incl.
bytes/fixed `\u00XX` strings), `wide` (100 fields), `empty` (zero-field record), `interop`, `logical`
(all logical-type fields incl. the deferred big-decimal carried as bytes); **non-record roots** (`int`,
`string`, `double`, `bytes`, enum, fixed, array, map, union) for every codec; data from Java `random
--seed` and hand-built JSON via `fromjson` (boundary logical values), recodec'd by Java to `deflate`,
`snappy`, `bzip2`, `xz` (`--level 6`), `zstandard`, and written by fastavro for **all six codecs**; Java
`tojson` expectations; schema-evolution pairs with Java `ReadWithReader` expectations labelled `:java`
(`everything_readerA`: promotions, reorder, defaults, enum-default fallback, union reorder;
`everything_readerB`: type/field aliases and nested renames; `weather_reader`, `weather_reader_fail`)
plus **directly written spec-policy expectations** (branch index + result type) for the union cases;
single-object bytes from Java `BinaryMessageEncoder`; blocking-encoder datums (negative-count sized
blocks at every level); Java `canonical` forms and CRC-64/MD5/SHA-256 fingerprints for every schema;
Java `TimeConversions` vectors (49, incl. the spec's Helsinki example and the out-of-range nanos error);
sort-order verdicts from both Java comparators (32 cases, incl. the signed-bytes deviation); Java
decimal edge vectors; real Avro.jl 1.1.2 files (padding, `zstd`, decimals; five codecs).

Additional matrix rows (Phase 4): empty file (header only), zero-datum blocks, many small blocks (1-datum
blocks), user metadata, unknown codec, user `avro.*` keys (rejected), named-branch collisions in unions,
negative collection blocks, negative block count/size headers, work-rule blocks (huge count, tiny size),
codec bombs (zeros compressed with every codec), truncated compressed streams and valid-stream-plus-
suffix for every codec, corrupt snappy CRC, boundary logical values.

### 8.3 Java harness (`test/interop/java/`, compiled in CI against the pinned jar)

`ReadWithReader` (reader-schema resolution → JSON), `SingleObject` (encode/decode), `BlockingEncode`
(sized blocks), `Compare`/`CompareBytes` (sort order, object-level and encoded), `LogicalCaps`
(capability probe), `TimeVectors` (time/timestamp conversions), `DecEdge` (decimal edges), `BigDec`
(deferred big-decimal vectors), `TimeRead` (in-JVM decode timing).

### 8.4 Oracle capability matrix (measured in the pinned environment; updated when tools move)

| Feature | Java 1.12.2 | fastavro 1.12.2 + cramjam 2.11.0 | avro-py 1.12.2 (as installed) | Spec 1.13-SNAPSHOT |
|---|---|---|---|---|
| codecs | null, deflate, snappy, bzip2, xz, zstandard | null, deflate, snappy, bzip2, xz, zstandard (+ non-spec lz4) | `KNOWN_CODECS` = null, deflate, bzip2 (snappy/zstandard need extra libraries; no xz) | all six |
| decimal bytes/fixed | yes (no decode-side precision check) | yes | yes | yes |
| uuid string / fixed | yes / yes | yes / no | yes / no | yes / yes |
| date, time-millis/micros | yes | yes | date only | yes |
| timestamp-millis/micros, local-* | yes | yes | timestamp only | yes |
| timestamp-nanos, local-timestamp-nanos | recognised; conversions raise out of range | no | no | yes (spec vectors + Java vectors) |
| big-decimal | conversion class (negative scale accepted) | no | no | yes (conflicting on scale) — deferred |
| duration | recognised (raw) | no | no | yes |
| union branch identity (primitive reader unions) | observable (`ReadWithReader` JSON wrapper key) | **not observable** (`7 int`) | not observable | — |

### 8.5 Live differential tests (`test/interop/`, run when `java`/`AVRO_TOOLS_JAR` and the pinned Python venv are available; a CI job on ubuntu installs Temurin 21, the checksummed jar, `avro==1.12.2`, `fastavro==1.12.2`, `cramjam==2.11.0`)

For the schema matrix: (1) Julia-written container files for every codec read by Java `tojson` and
fastavro, compared **semantically** to Julia's `tojson` (decoded datum sequences, schema, metadata,
datum counts, codec name — never OCF bytes, since sync markers and framing are regenerated);
(2) Java `random` data decoded by Julia and re-encoded as raw datums: **byte-exact** comparison through
`jsontofrag`/`fragtojson` only for primitives, fixed, enums, unions of those, and records of those
(deterministic encodings); arrays and maps are compared semantically (decode both sides), with positive
and sized block forms tested independently; (3) `canonical` and `fingerprint` outputs compared for every
schema; (4) schema-resolution pairs read by Julia (both union policies), Java (`ReadWithReader`, `:java`
policy, branch identity via the JSON union wrapper) and fastavro (values only); (5) single-object bytes
cross-decoded; (6) sort-order verdicts (`CompareBytes` as the spec-conformant oracle; `Compare`
differences recorded); (7) negative oracles: a corpus of malformed schemas, datums, blocks, and JSON with
each oracle's accept/reject verdict recorded, which Julia must match (spec-justified deviations recorded
in the fixture).

---

## 9. Testing strategy (deterministic; per-case `Xoshiro(seed)`; failing inputs and seeds persisted)

1. **Unit**: every primitive read/write/skip incl. boundary values (`typemin/typemax`, −0.0, NaN payload
   bit patterns, 10-byte varints, `typemin` counts, negative block headers), every schema constraint and
   limit in §3/§4.4 (positive and negative), every member of the value set `E` (an enumeration test
   asserts `E` is exactly the documented set), every §4.8 logical-type row, every §4.11 JSON row,
   `Fixed`/`EnumValue`/`UnionValue` equality and branch recovery, `minsize` on recursive schemas,
   writer option validation, reserved-metadata rejection.
2. **Spec examples**: the encoding tables and examples from the specification text are literal tests
   (`36 06 66 6f 6f`, `04 06 36 00`, `02 02 61`, the `Example` fullname schema, the LongList schema, the
   Helsinki timestamp example, the `Suit` enum default).
3. **Conformance**: §8.1/§8.2 fixtures; `schema-tests.txt` harness; messageV1; blocking-encoder datums;
   non-record roots for every codec; time/decimal/sort vectors.
4. **Round-trip properties with independent assertions**: a bounded random schema generator (all kinds,
   recursion, logical types, unions, aliases/defaults, props) with a matching random value generator;
   `decode(encode(x))` compared with `isequal` **and** bit-exact float comparison; `parseschema(json(s))`
   compared with structural `==` and per-attribute assertions (defaults, aliases, props, logical types,
   docs, order); `canonical(parseschema(canonical(s))) == canonical(s)`; resolved decode with
   `reader == writer` equals plain decode; columnar `Avro.Table` equals `Tables.columntable(Avro.Rows)`;
   frozen schemas reject every mutation attempt (incl. nested props/defaults).
5. **Schema evolution**: a table-driven matrix of (writer, reader) pairs for every resolution rule and
   failure rule under both union policies with directly written expectations, including recursive pairs,
   ambiguous unions, alias collisions, mutable-default isolation (two decoded records never share a
   default object), invalid UTF-8 under `bytes→string`, decimal mismatch, logical-vs-underlying cases,
   invalid-name/invalid-default repair through reader aliases; Java and fastavro expectations where they
   can witness the result.
6. **Differential**: §8.5.
7. **Mutation / fuzz**: for every fixture and generated datum, 10k seeded mutations (bit flips, byte
   insert/delete, truncation at every offset for small inputs, varint lengthening, count/size
   substitution with extreme values) over owned byte buffers and built-in targets: raw datums under a
   fixed valid schema must yield `DataError`/`LimitError` or a valid decode; whole files, embedded
   schemas, and JSON must yield any `AvroError` or a valid result — never another exception type; each
   case runs under `Limits`; the harness runs cases in batches inside a subprocess with wall-clock, CPU,
   and RSS limits (`ulimit`/`setrlimit` where available, watchdog kill otherwise); failing inputs are
   shrunk (byte-level delta debugging) and persisted. A longer loop runs under `AVRO_FUZZ_ITERATIONS`.
8. **Acceptance equivalence**: the malformed corpus is run through full decode, projection (every
   single-column selection), resolution with skipped fields, and `comparebytes`; verdicts must agree
   except for the documented skipped-string UTF-8 exception.
9. **Truncation / corruption of containers**: header truncation at every byte, sync mismatch, block size
   larger than the file, negative count/size, work-rule violations, wrong snappy CRC, truncated and
   suffixed compressed streams for every codec, decompression bombs for every codec against
   `max_block_bytes`/`max_total_bytes`, trailing garbage, 1.x-style cushion blocks (strict vs legacy),
   header-only files (read and write), writer failure injection at every phase (encode, compress, write,
   flush, close, rename) with the §4.9 contract asserted.
10. **Resource limits and budgets**: each `Limits` field has a test that trips it quickly (< 10 ms,
    < 1 MB allocated) and one that passes just under it; `max_depth` verified inside `Threads.@spawn`;
    cumulative budgets verified across blocks and under parallel decode (atomic reservation); the corpus
    decodes under `max_total_bytes = 256 MiB`; the work rule rejects the 23-byte/2^30-count block in
    microseconds; the symbol-admission budget trips and `names=:trusted` bypasses it.
11. **Concurrency**: multi-block files decoded with `ntasks ∈ {1,2,8}` produce identical tables; two
    blocks failing concurrently (a higher index failing first) surface the lowest block index
    deterministically across 100 scheduled repetitions; GC-stress runs on the parallel path; peak memory
    (chunks + assembly) measured against the bound; writer flush/close/abort/poison ordering;
    caller-owned IO never closed.
12. **Allocation budgets**: measured with a per-Julia-version script (`@allocated` bytes after warm-up;
    allocation counts from `Base.gc_num` deltas where the API exists, otherwise from `@allocations` on
    Julia ≥ 1.9) for primitive decode (0), typed record decode of isbits fields (0), column decode per
    row of isbits fields (0), string row (1 allocation per string).
13. **Compile-cost gate**: §4.5 (zero new method instances after the `E` warm-up, invalidations,
    RSS bound; native-code size informational).
14. **Quality gates**: Aqua (ambiguities, unbound type parameters, undefined exports, piracy, stale deps,
    compat bounds, `project_extras`), JET (`report_package` zero errors; `@test_opt` on hot paths),
    docstring coverage for every public name, Documenter doctests, a Julia 1.10 load/parse test.
15. **Cross-package smoke**: §6 (registered Arrow 2.x required; Arrow 3 candidate informational).
16. **1.x regression port**: the existing `runtests.jl` cases re-expressed through the shims and the new
    API, plus real 1.1.2-written fixtures decoded through `legacy=:avrojl1` / `decimal_byteorder=:little`.

---

## 10. Benchmarks and performance targets

### 10.1 Baselines (Apple M-series, Julia 1.12.6, 1 thread, 1M rows `{id:long, x:double, name:string, flag:boolean}`)

Measured in the authoring session and re-run by the reviewer; the Phase 0 harness (`benchmarks/`,
separate pinned processes for 1.1.2, fastavro, and Java, raw logs with exact commits, CPU/threads/codec
level/bytes/peak RSS) re-measures all of them.

| Implementation | Write (null) | Write (zstd) | Read |
|---|---|---|---|
| Avro.jl 1.1.2 (file via `writetable`: 23,100,363 bytes, of which ~2.2 MB is cushion padding) | 1.02–1.21 s | 1.24 s | index 2.9–3.9 s + materialise 3.9 s (lazy records → columns) |
| fastavro 1.12.2 (Cython, dict rows) | 0.65 s | 0.78 s | 0.50 s |
| Java avro-tools 1.12.2 | — | — | in-JVM `GenericDatumReader` warm best-of-5: **49 ms**; CLI `count` 0.67 s / `tojson` 0.91 s incl. JVM start |

Cross-language numbers are **informational**: the result layouts differ. Gates compare Avro.jl 2.0
against Avro.jl 1.1.2 on the same host in the same session.

**Measurement protocol.** Every gated number is the median of 5 cold processes (`julia --startup-file=no
-e …`), each reporting the best of 3 in-process repetitions after one warm-up; load time is
`@elapsed @eval using Avro` in a cold process (median of 5); counters that a Julia version does not
expose are reported as "unavailable" and never silently pass. Scripts live in `benchmarks/` and
`test/perf/` and are version-gated by `VERSION`.

### 10.2 Targets

| Metric | Gate (same host, ratio vs 1.1.2) | Tracked absolute (named authoring host; informational) |
|---|---|---|
| `Avro.write` 1M rows null codec, 1 thread | ≥ 4× faster | ≤ 0.25 s (≥ 80 MB/s) |
| `Avro.Table` 1M rows null codec, 1 thread | ≥ 10× faster than 1.1.2 read+materialise | ≤ 0.30 s (fastavro 0.50 s, Java warm 0.05 s for reference) |
| `Avro.Table` 1M rows, 8 threads | ≥ 3× single-thread, measured on a named host with ≥ 8 physical cores | — |
| codec read overhead | `Avro.Table` on a zstandard/deflate/snappy file ≤ 1.3× of (null-codec `Avro.Table` time + `transcode` time of the same block bytes with the same codec object), the codec kernel measured in isolation | — |
| Typed single record decode (3 isbits + 1 string field) | ≤ 1 allocation (the string) | ≤ 150 ns |
| Typed single record encode with `encode!` | 0 allocations | — |
| `parseschema(interop.avsc)` | — | ≤ 100 µs, ≤ 300 allocations |
| Projection `select=(:id,)` on the 4-column file (Scan-enabled release) | ≥ 2× faster than full decode | — |
| Package load time | — | ≤ 0.5 s |
| Time-to-first-table on a fresh session | — | ≤ 1.5 s |

---

## 11. Engineering deliverables

* **Package metadata**: `Project.toml` version `2.0.0-DEV`; deps `JSON` (1.7), `StructUtils` (2.8),
  `Tables` (1.13; `[sources]` pin to the Scan SHA on the development branch only), `DataAPI` (1),
  `CodecZlib` (0.7), `CodecZstd` (0.8), `Snappy` (0.4), `TranscodingStreams` (0.11), `MD5` (0.2),
  stdlibs `Dates`, `UUIDs`, `Mmap`, `SHA`, `Random`; `PrecompileTools` (1); weak deps `CodecBzip2`
  (0.8), `CodecXz` (0.7), `TimeZones` (1); `[compat]` bounds for everything incl. `julia = "1.10"`;
  `test/Project.toml` with `Aqua`, `JET`, `Test`, `Tables`, **`CodecBzip2`, `CodecXz`**, `TimeZones`,
  `CSV`, `Arrow`, `DataFrames` (exact compatible versions recorded in the test Manifest). `SentinelArrays`,
  `JSON3`, `StructTypes` dropped. `public` names are declared through a version-gated
  `Core.eval(Expr(:public, ...))` so Julia 1.10 still parses the module. **Julia 1.10 bootstrap**: Pkg
  on 1.10 ignores `[sources]`, so the test/CI setup on the development branch adds Tables explicitly with
  `Pkg.add(PackageSpec(url=..., rev=<full SHA>))` (Arrow 3.0's precedent); release archives need nothing
  special.
* **Licensing**: MIT (unchanged) for the package; `test/fixtures/apache/LICENSE` (Apache-2.0) + `NOTICE`
  attribution in `README.md`; the specification text is referenced, not vendored.
* **Docs** (Documenter): `index.md` (quick start), `manual/schemas.md`, `manual/encoding.md`,
  `manual/container.md`, `manual/tables.md`, `manual/evolution.md`, `manual/logicaltypes.md`,
  `manual/singleobject.md`, `manual/sortorder.md`, `manual/limits-and-security.md` (budgets, work rule,
  skip policy, symbol admission, error guarantee), `manual/performance.md`, `migration.md`,
  `benchmarks.md`, `reference.md` (autodocs). `examples/`: CSV→Avro→Arrow pipeline, Kafka-style
  single-object producer/consumer with a `SchemaCache`, schema evolution walkthrough, struct mapping
  with StructUtils.
* **CHANGELOG.md** (Keep-a-Changelog style) with the 2.0.0 entry.
* **CI** (`.github/workflows/ci.yml`): required jobs — Julia `1.10`, `1.11`, `1` on ubuntu/macos/windows
  with `JULIA_NUM_THREADS ∈ {1, 4}`; docs build; Aqua+JET; interop (ubuntu: Temurin 21 + checksummed jar
  + pinned Python packages). Non-blocking jobs — `pre` and `nightly` on ubuntu only; `juliac --trim`
  smoke compile of a reader program; Arrow 3 candidate smoke. `TagBot`, `CompatHelper`. Trigger on
  `push: branches: ['**']` plus `pull_request` with a same-repo duplicate guard.
* **Precompile**: PrecompileTools workload covering schema parse/print/canonical/fingerprint, encode/
  decode of a representative NamedTuple and struct, container round trip with null/deflate/zstandard/
  snappy, `Avro.Table` (with a `Scan` in a Scan-enabled release), `Avro.Rows`, single-object, JSON
  encoding; budget ≤ 15 s precompile, ≤ 0.5 s load.
* **Trim**: no `eval`/`@generated` on runtime data, no `Symbol`-to-function lookups; `test/trim/` smoke.
* **Code style**: AGENTS.md rules (explicit `return`, guard clauses, `T[]`, `@atomic`, `errormonitor`,
  small functions, whitespace discipline); no formatter enforcement.

---

## 12. Phased milestones and gates

Each phase lands as small local commits with tests; `Pkg.test()` of the new, phase-scoped suite passes
on Julia 1.10 and 1.12 at the end of every phase (and nothing more is implied by that).

| Phase | Scope | Acceptance gate |
|---|---|---|
| **0 — Foundation** | branch; legacy code removed; `Project.toml` 2.0.0-DEV + pinned deps (Tables SHA + 1.10 bootstrap, jar checksum, Python pins incl. cramjam); `test/Project.toml` incl. CodecBzip2/CodecXz; CI skeleton; vendored + generated fixtures with licence and generator script (incl. non-record roots, fastavro all codecs, real 1.x files, time/decimal/sort vectors); Java harness; `errors.jl`, `limits.jl`, `frozen.jl`, `values.jl` skeleton; benchmark harness with raw-logged 1.1.2/fastavro/Java baselines and the measurement protocol; Julia 1.10 load test; `public` gating | `Pkg.test` skeleton green on 1.10 and 1.12; fixtures licensed and regenerable; baseline logs committed |
| **1 — Schema model** | JSON pre-scan + lazy traversal, names, parser/validator (all §3 rules and §4.4 schema limits, repair options), defaults as frozen JSON (with the minimal JSON datum rules needed to validate them), transitive freezing with freeze-time hashes, normalised structural `==`, printer, canonical form, fingerprints, logical-type validation, Julia-type mapping, `minsize` | `schema-tests.txt` 100%; every constraint and limit tested positive/negative; duplicate-key, depth, recursion, equality-on-cycles and transitive-immutability gates (any mutation after freeze throws, incl. nested props/defaults); repair options round-trip invalid legacy schemas; `canonical`/`fingerprint` equal to Java for all fixture schemas; Aqua+JET clean |
| **2 — Binary core** | Decoder/Encoder with checked arithmetic, validation and the skip policy, budgets incl. the work rule (atomic), generic dynamic plans with `PlanRef`, the enumerated value set `E` (`Record`, `EnumValue`, `Fixed`, `UnionValue` for all non-nullable unions, exact timestamps, runtime-scaled decimals, one-level collections), typed plans with `AvroStyle` eligibility, column builders over `E`, logical values with decode-side checks, bounded JSON datum encoding with the strict/permissive rule, single-object + `SchemaCache` ambiguity rules, value-level `Avro.schema(x)`, `ConversionError` | spec-example tests; round-trip properties with independent assertions; 1.x round-trip cases ported; Java `fragtojson`/`jsontofrag` differential on deterministic datums + semantic comparison on collections; blocking-encoder fixtures; fuzz 100k mutations clean in sandboxed batches under the split gate; acceptance-equivalence tests; limit/budget/work-rule tests; allocation budgets; messageV1; numeric compile-cost gate with `E` warm-up |
| **3 — Resolution and order** | `resolve` with both union policies, resolving plans (memoised pairs), aliases, defaults (incl. invalid-default repair), enum defaults by symbol, decimal rule, `Avro.compare`/`comparebytes` (budgeted, exact consumption) | resolution matrix with direct expectations (both policies) plus Java/fastavro where observable; sort-order vectors from both Java comparators incl. NaN/−0.0/ignore/maps/signed-bytes deviation |
| **4a — Strict containers and codecs** | Reader/Writer/`Avro.write` with strict validation (lower bounds, work rule, codec EOS/consumption contract), atomic path writes, full writer failure contract and option validation, reserved-metadata rejection, codecs (+ extensions), legacy mode, `decimal_byteorder`, repair options, `Avro.inspect`, any-root `eachdatum`/`eachblock` (owned bytes), explicit `inferschema` returning data | Apache corpus reads; Java **and** fastavro read Julia files — checked after each codec lands; Julia reads Java and fastavro files for every codec and every root kind; 1.x fixtures read under legacy options and rejected under strict; truncation/corruption/bomb/EOS/suffix/negative-header/work-rule/empty-file/zero-datum/metadata tests; writer failure-injection tests |
| **4b — Tables basics and ownership** | `Avro.Table` (sequential), `Avro.Rows` (record and non-record roots), partitions, DataAPI metadata interface, stored schemas, symbol admission, zero-column/zero-row behaviour, close semantics | `Table == columntable(Rows)` property; ownership/close tests (caller IO untouched); Tables and DataAPI interface tests; symbol-admission tests |
| **4c — Parallel decode** | block pre-scan, bounded worker pool, per-block chunks, atomic budget reservation, lower-blocks-never-abandoned cancellation, lowest-index error selection, assembly | identical results for `ntasks ∈ {1,2,8}`; deterministic failure selection over repeated schedules; GC-stress; in-flight and peak-memory bounds |
| **4d — Scan and performance** | `src/scan.jl` against the pin (`Tables.resolve`, residual with retained overrides), projection skipping, performance work | Scan equivalence matrix; §10.2 ratio gates on the named host under the measurement protocol |
| **5 — Release engineering** | shims, docs, examples, changelog, precompile workload, trim smoke, benchmarks doc, README, CI matrix live, cross-package smoke | docs build without warnings; load-time budget; full local matrix green on 1.10/1.11/1.12 (+1.13-rc if installed); Aqua/JET; interop job green locally |

Readiness levels (this task stops at **PR-ready**; nothing is pushed):

* **Review-ready**: clean exact local head; scoped diff; local supported-version tests, docs, licensing,
  and interop artifacts complete.
* **PR-ready**: review-ready plus a reproducible branch and exact dependency pins (`[sources]` SHA with
  the 1.10 bootstrap, jar checksum, Python pins, test Manifest) and the status record (§15) up to date.
* **Merge-ready**: hosted exact-head CI green and review issues resolved.
* **RC-ready**: the Scan release rule (§6) applied (ship with a minimum registered Tables, or remove
  `src/scan.jl`/the keyword); `[sources]` removed; **a clean environment instantiated from the exact RC
  source archive re-runs the complete matrix** (supported Julia versions, all codecs, interop, fuzz,
  docs, cross-package) and the release tag tree must equal that tested tree; lower compat bounds
  resolve; registered dependencies exist; reverse dependencies/PkgEval checked.
* **Release-ready**: exact version/tag state approved; registration and docs deployment are release
  actions, not evidence of readiness.

---

## 13. Correctness, security, performance, allocation, streaming, concurrency goals (summary)

* Correctness: every spec rule in §3 has a test; all Apache fixtures pass; Java and fastavro read every
  file we write; we read every file they write (all six codecs, every root kind); oracle-verified
  resolution (both policies) and sort order.
* Security: checked arithmetic, no `@inbounds` without a proven bound, no allocation sized by untrusted
  counts, cumulative budgets with an input-proportional work rule reserved atomically, bounded
  recursion/decompression/schema/metadata/JSON/symbol admission, strict validation with a documented skip
  policy, codec EOS/consumption contract, the §4.13 error guarantee; fuzz-clean under sandboxed batches.
* Performance: §10.2; plans compiled per (schema, T) only on request; zero-allocation primitives;
  column-major materialisation; projection skips; block-parallel decode with bounded memory.
* Streaming: `Rows`/`eachdatum`, `Writer`, `IO` sources with one block resident; partitions for
  pipelines; explicit, bounded schema inference that returns its materialisation.
* Concurrency: §4.14 contract; no global mutable state beyond the documented atomic symbol counter;
  structured task lifecycle with deterministic error selection.

---

## 14. Decisions, open risks, and review log

Decisions (each with rationale; reviewers may challenge any):

1. `missing` is the Julia value of `null`; `nothing` encodes as null.
2. Generic enums are `Avro.EnumValue` (equality by fullname + symbol; no interning); typed targets may
   choose `Base.Enum`/`Symbol`/`String`.
3. Generic records are always `Avro.Record`; generic fixed values are `Avro.Fixed`; typed `T` is opt-in.
4. All timestamp logical types decode to exact `Avro.Timestamp{P}`/`Avro.LocalTimestamp{P}` wrappers;
   `DateTime` conversion is explicit, range-checked (`ConversionError`) and documented
   (`instants=:datetime`); a naive `DateTime` derives `local-timestamp-millis`.
5. Decimals carry runtime scale (`Avro.Decimal` Int128 ≤ 38 digits, `Avro.WideDecimal` beyond); digit
   counts are validated on both encode and decode (stricter than Java on decode).
6. Codec dependency split: `deflate`/`snappy`/`zstandard` hard, `bzip2`/`xz` extensions.
7. Strict container validation is the default; `legacy=:avrojl1` covers only the two unambiguous 1.x
   tolerances; decimal byte-order reinterpretation is explicit-only.
8. `Avro.write(dst, table)` is the container writer; datum writing is `encode`/`encode!`; schema-free
   `encode(x)` exists only where `Avro.schema(x)` is unambiguous and is not a round-trip mechanism.
9. `Tables.Scan` support ships only in a release that requires a registered Tables with `Scan`; no
   dormant or runtime-gated Scan code.
10. Default block size 64 KiB; positive-count array/map blocks written; both forms read.
11. RPC, big-decimal, append, writer-side parallel compression, and borrowed byte views are deferred to
    2.x with their requirements recorded (§3, §4.9).
12. Limits default conservatively with a RAM-aware cumulative byte budget and an input-proportional work
    rule (§4.4); every corpus file decodes under the defaults and under a 256 MiB floor.
13. Union-branch selection during resolution is spec-normative (first match including promotion) by
    default; `union_resolution=:java` is an explicit compatibility option.
14. Only two-branch nullable unions decode bare; every other union decodes as `Avro.UnionValue`; branch
    recovery prefers exact representation before coercion.
15. Booleans other than 0/1 and invalid UTF-8 strings are rejected (spec over Java leniency); skipped
    strings are the documented UTF-8 exception.
16. Header-only container files are accepted and written (Java-compatible extension of "one or more
    data blocks").
17. Generic collections are typed one level deep, `Any`-typed beyond, to keep the value set finite.
18. Byte order for `bytes`/`fixed` comparison is unsigned (spec) even though Java's object-level
    comparator is signed.
19. Symbol admission is budgeted process-wide with a `names=:trusted` bypass (§6).

Intentionally unresolved risks:

* The `Avro.Record` generic row boxes isbits fields; the typed path and `Avro.Table` are the fast paths.
  If `Rows` throughput for generic rows proves inadequate, an unboxed layout is a 2.x addition.
* Tables.jl `Scan` release timing is outside this repository (handled by the release rule in §6).
* Worker-task stack depth vs `max_depth=1024`: 500k frames were measured on macOS; Linux/Windows are
  measured in Phase 2 and the default lowered if needed.
* The RAM-aware default budget is deterministic per machine but differs across machines; CI pins it
  explicitly in every test.

Review log:

* **Round 1** (`reviews/codex-review-1.md`, 29 findings: 8 blockers, 20 majors, 1 minor; `VERDICT:
  REVISE`). All 29 adopted, 5 with amendments; dispositions in `reviews/response-1.md`.
* **Round 2** (`reviews/codex-review-2.md`: 12 round-1 items resolved, 16 partially, 1 not; 16 new
  findings: 2 blockers, 12 majors, 2 minors; `VERDICT: REVISE`). All adopted; dispositions in
  `reviews/response-2.md`.
* **Round 3** (`reviews/codex-review-3.md`: 10 of 17 open items resolved, 7 partially (2 flagged
  blocker-level); 19 new findings: 11 majors, 8 minors; `VERDICT: REVISE`). All adopted; dispositions in
  `reviews/response-3.md`. Major changes: input-proportional work rule, `max_blocks`, budget floor
  clamp, per-datum/per-block encode limits reachable from every API (§4.4, §5); transitive freezing via
  `FrozenJSON` props/defaults, freeze-time hashes, no side table (§4.2); finite enumerated value set `E`
  with `UnionValue` for all non-nullable unions and the compile gate defined over it (§4.5, §4.6);
  writer failure contract for every phase, option validation, reserved-metadata rejection, owned
  `eachblock` bytes (§4.9); lower-blocks-never-abandoned deterministic parallel failure (§4.9);
  symbol-admission budget (§6); schema-free encoding scoped and documented as non-round-trip, `fixed<N>`
  default names (§4.8); direct spec-union expectations and fastavro's non-observability recorded (§4.7,
  §8.4); `inferschema` returns the materialised table, `write(...; infer=true)` (§4.9); error hierarchy
  flattened (`LimitError`/`CodecError`/`UnsupportedCodecError`/`ConversionError` direct subtypes) and the
  fuzz gate split (§4.13, §9.7); invalid-schema repair options on all container entry points (§3, §4.9);
  codec EOS/consumption contract (§4.9); cycle-safe `minsize` (§4.4); Scan release rule without dormant
  code and the Arrow 3 candidate smoke (§6); CodecBzip2/CodecXz in the test environment (§11); RC
  full rerun from the exact archive (§12); `fromjson` strict/permissive rule (§4.11); `comparebytes`
  exact consumption (§4.12); full DataAPI interface (§6); decimal edge rules from Java vectors (§4.6);
  measurement protocol (§10).

---

## 15. Status record (maintained through implementation)

* Assumptions: local-only work; no pushes/PRs/tags/registration; CSV/Arrow/Parquet checkouts untouched;
  network used only for specs, dependencies, and interop tools.
* Commands and results: recorded in `STATUS.md` in the worktree as phases complete (exact commands,
  Julia versions, pass/fail counts, benchmark numbers).
* Current state: **plan under review (round 4); no production code changed yet.**

---

## Appendix A — Probe scripts and raw results

Kept in the scratchpad of the authoring session (`probe/`, `fixtures/`, `javah/`, `baselines/`) and
summarised in §2.2, §8.2, §8.3, §10.1; the scripts are re-created as tests and harness sources in
Phases 0, 2 and 4.

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
t = Avro.Table("w.avro")                    # columns; `scan=Tables.Scan(...)` in a Scan-enabled release
for row in Avro.Rows("w.avro")              # streaming, one block resident
    row.temp
end
w = Avro.Writer("out.avro", sch; codec=:snappy); push!(w, row); close(w)

new = Avro.parseschema(...)                 # reader schema with a new defaulted field
Avro.Table("w.avro"; reader_schema=new)     # resolved decode (spec union policy)
store = Avro.SchemaCache(); Avro.register!(store, sch)
msg = Avro.encodesingle(sch, row); Avro.decodesingle(msg, store)
Avro.Table("old.avro"; legacy=:avrojl1)     # file written by Avro.jl 1.x (padding / zstd name only)
Avro.Table("ts.avro"; instants=:datetime)   # DateTime columns instead of exact timestamp wrappers
Avro.write("out.avro", rows; infer=true)    # explicit, bounded schema inference for a schema-less source
```
