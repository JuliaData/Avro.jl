# Avro.jl 2.0 — Audit and Rewrite Plan

Status: DRAFT v11 (revised after Codex review rounds 1–10; see §14 review log and `reviews/response-N.md`).
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
buffers with one portable, fixed-default resource ceiling per operation (memory, work, codec windows,
resolution effort) enforced identically by writers and readers, actual-size reservations with
lowest-block priority for parallel decoding, and an available-memory guard, a faithful, recursively frozen schema
model (names, aliases, defaults, logical types, canonical form, fingerprints, bounded resolution, sort
order), a strict, streaming, parallel container reader/writer for any root schema, a columnar
`Avro.Table`, a row/datum-streaming `Avro.Rows`, prepared reusable datum readers/writers, a
StructUtils-based typed API, single-object encoding, JSON encoding, and `Tables.Scan` pushdown that ships
only against a registered Tables release, gated by conformance against Apache Java, fastavro, and
avro-python. RPC, big-decimal, schema inference, append mode, writer-side parallel compression, and
borrowed byte views are deferred to 2.x with their requirements recorded. The release is **Avro.jl
2.0.0** (breaking; §7 gives the migration policy and the catalogue of 1.x data defects).

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
* `Tables.dictrowtable` implicit schema inference → removed; schema-less sources need an explicit
  schema (inference deferred to 2.x, §3).
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
| Schema declaration, primitive & complex types, attributes as metadata | Required | unknown/custom attributes preserved (`props`) and re-emitted, never validated; stripped by canonical form; among the optional numeric/logical attributes only `fixed.size` is validated as schema syntax; `precision`/`scale`/`logicalType` are interpreted only when a recognised logical type is evaluated (§4.2) |
| Names, namespaces, fullname algorithm, define-before-use, uniqueness, reserved primitive names | Required | §4.2; the fullname algorithm runs first, so a namespace the algorithm ignores is never validated; a self-alias (alias equal to the type's own name) is idempotent, as Java accepts (Apache `schema-tests.txt` cases 023/024); the spec's `Example`/`Simple`/`a.full.Name` example is a test |
| Aliases (type and field; relative/qualified; any string) | Required | used in resolution; subject to uniqueness across *distinct* names |
| Record field `default` (all types; union default = bare JSON matched against branches in order, first match wins; bytes/fixed code points 0–255), `order`, `doc` | Required | defaults validated at parse time; selected union branch retained; quoted non-finite float defaults accepted (Java-compatible extension, §14) |
| Enum `default` | Required | used in resolution; repairable like field defaults |
| Union constraints (no immediate nesting; branch identity = kind for unnamed schemas and fullname for named schemas, each at most once; a definition plus a reference to the same fullname is a duplicate) | Required | negative fixture for each rule |
| "Fixing an invalid, but previously accepted, schema" | Required | `allow_invalid_names=true` (invalid simple names, namespaces, fullnames, field names, symbols — syntax only) and `allow_invalid_defaults=true` (keep invalid field and enum defaults as raw JSON) are accepted by `parseschema` **and every container entry point** (`Reader`/`Rows`/`Table`); `Avro.inspect` always performs a bounded permissive diagnostic parse; structural and uniqueness rules always stay |
| Binary encoding, all types, blocked arrays/maps with negative counts and sizes | Required | reader accepts both block forms with exact size exhaustion; writer emits positive-count blocks |
| JSON encoding of datums | Required | full rule table §4.11; bounded in both directions with one common datum-JSON depth |
| Single-object encoding (`C3 01` + CRC-64-AVRO LE + payload) | Required | with an ambiguity-safe `SchemaStore` (§4.10) |
| Sort order (values and encoded datums, record `order`, map error, logical types by underlying encoding) | Required (Phase 3) | `Avro.compare` / `Avro.comparebytes` with the canonical-encoding contract (§4.12); Java-backed vectors (§8.2) |
| Object container files: header, metadata, blocks, sync, codecs `null`/`deflate`; **any root schema** | Required | strict by default (§4.9); `Avro.Rows`/`eachdatum` decode non-record roots |
| Codecs `snappy` (with CRC32), `zstandard` | Required | hard dependencies (Snappy.jl, CodecZstd ≥ 0.8.7 for `windowLogMax`) |
| Codecs `bzip2`, `xz` | Required, via package extensions | CodecBzip2/CodecXz as weak deps (present in the test environment); xz `memlimit` enforced |
| Schema resolution (all rules incl. promotions, reorder, defaults, enum default, unions, aliases, decimals, other logical pairs) | Required | §4.7; spec-normative union first-match by default, Java mode opt-in; output follows the reader schema; bounded by a resolution work budget |
| Parsing Canonical Form, fingerprints (CRC-64-AVRO, MD5, SHA-256) | Required | conformance file `schema-tests.txt` (100%, including the self-alias cases) |
| Logical types: decimal (bytes/fixed), uuid (string/fixed), date, time-millis/micros, timestamp-millis/micros/nanos, local-timestamp-millis/micros/nanos, duration | Required | per-type contract §4.8; invalid logical types ignored per spec |
| Logical type `big-decimal` | **Deferred to 2.x** | the spec says scale is non-negative and refers to an undeclared precision, while Java 1.12.2 accepts negative scale (`1E+3` → scale −3); needs a recorded policy plus negative-scale, malformed-inner-payload, scale-bound and exact-inner-consumption vectors. Decoded as the underlying `bytes` with the logical attribute preserved |
| Schema inference for schema-less Tables sources | **Deferred to 2.x** | requires a deterministic field-set and type-join lattice with order-independent tests. In 2.0 a source without `Tables.schema` must be given `schema=` (the error message suggests `Tables.dictrowtable` + `Avro.schema(Tables.schema(...))`) |
| Protocol declaration (`.avpr`), messages, errors, MD5 | **Deferred to 2.x** | requires: distinct anonymous request-schema type, property preservation, a deterministic Java-compatible printer whose bytes are what MD5 hashes, one-way validation, ping. The package documents that it implements the Avro data format, not Avro RPC |
| Handshake, framing, call format; HTTP transport | **Deferred to 2.x** | requires real wire tests: `BOTH`/`CLIENT`/`NONE` handshakes and retry, multi-frame messages, invalid frame lengths, metadata, declared and system errors, ping, one-way, protocol evolution, and a live interchange with `avro-tools rpcsend/rpcreceive`. The `share/test/interop/rpc` files are OCF datum files, not wire captures, and are not a gate |
| Avro IDL (`.avdl`), Trevni, tethered MapReduce | Deferred | outside the data format |

---

## 4. Architecture

### 4.1 Principles

1. **Spec first, Java as tie-breaker only where the spec is silent.** Normative spec rules win over Java
   behaviour (union first-match, boolean bytes 0/1, unsigned byte order, decimal precision on decode);
   every deliberate interoperability extension beyond the spec (e.g. header-only files, self-aliases) is
   recorded in §14.
2. **Never trust bytes.** Every decode path is bounds-checked against an explicit end position with checked
   arithmetic; every length/count is validated before allocation; **one fixed-default, portable
   per-operation ceiling** bounds package-owned memory (committed output, in-flight buffers, codec
   workspaces, internal tables) through **actual-size reservations made before every allocation**, work
   as a function of input bytes, and resolution effort; an **available-memory guard** lowers the
   effective ceiling in constrained processes so the package fails with a `LimitError` rather than being
   killed; writers enforce exactly the limits readers enforce — including the decoder requirements of
   the codec frames they emit — so whatever a default writer produces a default reader accepts;
   package-detected malformed content always surfaces as an `AvroError`.
3. **Stable generic values, opt-in specialisation.** Untrusted schemas never drive Julia compilation:
   the generic path uses dynamic plans, a finite enumerated set of value types (§4.6), and
   schema-independent runtime containers; specialised, unrolled code is generated only for a
   caller-supplied target type `T`.
4. **Stream by default, materialise on request.** Container reading is block-at-a-time with bounded
   in-flight memory; `Avro.Table` is the explicit materialisation and owns copies of everything; path
   sources are either memory-mapped or streamed — never read whole into memory.
5. **No global mutable state**, with one documented exception: the default process-wide symbol admission
   table (§6), because `Tables.columnnames` must return `Symbol`s; callers may supply their own admission
   object instead, and every typed decoding path that interns (`DatumReader`, `decode`, `decodesingle`,
   `fromjson`, `Rows`, `Table`) goes through the same boundary. Codec instances, buffers, budgets, and
   plans are owned by reader/writer objects or operations; hashes are computed at freeze time and stored
   in the immutable nodes. The logical-type set is closed in 2.0.
6. **Strict by default, fast by choice.** `validate=:strict` walks every byte it skips (the single
   recorded exception: skipped strings are not UTF-8-validated); `validate=:fast` may jump over sized
   regions and documents exactly what it then cannot detect.
7. **Qualified API, zero exports.**

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
struct EnumSchema <: Schema; name::FullName; aliases; doc; symbols::FrozenVector{String}; default::Default; symbolindex::FrozenDict{String,Int}; props; hash; end
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
* **Alias normalisation.** Type aliases are normalised to fullnames at parse time (a relative alias takes
  the namespace of the type it aliases, per the spec; the raw alias text is kept only for exact
  re-emission); field aliases are plain names. Equality, hashing and resolution use the normalised
  fullnames (`Bar` and `a.Bar` are the same alias for a type in namespace `a`; tested).
* **Empty unions.** `[]` is a valid schema (Apache `schema-tests.txt` 016): it parses, canonicalises and
  fingerprints; it has no finite datum (`minsize = ∞`), `juliatype([]) = Avro.UnionValue`, decoding any
  bytes against it is a `DataError` (no branch index is valid), and encoding any value is an
  `EncodeError`.
* **Equality and hashing.** `==` is structural and semantic: kind, fullname, fields (name, schema,
  default *value* and selected branch — not the JSON spelling — `order`, normalised aliases, doc),
  symbols, enum default, size, logical type and attributes, and `props` compared as unordered maps of
  JSON values;
  recursion uses a visited set of `(a,b)` pairs so cyclic graphs terminate. `hash` is the stored digest
  of the same normalised content, so `==` and `hash` agree. `Avro.parsingequivalent(a, b)` compares
  Parsing Canonical Forms.
* **Attributes.** The required structural attributes (`type`, `name`, `fields`, `symbols`, `items`,
  `values`, `size`) are schema syntax and validated as such; of the optional attributes, `fixed.size` is
  a JSON integer with `0 ≤ size ≤ typemax(Int)`. `precision`, `scale`, `logicalType`, and every
  unknown/custom attribute are metadata: preserved verbatim in `props`, never validated as such,
  re-emitted by the printer, and stripped by the canonical form.
* Logical types are *attributes* of the underlying schema (`logical`), a closed set:
  `Decimal(precision::Int, scale::Int)` (scale defaults to 0 when absent), `UUIDLogical`, `DateLogical`,
  `TimeMillis`, `TimeMicros`, `TimestampMillis/Micros/Nanos`, `LocalTimestampMillis/Micros/Nanos`,
  `DurationLogical`, and `UnknownLogical(name)` (kept so the schema re-serialises faithfully;
  `big-decimal` is carried this way in 2.0). A recognised logical type is **evaluated in context**: when
  its underlying type is wrong, or its attributes are malformed (`precision`/`scale` not JSON integers
  within `Int`, `precision ≤ 0`, `scale < 0`, `scale > precision`, precision exceeding
  `floor(log10(2^(8n−1) − 1))` for fixed size n ≥ 1 — for `fixed(0)` no positive precision is valid, so
  the annotation is always dropped and the formula is never evaluated at n = 0 (parsing never leaks a
  `DomainError`; tested) —, duration size ≠ 12, uuid fixed size ≠ 16), the annotation is dropped to the
  underlying type per spec with the raw attributes preserved in `props` (matching Java, which warns and
  reduces such schemas to their underlying type).
* `Default` (record fields **and enum defaults**) is `nothing` (absent) or
  `Some(DefaultValue(json::FrozenJSON, branch, span, index, valid::Bool))`: the frozen JSON tree of the
  default, the selected union branch (first branch that accepts the bare default, in declaration order),
  the exact source text span, the resolved enum symbol index (enums only), and whether it validated.
  Plans materialise the Julia default **fresh on every use** from the frozen tree (a plan may hold a
  private immutable prototype), charged to the operation's budget. Defaults never make a field optional
  when encoding. With `allow_invalid_defaults=true` an invalid default is kept with `valid=false` and is
  re-serialised verbatim; using such a default during resolution is a `ResolutionError` unless the
  reader schema supplies a valid one.
* **Parsing.** `Avro.parseschema(src; allow_invalid_names=false, allow_invalid_defaults=false,
  limits=Limits())`:
  1. A lexical pre-scan (no allocation; handles strings/escapes) rejects inputs over `max_schema_bytes`
     or deeper than `max_schema_depth` *before* any parser recursion.
  2. The JSON is traversed with `JSON.lazy(...; duplicate_keys=:error)` (`applyobject`/`applyarray`),
     so recursion depth is ours, duplicate keys are errors, and value byte spans are available.
  3. A `ParseContext{namespace stack, named-type table, depth, counters}` first applies the **fullname
     algorithm** (a dotted `name` wins and any supplied `namespace` is ignored without validation; a
     simple name takes the explicit or inherited namespace) and then validates every rule on the
     resulting names: name and namespace grammar (unless `allow_invalid_names`), fullname uniqueness,
     primitive names never redefined, define-before-use (depth-first, left-to-right), field/symbol
     uniqueness within scope, alias uniqueness — an alias equal to its own type's or field's name is
     **idempotent and ignored** (Java-compatible; `schema-tests.txt` 023/024), while an alias colliding
     with a *different* name or alias is a `SchemaError` — union branch identity (kind for unnamed,
     fullname for named; a definition plus a reference to the same fullname counts twice), enum default
     membership, `fixed.size` syntax, `order` values, default validity (including union branch selection
     and integer range checks: JSON integers that do not fit `Int32`/`Int64`, e.g. `BigInt`, are invalid
     defaults), and the schema limits `max_schema_nodes`, `max_fields`, `max_union_branches`,
     `max_enum_symbols`, `max_name_bytes`, `max_named_types`. Errors are `SchemaError`s carrying a JSON
     path.
  4. Named-type references resolve by fullname (qualified, or relative to the enclosing namespace);
     recursive references bind to the already-registered (field-less) record and are closed when its
     `fields` are filled and frozen.
* **Printing.** `Avro.json(schema; pretty=false)` / `JSON.json(schema)` emits spec JSON (first occurrence
  of a named type in full, later references by fullname, namespace attribute only when it differs from
  the enclosing one, custom `props` and exact default text included). `Avro.canonical(schema)`
  implements the seven PCF transformations; `Avro.fingerprint(schema; algorithm=:crc64avro|:md5|:sha256)`
  hashes the PCF bytes (CRC-64-AVRO in-house from the spec pseudo-code; MD5 via `MD5.jl`; SHA-256 via
  `SHA`).
* **JSON label collisions.** The spec lets named types reuse the words `record`/`enum`/`array`/`map`/
  `fixed`/`union`. Such schemas parse and encode/decode in binary normally; `tojson`/`fromjson` raise
  `EncodeError`/`DataError` when a union's branch labels collide (e.g. a record named `map` beside a map
  branch), because the JSON encoding cannot distinguish them (Java rejects such schemas outright,
  fastavro accepts them; recorded in §14 and excluded from the oracle-readability promise of §8.5).

### 4.3 Byte-level decoder and encoder (`src/decoder.jl`, `src/encoder.jl`)

```julia
mutable struct Decoder
    buf::Vector{UInt8}; pos::Int; stop::Int        # next byte; last valid byte (inclusive)
    depth::Int; budget::Budget; limits::Limits; validate::Symbol   # :strict | :fast
end
```

* **Buffers.** The decoder works on `Vector{UInt8}` and on `Mmap`-backed `Vector{UInt8}` (both one-based,
  unit-stride, stable). Other `AbstractVector{UInt8}` inputs (views, `Memory`, custom arrays) are
  accepted at the public API and copied into a `Vector{UInt8}` (the copy charged to the operation
  budget) unless they are `SubArray`s of a `Vector{UInt8}` with unit stride, which are decoded through
  `(parent, offset, length)`. Caller-owned byte vectors are not charged.
* **Primitives**: `readbool` (byte must be 0 or 1), `readint` (≤ 5 bytes, value range-checked into
  `Int32`), `readlong` (≤ 10 bytes, bit 64 overflow rejected), `readfloat`/`readdouble` (little-endian
  `reinterpret`; NaN payloads preserved bit-for-bit), `readlen` (non-negative, `≤ stop − pos + 1`),
  `readbytes` → copy, `readstring` → `String` validated as UTF-8 (`DataError` otherwise; also map keys),
  `readfixed(n)`. All arithmetic on positions/sizes uses checked operations; block counts equal to
  `typemin(Int64)` are rejected (negation overflow). Every failure throws a `DataError(msg, pos)`. No
  `@inbounds` anywhere that is not immediately preceded by an explicit range check on the same values.
* **Validation modes (documented guarantees).** `validate=:strict` (default everywhere): skipping a
  value *walks* it — every varint, boolean byte, length, enum/union index, nesting depth, sized-block
  frame (walked, never jumped) and budget is validated exactly as in a full decode; the single
  difference from full decoding is that **skipped strings are not UTF-8-validated** (length-only).
  Container offset/limit windows in strict mode still decompress and walk every block of the file
  (materialisation stops at the limit; validation does not). `validate=:fast` (opt-in on
  `decode`/`DatumReader`/`Rows`/`Table`/`Reader`/`comparebytes`): sized array/map blocks are jumped by
  their byte size, and container blocks outside the requested row window are not decompressed; the
  documentation states precisely that malformed content inside jumped regions (invalid booleans,
  enum/union indexes, nested framing, corrupt compressed payloads) is not detected in that mode.
  Acceptance-equivalence tests compare full decode, projection, resolution skips, and `comparebytes` on
  the malformed corpus: in `:strict` they must agree except for the skipped-string UTF-8 exception; in
  `:fast` the documented differences are asserted explicitly.
* **Sized blocks.** A negative array/map count is followed by a byte size; the block is decoded through a
  bounded child range `[pos, pos+size)` and must be **exactly exhausted** (`DataError` otherwise).
* **Trailing bytes and positions.** `Avro.decode(schema, bytes)` rejects trailing bytes; `Avro.decode(schema,
  bytes, pos::Int) -> (value, nextpos)` decodes one datum starting at `pos` and returns the consumed
  position; single objects and `comparebytes` require exact consumption of their inputs.
* **IO sources.** The container layer reads blocks of known size into owned buffers. `Avro.decode(schema,
  io; limits)` reads at most `max_datum_bytes + 1` bytes and errors if the datum is larger or if bytes
  remain.
* **Encoder** is a growable `Vector{UInt8}` with `pos`, `ensureroom!` (checked growth), and write
  primitives (`writelong` = 10-byte unrolled varint, `writebytes`, `writestring`, `writefloat`, …).
  `Encoder` is re-usable (`reset!`) and its buffer can be handed to a codec or `IO` without copying. No
  pre-sizing pass. **Encoding is bounded by the same `Limits` as decoding** (§4.4): per datum
  `max_datum_bytes`, per block `max_block_bytes`, per operation `max_total_values`, `max_total_bytes`
  (charged with the same output estimate a reader would compute for the values being encoded plus the
  encoded payload bytes), `max_rows`, the work rule, and `max_depth` (self-referential Julia values fail
  cleanly); the container writer additionally charges its compressor workspace to the ceiling and
  refuses a codec level whose emitted frames a reader could not decode under the same
  `max_codec_memory` (§4.9); it validates values against the schema (union branch acceptance, enum membership, fixed
  length, integer ranges, UTF-8 of strings produced from `codeunits`, decimal precision **and exact
  scale equality** (`value.scale == schema.scale`, else `EncodeError`; rescaling is the caller's job),
  time-of-day ranges, timestamp range for `DateTime` sources — Java raises on out-of-range nanosecond
  conversions, and so do we).

### 4.4 Limits and budgets (`src/limits.jl`)

```julia
Base.@kwdef struct Limits
    # per value
    max_depth::Int            = 1024          # nesting depth of values (recursive schemas); encode, decode, compare, JSON
    max_bytes::Int            = 64 << 20      # one bytes/string/fixed value (64 MiB)
    max_datum_bytes::Int      = 64 << 20      # one encoded datum: decode from IO, single-object payloads, encode output per datum, JSON text
    max_json_depth::Int       = 1024          # datum JSON nesting (tojson and fromjson alike; independent of the schema-document depth)
    # per container block (both bounds enforced before any arithmetic: 0 ≤ count, 0 ≤ size)
    max_block_bytes::Int      = 16 << 20      # compressed and decompressed size of one block (hard cap); encode output per block
    max_block_count::Int      = 1 << 24       # declared datums per block (16M)
    max_block_output_bytes::Int = 64 << 20    # decoded output of one block (hard cap, charged incrementally — not a reservation)
    max_blocks::Int           = 1 << 28       # blocks per container operation
    max_codec_memory::Int     = 32 << 20      # ONE meaning: cap on the COMPLETE decoder memory requirement of one frame as reported by the library (xz `memlimit`; zstd `ZSTD_estimateDStreamSize_fromFrame`); enforced on every frame read and written (§4.9)
    # ONE per-operation ceiling for package-owned memory (committed output + in-flight reservations + internal tables + owned input copies + codec workspaces)
    max_total_bytes::Int      = 256 << 20     # FIXED, portable default (256 MiB); raise explicitly on every side that processes the data
    max_total_values::Int     = 1 << 28       # values decoded or encoded, including zero-size ones (provisional; set by the latency gate below)
    max_rows::Int             = 1 << 28       # container datums per operation (provisional; same gate)
    max_values_per_byte::Int  = 16            # WORK RULE: values ≤ max_values_per_byte × input bytes + work_allowance (one allowance per operation; provisional)
    work_allowance::Int       = 65_536        #   input bytes = decompressed block bytes + block framing + raw datum bytes + JSON text bytes (never compressed size)
    max_resolution_work::Int  = 1_000_000     # match attempts + memo entries + resolving-plan nodes per resolve()
    # schema / metadata (enforced by parser and writer alike)
    max_schema_bytes::Int     = 16 << 20
    max_schema_depth::Int     = 256           # JSON nesting depth of a schema document
    max_schema_nodes::Int     = 1_000_000
    max_fields::Int           = 65_535        # per record
    max_union_branches::Int   = 1_024
    max_enum_symbols::Int     = 65_535
    max_name_bytes::Int       = 1_024         # any name, namespace, symbol, alias
    max_named_types::Int      = 10_000
    max_metadata_bytes::Int   = 16 << 20      # total OCF metadata (keys + values, including avro.schema)
    max_metadata_entries::Int = 10_000
    # concurrency
    max_inflight_blocks::Int  = 0             # 0 = ntasks; the effective in-flight count is further bounded by the ceiling (§4.9)
end
```

**Portable defaults and the available-memory guard.** Every default is a fixed number. The 256 MiB
ceiling is deliberately small: it is the amount of package-owned memory an untrusted operation may
use without anyone raising a limit, and typical Avro files (64 KiB blocks, 8 MiB codec windows) need a
fraction of it. At the start of every operation the **effective ceiling** is
`min(limits.max_total_bytes, available ÷ 2)` with `available = min(Sys.free_memory(),
Sys.total_memory())` (`total_memory` is cgroup-constrained; `free_memory` is host-wide on Julia ≤ 1.12 —
documented); if the effective ceiling cannot admit the operation's first unit of progress (§4.9) the
operation fails with a `LimitError` naming the available memory **before any allocation**, so a
constrained process gets an Avro error rather than an OOM kill. The guard is a safety valve and is not
part of the writer/reader invariant (which is stated under identical configured limits); tests inject
the available-memory value. Concurrent operations each own a ceiling (documented; a caller-side
governor is advised for many concurrent untrusted operations). The ceiling counts package-owned
memory as estimated by the rules below and excludes Julia's runtime, allocator slack and caller-owned
sources; the documentation says so. The `LimitError` message names the limit, the observed value and
the keyword to raise it, and the manual states that limits must be raised on **every** side that
processes the data.

**Constructor validation** (once, at `Limits(...)`): every field ≥ 0; `max_codec_memory ≥ 16 MiB` (the
smallest value under which bzip2 — ≤ 3.7 MiB —, xz preset 6 — 8.06 MiB as reported by liblzma — and
zstandard frames with the default window of every level up to 19 (window log ≤ 23:
`ZSTD_estimateDStreamSize(8 MiB)` = 8,877,872 bytes with zstd 1.5.7, whose fixed per-stream overhead is
489,264 bytes above the window for windows ≥ 1 MiB — reported by the library, never hard-coded) all
decode); `max_datum_bytes ≤ max_bytes +
1 MiB`; `max_block_bytes ≤ max_total_bytes ÷ 4`; `max_codec_memory + 4 MiB ≤ max_total_bytes ÷ 4`;
`max_block_output_bytes ≤ max_total_bytes ÷ 2`; `max_metadata_bytes + max_schema_bytes ≤
max_total_bytes ÷ 2`. These relations guarantee that the first unit of progress of any operation —
header, schema, one compressed block, its decompression workspace, and its first datum — fits under
the ceiling with checked arithmetic (defaults: 16 + 36 + 16 MiB ≪ 256 MiB). Public entry points validate
their own options (`ntasks ≥ 1`, actual tasks capped at the block count; `Writer.block_bytes ≤
limits.max_block_bytes`).

**Work rule (identical on encode and decode).** Let *input bytes* be the bytes that carry the values:
decompressed block payload plus each block's framing (the count and size varints and the 16-byte sync
marker), raw datum bytes, or JSON text; let *values* be every value encountered, including zero-size
ones and the datum itself. The rule `values ≤ max_values_per_byte × input_bytes + work_allowance` is
enforced per datum, per block, and cumulatively per operation with **one** shared allowance deficit
counter per operation (the writer consumes the same counter when it decides to flush, so writer and
reader see the same arithmetic). Before decompression only the hard limits apply (`0 ≤ count ≤
max_block_count`, `0 ≤ size ≤ max_block_bytes` and ≤ remaining input, the codec window cap); there is
**no compressed-size work pre-check** (a one-million-empty-string block compresses to 134 bytes under
zstandard and is valid); decompression runs under the output cap and the work rule is evaluated on the
decompressed size before datum iteration begins, with `count` as its lower bound. The **writer enforces
the same rule**: it counts every encoded value, flushes a block before the block would exceed
`max_values_per_byte × (payload + framing)` values beyond the shared allowance, and raises `LimitError`
for a single datum that exceeds the rule unless the caller raises the limits on both sides.

**Writer/Reader invariant.** With identical `Limits` on both sides — and the defaults are identical
everywhere because they are fixed — everything a `Writer` emits is accepted by a `Reader`: the writer
enforces every per-datum, per-block and cumulative limit a reader enforces (`max_datum_bytes`,
`max_block_bytes`, `max_block_count`, `max_blocks`, the work rule, `max_total_values`, `max_rows`,
`max_total_bytes` charged with the reader's output estimate of the encoded values plus decompressed
payload bytes and the block table a reader will build, `max_metadata_bytes`/`max_metadata_entries` on the header it writes, `max_schema_bytes`/`max_schema_nodes`/… on the schema it
serialises, and `max_codec_memory` on the decoder requirement of the frames it emits — §4.9). The
invariant is tested for null, empty-record, empty-fixed, all-null-field record, nested-empty-array and
nested-all-null-record roots, multi-block files near the ceiling, maximal metadata and a maximal schema.

**Latency gate for the provisional work constants.** `max_values_per_byte`, `work_allowance`,
`max_total_values` and `max_rows` are provisional; Phase 2 measures the slowest legal zero-byte shapes
(null roots, all-null-field records, nested empty arrays/maps, deeply nested empty records, a
32,768-empty-block file) on every supported Julia version on the named host and fixes the constants so
that the worst case admitted by the defaults decodes in **≤ 10 s** single-threaded; the measured numbers
and the chosen constants are recorded in `manual/limits-and-security.md`. Trusted bulk workloads raise
them explicitly.

`Budget` is a per-operation accumulator (values, committed output bytes, in-flight reservations, input
bytes, rows, blocks, resolution work, allowance deficit) whose cumulative commits happen in block-index
order (§4.9). Exceeding any field throws `LimitError` naming the limit, the observed value, and the
keyword to raise it.

**Accounting categories (disjoint; this list is the authority for every charge).** (a) **Storage
shells** — the exact Julia storage of final columns and per-block chunk columns, charged at allocation
for their capacity (`rows × Base.elsize`, plus one tag byte per element for isbits-`Union` element types,
plus the array header), never per value; (b) **referenced payload** — the Julia storage of every reference
value produced, charged as it is produced by **representation-specific formulas** in `src/limits.jl`
(`storagebytes`) of the form `header + per_element × n`, whose constants are **measured at package
initialisation** from canonical probe objects with `Base.summarysize` (an empty `Vector{UInt8}`, an empty
`String`, a boxed `Int64` in an `Any` slot, an 8-element isbits-`Union` vector, a small `BigInt`), so a
future Julia layout change is picked up automatically rather than trusted; the values measured on Julia
1.10.11 and 1.12.6 are recorded and asserted by a test on every supported version: `String` 8 + n (charged
16 + n), `Vector{UInt8}` 40 + n, `Avro.Fixed` 56 + n, `Avro.WideDecimal` 64 + n (BigInt: 48 + 8 limbs),
a boxed isbits value in an `Any` slot (`Record` fields, `UnionValue`, `Vector{Any}`, `Avro.Map{Any}`)
16 + `sizeof`, `Avro.EnumValue` 16, `Avro.Record` with k fields 56 + 8k plus its boxed/referenced fields,
`Avro.UnionValue` 16 plus its boxed value, `Vector{x}` 40 + n × (elsize + tag), and `Avro.Map{x}` (§4.6:
a package-owned map with **deterministic capacity** — keys vector, values vector and an `Int32`
open-addressing index of exactly `tablesz(n)` slots, the power of two `≥ max(16, 2n)`, so its storage
is `128 + n × (8 + slot bytes of x) + 4 × tablesz(n)` and is never rehashed or grown after construction;
**no formula depends on `Base.Dict`'s private layout or on its collision-driven rehashing**). The test
oracle is `Base.summarysize(x; exclude=Avro.Schema)` — schema references carried by `Fixed`,
`EnumValue` and `Record` are excluded because the schema graph is charged once under category (e) — and
the gate `storagebytes(x) ≥ summarysize` holds for generated values of **every member of `E`** and for
the three identity-bearing types with shared and with distinct schema identities; an isbits value stored
inline in a typed column or chunk charges nothing here (its slot is in (a)); ownership of payload
transfers from in-flight chunks to committed columns at commit **without re-charging**; **exactness is
guaranteed for the generic value set `E` and the column path only** — a caller-chosen typed target `T`
(§4.8: `Dict` fields, user structs) is charged the generic estimate of the same datum and documented as
approximate, because its layout is the caller's; (c) **input, codec and output buffers** — the
compressed block buffer (streamed sources), the decompressed buffer as it grows, the codec workspace
(the library-reported requirement of the member about to be decoded where the library reports one —
zstandard `ZSTD_estimateDStreamSize_fromFrame` — and otherwise the configured `max_codec_memory` — xz,
whose per-block requirement liblzma enforces against `memlimit` but does not report before allocation;
fixed bounds for bzip2 ≤ 3.7 MiB, deflate ≤ 64 KiB, snappy 0), the `Encoder` buffer, compressor output
buffers and JSON text buffers (each growth step reserves the replacement capacity before it is
allocated), and owned copies of non-conforming byte sources; (d) **internal tables** — block table, prefix
sums, row offsets, filter masks, the symbol-admission table (the same seeded deterministic-capacity map
as `Avro.Map`, grown by doubling with the new capacity reserved first, under its own
`max_names`/`max_bytes`), and worker/coordinator state (a documented 16 KiB per worker task; task stacks
are allocated lazily by the runtime and excluded, documented); (e) **schema and plan graphs** — every
`Schema`/`Field`/`FullName` node, frozen JSON tree (props and defaults), parser state (`ParseContext`,
name tables), printer/canonical/fingerprint buffers, read/write/resolving plan nodes and their
memoisation tables, charged per node by the same kind of measured formulas under the budget of the
operation that creates them (`parseschema`, `json`/`canonical`/`fingerprint`, `Avro.schema(T)`,
`resolve`, prepared-codec construction — each an operation with its own ceiling, §"Budget scopes"), so
a schema of `max_schema_nodes` nodes with large properties or defaults fails with a `LimitError`
instead of allocating past the ceiling (tests: shallow wide schemas, large defaults/props, and large
plans near the ceiling). **Yield-time transfer:** `Rows`, `eachdatum` and `eachblock` hand each yielded
value to the caller at yield; the operation releases that value's charge at the next iteration step
(the caller now owns it) while the cumulative value/row/work counters persist, so a stream larger than
the ceiling iterates under the ceiling when the caller does not retain the outputs (tested both ways);
`Avro.Table` keeps everything charged until it returns the caller-owned table. Collections in the
generic path are decoded incrementally: `sizehint!` is capped at `min(count, 1024)`, growth is by `push!`
with the new capacity reserved before each growth step, the declared count is additionally bounded by
`remaining_bytes ÷ minsize(item schema)` when `minsize > 0`, and maps are decoded as key and value
vectors first (charged as such) and materialised into one `Avro.Map` sized from the decoded pair count
(duplicate keys: the last value wins, as in Java's `HashMap`-backed reader, documented and tested).
`minsize(schema)`
is the minimal encoded size of a datum: a memoised, cycle-safe fixed point (an active recursion edge
contributes 0; unions take the minimum over branches plus the index byte; records sum fields;
arrays/maps contribute 1; tested on direct recursion, mutual recursion, nullable recursive unions, and a
recursive schema with no finite datum). Every corpus file in §8 decodes under the defaults.

**Budget scopes.** A container `Reader`/`Writer`, `Avro.Table`, `Avro.Rows`, `compare`, `tojson`,
`fromjson`, `resolve`, `parseschema`, `json`/`canonical`/`fingerprint`, `Avro.schema(T)`, and the
construction of a prepared `DatumReader`/`DatumWriter` each own one operation budget for their
lifetime; each call of a prepared `DatumReader`/`DatumWriter` and each one-shot `decode`/`encode` gets
a fresh operation budget (the one-shot forms charge plan construction to that budget). A
`SymbolAdmission` object owns its own table budget.

### 4.5 Plans: schema-directed codecs (`src/plan_read.jl`, `src/plan_write.jl`)

* A **read plan** is a tree of plan nodes, one per schema node. The **generic** plan family is dynamic
  (`Vector{ReadPlan}` children, one dynamic dispatch per child behind a function barrier) and produces
  the finite value set of §4.6; it never specialises on schema shape. Named-type recursion is represented
  by `PlanRef` nodes: plans are built in two passes keyed by schema object identity (and by `(writer,
  reader)` identity pairs for resolving plans), so `LongList`-style schemas and recursive writer/reader
  pairs terminate; plan construction is charged to `max_resolution_work`.
* The **typed** plan family (`Avro.DatumReader(schema, T)`, `Avro.Rows(src; T)`) specialises on a
  caller-supplied `T`: `RecordPlan{names, Ps<:Tuple}` unrolled via `ntuple`/`Val` (any width the caller
  chose), leaf plans per Julia type, recursion through the user's own recursive struct types. Targets
  that intern (`Symbol` fields, `Symbol`-valued enums) decode through the symbol-admission object of the
  operation (§6; default process-wide, caller-owned via `names=`, bypass via `names=:trusted`) on every
  typed path: `DatumReader`, `decode`, `decodesingle`, `fromjson`, `Rows`, `Table`.
* The **column** plan family (`Avro.Table`) uses `ColumnBuilder{e}` objects for `e` in the enumerated
  set `E` of §4.6, stored in a **schema-independent `Vector{ColumnBuilder}`** and driven by one loop with
  a function barrier per column (one dynamic dispatch per cell; no tuple unrolling, no specialisation on
  column order or width). Unselected fields use `skip`.
* Write plans mirror read plans with value-extraction strategies for NamedTuples, StructUtils structs
  (§4.8), `Tables.AbstractRow`s (by column index), `AbstractDict`s, iterables, and the generic values.
* **Prepared codecs.** `Avro.DatumReader(writer_schema, T=…; reader_schema, union_resolution, limits,
  instants, validate, names)` owns its plans, is immutable after construction and **safe to share across
  tasks** (each call allocates its own decoder state and operation budget): `reader(bytes)`,
  `reader(bytes, pos) -> (value, nextpos)`, `reader(io)`. `Avro.DatumWriter(schema; limits)` owns a
  reusable encoder and is therefore **single-owner**: `writer(x) -> Vector{UInt8}` returns a fresh owned
  buffer; `writer(enc_or_io, x)` appends into the caller's encoder or stream. `Avro.decode`/`Avro.encode`
  are one-shot conveniences that build a prepared object per call. Kernel performance gates (§10.2)
  apply to prepared objects; the one-shot path is benchmarked separately (informational).
* **Compile-cost gate** (Phase 2, numeric): a warm-up decodes one schema per member of `E` and one
  `ColumnBuilder{e}` per `e ∈ E`; afterwards decoding 1,000 random heterogeneous schemas (random widths,
  orders, nesting) through the generic and column paths must create **zero new method instances** for
  Avro's functions (measured with `Base.specializations` counts per Julia version, documented script),
  zero invalidations (SnoopCompileCore when available, otherwise informational), and bounded RSS growth
  (< 50 MB, `Sys.maxrss` delta). Native-code size is informational. The typed path is measured separately
  per `T`.

### 4.6 Julia value model (generic decoding, `Avro.juliatype(schema)`)

The **finite value set** `E` (enumerated in `src/values.jl` and asserted by a test):

* Leaves `L` = {`Missing`, `Bool`, `Int32`, `Int64`, `Float32`, `Float64`, `Vector{UInt8}`, `String`,
  `Avro.Fixed`, `Avro.EnumValue`, `Avro.Decimal`, `Avro.WideDecimal`, `UUID`, `Date`, `Time`,
  `Avro.Timestamp{Millisecond}`, `Avro.Timestamp{Microsecond}`, `Avro.Timestamp{Nanosecond}`,
  `Avro.LocalTimestamp{Millisecond}`, `Avro.LocalTimestamp{Microsecond}`, `Avro.LocalTimestamp{Nanosecond}`,
  `Avro.Duration`}.
* Composites `C` = {`Avro.Record`, `Avro.UnionValue`} ∪ {`Vector{x}`, `Avro.Map{x}` : x ∈ L ∪ {`Record`,
  `UnionValue`} ∪ {`Union{Missing,y}` : y ∈ L∖{Missing} ∪ {`Record`}}} ∪ {`Vector{Any}`, `Avro.Map{Any}`}.
* `E` = L ∪ C ∪ {`Union{Missing, x}` : x ∈ (L ∪ C)∖{Missing}}.

| Avro | Julia (generic decode) | Notes |
|---|---|---|
| null | `Missing` (`missing`) | `nothing` also encodes as null |
| boolean / int / long / float / double | `Bool` / `Int32` / `Int64` / `Float32` / `Float64` | |
| bytes | `Vector{UInt8}` | always copied (no borrowed views in 2.0) |
| string | `String` | UTF-8 validated |
| fixed(N) | `Avro.Fixed` (named fixed schema reference + `Vector{UInt8}` of length N; `==` by fullname, size and bytes; `show` prints name and hex) | typed API accepts `NTuple{N,UInt8}`/`Vector{UInt8}` |
| enum | `Avro.EnumValue` (enum schema reference + `Int32` index; `String(x)`, `Symbol(x)`, `Int(x)`; `==` by fullname and symbol string; `show` prints the symbol) | typed API accepts `Base.Enum` subtypes, `Symbol` (through admission), `String` (validated); encoding under a different enum schema remaps by symbol, never by index |
| array | `Vector{e}` with `e = juliatype(items)` when that is in L ∪ {`Record`, `UnionValue`} ∪ {`Union{Missing,…}`} of those; `Vector{Any}` when items are themselves arrays or maps | one level of typed nesting |
| map | `Avro.Map{e}` with the same rule — a package-owned `AbstractDict{String,e}`: insertion-ordered key and value vectors plus an `Int32` open-addressing index of fixed capacity `tablesz(n)` (power of two ≥ `max(16, 2n)`) built once from the decoded pair count, hashed with a **per-instance random seed** (`hash(key, seed)`; Julia's string hash mixes the seed into MurmurHash, so collisions cannot be precomputed offline), never rehashed or grown; expected O(1) lookups, worst-case O(n) under collisions with memory unchanged (documented); `Dict(m)` converts; duplicate keys: last wins | the typed API accepts `Dict{String,T}`/`AbstractDict` targets (built outside exact accounting, §4.4) and encodes any `AbstractDict` with string-convertible keys |
| union | **bare value** only for the two-branch nullable form `["null", T]` / `[T, "null"]`: `Union{Missing, juliatype(T)}`; **every other union** decodes every value — the null branch included — as `Avro.UnionValue(index, value)` | closed set by construction; branch recovery on encode: (1) for bare nullable values, `missing` → the null branch, anything else → the other branch; (2) `UnionValue` is exact; (3) for plain Julia values encoded against any union, a branch whose generic representation type equals `typeof(value)` exactly, else the first branch that accepts the value by conversion. **Under resolution the representation follows the reader schema** (§4.7) |
| record | `Avro.Record` (schema reference + `Vector{Any}` values; `Tables.AbstractRow`; property access by name; `==` structural) | stable at every width and for recursive records |
| decimal(bytes/fixed) | `Avro.Decimal` (`Int128` unscaled, `scale::Int`) for precision ≤ 38; `Avro.WideDecimal` (`BigInt` unscaled, `scale::Int`) for larger precision (chosen per schema at plan time; any `Int` precision/scale the schema carries is representable) | big-endian two's complement; minimal bytes for `bytes`, sign-extended to size for `fixed`; an empty `bytes` payload is rejected (Java raises `NumberFormatException`); absent `scale` = 0; **both encode and decode validate `digits(unscaled) ≤ precision`** (`EncodeError` / `DataError`; Java validates only on encode — recorded deviation in favour of the spec's "maximum precision"); encode requires exact scale equality |
| uuid (string / fixed 16) | `UUIDs.UUID` | string form must be RFC-4122 `8-4-4-4-12` hex, either case (`DataError` otherwise); fixed form is the 16 big-endian bytes; the representation (string vs fixed, letter case) is **not** carried by the value — round trips use the writer schema (§4.8) |
| date | `Dates.Date` | any `Int32` day count is representable |
| time-millis / time-micros | `Dates.Time` | exact (ns-resolution `Time`); decode range-checked to one day; **encode rejects non-aligned values** unless `Avro.truncate(x, P)`/`Avro.round(x, P)` or `Avro.Time{P}` is used (Java truncates silently — recorded deviation) |
| timestamp-millis / micros / nanos | `Avro.Timestamp{Millisecond|Microsecond|Nanosecond}` — exact `Int64` ticks since the Unix epoch, a global instant | `DateTime(x)` is an explicit, **range-checked** conversion raising `Avro.ConversionError` (never `DataError`, never wrap), lossy (floor) for sub-ms units; `Avro.Timestamp{P}(::DateTime)` exact and range-checked; `ZonedDateTime` via the TimeZones extension |
| local-timestamp-millis / micros / nanos | `Avro.LocalTimestamp{Millisecond|Microsecond|Nanosecond}` — exact `Int64` ticks | same conversion rules |
| duration | `Avro.Duration(months::UInt32, days::UInt32, millis::UInt32)` | little-endian unsigned |

`instants=:exact` (default, all of `decode`/`DatumReader`/`Rows`/`Table`/`decodesingle`) keeps the
wrappers; `instants=:datetime` converts every timestamp kind to `DateTime` (a `ConversionError` on
out-of-range values) with documented floor-to-ms loss for sub-ms units. The value model is identical for
`Avro.decode`, `Avro.Rows`, `Avro.Table` cells, defaults, JSON conversion, and sorting.

### 4.7 Schema resolution (`src/resolution.jl`)

`Avro.resolve(writer::Schema, reader::Schema; union_resolution=:spec, limits=Limits()) ->
ResolvedSchema` implements every rule of the "Schema Resolution" section, memoised on `(writer, reader)`
identity pairs and **bounded by `max_resolution_work`** (every branch-match attempt, memo entry and
resolving-plan node is charged; exceeding it is a `LimitError`):

* Match by kind: arrays/maps recursively; enums, fixed (plus size), records by unqualified name *after*
  applying the reader's type aliases; primitives equal or promotable `int→long/float/double`,
  `long→float/double`, `float→double`, `string↔bytes` (`bytes→string` validates UTF-8 at decode time).
* Records: fields matched by name after reader field aliases; **a reader alias consumes the writer
  field** (a reader that also has a field with the writer's original name gets no value for it and must
  have a default); writer-only fields are skipped (validation modes §4.3); reader-only fields require a
  valid default (fresh copy per record); reader field order wins.
* Enums: symbol remap by name; writer symbols absent in the reader use the reader `default` or error.
* Unions: `union_resolution=:spec` (default, normative) selects **the first reader branch that matches
  the writer branch, where matching includes promotion** (writer `int` against reader `["long","int"]`
  selects `long`; against `["double","int"]` selects `double`). `union_resolution=:java` reproduces
  Apache Java's exact-match-first-then-promotion selection. Both policies are tested on
  `["long","int"]` and `["double","int"]` with **directly written expectations (branch index and result
  type)**; Java fixture expectations are labelled `:java`; fastavro is used as an oracle only where its
  result preserves branch identity. Reader-union-only and writer-union-only cases follow the same
  selection.
* **Output representation follows the reader schema.** Resolved values are the §4.6 values of the
  *reader* schema: a non-union reader receives the bare resolved value (a writer union's branch is
  resolved recursively against it and any writer tag is dropped); a two-branch nullable reader yields the
  bare `Union{Missing,T}` form; any other reader union yields `Avro.UnionValue` with the **reader**
  branch index (remapped from the writer's selected branch). `T` defaults from the effective reader
  schema (`reader_schema` when given, else `writer_schema`). All writer-union/reader-union and
  union/non-union directions are tested.
* Logical types: two recognised `decimal`s match only if both precision and scale match
  (`ResolutionError` otherwise). Every other pairing — a recognised logical type against a plain
  underlying type, or two **different** recognised logical types on matching/promotable underlying
  schemas (`timestamp-millis` → `timestamp-micros`, global → local, `date` → `time-millis`) — resolves
  through the underlying schemas and the **reader's interpretation wins over the raw value** (no unit
  conversion is performed; the documentation flags the hazard). Pairings whose underlying kinds differ
  (`uuid` string → `uuid` fixed) fail like any other kind mismatch.
* Failures are `ResolutionError`s carrying both paths. The result is consumed by `Avro.plan` to produce a
  resolving read plan (writer-driven field order with `SkipPlan`/`DefaultPlan`/`PromotePlan`/
  `EnumRemapPlan`/`UnionRemapPlan`/`UnwrapPlan` nodes).

### 4.8 Typed API and Julia type mapping (`src/types.jl`, StructUtils)

* `Avro.schema(T::Type)` derives a schema from a Julia type: the §4.6 table inverted, plus `Int8/16/
  UInt8/16 → int`, `UInt32/UInt64 → long` (range-checked on write), `Float16 → float`, `AbstractString`/
  `Symbol`/`Char → string`, `NTuple{N,UInt8} → fixed` named `fixed<N>` by convention (the enclosing
  record's namespace applies), `Union{Missing,T} → ["null", T]`, other `Union`s → union in Julia's
  member order (documented) — **members that map to the same unnamed Avro kind (e.g.
  `Union{String,Symbol}` → two `string` branches) are an `ArgumentError` naming the colliding members,
  never silently deduplicated** — `Base.Enum` subtypes → enum, `DateTime → local-timestamp-millis` (a
  naive `DateTime` is a local timestamp; global instants are `Avro.Timestamp{P}` or `ZonedDateTime`),
  `Date → date`, `Time → time-micros`, `UUID → string uuid`, `Nothing → null` (so `nothing` encodes
  schema-free; `Union{Missing,Nothing}` collides on two `null` branches and is an `ArgumentError` like
  any other union-member collision), `Avro.Decimal` → no type-level schema (a decimal needs
  precision/scale: `schema=` is required), structs → records via StructUtils
  (`fieldnames`/`fieldtypes`; `@kwarg`/`@defaults` field defaults become Avro defaults when
  JSON-encodable), `Tables.Schema` → record (through the `names`/`types` accessors, which also serve
  stored schemas — fixes #18).
* **Complete name policy for Julia-derived schemas.** Every name the derivation produces — type names,
  namespace components (the module path), field names, enum symbols, Tables column names, and the
  sanitised parameter spellings of parametric types — must satisfy the Avro name grammar
  `[A-Za-z_][A-Za-z0-9_]*`. The derivation does **not** transliterate: a Julia identifier that is not
  already a valid Avro name (Unicode such as `Å`, `var"bad-name"`, a column named `"my col"`) is an
  `ArgumentError` carrying the Julia path (`MyType.field`, `Main.M.Box{Int}`) and the remedies.
  Remedies: (1) field names — the StructUtils `&(avro=(name="…",),)` field tag (or the generic
  `name=` tag) gives the Avro field name; (2) enum symbols — the same tag on `Base.Enum` instances via
  `Avro.avrosymbol(::Type{E}, ::E)`, overridable; (3) type names and namespaces — the overridable method
  `Avro.avroname(::Type{T}) -> (name::String, namespace::String)` (default: `nameof(T)` with the
  module path; parametric types as `nameof(T)` followed by `_` and the sanitised parameter spelling,
  where the **sanitisation algorithm** is: take `string(p)` of each type parameter in order, join with
  `_`, replace every character outside `[A-Za-z0-9_]` by `_`, collapse runs of `_`, prefix `_` if the
  result starts with a digit; if the resulting full name exceeds 128 characters keep its first 100
  characters and append `_` plus the first 16 hex digits of the SHA-256 of the full `string(T)` —
  e.g. `Box{Int64}` → `Box_Int64`, `Dict{String,Vector{Int64}}` → `Dict_String_Vector_Int64`; the
  algorithm uses only `string`, so it is identical across supported Julia versions for the same
  spelling) — this method, not a root-only keyword,
  is the documented remedy for nested collisions; (4) Tables columns — `Avro.schema(::Tables.Schema;
  names=Dict(:col => "avro_name"))`; (5) any source — an explicit schema via `schema=`. Within one
  derivation the same Julia type always reuses its first definition, and two *distinct* Julia types
  that would define the same fullname raise an `ArgumentError` naming both types and pointing at
  `Avro.avroname`. `NamedTuple`/`Tables.Schema` records are named `Record` with nested anonymous records
  `Record_1`, `Record_2`, … in depth-first order; `name=`/`namespace=` keywords override the root.
* **Value-level `Avro.schema(x)`** (used by `Avro.encode(x)`): identity-bearing generic values return
  their instance schema (`Record`, `EnumValue`, `Fixed`); plain Julia values use the **documented
  conventional schema** `Avro.schema(typeof(x))`; a bare `UnionValue` (no enclosing union) and a
  `Decimal` raise an `ArgumentError` directing the caller to `Avro.encode(schema, x)`. Schema-free
  encoding constructs datums from Julia values by convention; it is **not** a round-trip mechanism for
  decoded data — `UUID` (string vs fixed) and `Vector{UInt8}` (bytes) lose their source representation —
  and the documentation says so: round trips go through the writer schema (`Avro.write(dst,
  Avro.Rows(src))`, `Avro.schema(table)`).
* **Typed decoding** goes through a dedicated `Avro.AvroStyle <: StructUtils.StructStyle`. A plain-DTO
  fast route (direct positional construction from a typed plan) is used only when an eligibility check
  passes: `T` is a concrete struct or `NamedTuple`, no custom `StructUtils.make`/`lift`/`choosetype`
  methods apply for `AvroStyle`, no field tags affect construction, every field has a mapped schema, and
  any field defaults are static. Everything else takes the semantic route: generic decode then
  `StructUtils.make(AvroStyle(), T, generic)`. Both routes are tested against the pinned StructUtils
  version, and a test asserts the fast route is never taken when a custom hook exists. `Symbol` targets
  (fields of type `Symbol`, enums decoded to `Symbol`) are a documented trust boundary: every distinct
  value is admitted through the operation's symbol-admission object (§6) before interning, on every
  typed path including `fromjson`, so repeated untrusted inputs cannot grow the symbol table beyond the
  admission budget; encoding `Symbol`s needs no admission.
* Logical-type contract (each row has tests for schema validation, binary boundaries, JSON/default form,
  resolution, native conversion, sort order, and oracle coverage; decimal vectors include empty payload
  (reject), `00`, `ff`, non-minimal `00 00`, sign extension, maximum precision, one-digit overflow,
  absent scale, scale beyond `typemax(Int32)`; UUID vectors include mixed-case strings):

| Logical type | Schema validation (in context) | Binary / value bounds | JSON & defaults | Oracle (Java 1.12.2 / fastavro / avro-py) |
|---|---|---|---|---|
| decimal | precision > 0; 0 ≤ scale ≤ precision (scale default 0); fixed-size bound; malformed → annotation ignored | big-endian two's complement; non-empty; digits ≤ precision on encode and decode; exact scale on encode | bytes/fixed string form | Java `fromjson`/`tojson` and `DecimalConversion` vectors (Java: no decode-side precision check); fastavro `bytes-decimal`/`fixed-decimal`; avro-py decimal |
| uuid | string, or fixed size 16 | RFC-4122 text (any case); 16 bytes | string / fixed string | Java both; fastavro string only |
| date | int | any Int32 | integer | all three |
| time-millis / micros | int / long | `0 ≤ v < 86_400_000` / `< 86_400_000_000` | integer | Java `TimeConversions` vectors (Java truncates sub-unit input); fastavro both; avro-py none |
| timestamp-millis/micros | long | any Int64 (exact wrapper) | integer | Java `TimeConversions` vectors; fastavro; avro-py |
| timestamp-nanos | long | any Int64; Java raises on out-of-range conversion | integer | Java vectors + spec vectors |
| local-timestamp-millis/micros | long | any Int64 | integer | Java vectors; fastavro |
| local-timestamp-nanos | long | any Int64 | integer | Java vectors + spec vectors |
| duration | fixed size 12 | three LE UInt32 | fixed string | Java (raw bytes) |

### 4.9 Container files (`src/container.jl`, `src/codecs.jl`)

* `Avro.Reader(src; limits, legacy=nothing, allow_invalid_names=false, allow_invalid_defaults=false,
  validate=:strict, mmap=true)`: parses the header (magic; metadata map decoded with the spec's
  `{"type":"map","values":"bytes"}` schema under `max_metadata_bytes`/`entries`; sync marker;
  `avro.schema` parsed with §4.2 under `max_schema_bytes` and the repair options), selects the codec from
  `avro.codec` (`null`/absent, `deflate`, `snappy`, `bzip2`, `xz`, `zstandard`; unknown →
  `UnsupportedCodecError`), and iterates blocks **strictly**: `0 ≤ count ≤ max_block_count`;
  `0 ≤ size ≤ max_block_bytes` and `size ≤` remaining input — all checked before any arithmetic (tests
  use −1 and `typemin(Int64)` for both); block index ≤ `max_blocks`; data; sync (mismatch → `DataError`
  with block index); framing bytes credited to the work rule. **Codec contract — `max_codec_memory` has exactly one meaning: the cap on the complete decoder memory
  requirement of one codec member (frame or stream), as reported by the codec library, enforced on every
  member read and on every member written (decision 6 = decision 28); it is not a process-memory cap:**
  xz passes it as `XzDecompressor(; memlimit)` — liblzma counts the dictionary **plus its decoder state**
  (the complete requirement) against `memlimit` for every block of every stream, so a block whose
  requirement exceeds the cap fails with `CodecError`; because liblzma does not report a block's
  requirement before allocating, **the xz workspace reservation is the configured cap itself** (§4.4
  category (c); it only reduces xz parallelism under lowest-block priority, never acceptance, since
  sequential decoding reserves the same). Zstandard calls `ZSTD_estimateDStreamSize_fromFrame` on the
  member's frame header (up to 18 bytes, `ZSTD_FRAMEHEADERSIZE_MAX`; a valid empty frame is 9 bytes) before any decompressor is created and rejects a frame whose
  estimate exceeds the cap with a `CodecError` naming both numbers; the reservation is that estimate,
  re-evaluated at every member boundary; additionally `ZstdDecompressor(; windowLogMax = L)` is passed
  with `L` the largest supported log (10…31) for which `ZSTD_estimateDStreamSize(2^L) ≤ cap` (belt and
  braces, never the primary check; caps at or above the estimate for 2^31 permit every supported frame).
  Deflate's window is fixed at 32 KiB; bzip2's decoder needs ≤ 3.7 MiB for its fixed 900 KiB block
  format (within the 16 MiB constructor floor); snappy is bounded by the capped output. Both enforcements
  were verified in the authoring session to reject over-limit frames. **Members and suffixes.** A block
  payload of a multi-member format — zstandard (frames), xz (streams), bzip2 (streams) — is decoded as a
  sequence of valid members until the payload is **exactly exhausted**, with the decoder requirement
  checked and reserved per member and the output cap and work rule applied cumulatively. Zstandard
  members are delimited with `ZSTD_findFrameCompressedSize` (a required symbol, §11), which also
  recognises **skippable frames** (magic `0x184D2A5?`): they are consumed, produce no output, are charged
  nothing, and are bounded by the payload; a truncated skippable frame is a `CodecError`. Xz **stream
  padding** — all-zero bytes in a multiple of four — is accepted between streams and at the end of the
  payload as the xz format requires of concatenation-capable decoders; non-zero padding or a length that
  is not a multiple of four is a `CodecError`. Deflate has no member concept, so bytes after the final
  (`BFINAL`) block are rejected (no writer produces them; recorded in §14); snappy's framing leaves no
  room for suffixes. **Measured oracle behaviour (authoring session, recorded in §8.4):** a two-frame
  zstandard block is read by Java and fastavro; two-stream xz and bzip2 blocks are read by fastavro while
  Java fails with `EOFException` (its xz and bzip2 codecs stop after the first stream); a deflate block
  with three suffix bytes is read by both (they ignore the suffix); a padded xz block is read by Java
  and rejected by fastavro. None of these affect the oracle-readability promise, because Avro.jl writes
  single members without padding. A truncated member, or bytes that do not begin a valid member or
  valid padding, are a `CodecError` (tests cover truncation, garbage suffixes, concatenated valid members
  for every codec, zstandard empty and skippable frames before/between/after/only, xz padding of 4 and 8
  zero bytes (accepted) and of 1 byte or non-zero bytes (rejected), plus the high-window fixtures: the
  1 GiB-dictionary xz block fails under the default cap and under any cap below liblzma's reported
  requirement (1 GiB + ≈ 64 KiB) and succeeds above it; the window-log-30 zstd frame fails under any cap
  below its reported estimate (1,074,231,088 bytes = 1 GiB + 489,264 with zstd 1.5.7) and succeeds at or
  above it — run under an RSS-limited subprocess); decompressed size is capped at `max_block_bytes` (streamed with a cap); the
  exact work rule is applied to the decompressed size plus framing; exactly `count` datums must consume
  exactly the decompressed block (`DataError` otherwise); a partial trailing block → `DataError("truncated
  file")`. A header-only file is accepted and written as the zero-datum file (a deliberate
  Java-compatible extension of the spec's "one or more data blocks", recorded in §14); zero-count blocks
  are valid. **Any root schema** is supported: `eachblock(r)` → `(count, bytes)` where `bytes` is an
  **owned copy of the decompressed block** that remains valid after iteration advances (the compressed
  form is not exposed), and `eachdatum(r)` → schema-directed generic values; `Avro.Rows` yields
  `Avro.Record`s for record roots and §4.6 values otherwise (the Tables.jl row interface is provided
  only for record roots); `Avro.Table` requires a record root and raises a clear `ArgumentError`
  otherwise. Sources: file path (memory-mapped by default; `mmap=false` opens the file and uses the
  **sequential streaming block reader** — the file is never read whole into memory), `Vector{UInt8}`/
  views (caller-owned, not charged), `IO` (streaming: the compressed block buffer and its decompressed buffer are both reserved under the
  ceiling before they are allocated; one block resident), `IOBuffer` (its written bytes only).
* **Legacy mode.** `legacy=:avrojl1` enables exactly two *unambiguous* tolerances for files written by
  Avro.jl ≤ 1.1.2: `avro.codec == "zstd"` read as zstandard, and null-codec blocks with trailing bytes
  after `count` datums accepted (one `@warn` per source). The 1.x native-endian decimal defect is **never
  inferred**: reinterpreting fixed/bytes decimals little-endian requires the explicit keyword
  `decimal_byteorder=:little` (default `:big`). The deprecated `readtable` shim sets `legacy=:avrojl1`
  only. Strict mode is the default for every new API.
* **Ownership and lifetime.** `Avro.Table` copies all data: path sources are opened, mapped or streamed,
  decoded, and closed within the call; caller-owned `IO`/byte sources are left open and unreferenced
  afterwards. `Rows` and `Reader` hold a path-owned mapping/handle until `close` (idempotent; iteration
  after close throws); for caller-owned `IO` they only drop their reference on `close` and never close
  the caller's stream. Truncating a memory-mapped file while it is being read is undefined at the OS
  level (SIGBUS); `mmap=false` (streamed) is the safe option for files that may change and is documented
  as such.
* **Parallel decoding** (`Avro.Table` only, memory-mapped or byte sources, `ntasks > 1`, actual tasks
  capped at the number of blocks; `IO`/streamed sources decode sequentially, growing columns geometrically and reserving each new
  capacity — the same exact storage formula as below — before every `sizehint!`/`resize!`). The in-flight
  block count is `inflight = min(max_inflight_blocks == 0 ? ntasks : max_inflight_blocks, ntasks,
  nblocks)`, further bounded at run time by the ceiling; a **fixed worker pool** of `min(ntasks,
  Threads.nthreads(), inflight)` tasks is created once (never one task per block), and its state is
  charged to the ceiling (§4.4 category (d)). Stage 1 pre-scans block
  headers (no decompression) into a block table with checked prefix sums under `max_rows`/`max_blocks`
  (the table itself charged to the ceiling) and **preallocates every final column once, at its exact
  final capacity**, charging the **exact Julia storage** of `Vector{e}(undef, rows)` — `rows ×
  Base.elsize(Vector{e})`, plus one type-tag byte per element for isbits-`Union` element types
  (`Union{Missing,Float64}` is 9 bytes per row, `Union{Missing,Bool}` 2, `Union{Missing,UUID}` 17;
  verified on Julia 1.12 with `Base.summarysize`, which is the test oracle for the empty shell), plus the
  40-byte array header, with checked arithmetic — not the logical length — to the ceiling up front;
  reference element types charge 8 bytes per row for the slot, and their referenced payload (§4.4 category (b)
  formulas) is charged separately as committed output when the value is produced, so **storage and payload are never double-charged**; so final columns are
  never reallocated and a table whose column shells alone exceed the ceiling fails deterministically
  before decoding. Stage 2 runs under **one accumulator — the effective ceiling — shared by committed
  output, final-column capacity and in-flight reservations**, with these rules:
  1. **Actual-size reservations, before every allocation.** A worker reserves exactly what it is about
     to allocate: the compressed block buffer (declared `size`, for streamed sources), the codec
     workspace (§4.4 category (c): zstandard's `fromFrame` estimate per member, the configured cap for
     xz, the fixed bounds for bzip2 and deflate, none for snappy), the decompressed buffer **as it grows** (streamed decompression reserves
     each growth step before performing it, under the `max_block_bytes` hard cap), and referenced payload
     **as it is produced** (§4.4 category (b); isbits cells charge nothing here because their slots are
     in the chunk's storage shell; the `max_block_output_bytes` hard cap counts shells plus payload).
     Nothing is reserved at a worst-case size except the xz workspace (§4.4 (c)); reservations are
     released when the buffer is freed.
  2. **Lowest-block priority.** A reservation request by block *i* is admitted iff `committed +
     capacity + Σ_{j ≤ i, in flight} reserved_j + Δ ≤ ceiling` — blocks with a *higher* index are not
     counted against block *i*. If the physical total `committed + capacity + Σ_all reserved + Δ` would
     exceed the ceiling, the coordinator **evicts the highest-index in-flight block** (it abandons its
     partial work, releases its reservations and is requeued) and retries, until Δ fits or no higher
     block remains; only then does block *i* itself fail with a `LimitError` — which is exactly the
     condition under which sequential decoding fails at block *i*. Hence **acceptance is identical for
     every `ntasks`**: a file is accepted iff the sequential rule accepts it; parallelism only costs
     wasted work under memory pressure, never acceptance. **Eviction is cooperative and acknowledged:**
     a worker observes its eviction flag between decompression steps (streamed decompression advances in
     bounded steps of at most 64 KiB of output) and between datums, frees its buffers, and acknowledges;
     the coordinator counts the reservation as released only after the acknowledgement, so "released"
     means physically freed, and the wait for it is bounded by one step. **Each block is attempted at
     most twice:** the first attempt is speculative (evictable); an evicted block is marked serial-only
     and retried only when it is the lowest uncommitted block, where by rule 3 it is never evicted again.
     The value and work counters of a discarded attempt are **rolled back** — cumulative rules are
     checked only at commit — so acceptance stays identical to sequential decoding, and the total
     work is bounded by **twice the sequential work bound** in deterministic units — every block is
     decompressed at most twice per pass and every value walked at most twice per pass — stated in the manual and
     **gated in those units** under forced eviction schedules (attempt counter ≤ 2 per block per pass;
     decompression and value counters ≤ 2 × the sequential run's). Measured CPU time is reported
     alongside with a predeclared tolerance (≤ 2.5 × sequential on the named host) and is informational,
     because scheduling, atomics and the assembly copy are not part of the deterministic bound. A filtered
     scan (§6) has two passes with separate attempt scopes, so its absolute bound is four decompressions
     per block against the sequential filtered scan's two — the same 2 × ratio.
  3. **Liveness.** The lowest-index in-flight block never waits on any other block (its reservations are
     admitted by evicting higher blocks, waiting only for their bounded acknowledgement) and the
     coordinator waits only on it; every commit releases that
     block's reservations; evicted blocks are retried after the next commit. No task ever waits while
     holding memory that a lower-index block needs.
  4. **Streaming ordered assembly.** The coordinator commits the lowest uncommitted block in index order:
     it checks the cumulative rules (`max_total_values`, `max_rows`, the operation work rule, the exact
     committed total against the ceiling), copies isbits chunk data into the preallocated final columns
     at the block's prefix-sum offset (references are moved, not copied, and their payload charge moves
     from in-flight to committed without re-charging), then frees the chunk shells and releases the
     block's remaining reservations. The copy never allocates (destination capacity was reserved in stage 1),
     so the physical peak is `capacity + committed payload + in-flight reservations ≤ ceiling` at all
     times.
  5. **Deterministic failure ordering.** Cumulative budget failures occur at the coordinator in index
     order; content failures record their block index in an `@atomic` minimum, workers abandon only
     blocks with a *higher* index, and after all tasks settle the error of the lowest failing block index
     is rethrown — **the lowest index wins regardless of failure kind** (if block 3 has a content error
     and block 5 a budget error, block 3's error is reported; if block 3's commit exceeds the budget and
     block 5 is corrupt, the budget error at block 3 is reported).
  The parallel gates: identical results and identical acceptance for `ntasks ∈ {1,2,8}` under default
  limits (incl. files whose total exceeds the ceiling, which fail at the same block index); forced
  eviction schedules; and the peak-RSS gate, whose method is fixed now: a fresh process is warmed
  (`using Avro` plus one small decode), the input file is read into a caller-owned byte buffer and
  faulted **before** the baseline RSS is recorded (so the parallel byte-source path is exercised, not
  the sequential streamed path), the table is decoded with `ntasks = 8`, a test-only `@atomic`
  high-water counter asserts that at least two blocks were in flight concurrently, and `peak_rss −
  baseline_rss ≤ effective_ceiling + 128 MiB` with both numbers recorded. **The measurement primitive (a sampled
  high-water mark, supplemented by OS and allocator high-water marks):** the decode runs in a child
  process; the parent samples the child's *current* resident set (`ps -o rss= -p <pid>`, every 10 ms, on
  macOS and Linux) from the moment the child prints and flushes a start line — emitted after the warm-up
  and after the input buffer is faulted — until it prints and flushes a done line; `baseline_rss` is the
  first sample; `peak_rss` is the maximum of the samples, the child's `Sys.maxrss` reported at the end
  (the OS lifetime high-water mark, which the small warm-up does not dominate), and the child's own
  `Base.gc_live_bytes()` high-water sampled around every block commit; on Windows only the child's
  `Sys.maxrss` is available and the gate is informational (documented). The deterministic reservation
  oracle (every allocation preceded by a reservation, asserted by a test-only allocation hook) is the
  primary safety gate; RSS is the physical confirmation. The gate runs on highly compressible
  16 MiB blocks and on a table whose final columns exceed half the ceiling. The eight-thread performance gate (§10.2) runs with an explicit
  `Limits(max_total_bytes = 4 GiB)` stated in the benchmark protocol, because default limits allow only
  as much parallelism as fits under 256 MiB.
* `Avro.Writer(io_or_path, schema; codec=:null, level=nothing, metadata=Dict{String,Vector{UInt8}}(),
  sync=nothing, block_bytes=64*1024, atomic=true, fsync=false, limits=Limits())`: **option validation** —
  `sync` must be exactly 16 bytes and is copied (`nothing` → 16 bytes from `RandomDevice`); `0 <
  block_bytes ≤ limits.max_block_bytes`; `level` validated against the codec's documented range
  (`nothing` → codec default); **every user-supplied `avro.*` metadata key is rejected** (`avro.schema`
  and `avro.codec` come only from the constructor arguments); the header it is about to write is
  validated against `max_metadata_bytes`/`max_metadata_entries`, and the schema it serialises against
  the schema limits, exactly as a reader would. It writes the header, buffers encoded datums in an
  `Encoder` under every reader limit (§4.4 invariant), emits a block when `block_bytes` is reached, when
  the next datum would violate the block work rule, or on `flush`/`close`; `push!(w, datum)` / `write(w,
  datums)`; `close(w)` writes the final block and, for path targets, renames the sibling temp file into
  place — caller-owned `IO` is flushed but never closed. **Codec requirements on the writing side:**
  at construction the writer computes (a) the compressor workspace of the chosen codec and level (xz:
  `lzma_easy_encoder_memusage(preset)` — 93 MiB at preset 6, 673 MiB at preset 9; zstandard:
  `ZSTD_estimateCStreamSize(level)` — 3.5 MiB at level 3, 834 MiB at level 22; bzip2 ≤ 7.6 MiB; deflate
  ≤ 320 KiB; snappy ≈ the block size) and charges it to the writer's ceiling, and (b) the **decoder
  requirement of the frames it will emit** (xz: `lzma_easy_decoder_memusage(preset)`; zstandard: the
  writer sets the frame's `windowLog` explicitly to the largest `L` in `10 … default windowLog of the
  level` (`ZSTD_getCParams`: 19 at level 1, 21 at level 3, 23 at level 19, 27 at level 22) with
  `ZSTD_estimateDStreamSize(2^L) ≤ max_codec_memory` — no arithmetic on a hard-coded overhead — and
  **verifies every emitted frame** with `ZSTD_estimateDStreamSize_fromFrame` on its header before the
  block is written (a violation is a `CodecError`; unreachable by construction and gated);
  bzip2/deflate/snappy always fit the 16 MiB floor); if either exceeds the configured
  limits the constructor raises `LimitError` naming the level and the keyword to raise — so a writer
  never emits a frame that a reader with identical limits cannot decode (gated at minimum, default,
  high and raised limits for every codec, and for zstandard at caps one byte below, at, and one byte
  above `ZSTD_estimateDStreamSize(2^L)` for every supported `L`, asserting the selected `windowLog` and
  the `fromFrame` estimate of the emitted frame). **Atomic path
  contract** (`atomic=true`): the temp file is created with `mktemp` in the destination's directory
  (same filesystem by construction; default permissions from the umask), data is written and closed,
  then `Base.Filesystem.rename` replaces the destination (an existing destination file is replaced; if
  the destination path is a symlink, the link itself is replaced, not its target; directories are
  errors); on Windows the replacement fails with an `IOError` if the destination is open elsewhere, and
  the temp file is then removed; `fsync=true` additionally syncs the file before rename (the directory is
  not synced; documented); `atomic=false` writes the destination in place. Replacement and rename
  failure are tested on every supported OS. **Failure contract (all phases, including final-block
  compression, sink write/flush, temp-file close, and rename):** the first exception poisons the writer
  (further operations throw `WriterClosedError` carrying the original cause); in atomic mode the temp
  file is deleted and the destination is untouched; in non-atomic mode or for caller-owned `IO` the sink
  keeps every block whose bytes were completely written plus possibly a partial final block
  (documented; no repair is promised); `close(w; abort=true)` discards the buffered block and performs
  the same cleanup; `close` is idempotent. Append mode is deferred.
* `Avro.write(dst, table; schema=nothing, codec, limits, …)`: schema from `schema=` or from
  `Tables.schema(table)`; a source without a `Tables.schema` raises an `ArgumentError` whose message
  shows how to supply one (`Tables.dictrowtable` + `Avro.schema(Tables.schema(...))`) — inference is
  deferred (§3). `Tables.partitions` become block boundaries (each partition ≥ 1 block); row encoding
  through write plans with column-major extraction for column-accessible partitions. The block payload
  is exactly the encoded bytes.
* Codecs: `deflate` = raw RFC 1951 via CodecZlib; `snappy` = Snappy.jl block format followed by the
  4-byte big-endian CRC32 of the *uncompressed* data (verified on read; in-house table CRC32 tested
  against `Zlib_jll`'s `crc32`); `zstandard` via CodecZstd (≥ 0.8.7); `bzip2`/`xz` through extensions
  `AvroCodecBzip2Ext`/`AvroCodecXzExt` (weak deps, both present in the test environment so the
  all-codec gates run) with the error message naming the package to load. Codec objects are allocated
  per task (never shared) and finalised deterministically.

### 4.10 Single-object encoding and schema stores (`src/singleobject.jl`)

`Avro.encodesingle(schema, x; limits) -> Vector{UInt8}` (marker `C3 01`, little-endian CRC-64-AVRO of
the PCF, payload). `Avro.decodesingle(bytes, store; reader_schema=nothing, union_resolution=:spec,
instants=:exact, validate=:strict, limits=Limits(), names=…, T=…)` validates the marker, looks up the
writer schema by fingerprint, **recomputes the fingerprint of the returned schema and rejects a
mismatch**, resolves against `reader_schema` when given, decodes, and requires exact payload
consumption. `SchemaStore` interface: `Avro.lookup(store, fp::UInt64)`; the built-in
`Avro.SchemaCache(; max_entries=10_000, max_bytes=64 << 20)`: registration is idempotent only for a
schema that is structurally `==` to the stored one; any other schema under an existing fingerprint —
whether a different PCF (CRC collision) or a parsing-equivalent schema with different logical
types/defaults/props — is rejected as `AmbiguousSchemaError`; entries and bytes are bounded.
Fingerprints are identifiers, not authentication (documented). Unknown fingerprints raise
`UnknownSchemaError(fp)`.

### 4.11 JSON encoding (`src/jsonencoding.jl`)

`Avro.tojson(schema, x; pretty=false, limits=Limits())` (bounded: output text ≤ `max_datum_bytes`,
values ≤ `max_total_values`, depth ≤ `max_json_depth`; cyclic in-memory values therefore fail with
`LimitError`) and `Avro.fromjson(schema, json, T=…; strict=true, limits=Limits(),
names=Avro.DEFAULT_ADMISSION)` implement the JSON encoding. `fromjson` applies the same protections as
schema parsing — lexical pre-scan against `max_datum_bytes` and **`max_json_depth` (the same ceiling
`tojson` uses, so default output always re-parses under defaults; boundary round trips at 1024 and 1025
levels)**, lazy traversal with `duplicate_keys=:error`, the operation budget (the work rule counts JSON
text bytes as input), and symbol admission for typed targets — with this rule table (positive and
negative tests for each row):

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
| union | `null`, or a one-member object keyed by the branch's **fullname** for named types and the type name otherwise; ambiguous labels (§4.2) → `EncodeError` | exactly one member whose key names a branch; `null`; ambiguous labels → `DataError` | — |

**Defaults are the exception for unions:** a union-typed field default is the bare JSON of the value
(no wrapper), matched against the branches in declaration order (§4.2); every other row applies to
defaults verbatim (strict; the quoted non-finite forms are thus accepted in defaults — a Java-compatible
extension recorded in §14). `tojson` uses the §4.6 branch recovery for unions. NaN payload bits are not
representable in JSON (documented; the binary path preserves them).

### 4.12 Sort order (`src/compare.jl`, Phase 3)

`Avro.compare(schema, a, b; limits=Limits()) -> Int` on Julia values (depth- and value-bounded; cyclic
values fail with `LimitError`) and `Avro.comparebytes(schema, abytes, bbytes; limits=Limits(),
validate=:strict) -> Int` on encoded datums (without materialising; budgeted; **each buffer must
contain exactly one complete datum** — trailing bytes on either side are a `DataError`) implement the
spec order: null equal; booleans; numerics ascending with Java's `Double.compare` policy for `NaN`
(greater than everything, equal to itself) and `-0.0 < 0.0` (verified against the `Compare`/
`CompareBytes` harnesses); bytes/fixed **unsigned** lexicographic (the spec rule; Java's object-level
`GenericData.compare` orders bytes signed while its encoded `BinaryData.compare` is unsigned — recorded
deviation, spec wins); strings by code point (byte order of UTF-8); arrays lexicographic **independent
of block form** (positive vs sized blocks compare equal when their items are equal — gated by cross-form
vectors); enums by index; unions by branch then value; records by fields honouring
`ascending`/`descending`/`ignore`; maps are an error unless inside an `ignore` field. **Logical types
sort by their underlying Avro encoding**, not by native value: a `decimal` compares as its
bytes/fixed payload (so `-1` = `ff` sorts after `0` = `00`), `duration` as its 12 little-endian bytes,
`uuid` as string/fixed, timestamps/dates/times as their integers. **Cross-API contract:** `compare` on
native values compares the **canonical encoding** of each value (minimal big-endian two's complement for
`bytes` decimals, size-padded for `fixed`, lower-case RFC-4122 text for string UUIDs — exactly what
Avro.jl's encoder emits), so `compare(schema, a, b) == comparebytes(schema, encode(schema, a),
encode(schema, b))` holds for every orderable schema (the generated property excludes map-bearing
schemas, which raise by design) and every Avro.jl-produced encoding; `comparebytes` on *non-canonical*
encodings (a non-minimal decimal `00 00`, an upper-case UUID) compares the raw bytes per the spec and
may therefore differ from `compare` on the decoded values — documented, with non-minimal decimal and
mixed-case UUID vectors in both matrices, normalised to `-1`/`0`/`1`/`ERROR:<class>`.

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

* `Schema`, plans, `Limits`, prepared `DatumReader` (immutable; per-call decoder state and budget), and
  `SchemaCache` (lock-protected) are safe to share across tasks.
* `Decoder`/`Encoder`/`Reader`/`Writer`/`DatumWriter`/codec instances are single-owner; concurrent use
  is a bug.
* `Avro.Table` columns are plain vectors (concurrent reads safe). `Avro.Rows` is a single-consumer iterator.
* Parallel decode memory (final-column capacity plus committed payload plus actual-size in-flight
  reservations plus internal tables) is bounded by the effective operation ceiling; reservations have
  lowest-block priority with cooperative, acknowledged eviction of higher blocks (at most two attempts
  per block per pass; a filtered scan's two passes give an absolute bound of four decompressions per
  block); commits are ordered; the symbol-admission table is lock-protected. Concurrent operations each own a ceiling (documented; caller-side governor advised).
* Writer-side parallel compression is deferred to 2.x.

### 4.15 Module layout

```
src/Avro.jl            module, includes, public API docstrings, version-gated `public` declaration
src/errors.jl          error types
src/limits.jl          Limits (validated constructor), Budget, minsize, work rule, ceiling accounting
src/frozen.jl          FrozenVector, FrozenDict, FrozenRef, FrozenJSON, freeze!
src/names.jl           FullName, fullname algorithm, name validation, namespace resolution
src/admission.jl       SymbolAdmission (default process-wide table + caller-owned objects)
src/jsonscan.jl        lexical pre-scan (size/depth) and lazy JSON traversal helpers
src/schema.jl          Schema types, Field/Default, parser, validator, printer, equality
src/logical.jl         LogicalType structs, contextual validation, value types (Decimal, WideDecimal, Timestamp, LocalTimestamp, Duration)
src/canonical.jl       Parsing Canonical Form, CRC-64-AVRO, fingerprints
src/values.jl          Record, EnumValue, Fixed, UnionValue, the enumerated value set E
src/decoder.jl         Decoder + primitive reads/skips (strict walk, fast jump)
src/encoder.jl         Encoder + primitive writes
src/plan_read.jl       read plans (generic dynamic, typed, resolving, PlanRef)
src/plan_write.jl      write plans (NamedTuple/struct/row/dict/iterable/generic extraction)
src/prepared.jl        DatumReader, DatumWriter
src/columns.jl         column builders (Vector{ColumnBuilder}) and column plans
src/types.jl           Julia type <-> schema mapping (name policy, avroname/avrosymbol hooks), AvroStyle, StructUtils integration
src/resolution.jl      resolve(writer, reader) with work budget, reader-directed output
src/jsonencoding.jl    tojson/fromjson
src/compare.jl         sort order (values and bytes, canonical-encoding contract)
src/codecs.jl          codec registry, CRC32, deflate/snappy/zstandard, window caps, EOS/consumption contract
src/container.jl       Reader/Writer, header/block parsing, ceiling-permit parallel decode with streaming assembly, Avro.write, atomic paths
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
Avro.schema(T::Type; name=nothing, namespace=nothing) -> Schema      # Julia type → conventional schema (name policy §4.8; collisions/invalid names error)
Avro.avroname(::Type{T}) -> (name, namespace)                        # overridable naming hook for nested/parametric types
Avro.avrosymbol(::Type{E}, x::E) -> String                           # overridable enum symbol hook
Avro.schema(x)                                                       # value-level (identity-bearing generic values; ambiguous → ArgumentError)
Avro.schema(::Tables.Schema; name="Record", namespace="", names=Dict())   # table schema → record, optional column renames
Avro.schema(x::Avro.Table / Avro.Rows / Reader)                      # the file's writer schema
Avro.json(schema; pretty=false) / JSON.json(schema)                  # spec JSON text
Avro.canonical(schema) -> String                                      # Parsing Canonical Form
Avro.fingerprint(schema; algorithm=:crc64avro) -> UInt64 | Vector{UInt8} (md5/sha256)
Avro.parsingequivalent(a, b) -> Bool
Avro.resolve(writer, reader; union_resolution=:spec, limits=Limits()) -> ResolvedSchema
Avro.juliatype(schema) -> Type
Avro.fullname(named_schema) -> String
Base.:(==), Base.hash, Base.show on schemas
```

### 5.2 Datums

```julia
Avro.DatumReader(writer_schema, T=juliatype(effective reader); reader_schema=nothing, union_resolution=:spec, limits=Limits(), instants=:exact, validate=:strict, names=Avro.DEFAULT_ADMISSION)
    reader(bytes) -> value;  reader(bytes, pos) -> (value, nextpos);  reader(io)         # shareable; per-call state and budget
Avro.DatumWriter(schema; limits=Limits())
    writer(x) -> Vector{UInt8} (owned);  writer(enc_or_io, x)                           # single-owner (reusable encoder)
Avro.encode(schema, x; limits) -> Vector{UInt8};  Avro.encode!(enc_or_io, schema, x; limits);  Avro.encode(x; limits)   # one-shot conveniences
Avro.decode(writer_schema, src, T=…; reader_schema=nothing, union_resolution=:spec, limits=Limits(), instants=:exact, validate=:strict, names=…)
Avro.decode(writer_schema, bytes, pos::Int, T=…; kw...) -> (value, nextpos)
Avro.encodesingle(schema, x; limits); Avro.decodesingle(src, store; reader_schema=nothing, union_resolution=:spec, instants=:exact, validate=:strict, limits=Limits(), names=…, T=…)
Avro.tojson(schema, x; pretty=false, limits=Limits()) -> String;  Avro.fromjson(schema, json, T=…; strict=true, limits=Limits(), names=…)
Avro.compare(schema, a, b; limits=Limits()) -> Int;  Avro.comparebytes(schema, abytes, bbytes; limits=Limits(), validate=:strict) -> Int
Avro.UnionValue(i, x), Avro.EnumValue, Avro.Fixed, Avro.Record, Avro.Map, Avro.Decimal, Avro.WideDecimal, Avro.Duration,
Avro.Timestamp{P}, Avro.LocalTimestamp{P}, Avro.Time{P}, Avro.truncate(x, P), Avro.round(x, P)
```

### 5.3 Container files and tables

```julia
Avro.Table(src; reader_schema=nothing, union_resolution=:spec, ntasks=Threads.nthreads(), limits=Limits(), legacy=nothing, decimal_byteorder=:big,
           allow_invalid_names=false, allow_invalid_defaults=false, validate=:strict, mmap=true, instants=:exact, names=Avro.DEFAULT_ADMISSION)   # + `scan=` in a Scan-enabled release
    # Tables.jl columns table (record roots only); `Avro.metadata(t)`, `Avro.schema(t)`, `Avro.codec(t)`, `Avro.sync(t)`, `length`, `Tables.partitions` (per block)
Avro.Rows(src; T=nothing, reader_schema=nothing, union_resolution, limits, legacy, decimal_byteorder, allow_invalid_names, allow_invalid_defaults, validate, mmap, instants, names)
    # streaming datum iterator for any root schema (one decompressed block resident); Tables.rows/schema for record roots; `close`
Avro.Reader(src; limits, legacy, allow_invalid_names, allow_invalid_defaults, validate, mmap)   # block-level: header, `eachblock(r)` → (count, owned decompressed bytes), `eachdatum(r)`, `close`
Avro.Writer(io_or_path, schema; codec=:null, level=nothing, metadata=Dict(), sync=nothing, block_bytes=64*1024, atomic=true, fsync=false, limits=Limits())
    push!(w, datum); Base.write(w, datums); flush(w); close(w; abort=false)
Avro.write(dst, table; schema=nothing, codec=:null, level, metadata, block_bytes, name, namespace, atomic=true, fsync=false, limits=Limits()) -> dst
Avro.tobuffer(table; kw...) -> IOBuffer
Avro.codecs() -> available codec names
Avro.inspect(src; limits) -> diagnostic report (bounded permissive parse: codec, block count/sizes, padding/legacy issues, root schema, invalid-name/default findings)
Avro.SymbolAdmission(; max_names=1_000_000, max_bytes=64 << 20)   # caller-owned admission table; Avro.DEFAULT_ADMISSION is the process-wide one
```

### 5.4 Deprecated 1.x shims (`src/deprecated.jl`, removed in 3.0)

`Avro.readtable(src; kw...) → Avro.Table(src; legacy=:avrojl1)`; `Avro.writetable(dst, tbl;
compress=:zstd → codec=:zstandard)` (a schema-less source raises the §4.9 `ArgumentError`, since 1.x's
implicit `dictrowtable` inference is gone); `Avro.read(src, T_or_schema) → Avro.decode`; `Avro.write(x;
schema) → Avro.encode` (only the one-positional-argument datum form; `Avro.write(dst, table)` is the new
container writer, and `Avro.write(io, x)` where `x` is not a table raises a clear error pointing at
`encode!`); `Avro.parseschema` and `Avro.tobuffer` keep their names. All shims call `Base.depwarn`.

---

## 6. Tables.jl integration and `Tables.Scan` pushdown

* `Avro.Table` is a column table: `Tables.columnaccess`, `Tables.columns`, `Tables.schema` — **always a
  stored `Tables.Schema{nothing,nothing}`** for file-derived schemas, so untrusted field names and
  element types never become type parameters — `Tables.partitions` (one `Avro.Table` per block,
  materialised lazily), and the complete read-only DataAPI metadata interface
  (`DataAPI.metadatasupport`, `DataAPI.metadatakeys`, `DataAPI.metadata(t, key; style)`) for file
  metadata (DataAPI is a direct dependency; tested like Arrow's implementation). Columns are plain
  `Vector`s. Zero-column and zero-row tables keep an authoritative row count (`length`).
* **Symbol admission (the trust boundary).** Julia interns `Symbol`s permanently. An
  `Avro.SymbolAdmission` object (a lock-protected, seeded, deterministic-capacity string table — the
  `Avro.Map` machinery of §4.6, never `Base.Dict` — with `max_names`/`max_bytes`) decides
  which untrusted strings may be interned: Tables field names (`Tables.columnnames` must return
  `Symbol`s) **and** typed datum values decoded into `Symbol` on every typed path (`DatumReader`,
  `decode`, `decodesingle`, `fromjson`, `Rows`, `Table`; §4.8). `Avro.DEFAULT_ADMISSION` (1,000,000
  names / 64 MiB) is process-wide; callers may pass their own object via `names=` (per tenant, per job)
  or `names=:trusted` to bypass admission for trusted sources. Exceeding the budget raises `LimitError`;
  strings already admitted do not count twice. `Avro.Rows` admits field names **lazily**, only when
  `Tables.schema`/`Tables.columnnames` is actually requested, so generic datum iteration over untrusted
  files never interns anything. Tests exercise repeated files across operations with default,
  caller-owned, exhausted and `:trusted` admission, for binary and JSON paths.
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
  * **Projection**: only `b.columns` ∪ `b.filtercols` are decoded; unselected fields are skipped under
    the active validation mode (§4.3: strict walks, fast jumps); output order = `b.columns` order;
    `select=()` yields a zero-column table with the correct row count.
  * **Filter**: per block, pass 1 decodes only `b.filtercols` and records each datum's byte offset;
    `Tables.filtermask(b, subset_table)`; pass 2 decodes the selected columns for qualifying rows only.
  * **Filter in the parallel path** (`ntasks > 1`): header counts describe source rows, so a
    row-dependent filter runs as a **global filter pass** first — every block's filter columns are
    decoded (in parallel, under the §4.9 reservation rules; in `:strict` mode the remaining fields are
    walked, so validation is complete after this pass), per-block masks (`Vector{Bool}`, `rows` bytes,
    charged) and qualifying counts are retained — then `offset`/`limit` are applied to the qualifying
    prefix sums, the exact final capacity is preallocated, and a **selection pass** decodes the selected
    columns of qualifying rows only (blocks with no qualifying rows inside the window are skipped in both
    modes, having been validated in the first pass). Each pass is its own attempt scope under the §4.9
    rule — at most two attempts per block per pass, and the filter pass's retained masks are never
    recomputed by the selection pass — so a filtered block is decompressed at most four times against
    the sequential filtered scan's two: the same ≤ 2 × work ratio, gated with forced evictions under
    row-dependent filters. The sequential path keeps the per-block two-pass rule with
    explicitly reserved geometric column growth. Gate: identical results for `ntasks ∈ {1,2,8}` over
    low-selectivity filters, `offset`/`limit` across block boundaries, and near-ceiling files.
  * **offset / limit**: Tables' pipeline is filter → offset/limit → projection, so header counts
    describe *source* rows. **Without a filter** (or with a constant-true filter), whole blocks outside
    the row window are skipped by header `count`; **with a row-dependent filter**, qualifying rows are
    counted as blocks are decoded and no block is skipped by count. Materialisation stops at the limit
    in both cases; in `:strict` mode every remaining block is still decompressed and walked (validation
    never stops early), in `:fast` mode remaining blocks are not decompressed; offset beyond the
    qualifying row count yields an empty table.
  * **Type overrides**: exact widenings (`int→Int64`, `float→Float64`, `int/long→Float64`) are decoded
    directly; every other override is left to the residual.
  * **Residual**: filter, limit and offset are always pushed, so the residual carries only the unhandled
    overrides over the output names (`Tables.Scan(; select=…, validate=scan.validate)`, or the identity
    `Tables.Scan()`), applied with `Tables.scan(table, residual)` so the conversion rules are Tables' own.
    `Tables.describe(scan, residual)` works.
  * Gate: names, order, eltypes, `Tables.schema`, row count, and values identical to
    `Tables.scan(Avro.Table(src), scan)` for a generated matrix of scans (selection × filter × limit/
    offset × validate × overrides incl. supertypes and no-op subtype overrides), including multi-block
  files, empty records, `select=()`, missing-only filters, low-selectivity filters, and filters combined
  with offset/limit across block boundaries, for `ntasks ∈ {1,2,8}`, in both validation modes; malformed data inside projected-away fields and
    skipped blocks is tested under each mode's documented policy.
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
  schema-less sources need an explicit schema; invalid Julia-derived names are errors rather than
  silently emitted.
* **Known 1.x data defects** (files written by Avro.jl ≤ 1.1.2), reported by `Avro.inspect`: (1)
  null-codec blocks padded with uninitialised bytes after the last datum and (2) codec name `zstd` —
  both handled by `legacy=:avrojl1`; (3) `decimal` values written native-endian (little-endian on all
  supported platforms) as fixed-16 regardless of the schema's size — handled only by the explicit
  `decimal_byteorder=:little`; (4) `Duration` fields signed (identical bytes for values < 2^31); (5)
  record names `Record_<hash>`. Rewrite recipe: `Avro.write(dst, Avro.Rows(src; legacy=:avrojl1,
  decimal_byteorder=...); codec=...)`. The shims are tested against real 1.x files (generated with the
  pinned 1.1.2 in a separate environment and committed as fixtures).
* Deprecation shims for one major cycle; removal in 3.0.
* Behavioural guarantees stated in docs: the error guarantee (§4.13); **oracle readability** — every
  file Avro.jl writes from a schema that the Java 1.12.2 and fastavro 1.12.2 oracles accept is read by
  both (CI-enforced over the §8 matrix); the recorded exceptions, where Avro.jl follows the spec and an
  oracle does not, are unions with colliding JSON branch labels (Java rejects the schema), files
  carrying invalid-but-repaired legacy schemas (accepted only with the repair options), and user
  metadata values that are not valid UTF-8 (the spec's header is `map<string,bytes>` and Java accepts
  them; fastavro 1.12.2 decodes every metadata value as UTF-8 and raises `UnicodeDecodeError` — verified)
  — listed in `manual/limits-and-security.md`; the writer/reader invariant under identical limits (§4.4);
  thread-safety contract (§4.14), ownership contract (§4.9), the validation modes (§4.3), the
  symbol-admission boundary (§6). The package documents itself as an implementation of the Avro data
  format (schemas, encodings, container files), not of Avro RPC.

---

## 8. Interoperability and conformance strategy

### 8.1 Vendored Apache fixtures (`test/fixtures/apache/`, Apache-2.0 with LICENSE/NOTICE, commit pinned)

`schema-tests.txt` (PCF + CRC-64 fingerprints — parsed and executed in full, including the self-alias
cases 023/024), `interop.avsc`, `weather{,-deflate,-snappy,-zstd,-sorted}.avro`, `weather.avsc`,
`weather.json`, `syncInMeta.avro`, `schemas/simple`, `schemas/withUnion`, `messageV1` (single-object
bytes + schema), `reserved.avsc`, `TestRecordWithLogicalTypes.avsc`. `test.avro12` is an obsolete
pre-1.3 format (Java: "Not an Avro data file") and is excluded. The `interop/rpc` files are excluded
(not wire captures).

### 8.2 Generated corpus (`test/fixtures/generated/`, ~3 MB, committed; generator `test/fixtures/generate.sh` checked in)

Already produced in the authoring session and regenerated by the script: schemas `bench`,
`everything` (all kinds, recursive `LongList`, nested `Inner`, 13-branch union, every default kind incl.
bytes/fixed `\u00XX` strings), `wide` (100 fields), `empty` (zero-field record), `interop`, `logical`
(all logical-type fields incl. the deferred big-decimal carried as bytes); **non-record roots** for
**every kind** (`null`, `boolean`, `int`, `long`, `float`, `double`, `string`, `bytes`, enum, fixed,
array, map, union) for every codec; data from Java `random --seed` and hand-built JSON via `fromjson`
(boundary logical values), recodec'd by Java to `deflate`, `snappy`, `bzip2`, `xz` (`--level 6`),
`zstandard`, and written by fastavro for **all six codecs**; Java `tojson` expectations;
schema-evolution pairs with Java `ReadWithReader` expectations labelled `:java` plus **directly written
spec-policy expectations** (branch index + result type, reader representation) for the union cases;
single-object bytes from Java `BinaryMessageEncoder`; blocking-encoder datums (negative-count sized
blocks at every level) and cross-form array pairs (positive/positive, sized/sized, positive/sized; equal
and unequal); Java `canonical` forms and CRC-64/MD5/SHA-256 fingerprints for every schema; Java
`TimeConversions` vectors (49, incl. the spec's Helsinki example and the out-of-range nanos error);
sort-order verdicts from both Java comparators (51 cases normalised to `-1`/`0`/`1`/`ERROR:<class>`,
incl. every logical type, non-minimal decimal `00 00`/`00 7b`, mixed-case UUID, and `7f` vs `80` where
the encoded comparator says `-1` and the object comparator `1`); Java decimal edge vectors; real
Avro.jl 1.1.2 files (padding, `zstd`, decimals; five codecs); **high-window codec blocks** with a
three-datum payload: an xz block with a 1 GiB LZMA2 dictionary (fastavro reads it without a limit; the
documented success threshold is liblzma's reported requirement, 1 GiB + ≈ 64 KiB) and a zstd frame
advertising window log 30 (rejected by Python's own zstd module: "Frame requires too much memory"; its
reported decoder estimate is 1,074,231,088 bytes), both of which must fail under `max_codec_memory`
defaults and succeed at or above their library-reported thresholds; a
**one-million-empty-string single block** (134 bytes under zstandard, 1,071 under deflate, ≈ 1 MiB
decompressed) that must be accepted under defaults (no compressed-size work pre-check).

Additional matrix rows (Phase 4): empty file (header only), zero-datum blocks, many small blocks (1-datum
blocks), user metadata (including a non-UTF-8 value `x => ff ff` that Julia and Java accept and
fastavro is expected to reject), unknown codec, user `avro.*` keys (rejected), named-branch collisions in unions,
negative collection blocks, negative block count/size headers, work-rule blocks (huge count, tiny
decompressed size; the 32,768-empty-block file), default-writer files for every zero-size and
all-null-field root, multi-block files near the ceiling, maximal metadata and maximal schema (all must
read under defaults), codec bombs (zeros compressed with every codec), truncated compressed streams, a valid member followed by garbage for every codec, **concatenated valid
members** (two zstandard frames, two xz streams, two bzip2 streams — accepted; oracle behaviour measured and
recorded in §8.4), zstandard empty and skippable frames (before, between, after, only, truncated), xz
stream padding (4 and 8 zero bytes accepted; 1 byte or non-zero rejected), deflate bytes after `BFINAL`
(rejected), corrupt snappy CRC, boundary logical values, malformed content
inside skipped regions (for both validation modes).

### 8.3 Java harness (`test/interop/java/`, compiled in CI against the pinned jar)

`ReadWithReader` (reader-schema resolution → JSON), `SingleObject` (encode/decode), `BlockingEncode`
(sized blocks), `Compare`/`CompareBytes` (sort order, object-level and encoded), `LogicalCaps`
(capability probe), `TimeVectors` (time/timestamp conversions), `DecEdge` (decimal edges), `BigDec`
(deferred big-decimal vectors), `TimeRead` (in-JVM decode timing).

### 8.4 Oracle capability matrix (measured in the pinned environment; updated when tools move)

| Feature | Java 1.12.2 | fastavro 1.12.2 + cramjam 2.11.0 | avro-py 1.12.2 (as installed) | Spec 1.13-SNAPSHOT |
|---|---|---|---|---|
| codecs | null, deflate, snappy, bzip2, xz, zstandard | null, deflate, snappy, bzip2, xz, zstandard (+ non-spec lz4) | `KNOWN_CODECS` = null, deflate, bzip2 (snappy/zstandard need extra libraries; no xz) | all six |
| codec memory caps | xz/zstd defaults (no explicit cap) | xz unlimited; zstd via Python's module limit | n/a | n/a |
| decimal bytes/fixed | yes (no decode-side precision check; malformed attributes → underlying type with a warning) | yes | yes | yes |
| uuid string / fixed | yes / yes | yes / no | yes / no | yes / yes |
| date, time-millis/micros | yes | yes | date only | yes |
| timestamp-millis/micros, local-* | yes | yes | timestamp only | yes |
| timestamp-nanos, local-timestamp-nanos | recognised; conversions raise out of range | no | no | yes (spec vectors + Java vectors) |
| big-decimal | conversion class (negative scale accepted) | no | no | yes (conflicting on scale) — deferred |
| duration | recognised (raw) | no | no | yes |
| union branch identity (primitive reader unions) | observable (`ReadWithReader` JSON wrapper key) | **not observable** (`7 int`) | not observable | — |
| sort order of bytes/fixed | object-level signed (bug), encoded unsigned | n/a | n/a | unsigned |
| unions with colliding JSON labels | schema rejected | accepted | — | schema valid |
| self-alias (`name:"foo", aliases:["foo"]`) | accepted | accepted | accepted | silent |
| non-UTF-8 user metadata values | accepted (bytes) | **rejected** (`UnicodeDecodeError`; every value decoded as UTF-8) | accepted | `map<string,bytes>` |
| concatenated codec members in one block | zstandard: read; xz and bzip2: `EOFException` after the first stream (measured with avro-tools 1.12.2) | zstandard, xz, bzip2: read (measured) | restarts bzip2 decoders for concatenated streams | silent; the libraries define concatenation as valid input; Avro.jl reads them |
| deflate suffix bytes after `BFINAL` | ignored (measured) | ignored (measured) | — | silent; Avro.jl rejects (§4.9) |
| xz stream padding (all-zero, multiple of four) | read (measured) | rejected (measured) | — | required of concatenation-capable decoders by the xz format; Avro.jl reads it |

### 8.5 Live differential tests (`test/interop/`, run when `java`/`AVRO_TOOLS_JAR` and the pinned Python venv are available; a CI job on ubuntu installs Temurin 21, the checksummed jar, and CPython 3.14 (3.14.2 in the authoring venv) with `avro==1.12.2`, `fastavro==1.12.2`, `cramjam==2.11.0`)

For the schema matrix (every schema both oracles accept; the §7 exceptions are tested separately as
spec-over-oracle cases): (1) Julia-written container files for every codec read by Java `tojson` and
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
   limit in §3/§4.4 (positive and negative, incl. `Limits` constructor validation and cross-field
   relations, contextual logical-attribute handling, ignored-namespace cases, self-alias idempotence,
   union branch identity), every member of the value set `E` (an enumeration test asserts `E` is
   exactly the documented set), every §4.8 logical-type row, every §4.11 JSON row, `Fixed`/`EnumValue`/
   `UnionValue` equality and branch recovery, reader-directed union output in every direction, `minsize`
   on recursive schemas and the empty union, alias normalisation (`Bar` vs `a.Bar`), writer option and
   header validation, reserved-metadata rejection, the complete Julia-derived name policy (every
   component, every remedy, the exact sanitisation algorithm, collision errors incl. `Nothing`),
   union-member collision errors, decimal scale equality on encode.
2. **Spec examples**: the encoding tables and examples from the specification text are literal tests
   (`36 06 66 6f 6f`, `04 06 36 00`, `02 02 61`, the `Example` fullname schema, the LongList schema, the
   Helsinki timestamp example, the `Suit` enum default).
3. **Conformance**: §8.1/§8.2 fixtures; `schema-tests.txt` harness (100%); messageV1; blocking-encoder
   datums; every root kind for every codec; time/decimal/sort vectors; cross-form array comparisons;
   high-window codec blocks; the million-empty-string block.
4. **Round-trip properties with independent assertions**: a bounded random schema generator (all kinds,
   recursion, logical types, unions, aliases/defaults, props) with a matching random value generator;
   `decode(encode(x))` compared with `isequal` **and** bit-exact float comparison; `parseschema(json(s))`
   compared with structural `==` and per-attribute assertions (defaults, aliases, props, logical types,
   docs, order); `canonical(parseschema(canonical(s))) == canonical(s)`; resolved decode with
   `reader == writer` equals plain decode; columnar `Avro.Table` equals `Tables.columntable(Avro.Rows)`;
   frozen schemas reject every mutation attempt (incl. nested props/defaults); prepared readers/writers
   equal the one-shot API; `compare(schema, a, b) == comparebytes(schema, encode(schema, a),
   encode(schema, b))` for generated values of orderable schemas; `tojson`/`fromjson` round trips at
   the 1024/1025 depth boundary; **writer/reader invariant**: every generated table written under
   `Limits()` reads back under `Limits()`.
5. **Schema evolution**: a table-driven matrix of (writer, reader) pairs for every resolution rule and
   failure rule under both union policies with directly written expectations, including recursive pairs,
   ambiguous unions, alias collisions, mutable-default isolation (two decoded records never share a
   default object), invalid UTF-8 under `bytes→string`, decimal mismatch, every logical-vs-logical and
   logical-vs-plain pairing (with the documented reinterpretation hazard asserted), invalid-name/
   invalid-default (field and enum) repair through reader aliases, and a resolution-work-limit case
   (wide unions on both sides trip `max_resolution_work` quickly); Java and fastavro expectations where
   they can witness the result.
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
   single-column selection), resolution with skipped fields, and `comparebytes`, in both validation
   modes; `:strict` verdicts must agree except for the documented skipped-string UTF-8 exception;
   `:fast` differences must be exactly the documented ones (malformed content inside jumped sized blocks
   and un-decompressed skipped OCF blocks).
9. **Truncation / corruption of containers**: header truncation at every byte, sync mismatch, block size
   larger than the file, negative count/size, work-rule violations (incl. the 32,768-empty-block file),
   wrong snappy CRC, truncated and suffixed compressed streams for every codec, decompression bombs and
   high-window blocks for every codec against `max_block_bytes`/`max_codec_memory`/`max_total_bytes`
   (RSS-limited subprocess), trailing garbage, 1.x-style cushion blocks (strict vs legacy), header-only
   files (read and write), default-writer files for every zero-size and all-null-field root, near-ceiling
   multi-block files, maximal metadata and maximal schema (all readable under defaults), writer failure
   injection at every phase (encode, compress, write, flush, close, rename) with the §4.9 contract
   asserted, atomic replacement and rename-failure tests on every supported OS.
10. **Resource limits and budgets**: each `Limits` field has a test that trips it quickly (< 10 ms,
    < 1 MB allocated) and one that passes just under it; `max_depth` verified inside `Threads.@spawn`;
    cumulative budgets verified across blocks and under parallel decode (ordered commit); the corpus
    decodes under the fixed defaults; the work rule rejects the 23-byte/2^30-count block in microseconds
    and bounds the 32,768-block file; the **latency gate** (§4.4) holds on every supported Julia version;
    encode-side work and value rules reject a `Vector{Missing}` of 2^31 elements and a million-null array
    without iterating; the symbol-admission budget trips for field names and for typed `Symbol` values
    across repeated files on binary and JSON paths, a caller-owned admission object isolates tenants, and
    `names=:trusted` bypasses it; `Rows` iteration interns nothing; `mmap=false` streams a file larger
    than the ceiling without allocating it; the available-memory guard (injected value) fails before
    allocation; `Writer` construction rejects xz preset 9 and zstd level 22 under default limits and
    accepts them with raised limits, and every frame a default writer emits decodes under default
    `max_codec_memory` for every codec; the constructor relations are checked at equality and one byte
    beyond; the zstd `windowLog` selection and `fromFrame` verification at caps one byte below, at, and
    above every power-of-two estimate; `fixed(0)` with a decimal annotation parses to plain `fixed(0)`
    without a `DomainError`; a non-UTF-8 metadata value round-trips through Julia and Java; `storagebytes ≥ Base.summarysize` for
    generated values of every member of `E` (the representation-formula oracle); near-ceiling tables of
    empty `bytes`, `fixed`, wide decimals and nullable isbits columns are charged exactly; concatenated
    valid members decode for zstandard, xz and bzip2, zstandard skippable/empty frames and xz padding
    follow the §4.9 rules, and deflate rejects bytes after `BFINAL`; `Avro.Map` capacity and storage at
    n = 0, 1, 11, 16, 17, 43, 1024 and under adversarial equal-hash keys (memory unchanged, lookups
    correct, duplicate keys last-wins); schema/plan graphs near the ceiling (shallow wide schema, large
    defaults/props, large resolving plan) fail with `LimitError`; the `storagebytes` oracle with
    `exclude=Avro.Schema` for shared and distinct schema identities; output-buffer growth reserved
    before allocation; `Rows` streams more than the ceiling with unretained outputs and fails with
    retained ones only through the caller's own memory, never the operation ceiling; the `__init__`
    layout probes equal the recorded constants.
11. **Concurrency**: multi-block files decoded with `ntasks ∈ {1,2,8}` produce identical tables **and
    identical acceptance** (files that exceed the ceiling fail at the same block index for every
    `ntasks`); forced eviction schedules (a higher block holding memory the lowest block needs) complete
    with the sequential result; the peak-RSS gate runs under the fixed method of §4.9; two
    blocks failing concurrently (a higher index failing first) surface the lowest block index
    deterministically across 100 scheduled repetitions, for content/content, budget/content and
    content/budget pairs; a ceiling that admits exactly one of two blocks fails deterministically at the
    higher index under forced opposite schedules; measured peak RSS with eight workers on 16 MiB highly
    compressible blocks decoded from a caller-owned byte buffer faulted before the baseline (in-flight
    high-water counter ≥ 2), and on a table whose final columns exceed half the ceiling, stays within the
    ceiling plus documented runtime overhead and the run completes (liveness); forced eviction schedules
    show ≤ 2 attempts per block per pass and decompression/value counters ≤ 2 × the sequential run's
    (CPU time reported with the ≤ 2.5 × tolerance, informational), including row-dependent filtered
    scans; nullable `Int64`/`Float64`/`Date`/`UUID`
    columns near the ceiling are charged exactly (`Base.summarysize` oracle); GC-stress runs on the
    parallel path; writer flush/close/abort/poison ordering; caller-owned IO never closed.
12. **Allocation budgets**: measured with a per-Julia-version script (`@allocated` bytes after warm-up;
    allocation counts from `Base.gc_num` deltas / `@allocations`) on prepared readers/writers for
    primitive decode (0), typed record decode of isbits fields (0), column decode per row of isbits
    fields (0), string row (1 allocation per string); the one-shot API is measured separately
    (informational).
13. **Compile-cost gate**: §4.5 (zero new method instances after the `E` warm-up for random widths and
    orders, invalidations, RSS bound; native-code size informational).
14. **Quality gates**: Aqua (ambiguities, unbound type parameters, undefined exports, piracy, stale deps,
    compat bounds, `project_extras`), JET (`report_package` zero errors; `@test_opt` on hot paths),
    docstring coverage for every public name, Documenter doctests, a Julia 1.10 load/parse test,
    **source coverage** via `julia-processcoverage` uploaded to Codecov and **reported informationally**
    (explicitly not a gate; the conformance gates are the evidence).
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
-e …`), each reporting the best of 3 in-process repetitions after one warm-up; kernel gates use prepared
`DatumReader`/`DatumWriter` objects constructed outside the timed region; the one-shot API (`decode`/
`encode`, which constructs plans per call) is reported alongside as informational; load time is
`@elapsed @eval using Avro` in a cold process (median of 5); counters that a Julia version does not
expose are reported as "unavailable" and never silently pass. Scripts live in `benchmarks/` and
`test/perf/` and are version-gated by `VERSION`.

### 10.2 Targets

| Metric | Gate (same host, ratio vs 1.1.2) | Tracked absolute (named authoring host; informational) |
|---|---|---|
| `Avro.write` 1M rows null codec, 1 thread | ≥ 4× faster | ≤ 0.25 s (≥ 80 MB/s) |
| `Avro.Table` 1M rows null codec, 1 thread | ≥ 10× faster than 1.1.2 read+materialise | ≤ 0.30 s (fastavro 0.50 s, Java warm 0.05 s for reference) |
| `Avro.Table` 1M rows, 8 threads | ≥ 3× single-thread, measured on a named host with ≥ 8 physical cores **with `Limits(max_total_bytes = 4 GiB)` on both measurements** (default limits cap parallelism at what fits under 256 MiB) | — |
| codec read overhead | `Avro.Table` on a zstandard/deflate/snappy file ≤ 1.3× of (null-codec `Avro.Table` time + `transcode` time of the same block bytes with the same codec object), the codec kernel measured in isolation | — |
| Prepared typed single-record decode (3 isbits + 1 string field) | ≤ 1 allocation (the string) | ≤ 150 ns |
| Prepared typed single-record encode into a reused encoder | 0 allocations | — |
| One-shot `decode`/`encode` of the same record | — | reported (includes plan construction) |
| Worst-density legal input at default limits (latency gate, §4.4) | ≤ 10 s single-threaded on every supported Julia version | — |
| `parseschema(interop.avsc)` | — | ≤ 100 µs, ≤ 300 allocations |
| Projection `select=(:id,)` on the 4-column file (Scan-enabled release, `validate=:fast`) | ≥ 2× faster than full decode | strict-mode projection reported |
| Package load time | — | ≤ 0.5 s |
| Time-to-first-table on a fresh session | — | ≤ 1.5 s |

---

## 11. Engineering deliverables

* **Package metadata**: `Project.toml` version `2.0.0-DEV`; deps `JSON` (1.7), `StructUtils` (2.8),
  `Tables` (1.13; `[sources]` pin to the Scan SHA on the development branch only), `DataAPI` (1),
  `CodecZlib` (0.7), `CodecZstd` (0.8.7), `Zstd_jll` (1.5 — a direct dependency for the `ccall`s to
  the stable public estimators `ZSTD_estimateDStreamSize`, `ZSTD_estimateDStreamSize_fromFrame`,
  `ZSTD_estimateCStreamSize`, `ZSTD_getCParams` and the member delimiter `ZSTD_findFrameCompressedSize`; `__init__` checks the symbols with `Libdl.dlsym(…;
  throw_error=false)` and, if any is absent, `Avro.codecs()` omits `zstandard` and its use raises
  `UnsupportedCodecError` naming the symbol), `Snappy` (0.4), `TranscodingStreams` (0.11), `MD5` (0.2),
  stdlibs `Dates`, `UUIDs`, `Mmap`, `SHA`, `Random`, `Libdl`; `PrecompileTools` (1); weak deps
  `CodecBzip2` (0.8), `CodecXz` (0.7) together with `XZ_jll` (5.8; the xz extension triggers on
  `[CodecXz, XZ_jll]` and `ccall`s `lzma_easy_encoder_memusage`/`lzma_easy_decoder_memusage` for the
  writer's preset checks — the reader reserves the configured cap and relies on liblzma's `memlimit`
  enforcement, so no per-block estimator is needed — with the same symbol check at extension load),
  `TimeZones` (1); `[compat]` bounds for everything incl. `julia = "1.10"`;
  `test/Project.toml` with `Aqua`, `JET`, `Test`, `Tables`, `CodecBzip2`, `CodecXz`, `TimeZones`, `CSV`,
  `Arrow`, `DataFrames` (exact compatible versions recorded in the test Manifest). `SentinelArrays`,
  `JSON3`, `StructTypes` dropped. `public` names are declared through a version-gated
  `Core.eval(Expr(:public, ...))` so Julia 1.10 still parses the module. **Julia 1.10 bootstrap**: Pkg
  on 1.10 ignores `[sources]`, so the test/CI setup on the development branch adds Tables explicitly with
  `Pkg.add(PackageSpec(url=..., rev=<full SHA>))` (Arrow 3.0's precedent); release archives need nothing
  special.
* **Licensing**: MIT (unchanged) for the package; `test/fixtures/apache/LICENSE` (Apache-2.0) + `NOTICE`
  attribution in `README.md`; the specification text is referenced, not vendored.
* **Docs** (Documenter): `index.md` (quick start), `manual/schemas.md`, `manual/encoding.md`,
  `manual/container.md`, `manual/tables.md`, `manual/evolution.md`, `manual/logicaltypes.md`,
  `manual/singleobject.md`, `manual/sortorder.md`, `manual/limits-and-security.md` (fixed defaults and
  how to raise them on every side, the ceiling, work rule and latency constants, codec windows,
  validation modes, symbol admission, error guarantee, oracle-readability exceptions, concurrent
  operations), `manual/performance.md` (prepared codecs), `migration.md`, `benchmarks.md`,
  `reference.md` (autodocs). `examples/`: CSV→Avro→Arrow pipeline, Kafka-style single-object
  producer/consumer with a `SchemaCache`, schema evolution walkthrough, struct mapping with StructUtils.
* **CHANGELOG.md** (Keep-a-Changelog style) with the 2.0.0 entry.
* **CI** (`.github/workflows/ci.yml`): required jobs — Julia `1.10`, `1.11`, `1` on ubuntu/macos/windows
  with `JULIA_NUM_THREADS ∈ {1, 4}`; docs build; Aqua+JET; interop (ubuntu: Temurin 21 + checksummed jar
  + CPython 3.14 with pinned Python packages). Informational jobs — coverage (processcoverage + Codecov, no threshold);
  `pre` and `nightly` on ubuntu only; `juliac --trim` smoke compile of a reader program; Arrow 3
  candidate smoke. `TagBot`, `CompatHelper`. Trigger on `push: branches: ['**']` plus `pull_request`
  with a same-repo duplicate guard.
* **Precompile**: PrecompileTools workload covering schema parse/print/canonical/fingerprint, prepared
  and one-shot encode/decode of a representative NamedTuple and struct, container round trip with
  null/deflate/zstandard/snappy, `Avro.Table` (with a `Scan` in a Scan-enabled release), `Avro.Rows`,
  single-object, JSON encoding; budget ≤ 15 s precompile, ≤ 0.5 s load.
* **Trim**: no `eval`/`@generated` on runtime data, no `Symbol`-to-function lookups; `test/trim/` smoke.
* **Runtime layout probes**: `__init__` measures the storage constants of §4.4 (b) from canonical probe
  objects with `Base.summarysize` and stores them in a module-level `const Ref`; a test asserts the
  recorded 1.10/1.12 values on every CI version, and the formulas never assume a layout the running
  Julia has not confirmed.
* **Code style**: AGENTS.md rules (explicit `return`, guard clauses, `T[]`, `@atomic`, `errormonitor`,
  small functions, whitespace discipline); no formatter enforcement.

---

## 12. Phased milestones and gates

Each phase lands as small local commits with tests; `Pkg.test()` of the new, phase-scoped suite passes
on Julia 1.10 and 1.12 at the end of every phase (and nothing more is implied by that).

| Phase | Scope | Acceptance gate |
|---|---|---|
| **0 — Foundation** | branch; legacy code removed; `Project.toml` 2.0.0-DEV + pinned deps (Tables SHA + 1.10 bootstrap, jar checksum, Python pins incl. cramjam, CodecZstd ≥ 0.8.7); `test/Project.toml` incl. CodecBzip2/CodecXz; CI skeleton incl. informational coverage; vendored + generated fixtures with licence and generator script (every root kind, fastavro all codecs, real 1.x files, time/decimal/sort vectors normalised, cross-form arrays, high-window codec blocks with liblzma's reported threshold and the window-log-30 description, the million-empty-string block); Java harness; `errors.jl`, `limits.jl` (validated; fixed defaults; ceiling accounting; available-memory guard), `frozen.jl`, `values.jl` skeleton, `admission.jl`; benchmark harness with raw-logged 1.1.2/fastavro/Java baselines and the measurement protocol; Julia 1.10 load test; `public` gating; codec-cap feasibility (verified) through the library estimators (`Zstd_jll` direct, `XZ_jll` weak, symbol capability checks) with the one recorded cap meaning | `Pkg.test` skeleton green on 1.10 and 1.12; fixtures licensed and regenerable; baseline logs committed |
| **1 — Schema model** | JSON pre-scan + lazy traversal, fullname algorithm before validation, parser/validator (all §3 rules incl. self-alias idempotence and union branch identity, `fixed.size` syntax, contextual logical attributes with `Int` precision/scale, §4.4 schema limits, repair options incl. enum defaults), defaults as frozen JSON, transitive freezing with freeze-time hashes, normalised structural `==`, printer, canonical form, fingerprints, the complete Julia-derived name policy with `avroname`/`avrosymbol` hooks and collision errors, `minsize`, `inspect`'s permissive diagnostic parse, schema-graph budgeting (§4.4 category (e)) and the shared-object storage oracle | `schema-tests.txt` **100%** (incl. 023/024); every constraint and limit tested positive/negative; duplicate-key, depth, recursion, equality-on-cycles and transitive-immutability gates; repair options round-trip invalid legacy schemas incl. enum defaults; Java-accepted metadata/invalid-logical/ignored-namespace/self-alias cases accepted identically; `canonical`/`fingerprint` equal to Java for all fixture schemas; Aqua+JET clean |
| **2 — Binary core** | Decoder/Encoder with checked arithmetic, both validation modes, the shared work rule and every cumulative limit on encode and decode, generic dynamic plans with `PlanRef`, the enumerated value set `E` with `Avro.Map` and the representation storage formulas (`storagebytes`, init-measured constants, oracle-asserted), typed plans with `AvroStyle` eligibility and Symbol admission on every typed path, prepared `DatumReader`/`DatumWriter`, column builders in schema-independent containers, logical values with decode-side checks and `Int` scale, bounded JSON datum encoding in both directions with the common `max_json_depth`, single-object + `SchemaCache` ambiguity rules, value-level `Avro.schema(x)`, `ConversionError`, **the latency gate measurement that fixes the work constants** | spec-example tests; round-trip properties with independent assertions; 1.x round-trip cases ported; Java `fragtojson`/`jsontofrag` differential on deterministic datums + semantic comparison on collections; blocking-encoder fixtures; fuzz 100k mutations clean in sandboxed batches under the split gate; acceptance-equivalence tests in both modes; limit/budget/work-rule/encode-work tests; latency gate ≤ 10 s on every supported Julia version with constants recorded; allocation budgets on prepared objects; messageV1; numeric compile-cost gate with `E` warm-up over random widths/orders |
| **3 — Resolution and order** | `resolve` with both union policies, reader-directed output and the resolution work budget, resolving plans (memoised pairs), aliases, defaults (incl. invalid-default repair), enum defaults by symbol, decimal rule and all other logical pairings, `Avro.compare`/`comparebytes` (budgeted, exact consumption, canonical-encoding contract, cross-form arrays) | resolution matrix with direct expectations (both policies, every output direction, every logical pairing, work-limit case) plus Java/fastavro where observable; sort-order vectors from both Java comparators incl. NaN/−0.0/ignore/maps/signed-bytes deviation/logical types/non-minimal decimal/mixed-case UUID/cross-form arrays; `compare`/`comparebytes` agreement property |
| **4a — Strict containers and codecs** | Reader/Writer/`Avro.write` with strict validation (lower bounds, framing-credited work rule evaluated after decompression with no compressed-size pre-check, codec decoder caps with liblzma/zstd thresholds, EOS/consumption contract), streamed `mmap=false`, atomic path contract, full writer failure contract, option/header/schema validation against reader limits, **writer-side codec workspace charging and emitted-frame decoder-requirement checks (zstd `windowLog` selection + per-frame `fromFrame` verification)**, work-rule-driven block flushing, reserved-metadata rejection, codecs (+ extensions), legacy mode, `decimal_byteorder`, repair options, `Avro.inspect`, any-root `eachdatum`/`eachblock` (owned decompressed bytes), explicit-schema requirement for schema-less sources | Apache corpus reads; Java **and** fastavro read Julia files — checked after each codec lands; Julia reads Java and fastavro files for every codec and every root kind; the million-empty-string block reads under defaults; 1.x fixtures read under legacy options and rejected under strict; truncation/corruption/bomb/high-window/EOS/suffix/negative-header/work-rule/empty-file/zero-datum/metadata tests; **writer/reader invariant tests** (every zero-size and all-null-field root, near-ceiling multi-block files, maximal metadata and schema) pass under the fixed defaults; writer failure-injection and atomic-replacement tests on every OS; `mmap=false` streams a file larger than the ceiling; the non-UTF-8 metadata fixture (Julia/Java accept, fastavro rejects); concatenated-member blocks accepted for zstandard/xz/bzip2 with per-member caps, zstandard skippable frames and xz stream padding accepted per §4.9, deflate `BFINAL` suffix rejected, oracle behaviour recorded |
| **4b — Tables basics and ownership** | `Avro.Table` (sequential), `Avro.Rows` (record and non-record roots, lazy admission), partitions, DataAPI metadata interface, stored schemas, exact column-storage charging (isbits-union tag bytes) with reserved geometric growth on streamed sources, symbol admission objects (names and typed values, binary and JSON), zero-column/zero-row behaviour, close semantics | `Table == columntable(Rows)` property; ownership/close tests (caller IO untouched); Tables and DataAPI interface tests; symbol-admission tests incl. caller-owned objects and repeated files |
| **4c — Parallel decode** | block pre-scan with exact final-column preallocation (exact Julia storage incl. tag bytes), actual-size reservations before every allocation with lowest-block priority and cooperative, acknowledged eviction of higher blocks (at most two attempts per block; discarded counters rolled back), per-block local budgets, streaming ordered assembly with ordered cumulative commits, lower-blocks-never-abandoned cancellation, lowest-index error selection across failure kinds | identical results **and acceptance** for `ntasks ∈ {1,2,8}` under default limits; deterministic failure selection over forced schedules for every failure-kind pairing incl. evictions; GC-stress; peak RSS under the fixed §4.9 method (`peak − baseline ≤ effective ceiling + 128 MiB`, caller-owned faulted byte buffer, in-flight high-water ≥ 2) on highly compressible 16 MiB blocks with eight workers and on half-ceiling tables, with liveness; attempt counter ≤ 2 per block per pass and decompression/value counters ≤ 2 × sequential under forced evictions (CPU time informational, ≤ 2.5 × tolerance); worker pool bounded and charged |
| **4d — Scan and performance** | `src/scan.jl` against the pin (`Tables.resolve`, residual with retained overrides, both validation modes, filter-aware offset/limit, the global filter pass before exact preallocation in the parallel path), projection skipping, performance work | Scan equivalence matrix in both modes incl. filters across block boundaries and malformed data in projected-away fields and skipped blocks; §10.2 ratio gates on the named host under the measurement protocol |
| **5 — Release engineering** | shims, docs (incl. limits guidance and the oracle-readability exceptions), examples, changelog, precompile workload, trim smoke, benchmarks doc, README, CI matrix live, coverage report, cross-package smoke | docs build without warnings; load-time budget; full local matrix green on 1.10/1.11/1.12 (+1.13-rc if installed); Aqua/JET; interop job green locally |

Readiness levels (this task stops at **PR-ready**; nothing is pushed):

* **Review-ready**: clean exact local head; scoped diff; local supported-version tests, docs, licensing,
  and interop artifacts complete.
* **PR-ready**: review-ready plus a reproducible branch and exact dependency pins (`[sources]` SHA with
  the 1.10 bootstrap, jar checksum, Python pins, test Manifest) and the status record (§15) up to date.
* **Merge-ready**: hosted exact-head CI green and review issues resolved.
* **RC-ready**: the Scan release rule (§6) applied (ship with a minimum registered Tables, or remove
  `src/scan.jl`/the keyword); `[sources]` removed; **a clean environment instantiated from the exact RC
  source archive re-runs the complete matrix** (supported Julia versions, all codecs, interop, fuzz,
  resource gates, docs, cross-package) and the release tag tree must equal that tested tree; lower
  compat bounds resolve; registered dependencies exist; reverse dependencies/PkgEval checked.
* **Release-ready**: exact version/tag state approved; registration and docs deployment are release
  actions, not evidence of readiness.

---

## 13. Correctness, security, performance, allocation, streaming, concurrency goals (summary)

* Correctness: every spec rule in §3 has a test; all Apache fixtures pass (`schema-tests.txt` 100%);
  Java and fastavro read every file we write from oracle-accepted schemas (subject to the §7 recorded
  exceptions); we read every file they write
  (all six codecs, every root kind); oracle-verified resolution (both policies, reader-directed output)
  and sort order (incl. logical types, canonical-encoding contract).
* Security: checked arithmetic, no `@inbounds` without a proven bound, no allocation sized by untrusted
  counts, one fixed-default per-operation ceiling covering final-column capacity, committed payload and
  actual-size in-flight reservations (lowest-block priority with cooperative eviction, at most two attempts per block per pass, exact
  column storage incl. isbits-union tag bytes, streaming assembly into preallocated columns, a liveness
  argument, acceptance identical to sequential, CPU work ≤ 2 × sequential), an available-memory
  guard, writer-side codec workspace and decoder-requirement checks, an input-proportional work rule with latency-gated constants enforced identically
  on encode and decode, decode-side codec window caps, a resolution work budget, bounded
  recursion/schema/metadata/JSON/symbol admission (names and typed values, binary and JSON), streamed
  path sources, strict validation by default with a documented fast mode, codec EOS/consumption
  contract, the §4.13 error guarantee; fuzz-clean under sandboxed batches.
* Performance: §10.2 on prepared codecs; plans compiled per (schema, T) only on request;
  zero-allocation primitives; column-major materialisation; projection skips; block-parallel decode
  with bounded memory.
* Streaming: `Rows`/`eachdatum`, `Writer`, `IO` and streamed path sources with one block resident;
  partitions for pipelines.
* Concurrency: §4.14 contract; no global mutable state beyond the documented default admission table;
  structured task lifecycle with deterministic failure selection across failure kinds.

---

## 14. Decisions, open risks, and review log

Decisions (each with rationale; reviewers may challenge any):

1. `missing` is the Julia value of `null`; `nothing` encodes as null.
2. Generic enums are `Avro.EnumValue` (equality by fullname + symbol; no interning); typed targets may
   choose `Base.Enum`/`Symbol` (through admission)/`String`.
3. Generic records are always `Avro.Record`; generic fixed values are `Avro.Fixed`; typed `T` is opt-in.
4. All timestamp logical types decode to exact `Avro.Timestamp{P}`/`Avro.LocalTimestamp{P}` wrappers;
   `DateTime` conversion is explicit, range-checked (`ConversionError`) and documented
   (`instants=:datetime`); a naive `DateTime` derives `local-timestamp-millis`.
5. Decimals carry runtime `Int` scale (`Avro.Decimal` Int128 ≤ 38 digits, `Avro.WideDecimal` beyond);
   digit counts are validated on both encode and decode (stricter than Java on decode); encode requires
   exact scale equality.
6. Codec dependency split: `deflate`/`snappy`/`zstandard` hard, `bzip2`/`xz` extensions;
   `max_codec_memory` has one meaning — the cap on the complete decoder memory requirement of one codec
   member as reported by the library (liblzma `memlimit`; `ZSTD_estimateDStreamSize_fromFrame`) —
   enforced on every member read and written (decision 28); it is not a process-memory cap; the xz
   workspace reservation is the cap itself because liblzma reports nothing before allocating.
7. Strict container validation is the default; `legacy=:avrojl1` covers only the two unambiguous 1.x
   tolerances; decimal byte-order reinterpretation is explicit-only.
8. `Avro.write(dst, table)` is the container writer; datum writing is `encode`/`encode!`/`DatumWriter`;
   schema-free `encode(x)` uses the documented conventional schema and is not a round-trip mechanism.
9. `Tables.Scan` support ships only in a release that requires a registered Tables with `Scan`; no
   dormant or runtime-gated Scan code.
10. Default block size 64 KiB, additionally bounded by the block work rule; positive-count array/map
    blocks written; both forms read.
11. RPC, big-decimal, schema inference, append, writer-side parallel compression, and borrowed byte views
    are deferred to 2.x with their requirements recorded (§3, §4.9).
12. **Limits are fixed, portable defaults** (256 MiB ceiling), enforced identically by writers and
    readers so that default writer output is default-readable by construction; an available-memory
    guard lowers the effective ceiling in constrained processes (a safety valve outside the invariant);
    raising a limit is an explicit, documented, both-sides decision; work constants are provisional until
    the Phase 2 latency gate fixes them (§4.4).
13. Union-branch selection during resolution is spec-normative (first match including promotion) by
    default; `union_resolution=:java` is an explicit compatibility option; output representation follows
    the reader schema.
14. Only two-branch nullable unions decode bare; every other union decodes as `Avro.UnionValue`; branch
    recovery prefers exact representation before coercion.
15. Booleans other than 0/1 and invalid UTF-8 strings are rejected (spec over Java leniency); skipped
    strings are the documented UTF-8 exception in strict mode; `validate=:fast` is opt-in with documented
    blind spots.
16. Header-only container files are accepted and written (Java-compatible extension of "one or more
    data blocks").
17. Generic collections are typed one level deep, `Any`-typed beyond, to keep the value set finite;
    generic maps are `Avro.Map` (package-owned, seeded, deterministic capacity), not `Base.Dict`;
    column builders live in schema-independent containers.
18. Byte order for `bytes`/`fixed` comparison is unsigned (spec) even though Java's object-level
    comparator is signed; logical types sort by their underlying encoding; native-value comparison uses
    the canonical encoding (agreement with `comparebytes` is guaranteed for Avro.jl-produced encodings).
19. Symbol admission is budgeted through admission objects (default process-wide) covering Tables field
    names and typed `Symbol` values on every typed path incl. JSON, with `names=:trusted` and
    caller-owned objects as alternatives (§6); `Rows` admits lazily.
20. Different recognised logical types on matching underlying schemas resolve through the underlying
    schema with the reader's interpretation (no unit conversion; documented hazard).
21. JSON union-label collisions are accepted at the schema level and rejected at JSON conversion time
    (Java rejects the schema; fastavro accepts silently); excluded from the oracle-readability promise.
22. Parallel decoding runs under the single operation ceiling with actual-size reservations before every
    allocation, lowest-block priority with cooperative, acknowledged eviction of higher blocks (at most two
    attempts per block per pass — four decompressions absolute for a filtered scan's two passes;
    discarded attempts roll back their counters), exact final-column preallocation and streaming
    ordered assembly; acceptance is identical to sequential decoding, deterministic work is at most
    twice the sequential bound, and the lowest failing block index wins regardless of failure kind.
23. Only `fixed.size` among the optional numeric attributes is schema syntax; logical-type attributes are
    evaluated in context and malformed ones drop the annotation (Java-compatible); unknown attributes
    are never validated.
24. Julia-derived names follow one complete policy (§4.8): every component must already be a valid Avro
    name, remedies are tags/hooks/explicit schemas, parametric types get parameter-aware names, and
    fullname or union-member collisions are errors, never silent merges.
25. Quoted non-finite float strings are accepted in JSON defaults and datums (Java-compatible extension
    of the JSON grammar); bare tokens only in permissive mode.
26. A self-alias is idempotent (Java-compatible; required by the Apache canonical-form fixture).
27. Path sources are memory-mapped or streamed, never read whole into memory; caller-owned byte sources
    are not charged to the ceiling.
28. The writer charges its compressor workspace to its ceiling and refuses codec levels whose emitted
    frames exceed `max_codec_memory` (zstd windows are set explicitly; xz presets are checked with
    liblzma's reported decoder requirement), so identical limits guarantee readability.
29. Type aliases are normalised to fullnames at parse time; the empty union is a valid schema with no
    datum; `Nothing` maps to `null` at the type level.
30. Memory is charged in four disjoint categories (§4.4): storage shells at their exact Julia size
    (element size plus the isbits-union tag byte plus the array header), referenced payload by
    representation-specific formulas asserted `≥ Base.summarysize` for every member of `E`, input/codec
    buffers, and internal tables incl. worker state; payload ownership transfers at commit without
    re-charging.
31. Filtered scans in the parallel path run a global filter pass before exact preallocation; sequential
    paths keep per-block two-pass decoding with reserved geometric growth.
32. Non-UTF-8 user metadata is written as the spec allows and recorded as a fastavro readability
    exception; `Zstd_jll` is a direct dependency and `XZ_jll` a weak one so the codec estimators are
    reached through pinned, symbol-checked `ccall`s rather than package internals.
33. Multi-member codec payloads (zstandard frames, xz streams, bzip2 streams) are decoded member by
    member to exact exhaustion with per-member caps; deflate bytes after `BFINAL` are rejected (Java and
    fastavro ignore them; no writer emits them); truncated members and garbage suffixes are errors.
34. Parallel decoding uses a fixed worker pool of `min(ntasks, Threads.nthreads(), inflight)` tasks
    whose state is charged; the parallel work bound is gated in deterministic units (attempts,
    decompressions, values) and CPU time is informational with a predeclared tolerance.
35. Generic maps and the symbol-admission table use a package-owned seeded map with deterministic,
    never-rehashed capacity, so no memory charge depends on `Base.Dict`'s layout or collision behaviour;
    typed `Dict` targets are built outside exact accounting and documented as approximate.
36. Schema and plan graphs are charged (category (e)) under the budget of the operation that builds
    them; storage constants are measured at `__init__`; the storage oracle excludes `Avro.Schema`
    references, which are charged once with the graph.
37. Zstandard skippable frames and xz stream padding are accepted as the codec formats require; the
    measured oracle behaviour for concatenated members, padding and deflate suffixes is recorded in
    §8.4 and does not affect the readability promise (Avro.jl writes single members without padding).

Intentionally unresolved risks:

* The `Avro.Record` generic row boxes isbits fields; the typed path and `Avro.Table` are the fast paths.
  If `Rows` throughput for generic rows proves inadequate, an unboxed layout is a 2.x addition.
* The per-cell dynamic dispatch in the column path (no tuple unrolling) is accepted for 2.0; if the
  §10.2 ratio gates fail because of it, a per-leaf-type batched fast path is the pre-agreed fallback.
* Tables.jl `Scan` release timing is outside this repository (handled by the release rule in §6).
* Worker-task stack depth vs `max_depth=1024`: 500k frames were measured on macOS; Linux/Windows are
  measured in Phase 2 and the default lowered if needed.
* The fixed 256 MiB default ceiling will be too small for many large-table users and must be raised
  explicitly on both sides; the error message and manual make this a one-line change. The ceiling is an
  estimate of package-owned memory, not a hard OS limit; `Sys.free_memory()` is host-wide on Julia ≤
  1.12, so the guard is weaker inside cgroups than `Sys.total_memory()` alone suggests.
* Eviction under memory pressure wastes work (bounded: at most one discarded attempt per block, so CPU
  work ≤ 2 × sequential); the design trades throughput for identical acceptance and bounded memory, and
  the measured parallel speedups are reported with the explicit limits used.
* The codec estimators are library-reported numbers for the pinned `Zstd_jll`/`XZ_jll` versions; a
  future library release may change them, which the symbol checks cannot detect — the threshold tests
  pin the behaviour and fail loudly on a change.
* The provisional work constants may change at the Phase 2 latency gate; the defaults in this plan are
  the starting point, not the commitment.

Review log:

* **Round 1** (`reviews/codex-review-1.md`, 29 findings: 8 blockers, 20 majors, 1 minor; `VERDICT:
  REVISE`). All 29 adopted, 5 with amendments; dispositions in `reviews/response-1.md`.
* **Round 2** (`reviews/codex-review-2.md`: 12 round-1 items resolved, 16 partially, 1 not; 16 new
  findings: 2 blockers, 12 majors, 2 minors; `VERDICT: REVISE`). All adopted; dispositions in
  `reviews/response-2.md`.
* **Round 3** (`reviews/codex-review-3.md`: 10 of 17 open items resolved, 7 partially (2 flagged
  blocker-level); 19 new findings: 11 majors, 8 minors; `VERDICT: REVISE`). All adopted; dispositions in
  `reviews/response-3.md`.
* **Round 4** (`reviews/codex-review-4.md`: 5 of 8 carried rows resolved, 3 partially; round-3 findings
  16 of 19 resolved, 3 partially; 20 new findings: 2 blockers, 10 majors, 7 minors, 1 nit; `VERDICT:
  REVISE`). All adopted; dispositions in `reviews/response-4.md`.
* **Round 5** (`reviews/codex-review-5.md`: carried rows 6 of 8 resolved, 2 partially; round-4 findings
  15 of 20 resolved, 5 partially; 15 new findings: 2 blockers, 8 majors, 4 minors, 1 nit; `VERDICT:
  REVISE`). All adopted; dispositions in `reviews/response-5.md`.
* **Round 6** (`reviews/codex-review-6.md`: carried rows 6 of 8 resolved, 2 partially; round-4/5 carried
  findings all but 6 resolved; 13 new findings: 3 blockers, 7 majors, 2 minors, 1 nit; `VERDICT:
  REVISE`). All adopted; dispositions in `reviews/response-6.md`. Major changes: fixed portable
  defaults and one per-operation ceiling (§4.4); coordinator-issued permits in block order with full
  worst-case reservation, streaming ordered assembly and a liveness argument (§4.9); writer enforcement
  of every cumulative reader limit incl. metadata and schema limits, with the invariant stated under
  identical limits (§4.4, §4.9); no compressed-size work pre-check and the million-empty-string fixture
  (§4.4, §8.2); streamed `mmap=false` (§4.9); self-alias idempotence and union branch identity (§3,
  §4.2); `names=` on `fromjson` (§4.11, §5.2); the complete Julia-derived name policy with
  `avroname`/`avrosymbol` hooks (§4.8, §5.1); liblzma and clamped zstd thresholds (§4.9, §8.2); the
  latency gate for provisional work constants (§4.4, §10.2, §12); common `max_json_depth` (§4.4,
  §4.11); attribute wording (§4.2); shared allowance counter, failure-kind precedence, comparison
  property scope, `eachblock` decompressed bytes, decode-only codec cap (§4.4, §4.9, §4.12).
* **Round 7** (`reviews/codex-review-7.md`: carried rows all resolved except the resource model; 10 new
  findings: 2 blockers, 3 majors, 5 minors; `VERDICT: REVISE`). All adopted; dispositions in
  `reviews/response-7.md`. Major changes: smaller fixed defaults (256 MiB ceiling, 16 MiB blocks, 32 MiB
  codec cap with a 16 MiB floor) plus an available-memory guard (§4.4); actual-size reservations before
  every allocation, lowest-block priority with eviction, exact final-column preallocation, streaming
  assembly without reallocation, acceptance identical to sequential, fixed peak-RSS method, and the
  explicit limits used for the thread gate (§4.9, §10.2); writer-side compressor workspace charging and
  emitted-frame decoder-requirement checks using liblzma/libzstd estimate functions (§4.9, §4.3);
  constructor relations that guarantee the first unit of progress (§4.4); empty-union semantics and
  alias normalisation (§4.2); `Nothing → null` and the exact sanitisation algorithm (§4.8); streamed
  compressed buffers reserved (§4.9).
* **Round 10** (`reviews/codex-review-10.md`: 9 new findings — 4 majors, 5 minors — plus follow-ups;
  `VERDICT: REVISE`). All adopted; dispositions in `reviews/response-10.md`. Changes: `Avro.Map`
  replaces `Dict{String,x}` in `E` (seeded, deterministic capacity, no rehash) and the admission table
  uses the same machinery (§4.4, §4.6, §6, decision 35); schema/plan graphs become accounting category
  (e) with new budget scopes (§4.4, decision 36); the storage oracle excludes `Avro.Schema` and the
  constants are measured at `__init__` (§4.4, §11); xz stream padding and zstandard skippable/empty
  frames accepted, `ZSTD_findFrameCompressedSize` required, measured oracle rows for concatenation,
  padding and deflate suffixes (§4.9, §8.2, §8.4, decision 37); stale `length + 32`, xz-estimate and
  "twice per block" clauses replaced by the authoritative rules (§4.9, §4.14, §13, decision 22); output
  buffers and yield-time ownership transfer specified (§4.4); the RSS primitive supplemented by OS and
  allocator high-water marks with the reservation hook as the primary gate (§4.9).
* **Round 9** (`reviews/codex-review-9.md`: 8 new findings — 5 majors, 3 minors — plus follow-ups;
  `VERDICT: REVISE`). All adopted; dispositions in `reviews/response-9.md`. Changes: four disjoint
  accounting categories with representation-specific, oracle-asserted storage formulas measured on
  Julia 1.10 and 1.12, maps materialised from vectors with one `sizehint!`, payload ownership transfer
  at commit (§4.4, §4.9, decision 30); the xz workspace reservation is the configured cap (§4.4, §4.9,
  decision 6); multi-member codec payloads decoded to exact exhaustion with per-member caps, deflate
  `BFINAL` suffix rejection, fixtures and oracle rows (§4.9, §8.2, §8.4, decision 33); separate attempt
  scopes per Scan pass with the same ≤ 2 × ratio (§6, §4.9); the parallel work gate restated in
  deterministic units with CPU time informational (§4.9, §9.11, §12, decision 34); a fixed, charged
  worker pool (§4.9, decision 34); the peak-RSS sampling primitive (§4.9); §13 qualified by the §7
  exceptions.
* **Round 8** (`reviews/codex-review-8.md`: 8 new findings — 6 majors, 2 minors — plus follow-ups;
  `VERDICT: REVISE`). All adopted; dispositions in `reviews/response-8.md`. Changes: exact Julia column
  storage incl. isbits-union tag bytes, storage vs payload separation (§4.9, decision 30);
  `max_codec_memory` given one meaning — the library-reported complete decoder requirement — with
  `ZSTD_estimateDStreamSize_fromFrame` on read and on every emitted frame, writer `windowLog` selection
  by the estimate, and in-house xz header estimates cross-checked against liblzma (§4.4, §4.9, decision
  6); cooperative, acknowledged eviction with at most two attempts per block, rolled-back counters and a
  ≤ 2 × work bound (§4.9, decision 22); the peak-RSS gate run from a caller-owned faulted byte buffer
  with an in-flight high-water assertion (§4.9, §9, §12); the non-UTF-8 metadata fastavro exception and
  fixture (§7, §8, decision 32); the global filter pass for filtered parallel scans (§6, decision 31);
  `fixed(0)` decimal handling (§4.2); `Zstd_jll`/`XZ_jll` pinned estimator access with symbol checks
  (§11); task cap and streamed column growth stated (§4.9); stale 8 MiB/64 MiB wordings fixed; CPython
  3.14 pinned for the interop job.

---

## 15. Status record (maintained through implementation)

* Assumptions: local-only work; no pushes/PRs/tags/registration; CSV/Arrow/Parquet checkouts untouched;
  network used only for specs, dependencies, and interop tools.
* Commands and results: recorded in `STATUS.md` in the worktree as phases complete (exact commands,
  Julia versions, pass/fail counts, benchmark numbers).
* Current state: **plan under review (round 11); no production code changed yet.**

---

## Appendix A — Probe scripts and raw results

Kept in the scratchpad of the authoring session (`probe/`, `fixtures/`, `javah/`, `baselines/`,
`apienv/`) and summarised in §2.2, §8.2, §8.3, §10.1; the scripts are re-created as tests and harness
sources in Phases 0, 2 and 4.

## Appendix B — API sketch

```julia
using Avro, Tables

sch = Avro.parseschema("""{"type":"record","name":"Weather","namespace":"test","fields":[
  {"name":"station","type":"string"},{"name":"time","type":"long"},{"name":"temp","type":"int"}]}""")
Avro.fingerprint(sch)                       # CRC-64-AVRO of the canonical form (UInt64)
bytes = Avro.encode(sch, (station="011990-99999", time=-619524000000, temp=0))
r = Avro.decode(sch, bytes)                 # Avro.Record; r.station == "011990-99999"
reader = Avro.DatumReader(sch, MyWeather)   # prepared, shareable, typed (StructUtils AvroStyle)
reader(bytes)

Avro.write("w.avro", table; codec=:zstandard, metadata=Dict("source"=>b"sensor"))
t = Avro.Table("w.avro")                    # columns; `scan=Tables.Scan(...)` in a Scan-enabled release
for row in Avro.Rows("w.avro")              # streaming, one block resident
    row.temp
end
w = Avro.Writer("out.avro", sch; codec=:snappy); push!(w, row); close(w)

big = Avro.Limits(max_total_bytes=16 << 30, max_block_bytes=64 << 20)   # explicit, both sides
Avro.write("big.avro", bigtable; limits=big); Avro.Table("big.avro"; limits=big)

new = Avro.parseschema(...)                 # reader schema with a new defaulted field
Avro.Table("w.avro"; reader_schema=new)     # resolved decode (spec union policy; reader-directed values)
store = Avro.SchemaCache(); Avro.register!(store, sch)
msg = Avro.encodesingle(sch, row); Avro.decodesingle(msg, store)
Avro.Table("old.avro"; legacy=:avrojl1)     # file written by Avro.jl 1.x (padding / zstd name only)
Avro.Table("ts.avro"; instants=:datetime)   # DateTime columns instead of exact timestamp wrappers
Avro.Table("big.avro"; validate=:fast)      # opt-in fast skipping with documented blind spots
```
