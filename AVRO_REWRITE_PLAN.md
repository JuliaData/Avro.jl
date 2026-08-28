# Avro.jl 2.0 — Audit and Rewrite Plan

Status: AGREED v24 (Codex round 23 returned `VERDICT: AGREE` on v23; v24 applies its non-blocking follow-ups; revised after Codex review rounds 1–23; see §14 review log and `reviews/response-N.md`).
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
StructUtils-based typed API, single-object encoding, JSON encoding, and projection pushdown (`select=`;
`Tables.Scan` pushdown is deferred to 2.1), gated by conformance against Apache Java, fastavro, and
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
| Protocol declaration (`.avpr`), messages, errors, MD5 | **Deferred to 2.x** | requires: distinct anonymous request-schema type, property preservation, a deterministic Java-compatible printer whose bytes are what MD5 hashes, one-way validation, ping, effective error unions (`["string", declared errors...]`), the framing rules (big-endian 4-byte buffer lengths, zero-length terminator), handshake and HTTP transport behaviour. A standalone `{"type":"error"}` schema parses as a record with `iserror=true` today: it prints as `"type":"error"`, its canonical form and fingerprint are those of the record (Java treats an error as a record with an `isError` flag), and `==` distinguishes `iserror`. The package documents that it implements the Avro data format, not Avro RPC |
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
   per-operation ceiling** bounds package-owned memory through **actual-size reservations made before
   every allocation** for the generic value set and the column path, input/codec/output buffers and
   internal tables, and through input-bounded per-node charges for schema and plan graphs (§4.4
   categories (a)–(e)); work is bounded as a function of input bytes (values, comparisons and resolution
   effort); the one documented boundary is the typed conversion of §4.8, which runs after ownership
   transfer in caller space; a best-effort **available-memory guard** lowers the
   effective ceiling in constrained processes so that the package usually fails with a `LimitError`
   before the process is killed; writers enforce exactly the limits readers enforce — including the decoder requirements of
   the codec frames they emit — so whatever a default writer produces a default reader accepts;
   package-detected malformed content always surfaces as an `AvroError`.
3. **Stable generic values, opt-in specialisation.** Untrusted schemas never drive Julia compilation:
   the generic path uses dynamic plans, a finite enumerated set of value types (§4.6), and
   schema-independent runtime containers; specialised, unrolled code is generated only for a
   caller-supplied target type `T`.
4. **Stream by default, materialise on request.** Container reading is block-at-a-time with bounded
   in-flight memory; `Avro.Table` is the explicit materialisation and owns copies of everything; path
   sources are either memory-mapped or streamed — never read whole into memory.
5. **No global mutable state**, with three documented exceptions: the default process-wide symbol
   admission table (§6), because `Tables.columnnames` must return `Symbol`s; the write-once storage
   constants measured in `__init__` (§4.4), frozen before any public operation runs; and the
   package-wide `@atomic` pending-reservation counter of the available-memory guard (§4.4); callers may supply their own admission
   object instead, and every typed decoding path that interns (`DatumReader`, `decode`, `decodesingle`,
   `fromjson`, `Rows`; `Table` for column names) goes through the same boundary. Codec instances, buffers, budgets, and
   plans are owned by reader/writer objects or operations; hashes are computed at freeze time and stored
   in the immutable nodes. The logical-type set is closed in 2.0.
6. **Strict by default, fast by choice.** `validate=:strict` walks every byte it skips (the single
   recorded exception: skipped strings are not UTF-8-validated); `validate=:fast` may jump over sized
   regions and documents exactly what it then cannot detect.
7. **Qualified API, zero exports.**

### 4.2 Schema model (`src/schema.jl`, `src/names.jl`, `src/logical.jl`, `src/canonical.jl`, `src/frozen.jl`)

```julia
abstract type Schema end
# Every Schema subtype is a heap node: `mutable struct`, with every field declared `const`.
mutable struct NullSchema <: Schema; const props::Props; const hash::UInt64; end        # Props = FrozenDict{String,FrozenJSON}; FrozenJSON = recursively
mutable struct BooleanSchema <: Schema; const props; const hash; end                    # frozen JSON tree (FrozenDict / FrozenVector / immutable scalars)
mutable struct IntSchema <: Schema; const logical::Union{Nothing,LogicalType}; const props; const hash; end
mutable struct LongSchema <: Schema; const logical; const props; const hash; end
mutable struct FloatSchema <: Schema; const props; const hash; end
mutable struct DoubleSchema <: Schema; const props; const hash; end
mutable struct BytesSchema <: Schema; const logical; const props; const hash; end
mutable struct StringSchema <: Schema; const logical; const props; const hash; end
mutable struct ArraySchema <: Schema; const items::Schema; const props; const hash; end
mutable struct MapSchema <: Schema; const values::Schema; const props; const hash; end
mutable struct UnionSchema <: Schema; const branches::FrozenVector{Schema}; const hash; end
# every node additionally carries `id::FrozenRef{Int32}` (dense, graph-local, 0-based) and `graph::FrozenRef{GraphInfo}`
# (the `Limits` the graph was admitted under, its repair flags `repaired_names`/`repaired_defaults`, node and
# named-type counts), both filled exactly once by `freeze!` on every creation path — parser, public
# constructors, `Avro.schema(T)` — so no identity side table exists and `==`/plans/resolution key on ids; a public constructor that
# receives an already-frozen child deep-copies it into the new graph with fresh ids through one graph-wide copy memo, so the same child passed twice is one definition plus references (charged to the constructing
# operation); `max_schema_nodes` keeps ids within `Int32` (validated)
mutable struct FixedSchema <: Schema; const name::FullName; const aliases::FrozenVector{String}; const size::Int; const logical; const props; const hash; end   # no `doc`: fixed defines none; a fixed `doc` lives in `props`
mutable struct EnumSchema <: Schema; const name::FullName; const aliases; const doc; const symbols::FrozenVector{String}; const default::Default; const symbolindex::FrozenDict{String,Int}; const props; const hash; end
struct Field; name::String; schema::Schema; doc; default::Default; order::Order; aliases::FrozenVector{String}; props; end
mutable struct RecordSchema <: Schema   # heap node with all-`const` fields (see the amendment below);
    const name::FullName; const aliases; const doc; const iserror::Bool; const props   # `fields`/`fieldindex`/`hash` are frozen
    const fields::FrozenVector{Field}; const fieldindex::FrozenDict{String,Int}; const hash::FrozenRef{UInt64}   # containers / a Ref filled once, then frozen
end
struct FullName; name::String; namespace::String; end       # fullname(x) = isempty(ns) ? name : ns*"."*name
```

* **Amendment (implementation round 1, evidence-driven): heap schema nodes.** Every schema node is a
  `mutable struct` whose fields are all `const` — semantically immutable (no field can ever be
  reassigned), but guaranteed a stable heap identity. Plain immutable structs were inlined by Julia
  into every value that references a schema (`sizeof(Avro.Record)` measured 104 bytes; a
  `Vector{EnumValue}` slot 104 bytes), which broke the §4.4 category-(b) storage formulas and the
  `Base.summarysize(x; exclude=Avro.Schema)` oracle — the schema graph must be charged once under
  category (e), which requires values to hold *references*. Identity (`===` is pointer identity),
  structural `==`/`hash`, recursion, the storage oracle and cross-version behaviour are certified by
  the Phase 1/2 gates on Julia 1.10 and 1.12. The transitive-immutability contract below is unchanged.
* **Enforced, transitive immutability.** Every schema node is semantically immutable. `FrozenVector`,
  `FrozenDict`, and `FrozenRef` are private containers with a `frozen` flag: the parser fills them and
  calls `freeze!` once (public constructors freeze immediately); after that every mutating method throws.
  `props` and default values are stored as **recursively frozen JSON trees** (`FrozenJSON`: frozen
  containers of immutable scalars), so no public operation can mutate anything reachable from a frozen schema (the contract covers the
  public API and the frozen containers' methods; reaching into backing storage with `getfield` is
  outside it, documented). There is no identity side table: each node's `hash` is computed bottom-up
  at freeze time (recursive named-type references hash by fullname only) and stored in the node, as
  are its dense `id` and the shared `GraphInfo` (§4.4 category (e)); `canonical(schema)` is computed on demand
  and not cached. The parser's mutable state (`ParseContext`) is private.
* **Alias normalisation.** Type aliases are normalised to fullnames at parse time (a relative alias takes
  the namespace of the type it aliases, per the spec; the raw alias text is kept only for exact
  re-emission); field aliases are plain names. Equality, hashing and resolution use the normalised
  fullnames (`Bar` and `a.Bar` are the same alias for a type in namespace `a`; tested).
* **Empty enums.** `{"type":"enum","name":"E","symbols":[]}` is valid (Java accepts it): it parses,
  canonicalises and fingerprints, has no finite datum (`minsize = ∞`), decoding any bytes against it is
  a `DataError` and encoding any value an `EncodeError`; a default is then always invalid.
* **Empty unions.** `[]` is a valid schema (Apache `schema-tests.txt` 016): it parses, canonicalises and
  fingerprints; it has no finite datum (`minsize = ∞`), `juliatype([]) = Avro.UnionValue`, decoding any
  bytes against it is a `DataError` (no branch index is valid), and encoding any value is an
  `EncodeError`.
* **Equality and hashing.** `==` is structural and semantic: kind, fullname, fields (name, schema,
  default *value* and selected branch — compared with `isequal` semantics on the decoded value under the
  field's schema, so `-0.0` and `0.0` defaults differ and quoted `"NaN"` equals `"NaN"`; an invalid
  default retained under `allow_invalid_defaults` (`valid=false`) compares by its exact lexical span
  instead — `order`, normalised aliases, doc, `iserror`; `props` passed to a public constructor with
  duplicate keys, or with keys that the *same* constructor emits structurally (`type` everywhere;
  `name`/`namespace`/`aliases`/`fields`/`symbols`/`size`/`items`/`values` only on the constructor that
  defines them, and `logicalType`/`precision`/`scale` whenever a `logical=` annotation synthesises
  them — consistent with context-inapplicable attributes being metadata), are an `ArgumentError`),
  symbols, enum default, size, logical type and attributes, and `props` compared as unordered maps of
  JSON values;
    recursion uses a visited set of `(a,b)` pairs keyed by the **dense node IDs** every schema node
  receives from `freeze!` (§4.4 category (e): a per-`a` sorted small vector of visited `b` IDs,
  binary-searched; every inspected entry and insertion move is charged to `max_resolution_work`, so `==` on two adversarially shared
  graphs fails with `LimitError` rather than running unbounded — documented; the limits used are the
  larger of the two schemas' recorded parse limits, which every root node stores) so cyclic graphs
  terminate. `hash` is the stored digest
  of the same normalised content, so `==` and `hash` agree. `Avro.parsingequivalent(a, b)` compares
  Parsing Canonical Forms.
* **Attributes.** The required structural attributes (`type`, `name`, `fields`, `symbols`, `items`,
  `values`, `size`) are schema syntax and validated as such; of the optional attributes, `fixed.size` is
  a JSON integer with `0 ≤ size ≤ typemax(Int)`. `precision`, `scale`, `logicalType`, and every
  unknown/custom attribute are metadata: preserved verbatim in `props`, never validated as such,
  re-emitted by the printer, and stripped by the canonical form.
* Logical types are *attributes* of the underlying schema (`logical`), a closed set:
    `DecimalLogical(precision::Int, scale::Int)` (scale defaults to 0 when absent; distinct from the datum
  type `Avro.Decimal`), `UUIDLogical`, `DateLogical`,
  `TimeMillis`, `TimeMicros`, `TimestampMillis/Micros/Nanos`, `LocalTimestampMillis/Micros/Nanos`,
  `DurationLogical`, and `UnknownLogical(name)` (kept so the schema re-serialises faithfully;
  `big-decimal` is carried this way in 2.0). A recognised logical type is **evaluated in context**: when
  its underlying type is wrong, or its attributes are malformed (`precision`/`scale` not JSON integers
  within `Int`, `precision ≤ 0`, `scale < 0`, `scale > precision`, precision exceeding
  `floor((8n − 1) × log10(2))` for fixed size n ≥ 1 — evaluated with checked, saturating integer arithmetic, never by constructing `2^(8n−1)` — for `fixed(0)` no positive precision is valid, so
  the annotation is always dropped and the formula is never evaluated at n = 0 (parsing never leaks a
  `DomainError`; tested) —, duration size ≠ 12, uuid fixed size ≠ 16), the annotation is dropped to the
  underlying type per spec with the raw attributes preserved in `props` (matching Java, which warns and
  reduces such schemas to their underlying type).
* `Default` (record fields **and enum defaults**) is `Avro.nodefault` (absent; the public constructors
  use the same sentinel, so `default=nothing` and `default=missing` both mean a JSON `null` default) or
  `Some(DefaultValue(json::FrozenJSON, branch, span, index, valid::Bool))`: the frozen JSON tree of the
  default, the selected union branch (first branch that accepts the bare default, in declaration order),
  an **owned immutable copy** of the exact source text (category (e) of §4.4; never a slice of the
  caller's buffer — a test mutates the caller's input after parsing), the resolved enum symbol index
  (enums only), and whether it validated.
  Plans materialise the Julia default **fresh on every use** from the frozen tree (a plan may hold a
  private immutable prototype), charged to the operation's budget. Defaults never make a field optional
  when encoding. With `allow_invalid_defaults=true` an invalid default is kept with `valid=false` and is
  re-serialised verbatim; using such a default during resolution is a `ResolutionError` unless the
  reader schema supplies a valid one.
* **Parsing.** `Avro.parseschema(src; allow_invalid_names=false, allow_invalid_defaults=false,
  limits=Limits())`:
    1. The source is an owned contiguous byte buffer: `String`, `SubString{String}`, `Vector{UInt8}`,
     `Mmap` vectors and unit-stride views of those are used in place; **every other `AbstractString` or
     `AbstractVector{UInt8}`** (pointer-incompatible strings, stepped views, custom vectors) is first
     copied into an Avro-owned, reserved buffer (category (c), which lists string copies explicitly), so JSON.jl never copies behind the
     reservation boundary (exact-boundary tests with custom strings and stepped views); an `IO` source is
     read **incrementally** into an Avro-owned buffer whose capacity is reserved step by step and which
     stops after `max_schema_bytes + 1` bytes (`LimitError`; JSON.jl's own `lazy(io)` would read the
     whole stream first and is never used on an `IO`; tested on a non-seekable over-limit stream and at
     both byte boundaries). A lexical pre-scan (no allocation; handles strings/escapes) then rejects
     inputs over `max_schema_bytes` or deeper than `max_schema_depth` *before* any parser recursion,
     validates every **number token against the RFC 8259 grammar** (`-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`;
     no leading `+`, no bare `NaN`/`Infinity` — the permissive datum forms of §4.11 are recognised by
     Avro's own reader only where that table allows them) without allocating (the recursive-descent
     reader rediscovers token ends; no endpoint vector is kept), rejects every RFC 8259 syntax error (raw control characters in strings, a BOM, malformed literals, invalid whitespace, trailing commas, trailing content — each a negative fixture) and **validates the text as strict UTF-8** (no overlongs, no raw surrogate code points, nothing above
     U+10FFFF; RFC 8259 requires UTF-8) **and the syntax of every escape** (`\uXXXX` hex digits, no
     invalid escapes) — JSON.jl does not validate UTF-8 and would return invalid `String`s — so malformed
     bytes anywhere are `SchemaError`s/`DataError`s. **JSON strings are decoded by Avro's own decoder directly from their raw spans** (JSON.jl's decoder
     merges and corrupts surrogate escapes — `"\uD800\uD800"` and `"\uFC00"` both become `ef b0 80`
     — and is never used). The decoder
     produces **WTF-8**: a high surrogate immediately followed by a low surrogate becomes the 4-byte
     scalar; every other surrogate code unit becomes its own 3-byte generalised-UTF-8 sequence, so
     distinct code-unit sequences give distinct byte strings and the printer re-escapes every surrogate
     sequence as `\uXXXX`, round-tripping metadata code-unit-exactly (owned, charged copies of the raw lexical spans are also retained for every metadata string and
     number — category (e) — so re-emission is verbatim). Decoding writes into reserved scratch
     (charged). Duplicate detection, equality, sorting and freezing operate on the WTF-8 bytes. The
     **Unicode-scalar requirement is then applied contextually, after the fullname algorithm**: a value
     that is an Avro string — a name or namespace *actually used* (an ignored namespace is never
     validated beyond being a JSON string, §4.2 step 3), an enum symbol, a default string value, a datum
     string, a map key, a union label — must be valid UTF-8 (`SchemaError`/`DataError` otherwise; under
     `allow_invalid_names=true` names are admitted as WTF-8 bytes with `repaired_names` set, and under
     `allow_invalid_defaults=true` a surrogate-bearing default is retained with `valid=false`), while
     aliases, `doc`, custom property names and values, and nested metadata keys are preserved
     code-unit-exactly. **Aliases are "any string" (spec) and match by exact WTF-8 bytes**, so a reader
     alias repairs an invalid legacy writer name exactly when the writer schema was parsed with
     `allow_invalid_names=true` (direct writer-invalid/reader-alias resolution fixtures, incl. a
     surrogate-bearing name). Tests: lone high, lone low, high/high, low/low, low/high, valid pairs,
     separated surrogates and both collision pairs above, in keys and values, for every context; the
     oracles accept such namespaces, aliases and property names (verified by the round-14 review).
     `fromjson` applies the same source, decoding and validation rules.
    2. **Avro owns its JSON reader.** The document is parsed by an in-package recursive-descent reader
     over the pre-scanned buffer (objects, arrays, strings through the WTF-8 decoder below, numbers,
     literals; explicit depth; every allocation charged). JSON.jl is **not used for parsing at all** —
     its lazy parser materialises every number it merely skips, growing a `BigInt` digit by digit
     (superlinear, unreserved work: 32,000 digits allocate 446 MB), accepts a leading `+`, and would
     retain mutable `BigInt`s — and remains a dependency only for the `JSON.json(schema)` printing
     overload. Numbers: required integer attributes (`size`, `precision`, `scale`) and integer defaults
     are parsed with checked `Int64` accumulation (overflow or a fraction/exponent → invalid), required
     floats and float defaults through **separate, correctly rounded, linear-time `Float32` and
     `Float64` parsers** over the whole token (bounded only by the document size — fastavro accepts a
     2,000-digit `1.000…` as `1.0`; parsing an Avro `float` through `Float64` would double-round:
     `1.000000059604644775390625827180612553027674871408692069962853565812110900878906` is
     `0x3f800001` as `Float32` but `0x3f800000` via `Float64`, and Java's `jsontofrag` emits
     `01 00 80 3f` — this vector is gated byte-for-byte for defaults and `fromjson`, together with midpoint, subnormal,
     overflow, underflow, huge-mantissa and huge-exponent differential vectors and linear time/allocation
     checks; a token that overflows parses to ±`Inf` and one that underflows to ±0.0 or a subnormal per
     IEEE 754, exactly as Java's `parseDouble`, and `-0.0` is preserved — for datums and defaults alike), and numbers
     inside metadata are stored as charged immutable **raw number tokens** (never `BigInt`/`BigFloat`),
     compared and hashed by their lexical bytes (`1`, `1.0` and `1e0` are three different property
     values, documented), re-emitted verbatim (tests: huge integers, mantissas and exponents in known fields, defaults, nested
     and skipped properties, and `fromjson`). **Duplicate keys are detected by Avro** per object by
     sorting the object's **decoded** keys (escapes unescaped into charged scratch, so `"type"` and
     `"\u0074ype"` are equal; order and equality over the WTF-8 bytes) in a charged vector of key
     references (no hashing, no `Dict`/`Set`); every compared byte is charged to the **comparison rule**
     of §4.4, and duplicates are `SchemaError`s (tests: simple escapes, BMP escapes, surrogate pairs,
     mixed spellings). **Attributes are grammar only in the context where the specification defines them**, validated as
     syntax only there, with **two separate grammar tables**. Schema objects: `type` must be a **string naming a primitive** (`null`, `boolean`, `int`, `long`,
     `float`, `double`, `bytes`, `string`) **or one of `record`, `error`, `enum`, `array`, `map`,
     `fixed`** — a named-type reference is a schema *string*, never `{"type":"E"}`, and a union is a
     JSON *array*, never `{"type":"union"}` (Java rejects both; an object- or array-valued `type` in a
     schema object is likewise a `SchemaError`; positive and negative fixtures for every form); `name` (string), `namespace` (string) and
     `aliases` (array of strings) on records, enums and fixed; `doc` (string) on records and enums only
     (fixed defines no `doc`, so a fixed `doc` of any JSON type is metadata — Java accepts a numeric one);
     `fields` (array of objects) on records; `symbols` (array of strings) and `default` (a string) on
     enums; `items`/`values` (schemas) on arrays/maps; `size` (integer) on fixed. Record fields: `name`
     (string), `type` (**any schema**: string, object or union array — the one place a schema value is
     allowed), optional `doc` (string), `default` (any JSON value), `order` (string in
     `ascending`/`descending`/`ignore`), `aliases` (array of strings). The same keys on any other form —
     `name` on an `int`, `size` on an array — are undefined attributes and therefore metadata preserved
     verbatim (Java accepts `{"type":"int","name":123}` and `size:"x"` on an array — verified by the
     round-17 review; direct Java-backed fixtures for each table), and `logicalType`/`precision`/`scale`
     are always metadata evaluated contextually by the logical-type rules above — a malformed annotation
     drops to the underlying schema and never rejects it (negative and positive fixtures per context).
3. A `ParseContext{namespace stack, named-type table, depth, counters}` (its named-type table is a
     sorted vector keyed by fullname bytes — at most `max_named_types` entries, so insertion moves are
     bounded and charged to the comparison rule) first applies the **fullname algorithm** (a dotted `name` wins and any supplied `namespace` is ignored — it must still be a JSON string (Java accepts non-strings there; recorded as a Java-compatible leniency we do not adopt) but is otherwise never validated; a
     simple name takes the explicit or inherited namespace) and then validates every rule on the
     resulting names: name and namespace grammar (unless `allow_invalid_names`), fullname uniqueness,
     primitive names never redefined in the null namespace (`a.int` is a valid fullname; both cases gated), define-before-use (depth-first, left-to-right), field/symbol
     uniqueness within scope, alias uniqueness — an alias equal to its own type's or field's name is
     **idempotent and ignored** (Java-compatible; `schema-tests.txt` 023/024), while an alias colliding
     with a *different* name or alias is a `SchemaError` — union branch identity (kind for unnamed,
     fullname for named; a definition plus a reference to the same fullname counts twice), enum default
     membership, `fixed.size` syntax, `order` values, default validity — evaluated by a **recursive default rule** distinct from the datum JSON rule: a record default supplies a missing nested field from that field's own validated default and is invalid only when no such default exists (Java accepts an outer `{}` when the nested field defaults to `1`), unknown members are ignored semantically while the exact source span is retained for re-emission, and every other kind follows the §4.11 strict row (gated for parsing and for reader-schema resolution) — (including union branch selection
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
  `SHA`). **Repaired schemas:** a graph whose `GraphInfo` carries `repaired_names` has no specification
  PCF (PCF presumes valid names and UTF-8), so `canonical`, `fingerprint`, `parsingequivalent`,
  `register!`, `encodesingle`, `tojson` and `fromjson` reject it with an `ArgumentError` naming the
  flag (repaired invalid names are binary/OCF-only, readable with the same options); `repaired_defaults`
  does not affect PCF (defaults are stripped) and is allowed everywhere except the OCF writer's opt-in
  rule (§4.4). Negative gates for each operation.
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
  `validate=:fast` (opt-in on
  `decode`/`DatumReader`/`Rows`/`Table`/`Reader`/`comparebytes`): sized array/map blocks are jumped by
  their byte size; the documentation states precisely that malformed content inside jumped regions (invalid booleans,
  enum/union indexes, nested framing, malformed decompressed datum bytes) is not detected in that mode.
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
* **Encoder** is a growable `Vector{UInt8}` with `pos`, `ensureroom!` (checked growth by explicit
  replacement: a new `Vector{UInt8}(undef, newcap)` of exact capacity is reserved and allocated, the
  bytes copied, and the old buffer released — never `resize!`/`sizehint!`/`push!`, whose over-allocation
  heuristics would allocate behind the reservation), and write
  primitives (`writelong` = 10-byte unrolled varint, `writebytes`, `writestring`, `writefloat`, …).
  `Encoder` is re-usable (`reset!`) and its buffer can be handed to a codec or `IO` without copying. No
  pre-sizing pass. **Encoding is bounded by the same `Limits` as decoding** (§4.4): per datum
  `max_datum_bytes`, per block `max_block_bytes`/`max_block_count`/`max_block_output_bytes`, per operation `max_total_values`, `max_total_bytes`
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
    max_codec_memory::Int     = 32 << 20      # ONE meaning: cap on the COMPLETE decoder memory requirement of one codec member as reported by the library (xz `memlimit`; zstd `ZSTD_estimateDStreamSize_fromFrame`); enforced on every frame read and written (§4.9)
    # ONE per-operation ceiling for package-owned memory (committed output + in-flight reservations + internal tables + owned input copies + codec workspaces)
    max_total_bytes::Int      = 256 << 20     # FIXED, portable default (256 MiB); raise explicitly on every side that processes the data
    max_total_values::Int     = 3 << 26       # values decoded or encoded, including zero-size ones (finalised, round-1 calibration: the worst legal shape - depth-80 record skipping - is bounded by total values, not density; 2^28 values measured 9.5-9.6 s against the 10 s gate on the authoring host, and 3*2^26 (~201M) restores ~28% headroom)
    max_rows::Int             = 1 << 28       # container datums per operation (provisional; same gate)
    max_values_per_byte::Int  = 16            # WORK RULE: values <= max_values_per_byte x input bytes + work_allowance (one allowance per operation)
        work_allowance::Int       = 65_536        #   input bytes = decompressed block bytes + block framing + raw datum bytes + JSON text bytes (never compressed size)
    max_compare_bytes_per_byte::Int = 64      # COMPARISON RULE: key bytes compared by duplicate-key detection and map construction/lookup ≤ 64 × input bytes + work_allowance
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
        max_inflight_blocks::Int  = 0             # higher blocks that may be in flight beyond the direct head; 0 = ntasks − 1; further bounded by the headroom (§4.9)
end
```

**Portable defaults and the available-memory guard.** Every default is a fixed number. The 256 MiB
ceiling is deliberately small: it is the amount of package-owned memory an untrusted operation may
use without anyone raising a limit, and typical Avro files (64 KiB blocks, 8 MiB codec windows) need a
fraction of it. At the start of every operation the **effective ceiling** is
`min(limits.max_total_bytes, available ÷ 2)` with `available = min(host_free, Sys.total_memory(),
cgroup_remaining) − pending_reservations`, where `host_free` is `Sys.free_memory()` except on macOS,
where it is free **plus inactive** pages from `host_statistics64` (amendment, implementation round 1,
evidence-driven: macOS keeps reclaimable memory in inactive/file-cache pages, so `Sys.free_memory()`
reports only truly free pages — measured 0.08 GiB free on an otherwise healthy 96 GiB authoring host,
under which even the default 256 MiB ceiling's first-unit check would fail and every operation would
raise `:available_memory`; free + inactive measured ≈ 33 GiB on the same host, the OS's actual
reclaimable estimate) — and `cgroup_remaining` is read on
Linux from cgroup v2 `memory.max`/`memory.current` (or v1 `memory.limit_in_bytes`/`memory.usage_in_bytes`)
when readable and is `∞` otherwise, and `pending_reservations` is a package-wide `@atomic` counter of
bytes that live operations have **reserved but not yet allocated** (resident allocations are already
visible to `free_memory`/`memory.current`, so only the not-yet-resident part is subtracted — no double
counting); the counter uses checked, clamped arithmetic and is restored in a `finally` on every exit
path (success, `LimitError`, interruption, writer poisoning, worker failure, abandonment via the
`Rows`/`Reader`/`Writer` finalizers) (`free_memory` is host-wide on Julia ≤ 1.12 — documented); if the
effective ceiling cannot admit the operation's first unit of progress (§4.9) the operation fails with
a `LimitError` naming the available memory **before any allocation**. **The guard is best-effort**: it
makes an OOM kill less likely in a constrained process but cannot promise to prevent one (process
limits, runtime headroom and other allocators are invisible to it — documented); it is a safety valve
and is not part of the writer/reader invariant (which is stated under identical configured limits and
effective ceilings that admit the preflighted peak); tests inject total, current-usage and
live-reservation boundary values. Concurrent operations each own a ceiling (documented; a caller-side
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
max_total_bytes ÷ 2`. These relations guarantee that each component of the first unit of progress —
header, schema, one compressed block, its decompression workspace, and its first datum — fits under
its fraction of the ceiling with checked arithmetic; the combined peak of a concrete operation is not
proven by the constructor but enforced at run time (`LimitError` before allocation) (defaults: 16 + 36 + 16 MiB ≪ 256 MiB). Public entry points validate
their own options (`ntasks ≥ 1`, actual tasks capped at the block count; `Writer.block_bytes ≤
limits.max_block_bytes`).

**Map structure (deterministic; no package-owned hash-table lookup of untrusted keys and no hash-dependent charge anywhere in the guarded path — runtime `Symbol` interning is the documented exception).** `Avro.Map` keeps its keys
and values in insertion order and an `Int32` permutation **sorted by decoded key bytes** (`npairs ≤
typemax(Int32)` is checked first), built once by an in-package deterministic merge sort over **all
`npairs` candidates** with package-owned scratch (the construction peak is the `npairs` permutation plus
`cld(npairs, 2)` scratch entries, both reserved before the sort; after sorting, duplicate groups are
resolved last-wins and the permutation is compacted in place to `nunique`, so an all-duplicate map
with `npairs = 10^6` and `nunique = 1` is charged for its million candidates during construction and
keeps that `npairs` capacity charged afterwards (length `nunique`) — odd sizes and near-ceiling all-duplicate maps are gated); `getindex` is a binary search
(≤ ⌈log2(nunique)⌉ + 1 comparisons, a fixed per-call bound that needs no budget after the map is
handed to the caller); there are no seeds, probe limits, overflow indexes, rebuilds or growth, so memory
and work are functions of the counts and key bytes alone and acceptance cannot depend on anything
random. The **symbol-admission table** is a different, long-lived structure with the same property: a
set of **sorted runs** of geometrically increasing size (a 1,024-entry unsorted recent buffer scanned
linearly; when full it is sorted and merged with runs of equal size, as in a log-structured merge).
Merges are **deamortised**: an in-progress merge advances by a fixed step of at most 2,048 moved
entries per admission, lookups search both the old runs and the partially built run until the merge
completes, and a failed admission leaves the table unchanged (staging is transactional); so one
admission costs ≤ 1,024 comparisons in the recent buffer + ≤ log2(N ÷ 1024) + 1 binary searches +
2,048 moves, a fixed per-operation bound; whether the default allowance affords it for 1,024 maximal-length
common-prefix names is established by the Phase 2 gate, which may raise the allowance or multiplier or
shrink the recent-buffer step (e.g. to 256) until one-symbol admission at every boundary passes. Merge maintenance is the table's own work (≤ N log N moves over its lifetime),
charged to the table's `max_bytes`/capacity accounting and reserved before every run or scratch
allocation, while the admitting operation's comparison rule is charged only for its own lookups and
its fixed merge step (gate: one-symbol operations across every power-of-two boundary up to
`max_names`; one million incremental admissions with latency recorded; capacity boundaries at
`max_names`/`max_bytes`).

**Comparison rule.** Let *compared bytes* be every key byte examined and every moved permutation or
reference entry (4 or 8 bytes per move) produced by duplicate-key detection (§4.2, §4.11), by JSON and
map sorting, by the named-type table's comparisons and moves, and by the admission table's
recent-buffer scan, sort and fixed merge step (all calibrated against the Phase 2 constants with
maximum-length common-prefix names at every flush and carry boundary), by map construction (the sort) and by symbol admission (run searches and merges; a moved index entry
counts as 4 bytes). The rule `compared_bytes ≤ max_compare_bytes_per_byte × input_bytes +
work_allowance` is enforced per operation (the allowance is the shared one; for operations without
encoded input — constructed schemas, `Avro.schema(T)`, `tojson` — `input_bytes` is the serialised
schema or produced text size, and for the copying public constructors the copied key, value and
representation bytes — a near-limit `Avro.Map` constructor gate); the multiplier is finalised by the
Phase 4a gate from both comparisons and moves (a merge sort compares ≤ ⌈log2 k⌉ + 1 times per key and
moves each permutation entry ⌈log2 k⌉ times, and k ≤ `max_total_values` = 2^28 keys give ≤ 29 × input
in comparisons alone) so that the generated legitimate corpus passes; like every resource limit it can
reject valid data by design and is raisable. The writer's preflight (below) charges the same
deterministic comparison work for every map it writes (it knows each map's pair count and key bytes),
so the comparison rule is part of the identical-limits invariant. Phase 2's latency gate adds wide
objects and long-common-prefix keys to its fixtures and records `max_compare_bytes_per_byte` with the
other constants.

**Work rule (identical on encode and decode).** Let *input bytes* be the bytes that carry the values:
decompressed block payload plus each block's framing (the count and size varints and the 16-byte sync
marker), raw datum bytes, or JSON text; let *values* be every value encountered, including zero-size
ones, the datum itself, and **every codec member** (a compressed, empty or skippable frame/stream
counts as one value, so dense member sequences are bounded like any other zero-size values). The rule `values ≤ max_values_per_byte × input_bytes + work_allowance` is
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
everywhere because they are fixed — everything a **successfully closed** `Writer` emits is accepted by a `Reader` (caller-owned `IO` may hold partial output after a failure, §4.9): the writer
enforces every per-datum, per-block and cumulative limit a reader enforces (`max_datum_bytes`,
`max_block_bytes`, `max_block_count`, `max_block_output_bytes`, `max_blocks`, the work rule, `max_total_values`, `max_rows`,
`max_total_bytes` charged with the reader's output estimate of the encoded values plus decompressed
payload bytes and the block table a reader will build, `max_metadata_bytes`/`max_metadata_entries` on the header it writes, `max_schema_bytes`/`max_schema_nodes`/… on the schema it
serialises, and `max_codec_memory` on the decoder requirement of the frames it emits — §4.9). **The
writer preflights the reader's complete peak under its own budget:** it charges the header and metadata
representation a reader materialises, its parsed schema graph (the same graph a reader builds from the
same JSON — a test asserts the two charges are equal), a generic read plan built at construction
(category (e)), the block table, one block's `reader_block_peak` — **one function**, used by the
writer preflight, sequential decoding and the parallel `W`: compressed and decompressed buffers, codec
workspace, output slots and payload, map-sort permutation and scratch, exact-replacement overlap,
WTF-8 scratch and per-block state —,
the committed output estimate, the reader's deterministic comparison work for every map (the
comparison rule), and — because `Avro.Table` from a non-seekable `IO` materialises through per-block
chunks assembled once at the end (§4.9; no geometric growth) — the streamed consumer's **exact** peak:
the sum of every chunk shell (`nblocks × ncolumns` array headers plus chunk capacities, which dominate
for wide mostly-null records over many tiny blocks — a gated fixture: a 1,001-field record across
10,000 one-row blocks), the final shells (both sets of reference slots), the referenced payload
counted **once** (ownership moves from chunks to final columns), the block table and the assembly
buffers; it fails with `LimitError` when the largest of these consumer peaks would exceed the ceiling.
**The guaranteed consumers** are `Reader` and `Rows` for every root schema and `Table` for record
roots, from bytes, a mapped path, `mmap=false` and a non-seekable `IO`, **in generic default
consumption**, **whenever both sides' effective ceilings (§4.4 available-memory guard) admit the
writer-preflighted peak** — a reader on a host whose guard lowers its effective ceiling below that peak
fails with the separate low-memory `LimitError`, which is a safety guarantee, not a readability
failure under identical limits — (`reader_schema=nothing`, no typed `T`, exact timestamp wrappers, `decimal_byteorder=:big`,
`validate=:strict`, repair options matching the writer's), **under identical `Limits`, the same codec
availability, and fresh or sufficient symbol-admission state**; for **portability across supported
Julia versions and pinned codec-library revisions** the preflight uses conservative maxima — the
largest storage constant recorded for any Julia version in the **certified matrix** — the finite set of
Julia versions in CI (1.10, 1.11, 1.12, plus each later minor when it is added) with the JLL versions
pinned in the test Manifest, listed in the manual; an unlisted Julia or JLL version is supported for
use but not certified for the near-ceiling invariant — and the library-reported codec requirements of
the pinned JLLs; cross-read gates run every near-ceiling invariant file on every version in the
matrix (the container-only part
is gated in Phase 4a and the complete consumer/source-mode gate in Phase 4b, when `Rows`/`Table` exist); a `Writer`
rejects a schema whose `GraphInfo` carries `repaired_names`/`repaired_defaults` unless the matching
`allow_invalid_names`/`allow_invalid_defaults=true` is passed explicitly (such files are documented as
readable only with the same options). Gates: one near-ceiling file through all four source modes,
repaired-schema rejection and opt-in, and an exhausted-admission case. The invariant is tested for null, empty-record, empty-fixed, all-null-field record,
nested-empty-array and nested-all-null-record roots, multi-block files near the ceiling, maximal
metadata, a maximal schema, **and the combinations** (maximal schema plus near-ceiling data; maximal
metadata plus a large plan).

**Latency gate for the provisional work constants.** `max_values_per_byte`, `work_allowance`,
`max_total_values`, `max_rows` and `max_compare_bytes_per_byte` are provisional; Phase 2 measures the slowest legal zero-byte datum and schema shapes
(null roots, all-null-field records, nested empty arrays/maps, deeply nested empty records, wide
objects and long-common-prefix keys) and Phase 4a adds the container shapes (the 32,768-empty-block
file, dense codec members) once the container reader exists, each on every supported Julia version on
the named host; the Phase 2 values are **provisional** and the constants are **finalised and recorded
only after the Phase 4a gate**, so
that the worst-density legal input admitted by the defaults decodes in **≤ 10 s** single-threaded (a worst-density gate, not a per-file guarantee for every admitted streamed file); the measured numbers
and the chosen constants are recorded in `manual/limits-and-security.md`. Trusted bulk workloads raise
them explicitly.

`Budget` is a per-operation accumulator (values, committed output bytes, in-flight reservations, input
bytes, compared bytes, rows, blocks, codec members, resolution work, allowance deficit) whose cumulative commits happen in block-index
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
a package-owned map with **deterministic capacity** — a keys vector and a values vector allocated at
`npairs` capacity (the decoded pair count; after last-wins compaction `length = nunique ≤ npairs`) and
a sorted `Int32` permutation of `npairs` capacity and `nunique` length — whose shell constant is **measured at
`__init__` from an empty `Avro.Map`** like every other constant, and which charges the **actual
allocated capacities** of all three parts plus the transient sort scratch, each reserved before it is
allocated; nothing is ever rehashed, re-seeded or grown; **no formula depends on `Base.Dict`'s private
layout or on its collision-driven rehashing**, and the oracle gate includes duplicate-heavy and
adversarial-key maps). **Typed fast-route shells** (§4.8) are measured once per `T` at plan
construction from a probe instance built by the same constructor-free builder with zero/empty field
values (`Base.summarysize` of the probe minus its referenced payload; a checked bound of
`sizeof(T) + 8 × fieldcount(T) + 64` bytes is reserved before the probe is built and trued up
afterwards), so the charge is exact for
mutable heap objects (8-byte minimum even with no fields), for immutable non-isbits structs (inline in
their parents, a heap object only when boxed) and for isbits layouts; the oracle covers generated
nested, padded, mutable, zero-field, reference-bearing and nullable layouts and arrays of them. The test
oracle is `Base.summarysize(x; exclude=Avro.Schema)` — schema references carried by `Fixed`,
`EnumValue` and `Record` are excluded because the schema graph is charged once under category (e) — and
the gate `storagebytes(x) ≥ summarysize` holds for generated values of **every member of `E`** and for
the three identity-bearing types with shared and with distinct schema identities, and the `Avro.Row` shell (record reference plus admission reference, measured with the already-accounted `Record` and `SymbolAdmission` excluded) whose ownership transfers to the caller at yield; an isbits value stored
inline in a typed column or chunk charges nothing here (its slot is in (a)); ownership of payload
transfers from in-flight chunks to committed columns at commit **without re-charging**; **exactness covers the generic value set `E`, the column path, and the typed fast route's approved
representations** (§4.8: isbits fields, `String`, `Vector{UInt8}`, `Vector{T}`, `Union{Missing,T}`,
`NamedTuple`s and plain structs assembled from them, `Base.Enum`, `Symbol` through admission,
`Avro.Map`); every other typed target takes the semantic route, in which the generic value is decoded
under the ceiling and **converted after ownership transfer, in caller space** (§4.8 — the typed
conversion boundary; `Dict` fields and custom StructUtils hooks live there); (c) **input, codec and output buffers** — the
compressed block buffer (streamed sources), the decompressed buffer as it grows, the codec workspace
(the library-reported requirement of the member about to be decoded where the library reports one —
zstandard `ZSTD_estimateDStreamSize_fromFrame` — and otherwise the configured `max_codec_memory` — xz,
whose per-block requirement liblzma enforces against `memlimit` but does not report before allocation;
fixed bounds for bzip2 ≤ 3.7 MiB, deflate ≤ 64 KiB, snappy 0), the `Encoder` buffer, compressor output
buffers and JSON text buffers (each growth step reserves the replacement capacity before it is
allocated), and owned copies of
non-conforming byte and string sources (§4.2 step 1); (d) **internal tables** — block table, prefix
sums, row offsets, the symbol-admission table (the sorted-runs structure above, every run and merge scratch reserved
before allocation, under its own `max_names`/`max_bytes`), and worker/coordinator state (a documented 16 KiB per worker task; task stacks
are allocated lazily by the runtime and excluded, documented); (e) **schema and plan graphs** — every
`Schema`/`Field`/`FullName` node, frozen JSON tree (props and defaults), parser state (`ParseContext`,
name tables), printer/canonical/fingerprint buffers, read/write/resolving plan nodes and their
memoisation tables — all of which are keyed by the **dense node IDs** assigned by `freeze!` on every creation path (a node-indexed dense array
when the key is one node — plan memo tables, `PlanRef` targets; per-node **sorted** small vectors of partner IDs
when the key is a pair — equality visits, resolution memo entries, resolving-plan pairs — binary-searched,
with every inspected entry and every insertion move charged to `max_resolution_work` (one node paired
with many partners near the limit is a gate), with adversarial
reverse-ordered, cyclic and near-limit tests) or are sorted vectors with charged moves (the named-type
table), never `Base.Dict`/`Set` and never hashing, and JSON.jl's duplicate tracker is never enabled (§4.2) — charged per node by the
same kind of measured formulas under the budget of the operation that creates them (`parseschema`, `json`/`canonical`/`fingerprint`, `Avro.schema(T)`,
`resolve`, prepared-codec construction — each an operation with its own ceiling, §"Budget scopes"), so
a schema of `max_schema_nodes` nodes with large properties or defaults fails with a `LimitError`
instead of allocating past the ceiling (tests: shallow wide schemas, large defaults/props, and large
plans near the ceiling). **Yield-time transfer:** `Rows`, `eachdatum` and `eachblock` hand each yielded
value to the caller at yield; the operation releases that value's charge at the next iteration step
(the caller now owns it) while the cumulative value/row/work counters persist, so a stream larger than
the ceiling iterates under the ceiling when the caller does not retain the outputs (tested both ways);
`Avro.Table` keeps everything charged until it returns the caller-owned table. **Growth rule (every charged buffer).** No charged buffer is ever grown with `push!`, `resize!` or
`sizehint!` (Julia's growth heuristics over-allocate behind the reservation: `sizehint!(Int[], 1)`
retains three slots on Julia 1.12); growth always allocates an explicit replacement
`Vector{T}(undef, newcap)` of exact capacity after reserving it (old and new storage are both charged
during the copy; the old charge is released afterwards), through package-owned builders used by
generic collections, streamed columns, the `Encoder`, owned JSON/IO buffers and compressor output
buffers (allocation-hook gate: every allocation in a guarded path is preceded by a reservation of at
least its size). Collections in the generic path are decoded incrementally with geometric replacement
growth starting at `min(count, 1024)` elements, the declared count is additionally bounded by
`remaining_bytes ÷ minsize(item schema)` when `minsize > 0`, and maps are decoded as key and value
vectors first (charged as such) and materialised into one `Avro.Map` sized from the decoded pair count
(duplicate keys: the last value wins and the entry keeps the **first occurrence's iteration position**;
documented and tested; after in-place compaction the vectors and the permutation retain their `npairs`
capacity, which stays charged — an all-duplicate map is charged `npairs` slots, not one).
`minsize(schema)`
is the minimal encoded size of a datum: a memoised, cycle-safe fixed point (a required recursive cycle with no
nullable/array/map escape has no finite datum and yields `∞`; unions take the minimum over branches
plus the exact varint length of that branch's index; records sum fields;
arrays/maps contribute 1; tested on direct recursion, mutual recursion, nullable recursive unions, and a
recursive schema with no finite datum). Every corpus file in §8 except the deliberately over-limit fixtures (high-window codec blocks, the 32,768-empty-block work-rule file, codec bombs) decodes under the defaults.

**Budget scopes.** A container `Reader`/`Writer`, `Avro.Table`, `Avro.Rows`, `Avro.inspect`, `compare`, `tojson`,
`fromjson`, `resolve`, `parseschema`, `json`/`canonical`/`fingerprint`/`parsingequivalent`,
`Avro.schema(T)`/`Avro.schema(x)`/`Avro.schema(::Tables.Schema)`, the public schema constructors
(`Avro.RecordSchema(...)` and friends take `limits=Limits()`; raw node constructors are private), the
copying public value constructors (`Avro.Record`, `Avro.Fixed`, `Avro.EnumValue`, `Avro.Map`),
`encodesingle`/`decodesingle`/`register!`/`lookup`, and the construction of a prepared
`DatumReader`/`DatumWriter` each own one operation budget for their lifetime and take `limits=`
(§5.1); each prepared `DatumWriter` call precharges the retained `Encoder` capacity to its fresh budget; an operation that calls another passes its own `Budget` through an internal `budget` argument,
so nested operations never stack fresh default budgets (`JSON.json(schema)` uses `Limits()`); each call of a prepared `DatumReader`/`DatumWriter` and each one-shot `decode`/`encode` gets
a fresh operation budget (the one-shot forms charge plan construction to that budget). A
`SymbolAdmission` object owns its own table budget.

### 4.5 Plans: schema-directed codecs (`src/plan_read.jl`, `src/plan_write.jl`)

* A **read plan** is a tree of plan nodes, one per schema node. The **generic** plan family is dynamic
  (`Vector{ReadPlan}` children, one dynamic dispatch per child behind a function barrier) and produces
  the finite value set of §4.6; it never specialises on schema shape. Named-type recursion is represented
  by `PlanRef` nodes: plans are built in two passes keyed by the dense node IDs of §4.4 (and by
  `(writer, reader)` ID pairs for resolving plans), so `LongList`-style schemas and recursive writer/reader
  pairs terminate; plan construction is charged to `max_resolution_work`.
* The **typed** plan family (`Avro.DatumReader(schema, T)`, `Avro.Rows(src; T)`) specialises on a
  caller-supplied `T`: `RecordPlan{names, Ps<:Tuple}` unrolled via `ntuple`/`Val` (any width the caller
  chose), leaf plans per Julia type, recursion through the user's own recursive struct types. Targets
  that intern (`Symbol` fields, `Symbol`-valued enums) decode through the symbol-admission object of the
  operation (§6; default process-wide, caller-owned via `names=`, bypass via `names=:trusted`) on every
  typed path: `DatumReader`, `decode`, `decodesingle`, `fromjson`, `Rows` (`Table` has no `T`).
* The **column** plan family (`Avro.Table`) uses `ColumnBuilder{e}` objects for `e` in the enumerated
  set `E` of §4.6, stored in a **schema-independent `Vector{ColumnBuilder}`** and driven by one loop with
  a function barrier per column (one dynamic dispatch per cell; no tuple unrolling, no specialisation on
  column order or width). Unselected fields use `skip`.
* Write plans mirror read plans with value-extraction strategies for NamedTuples, StructUtils structs
  (§4.8), `Tables.AbstractRow`s (by column index), `AbstractDict`s, iterables, and the generic values.
* **Prepared codecs.** `Avro.DatumReader(writer_schema, T=…; reader_schema, union_resolution, limits,
  validate, names)` owns its plans, is immutable after construction and **safe to share across
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
| enum | `Avro.EnumValue` (enum schema reference + `Int32` **1-based position** into `symbols`; `Avro.ordinal(x)` is the zero-based wire value; `String(x)`, `Symbol(x)` (an explicit, caller-owned interning action outside the ceiling; §6); `==` by fullname and symbol string; `show` prints the symbol) | typed API accepts `Base.Enum` subtypes, `Symbol` (through admission), `String` (validated); encoding under a different enum schema remaps by symbol, never by index |
| array | `Vector{e}` with `e = juliatype(items)` when that is in L ∪ {`Record`, `UnionValue`} ∪ {`Union{Missing,…}`} of those; `Vector{Any}` when items are themselves arrays or maps | one level of typed nesting |
| map | `Avro.Map{e}` with the same rule — a package-owned `AbstractDict{String,e}`: insertion-ordered key and value vectors plus an `Int32` permutation sorted by decoded key bytes, built once by a deterministic merge sort from the decoded pairs (§4.4 map structure); `getindex` is a binary search with a fixed per-call bound (≤ ⌈log2(nunique)⌉ + 1 comparisons; no budget is retained in a returned map); no hashing, seeds, rehashing or growth, so memory and work depend only on the counts and key bytes; terms: `npairs` (decoded pairs), `nunique` (retained entries after last-wins), capacities (`npairs` for the vectors and for the permutation, whose length is `nunique`); `Dict(m)` converts; duplicate keys: last wins | the typed API accepts `Dict{String,T}`/`AbstractDict` targets (built outside exact accounting, §4.4) and encodes any `AbstractDict` whose keys are `AbstractString`s or `Symbol`s (converted with `String`; two keys that convert to the same string — `:a` and `"a"` — are an `EncodeError`) |
| union | **bare value** only for the two-branch nullable form `["null", T]` / `[T, "null"]`: `Union{Missing, juliatype(T)}`; **every other union** decodes every value — the null branch included — as `Avro.UnionValue(index, value)` with `index` the **1-based position** into `branches` (`Avro.ordinal(x)` is the zero-based wire index) | closed set by construction; branch recovery on encode: (1) for bare nullable values, `missing` and `nothing` → the null branch, anything else → the other branch; (2) `UnionValue` is exact; (3) an identity-bearing value (`Record`, `EnumValue`, `Fixed`) selects the branch whose **named schema identity** it carries (fullname; for fixed also size) — before any representation-type rule, so several record/enum/fixed branches never collide (gated with multiple named branches of each kind) — and has no branch if none matches; (4) for other plain Julia values, a branch whose generic representation type equals `typeof(value)` exactly, else the first branch that accepts the value by conversion. **Under resolution the representation follows the reader schema** (§4.7) |
| record | `Avro.Record` (schema reference + `Vector{Any}` values; `record[:f]`/`record["f"]`/`getproperty` look a name up **without interning** — the caller's `Symbol` is compared against the field-name strings; `keys(record)` returns `String`s; `==` structural). The Tables row interface (`Tables.AbstractRow`, `Tables.columnnames`, `propertynames`; `NamedTuple(row)` is an explicit caller-space conversion outside the guarded specialisation guarantee — it compiles one layout per field-name set — and Avro never invokes it internally) is provided by `Avro.Row`, the row type yielded by `Avro.Rows`, which wraps the record together with the **operation's admission object** (§6), so every `Symbol`-producing path honours the caller's boundary; a detached `Avro.Record` from `decode`/`eachdatum` is not a Tables row (wrap it with `Avro.Row(record; names=…)` to get one) | stable at every width and for recursive records |
| decimal(bytes/fixed) | `Avro.Decimal` (`Int128` unscaled, `scale::Int`) for precision ≤ 38; `Avro.WideDecimal` (`BigInt` unscaled, `scale::Int`) for larger precision (chosen per schema at plan time; any `Int` precision/scale the schema carries is representable) | big-endian two's complement; minimal bytes for `bytes`, sign-extended to size for `fixed`; an empty `bytes` payload is rejected (Java raises `NumberFormatException`); absent `scale` = 0; **both encode and decode validate `digits(unscaled) ≤ precision`** (`EncodeError` / `DataError`; Java validates only on encode — recorded deviation in favour of the spec's "maximum precision"); encode requires exact scale equality |
| uuid (string / fixed 16) | `UUIDs.UUID` | string form must be RFC-4122 `8-4-4-4-12` hex, either case (`DataError` otherwise); fixed form is the 16 big-endian bytes; the representation (string vs fixed, letter case) is **not** carried by the value — round trips use the writer schema (§4.8) |
| date | `Dates.Date` | any `Int32` day count is representable |
| time-millis / time-micros | `Dates.Time` | exact (ns-resolution `Time`); decode range-checked to one day; **encode rejects non-aligned values** unless the caller aligns them with `Avro.truncate(x, P)`/`Avro.round(x, P)` (Java truncates silently — recorded deviation) |
| timestamp-millis / micros / nanos | `Avro.Timestamp{Millisecond|Microsecond|Nanosecond}` — exact `Int64` ticks since the Unix epoch, a global instant | `DateTime(x)` is an explicit, **range-checked** conversion raising `Avro.ConversionError` (never `DataError`, never wrap), lossy (floor) for sub-ms units; `Avro.Timestamp{P}(::DateTime)` exact and range-checked; `ZonedDateTime` via the TimeZones extension: `Avro.schema(ZonedDateTime)` is `timestamp-millis` (a global instant at `DateTime` precision); encoded as the UTC instant at the schema's precision (sub-unit precision is rejected like `Time`, §4.3), decoded as `Avro.Timestamp{P}` — the zone is not carried by Avro; `ZonedDateTime(x, tz)` is the explicit reconstruction |
| local-timestamp-millis / micros / nanos | `Avro.LocalTimestamp{Millisecond|Microsecond|Nanosecond}` — exact `Int64` ticks | same conversion rules |
| duration | `Avro.Duration(months::UInt32, days::UInt32, millis::UInt32)` | little-endian unsigned |

Timestamps always decode to the exact wrappers in the generic model (there is no `instants=` option:
`DateTime` is outside the closed set `E`); a `DateTime` is obtained either by the explicit, range-checked
`DateTime(x)` conversion (`ConversionError`, floor-to-ms for sub-ms units) or by a typed `T` whose field type is `DateTime` (the typed plan performs the same conversion; an explicit `T` is always
authoritative); a typed `ZonedDateTime` target decodes as the UTC instant (`ZonedDateTime(x, tz"UTC")`,
the zone being absent from Avro), a typed `Char` target requires a one-character string, and every
such failure is a `ConversionError`. The value model is identical for `Avro.decode`, `Avro.Rows`, `Avro.Table` cells,
defaults, JSON conversion, and sorting.

### 4.7 Schema resolution (`src/resolution.jl`)

`Avro.resolve(writer::Schema, reader::Schema; union_resolution=:spec, limits=Limits()) ->
ResolvedSchema` implements every rule of the "Schema Resolution" section, memoised on `(writer, reader)`
dense-ID pairs (§4.4 category (e)) and **bounded by `max_resolution_work`** (every branch-match
attempt, memo entry, memo scan step and resolving-plan node is charged; exceeding it is a
`LimitError`):

* Match by kind: arrays/maps recursively; enums, fixed (plus size), records by name — first by exact
  normalised fullname (reader type aliases are normalised fullnames, §4.2), then by the ordinary
  unqualified-name match; primitives equal or promotable `int→long/float/double`,
  `long→float/double`, `float→double`, `string↔bytes` (`bytes→string` validates UTF-8 at decode time).
* Records: fields matched by name after reader field aliases; **a reader alias consumes the writer
  field** (a reader that also has a field with the writer's original name gets no value for it and must
  have a default; a reader field whose name or aliases match more than one writer field is a
  `ResolutionError`); writer-only fields are skipped (validation modes §4.3); reader-only fields require a
  valid default (fresh copy per record); reader field order wins.
* Enums: symbol remap by name; writer symbols absent in the reader use the reader `default` or error.
* Unions: `union_resolution=:spec` (default, normative) selects **the first reader branch that matches
  the writer branch, where matching includes promotion** (writer `int` against reader `["long","int"]`
  selects `long`; against `["double","int"]` selects `double`). `union_resolution=:java` reproduces
  Apache Java's three-pass selection. Its first pass accepts only the exact kind and, for named types,
  exact fullname or reader alias. If no exact branch matches and the writer is a record, its second pass
  probes record branches by Java's structural soft rule: reader field names and aliases must make
  unambiguous one-to-one claims on writer fields; unmatched reader fields need valid defaults; immediate
  non-record field actions must match or promote. A nested record action is provisionally accepted, and
  named-record checks are suppressed recursively only after that soft candidate is selected. A failure
  while building the selected candidate does not retry a later branch. The first structural candidate is
  retained, except that a later candidate whose unqualified name equals the writer's unqualified name
  replaces it (the last such short-name candidate wins). The final pass applies the ordinary match and
  promotion rules in branch order. Both policies are tested on
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
* Logical types (additional to the underlying-kind, fullname and fixed-size rules above): two
  recognised `decimal`s match only if both precision and scale match (`ResolutionError` otherwise). Every other pairing — a recognised logical type against a plain
  underlying type, or two **different** recognised logical types on matching/promotable underlying
  schemas (`timestamp-millis` → `timestamp-micros`, global → local, `date` → `time-millis`) — resolves
  through the underlying schemas and the **reader's interpretation wins over the raw value** (no unit
  conversion is performed; the documentation flags the hazard). Pairings whose underlying kinds differ
  (`uuid` string → `uuid` fixed) fail like any other kind mismatch.
* Failures are `ResolutionError`s carrying both paths. The result is consumed by the internal `plan` function to produce a
  resolving read plan (writer-driven field order with `SkipPlan`/`DefaultPlan`/`PromotePlan`/
    `EnumRemapPlan`/`UnionRemapPlan`/`UnwrapPlan` nodes; a writer-union branch that cannot resolve
  against the reader becomes an `UnresolvableBranch` node that raises `ResolutionError` only when a
  datum selects it — gate: writer `["int","string"]` to reader `"long"` decodes an integer datum and
  raises on a string datum).

### 4.8 Typed API and Julia type mapping (`src/types.jl`, StructUtils)

* `Avro.schema(T::Type)` derives a schema from a Julia type: the §4.6 table inverted, plus `Int8/16/
  UInt8/16 → int`, `UInt32/UInt64 → long` (range-checked on write; typed decode into any narrow integer target is range-checked with `ConversionError`), `Float16 → float` (typed decode rounds to nearest, documented as lossy), `AbstractString`/
  `Symbol`/`Char → string`, `NTuple{N,UInt8} → fixed` named `fixed_N` (e.g. `fixed_16`) by convention (the enclosing
  record's namespace applies), `Union{Missing,T} → ["null", T]`, other `Union`s → union in Julia's
  member order (documented) — **members that map to the same unnamed Avro kind (e.g.
  `Union{String,Symbol}` → two `string` branches) are an `ArgumentError` naming the colliding members,
  never silently deduplicated** — `Base.Enum` subtypes → enum, `DateTime → local-timestamp-millis` (a
  naive `DateTime` is a local timestamp; global instants are `Avro.Timestamp{P}` or `ZonedDateTime`),
  `Date → date`, `Time → time-micros`, `UUID → string uuid`, `Nothing → null` (so `nothing` encodes
  schema-free; `Union{Missing,Nothing}` collides on two `null` branches and is an `ArgumentError` like
  any other union-member collision), `Avro.Decimal` → no type-level schema (a decimal needs
  precision/scale: `schema=` is required), structs → records via StructUtils
  (`fieldnames`/`fieldtypes`; `@kwarg`/`@defaults` field defaults become Avro defaults when they are
  JSON-encodable under the field's schema, and an `ArgumentError` names the field otherwise — the
  `&(avro=(default=…,),)` tag supplies an explicit JSON default), `Tables.Schema` → record (through the `names`/`types` accessors, which also serve
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
  their instance schema (`Record`, `EnumValue`, `Fixed`); a `Vector`/`Avro.Map` whose elements are all
  identity-bearing values of one schema identity returns the array/map of that schema, while mixed
  identities, `Vector{Any}`/`Avro.Map{Any}` and empty collections of identity-bearing element types are
  `ArgumentError`s directing the caller to `Avro.encode(schema, x)`; plain Julia values use the **documented
  conventional schema** `Avro.schema(typeof(x))`; a bare `UnionValue` (no enclosing union) and a
  `Decimal` raise an `ArgumentError` directing the caller to `Avro.encode(schema, x)`. Schema-free
  encoding constructs datums from Julia values by convention; it is **not** a round-trip mechanism for
  decoded data — `UUID` (string vs fixed) and `Vector{UInt8}` (bytes) lose their source representation —
  and the documentation says so: round trips go through the writer schema (`Avro.write(dst,
  Avro.Rows(src))`, `Avro.schema(table)`).
* **Typed decoding** goes through a dedicated `Avro.AvroStyle <: StructUtils.StructStyle`. A plain-DTO
  fast route (direct field-wise construction from a typed plan) is used only when an eligibility check
  passes: `T` is a concrete struct or `NamedTuple`, no custom `StructUtils.make`/`lift`/`choosetype`
  methods apply for `AvroStyle`, no field tags affect construction, every field has a mapped schema, and
  any field defaults are static. **The fast route never invokes a user constructor:** `NamedTuple`s are
  built directly and structs — mutable and immutable alike — are built with `Expr(:new, T, fields...)`
  in a function generated for the compile-time type `T` (the form a default constructor lowers to; no
  temporary field vector, no boxing; consistent with the trim rule, which forbids generation on runtime
  data, not on `T`), so no user code runs inside the ceiling and the route's allocations are exactly
  the approved representations (every field is always provided — reader-only fields need defaults — so
  no partially initialised object is ever produced; tests over mutable, immutable, zero-field, isbits
  and reference-bearing layouts on every supported Julia version); the documentation states that typed decoding,
  like `deserialize`, does not run inner constructors — callers who need constructor validation use the
  semantic route (a `StructUtils` hook) or validate afterwards (regression test: a struct with an
  allocating, throwing positional constructor decodes on the fast route without calling it). Everything else takes the semantic route: generic decode then
  `StructUtils.make(AvroStyle(), T, generic)`. **Typed conversion boundary:** the fast route allocates
  only the approved representations of §4.4 (exactly charged); the semantic route decodes the generic
  value under the ceiling and then runs the conversion **after ownership transfer, in caller space** —
  exactly as if the caller had called `StructUtils.make` on a returned generic value — so `Dict` fields and
  custom hooks are outside the ceiling by construction and the documentation says so (untrusted input that must stay under the ceiling is decoded generically and converted by the
  caller). Both routes are tested against the pinned StructUtils version, and a test asserts the fast
  route is never taken when a custom hook exists. `Symbol` targets
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

* `Avro.Reader(src; limits, legacy=nothing, decimal_byteorder=:big, allow_invalid_names=false,
  allow_invalid_defaults=false, validate=:strict, mmap=true)` (do-block form available): parses the header (magic; metadata map decoded — into an `Avro.Map{Vector{UInt8}}`, charged; duplicate keys in the header map, including `avro.schema`/`avro.codec`, are a `DataError` — with the spec's
  `{"type":"map","values":"bytes"}` schema under `max_metadata_bytes`/`entries`; sync marker;
  `avro.schema` — required; its absence is a `DataError` — parsed with §4.2 under `max_schema_bytes` and the repair options), selects the codec from
  `avro.codec` (its bytes must be valid UTF-8 — a `DataError` otherwise, before any codec lookup;
  `null`/absent, `deflate`, `snappy`, `bzip2`, `xz`, `zstandard`; unknown → `UnsupportedCodecError`), and iterates blocks **strictly**: `0 ≤ count ≤ max_block_count`;
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
  recognises **skippable frames** (magic `0x184D2A5?`): they are consumed, produce no output and no codec-workspace charge, count as one member/value each
  under the work rule, and are bounded by the payload; a truncated skippable frame is a `CodecError`. Xz **stream
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
  are valid. **Any root schema** is supported: `eachblock(r)` → `(count, bytes)` where `bytes` is the
  **owned decompressed block** — ownership of the decompression buffer is transferred to the caller, no
  second copy is made, and the reader reserves the next block's buffer afresh, so `reader_block_peak`
  holds (boundary fixture) — which remains valid after iteration advances (the compressed form is not
  exposed; in `:strict` mode exactly `count` datums are walked and exhaustion checked before
  the block is yielded, in `:fast` mode it is yielded after decompression), and `eachdatum(r)` → schema-directed generic values; `Avro.Rows` yields
  `Avro.Row`s (§4.6: the record plus the operation's admission object) for record roots in generic
  mode, plain `T` values in typed mode, and §4.6 values for non-record roots (the Tables.jl row
  interface exists only in generic record mode, §6); `select=` applies to record
  roots only, is not combinable with a typed `T`, and rejects duplicate or unknown names
  (`ArgumentError`); a projected schema keeps the root's fullname, aliases, doc, props, `iserror`,
  repair flags and graph limits, and a recursive root is rewritten with one graph-wide memo that
  substitutes the new root for the old one in every selected field; `Avro.Table` requires a record root and raises a clear `ArgumentError`
  otherwise. Sources: file path (memory-mapped by default; `mmap=false` opens the file and uses the
  **sequential streaming block reader** — the file is never read whole into memory), `Vector{UInt8}`/
  views (caller-owned, not charged), `IO` (streaming: the compressed block buffer and its decompressed buffer are both reserved under the
  ceiling before they are allocated; one block resident), `IOBuffer` (its written bytes only).
* **Legacy mode.** `legacy=:avrojl1` enables exactly three *unambiguous* tolerances for files written
  by Avro.jl ≤ 1.1.2: `avro.codec == "zstd"` read as zstandard; null-codec blocks with trailing bytes
  after `count` datums accepted (one `@warn` per source); and **nameless fixed schemas** (amendment,
  implementation round 1, evidence-driven: Avro.jl ≤ 1.1.2 serialised fixed schemas without a `"name"`
  attribute — the committed real 1.1.2 fixtures prove it — so under `legacy=:avrojl1` a fixed schema
  object in a **container header** whose `"name"` key is absent parses with a synthesised name
  `_avrojl1_fixed_N`, `N` the 1-based ordinal of the nameless fixed in document order; the tolerance
  applies only to the missing-key case — an invalid *present* name still requires
  `allow_invalid_names=true` — and never outside legacy mode). The 1.x native-endian decimal defect is **never
  inferred**: reinterpreting fixed/bytes decimals little-endian requires the explicit keyword
  `decimal_byteorder=:little` (default `:big`); recovery covers `bytes` decimals (length-prefixed, so
  framing is intact) and `fixed` decimals whose declared size is 16 — 1.x wrote 16 bytes regardless of
  the declared size, so a `fixed` decimal of any other size is misframed and unrecoverable (a real
  non-16-size 1.1.2 fixture is committed and `Avro.inspect` reports it). The deprecated `readtable` shim sets `legacy=:avrojl1`
  only. Strict mode is the default for every new API.
* **Ownership and lifetime.** `Avro.Table` copies all data: path sources are opened, mapped or streamed,
  decoded, and closed within the call; caller-owned `IO`/byte sources are left open and unreferenced
  afterwards. `Rows` and `Reader` hold a path-owned mapping/handle until `close` (idempotent; iteration
  after close throws; the do-block forms `Avro.Rows(f, src; kw...)`/`Avro.Reader(f, src; kw...)` close
  on exit, and a finalizer closes a path-owned handle that was never closed, documented as a fallback); for caller-owned `IO` they only drop their reference on `close` and never close
  the caller's stream. Truncating a memory-mapped file while it is being read is undefined at the OS
  level (SIGBUS); `mmap=false` (streamed) is the safe option for files that may change and is documented
  as such.
* **Parallel decoding** (`Avro.Table` only, memory-mapped or byte sources, `ntasks > 1`; `IO`/streamed
  sources decode sequentially into **per-block exact chunk columns** — each block's count is known from
  its header — retained until the end and assembled once into exactly sized final columns, with the
  exact deterministic peak of §4.4). **Design rule: parallelism only ever uses headroom.** Stage 1
  pre-scans block headers (no decompression) into a block table with checked prefix sums under
  `max_rows`/`max_blocks` (the table charged to the ceiling) and preallocates every final column once at
  its exact Julia storage (§4.4 category (a)), charging that capacity up front. Stage 2 decodes the lowest
  uncommitted block directly into the final columns and every higher in-flight block into per-block
  chunk columns, committing in index order:
      1. **The direct head follows the sequential rule exactly and physically.** The caller's task is the
     *head*: it decodes the lowest block that has not been assigned to a worker **directly into its
     final-column slice**; **admission-wave barrier:** after finishing its direct block the head never
     starts a block beyond an unresolved lower worker block — it waits for that fully reserved worker
     block (which needs nothing and cannot deadlock), commits its chunk, and only then takes the next
     unassigned block — so assignment is in block order and a failure at block *k* can leave at most
     the higher blocks that were in flight at that moment as surplus (the §4.9 failure bound) (its prefix-sum offset is known and the final columns are
     preallocated), exactly as the `ntasks = 1` direct path does — no chunk columns, no parallel-only
     allocation of any kind — reserving actual sizes before every allocation (§4.4 categories
     (b)–(d)), never waiting for memory (only for the barrier above), and failing with a `LimitError`
     exactly when sequential decoding would fail at that block (`max_block_output_bytes` counts the slots and payload the block
     produces, identically on both paths).
    2. **Higher blocks are admitted strictly in block order and only into headroom.** Block *i* (not the
               lowest) is admitted when `live_base + committed_payload + capacity + sequential_peak_reserve + worker_state +
     Σ_{in-flight j < i} W_j + W_i ≤ ceiling`, where `W_j` is the **complete worst-case reservation** of
     block *j*: its compressed size, `max_block_bytes` for the decompressed buffer, the codec workspace
     (§4.4 (c)), its chunk shells (`ncolumns` headers plus chunk capacities for `count_j` rows), the
     chunk payload up to `max_block_output_bytes`, the transient maxima of every category-(b)/(c)
     scratch it may use (map-sort permutation and scratch, replacement-growth overlap, WTF-8 decoding
     scratch, each bounded by the block's own caps), and its per-block state — reserved in full at
     admission and never exceeded; otherwise it waits for the next commit. The worker pool itself is
     created **only from headroom**: if `worker_state + W` for one extra block does not fit above the
     sequential peak reserve, no workers are created and the operation *is* the `ntasks = 1` direct
     path. Admitted blocks decode under their own reservation (a block that would exceed `W_j` is
     malformed under the hard caps and fails as content). **Every decoded block is decoded at most once — exactly once on success — and
     nothing is ever evicted, retried or restarted.**
    3. **Liveness.** The lowest uncommitted block holds everything it needs and waits for no memory; every
     other in-flight block holds a full reservation and needs nothing more; the head's barrier waits
     only on such blocks; commits proceed in index
     order; waiting blocks are admitted after commits. `ntasks` counts the caller's task, which always
     decodes the direct lowest block, plus the workers: a pool of at most `min(ntasks − 1,
     Threads.nthreads() − 1, inflight, nblocks − 1)` worker tasks (`inflight = min(max_inflight_blocks
     == 0 ? ntasks − 1 : max_inflight_blocks, ntasks − 1)` is the number of higher blocks that may be in
     flight) is created once, only as far as the headroom admits, and its state is charged (§4.4 (d));
     `ntasks = 1` creates no workers.
  4. **Streaming ordered assembly.** The coordinator commits the lowest uncommitted block in index
     order: it checks the cumulative rules (`max_total_values`, `max_rows`, the work rule, the exact
          committed total) and, for a chunked block, copies isbits chunk data into the preallocated final
     columns at the block's prefix-sum offset (references are moved, and their payload charge moves
     from in-flight to committed without re-charging), then frees the chunk shells and releases the
     block's reservation; a direct block's commit only performs the checks and releases its
     reservation.
    5. **Deterministic failure ordering.** Cumulative budget failures occur at the coordinator in index
     order; content failures record their block index in an `@atomic` minimum; a stage-1 structural
     failure at block *k* (a bad block header found during the pre-scan) is carried as a pending indexed
     failure while earlier blocks run, so a lower content failure still wins; workers abandon only
     blocks with a *higher* index than a recorded failure; after all tasks settle, the error of the
     lowest failing block index is rethrown — the lowest index wins regardless of failure kind.
    (`live_base` is every persistent live charge not represented by `capacity`, `committed_payload`,
  `worker_state`, the sequential reserve or `W` — header and metadata representation, schema graph,
  plans, block table, coordinator state — and the final `Avro.Table` shells: the table wrapper, its column-reference container, the stored `Tables.Schema`, metadata and
  partition state and the row count, all measured in the storage oracle; `W` is charged atomically
  against that total.) **Acceptance is identical for every `ntasks` by construction**: the lowest uncommitted block is
  always processed under the sequential rule, and higher blocks only consume headroom above the
  sequential peak reserve (`sequential_peak_reserve` = one `W` for the lowest block, so that the lowest
  block's actual-size reservations always fit). **Work is identical to sequential for every successful
  operation** in deterministic units (each block decompressed and walked once) plus the assembly
  copies, bounded exactly by `assembly_bytes ≤ committed_bytes`, where `committed_payload` (the
  admission formula above) is the category-(b) payload committed so far — category-(a) capacity is the
  separate `capacity` term, never added twice — and `committed_bytes` is the total of category-(a) slot
  bytes and category-(b) payload bytes copied or moved out of chunked blocks; **on a failing operation** the parallel
  path may additionally have decoded the higher blocks that were in flight when the failure became
  authoritative — at most `inflight` blocks beyond the sequential run's work when the direct head is
  the authoritative failure, and at most `inflight − 1` when a higher block is — each bounded by its
  own per-block work caps — which the failure gates assert with both forced schedules (the error
  itself is identical); measured CPU time is informational with a predeclared ≤ 1.5 × tolerance on
  successful runs and is only recorded on failures (one cap-bounded speculative block can dwarf an
  immediate sequential failure).
  Under default limits (256 MiB ceiling; `W` at default caps is the sum of the declared terms — 16 MiB
  compressed, 16 MiB decompressed, 32 MiB codec workspace, 64 MiB chunk shells and payload, plus the
  scratch maxima and per-block state, whose default-cap maximum is measured after Phase 4b, when the
  chunk and table representations exist, and recorded before the Phase 4c gate — Phase 4a finalises
  the codec and value constants — illustratively ≈ 130 MiB pending that measurement) at most one extra block fits in flight — parallel speedups need raised limits, which the §10.2 thread gate states explicitly. The
  parallel gates: identical results and identical acceptance for `ntasks ∈ {1,2,8}` under default and
  raised limits (incl. files whose total exceeds the ceiling, which fail at the same block index); forced
  schedules where higher blocks wait for commits; and the peak-RSS gate, whose method is fixed now: a
  fresh process is warmed (`using Avro` plus one small decode), the input file is read into a
  caller-owned byte buffer and faulted **before** the baseline RSS is recorded (so the parallel
  byte-source path is exercised, not the sequential streamed path), the table is decoded with `ntasks =
  8` under `Limits(max_total_bytes = 4 GiB)`, a test-only `@atomic` high-water counter asserts that at
  least two blocks were in flight concurrently, and `peak_rss − baseline_rss ≤ effective_ceiling +
  128 MiB` with both numbers recorded. **The measurement primitive (a sampled high-water mark,
  supplemented by OS and allocator high-water marks):** the decode runs in a child process; the parent
  samples the child's *current* resident set (`ps -o rss= -p <pid>`, every 10 ms, on macOS and Linux)
  from the moment the child prints and flushes a start line — emitted after the warm-up and after the
  input buffer is faulted — and the **parent acknowledges it** (the child does not start decoding before
  reading the acknowledgement, so the baseline never includes decode allocation) until it prints and
  flushes a done line; `baseline_rss` is the first sample;
  `peak_rss` is the maximum of the samples, the child's `Sys.maxrss` reported at the end, and the child's
  own `Base.gc_live_bytes()` high-water sampled around every block commit; on Windows only the child's
  `Sys.maxrss` is available and the gate is informational (documented). The deterministic reservation
  oracle (every allocation preceded by a reservation, asserted by the test-mode assertions inside the
  package's own allocation wrappers — every guarded allocation goes through `Avro.alloc`-style helpers —
  with an explicit, enumerated list of allocations outside the wrappers: codec libraries (covered by the
  workspace reservations; snappy decompresses into an Avro-owned buffer after querying
  `snappy_uncompressed_length`, never through `Snappy.uncompress`'s own allocation), `Mmap` mappings,
  runtime `Symbol` interning (bounded by admission counts and bytes; excluded from the ceiling,
  documented), GMP allocations behind `BigInt` (the complete limb and temporary peak is reserved
  before a `WideDecimal` is constructed), task stacks (runtime-owned, documented), and
  `Base.summarysize` in test mode;
  `Profile.Allocs` is used only as a secondary check) is the primary safety gate; RSS is the physical
  confirmation. The gate runs on highly compressible 16 MiB blocks and on a table whose final columns
  exceed half the ceiling.
* `Avro.Writer(io_or_path, schema; codec=:null, level=nothing, metadata=Dict{String,Vector{UInt8}}() (any `AbstractDict{<:AbstractString,<:AbstractVector{UInt8}}` is accepted and its values copied, so `b"…"` works),
  sync=nothing, block_bytes=64*1024, atomic=true, fsync=false, allow_invalid_names=false,
  allow_invalid_defaults=false, limits=Limits())`: **option validation** — a repaired schema
  (`GraphInfo` flags) requires the matching option (§4.4 invariant);
  `sync` must be exactly 16 bytes and is copied (`nothing` → 16 bytes from `RandomDevice`); `0 <
  block_bytes ≤ limits.max_block_bytes`; `level` validated against the codec's documented range
  (`nothing` → codec default); **every user-supplied `avro.*` metadata key is rejected** (`avro.schema`
  and `avro.codec` come only from the constructor arguments); the header it is about to write is
  validated against `max_metadata_bytes`/`max_metadata_entries`, and the schema it serialises against
  the schema limits, exactly as a reader would. It writes the header, buffers encoded datums in an
  `Encoder` under every reader limit (§4.4 invariant), emits a block when `block_bytes` is reached, when
  the next datum would violate the block work rule, when the pending block's **exact reader-side
  output estimate** (§4.4 category (a)/(b) formulas, tracked per pending block) would exceed
  `max_block_output_bytes` — one consumer-independent formula: the **generic representation** of the
  block's datums (category-(b) payload plus 8 bytes per reference slot and `sizeof` per isbits slot of
  the §4.6 values; row wrappers, chunk and column headers are not counted), applied identically by
  `Reader`, `Rows` and `Table` — when its datum count would exceed `max_block_count`, or on
  `flush`/`close`; a single datum whose output estimate alone exceeds `max_block_output_bytes` is a
  `LimitError`; the compressed block is checked against `max_block_bytes` before emission (an
  incompressible block at the boundary is a fixture) (fixture: an `array<string>` datum of four million empty strings — ≈ 4 MiB on the wire,
  ≈ 96 MiB of reader-side slots and payload charges — is refused under defaults and accepted with a
  raised cap on both sides); `push!(w, datum)` / `write(w,
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
  the same cleanup; `close` is idempotent; the do-block form `Avro.Writer(f, dst, schema; kw...)`
  closes on normal exit and aborts on error, and a finalizer aborts a path-owned writer that was never
  closed (temp file removed; documented fallback). Append mode is deferred.
* `Avro.write(dst, table; schema=nothing, codec, allow_invalid_names=false, allow_invalid_defaults=false, limits, …)` (the repair options are forwarded to the `Writer`; `Avro.tobuffer` likewise): **schema precedence** — (1) an explicit
  `schema=`; (2) the **effective retained schema** of an Avro source (`Avro.Rows`/`Avro.Table`/`Reader`:
  the reader schema when one was given, else the writer schema — exposed as `Avro.schema(src)`, with
  `Avro.writerschema(src)` exposing the file's writer schema), so `Avro.write(dst, Avro.Rows(src))`
  preserves named records, enums, fixed, general unions, decimals and every logical type; (3) the
  conventional `Tables.schema` derivation; a source with none of these raises an `ArgumentError` whose
  message shows how to supply one (`Tables.dictrowtable` + `Avro.schema(Tables.schema(...))`) —
  inference is deferred (§3). Gates: direct copies with enum, fixed, decimal, logical and general-union
  values; evolved copies that add, drop, reorder and promote fields; for a non-record root, `Avro.write(dst, Avro.Rows(src))` writes the datums under the retained schema without a Tables interface. `Tables.partitions` become block boundaries (each partition ≥ 1 block); row encoding
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
the PCF, payload). `Avro.decodesingle(bytes, store; reader_schema=nothing, union_resolution=:spec, validate=:strict, limits=Limits(), names=…, T=…)` validates the marker, looks up the
writer schema by fingerprint, **recomputes the fingerprint of the returned schema and rejects a
mismatch**, resolves against `reader_schema` when given, decodes, and requires exact payload
consumption. `SchemaStore` interface: `Avro.lookup(store, fp::UInt64; limits=Limits()) -> Schema` (throws
`UnknownSchemaError`) and `Avro.register!(store, schema; limits=Limits()) -> UInt64` (a custom store
owns its table budget); the built-in
`Avro.SchemaCache(; max_entries=10_000, max_bytes=64 << 20)`: registration is idempotent only for a
schema that is structurally equal to the stored one (compared by an internal equality routine that
charges the caller's shared `Budget`; the graph-selected limits apply only to direct public `==`) (this
ambiguity rule is `SchemaCache`'s; a custom `SchemaStore` owns its own policy); an error schema and the otherwise identical record share a PCF, so registering both is an `AmbiguousSchemaError`; any other schema under an existing fingerprint —
whether a different PCF (CRC collision) or a parsing-equivalent schema with different logical
types/defaults/props — is rejected as `AmbiguousSchemaError`; entries and bytes are bounded, and the
index is a sorted `UInt64` fingerprint vector (binary search; insertion moves charged to the cache's
table budget), consistent with the no-hash-table contract.
Fingerprints are identifiers, not authentication (documented). Unknown fingerprints raise
`UnknownSchemaError(fp)`.

### 4.11 JSON encoding (`src/jsonencoding.jl`)

`Avro.tojson(schema, x; pretty=false, limits=Limits())` (bounded: output text ≤ `max_datum_bytes`,
values ≤ `max_total_values`, depth ≤ `max_json_depth`; cyclic in-memory values therefore fail with
`LimitError`) and `Avro.fromjson(schema, json, T=…; strict=true, unknown=:ignore, limits=Limits(),
names=Avro.DEFAULT_ADMISSION)` implement the JSON encoding. `fromjson` applies the same protections as
schema parsing — lexical pre-scan against `max_datum_bytes` and **`max_json_depth` (the same ceiling
`tojson` uses, so default output always re-parses under defaults; boundary round trips at 1024 and 1025
levels)**, Avro's own JSON reader with sorted decoded-key duplicate detection and raw number tokens (§4.2;
JSON.jl is never used for parsing), the operation budget (the work rule counts JSON
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
| record | object in field order | object: every field present exactly once; unknown members are ignored (Java-compatible; `fromjson(...; unknown=:error)` opts into rejection) | — |
| union | `null`, or a one-member object keyed by the branch's **fullname** for named types and the type name otherwise; ambiguous labels (§4.2) → `EncodeError` | exactly one member whose key names a branch; `null`; ambiguous labels → `DataError` | — |

**Defaults are the exception for unions:** a union-typed field default is the bare JSON of the value
(no wrapper), matched against the branches in declaration order (§4.2); record defaults follow the recursive default rule of §4.2 (missing nested fields
supplied from their own defaults; unknown members ignored with the span retained); every other row
applies to defaults verbatim (strict; the quoted non-finite forms are thus accepted in defaults — a
Java-compatible extension recorded in §14). `tojson` uses the §4.6 branch recovery for unions. NaN payload bits are not
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
* `Base.show`, `==` and `hash` on schemas use the recorded `GraphInfo` limits of the graphs involved
  (the larger when two graphs are compared) for their scratch structures.
* Parallel decode memory (final-column capacity plus committed payload plus the lowest block's
  actual-size reservations plus higher blocks' worst-case reservations plus internal tables) is bounded
  by the effective operation ceiling; higher blocks only use headroom and are never evicted; commits are
  ordered; the symbol-admission table is lock-protected. Concurrent operations each own a ceiling (documented; caller-side governor advised).
* Writer-side parallel compression is deferred to 2.x.

### 4.15 Module layout

```
src/Avro.jl            module, includes, public API docstrings, version-gated `public` declaration
src/errors.jl          error types
src/limits.jl          Limits (validated constructor), Budget, minsize, work rule, ceiling accounting
src/frozen.jl          FrozenVector, FrozenDict, FrozenRef, FrozenJSON, freeze!
src/names.jl           FullName, fullname algorithm, name validation, namespace resolution
src/admission.jl       SymbolAdmission (default process-wide table + caller-owned objects)
src/jsonreader.jl      Avro-owned JSON reader: pre-scan (size/depth/UTF-8/escapes/number grammar), WTF-8 string decoder, recursive descent
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

src/singleobject.jl    single-object encoding, SchemaStore, SchemaCache
src/deprecated.jl      1.x shims (legacy mode)
src/precompile.jl      PrecompileTools workload
ext/AvroCodecBzip2Ext.jl, ext/AvroCodecXzExt.jl, ext/AvroTimeZonesExt.jl
```

---

## 5. Public API (all qualified; nothing exported)

### 5.1 Schemas

```julia
Avro.Limits(; fields...)                                             # validated, fixed-default limits (§4.4); Avro.SchemaCache(; max_entries, max_bytes)
Avro.AvroError and subtypes: SchemaError, EncodeError, ResolutionError, LimitError, CodecError, UnsupportedCodecError, ConversionError, UnknownSchemaError, AmbiguousSchemaError, WriterClosedError, DataError (§4.13)
Avro.parseschema(src::Union{AbstractString, AbstractVector{UInt8}, IO}; allow_invalid_names=false, allow_invalid_defaults=false, limits=Limits()) -> Schema
Avro.NullSchema(; props=(;), limits=Limits()), Avro.BooleanSchema(; props=(;), limits=Limits()), Avro.IntSchema(; logical=nothing, props=(;), limits=Limits()), Avro.LongSchema(; logical=nothing, props=(;), limits=Limits()), Avro.FloatSchema(; props=(;), limits=Limits()), Avro.DoubleSchema(; props=(;), limits=Limits()), Avro.BytesSchema(; logical=nothing, props=(;), limits=Limits()), Avro.StringSchema(; logical=nothing, props=(;), limits=Limits()) — the §4.4 construction scope applies to the primitive wrappers exactly as to the complex constructors (amendment, implementation round 1)
Avro.ArraySchema(items; props=(;), limits=Limits()), Avro.MapSchema(values; props=(;), limits=Limits()), Avro.UnionSchema(branches; limits=Limits())
Avro.RecordSchema(name; namespace="", fields=Avro.Field[], aliases=String[], doc=nothing, iserror=false, props=(;), limits=Limits())
Avro.RecordSchema(f, name; kw...)   # recursion: `f(ref)` runs with the record registered but unfilled and returns its fields (register-before-fill, as the parser does); mutual recursion nests builders (an inner `RecordSchema(g, …)` may use outer refs); if `f` throws, the partial graph is discarded and nothing is registered; direct constructors otherwise accept only acyclic graphs
Avro.Field(name, schema; default=Avro.nodefault, order=:ascending, aliases=String[], doc=nothing, props=(;), limits=Limits())   # copies aliases/props/default text under `limits`
Avro.DecimalLogical(precision, scale=0), Avro.UUIDLogical(), Avro.DateLogical(), Avro.TimeMillis(), Avro.TimeMicros(), Avro.TimestampMillis(), …, Avro.DurationLogical()   # values for `logical=`
Avro.EnumSchema(name, symbols; namespace="", default=Avro.nodefault, aliases=String[], doc=nothing, props=(;), limits=Limits()), Avro.FixedSchema(name, size; namespace="", logical=nothing, aliases=String[], props=(;), limits=Limits())
    # public constructors validate every §4.2 rule, reject `props` keys that collide with structural keys, deep-copy frozen children, and freeze
Avro.schema(T::Type; name=nothing, namespace=nothing, limits=Limits()) -> Schema   # Julia type → conventional schema (name policy §4.8; collisions/invalid names error)
Avro.avroname(::Type{T}) -> (name, namespace)                        # overridable naming hook for nested/parametric types
Avro.avrosymbol(::Type{E}, x::E) -> String                           # overridable enum symbol hook
Avro.schema(x; limits=Limits())                                      # value-level (identity-bearing generic values; ambiguous → ArgumentError); shares one budget with the type derivation it delegates to
Avro.schema(::Tables.Schema; name="Record", namespace="", names=Dict(), limits=Limits())   # table schema → record, optional column renames
Avro.schema(x::Avro.Table / Avro.Rows / Reader)                      # the effective schema (reader schema if given, else writer schema); Avro.writerschema(x) is the file's writer schema
Avro.ordinal(x::Avro.EnumValue / Avro.UnionValue) -> Int             # zero-based wire ordinal (indices in the value types are 1-based positions)
Avro.register!(store, schema; limits=Limits()) -> UInt64; Avro.lookup(store, fp; limits=Limits()) -> Schema   # SchemaCache charges its own table budget; custom stores own theirs
Avro.Row(record::Avro.Record; names=Avro.DEFAULT_ADMISSION)          # Tables row wrapper carrying admission provenance
Avro.Record(schema, values; limits=Limits()), Avro.Fixed(schema, bytes; limits=Limits()), Avro.EnumValue(schema, index; limits=Limits()), Avro.Map(pairs; limits=Limits())   # validated (lengths, ranges, key uniqueness), copying into owned storage under their own budget scope
Avro.UnionValue(index, x)                                            # non-copying wrapper retaining `x`; checks only index ≥ 1 (branch acceptance is checked at encode against the enclosing union)
Avro.schema(row::Avro.Row) -> Schema                                 # the wrapped record's schema
Avro.json(schema; pretty=false, limits=Limits()) / JSON.json(schema) (uses Limits())   # spec JSON text
Avro.canonical(schema; limits=Limits()) -> String                     # Parsing Canonical Form
Avro.fingerprint(schema; algorithm=:crc64avro, limits=Limits()) -> UInt64 | Vector{UInt8} (md5/sha256)
Avro.parsingequivalent(a, b; limits=Limits()) -> Bool
Avro.resolve(writer, reader; union_resolution=:spec, limits=Limits()) -> ResolvedSchema
Avro.juliatype(schema) -> Type                                        # allocates nothing beyond the type; no budget scope
Avro.fullname(named_schema) -> String
Base.:(==), Base.hash, Base.show on schemas
```

### 5.2 Datums

```julia
Avro.DatumReader(writer_schema, T=juliatype(effective reader); reader_schema=nothing, union_resolution=:spec, limits=Limits(), validate=:strict, names=Avro.DEFAULT_ADMISSION)
    reader(bytes) -> value;  reader(bytes, pos) -> (value, nextpos);  reader(io)         # shareable; per-call state and budget
Avro.DatumWriter(schema; limits=Limits())
    writer(x) -> Vector{UInt8} (owned);  writer(enc_or_io, x)                           # single-owner (reusable encoder)
Avro.encode(schema, x; limits) -> Vector{UInt8};  Avro.encode!(enc_or_io, schema, x; limits);  Avro.encode(x; limits)   # one-shot conveniences
Avro.decode(writer_schema, src, T=…; reader_schema=nothing, union_resolution=:spec, limits=Limits(), validate=:strict, names=…)
Avro.decode(writer_schema, bytes, pos::Int, T=…; kw...) -> (value, nextpos)
Avro.encodesingle(schema, x; limits); Avro.decodesingle(src, store; reader_schema=nothing, union_resolution=:spec, validate=:strict, limits=Limits(), names=…, T=…)
Avro.tojson(schema, x; pretty=false, limits=Limits()) -> String;  Avro.fromjson(schema, json, T=…; strict=true, unknown=:ignore, limits=Limits(), names=…)
Avro.compare(schema, a, b; limits=Limits()) -> Int;  Avro.comparebytes(schema, abytes, bbytes; limits=Limits(), validate=:strict) -> Int
Avro.UnionValue(i, x), Avro.EnumValue, Avro.Fixed, Avro.Record, Avro.Map, Avro.Decimal, Avro.WideDecimal, Avro.Duration,
Avro.Timestamp{P}, Avro.LocalTimestamp{P}, Avro.truncate(x, P), Avro.round(x, P)
```

### 5.3 Container files and tables

```julia
Avro.Table(src; reader_schema=nothing, union_resolution=:spec, ntasks=Threads.nthreads(), limits=Limits(), legacy=nothing, decimal_byteorder=:big,
           allow_invalid_names=false, allow_invalid_defaults=false, validate=:strict, mmap=true, names=Avro.DEFAULT_ADMISSION, select=nothing)   # `select=` = projection pushdown (§6)
        # Tables.jl columns table (record roots only); `Avro.schema(t)`, `length`, `Tables.partitions` (per block)
Avro.metadata(t_or_rows_or_reader) -> Avro.Map{Vector{UInt8}}   # owned copy of the header metadata (avro.* keys included)
Avro.codec(t_or_rows_or_reader) -> Symbol; Avro.sync(t_or_rows_or_reader) -> NTuple{16,UInt8}; Avro.writerschema(t_or_rows_or_reader) -> Schema
Avro.Rows(src; T=nothing, reader_schema=nothing, union_resolution, limits, legacy, decimal_byteorder, allow_invalid_names, allow_invalid_defaults, validate, mmap, names, select=nothing)
    # three modes (§6): generic record mode yields Avro.Row and is a Tables row source with partitions; typed mode (T) is a plain iterator of T (writable through its retained schema, not a Tables source); non-record mode is a plain iterator; `close`
Avro.Reader(src; limits, legacy, decimal_byteorder=:big, allow_invalid_names, allow_invalid_defaults, validate, mmap)   # block-level (do-block form available): header, `eachblock(r)` → (count, owned decompressed bytes), `eachdatum(r)`, `close`
Avro.Writer(io_or_path, schema; codec=:null, level=nothing, metadata=Dict(), sync=nothing, block_bytes=64*1024, atomic=true, fsync=false, allow_invalid_names=false, allow_invalid_defaults=false, limits=Limits())
    push!(w, datum); Base.write(w, datums); flush(w); close(w; abort=false)
Avro.write(dst, table; schema=nothing, codec=:null, level, metadata, block_bytes, name, namespace, atomic=true, fsync=false, allow_invalid_names=false, allow_invalid_defaults=false, limits=Limits()) -> dst
Avro.tobuffer(table; kw...) -> IOBuffer
Avro.codecs() -> available codec names
Avro.inspect(src; limits) -> diagnostic report (its own budget scope; bounded permissive parse: codec, block count/sizes, padding/legacy issues, root schema, invalid-name/default findings)
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

## 6. Tables.jl integration and projection pushdown

* `Avro.Table` is a column table: `Tables.columnaccess`, `Tables.columns`, `Tables.schema` — **always a
  stored `Tables.Schema{nothing,nothing}`** for file-derived schemas, so untrusted field names and
  element types never become type parameters — `Tables.partitions` (one `Avro.Table` per block,
  materialised lazily), and the complete read-only DataAPI metadata interface
  (`DataAPI.metadatasupport`, `DataAPI.metadatakeys`, `DataAPI.metadata(t, key; style)`) for file
  metadata (DataAPI is a direct dependency; tested like Arrow's implementation). Columns are plain
  `Vector`s. Zero-column and zero-row tables keep an authoritative row count (`length`).
* **Symbol admission (the trust boundary).** Julia interns `Symbol`s permanently. An
  `Avro.SymbolAdmission` object (a lock-protected sorted-runs string table — §4.4 map structure, never
  `Base.Dict` and never hashing — with `max_names`/`max_bytes`) decides
  which untrusted strings may be interned: Tables field names (`Tables.columnnames` must return
  `Symbol`s) **and** typed datum values decoded into `Symbol` on every typed path (`DatumReader`,
  `decode`, `decodesingle`, `fromjson`, `Rows`; §4.8 — `Avro.Table` has no `T` and admits only
  column names). `Avro.DEFAULT_ADMISSION` (1,000,000
  names / 64 MiB) is process-wide; callers may pass their own object via `names=` (per tenant, per job)
  or `names=:trusted` to bypass admission for trusted sources. Exceeding the budget raises `LimitError`;
  strings already admitted do not count twice. `Avro.Rows` admits field names **lazily**, only when
  `Tables.schema`/`Tables.columnnames` is actually requested, so generic datum iteration over untrusted
  files never interns anything. Tests exercise repeated files across operations with default,
  caller-owned, exhausted and `:trusted` admission, for binary and JSON paths.
* `Avro.Rows` has three modes. **Generic record mode** (`T=nothing`, record root): a row table
  yielding `Avro.Row` (`Tables.rowaccess`, `Tables.rows`, stored `Tables.schema`, `Base.IteratorSize`
  `SizeUnknown` for `IO`, `HasLength` when the block pre-scan is possible); `Tables.partitions(rows)`
  yields per-block `Avro.Table`s so `Avro.write(dst, Avro.Rows(src))` streams with bounded memory.
  **Typed mode** (`T` supplied): a plain iterator of `T` values with no Tables row interface (a `T`
  may be a scalar, a `Dict` or a custom StructUtils result), no partitions and no lazy name
  admission — `Symbol` values inside `T` still pass the operation's admission object. **Non-record
  mode**: a plain iterator of §4.6 values. `select=` applies to the generic record mode only.
* `Avro.write` accepts any Tables.jl source with a `Tables.schema` (row or column access) and honours
  `Tables.partitions`.
* **Projection pushdown in 2.0; `Tables.Scan` pushdown deferred to 2.1.** The Avro container is a
  row format without statistics, so the only pushdown that saves decoding work is **projection**
  (unselected fields are skipped under the active validation mode — strict walks, fast jumps). 2.0
  therefore provides `select=` on `Avro.Table` and `Avro.Rows` (a tuple of column names or `Symbol`s,
  in the caller's order; unknown names are an `ArgumentError`; `select=()` yields a zero-column table
  with the correct row count) and nothing else: the resulting table's **derived effective schema** is
  the record of the selected fields in the selected order with their names, types, defaults, aliases,
  `order`, docs and props unchanged (named types preserved), so `Avro.write(dst, projected)` round-trips
  it. Filters, `limit`/`offset`, renames, type overrides, residual conversion and schema provenance
  under arbitrary transformations — the `Tables.Scan` surface — are **deferred to 2.1** (decision 9),
  where they will be designed against a registered Tables release that contains `Scan` (design notes
  in decision 54). Gate for
  2.0: names, order, eltypes, `Tables.schema`, row count and values of `Avro.Table(src; select=cols)`
  equal `Tables.columntable(Avro.Table(src))[cols]` for a generated matrix of selections over every
  fixture (multi-block files, empty records, `select=()`, nested and nullable fields, directly and
  mutually recursive roots), in both
  validation modes, for `ntasks ∈ {1,2,8}`; malformed data inside projected-away fields is tested under
  each mode's documented policy; writing projected tables round-trips their derived schema.
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
  order and runtime scale; all timestamps decode as exact wrappers (`DateTime` through explicit conversion or a typed `T`);
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
| xz stream padding (all-zero, multiple of four) | read (measured; Java decodes `count` datums and never verifies payload exhaustion, so any suffix — even 1 byte — passes) | rejected (`LZMAError`, measured) | — | required of concatenation-capable decoders by the xz format; Avro.jl reads it |
| zstandard skippable and empty frames between members | read (measured) | read (measured) | — | valid zstandard input; Avro.jl reads them |

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
7. **Mutation / fuzz**: a fixed sampled corpus for required CI (200 fixtures/generated datums × 1,000
   seeded mutations each, the sample recorded) and the full corpus under `AVRO_FUZZ_ITERATIONS`; mutations are (bit flips, byte
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
   `:fast` differences must be exactly the documented ones (malformed content inside jumped sized
   blocks).
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
    without a `DomainError`; a non-UTF-8 metadata value round-trips through Julia and Java; `storagebytes ≥ Base.summarysize(x; exclude=Avro.Schema)` for
    generated values of every member of `E` (the representation-formula oracle); near-ceiling tables of
    empty `bytes`, `fixed`, wide decimals and nullable isbits columns are charged exactly; concatenated
    valid members decode for zstandard, xz and bzip2, zstandard skippable/empty frames and xz padding
    follow the §4.9 rules, and deflate rejects bytes after `BFINAL`; `Avro.Map` capacity and storage at
    n = 0, 1, 11, 16, 17, 23, 43, 1001, 1024 (odd sizes included) and for all-duplicate, reversed and
    long-common-prefix keys (construction peak equal to the `npairs` + `cld(npairs, 2)` formula, final
    storage equal to the `npairs`-capacity formula, lookups correct, last-wins); schema/plan graphs near the ceiling (shallow wide schema, large
    defaults/props, large resolving plan) fail with `LimitError`; the `storagebytes` oracle with
    `exclude=Avro.Schema` for shared and distinct schema identities; output-buffer growth reserved
    before allocation; `Rows` streams more than the ceiling with unretained outputs and fails with
    retained ones only through the caller's own memory, never the operation ceiling; the `__init__`
    layout probes equal the recorded constants (incl. the `Avro.Map` shell); `Avro.Map` construction under adversarial keys (long common prefixes, duplicates, reversed and
    random orders): no rebuild, no error, last-wins correct, memory equal to the count-based formula,
    compared bytes within the comparison rule; the admission table under one million incremental
    admissions (latency recorded) and at its capacity boundaries; duplicate-heavy maps in the storage
    oracle; parser duplicate-key detection on
    wide objects without any `Set`/`Dict` allocation (allocation hook); every schema operation incl. `Avro.schema(x)` honours
    `limits=` and nested operations share one budget; the combined writer/reader invariant cases; the wide mostly-null many-tiny-blocks streamed fixture;
    duplicate ordinary, `avro.schema` and `avro.codec` header keys (each a `DataError`); dense-ID
    creation gates for parser-, constructor- and type-derived graphs incl. reused frozen children;
    number-token fixtures (huge integers, mantissas, exponents) in every position; repaired-schema
    rejection by `canonical`/`fingerprint`/`register!`/`encodesingle`/`tojson`/`fromjson`; forced
    parallel schedules where higher blocks wait for commits; writing projected and zero-column tables;
    maps near the ceiling accepted identically for `ntasks ∈ {1,2,8}` and across repeated runs (no
    randomness in the guarded path); `parseschema(::IO)` on non-seekable over-limit streams and at both byte boundaries;
    strict UTF-8 and escape validation negatives for schema keys, names, props, defaults, datum strings,
    map keys and union labels; decoded-key duplicate detection across escape spellings; the comparison
    rule tripping on long-common-prefix keys and passing legitimate wide objects; dense empty/skippable
    codec members bounded by the value rule; caller-buffer mutation after `parseschema`; identity-bearing union branch recovery with several record, enum and fixed branches; the fast route
    never calling a user constructor (mutable and immutable structs); typed-shell storage oracle over
    generated layouts incl. zero-field mutable structs and arrays of them; the growth rule's
    allocation-hook gate on generic collections, streamed columns, the `Encoder` and owned buffers;
    pointer-incompatible strings and stepped views copied at exact boundaries; dense-ID tables under
    reverse-ordered fullnames, cyclic equality and near-`max_resolution_work` adversarial graphs;
    one-symbol admissions across every power-of-two boundary; lone-surrogate escapes
    rejected in string contexts and preserved in `doc`/props with verbatim re-emission and unchanged
    fingerprints.
11. **Concurrency**: multi-block files decoded with `ntasks ∈ {1,2,8}` produce identical tables **and
    identical acceptance** (files that exceed the ceiling fail at the same block index for every
    `ntasks`, under default and raised limits); forced schedules where higher blocks wait for commits
    complete with the sequential result; forced interruption (`InterruptException`) during
    decompression and during assembly while a worker holds `W` and chunk references: every task is
    joined, reservations are restored exactly once, no write reaches the caller's output after the
    return, and the original interruption propagates; the peak-RSS gate runs under the fixed method of §4.9; two
    blocks failing concurrently (a higher index failing first) surface the lowest block index
    deterministically across 100 scheduled repetitions, for content/content, budget/content and
    content/budget pairs; for **successful** operations the deterministic counters (decompressions,
    values walked, comparisons/moves) equal the sequential run's and `assembly_bytes ≤
    committed_bytes`; for **failing** operations the selected error is identical, no block is retried,
    and at most `inflight` higher blocks beyond the sequential run's work were decoded when the direct
    head failed (`inflight − 1` when a higher block failed), each within its block caps, both schedules
    forced (CPU time reported with the ≤ 1.5 × tolerance on successful runs, recorded on failures); measured peak RSS with `ntasks = 8` on 16 MiB highly compressible
    blocks decoded from a caller-owned byte buffer faulted before the baseline (in-flight high-water
    counter ≥ 2), and on a table whose final columns exceed half the ceiling, stays within the ceiling
    plus documented runtime overhead and the run completes (liveness); nullable
    `Int64`/`Float64`/`Date`/`UUID` columns near the ceiling are charged exactly (`Base.summarysize`
    oracle); GC-stress runs on the parallel path; writer flush/close/abort/poison ordering; caller-owned
    IO never closed.
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
| Projection `select=(:id,)` on the 4-column file (`validate=:fast`) | ≥ 2× faster than full decode | strict-mode projection reported |
| Package load time | — | ≤ 0.5 s |
| Time-to-first-table on a fresh session | — | ≤ 1.5 s |

---

## 11. Engineering deliverables

* **Package metadata**: `Project.toml` version `2.0.0-DEV`; deps `JSON` (1.7; used only for the `JSON.json(schema)` printing overload — parsing is Avro-owned, §4.2), `StructUtils` (2.8),
  `Tables` (1.13; no development pin — the Scan pin belongs to the 2.1 work), `DataAPI` (1),
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
  `Core.eval(Expr(:public, ...))` so Julia 1.10 still parses the module.
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
  null/deflate/zstandard/snappy, `Avro.Table` (with a `select=` projection), `Avro.Rows`,
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
| **0 — Foundation** | branch; legacy code removed; `Project.toml` 2.0.0-DEV + pinned deps (jar checksum, Python pins incl. cramjam, CodecZstd ≥ 0.8.7, Zstd_jll/XZ_jll); `test/Project.toml` incl. CodecBzip2/CodecXz; CI skeleton incl. informational coverage; vendored + generated fixtures with licence and generator script (every root kind, fastavro all codecs, real 1.x files, time/decimal/sort vectors normalised, cross-form arrays, high-window codec blocks with liblzma's reported threshold and the window-log-30 description, the million-empty-string block); Java harness; `errors.jl`, `limits.jl` (validated; fixed defaults; ceiling accounting; available-memory guard), `frozen.jl`, `values.jl` skeleton, `admission.jl`; benchmark harness with raw-logged 1.1.2/fastavro/Java baselines and the measurement protocol; Julia 1.10 load test; `public` gating; codec-cap feasibility (verified) through the library estimators (`Zstd_jll` direct, `XZ_jll` weak, symbol capability checks) with the one recorded cap meaning | `Pkg.test` skeleton green on 1.10 and 1.12; fixtures licensed and regenerable; baseline logs committed |
| **1 — Schema model** | Avro-owned JSON reader (pre-scan, WTF-8 strings, number grammar, contextual attributes), fullname algorithm before validation, parser/validator (all §3 rules incl. self-alias idempotence and union branch identity, `fixed.size` syntax, contextual logical attributes with `Int` precision/scale, §4.4 schema limits, repair options incl. enum defaults), defaults as frozen JSON, transitive freezing with freeze-time hashes, normalised structural `==`, printer, canonical form, fingerprints, the complete Julia-derived name policy with `avroname`/`avrosymbol` hooks and collision errors, `minsize`, `inspect`'s permissive diagnostic parse, schema-graph budgeting (§4.4 category (e)), incremental bounded `IO` buffering, strict UTF-8/escape validation in the pre-scan, Avro-owned decoded-key duplicate detection under the comparison rule, `limits=` on every schema operation incl. `schema(x)` with one shared budget, owned source spans, and the shared-object storage oracle | `schema-tests.txt` **100%** (incl. 023/024); every constraint and limit tested positive/negative; duplicate-key, depth, recursion, equality-on-cycles and transitive-immutability gates; repair options round-trip invalid legacy schemas incl. enum defaults; Java-accepted metadata/invalid-logical/ignored-namespace/self-alias cases accepted identically (except the deliberately rejected non-string ignored namespace, §4.2); `canonical`/`fingerprint` equal to Java for all fixture schemas; Aqua+JET clean |
| **2 — Binary core** | Decoder/Encoder with checked arithmetic, both validation modes, the shared work rule and every cumulative limit on encode and decode, generic dynamic plans with `PlanRef`, the enumerated value set `E` with `Avro.Map` (sorted permutation, no hashing) and the sorted-runs admission table and the representation storage formulas (`storagebytes`, init-measured constants incl. the map shell, oracle-asserted, typed shells included), the typed conversion boundary with constructor-free fast-route construction, the comparison rule, typed plans with `AvroStyle` eligibility and Symbol admission on every typed path, prepared `DatumReader`/`DatumWriter`, column builders in schema-independent containers, logical values with decode-side checks and `Int` scale, bounded JSON datum encoding in both directions with the common `max_json_depth`, single-object + `SchemaCache` ambiguity rules, value-level `Avro.schema(x)`, `ConversionError`, **the provisional latency measurement of datum and schema shapes (constants finalised in Phase 4a)** | spec-example tests; round-trip properties with independent assertions; 1.x round-trip cases ported; Java `fragtojson`/`jsontofrag` differential on deterministic datums + semantic comparison on collections; blocking-encoder fixtures; the recorded fuzz sample (200 × 1,000 mutations) clean in sandboxed batches under the split gate; acceptance-equivalence tests in both modes; limit/budget/work-rule/encode-work tests; provisional latency measurements ≤ 10 s on every supported Julia version recorded; allocation budgets on prepared objects; messageV1; numeric compile-cost gate with `E` warm-up over random widths/orders |
| **3 — Resolution and order** | `resolve` with both union policies, reader-directed output and the resolution work budget, resolving plans (memoised pairs), aliases, defaults (incl. invalid-default repair), enum defaults by symbol, decimal rule and all other logical pairings, `Avro.compare`/`comparebytes` (budgeted, exact consumption, canonical-encoding contract, cross-form arrays) | resolution matrix with direct expectations (both policies, every output direction, every logical pairing, work-limit case) plus Java/fastavro where observable; sort-order vectors from both Java comparators incl. NaN/−0.0/ignore/maps/signed-bytes deviation/logical types/non-minimal decimal/mixed-case UUID/cross-form arrays; `compare`/`comparebytes` agreement property |
| **4a — Strict containers and codecs** | final latency calibration (container shapes added; all work and comparison constants fixed and recorded), Reader/Writer/`Avro.write` with strict validation (lower bounds, framing-credited work rule evaluated after decompression with no compressed-size pre-check, codec decoder caps with liblzma/zstd thresholds, EOS/consumption contract), streamed `mmap=false`, atomic path contract, full writer failure contract, option/header/schema validation against reader limits, **writer-side codec workspace charging and emitted-frame decoder-requirement checks (zstd `windowLog` selection + per-frame `fromFrame` verification)**, work-rule-driven block flushing, reserved-metadata rejection, codecs (+ extensions), legacy mode, `decimal_byteorder`, repair options, `Avro.inspect`, any-root `eachdatum`/`eachblock` (owned decompressed bytes), explicit-schema requirement for schema-less sources | Apache corpus reads; Java **and** fastavro read Julia files — checked after each codec lands; Julia reads Java and fastavro files for every codec and every root kind; the million-empty-string block reads under defaults; 1.x fixtures read under legacy options and rejected under strict; truncation/corruption/bomb/high-window/EOS/suffix/negative-header/work-rule/empty-file/zero-datum/metadata tests; **container-only writer/reader invariant tests** (every zero-size and all-null-field root, near-ceiling multi-block files, maximal metadata and schema, and their combinations, through `Reader`) pass under the fixed defaults — the complete consumer/source-mode/preflight acceptance incl. table shells and streamed chunks is Phase 4b's gate; writer failure-injection and atomic-replacement tests on every OS; `mmap=false` streams a file larger than the ceiling; the non-UTF-8 metadata fixture (Julia/Java accept, fastavro rejects); concatenated-member blocks accepted for zstandard/xz/bzip2 with per-member caps, zstandard skippable frames and xz stream padding accepted per §4.9, deflate `BFINAL` suffix rejected, oracle behaviour recorded |
| **4b — Tables basics and ownership** | `Avro.Table` (sequential), `Avro.Rows` in its three modes (generic record mode yielding `Avro.Row` with lazy admission; typed mode; non-record mode), partitions, DataAPI metadata interface, stored schemas, exact column-storage charging (isbits-union tag bytes) with chunked materialisation on streamed sources, the complete writer/reader consumer and source-mode gate (§4.4), symbol admission objects (names and typed values, binary and JSON), zero-column/zero-row behaviour, close semantics | `Table == columntable(Rows)` property; ownership/close tests (caller IO untouched); Tables and DataAPI interface tests; symbol-admission tests incl. caller-owned objects and repeated files |
| **4c — Parallel decode** | block pre-scan with exact final-column preallocation (exact Julia storage incl. tag bytes), the lowest uncommitted block under the sequential rule, higher blocks admitted in order into headroom with full worst-case reservations (no eviction, no retry), fixed charged worker pool, streaming ordered assembly with ordered cumulative commits, lowest-index error selection across failure kinds | identical results **and acceptance** for `ntasks ∈ {1,2,8}` under default and raised limits; deterministic failure selection over forced schedules for every failure-kind pairing; GC-stress; peak RSS under the fixed §4.9 method (`peak − baseline ≤ effective ceiling + 128 MiB`, caller-owned faulted byte buffer, in-flight high-water ≥ 2) on highly compressible 16 MiB blocks with `ntasks = 8` and on half-ceiling tables, with liveness; deterministic counters equal to sequential on successful operations and `assembly_bytes ≤ committed_bytes`; on failures the same error, no retry and at most `inflight` (head failure) or `inflight − 1` (higher-block failure) speculative higher blocks within their caps, both schedules forced (CPU time informational, ≤ 1.5 × tolerance on successful runs) |
| **4d — Projection and performance** | `select=` projection on `Avro.Table`/`Avro.Rows` with skipping under both validation modes, derived effective schemas, performance work | projection equivalence matrix (selection × validation × `ntasks`) over every fixture incl. malformed data in projected-away fields under each mode's policy; writing projected tables round-trips their schema; §10.2 ratio gates on the named host under the measurement protocol |
| **5 — Release engineering** | shims, docs (incl. limits guidance and the oracle-readability exceptions), examples, changelog, precompile workload, trim smoke, benchmarks doc, README, CI matrix live, coverage report, cross-package smoke | docs build without warnings; load-time budget; full local matrix green on 1.10/1.11/1.12 (+1.13-rc if installed); Aqua/JET; interop job green locally |

Readiness levels (this task stops at **PR-ready**; nothing is pushed):

* **Review-ready**: clean exact local head; scoped diff; local supported-version tests, docs, licensing,
  and interop artifacts complete.
* **PR-ready**: review-ready plus a reproducible branch and exact dependency pins (jar checksum,
  Python pins, test Manifest) and the status record (§15) up to date.
* **Merge-ready**: hosted exact-head CI green and review issues resolved.
* **RC-ready**: **a clean environment instantiated from the exact RC
  source archive re-runs the complete matrix** (supported Julia versions, all codecs, interop, fuzz,
  resource gates, docs, cross-package) and the release tag tree must equal that tested tree; lower
  compat bounds resolve; registered dependencies exist; reverse dependencies/PkgEval checked.
* **Release-ready**: exact version/tag state approved; registration and docs deployment are release
  actions, not evidence of readiness.

---

## 13. Correctness, security, performance, allocation, streaming, concurrency goals (summary)

* Correctness: every spec rule in §3 has a test; all in-scope vendored Apache fixtures pass (`schema-tests.txt` 100%);
  Java and fastavro read every file we write from oracle-accepted schemas (subject to the §7 recorded
  exceptions); we read every file they write
  (all six codecs, every root kind); oracle-verified resolution (both policies, reader-directed output)
  and sort order (incl. logical types, canonical-encoding contract).
* Security: checked arithmetic, no `@inbounds` without a proven bound, no unchecked or unreserved
  allocation (exact final columns are sized from validated counts after reservation), one fixed-default per-operation ceiling covering final-column capacity, committed payload and
  the lowest block's actual-size reservations and higher blocks' worst-case headroom reservations (no
  eviction, exact column storage incl. isbits-union tag bytes, streaming assembly into preallocated
  columns, a liveness argument, acceptance identical to sequential, deterministic work identical on successful operations and bounded by the in-flight higher blocks on failures), a best-effort available-memory
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
* Concurrency: §4.14 contract; no global mutable state beyond the documented default admission table, the write-once storage
  constants measured in `__init__` and the guard's pending-reservation counter;
  structured task lifecycle with deterministic failure selection across failure kinds.

---

## 14. Decisions, open risks, and review log

Decisions (each with rationale; reviewers may challenge any):

1. `missing` is the Julia value of `null`; `nothing` encodes as null.
2. Generic enums are `Avro.EnumValue` (equality by fullname + symbol; no interning); typed targets may
   choose `Base.Enum`/`Symbol` (through admission)/`String`.
3. Generic records are always `Avro.Record`; generic fixed values are `Avro.Fixed`; typed `T` is opt-in.
4. All timestamp logical types decode to exact `Avro.Timestamp{P}`/`Avro.LocalTimestamp{P}` wrappers;
      `DateTime` conversion is explicit, range-checked (`ConversionError`) and documented (`DateTime(x)`
   or a typed `T` with `DateTime` fields; no `instants=` option); a naive `DateTime` derives
   `local-timestamp-millis`.
5. Decimals carry runtime `Int` scale (`Avro.Decimal` Int128 ≤ 38 digits, `Avro.WideDecimal` beyond);
   digit counts are validated on both encode and decode (stricter than Java on decode); encode requires
   exact scale equality.
6. Codec dependency split: `deflate`/`snappy`/`zstandard` hard, `bzip2`/`xz` extensions;
   `max_codec_memory` has one meaning — the cap on the complete decoder memory requirement of one codec
   member as reported by the library (liblzma `memlimit`; `ZSTD_estimateDStreamSize_fromFrame`) —
   enforced on every member read and written (decision 28); it is not a process-memory cap; the xz
   workspace reservation is the cap itself because liblzma reports nothing before allocating.
7. Strict container validation is the default; `legacy=:avrojl1` covers only the three unambiguous 1.x
   tolerances; decimal byte-order reinterpretation is explicit-only.
8. `Avro.write(dst, table)` is the container writer; datum writing is `encode`/`encode!`/`DatumWriter`;
   schema-free `encode(x)` uses the documented conventional schema and is not a round-trip mechanism.
9. 2.0 ships projection pushdown (`select=`) only; `Tables.Scan` pushdown (filters, limit/offset,
    renames, overrides, residuals) is deferred to 2.1 against a registered Tables release that contains
    `Scan`, with no dormant or runtime-gated Scan code in 2.0 (§6).
10. Default block size 64 KiB, additionally bounded by the block work rule; positive-count array/map
    blocks written; both forms read.
11. RPC, big-decimal, schema inference, append, writer-side parallel compression, and borrowed byte views
    are deferred to 2.x with their requirements recorded (§3, §4.9).
12. **Limits are fixed, portable defaults** (256 MiB ceiling), enforced identically by writers and
    readers so that default writer output is default-readable by construction whenever both effective
    ceilings admit the preflighted peak; the available-memory guard is a separate low-memory safety
    guarantee; raising a limit is an explicit, documented, both-sides decision; work constants are
    provisional until the Phase 4a latency gate finalises them (§4.4).
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
    generic maps are `Avro.Map` (package-owned, sorted, deterministic capacity, no hashing), not `Base.Dict`;
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
22. Parallel decoding only uses headroom: the lowest uncommitted block follows the sequential rule
    exactly, higher blocks are admitted in block order with full worst-case reservations, nothing is
    evicted, retried or restarted, every block is decoded once, assembly is ordered, and the lowest
    failing block index wins regardless of failure kind; acceptance is identical to sequential decoding
    by construction, deterministic work is identical on successful operations (assembly copies bounded
    by `committed_bytes`) and bounded by at most `inflight` (head failure) or `inflight − 1` (higher-block failure) speculative higher blocks on failures.
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
30. Memory is charged in five disjoint categories (§4.4): storage shells at their exact Julia size
    (element size plus the isbits-union tag byte plus the array header), referenced payload by
    representation-specific formulas asserted `≥ Base.summarysize(x; exclude=Avro.Schema)` for every
    member of `E`, input/codec/output buffers, internal tables incl. worker state, and schema/plan
    graphs; payload ownership transfers at commit without re-charging.
31. Projection is the only pushdown in 2.0; filtered scans (global filter pass before exact
    preallocation in the parallel path) are a 2.1 design note.
32. Non-UTF-8 user metadata is written as the spec allows and recorded as a fastavro readability
    exception; `Zstd_jll` is a direct dependency and `XZ_jll` a weak one so the codec estimators are
    reached through pinned, symbol-checked `ccall`s rather than package internals.
33. Multi-member codec payloads (zstandard frames, xz streams, bzip2 streams) are decoded member by
    member to exact exhaustion with per-member caps; deflate bytes after `BFINAL` are rejected (Java and
    fastavro ignore them; no writer emits them); truncated members and garbage suffixes are errors.
34. Parallel decoding uses a worker pool of at most `ntasks − 1` workers (the direct lowest block is
    the caller's task), bounded by `Threads.nthreads() − 1`, `inflight` and the headroom, whose state is
    charged; work is gated in deterministic units (decompressions, values walked, comparisons/moves —
    equal to sequential on successful operations) plus `assembly_bytes ≤ committed_bytes`, and CPU time
    is informational with a predeclared tolerance.
35. Nothing in the guarded path performs a hash-table lookup of untrusted keys: `Avro.Map` is a sorted-permutation map built by
    a deterministic merge sort, and the symbol-admission table is a sorted-runs structure; memory and
    work are functions of counts and key bytes alone (charged under the comparison rule), so no charge
    depends on `Base.Dict`, on seeds or on collisions, and acceptance is fully deterministic.
36. Schema and plan graphs are charged (category (e)) under the budget of the operation that builds
    them; storage constants are measured at `__init__`; the storage oracle excludes `Avro.Schema`
    references, which are charged once with the graph.
37. Zstandard skippable frames and xz stream padding are accepted as the codec formats require; the
    measured oracle behaviour for concatenated members, padding and deflate suffixes is recorded in
    §8.4 and does not affect the readability promise (Avro.jl writes single members without padding).
38. The typed fast route allocates only approved, exactly charged representations; every other typed
    target is converted after ownership transfer in caller space (the typed conversion boundary, §4.8).
39. JSON.jl's duplicate-key tracker is never enabled; Avro detects duplicates by sorting decoded keys
    under the comparison rule, and no parser, frozen-object or plan structure uses `Base.Dict`/`Set`;
    every schema operation (incl. `schema(x)`) takes `limits=` and nested operations share one budget.
40. The writer preflights the reader's complete peak (header representation, schema graph, a generic
    read plan, block table, one block's buffers, committed output) under its own budget, so the
    identical-limits invariant covers category (e).
41. JSON sources are validated as strict UTF-8 with valid escape syntax and number grammar in the
    pre-scan, and `IO` sources are buffered incrementally under `max_schema_bytes`/`max_datum_bytes`
    before parsing (decision 51: the reader is Avro-owned).
42. The typed fast route builds `NamedTuple`s directly and structs with `Expr(:new)` generated for the
    compile-time type, never running user constructors; constructor validation belongs to the semantic
    route; typed shells are measured per `T` from a probe instance.
43. A comparison rule (`max_compare_bytes_per_byte`) bounds key-comparison work alongside the value
    work rule, and codec members count as values.
44. JSON strings are decoded uniformly, preserving lone surrogate escapes as surrogate byte sequences
    that the printer re-escapes; the Unicode-scalar requirement applies contextually after the fullname
    algorithm (names and namespaces actually used, enum symbols, default strings, datum strings, map
    keys, union labels), while aliases, `doc`, custom property names/values and nested metadata keys
    are preserved verbatim, matching the oracles.
45. No charged buffer is grown with `push!`/`resize!`/`sizehint!`; growth is by reserved exact-capacity
    replacement through package-owned builders.
46. Category-(e) tables are keyed by dense node IDs stored in the nodes (`id`/`graph` filled by
    `freeze!` on every creation path; node-indexed arrays; sorted per-node partner vectors for pairs)
    with every inspected entry and move charged to `max_resolution_work`; the admission table's merges
    are deamortised to a fixed step per admission.
47. Guarded JSON strings are decoded by an Avro-owned decoder into WTF-8 (pairs combined, unmatched
    surrogates preserved as 3-byte sequences, re-escaped on output); aliases and repaired names match by
    exact bytes, so alias-based repair of invalid legacy names works under `allow_invalid_names=true`.
48. The Writer/Reader invariant names its consumers (`Reader`/`Rows`/`Table` from bytes, mapped path,
    `mmap=false`, non-seekable `IO`) and qualifications (identical `Limits`, codec availability,
    admission state); streamed `Table` materialisation is chunked with the exact deterministic peak of
    §4.4 (chunk shells incl. `nblocks × ncolumns` headers, chunk and final reference slots, referenced
    payload counted once, block table, assembly buffers); a `Writer` rejects repaired schemas unless
    the matching option is passed.
49. `Avro.write` schema precedence is explicit `schema=`, then the effective retained schema of an Avro
    source, then the `Tables.schema` derivation; `Avro.schema(src)` is the effective schema and
    `Avro.writerschema(src)` the file's writer schema.
50. `EnumValue`/`UnionValue` indices are 1-based Julia positions; `Avro.ordinal` gives the zero-based
    wire value.
51. Avro owns its JSON reader (pre-scan with number grammar, WTF-8 strings, recursive descent, raw
    number tokens for metadata); JSON.jl is a dependency only for the `JSON.json(schema)` printing
    overload.
52. Schemas with repaired invalid names are binary/OCF-only: `canonical`, `fingerprint`,
    `parsingequivalent`, `register!`, `encodesingle`, `tojson` and `fromjson` reject them.
53. In parallel decoding only the lowest uncommitted block can fail the budget; higher blocks wait for
    commits and are never evicted, so no cancellation machinery exists inside datums.
54. (2.1 design note) Scan binding and filters will be performed by Avro-owned, allocation-aware code
    under a `max_scan_work` counter with a closed set of pushed predicate forms and Tables-valid renames
    that clear provenance when a name cannot be an Avro name; 2.0's projected tables carry a derived
    effective schema.
55. Required floats are parsed by separate correctly rounded linear-time `Float32`/`Float64` parsers
    with no token cap; attributes are grammar only in their defined context and metadata elsewhere.
56. Acceptance never depends on `ntasks` because the lowest uncommitted block is physically the
    `ntasks = 1` path (it writes directly into its final-column slice) and workers plus higher blocks
    exist only in headroom with complete worst-case reservations; assembly copies are bounded
    separately by `assembly_bytes ≤ committed_bytes`; work equality holds for successful operations and
    failures are bounded by the in-flight higher blocks.
57. The Writer/Reader invariant is scoped to generic default consumption and made portable through
    conservative maxima over supported Julia versions and the pinned codec libraries, with cross-read
    gates.
58. Identity-bearing values select union branches by their carried named-schema identity before any
    representation-type rule; empty enums are valid schemas with no datum; a schema object's `type`
    is restricted to primitive names and the six complex-type keywords.

Intentionally unresolved risks:

* The `Avro.Record` generic row boxes isbits fields; the typed path and `Avro.Table` are the fast paths.
  If `Rows` throughput for generic rows proves inadequate, an unboxed layout is a 2.x addition.
* The per-cell dynamic dispatch in the column path (no tuple unrolling) is accepted for 2.0; if the
  §10.2 ratio gates fail because of it, a per-leaf-type batched fast path is the pre-agreed fallback.

* Worker-task stack depth vs `max_depth=1024`: 500k frames were measured on macOS; Linux/Windows are
  measured in Phase 2 and the default lowered if needed.
* The fixed 256 MiB default ceiling will be too small for many large-table users and must be raised
  explicitly on both sides; the error message and manual make this a one-line change. The ceiling is an
  estimate of package-owned memory, not a hard OS limit; `Sys.free_memory()` is host-wide on Julia ≤
  1.12, so the guard is weaker inside cgroups than `Sys.total_memory()` alone suggests.
* Parallelism is limited to the headroom above the sequential peak reserve, so default limits allow
  little of it; the design trades throughput for identical acceptance and bounded memory, and the
  measured parallel speedups are reported with the explicit limits used.
* Projection is the only pushdown in 2.0 (the Avro format gains little from filter pushdown; the
  `Tables.Scan` API convenience is a 2.1 item).
* The codec estimators are library-reported numbers for the pinned `Zstd_jll`/`XZ_jll` versions; a
  future library release may change them, which the symbol checks cannot detect — the threshold tests
  pin the behaviour and fail loudly on a change.
* The provisional work constants are finalised only after the Phase 4a latency gate; the defaults in
  this plan are the starting point, not the commitment.

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
* **Round 11** (`reviews/codex-review-11.md`: 9 new findings — 6 majors, 3 minors — plus follow-ups;
  `VERDICT: REVISE`). All adopted; dispositions in `reviews/response-11.md`. Changes: the `Avro.Map`
  shell is measured at `__init__` and actual capacities are charged, with `npairs`/`nunique`/capacity
  terms and a 64-probe cap with re-seeded rebuilds (§4.4, §4.6, decision 35); JSON.jl's duplicate-key
  tracker is never enabled — Avro sorts key spans — and no parser/frozen/plan structure uses
  `Base.Dict`/`Set` (§4.2, §4.4, §4.11, decision 39); the typed conversion boundary (§4.1, §4.4, §4.8,
  decision 38); `limits=` on every schema operation with one shared budget (§4.4, §5.1); the writer
  preflights the reader's complete peak incl. category (e) with combined tests (§4.4, §12, decision
  40); stale summaries (raw oracle, four categories, per-block wording, "frame") corrected; the
  write-once layout constants recorded as the second global-state exception (§4.1); measured oracle rows
  for xz padding and zstandard skippable/empty frames (§8.4).
* **Round 12** (`reviews/codex-review-12.md`: 12 new findings — 7 majors, 5 minors — plus follow-ups;
  `VERDICT: REVISE`). All adopted; dispositions in `reviews/response-12.md`. Changes: `Avro.Map`
  rebuilds replaced by a fixed 64-probe limit with a sorted overflow index charged at actual capacity
  (acceptance independent of seeds; no `LimitError`) (§4.4, §4.6, decision 35); incremental bounded
  `IO` buffering and strict UTF-8/escape validation in the pre-scan (§4.2, decision 41); decoded-key
  duplicate equality and the comparison rule with `max_compare_bytes_per_byte` (§4.2, §4.4, decision
  43); `Avro.schema(x; limits)` (§5.1); constructor-free fast-route construction (§4.8, decision 42);
  codec members counted as values (§4.4); owned source spans (§4.2); typed-shell charges and oracle
  (§4.4); decision 30 and the §13 global-state line corrected; bounded visited-pair table for schema
  `==` (§4.2); the header metadata map is an `Avro.Map` (§4.9).
* **Round 13** (`reviews/codex-review-13.md`: 11 new findings — 6 majors, 5 minors — plus follow-ups;
  `VERDICT: REVISE`). All adopted; dispositions in `reviews/response-13.md`. Changes: hashing removed
  from the guarded path — `Avro.Map` is a sorted-permutation map and the admission table a sorted-runs
  structure, both deterministic in memory and work (§4.4, §4.6, §6, decision 35); the comparison rule
  joins the writer's preflight and the Phase 2 constants (§4.4); constructor-free construction via
  `Expr(:new)` for mutable and immutable structs, typed shells measured per `T` (§4.4, §4.8, decision
  42); contextual surrogate policy with preserved `doc`/props and non-contiguous source copies (§4.2,
  decision 44); skippable-frame charge wording, budget-scope list, "sorted decoded-key" wording,
  in-scope fixture claim, and the §9 map gates corrected.
* **Round 14** (`reviews/codex-review-14.md`: 6 new findings — all majors — plus follow-ups; `VERDICT:
  REVISE`). All adopted; dispositions in `reviews/response-14.md`. Changes: `Avro.Map` construction
  peak over all `npairs` candidates with `cld(npairs, 2)` scratch and in-place compaction (§4.4);
  uniform surrogate-preserving JSON string decoding with contextual validation after the fullname
  algorithm and verbatim aliases/metadata (§4.2, decision 44); pointer-incompatible strings and
  non-contiguous vectors copied before the reservation boundary (§4.2); dense node IDs for every
  category-(e) table with charged scans (§4.2, §4.4, §4.5, §4.7, decision 46); deamortised admission
  merges with a fixed per-admission step and transactional staging (§4.4); the growth rule — reserved
  exact-capacity replacement instead of `push!`/`resize!`/`sizehint!` — for every charged buffer
  (§4.3, §4.4, §4.9, decision 45); `Int32` bound check, probe-shell reservation, `Avro.Table` typed-path
  clarification, `==` limits source, stale wording and decision order fixed.
* **Round 15** (`reviews/codex-review-15.md`, exhaustive pass: 5 majors and 27 minor/nit follow-ups;
  `VERDICT: REVISE`). All adopted; dispositions in `reviews/response-15.md`. Changes: Avro-owned WTF-8
  string decoding with byte-exact aliases and alias-based repair (§4.2, decision 47); node-stored dense
  IDs and `GraphInfo` on every creation path with sorted partner vectors charged per inspected entry
  (§4.2, §4.4, decision 46); the invariant's named consumers and qualifications, chunked streamed
  materialisation, repaired-schema rejection at the `Writer` (§4.4, §4.9, decision 48); `Avro.write`
  schema precedence with `Avro.schema`/`Avro.writerschema` (§4.9, §5.1, decision 49); 1-based indices
  with `Avro.ordinal` (§4.6, decision 50); and the 27 follow-ups (map capacity retention, probe
  reservation bound, string copies in category (c), admission calibration, comparison-work definition,
  `minsize` for required cycles and varint index length, allocation-wrapper hook definition, logical
  release wording, snappy cancellation granularity, duplicate-key position, immutability contract
  scope, do-block forms and finalizer fallback, `decimal_byteorder` on `Reader`, `SchemaCache` API,
  invalid surrogate defaults, code-unit-exact metadata, ambiguous field aliases, duplicate header
  keys, standalone `error` schemas, branch-recovery fixtures, default equality semantics, ignored
  namespace type, bounded CI fuzzing, worst-density wording, `show` scratch, RPC checklist, log order).

* **Round 16** (`reviews/codex-review-16.md`: 1 blocker, 7 majors, 5 minors, 10 follow-ups; `VERDICT:
  REVISE`). All adopted; dispositions in `reviews/response-16.md`. Changes: Avro-owned JSON reader with
  number grammar, checked integer parsing, bounded floats and raw metadata number tokens — JSON.jl no
  longer parses anything (§4.2, §4.11, §4.15, §11, decision 51); consumers narrowed to `Reader`/`Rows`
  for all roots and `Table` for records, exact streamed chunk-shell preflight, complete gate moved to
  Phase 4b (§4.4, §12); denied non-lowest blocks self-evict and requeue, cancellable permit waits and
  in-datum eviction checks (§4.9, decision 53); Avro-owned Scan filter evaluator, residual after
  ownership transfer, derived effective schema with provenance clearing (§6, decision 54); repaired
  schemas rejected by PCF-dependent and JSON operations (§4.2, decision 52); child-graph deep copies,
  repair options on `write`/`tobuffer`, invalid-default and `iserror` equality, attribute type table,
  legacy decimal framing limits, allocation exclusion list, snappy length query, `Writer` do-block and
  finalizer abort, `npairs` capacity reconciliation, owned metadata spans, `decimal_byteorder` on
  `Reader`, `show` limits, duplicate header fixtures, stale growth wording, fuzz sample wording,
  dense-ID gates, complete-work counters, non-record `Rows` copies.
* **Round 17** (`reviews/codex-review-17.md`: 8 majors, 5 minors, 1 nit; `VERDICT: REVISE`). All
  adopted; dispositions in `reviews/response-17.md`. Changes: contextual attribute grammar (§4.2,
  decision 55); correctly rounded `Float32`/`Float64` parsers with no token cap and the Java-verified
  double-rounding vector (§4.2); an Avro-owned Scan resolver under `max_scan_nodes` (§4.4, §6, decision
  54); the invariant scoped to generic default consumption with conservative cross-version maxima and
  cross-read gates (§4.4, decision 57); sequential fallback when parallel-only memory blocks progress
  and assembly excluded from the 2 × ratio with its own bound (§4.9, §9, §12, decision 56); Phase 2/4a
  calibration split (§4.4); public constructor `limits=`, `DatumWriter` precharge, single-object scope
  entries (§4.4); `npairs` capacity everywhere, exact streamed peak in decision 48, payload counted
  once; raw-number lexical equality and allocation-free pre-scan (§4.2); `Avro.write` signature,
  `Table` typed-path removal, rename validation and widening transformation rules (§5, §4.5, §6);
  admission affordability conditional on the Phase 2 gate (§4.4); stale Phase 1/decision 41/decision 54
  wording and the review-log order fixed.

* **Round 18** (`reviews/codex-review-18.md`: 9 majors, 15 minors, 1 nit; `VERDICT: REVISE`). All
  adopted, two of them by **simplification**: parallel decoding now only uses headroom (the lowest
  uncommitted block follows the sequential rule; higher blocks are admitted in order with full
  worst-case reservations; no eviction, retry, fallback or in-datum cancellation — §4.9, decisions 22,
  34, 53, 56), and `Tables.Scan` pushdown is deferred to 2.1 while 2.0 ships `select=` projection with
  derived schemas (§6, §5.3, §12, decisions 9, 31, 54). Also: separate schema-object and field grammar
  tables (§4.2); the recursive default rule (§4.2); `Avro.Row` carries admission provenance while bare
  `Avro.Record` never interns (§4.6); effective-ceiling qualification of the invariant (§4.4, decision
  12); constants finalised after Phase 4a (§4.4); and the fifteen minors (map capacity wording,
  path-independent block output charge and `committed_bytes`, budget channels for `inspect`/stores,
  comparison denominators, saturating fixed-decimal bound, graph copy memo, `nothing` on nullable
  unions, unknown record members ignored with an opt-in, map-key conversion, float differential vectors,
  constructor props collisions and lexical-span equality, `SchemaCache` scope and internal `plan`,
  explicit `Symbol(::EnumValue)` interning, standalone `error` semantics, summary wording).
* **Round 19** (`reviews/codex-review-19.md`: 4 majors, 15 minors, 1 nit; `VERDICT: REVISE`). All
  adopted; dispositions in `reviews/response-19.md`. Changes: schema-object `type` restricted to
  primitive names and the complex-type keywords (§4.2, decision 58); the lowest parallel block writes
  directly into its final-column slice, `W` made complete (chunk shells, scratch, overlap, state) and
  workers created only from headroom (§4.9, decision 56); work equality limited to successful
  operations with a bounded failure statement (§4.9); identity-first union branch recovery (§4.6,
  decision 58); and the minors: signatures for `register!`/`lookup`/`fromjson(unknown=)`/`Rows(select=)`
  and the public value constructors, projection rules for duplicates, typed `T` and recursive roots,
  `Avro.Row` in the storage oracle, map wording, Scan pin/bootstrap/precompile/offset-window text
  removed, `FixedSchema.doc` removed, empty enums, missing `avro.schema`, strict `eachblock` walking,
  decimal matching wording, GMP exceptions and `WideDecimal` reservation, StructUtils default errors and
  the TimeZones contract, fullname-first alias matching, pending stage-1 failures, the Phase 1
  ignored-namespace qualification, the certified portability matrix, and the review-log order.

* **Round 20** (`reviews/codex-review-20.md`: 4 majors, 12 minors, 1 nit; `VERDICT: REVISE`). All
  adopted; dispositions in `reviews/response-20.md`. Changes: the writer tracks the exact reader-side
  output estimate per pending block and enforces `max_block_output_bytes`/`max_block_count`, with the
  four-million-empty-strings fixture (§4.3, §4.4, §4.9); `Avro.Rows` has three explicit modes (§4.9,
  §6, §12); the available-memory guard reads cgroup remaining memory and live reservations and is
  documented as best-effort (§4.4); counter equality limited to successful operations with a separate
  failure bound in every gate and summary (§4.9, §9, §12, §13, decisions 22/34); and the minors
  (`SchemaStore`/`fromjson`/constructor signatures and scopes, `UnionValue` validation scope,
  `Avro.Time{P}` removed, schema-free identity-bearing collections, remaining Scan artifacts removed,
  direct-lowest-block prose, `ntasks`/worker semantics, the default `W` statement, the narrowed
  constructor claim, the narrowed no-hashing claim, the successfully-closed-writer qualification, the
  Phase 4a risk wording, decision order).

* **Round 21** (`reviews/codex-review-21.md`: 3 majors, 17 minors, 2 nits; `VERDICT: REVISE`). All
  adopted; dispositions in `reviews/response-21.md`. Changes: the `instants=` option is removed —
  `DateTime` is reached by explicit conversion or a typed `T`, which is always authoritative (§4.6,
  §5, §7, decision 4); the failure-work bound is `inflight` for head failures and `inflight − 1` for
  higher-block failures with both schedules forced (§4.9, §9, §12, decision 22); and the minors (guard
  wording, pending-reservation counter as the third global-state exception with checked arithmetic and
  `finally` restoration, `Rows` synopsis by mode and `Avro.schema(::Row)`, direct-head role and
  `max_inflight_blocks` semantics, `committed_payload`/`committed_bytes` definitions, one
  consumer-independent `max_block_output_bytes` formula, `Avro.Row` oracle exclusions, the Phase 4a `W`
  measurement and success-only CPU tolerance, comparison multiplier from comparisons and moves,
  security wording, the public surface for `Limits`/`SchemaCache`/errors/schema constructors, structural
  `props` collisions, non-copying `UnionValue`, `Avro.schema(ZonedDateTime)` and float overflow rules,
  `avro.codec` UTF-8 and compressed-size checks, copied `AbstractVector{UInt8}` metadata, dense-ID
  wording, recursive-root projection gates, PR-ready pins, `ntasks = 8` wording, Appendix B fixes).

* **Round 22** (`reviews/codex-review-22.md`: 1 major, 10 minors, 1 nit; `VERDICT: REVISE`). All
  adopted; dispositions in `reviews/response-22.md`. Changes: the head's admission-wave barrier (§4.9);
  one `reader_block_peak` function for the writer preflight, sequential decoding and `W`; `live_base`
  in the admission equation and table shells in the oracle; the RSS parent acknowledgement and forced
  interruption gates; `W` measured after Phase 4b; the constructor contract (acyclic graphs plus a
  builder form for recursion, `DecimalLogical` and the other logical annotation objects,
  `Avro.nodefault`, per-constructor structural collisions, complete signatures); typed `ZonedDateTime`,
  narrow integer, `Float16` and `Char` conversion rules; the `UnresolvableBranch` deferred-error gate;
  parser completeness (null-namespace primitive reservation, RFC 8259 negatives); the metadata default
  expression; budgeted cache equality; fast-validation wording; the remaining 2.1 Scan residue and the
  Appendix B `row` reference.

* **Round 23** (`reviews/codex-review-23.md`: no blocker or major; 6 minors, 5 nits and follow-ups;
  **`VERDICT: AGREE`**). Claude's agreement is recorded in `reviews/response-23.md`. v24 applies the
  follow-ups within the agreed contracts: Phase 4a/4b gate wording, the `live_base` definition, the
  constructor `input_bytes` denominator, `logical=` property collisions, the `SchemaCache` sorted
  fingerprint index, `NamedTuple(row)` as a caller-space conversion, "decoded at most once" wording,
  `eachblock` buffer-ownership transfer, `fixed_N` naming, the Appendix B do-block, container accessor
  signatures, executable constructor signatures with the recursive-builder contract, and the removed
  Scan risk line.

## 15. Status record (maintained through implementation)

* Assumptions: local-only work; no pushes/PRs/tags/registration; CSV/Arrow/Parquet checkouts untouched;
  network used only for specs, dependencies, and interop tools.
* Commands and results: recorded in `STATUS.md` in the worktree as phases complete (exact commands,
  Julia versions, pass/fail counts, benchmark numbers).
* Current state: **plan agreed (Codex round 23: `VERDICT: AGREE`; Claude: no material objections remain); Phases 0–5 implemented; takeover audit complete; clean branch ready for pull-request validation (see `STATUS.md`).** No production code changed before agreement.

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
t = Avro.Table("w.avro"; select=(:station, :temp))   # columns; projection pushdown
for row in Avro.Rows("w.avro")              # streaming, one block resident; `row` is an Avro.Row
    row.temp
end
first_row = Avro.Rows(first, "w.avro")      # do-block form: the file is closed deterministically
w = Avro.Writer("out.avro", sch; codec=:snappy); push!(w, first_row); close(w)

big = Avro.Limits(max_total_bytes=16 << 30, max_block_bytes=64 << 20)   # explicit, both sides
Avro.write("big.avro", bigtable; limits=big); Avro.Table("big.avro"; limits=big)

new = Avro.parseschema(...)                 # reader schema with a new defaulted field
Avro.Table("w.avro"; reader_schema=new)     # resolved decode (spec union policy; reader-directed values)
store = Avro.SchemaCache(); Avro.register!(store, sch)
msg = Avro.encodesingle(sch, first_row); Avro.decodesingle(msg, store)
Avro.Table("old.avro"; legacy=:avrojl1)     # file written by Avro.jl 1.x (padding / zstd name only)

Avro.Table("big.avro"; validate=:fast)      # opt-in fast skipping with documented blind spots
```
