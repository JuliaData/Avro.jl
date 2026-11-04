# Changelog

All notable changes to this project are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - Unreleased

Avro.jl 2.0 is a ground-up rewrite of the Avro data-format implementation: reusable schema and
encoding plans, a Tables.jl interface with projection and parallel decoding, strict validation,
input-proportional resource limits, and oracle-tested interoperability. It is SemVer-breaking;
deprecation shims cover the main 1.x entry points for one major cycle. The Avro 1.12 `big-decimal`
logical type is preserved as an unknown annotation and is deferred.

### Added

- Schema resolution (`Avro.resolve`; `reader_schema=` on `Avro.decode`, `Avro.DatumReader`,
  `Avro.decodesingle`, `Avro.Table`, and `Avro.Rows`) with both union-resolution policies (`:spec`,
  `:java`), aliases, promotions and defaults.
- The Avro sort order: `Avro.compare` and `Avro.comparebytes` (byte-level, without materialising).
- Parsing Canonical Form and schema fingerprints (`Avro.canonical`, `Avro.fingerprint`:
  CRC-64-AVRO, MD5, SHA-256).
- Single-object encoding (`Avro.encodesingle`/`Avro.decodesingle`) with `Avro.SchemaStore` and
  `Avro.SchemaCache`.
- The JSON encoding (`Avro.tojson`/`Avro.fromjson`).
- `Avro.Table`: a column table with DataAPI metadata, direct column access through `table.name`,
  `table[:name]` and `table[index]`, and a stored `Tables.Schema` (names and eltypes never become type
  parameters); `Tables.rowtable` builds typed rows from it on request.
- `Avro.Rows`: bounded-memory streaming of `Avro.Record`s (Tables.jl rows), typed values
  (`Avro.Rows(src, T)`), or plain values, with per-block `Tables.partitions`.
- `Avro.write` takes generators and vectors of structs or plain values as well as Tables.jl sources;
  `append=true` adds to an existing file; a rewrite keeps the source's user metadata; `metadata=` values
  may be strings; a table column the schema lacks is an error rather than a silent drop.
- `Avro.SingleObjectReader` and `Avro.SingleObjectWriter`: prepared single-object codecs for message streams.
- `references=` on `Avro.parseschema`, for named types shared across schema files.
- With Arrow.jl 3 loaded (ArrowTypes 2.4), nested `Avro.Record` columns write as Arrow struct columns typed by
  their schema, with their decimals as Arrow decimals; `Avro.Duration` values write as Arrow intervals.
- With TimeZones.jl loaded, `ZonedDateTime` reads and writes as an instant.
- Projection pushdown: `select=` on `Avro.Table` and `Avro.Rows` skips unselected fields and derives
  an effective schema that `Avro.write` round-trips.
- Parallel container decoding: `Avro.Table(src; ntasks=…)` decodes blocks straight into preallocated
  columns; results, acceptance and errors are identical at every `ntasks`.
- Parallel block compression: with a compressing codec, `Avro.write` and `Avro.Writer` compress up to
  `ntasks=` blocks at once while the caller encodes; the file is byte-identical at every `ntasks`.
- Resource limits (`Avro.Limits`): nesting depth, the largest materialised buffer, the number of values a
  block or datum may decode per input byte, and how far one read may decompress its input
  (`max_inflation`); every claimed length and count is checked against the remaining input before
  allocation.
- Writer safety: atomic file replacement, failure poisoning, and the reader's limits enforced at write
  time, so every successfully written file is readable under identical limits.
- Codecs: `zstandard`, `snappy`, `deflate`, `null` built in; `bzip2` and `xz` via package extensions
  (`using CodecBzip2` / `using CodecXz`).
- `Avro.inspect`: container header and framing diagnostics (codec, schema, block structure, legacy
  defects).
- Legacy support for files written by Avro.jl ≤ 1.1.2: `legacy=:avrojl1` (padded null-codec blocks,
  the `zstd` codec name, unnamed fixed schemas, native-endian fixed-16 decimals).
- Prepared codecs: `Avro.DatumReader(schema[, T])` and `Avro.DatumWriter(schema[, T])` reuse plans
  across calls; writers return a fresh byte vector or append to an `IO`. Typed reading and writing compile
  once per Julia type, whatever the schema, and every typed API takes the type positionally
  (`Avro.decode(schema, bytes, T)`, `Avro.Rows(src, T)`, `Avro.decodesingle(src, store, T)`).
- Programs compiled with `juliac --trim=safe` read and write through the typed APIs with default options:
  typed rows and structs, column tables, prepared readers and writers, and the null, deflate, snappy and
  zstandard codecs. Julia 1.13 and earlier can crash such a program at startup (JuliaLang/julia#63664);
  Julia 1.14 fixes it.
- Exact timestamps: `timestamp-*` values are `Durations.ZonedTimestamp{P,:UTC}` and `local-timestamp-*`
  values are `Durations.Timestamp{P}` (the `Dates.Timestamp` of Julia 1.14), so microsecond and
  nanosecond precision survives decoding and Arrow.jl writes them as native timestamp columns.

### Changed (breaking)

- Customisation moved from StructTypes to StructUtils; `JSON3`/`SentinelArrays` dependencies dropped.
- Generic decoding returns the closed value model: enums as `Avro.EnumValue` (a string), fixed as
  `Avro.Fixed` (a byte vector), nested records as `Avro.Record`, maps as `Dict{String,V}`, `["null", T]` unions as
  `Union{Missing,T}`, other unions as the bare value of the written branch (`Avro.UnionValue` only
  forces a branch when encoding); decimals are `Avro.Decimal`/`Avro.WideDecimal` with runtime scale,
  big-endian per the specification; `Avro.Duration` fields are `UInt32`.
- `Avro.readtable` → `Avro.Table` (columns, not lazy records); `Avro.writetable` → `Avro.write`;
  `compress=:zstd` → `codec=:zstandard` (the shims map these); `Avro.write(io, x)` with a non-table
  `x` is no longer the datum writer (use `Avro.encode`).
- Container reading validates every value; invalid Julia-derived names are errors instead of being silently
  emitted.
- Problems with a file's content raise `Avro.AvroError` subclasses throughout: asking `Avro.Table` for a
  non-record root or `select=` for a field the file lacks is an `Avro.SchemaError`, and a value a typed
  target cannot hold is an `Avro.ConversionError`. Typed decoding builds structs through their positional
  constructors, and encoding refuses integers a `float` or `double` cannot hold exactly.
- `Avro.Record{names,T,N}`, `Avro.Enum{names}` and `Avro.Array` are removed.

### Deprecated

- `Avro.readtable(src)` (reads with `legacy=:avrojl1`), `Avro.writetable(dst, tbl; compress=…)`,
  `Avro.read(src, T_or_schema)`, and the one-argument datum form `Avro.write(x)` — removal in 3.0.

### Fixed

- 1.x data defects are readable with `legacy=:avrojl1`: padded null-codec blocks, the `zstd` codec
  name, native-endian fixed-16 decimals, unnamed fixed schemas. `Avro.inspect` reports header-visible
  and framing issues, walks datum structure, and diagnoses legacy null-codec padding; it does not
  infer decimal byte order.
