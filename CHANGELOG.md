# Changelog

All notable changes to this project are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - Unreleased

Avro.jl 2.0 is a ground-up rewrite of the Avro data-format implementation, with fixed, portable
resource limits, strict validation by default, parallel container decoding, and oracle-tested
interoperability. It is SemVer-breaking; deprecation shims cover the main 1.x entry points for one
major cycle. The Avro 1.12 `big-decimal` logical type is preserved as an unknown annotation and is
deferred.

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
- `Avro.Rows`: bounded-memory streaming (generic `Avro.Row`s, typed `T`, or plain values), with
  `Tables.partitions`.
- Projection pushdown: `select=` on `Avro.Table` and `Avro.Rows` skips unselected fields and derives
  an effective schema that `Avro.write` round-trips.
- Parallel container decoding: `Avro.Table(src; ntasks=…)` — identical results, acceptance and errors
  at every `ntasks`; parallelism only uses headroom under the memory ceiling.
- Fixed, validated resource limits (`Avro.Limits`) with one 256 MiB per-operation default ceiling, a
  decompression work rule, per-block caps, and a bounded `Symbol`-admission table
  (`Avro.SymbolAdmission`, `names=` everywhere `Symbol`s are produced).
- Validation modes: `validate=:strict` (default) and `:fast`.
- Writer safety: atomic file replacement, failure poisoning, and a preflight that guarantees every
  successfully written file is readable by every guaranteed consumer under identical limits.
- Codecs: `zstandard`, `snappy`, `deflate`, `null` built in; `bzip2` and `xz` via package extensions
  (`using CodecBzip2` / `using CodecXz`); per-frame decoder-memory verification on write.
- `Avro.inspect`: bounded container header and framing diagnostics (codec, schema, block structure).
- DataAPI metadata on `Avro.Table`; stored `Tables.Schema` (names and eltypes never become type
  parameters).
- Legacy support for files written by Avro.jl ≤ 1.1.2: `legacy=:avrojl1` and
  `decimal_byteorder=:little`.
- Prepared codecs: `Avro.DatumReader(schema[, T])` and `Avro.DatumWriter(schema[, T])` — the typed
  writer encodes aligned NamedTuples with zero allocations.

### Changed (breaking)

- Customisation moved from StructTypes to StructUtils; `JSON3`/`SentinelArrays` dependencies dropped.
- Generic decoding returns the closed value model: enums as `Avro.EnumValue`, fixed as `Avro.Fixed`,
  nested records as `Avro.Record`, non-nullable unions as `Avro.UnionValue` (1-based positions;
  `Avro.ordinal` is the wire index); timestamps as exact `Avro.Timestamp`/`Avro.LocalTimestamp`
  wrappers (`DateTime(x)` is an explicit, range-checked conversion); `Avro.Duration` fields are
  `UInt32`; decimals are `Avro.Decimal`/`Avro.WideDecimal` with runtime scale, big-endian per the
  specification.
- `Avro.readtable` → `Avro.Table` (columns, not lazy records); `Avro.writetable` → `Avro.write`;
  `compress=:zstd` → `codec=:zstandard` (the shims map these); `Avro.write(io, x)` with a non-table
  `x` is no longer the datum writer (use `Avro.encode`).
- Container reading validates strictly by default; schema-less Tables sources require `schema=`;
  invalid Julia-derived names are errors instead of being silently emitted.
- `Avro.Record{names,T,N}`, `Avro.Enum{names}` and `Avro.Array` are removed.

### Deprecated

- `Avro.readtable(src)` (reads with `legacy=:avrojl1`) and `Avro.writetable(dst, tbl; compress=…)` —
  removal in 3.0.

### Fixed

- 1.x data defects are readable (`legacy=:avrojl1`, `decimal_byteorder=:little`): padded null-codec
  blocks, the `zstd` codec name, native-endian fixed-16 decimals, unnamed fixed schemas. `Avro.inspect`
  reports header-visible and framing issues, walks datum structure, and diagnoses legacy null-codec
  padding; it does not infer decimal byte order.
