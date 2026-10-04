# Avro

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://juliadata.github.io/Avro.jl/stable)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://juliadata.github.io/Avro.jl/dev)
[![CI](https://github.com/JuliaData/Avro.jl/workflows/CI/badge.svg)](https://github.com/JuliaData/Avro.jl/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/JuliaData/Avro.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/JuliaData/Avro.jl)

A pure-Julia implementation of the [Apache Avro](https://avro.apache.org/docs/++current++/specification/)
data format: schemas, binary and JSON encodings, schema resolution, sort order, single-object encoding,
and object container files — with a Tables.jl interface, parallel decoding, and fixed, portable
resource limits so untrusted files fail predictably instead of exhausting memory.

The package implements the Avro *data format*; Avro RPC is out of scope.

## Installation

```julia
using Pkg; Pkg.add("Avro")
```

## Quick start

```julia
using Avro

# Any Tables.jl source (DataFrame, CSV.File, a vector of NamedTuples, …)
rows = [(id=1, name="alice", score=9.5), (id=2, name="bob", score=7.0)]

# Write an object container file (the schema derives from the source and travels with the data)
Avro.write("data.avro", rows; codec=:zstandard)

# Read it back as a column table (Tables.jl-compatible: DataFrame(t), CSV.write(io, t), …)
t = Avro.Table("data.avro")

# Or stream it row by row with bounded memory
for row in Avro.Rows("data.avro")
    @show row.id, row.name
end

# Datum encoding without a container (schema supplied out of band)
s = Avro.parseschema("""{"type":"record","name":"P","fields":[
    {"name":"id","type":"long"},{"name":"name","type":"string"}]}""")
bytes = Avro.encode(s, (id=1, name="alice"))
value = Avro.decode(s, bytes)
```

## Highlights

* **Broad format support**: every primitive, named and complex type; all Avro 1.12 logical types except
  `big-decimal`, which is preserved as an unknown annotation and deferred; schema evolution
  (`reader_schema=` on `Avro.decode`, `Avro.DatumReader`, `Avro.decodesingle`, `Avro.Table`, and
  `Avro.Rows`; both union-resolution policies); canonical form and fingerprints; the Avro sort order
  (`Avro.compare`/`Avro.comparebytes`);
  single-object encoding with schema stores; the JSON encoding (`Avro.tojson`/`Avro.fromjson`).
* **Object container files**: `Avro.write`/`Avro.Table`/`Avro.Rows`/`Avro.Reader` with the
  `null`, `deflate`, `snappy`, `zstandard` codecs built in and `bzip2`/`xz` as package extensions;
  atomic file replacement on write; `Avro.inspect` for diagnosing files.
* **Tables.jl**: `Avro.Table` is a column table (with `Tables.partitions` per block and DataAPI
  metadata); `Avro.Rows` streams `Avro.Row`s; `select=` is projection pushdown that skips unselected
  fields; typed decoding into your own structs via StructUtils.
* **Parallel decoding**: `Avro.Table(src; ntasks=…)` decodes blocks concurrently with strictly
  bounded memory — parallelism only ever uses headroom under the memory ceiling, results and error
  behaviour are identical to sequential decoding at every `ntasks`.
* **Safety by default**: one fixed 256 MiB per-operation ceiling and validated limits govern every
  allocation; a work rule bounds decompression bombs; strict validation is the default; `Symbol`
  interning from untrusted data passes a bounded admission table. Every limit is raisable for trusted
  bulk workloads (`Avro.Limits`).
* **Interoperability**: files are round-tripped against the Java (avro-tools 1.12.2) and Python
  (fastavro 1.12.2) implementations in CI, over every codec and every root schema kind.

## Reading Avro.jl 1.x files

Files written by Avro.jl ≤ 1.1.2 have known defects (padded null-codec blocks, the `zstd` codec name,
native-endian decimals). Read them with the legacy options and rewrite them once:

```julia
Avro.write("clean.avro",
           Avro.Rows("old.avro"; legacy=:avrojl1, decimal_byteorder=:little);
           codec=:zstandard)
```

`Avro.inspect("old.avro")` reports header and framing issues, including the legacy `zstd` codec name.
It walks datum structure without materialising values and diagnoses legacy null-codec padding. It does
not infer decimal byte order. See the migration guide in the documentation for
the 1.x → 2.0 API changes (`writetable`/`readtable` still work, with deprecation warnings).

## Documentation

The [manual](https://juliadata.github.io/Avro.jl/stable) covers schemas, encodings, container files,
Tables.jl integration, schema evolution, logical types, sort order, single-object encoding,
performance, and the limits/security model.

## Acknowledgements

The test suite vendors fixtures from the [Apache Avro](https://github.com/apache/avro) repository
(`test/fixtures/apache`, Apache License 2.0; see the included `LICENSE` and `NOTICE` files).
