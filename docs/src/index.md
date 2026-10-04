# Avro.jl

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

Object container files carry their schema, so Tables.jl sources round-trip with no ceremony:

```julia
using Avro

rows = [(id=1, name="alice", score=9.5), (id=2, name="bob", score=7.0)]

Avro.write("data.avro", rows; codec=:zstandard)   # schema derived from the source

t = Avro.Table("data.avro")                       # a Tables.jl column table
t2 = Avro.Table("data.avro"; select=(:id,))       # projection pushdown

for row in Avro.Rows("data.avro")                 # bounded-memory streaming
    @show row.id
end
```

Datum encoding without a container (the schema supplied out of band):

```julia
s = Avro.parseschema("""{"type":"record","name":"P","fields":[
    {"name":"id","type":"long"},{"name":"name","type":"string"}]}""")
bytes = Avro.encode(s, (id=1, name="alice"))
value = Avro.decode(s, bytes)                     # an Avro.Record
```

## Where to go next

* [Schemas](manual/schemas.md) — parsing, constructing, deriving from Julia types, canonical form.
* [Encoding and decoding](manual/encoding.md) — datum APIs, the generic value model, typed decoding.
* [Container files](manual/container.md) — readers, writers, codecs, inspection, legacy 1.x files.
* [Tables.jl integration](manual/tables.md) — `Avro.Table`, `Avro.Rows`, projection, parallel decode.
* [Schema evolution](manual/evolution.md) — `reader_schema=`, resolution rules, union policies.
* [Limits and security](manual/limits-and-security.md) — the resource model every operation runs under.
* [Migration from 1.x](migration.md).
