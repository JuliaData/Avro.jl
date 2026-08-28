# Object container files

## Writing

[`Avro.write`](@ref) writes any Tables.jl source (row or column access) to a container file:

```julia
Avro.write("data.avro", table; codec=:zstandard, level=7, metadata=Dict("origin" => b"etl-v2"))
buf = Avro.tobuffer(table)                     # an IOBuffer instead of a file
```

The schema comes from `schema=`, else from the retained schema of an Avro source (an `Avro.Table`,
`Avro.Rows` or `Avro.Reader`), else from the source's `Tables.schema`. Sources without one need an
explicit `schema=`. `Tables.partitions` become block boundaries.

Writing to a path is **atomic by default**: output goes to a temporary file that is renamed over the
target only on a successful `close`. With `atomic=true`, a failure leaves the target unchanged. With
`atomic=false`, the destination is opened in place, so a failure can leave it truncated or containing
complete blocks plus a partial final block. `fsync=true` syncs the file before rename, but does not sync
the directory that contains it.

[`Avro.Writer`](@ref) is the streaming form:

```julia
w = Avro.Writer("data.avro", s; codec=:deflate, block_bytes=256 * 1024)
for datum in produce()
    push!(w, datum)
end
close(w)
```

A `Writer` enforces every limit a reader enforces and preflights the reader's complete memory peak,
so **everything a successfully closed writer emits is accepted by every guaranteed consumer under
identical limits** (see [Limits and security](limits-and-security.md)). The first failure poisons the
writer, including a rejected datum or limit failure from `push!`; further use throws
`Avro.WriterClosedError` carrying the original cause. Closing a poisoned writer discards its buffered
block. For `atomic=false` and caller-owned streams, no repair of partial output is promised.

## Reading

Three consumers, all schema-carrying and all bounded by the limits:

* [`Avro.Table`](@ref) — materialise as a column table ([Tables.jl integration](tables.md)).
* [`Avro.Rows`](@ref) — stream datums with per-block memory.
* [`Avro.Reader`](@ref) — the low-level form: [`Avro.eachdatum`](@ref) iterates values of any root
  schema, [`Avro.eachblock`](@ref) yields `(count, bytes)` per block.

```julia
Avro.Reader("data.avro") do r
    @show Avro.writerschema(r) Avro.codec(r) Avro.metadata(r)
    for v in Avro.eachdatum(r)
        # ...
    end
end
```

Sources: a file path (memory-mapped by default; `mmap=false` streams — the safe option for files that
may change concurrently), byte vectors, `IOBuffer`, or any `IO` (streamed). `Reader` and `Rows` hold
path-owned handles until `close` (do-block forms close on exit); caller-owned `IO` is never closed.

## Codecs

`null`, `deflate`, `snappy` and `zstandard` are built in; `bzip2` and `xz` activate as package
extensions when `CodecBzip2`/`CodecXz` are loaded. [`Avro.codecs`](@ref) lists what is available.
Decoders enforce `max_codec_memory` against each frame's declared requirement (a zstandard file
written with a huge window fails fast, with the limit to raise); writers verify every frame they emit
against the same cap, so a written file is always decodable under the same limits.

## Inspection and 1.x files

[`Avro.inspect`](@ref) reports header and framing issues, including a container's codec, schema, and
block structure. It walks each datum without materialising values and diagnoses payload count
mismatches and Avro.jl 1.x null-codec padding. It cannot infer decimal byte order.
Files written by Avro.jl ≤ 1.1.2 have known defects; read them with
`legacy=:avrojl1` (padded null-codec blocks, the `zstd` codec name, unnamed fixed schemas) and
`decimal_byteorder=:little` (native-endian fixed-16 decimals), and rewrite once:

```julia
Avro.write("clean.avro",
           Avro.Rows("old.avro"; legacy=:avrojl1, decimal_byteorder=:little);
           codec=:zstandard)
```
