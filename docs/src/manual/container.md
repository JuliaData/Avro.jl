# Object container files

## Writing

[`Avro.write`](@ref) writes any Tables.jl source (row or column access) or any collection of datums, such
as a vector of structs, to a container file:

```julia
Avro.write("data.avro", table; codec=:zstandard, level=7, metadata=Dict("origin" => b"etl-v2"))
Avro.write("people.avro", [Person(1, "alice"), Person(2, "bob")])
Avro.write("ids.avro", (i^2 for i in 1:100))   # a generator is collected first
buf = Avro.tobuffer(table)                     # an IOBuffer instead of a file
```

The schema comes from `schema=`, else from the retained schema of an Avro source (an `Avro.Table` or
`Avro.Rows`), else from the source's column types, or from the element type of a collection of datums.
`Tables.partitions` become block boundaries. A table column the schema has no field for is an `ArgumentError`.
`metadata` maps strings to strings or byte vectors; rewriting an Avro source keeps its user metadata unless you
pass your own.

Writing to a path is **atomic by default**: output goes to a temporary file that is renamed over the
target only on a successful `close`. The writer resolves the path's parent directory, including parent
symbolic links, when it opens. Changing the working directory later does not redirect the output. With
`atomic=true`, a failure leaves the target unchanged. With `atomic=false`, the destination is opened in
place, so a failure can leave it truncated or containing complete blocks plus a partial final block.
`fsync=true` syncs the file before rename, but does not sync the directory that contains it. A failed
file sync raises an error and prevents atomic publication. Atomic writes preserve an existing file's
permissions; new files use the process's normal creation permissions without changing the process-wide
umask.

`append=true` adds blocks to an existing file in place, like Java's `DataFileWriter.appendTo`:

```julia
Avro.write("events.avro", new_events; append=true)
```

The file's schema, codec, metadata and sync marker stay in force, and a missing or empty file is created
as usual. An explicit `schema=` must equal the file's schema, and `codec=` must match. Appending checks that the file
ends at a block boundary, so a torn file is reported before anything is added. A failed write removes the blocks it
added, but a process crash can still leave a torn last block. Only one writer may append at a time.

[`Avro.Writer`](@ref) is the streaming form, and it takes `append=true` too:

```julia
w = Avro.Writer("data.avro", s; codec=:deflate, block_bytes=256 * 1024)
for datum in produce()
    push!(w, datum)
end
close(w)
```

A `Writer` enforces the reader's limits as it writes, so **everything a successfully closed writer emits
is accepted by `Reader`, `Rows` and `Table` under identical limits** (see
[Limits and security](limits-and-security.md)). The first failure poisons the writer, including a
rejected datum or limit failure from `push!`; further use throws `Avro.WriterClosedError` carrying the original cause.
Closing a poisoned writer discards its buffered block. For `atomic=false` and caller-owned streams, no
repair of partial output is promised.

## Reading

Three consumers, all schema-carrying and all bounded by the limits:

* [`Avro.Table`](@ref) — materialise as a column table ([Tables.jl integration](tables.md)).
* [`Avro.Rows`](@ref) — stream datums with per-block memory.
* [`Avro.Reader`](@ref) — the low-level form: [`Avro.eachdatum`](@ref) iterates values of any root
  schema, [`Avro.eachblock`](@ref) yields `(count, bytes)` per decompressed block.

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
Decompression stops as soon as a block's output passes `max_bytes`.

## Inspection and 1.x files

[`Avro.inspect`](@ref) reports header and framing issues together with a container's codec, schema and
block structure. It walks each datum without materialising values and diagnoses payload count
mismatches and Avro.jl 1.x null-codec padding. It cannot infer decimal byte order.
Files written by Avro.jl ≤ 1.1.2 have known defects (padded null-codec blocks, the `zstd` codec
name, unnamed fixed schemas, native-endian fixed-16 decimals); read them with `legacy=:avrojl1`, which
tolerates all four, and rewrite once:

```julia
Avro.write("clean.avro", Avro.Rows("old.avro"; legacy=:avrojl1); codec=:zstandard)
```
