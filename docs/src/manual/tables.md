# Tables.jl integration

## `Avro.Table`

[`Avro.Table`](@ref) materialises a record-root container as a column table: plain `Vector` columns,
`Tables.columnnames`/`Tables.getcolumn`, `length` (row count), and read-only DataAPI metadata (`DataAPI.metadata(t, "key")` for the file's metadata).
The stored `Tables.Schema` carries names and eltypes in fields, never in type parameters, so untrusted
files cannot force compilation.

```julia
t = Avro.Table("data.avro")
ids = t.id                                    # also t[:id], or t[1] for the first column
df = DataFrame(t)                              # or any Tables.jl sink
```

`Avro.schema(t)` is the effective (reader) schema, `Avro.writerschema(t)` the file's writer schema.
Column names take precedence in property access, including fields named `schema` or `metadata`;
use `Avro.schema(t)` and `Avro.metadata(t)` to inspect file information.

## `Avro.Rows`

[`Avro.Rows`](@ref) streams with bounded memory, one decompressed block at a time. Three modes:

* **Generic record mode** (record root): a Tables.jl row source yielding [`Avro.Record`](@ref)s, which
  are `Tables.AbstractRow`s (`row.id`, `row[:id]`, `Tables.columnnames(row)`).
* **Typed mode** (`T=`): a plain iterator of `T` values.
* **Non-record mode**: a plain iterator of the generic values.

`close(rows)` releases the source (do-block form available). `Avro.write(dst, Avro.Rows(src))` re-encodes
one block at a time with bounded memory. `Tables.partitions(rows)` yields one `Avro.Table` per block when
the schema fixes every column's value types, so `Arrow.write(path, Avro.Rows(src))` streams too. A union of
several kinds or a nested record leaves a column's types to its values, and a sink such as Arrow.jl needs
every batch to agree, so such rows are one partition, read fully when the sink asks for columns.
Rows are consumed once. For bytes and memory-mapped files, `length(rows)` reports the number still
available; it decreases as rows or partitions are consumed. Streamed sources have no known length.

## Projection pushdown

`select=` on `Avro.Table` and generic `Avro.Rows` takes one column name or a collection of them. It skips
unselected fields (by their recorded size where the writer wrote sized blocks, walking them otherwise) and
derives an effective schema of exactly the selected fields, which `Avro.write` round-trips:

```julia
t = Avro.Table("data.avro"; select=(:id, :name))
ids = Avro.Table("data.avro"; select=:id).id
```

`select=()` yields a zero-column table with the authoritative row count.

## Parallel decoding

`Avro.Table(src; ntasks=n)` decodes blocks concurrently for byte and memory-mapped sources: the block
headers are scanned once, the final columns are allocated once, and each block decodes straight into
its row range. Results, acceptance and errors are identical at every `ntasks`: a failing file fails
with the same error at the same block.

```julia
t = Avro.Table("big.avro"; ntasks=8)
```

Files with multi-megabyte blocks (`block_bytes=` on the writer) parallelise best, since per-block
overheads amortise.

## Working with other packages

`Avro.Table` and `Avro.Rows` work with any Tables.jl sink, and `Avro.write` takes any Tables.jl source:

```julia
df = DataFrame(Avro.Table("data.avro"))      # file metadata travels as DataFrame metadata
Avro.write("out.avro", df; schema=Avro.schema(Avro.Table("data.avro")))
Avro.write("out.avro", CSV.File("data.csv"); codec=:zstandard)
Arrow.write("data.arrow", Avro.Table("data.avro"))
Avro.write("out.avro", Arrow.Table("data.arrow"))
rows = Tables.rowtable(Avro.Table("data.avro"))
```

Most decoded values fit those packages without conversion. An [`Avro.EnumValue`](@ref) is a string, so
`row.suit == "HEARTS"` holds, and `JSON.json` and `CSV.write` give the symbol. An [`Avro.Fixed`](@ref) is a byte
vector: `JSON.json` gives its bytes. With Arrow.jl loaded, a nested [`Avro.Record`](@ref) column is written as an
Arrow struct column whose type comes from the record's schema. A recursive record, a column mixing two record
types, and a top-level map whose values are maps, arrays or unions have no Arrow type, so Arrow.jl raises an
error for them.

A table's schema derives from its column types. An abstract column type such as `Any` becomes the union of
the types its values hold, and rows of differing types become the union of what each column holds. A column of
`Avro.Record`s or `Avro.Decimal`s has no conventional schema, so pass `schema=`, usually `Avro.schema(t)` of the
table it came from. Convert categorical columns with `DataAPI.unwrap.(column)` first. A column the schema has no
field for is an `ArgumentError`, not a silent drop, so select the columns to write first.

With TimeZones.jl loaded, `ZonedDateTime` is an instant of millisecond resolution: its columns derive
`timestamp-millis` and it encodes under any `timestamp-*` schema. It is also a typed target, as in
`Avro.Rows(src; T=@NamedTuple{at::ZonedDateTime})`, which gives the time in UTC, floored to milliseconds like
a `DateTime` target.
