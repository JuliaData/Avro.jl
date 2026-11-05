# Tables.jl integration

## `Avro.Table`

[`Avro.Table`](@ref) materialises a record-root container as a column table: `Vector` columns (string columns
are `StringVector`s, below), `Tables.columnnames`/`Tables.getcolumn`, `length` (row count), and read-only DataAPI
metadata (`DataAPI.metadata(t, "key")` for the file's metadata).
The stored `Tables.Schema` carries names and eltypes in fields, never in type parameters, so untrusted
files cannot force compilation.

```julia
t = Avro.Table("data.avro")
ids = t.id                                    # also t[:id], or t[1] for the first column
df = DataFrame(t)                              # or any Tables.jl sink
```

A string column is a `DataStrings.StringVector` of `DataString`s, the string type Arrow.jl 3 reads. A string
of up to 12 bytes is stored in place, and longer strings share a few large buffers, so a column of millions of
strings is a few allocations rather than one per string, and a read leaves the garbage collector little to do.
`DataString`s compare, hash and print like `String`s; `String(s)` copies one out.

`Avro.schema(t)` is the effective (reader) schema, `Avro.writerschema(t)` the file's writer schema.
Column names take precedence in property access, including fields named `schema` or `metadata`;
use `Avro.schema(t)` and `Avro.metadata(t)` to inspect file information.

## Typed tables

`Avro.Table(src, T)` decodes each row into `T`, a `NamedTuple` or struct type, as `Avro.Rows(src, T)` does, and
holds one `Vector{fieldtype(T, i)}` column per field of `T`. `T` chooses the columns and their types: its fields
match the file's by name, so a `T` with fewer fields reads fewer columns, and a file whose schema has evolved
reads as `Avro.Rows(src, T)` reads it. Column access is type-stable, and a program compiled with
`juliac --trim=safe` can read a table this way:

```julia
t = Avro.Table("data.avro", @NamedTuple{id::Int64, price::DataDecimals.Decimal{10,2,Int64}})
sum(t.id)                                     # t.id is a Vector{Int64}
```

`limit=` and `offset=` bound its rows, as for a generic table.

## `Avro.Rows`

[`Avro.Rows`](@ref) streams with bounded memory, one decompressed block at a time. Three modes:

* **Generic record mode** (record root): a Tables.jl row source yielding [`Avro.Record`](@ref)s, which
  are `Tables.AbstractRow`s (`row.id`, `row[:id]`, `Tables.columnnames(row)`).
* **Typed mode** (`Avro.Rows(src, T)`): a plain iterator of `T` values.
* **Non-record mode**: a plain iterator of the generic values.

`close(rows)` releases the source (do-block form available). `Avro.write(dst, Avro.Rows(src))` re-encodes
one block at a time with bounded memory. `Tables.partitions(rows)` yields one `Avro.Table` per block when
the schema fixes every column's value types, so `Arrow.write(path, Avro.Rows(src))` streams too, one record
batch per block. Arrow.jl then declares every field nullable, as it does for any source that may have several
partitions, so those columns read back as `Union{Missing,T}`. A union of several kinds or a nested record
leaves a column's types to its values, and a sink such as Arrow.jl needs every batch to agree, so such rows
are one partition, read fully when the sink asks for columns.
Rows are consumed once. For bytes and memory-mapped files, `length(rows)` reports the number still
available; it decreases as rows or partitions are consumed. Streamed sources have no known length.

## Scans: columns, rows and filters

`select=` on `Avro.Table` and generic `Avro.Rows` takes one column name or a collection of them. It skips
unselected fields (by their recorded size where the writer wrote sized blocks, walking them otherwise) and
derives an effective schema of exactly the selected fields, which `Avro.write` round-trips:

```julia
t = Avro.Table("data.avro"; select=(:id, :name))
ids = Avro.Table("data.avro"; select=:id).id
```

`select=()` yields a zero-column table with the authoritative row count.

`Avro.Table` also takes `filter=`, `limit=` and `offset=`, or a whole `Tables.Scan` as `scan=`, the request
Arrow.jl 3 and CSV.jl accept too:

```julia
using Tables: col, colcmp
Avro.Table("data.avro"; limit=10)                                    # the first ten rows
Avro.Table("data.avro"; select=:name, filter=col(:fare) > 10)       # names of the rows with a fare above 10
Avro.Table("data.avro"; scan=Tables.Scan(select=(:id => :key,), filter=colcmp(==, col(:city), "Oslo")))
```

A read decodes only the selected columns and the columns its filter reads. Without a filter, `offset` and
`limit` count the file's rows, so a read decodes only the blocks that hold them and stops after the last. With
a filter, they count the rows it keeps. A scan may rename and repeat columns, under Avro names, but not convert
their types: read a typed table for that. The result is the same as `Tables.scan(Avro.Table(src), scan)`.
A record whose fields refer back to it, such as a linked list's node, reads every field once under its own name.

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
rows = Tables.rowtable(Avro.Table("data.avro"))
```

Most decoded values fit those packages without conversion. An [`Avro.EnumValue`](@ref) is a string, so
`row.suit == "HEARTS"` holds, and `JSON.json` and `CSV.write` give the symbol. An [`Avro.Fixed`](@ref) is a byte
vector: `JSON.json` gives its bytes. [Arrow.jl](@ref arrow-jl) lists the Arrow type of each Avro type.

A table's schema derives from its column types. An abstract column type such as `Any` becomes the union of
the types its values hold, and rows of differing types become the union of what each column holds. A column of
`Avro.Record`s or `Avro.Decimal`s has no conventional schema, so pass `schema=`, usually `Avro.schema(t)` of the
table it came from. Convert categorical columns with `DataAPI.unwrap.(column)` first. A column the schema has no
field for is an `ArgumentError`, not a silent drop, so select the columns to write first.

With TimeZones.jl loaded, `ZonedDateTime` is an instant of millisecond resolution: its columns derive
`timestamp-millis` and it encodes under any `timestamp-*` schema. It is also a typed target, as in
`Avro.Rows(src, @NamedTuple{at::ZonedDateTime})`, which gives the time in UTC, floored to milliseconds like
a `DateTime` target.

## [Arrow.jl](@id arrow-jl)

Avro.jl works with Arrow.jl 3. `Arrow.write` takes an `Avro.Table` or an `Avro.Rows`, and `Avro.write` takes an
`Arrow.Table` or an `Arrow.Stream`:

```julia
Arrow.write("data.arrow", Avro.Table("data.avro"))
Avro.write("out.avro", Arrow.Table("data.arrow"))
```

With Arrow.jl loaded, Avro.jl gives each nested [`Avro.Record`](@ref) the struct type its schema describes, so a
record column has one Arrow type in every row. Each Avro type writes as one Arrow type:

| Avro type | Arrow type |
|---|---|
| `null` | Null |
| `boolean`, `int`, `long`, `float`, `double` | Bool, Int32, Int64, Float32, Float64 |
| `string`, `enum` | Utf8 |
| `bytes`, `fixed` | Binary |
| `uuid` | the canonical `arrow.uuid` extension type |
| `date` | Date32 |
| `time-millis`, `time-micros` | Time64 in nanoseconds |
| `timestamp-millis`, `-micros`, `-nanos` | Timestamp in the same unit, with the time zone `UTC` |
| `local-timestamp-millis`, `-micros`, `-nanos` | Timestamp in the same unit, without a time zone |
| `duration` | Interval in months, days and nanoseconds |
| `decimal` inside a record | Decimal128 of the schema's precision and scale, or Decimal256 above 38 digits |
| `array` | List |
| `map` | Map with Utf8 keys |
| `record` | Struct of the schema's fields |
| `["null", T]` | the Arrow type of `T`, nullable |
| another union inside a record | a dense Union of its branches' Arrow types, or the one type they share |

A top-level column of unions with several kinds, or of arrays or maps that hold arrays, maps or unions, takes its
Arrow type from the values it holds. Values of one kind write as that kind's Arrow type, and values of several
kinds as a dense Union of them.

A decimal outside a record is an `Avro.Decimal` or an `Avro.WideDecimal`, which carries a scale but no
precision. An `Avro.WideDecimal` column writes as a Decimal256 of 76 digits at its scale. Arrow.jl writes an
`Avro.Decimal` as a struct of its unscaled value and scale. To write an Arrow decimal of the schema's precision
instead, read a typed table with `DataDecimals.Decimal{P,S}` fields, or convert the column, as in
`DataDecimals.Decimal{9,2}.(t.price)`.

Arrow.jl raises an error for a value that has no Arrow type:

* a decimal of more than 76 digits;
* a duration of more than 2,147,483,647 months or days;
* a recursive record;
* a column that takes its Arrow type from its values but holds none to type it, as in an empty file.

Arrow.jl reads these columns back as its own types, and `Avro.write` derives a schema from them, so timestamps,
dates, times, UUIDs, decimals and durations keep their logical types on the way back to Avro.
