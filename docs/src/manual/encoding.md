# Encoding and decoding

## One-shot APIs

```julia
bytes = Avro.encode(schema, value)     # binary encoding of one datum
value = Avro.decode(schema, bytes)     # generic decoding
```

`Avro.encode(value)` without a schema uses `Avro.schema(value)`. One-shot calls build their plan per
call; use prepared objects for repeated work.

## Prepared codecs

[`Avro.DatumReader`](@ref) and [`Avro.DatumWriter`](@ref) prepare a schema once:

```julia
dr = Avro.DatumReader(s)               # generic decoding
drT = Avro.DatumReader(s, T)           # typed decoding into T
dw = Avro.DatumWriter(s)               # accepts any encodable value
dwT = Avro.DatumWriter(s, T)           # fixes the input type and reuses its prepared plan
bytes = dwT(value)                     # returns a new owned byte vector
dwT(io, value)                         # appends to an existing output stream
```

`DatumReader` objects are safe to share across tasks because each call owns its decoder state.
`DatumWriter` retains reusable encoder state and is single-owner; do not call the same writer
concurrently. Schemas and plans are immutable and shareable.

## The generic value model

Generic decoding returns a closed set of types (`Avro.valuetypes()`), chosen so untrusted schemas
never create new compiled code:

| Avro | Julia |
|---|---|
| null / boolean / int / long / float / double | `missing` / `Bool` / `Int32` / `Int64` / `Float32` / `Float64` |
| bytes / string | `Vector{UInt8}` / `String` |
| fixed / enum | [`Avro.Fixed`](@ref) (an `AbstractVector{UInt8}`) / [`Avro.EnumValue`](@ref) (an `AbstractString`) |
| record | [`Avro.Record`](@ref) (a `Tables.AbstractRow`: `r.name`, `r[:name]`, `r["name"]`, `r[i]`) |
| array / map | `Vector{e}` / `Dict{String,e}` (one level of typed nesting) |
| union `["null", T]` | `Union{Missing, T}` (bare values) |
| other unions | the bare value of the branch that was written (stored as `Any`) |

Logical types decode to dedicated types — see [Logical types](logicaltypes.md).

Encoding reads a record's fields by name from a `NamedTuple`, struct, dictionary or Tables.jl row, and ignores
any others the value has. Integers must fit exactly: `16777217` does not fit a `float`, and in a union like
`["float", "double"]` it takes the `double` branch.

Union values carry no wrapper: `["int", "string"]` data decodes to an `Int32` or a `String`, and
encoding picks the branch from the value (by exact Julia type, then by the first branch that accepts
it; records, enums and fixed values carry their schema and match by name). Pass
[`Avro.UnionValue`](@ref)`(index, value)` to force a branch when two branches would accept the same
value. An `Avro.EnumValue` behaves as its symbol's string and an `Avro.Fixed` as its bytes, so
`x == "HEARTS"` and `x == UInt8[1, 2]` work. `Avro.EnumValue` positions are 1-based and `Avro.ordinal` returns
the zero-based wire value.

## Typed decoding

Pass a target type to decode straight into your own types:

```julia
struct Person
    id::Int64
    email::Union{Missing,String}
end
dr = Avro.DatumReader(s, Person)
p = dr(bytes)
```

`NamedTuple`s, plain structs, `Dict` targets, `Base.Enum`s, `Symbol`s, `DateTime` (an explicit
range-checked conversion from the exact timestamp types) and StructUtils customisations are supported.
Conversion failures raise `Avro.ConversionError`. A numeric target must hold the value: an integer exactly and
a float up to rounding, so `1e300` into `Float32` is an error while `0.1` is not. A struct is built through its
positional constructor, so its validation runs and any error it raises propagates; a struct with no such
constructor needs a StructUtils hook. Fields of types Avro does not decode directly (a `Set`, a type with its own
StructUtils `lift`) convert one by one through StructUtils. A type customised through StructUtils itself
(hooks, field tags or defaults) is built by StructUtils: an error raised by a caller-defined hook propagates,
and any other failure raises `Avro.ConversionError`.

When the target has a field absent from the reader schema, its StructUtils default factory runs for
**each decoded value**, so mutable defaults stay independent and defaults such as `uuid4()` or `now()`
are evaluated per row. On StructUtils versions supporting dependent defaults, factories can use earlier
fields in the target type's declaration order.

## Validation

Every value is validated as it is decoded: booleans, UTF-8 strings, enum and union indices, time-of-day
ranges, UUID text and decimal precision. Unselected fields are skipped by their encoded size where the
writer recorded one (sized array and map blocks) and walked otherwise. Malformed data raises
`Avro.DataError` with the byte position.

## The JSON encoding

[`Avro.tojson`](@ref) and [`Avro.fromjson`](@ref) implement the specification's JSON encoding
(union framing by branch name, bytes/fixed as `\\u00XX` strings).
