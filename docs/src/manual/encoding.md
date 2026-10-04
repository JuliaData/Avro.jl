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
dwT = Avro.DatumWriter(s, T)           # the typed kernel: zero allocations for aligned NamedTuples
```

`DatumReader` objects are safe to share across tasks because each call owns its decoder state.
`DatumWriter` retains reusable encoder and typed-plan state and is single-owner; do not call the same
writer concurrently. Schemas and plans are immutable and shareable.

## The generic value model

Generic decoding returns a closed set of types (`Avro.valuetypes()`), chosen so untrusted schemas
never create new compiled code:

| Avro | Julia |
|---|---|
| null / boolean / int / long / float / double | `missing` / `Bool` / `Int32` / `Int64` / `Float32` / `Float64` |
| bytes / string | `Vector{UInt8}` / `String` |
| fixed / enum | [`Avro.Fixed`](@ref) / [`Avro.EnumValue`](@ref) |
| record | [`Avro.Record`](@ref) (field access by name, without interning) |
| array / map | `Vector{e}` / [`Avro.Map`](@ref)`{e}` (one level of typed nesting) |
| union `["null", T]` | `Union{Missing, T}` (bare values) |
| other unions | [`Avro.UnionValue`](@ref) (1-based position; [`Avro.ordinal`](@ref) is the wire index) |

Logical types decode to dedicated wrappers — see [Logical types](logicaltypes.md).

`Avro.EnumValue` and `Avro.UnionValue` positions are 1-based; `Avro.ordinal` returns the zero-based
wire value. `Symbol(x)` on an enum value is an explicit, caller-owned interning action.

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

`NamedTuple`s, plain structs, `Dict` targets, `Base.Enum`s, `Symbol`s (through the admission table),
`DateTime` (an explicit range-checked conversion from the exact timestamp wrappers) and StructUtils
customisations are supported. Conversion failures raise `Avro.ConversionError`.

## Validation modes

Every read path takes `validate=:strict` (the default: every value walked and validated, blocks
checked for exact consumption) or `validate=:fast` (sized array/map blocks are jumped by their byte
size; skipped strings are not UTF-8-validated — which also holds in strict mode; everything decoded
is still validated). Malformed data raises `Avro.DataError`.

## The JSON encoding

[`Avro.tojson`](@ref) and [`Avro.fromjson`](@ref) implement the specification's JSON encoding
(union framing by branch name, bytes/fixed as `\\u00XX` strings).
