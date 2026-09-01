# Logical types

Avro.jl maps the recognised logical types below to dedicated Julia representations. Unknown logical
types fall back to their underlying type and are preserved when re-encoding. The Avro 1.12
`big-decimal` logical type is deferred in 2.0 and follows this unknown-annotation behaviour.

| Logical type | Julia | Notes |
|---|---|---|
| `date` | `Dates.Date` | any `Int32` day count |
| `time-millis` / `time-micros` | `Dates.Time` | decode range-checked to one day; encode rejects sub-precision values — align with [`Avro.truncate`](@ref)/[`Avro.round`](@ref) |
| `timestamp-*` | [`Avro.Timestamp`](@ref)`{Millisecond\|Microsecond\|Nanosecond}` | exact `Int64` ticks since the Unix epoch; `DateTime(x)` is an explicit, range-checked conversion (`Avro.ConversionError` on overflow; floor for sub-ms) |
| `local-timestamp-*` | [`Avro.LocalTimestamp`](@ref)`{...}` | same conversion rules |
| `decimal` (bytes/fixed) | [`Avro.Decimal`](@ref) (`Int128`, precision ≤ 38) or [`Avro.WideDecimal`](@ref) (`BigInt`); `Decimals.Decimal{P,S,T}` as a typed target (below) | big-endian two's complement; both encode and decode validate digits ≤ precision |
| `uuid` (string / fixed-16) | `UUIDs.UUID` | strings must be RFC-4122 `8-4-4-4-12` hex |
| `duration` | [`Avro.Duration`](@ref) | `months`/`days`/`millis` as `UInt32` |

Timestamps never silently become `DateTime` — the exact wrappers preserve the full `Int64` range. A
typed target of `DateTime` (or `ZonedDateTime` via the TimeZones extension, decoding as the UTC
instant) performs the same explicit conversion.

Deviations from other implementations, in favour of the specification, are recorded in
[Limits and security](limits-and-security.md): decimal precision is validated on decode as well as
encode, and time values are never silently truncated on encode.

## Fixed-scale decimals

The generic representations above carry the scale at runtime. `Decimals.Decimal{P,S,T}` carries the
precision and scale as *type* parameters over an `isbits` storage integer (`Int32`, `Int64`, `Int128`
or `Int256` for `P ≤ 9`, `18`, `38`, `76`), so it is Avro.jl's exact typed decimal: the coefficient is
read straight out of the big-endian two's complement payload and written back the same way, with no
`BigInt` and no allocation per value.

```julia
using Avro, Decimals

s = Avro.schema(Decimal{9,2,Int32})     # {"type":"bytes","logicalType":"decimal","precision":9,"scale":2}
x = parse(Decimal{9,2,Int32}, "-12.34")
Avro.decode(s, Avro.encode(s, x), Decimal{9,2,Int32}) === x

rows = Avro.Rows("prices.avro"; T = @NamedTuple{sku::String, price::Decimal{18,4,Int64}})
```

`Avro.schema(Decimal{P,S,T})` derives `bytes` annotated `decimal(P, S)` rather than a `fixed(n)`:
`fixed` is a *named* type, so deriving one would have to synthesise an Avro name per precision/scale
pair, which collides across storage types and defines the same name twice whenever a record holds two
decimals of the same shape. Writing into an explicit `fixed(n)` decimal schema is fully supported and
emits exactly `n` sign-extended bytes.

A `Decimals.Decimal` target is admitted for a `decimal` schema when its scale equals the schema's
scale exactly — a differing scale is a different number, never a silent rescale — and its precision
covers the schema's, so every value the schema admits is representable. Other targets still decode
through the conversion route, which raises `Avro.ConversionError` on a scale mismatch or a value that
does not fit. Encoding requires the same exact scale (`Avro.EncodeError` otherwise; rescale first
with `Decimals.rescale`).
