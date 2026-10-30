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
| `decimal` (bytes/fixed) | [`Avro.Decimal`](@ref) (`Int128`, precision ≤ 38) or [`Avro.WideDecimal`](@ref) (`BigInt`) | big-endian two's complement; both encode and decode validate digits ≤ precision |
| `uuid` (string / fixed-16) | `UUIDs.UUID` | strings must be RFC-4122 `8-4-4-4-12` hex |
| `duration` | [`Avro.Duration`](@ref) | `months`/`days`/`millis` as `UInt32` |

Timestamps never silently become `DateTime` — the exact wrappers preserve the full `Int64` range. A
typed target of `DateTime` performs the same explicit conversion; a typed `Avro.Timestamp{P}` target
of another unit converts exactly or raises `Avro.ConversionError`.

Timestamps support sorting, comparisons and dictionary keys across all three precisions without
rounding. For example, `Avro.Timestamp{Millisecond}(1) == Avro.Timestamp{Microsecond}(1000)`.
The same applies to local timestamps. Global and local timestamps are distinct: they compare unequal
and cannot be ordered against each other because a local timestamp does not identify a global instant.

## Arrow integration

Loading Arrow.jl activates an optional extension for `Avro.Timestamp` and `Avro.LocalTimestamp`:

```julia
using Avro, Arrow
Arrow.write("events.arrow", Avro.Table("events.avro"))
```

These columns use native Arrow timestamps in their original millisecond, microsecond or nanosecond
unit. Global instants carry the UTC timezone; local timestamps have no timezone. Julia extension
metadata restores the exact Avro wrapper when both packages are loaded, including nullable columns
and the full `Int64` tick range. Other Arrow implementations see ordinary timestamp columns.
`Arrow.Table(path; convert=false)` exposes the native Arrow storage types.

This extension covers timestamps. Other Avro-specific values, such as records, enums, fixed bytes,
general unions and durations, do not have a guaranteed Arrow round trip. Convert them explicitly to
the representation required by the destination; Tables.jl compatibility alone does not preserve
every Avro logical type.

Deviations from other implementations, in favour of the specification, are recorded in
[Limits and security](limits-and-security.md): decimal precision is validated on decode as well as
encode, and time values are never silently truncated on encode.
