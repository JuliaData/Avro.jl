# Logical types

Avro.jl maps the recognised logical types below to dedicated Julia representations. Unknown logical
types fall back to their underlying type and are preserved when re-encoding. The Avro 1.12
`big-decimal` logical type is deferred in 2.0 and follows this unknown-annotation behaviour.

| Logical type | Julia | Notes |
|---|---|---|
| `date` | `Dates.Date` | any `Int32` day count |
| `time-millis` / `time-micros` | `Dates.Time` | decode range-checked to one day; encode rejects sub-precision values — align with [`Avro.truncate`](@ref)/[`Avro.round`](@ref) |
| `timestamp-millis` / `-micros` / `-nanos` | [`Avro.Timestamp`](@ref)`{P}` = `Durations.ZonedTimestamp{P,:UTC}` | an instant on the global timeline: exact `Int64` ticks since the Unix epoch, displayed in UTC |
| `local-timestamp-*` | [`Avro.LocalTimestamp`](@ref)`{P}` = `Durations.Timestamp{P}` | a wall-clock date-time without a zone (`Dates.Timestamp` on Julia ≥ 1.14) |
| `decimal` (bytes/fixed) | [`Avro.Decimal`](@ref) (`Int128`, precision ≤ 38) or [`Avro.WideDecimal`](@ref) (`BigInt`) | big-endian two's complement; both encode and decode validate digits ≤ precision |
| `uuid` (string / fixed-16) | `UUIDs.UUID` | strings must be RFC-4122 `8-4-4-4-12` hex |
| `duration` | [`Avro.Duration`](@ref) | `months`/`days`/`millis` as `UInt32`; converts to and from `Durations.Duration` |

Timestamps are `Dates.AbstractDateTime`s from [Durations.jl](https://github.com/quinnj/Durations.jl):
they support the `Dates` accessors, period arithmetic, comparison and sorting across precisions, and
dictionary keys, and they never silently lose sub-millisecond precision. `DateTime(x)` converts
explicitly (flooring to milliseconds); `Avro.LocalTimestamp{P}(x, UTC)` (that is,
`Durations.Timestamp{P}(x, UTC)`) gives the UTC wall-clock time of a global instant. Global and local
timestamps are distinct types and compare unequal: a local timestamp does not identify an instant.

Encoding accepts, for a `timestamp-*` field, any `Durations.ZonedTimestamp` whose UTC instant is
exactly representable in the field's unit, a `DateTime` (read as UTC), and, with TimeZones.jl loaded, a
`ZonedDateTime`; for a `local-timestamp-*` field, any `Durations.Timestamp` exactly representable in the
field's unit and a `DateTime`. A typed target of `DateTime` performs the same explicit conversion; a target
of another unit converts exactly or raises `Avro.ConversionError`.

## Arrow integration

Arrow.jl 3 writes these timestamps as Arrow timestamp columns in their own unit: global instants with the
time zone `UTC`, local timestamps without one. It reads them back as the same Durations.jl types, and other
Arrow readers see ordinary timestamp columns:

```julia
using Avro, Arrow
Arrow.write("events.arrow", Avro.Table("events.avro"))
```

Dates, times, UUIDs and durations have Arrow types of their own, and a decimal inside a record becomes an
Arrow decimal of its schema's precision and scale. [Arrow.jl](@ref arrow-jl) lists the Arrow type of each Avro
type.

Deviations from other implementations, in favour of the specification, are recorded in
[Limits and security](limits-and-security.md): decimal precision is validated on decode as well as
encode, and time values are never silently truncated on encode.
