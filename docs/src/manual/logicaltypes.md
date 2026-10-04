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
typed target of `DateTime` (or `ZonedDateTime` via the TimeZones extension, decoding as the UTC
instant) performs the same explicit conversion.

Deviations from other implementations, in favour of the specification, are recorded in
[Limits and security](limits-and-security.md): decimal precision is validated on decode as well as
encode, and time values are never silently truncated on encode.
