# Value types that do not reference schemas. `Record`, `EnumValue`, `Fixed` and `UnionValue` live in
# `generic.jl` because they carry schema identity.

using Dates: Dates, Millisecond, Microsecond, Nanosecond, DateTime, Date, Time, UTC, UTInstant

"""
    Avro.Decimal(unscaled::Int128, scale::Int)

A decimal with at most 38 digits: `unscaled × 10^-scale`. Decoded from `decimal` logical types with
precision ≤ 38; encoding requires `scale` to equal the schema's scale exactly.
"""
const Decimal = DataDecimals.DecimalValue{Int128}

"""
    Avro.WideDecimal(unscaled::BigInt, scale::Int)

A decimal whose precision exceeds 38 digits.
"""
struct WideDecimal
    unscaled::BigInt
    scale::Int
end

function Base.:(==)(a::WideDecimal, b::WideDecimal)
    return a.unscaled == b.unscaled && a.scale == b.scale
end

function Base.hash(a::WideDecimal, h::UInt)
    return hash(a.scale, hash(a.unscaled, hash(:WideDecimal, h)))
end

"""
    Avro.Timestamp{P}

The value of an Avro `timestamp-millis`, `timestamp-micros` or `timestamp-nanos`: an instant on the
global timeline, `Durations.ZonedTimestamp{P,:UTC}` — `Int64` ticks of `P` since the Unix epoch, an
`AbstractDateTime` displayed in UTC. `DateTime(x)` converts explicitly (flooring to milliseconds);
`Avro.LocalTimestamp{P}(x, UTC)` is the UTC wall-clock time. [`Avro.timestamp`](@ref) builds one from raw
ticks.
"""
const Timestamp{P} = Durations.ZonedTimestamp{P,:UTC}

"""
    Avro.LocalTimestamp{P}

The value of an Avro `local-timestamp-*`: a wall-clock date-time without a time zone,
`Durations.Timestamp{P}` (the `Dates.Timestamp` of Julia ≥ 1.14) in `Int64` ticks of `P` since
1970-01-01T00:00:00. [`Avro.localtimestamp`](@ref) builds one from raw ticks.
"""
const LocalTimestamp{P} = Durations.Timestamp{P}

"""
    Avro.timestamp(P, ticks) -> Avro.Timestamp{P}
    Avro.timestamp(P, dt::DateTime) -> Avro.Timestamp{P}

The instant `ticks` units of `P` (`Millisecond`, `Microsecond` or `Nanosecond`) after the Unix epoch, or
the one a `DateTime` names (a `DateTime` that does not fit raises `Avro.ConversionError`). `Dates.value(x)`
is the tick count of a timestamp.
"""
function timestamp(::Type{P}, ticks::Integer) where {P<:Dates.TimePeriod}
    return Timestamp{P}(localtimestamp(P, ticks), UTC)
end

function timestamp(::Type{P}, dt::DateTime) where {P<:Dates.TimePeriod}
    return timestamp(P, datetimeticks(P, dt))
end

"""
    Avro.localtimestamp(P, ticks) -> Avro.LocalTimestamp{P}
    Avro.localtimestamp(P, dt::DateTime) -> Avro.LocalTimestamp{P}

The wall-clock time `ticks` units of `P` after 1970-01-01T00:00:00, or the one a `DateTime` names, like
[`Avro.timestamp`](@ref) without a time zone.
"""
function localtimestamp(::Type{P}, ticks::Integer) where {P<:Dates.TimePeriod}
    return LocalTimestamp{P}(UTInstant(P(Int64(ticks))))
end

function localtimestamp(::Type{P}, dt::DateTime) where {P<:Dates.TimePeriod}
    return localtimestamp(P, datetimeticks(P, dt))
end

const UNIX_EPOCH_MS = Dates.value(DateTime(1970, 1, 1))

# Ticks per millisecond of each Avro timestamp unit.
function tickscale(::Type{Millisecond})
    return 1
end

function tickscale(::Type{Microsecond})
    return 1_000
end

function tickscale(::Type{Nanosecond})
    return 1_000_000
end

function datetimeticks(::Type{P}, dt::DateTime) where {P}
    delta, o1 = Base.Checked.sub_with_overflow(Dates.value(dt), UNIX_EPOCH_MS)
    ticks, o2 = Base.Checked.mul_with_overflow(delta, Int64(tickscale(P)))
    (o1 || o2) && throw(ConversionError("DateTime $dt does not fit a timestamp in $(nameof(P))s"))
    return ticks
end

# `DateTime(x)` with an out-of-range value raising `ConversionError` (it floors sub-millisecond units).
function todatetime(x::Union{Timestamp{P},LocalTimestamp{P}}) where {P}
    ms, overflow = Base.Checked.add_with_overflow(fld(Dates.value(x), tickscale(P)), UNIX_EPOCH_MS)
    overflow && throw(ConversionError("timestamp $(Dates.value(x)) in $(nameof(P))s is outside the DateTime range"))
    return DateTime(Dates.UTM(ms))
end

"""
    Avro.Duration(months::UInt32, days::UInt32, millis::UInt32)

The Avro `duration` logical type (three little-endian unsigned 32-bit integers in a 12-byte fixed).
"""
struct Duration
    months::UInt32
    days::UInt32
    millis::UInt32
end

"""
    Avro.truncate(t::Time, P) / Avro.round(t::Time, P)

Align a `Dates.Time` to the `time-millis` (`Millisecond`) or `time-micros` (`Microsecond`) precision
`P`; encoding a non-aligned `Time` is an `EncodeError`.
"""
function truncate(t::Time, ::Type{P}) where {P<:Dates.TimePeriod}
    ns = Dates.value(t)
    unit = Dates.tons(P(1))
    return Time(Nanosecond(fld(ns, unit) * unit))
end

"""
    Avro.round(t::Time, P)

Round-to-nearest alignment; see [`Avro.truncate`](@ref).
"""
function round(t::Time, ::Type{P}) where {P<:Dates.TimePeriod}
    ns = Dates.value(t)
    unit = Dates.tons(P(1))
    aligned = Base.round(Int64, ns / unit) * unit
    aligned >= 86_400_000_000_000 && (aligned -= unit)
    return Time(Nanosecond(aligned))
end

function Duration(x::Durations.Duration)
    iszero(rem(x.nanoseconds, 1_000_000)) || throw(ArgumentError("Avro duration requires exact milliseconds"))
    return Duration(UInt32(x.months), UInt32(x.days), UInt32(div(x.nanoseconds, 1_000_000)))
end

function Durations.Duration(x::Duration)
    return Durations.Duration(x.months, x.days, Int64(x.millis) * 1_000_000)
end

function decimalinput(x::DataDecimals.AbstractDecimal)
    return WideDecimal(BigInt(DataDecimals.unscaled(x)), DataDecimals.scale(x))
end
