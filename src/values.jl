# Value types that do not reference schemas (plan §4.6). `Record`, `EnumValue`, `Fixed`, `UnionValue` and
# `Avro.Map` live in `schema.jl`/`map.jl` because they carry schema identity or frozen storage.

using Dates: Dates, Millisecond, Microsecond, Nanosecond, DateTime, Date, Time

"""
    Avro.Decimal(unscaled::Int128, scale::Int)

A decimal with at most 38 digits: `unscaled × 10^-scale`. Decoded from `decimal` logical types with
precision ≤ 38; encoding requires `scale` to equal the schema's scale exactly.
"""
struct Decimal
    unscaled::Int128
    scale::Int
end

"""
    Avro.WideDecimal(unscaled::BigInt, scale::Int)

A decimal whose precision exceeds 38 digits.
"""
struct WideDecimal
    unscaled::BigInt
    scale::Int
end

function Base.:(==)(a::Decimal, b::Decimal)
    return a.unscaled == b.unscaled && a.scale == b.scale
end

function Base.hash(a::Decimal, h::UInt)
    return hash(a.scale, hash(a.unscaled, hash(:Decimal, h)))
end

function Base.:(==)(a::WideDecimal, b::WideDecimal)
    return a.unscaled == b.unscaled && a.scale == b.scale
end

function Base.hash(a::WideDecimal, h::UInt)
    return hash(a.scale, hash(a.unscaled, hash(:WideDecimal, h)))
end

"""
    Avro.Timestamp{P}(ticks::Int64)

An exact global instant: `ticks` units of `P` (`Millisecond`, `Microsecond` or `Nanosecond`) since the
Unix epoch. `DateTime(x)` is an explicit, range-checked conversion (`ConversionError`) that floors
sub-millisecond units.
"""
struct Timestamp{P<:Dates.TimePeriod}
    ticks::Int64
end

"""
    Avro.LocalTimestamp{P}(ticks::Int64)

An exact local (zone-less) timestamp: `ticks` units of `P` since 1970-01-01T00:00:00 local time.
"""
struct LocalTimestamp{P<:Dates.TimePeriod}
    ticks::Int64
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

const UNIX_EPOCH_MS = Dates.value(DateTime(1970, 1, 1)) # Rata Die milliseconds of the Unix epoch

function tickscale(::Type{Millisecond})
    return 1
end

function tickscale(::Type{Microsecond})
    return 1_000
end

function tickscale(::Type{Nanosecond})
    return 1_000_000
end

"""
    DateTime(x::Avro.Timestamp{P}) / DateTime(x::Avro.LocalTimestamp{P})

Range-checked conversion to a `DateTime` (floors sub-millisecond units). Out-of-range values raise
`Avro.ConversionError`.
"""
function Dates.DateTime(x::Union{Timestamp{P},LocalTimestamp{P}}) where {P}
    ms = fld(x.ticks, tickscale(P))
    r = Base.Checked.add_with_overflow(ms, UNIX_EPOCH_MS)
    r[2] && throw(ConversionError("timestamp $(x.ticks) $(P) is outside the DateTime range"))
    return DateTime(Dates.UTM(r[1]))
end

function fromdatetime(::Type{T}, dt::DateTime) where {T<:Union{Timestamp,LocalTimestamp}}
    P = T.parameters[1]
    delta = Base.Checked.sub_with_overflow(Dates.value(dt), UNIX_EPOCH_MS)
    delta[2] && throw(ConversionError("DateTime $dt does not fit a $(T)"))
    r = Base.Checked.mul_with_overflow(delta[1], Int64(tickscale(P)))
    r[2] && throw(ConversionError("DateTime $dt does not fit a $(T)"))
    return T(r[1])
end

function Timestamp{P}(dt::DateTime) where {P<:Dates.TimePeriod}
    return fromdatetime(Timestamp{P}, dt)
end

function LocalTimestamp{P}(dt::DateTime) where {P<:Dates.TimePeriod}
    return fromdatetime(LocalTimestamp{P}, dt)
end

function Base.show(io::IO, x::Timestamp{P}) where {P}
    print(io, "Avro.Timestamp{", nameof(P), "}(", x.ticks, ")")
    return nothing
end

function Base.show(io::IO, x::LocalTimestamp{P}) where {P}
    print(io, "Avro.LocalTimestamp{", nameof(P), "}(", x.ticks, ")")
    return nothing
end

"""
    Avro.truncate(t::Time, P) / Avro.round(t::Time, P)

Align a `Dates.Time` to the `time-millis` (`Millisecond`) or `time-micros` (`Microsecond`) precision
`P`; encoding a non-aligned `Time` is an `EncodeError` (Java truncates silently — recorded deviation).
"""
function truncate(t::Time, ::Type{P}) where {P<:Dates.TimePeriod}
    ns = Dates.value(t)
    unit = P === Millisecond ? 1_000_000 : (P === Microsecond ? 1_000 : 1)
    return Time(Nanosecond(fld(ns, unit) * unit))
end

"""
    Avro.round(t::Time, P)

Round-to-nearest alignment; see [`Avro.truncate`](@ref).
"""
function round(t::Time, ::Type{P}) where {P<:Dates.TimePeriod}
    ns = Dates.value(t)
    unit = P === Millisecond ? 1_000_000 : (P === Microsecond ? 1_000 : 1)
    aligned = Base.round(Int64, ns / unit) * unit
    aligned >= 86_400_000_000_000 && (aligned -= unit)
    return Time(Nanosecond(aligned))
end
