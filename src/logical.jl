# Logical types (plan §4.2, §4.8): a closed set of annotations on an underlying schema, evaluated in
# context — a malformed or misplaced annotation drops to the underlying type with the raw attributes
# preserved in `props` (as Java does), never rejecting the schema.

abstract type LogicalType end

"""
    Avro.DecimalLogical(precision, scale=0)

The `decimal` annotation (on `bytes` or `fixed`): `precision > 0`, `0 ≤ scale ≤ precision`.
"""
struct DecimalLogical <: LogicalType
    precision::Int
    scale::Int
end

DecimalLogical(precision::Integer) = DecimalLogical(Int(precision), 0)

struct UUIDLogical <: LogicalType end
struct DateLogical <: LogicalType end
struct TimeMillis <: LogicalType end
struct TimeMicros <: LogicalType end
struct TimestampMillis <: LogicalType end
struct TimestampMicros <: LogicalType end
struct TimestampNanos <: LogicalType end
struct LocalTimestampMillis <: LogicalType end
struct LocalTimestampMicros <: LogicalType end
struct LocalTimestampNanos <: LogicalType end
struct DurationLogical <: LogicalType end

"""
    Avro.UnknownLogical(name)

An unrecognised `logicalType` name (e.g. `big-decimal`, deferred), kept so the schema re-serialises
faithfully.
"""
struct UnknownLogical <: LogicalType
    name::String
end

logicalname(::DecimalLogical) = "decimal"
logicalname(::UUIDLogical) = "uuid"
logicalname(::DateLogical) = "date"
logicalname(::TimeMillis) = "time-millis"
logicalname(::TimeMicros) = "time-micros"
logicalname(::TimestampMillis) = "timestamp-millis"
logicalname(::TimestampMicros) = "timestamp-micros"
logicalname(::TimestampNanos) = "timestamp-nanos"
logicalname(::LocalTimestampMillis) = "local-timestamp-millis"
logicalname(::LocalTimestampMicros) = "local-timestamp-micros"
logicalname(::LocalTimestampNanos) = "local-timestamp-nanos"
logicalname(::DurationLogical) = "duration"
logicalname(l::UnknownLogical) = l.name

const SIMPLE_LOGICALS = Dict{String,Tuple{LogicalType,Tuple{Vararg{Symbol}}}}(
    "uuid" => (UUIDLogical(), (:string, :fixed)),
    "date" => (DateLogical(), (:int,)),
    "time-millis" => (TimeMillis(), (:int,)),
    "time-micros" => (TimeMicros(), (:long,)),
    "timestamp-millis" => (TimestampMillis(), (:long,)),
    "timestamp-micros" => (TimestampMicros(), (:long,)),
    "timestamp-nanos" => (TimestampNanos(), (:long,)),
    "local-timestamp-millis" => (LocalTimestampMillis(), (:long,)),
    "local-timestamp-micros" => (LocalTimestampMicros(), (:long,)),
    "local-timestamp-nanos" => (LocalTimestampNanos(), (:long,)),
    "duration" => (DurationLogical(), (:fixed,)),
)

const LOG10_2_192_HI = UInt64(0x4d104d427de7fbcc)
const LOG10_2_192_MID = UInt64(0x47c4acd605be48bc)
const LOG10_2_192_LO = UInt64(0x13569862a1e8f9a4)
const UINT64_WORD_MASK = UInt128(typemax(UInt64))

function lowword(x::UInt128)
    return UInt64(x & UINT64_WORD_MASK)
end

function mul_log10_2_bound(bits::UInt64, low::UInt64)
    p0 = widemul(bits, low)
    p1 = widemul(bits, LOG10_2_192_MID)
    p2 = widemul(bits, LOG10_2_192_HI)
    high0 = UInt64(p0 >> 64)
    sum1 = high0 + lowword(p1)
    carry1 = UInt64(sum1 < high0)
    high1 = UInt64(p1 >> 64)
    sum2 = high1 + lowword(p2)
    carry2 = UInt64(sum2 < high1)
    carried = sum2 + carry1
    carry2 |= UInt64(carried < sum2)
    return UInt64(p2 >> 64) + carry2
end

"""
    maxdecimalprecision(size) -> Int

The largest decimal precision a `fixed` of `size` bytes can hold: `floor((8·size − 1) × log10(2))`,
evaluated with checked, saturating integer arithmetic (never by constructing `2^(8n−1)`); 0 for `size
== 0`.
"""
function maxdecimalprecision(size::Int)
    size <= 0 && return 0
    bits = size > typemax(Int) ÷ 8 ? typemax(Int) : 8 * size - 1
    # These adjacent 192-bit integers bracket log10(2) when divided by 2^192.
    lower = mul_log10_2_bound(UInt64(bits), LOG10_2_192_LO)
    upper = mul_log10_2_bound(UInt64(bits), LOG10_2_192_LO + 1)
    lower == upper || throw(OverflowError("decimal precision exceeds the supported Int range"))
    return Int(lower)
end

jsonint(x::Int64) = x
jsonint(x) = nothing   # raw (overflowing) tokens and non-integers are not JSON integers that fit Int64

"""
    evaluatelogical(kind, size, props) -> Union{Nothing,LogicalType}

Evaluate a `logicalType` attribute in the context of its underlying schema kind (`:int`, `:long`,
`:bytes`, `:string`, `:fixed` with `size`) per plan §4.2; `nothing` when the annotation is absent or
invalid for the context (the raw attributes stay in `props`).
"""
function evaluatelogical(kind::Symbol, size::Int, props)
    haskey(props, "logicalType") || return nothing
    name = props["logicalType"]
    name isa String || return nothing
    if name == "decimal"
        kind in (:bytes, :fixed) || return nothing
        precision = jsonint(get(props, "precision", nothing))
        precision === nothing && return nothing
        scaleraw = get(props, "scale", Int64(0))
        scale = jsonint(scaleraw)
        scale === nothing && return nothing
        precision > 0 || return nothing
        0 <= scale <= precision || return nothing
        kind == :fixed && precision > maxdecimalprecision(size) && return nothing
        return DecimalLogical(Int(precision), Int(scale))
    end
    entry = get(SIMPLE_LOGICALS, name, nothing)
    entry === nothing && return UnknownLogical(name)
    logical, kinds = entry
    kind in kinds || return nothing
    logical isa UUIDLogical && kind == :fixed && size != 16 && return nothing
    logical isa DurationLogical && size != 12 && return nothing
    return logical
end
