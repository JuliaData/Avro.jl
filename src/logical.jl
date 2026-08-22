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

"""
    maxdecimalprecision(size) -> Int

The largest decimal precision a `fixed` of `size` bytes can hold: `floor((8·size − 1) × log10(2))`,
evaluated with checked, saturating integer arithmetic (never by constructing `2^(8n−1)`); 0 for `size
== 0`.
"""
function maxdecimalprecision(size::Int)
    size <= 0 && return 0
    bits = size >= typemax(Int) ÷ 8 ? typemax(Int) : 8 * size - 1
    # log10(2) = 0.30102999566398... ; use an integer approximation that is exact for every practical size
    # (floor((bits × 30103) ÷ 100000) never exceeds the true floor for bits < 2^40, and saturates beyond)
    bits >= (typemax(Int) ÷ 30103) && return typemax(Int)
    return (bits * 30103) ÷ 100000
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
