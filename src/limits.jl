# Resource limits: a few input-proportional bounds. Together with the decoder's rule that every claimed
# length or count is checked against the bytes that remain before anything is allocated, they keep memory
# and work proportional to the input an operation reads.

const KiB = 1 << 10
const MiB = 1 << 20
const GiB = 1 << 30
const INFINITE = typemax(Int)

"""
    Avro.Limits(; max_depth=1024, max_bytes=256 MiB, max_values_per_byte=16, value_allowance=65_536)

The resource limits every read and write runs under. Exceeding one raises [`Avro.LimitError`](@ref),
which names the limit and the keyword that raises it.

* `max_depth`: the nesting depth of values, JSON documents and schemas.
* `max_bytes`: the largest buffer one operation materialises from its input: a decompressed container
  block, a schema or metadata document, or one datum read from a stream.
* `max_values_per_byte`, `value_allowance`: one container block or standalone datum may decode at most
  `max_values_per_byte × input_bytes + value_allowance` values, which bounds values that take no input
  bytes (nulls, empty records, empty arrays).

Limits can reject valid data; raise them for trusted bulk workloads.
"""
struct Limits
    max_depth::Int
    max_bytes::Int
    max_values_per_byte::Int
    value_allowance::Int
end

const LIMIT_DEFAULTS = (max_depth=1024, max_bytes=256 * MiB, max_values_per_byte=16, value_allowance=65_536)

function limitvalue(name::Symbol, value)
    value isa Integer && !(value isa Bool) && typemin(Int) <= value <= typemax(Int) ||
        throw(ArgumentError("Avro.Limits: `$name` must be an Int-compatible integer"))
    v = Int(value)
    v >= 0 || throw(ArgumentError("Avro.Limits: `$name` must be ≥ 0, got $v"))
    return v
end

function Limits(; kwargs...)
    isempty(kwargs) && return DEFAULT_LIMITS
    for k in keys(kwargs)
        haskey(LIMIT_DEFAULTS, k) || throw(ArgumentError("unknown Avro.Limits keyword `$k`"))
    end
    limits = Limits(limitvalue(:max_depth, get(kwargs, :max_depth, LIMIT_DEFAULTS.max_depth)),
                    limitvalue(:max_bytes, get(kwargs, :max_bytes, LIMIT_DEFAULTS.max_bytes)),
                    limitvalue(:max_values_per_byte, get(kwargs, :max_values_per_byte, LIMIT_DEFAULTS.max_values_per_byte)),
                    limitvalue(:value_allowance, get(kwargs, :value_allowance, LIMIT_DEFAULTS.value_allowance)))
    return validate(limits)
end

function validate(l::Limits)
    l.max_depth >= 1 || throw(ArgumentError("Avro.Limits: `max_depth` must be ≥ 1"))
    l.max_values_per_byte >= 1 || throw(ArgumentError("Avro.Limits: `max_values_per_byte` must be ≥ 1"))
    return l
end

const DEFAULT_LIMITS = validate(Limits(LIMIT_DEFAULTS...))

function Base.show(io::IO, l::Limits)
    l === DEFAULT_LIMITS && return print(io, "Avro.Limits()")
    print(io, "Avro.Limits(")
    first = true
    for name in fieldnames(Limits)
        v = getfield(l, name)
        v == getfield(DEFAULT_LIMITS, name) && continue
        first || print(io, ", ")
        print(io, name, "=", v)
        first = false
    end
    return print(io, ")")
end

"The most values one scope of `inputbytes` input bytes may decode (saturating)."
function valuecap(l::Limits, inputbytes::Int)
    inputbytes > (typemax(Int) - l.value_allowance) ÷ l.max_values_per_byte && return typemax(Int)
    return l.max_values_per_byte * inputbytes + l.value_allowance
end

# For decoding an encoding this process just produced from caller values: the encode already did the work
# the value cap would bound.
function uncapped(l::Limits)
    return Limits(l.max_depth, l.max_bytes, l.max_values_per_byte, typemax(Int))
end

"Add two non-negative sizes and saturate at the largest representable Int."
function satadd(a::Int, b::Int)
    a >= 0 && b >= 0 || throw(ArgumentError("saturating sizes must be non-negative"))
    (a == INFINITE || b == INFINITE || a > INFINITE - b) && return INFINITE
    return a + b
end
