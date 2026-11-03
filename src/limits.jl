# Resource limits: a few input-proportional bounds. Together with the decoder's rule that every claimed
# length or count is checked against the bytes that remain before anything is allocated, they keep memory
# and work proportional to the input an operation reads.

const KiB = 1 << 10
const MiB = 1 << 20
const GiB = 1 << 30
const INFINITE = typemax(Int)

"""
    Avro.Limits(; max_depth=1024, max_bytes=256 MiB, max_values_per_byte=16, value_allowance=65_536,
                max_inflation=1024)

The resource limits every read and write runs under. Exceeding one raises [`Avro.LimitError`](@ref),
which names the limit and the keyword that raises it.

* `max_depth`: the nesting depth of values, JSON documents and schemas.
* `max_bytes`: the largest buffer one operation materialises from its input: a decompressed container
  block, a schema or metadata document, or one datum read from a stream.
* `max_values_per_byte`, `value_allowance`: a standalone datum may decode at most
  `max_values_per_byte × input_bytes + value_allowance` values. A container block may decode
  `max_values_per_byte` values per decompressed byte plus an allowance that shrinks with the block: at most
  `value_allowance`, and at most `max_values_per_byte` per byte of the block including its 18-byte framing.
  This bounds values that take no input bytes (nulls, empty records) per byte of the file.
* `max_inflation`: one read of a container file may decompress at most
  `max_bytes + max_inflation × compressed bytes` in total, so a small file cannot expand into many
  maximal blocks. Writers apply the same rules to the files they write.

Limits can reject valid data; raise them for trusted bulk workloads.
"""
struct Limits
    max_depth::Int
    max_bytes::Int
    max_values_per_byte::Int
    value_allowance::Int
    max_inflation::Int
end

const LIMIT_DEFAULTS = (max_depth=1024, max_bytes=256 * MiB, max_values_per_byte=16, value_allowance=65_536,
                        max_inflation=1024)

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
                    limitvalue(:value_allowance, get(kwargs, :value_allowance, LIMIT_DEFAULTS.value_allowance)),
                    limitvalue(:max_inflation, get(kwargs, :max_inflation, LIMIT_DEFAULTS.max_inflation)))
    return validate(limits)
end

function validate(l::Limits)
    l.max_depth >= 1 || throw(ArgumentError("Avro.Limits: `max_depth` must be ≥ 1"))
    l.max_values_per_byte >= 1 || throw(ArgumentError("Avro.Limits: `max_values_per_byte` must be ≥ 1"))
    l.max_inflation >= 1 || throw(ArgumentError("Avro.Limits: `max_inflation` must be ≥ 1"))
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

"The most values one standalone datum of `inputbytes` input bytes may decode (saturating)."
function valuecap(l::Limits, inputbytes::Int)
    inputbytes > (typemax(Int) - l.value_allowance) ÷ l.max_values_per_byte && return typemax(Int)
    return l.max_values_per_byte * inputbytes + l.value_allowance
end

# The least a container block takes in the file beyond its payload: a one-byte count, a one-byte size and
# the 16-byte sync marker.
const BLOCK_FRAMING = 18

"The most values one container block of `bytes` decompressed bytes may decode (saturating)."
function blockvaluecap(l::Limits, bytes::Int)
    framed = satadd(bytes, BLOCK_FRAMING)
    allowance = framed > l.value_allowance ÷ l.max_values_per_byte ? l.value_allowance : l.max_values_per_byte * framed
    bytes > INFINITE ÷ l.max_values_per_byte && return INFINITE
    return satadd(l.max_values_per_byte * bytes, allowance)
end

"The decompressed bytes one read may have produced from `compressed` compressed bytes (saturating)."
function inflationcap(l::Limits, compressed::Int)
    compressed > (INFINITE - l.max_bytes) ÷ l.max_inflation && return INFINITE
    return l.max_bytes + l.max_inflation * compressed
end

# For decoding an encoding this process just produced from caller values: the encode already did the work
# the value cap would bound.
function uncapped(l::Limits)
    return Limits(l.max_depth, l.max_bytes, l.max_values_per_byte, typemax(Int), l.max_inflation)
end

"Add two non-negative sizes and saturate at the largest representable Int."
function satadd(a::Int, b::Int)
    a >= 0 && b >= 0 || throw(ArgumentError("saturating sizes must be non-negative"))
    (a == INFINITE || b == INFINITE || a > INFINITE - b) && return INFINITE
    return a + b
end
