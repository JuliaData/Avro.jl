# Byte-level decoder: bounds-checked reads against an explicit end position; every length and count is
# validated against the remaining input before anything is allocated; malformed input is a `DataError`
# carrying the byte position.

"""
    Decoder(buf, limits=Limits(); pos=1, stop=length(buf))

A cursor over a contiguous byte buffer: `pos` is the next byte to read and `stop` the last valid byte
(inclusive). `values` counts the values decoded from the current scope against `valuecap`: the datum cap
`valuecap(limits, stop - pos + 1)`, or a container block's cap once `blockcap!` sets it.
"""
mutable struct Decoder{B<:AbstractVector{UInt8}}
    buf::B
    pos::Int
    stop::Int
    depth::Int
    values::Int
    valuecap::Int
    const limits::Limits
end

function Decoder(buf::AbstractVector{UInt8}, limits::Limits=Limits(); pos::Int=1, stop::Int=length(buf))
    Base.require_one_based_indexing(buf)
    1 <= pos <= stop + 1 || throw(ArgumentError("decoder position $pos out of range"))
    stop <= length(buf) || throw(ArgumentError("decoder stop $stop out of range"))
    return Decoder{typeof(buf)}(buf, pos, stop, 0, 0, valuecap(limits, stop - pos + 1), limits)
end

# A `Decoder` indexes from 1 under `@inbounds` and views strings in place, so it takes one-based contiguous
# bytes; other arrays are copied.
function onebased(bytes::AbstractVector{UInt8})
    bytes isa Union{DenseVector{UInt8},Base.FastContiguousSubArray{UInt8,1,<:DenseArray}} &&
        !Base.has_offset_axes(bytes) && return bytes
    return copyto!(Vector{UInt8}(undef, length(bytes)), bytes)
end

"Rebind a decoder to a new scope: a buffer window, zero depth and value count, a fresh value cap."
function reset!(d::Decoder{B}, buf::B, pos::Int, stop::Int=length(buf)) where {B}
    1 <= pos <= stop + 1 || throw(ArgumentError("decoder position $pos out of range"))
    stop <= length(buf) || throw(ArgumentError("decoder stop $stop out of range"))
    d.buf = buf
    d.pos = pos
    d.stop = stop
    d.depth = 0
    d.values = 0
    d.valuecap = valuecap(d.limits, stop - pos + 1)
    return d
end

"Give a decoder over one container block's bytes the block's value cap."
function blockcap!(d::Decoder)
    d.valuecap = blockvaluecap(d.limits, remaining(d))
    return d
end

function remaining(d::Decoder)
    return d.stop - d.pos + 1
end

function dataerror(d::Decoder, msg::AbstractString)
    throw(DataError(msg, d.pos))
end

@noinline function valuesexceeded(d::Decoder)
    throw(LimitError(:max_values_per_byte, d.values, d.valuecap))
end

"Count one decoded value against the scope's value cap."
@inline function countvalue!(d::Decoder)
    v = d.values + 1
    d.values = v
    v > d.valuecap && valuesexceeded(d)
    return nothing
end

@inline function countvalues!(d::Decoder, n::Int)
    v = satadd(d.values, n)
    d.values = v
    v > d.valuecap && valuesexceeded(d)
    return nothing
end

function enter!(d::Decoder)
    depth = d.depth + 1
    depth <= d.limits.max_depth || throw(LimitError(:max_depth, depth, d.limits.max_depth))
    d.depth = depth
    return nothing
end

function leave!(d::Decoder)
    d.depth -= 1
    return nothing
end

@inline function bytesavailable(d::Decoder, n::Int)
    n >= 0 || throw(ArgumentError("required byte count must be non-negative"))
    return n <= d.stop - d.pos + 1
end

@inline function readbyte(d::Decoder)
    d.pos <= d.stop || dataerror(d, "unexpected end of data")
    @inbounds b = d.buf[d.pos]
    d.pos += 1
    return b
end

"""
    readbool(d) -> Bool

A boolean byte; only `0x00` and `0x01` are accepted (spec over Java leniency).
"""
function readbool(d::Decoder)
    b = readbyte(d)
    b == 0x00 && return false
    b == 0x01 && return true
    d.pos -= 1
    dataerror(d, "invalid boolean byte 0x$(string(b; base=16, pad=2))")
end

"""
    readlong(d) -> Int64

A zig-zag varint of at most 10 bytes; a 10th byte with bits beyond the 64th set is rejected.
"""
function readlong(d::Decoder)
    start = d.pos
    b = readbyte(d)
    v = UInt64(b & 0x7f)
    shift = 7
    while b >= 0x80
        shift > 63 && (d.pos = start; dataerror(d, "varint longer than 10 bytes"))
        b = readbyte(d)
        if shift == 63
            (b & 0x7e) == 0 || (d.pos = start; dataerror(d, "varint overflows 64 bits"))
            v |= UInt64(b & 0x01) << 63
        else
            v |= UInt64(b & 0x7f) << shift
        end
        shift += 7
    end
    return reinterpret(Int64, (v >> 1) ⊻ (-(v & UInt64(1))))
end

"""
    readint(d) -> Int32

A zig-zag varint of at most 5 bytes whose value fits `Int32`.
"""
function readint(d::Decoder)
    start = d.pos
    b = readbyte(d)
    v = UInt32(b & 0x7f)
    shift = 7
    while b >= 0x80
        shift > 28 && (d.pos = start; dataerror(d, "int varint longer than 5 bytes"))
        b = readbyte(d)
        if shift == 28
            (b & 0x70) == 0 || (d.pos = start; dataerror(d, "int varint overflows 32 bits"))
            v |= UInt32(b & 0x0f) << 28
        else
            v |= UInt32(b & 0x7f) << shift
        end
        shift += 7
    end
    return reinterpret(Int32, (v >> 1) ⊻ (-(v & UInt32(1))))
end

function readfloat(d::Decoder)
    bytesavailable(d, 4) || dataerror(d, "unexpected end of data (float)")
    p = d.pos
    @inbounds v = UInt32(d.buf[p]) | (UInt32(d.buf[p + 1]) << 8) | (UInt32(d.buf[p + 2]) << 16) | (UInt32(d.buf[p + 3]) << 24)
    d.pos = p + 4
    return reinterpret(Float32, v)
end

function readdouble(d::Decoder)
    bytesavailable(d, 8) || dataerror(d, "unexpected end of data (double)")
    p = d.pos
    v = UInt64(0)
    @inbounds for i in 0:7
        v |= UInt64(d.buf[p + i]) << (8 * i)
    end
    d.pos = p + 8
    return reinterpret(Float64, v)
end

"""
    readlen(d) -> Int

A non-negative length that does not exceed the remaining bytes.
"""
function readlen(d::Decoder)
    n = readlong(d)
    n >= 0 || dataerror(d, "negative length $n")
    bytesavailable(d, Int(n)) || dataerror(d, "length $n exceeds the remaining $(remaining(d)) bytes")
    return Int(n)
end

function readbytes(d::Decoder)
    n = readlen(d)
    out = Vector{UInt8}(undef, n)
    copyto!(out, 1, d.buf, d.pos, n)
    d.pos += n
    return out
end

"""
    readstring(d) -> String

A length-prefixed string validated as strict UTF-8 (`DataError` otherwise).
"""
function readstring(d::Decoder)
    n = readlen(d)
    p = d.pos
    validutf8(d.buf, p, p + n - 1) || dataerror(d, "invalid UTF-8 in string")
    s = n == 0 ? "" : substring(d.buf, p, n)
    d.pos = p + n
    return s
end

# Strict UTF-8 validity of `buf[from:to]`: all-ASCII bytes, the common case, are checked a word at a time, and
# anything else goes to Base, which rejects overlongs, surrogates and code points above U+10FFFF.
function validutf8(buf::Vector{UInt8}, from::Int, to::Int)
    ascii = GC.@preserve buf asciibytes(pointer(buf, from), to - from + 1, length(buf) - from + 1)
    return ascii || isvalid(String, view(buf, from:to))
end

function validutf8(buf::AbstractVector{UInt8}, from::Int, to::Int)
    return isvalid(String, view(buf, from:to))
end

# Whether the `n` bytes at `p` are all ASCII, read a word at a time with the last word overlapping the one before
# it. `room` bytes from `p` are readable, so fewer than eight are read as one masked word when `room >= 8`.
@inline function asciibytes(p::Ptr{UInt8}, n::Int, room::Int)
    acc = UInt64(0)
    if n >= 8
        i = 0
        while i < n - 8
            acc |= unsafe_load(Ptr{UInt64}(p + i))
            i += 8
        end
        acc |= unsafe_load(Ptr{UInt64}(p + n - 8))
    elseif room >= 8
        acc = ltoh(unsafe_load(Ptr{UInt64}(p))) & (typemax(UInt64) >> (8 * (8 - n)))
    else
        for i in 0:n - 1
            acc |= unsafe_load(p + i)
        end
    end
    return acc & 0x8080808080808080 == 0
end

"The `n` bytes at `p` as a new `String` (one copy; a pointer path for contiguous memory)."
function substring(buf::AbstractVector{UInt8}, p::Int, n::Int)
    bytes = Vector{UInt8}(undef, n)
    copyto!(bytes, 1, buf, p, n)
    return String(bytes)
end

function substring(buf::Union{Vector{UInt8},Base.FastContiguousSubArray{UInt8,1,<:DenseArray}}, p::Int, n::Int)
    return GC.@preserve buf unsafe_string(pointer(buf, p), n)
end

function readfixed(d::Decoder, n::Int)
    bytesavailable(d, n) || dataerror(d, "fixed of $n bytes exceeds the remaining $(remaining(d)) bytes")
    out = Vector{UInt8}(undef, n)
    copyto!(out, 1, d.buf, d.pos, n)
    d.pos += n
    return out
end

function skipfixed(d::Decoder, n::Int)
    bytesavailable(d, n) || dataerror(d, "fixed of $n bytes exceeds the remaining $(remaining(d)) bytes")
    d.pos += n
    return nothing
end

function skiplen(d::Decoder)
    n = readlen(d)
    d.pos += n
    return nothing
end

"""
    readindex(d, nbranches) -> Int

A zero-based enum or union index, validated against the number of symbols/branches; returns the
1-based position.
"""
function readindex(d::Decoder, n::Int)
    i = readlong(d)
    0 <= i < n || dataerror(d, "index $i out of range (0:$(n - 1))")
    return Int(i) + 1
end

"""
    readblockcount(d) -> (count::Int, size::Int)

An array/map block header: a non-negative count, or a negative count followed by the block's byte
size (`size == -1` when absent). `typemin(Int64)` is rejected (negation overflow).
"""
function readblockcount(d::Decoder)
    c = readlong(d)
    c >= 0 && return (Int(c), -1)
    c == typemin(Int64) && dataerror(d, "block count typemin(Int64)")
    return (Int(-c), readlen(d))
end

"Reject a declared item count that the remaining bytes cannot hold (`minsize` bytes per item)."
function checkcount(d::Decoder, count::Int, minsize::Int)
    minsize <= 0 && return nothing
    minsize == INFINITE && count > 0 && dataerror(d, "block declares $count items of a schema with no finite datum")
    count <= remaining(d) ÷ minsize || dataerror(d, "block declares $count items but only $(remaining(d)) bytes remain")
    return nothing
end
