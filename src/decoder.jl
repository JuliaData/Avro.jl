# Byte-level decoder (plan §4.3): every read is bounds-checked against an explicit end position with
# checked arithmetic; every length/count is validated before allocation; failures are `DataError`s with
# the byte position.

"""
    Decoder(buf, pos, stop, budget; validate=:strict)

A cursor over a contiguous byte buffer: `pos` is the next byte to read and `stop` the last valid byte
(inclusive). `budget` is the operation's `Budget`; `validate` is `:strict` or `:fast`.
"""
mutable struct Decoder{B<:AbstractVector{UInt8}}
    buf::B                  # non-const: prepared readers reuse the decoder across calls (§10.2)
    pos::Int
    stop::Int
    depth::Int
    const budget::Budget
    const validate::Symbol
    start::Int              # reusable allocation-free encoded-span cursor state
    const limits::Limits
    datummax::Int
    values0::Int
    inputmax::Int
    values::Int
    blind::Bool
    scopevalues::Int
    datumlimitactive::Bool  # the current stop is a static max_datum_bytes boundary
    blockvalues0::Int       # operation value count before the active block, or -1
    blockinput0::Int        # operation input count before the active block, or -1
end

function Decoder(buf::AbstractVector{UInt8}, budget::Budget; pos::Int=1, stop::Int=length(buf), validate::Symbol=:strict)
    validate in (:strict, :fast) || throw(ArgumentError("validate must be :strict or :fast"))
    1 <= pos <= stop + 1 || throw(ArgumentError("decoder position out of range"))
    stop <= length(buf) || throw(ArgumentError("decoder stop out of range"))
    return Decoder{typeof(buf)}(buf, pos, stop, 0, budget, validate, pos,
                                budget.limits, 0, 0, 0, 0, false, 0, false, -1, -1)
end

function remaining(d::Decoder)
    return d.stop - d.pos + 1
end

function dataerror(d::Decoder, msg::AbstractString)
    throw(DataError(msg, d.pos))
end

@inline function bytesavailable(d::Decoder, n::Int)
    n >= 0 || throw(ArgumentError("required byte count must be non-negative"))
    required = n > typemax(Int) - d.pos + 1 ? typemax(Int) : d.pos + n - 1
    required <= d.stop && return true
    observed = required > typemax(Int) - d.start + 1 ?
        typemax(Int) : required - d.start + 1
    if d.datumlimitactive && observed > d.limits.max_datum_bytes
        throw(LimitError(:max_datum_bytes, observed, d.limits.max_datum_bytes,
                         :max_datum_bytes, :decode))
    end
    return false
end

@inline function readbyte(d::Decoder)
    bytesavailable(d, 1) || dataerror(d, "unexpected end of data")
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
    readlen(d, max) -> Int

A non-negative length not exceeding `max` nor the remaining bytes.
"""
function readlen(d::Decoder, max::Int, limit::Symbol)
    n = readlong(d)
    n >= 0 || dataerror(d, "negative length $n")
    n <= max || throw(LimitError(limit, Int(n), max, limit, :decode))
    bytesavailable(d, Int(n)) ||
        dataerror(d, "length $n exceeds the remaining $(remaining(d)) bytes")
    return Int(n)
end

function readbytes(d::Decoder)
    n = readlen(d, d.budget.limits.max_bytes, :max_bytes)
    reserve!(d.budget, bytesbytes(n))
    out = Vector{UInt8}(undef, n)
    allocated!(d.budget, bytesbytes(n))
    copyto!(out, 1, d.buf, d.pos, n)
    d.pos += n
    return out
end

"""
    readstring(d) -> String

A length-prefixed string validated as strict UTF-8 (`DataError` otherwise).
"""
function readstring(d::Decoder)
    n = readlen(d, d.budget.limits.max_bytes, :max_bytes)
    p = d.pos
    validutf8(d.buf, p, p + n - 1) || dataerror(d, "invalid UTF-8 in string")
    if n == 0
        retain!(d.budget, stringbytes(0))
        s = ""
    else
        s = ownedsubstring(d.buf, p, n, d.budget)
    end
    d.pos = p + n
    return s
end

function validutf8(buf::AbstractVector{UInt8}, from::Int, to::Int)
    i = from
    while i <= to
        len = utf8_valid_length(buf, i, to)
        len == 0 && return false
        i += len
    end
    return true
end

function unsafe_substring(buf::AbstractVector{UInt8}, p::Int, n::Int)
    n == 0 && return ""
    return String(buf[p:p + n - 1])
end

function unsafe_substring(buf::Vector{UInt8}, p::Int, n::Int)
    return n == 0 ? "" : unsafe_string(pointer(buf, p), n)
end

function ownedsubstring(buf::AbstractVector{UInt8}, p::Int, n::Int, budget::Budget)
    peak = bytesbytes(n) + stringbytes(0)
    reserve!(budget, peak)
    bytes = Vector{UInt8}(undef, n)
    allocated!(budget, bytesbytes(n))
    copyto!(bytes, 1, buf, p, n)
    text = String(bytes)
    allocated!(budget, stringbytes(0))
    release!(budget, bytesbytes(0))
    return text
end

function ownedsubstring(buf::Vector{UInt8}, p::Int, n::Int, budget::Budget)
    charge = stringbytes(n)
    reserve!(budget, charge)
    text = unsafe_string(pointer(buf, p), n)
    allocated!(budget, charge)
    return text
end

function ownedsubstring(buf::SubArray{UInt8,1}, p::Int, n::Int, budget::Budget)
    if !Base.iscontiguous(buf)
        return invoke(ownedsubstring,
                      Tuple{AbstractVector{UInt8},Int,Int,Budget},
                      buf, p, n, budget)
    end
    charge = stringbytes(n)
    reserve!(budget, charge)
    text = GC.@preserve buf unsafe_string(pointer(buf, p), n)
    allocated!(budget, charge)
    return text
end

function readfixed(d::Decoder, n::Int)
    checkvaluebytes(d.budget, n)
    bytesavailable(d, n) ||
        dataerror(d, "fixed of $n bytes exceeds the remaining $(remaining(d)) bytes")
    reserve!(d.budget, bytesbytes(n))
    out = Vector{UInt8}(undef, n)
    allocated!(d.budget, bytesbytes(n))
    copyto!(out, 1, d.buf, d.pos, n)
    d.pos += n
    return out
end

function skipfixed(d::Decoder, n::Int)
    checkvaluebytes(d.budget, n)
    bytesavailable(d, n) ||
        dataerror(d, "fixed of $n bytes exceeds the remaining $(remaining(d)) bytes")
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
    c >= 0 && (c <= d.budget.limits.max_block_count || throw(LimitError(:max_block_count, Int(c), d.budget.limits.max_block_count, :max_block_count, :decode)); return (Int(c), -1))
    c == typemin(Int64) && dataerror(d, "block count typemin(Int64)")
    c = -c
    c <= d.budget.limits.max_block_count || throw(LimitError(:max_block_count, Int(c), d.budget.limits.max_block_count, :max_block_count, :decode))
    size = readlen(d, d.budget.limits.max_datum_bytes, :max_datum_bytes)
    return (Int(c), size)
end

function enter!(d::Decoder)
    d.depth += 1
    checkdepth(d.budget, d.depth)
    return nothing
end

function leave!(d::Decoder)
    return (d.depth -= 1; nothing)
end

function skiplong(d::Decoder)
    readlong(d)
    return nothing
end

function skiplen(d::Decoder)
    n = readlen(d, d.budget.limits.max_bytes, :max_bytes)
    d.pos += n
    return nothing
end
