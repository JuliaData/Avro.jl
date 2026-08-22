# Byte-level encoder (plan §4.3): a growable buffer grown only by reserved exact-capacity replacement.

"""
    Encoder(budget; capacity=256)

A growable output buffer with a write position. Growth allocates a replacement `Vector{UInt8}` of exact
capacity after reserving it (old and new storage charged during the copy); never `resize!`/`push!`.
"""
mutable struct Encoder
    buf::Vector{UInt8}
    pos::Int          # number of bytes written
    const budget::Budget
    depth::Int
    credited::Int     # bytes already credited to the work rule (plan §4.3: the output is the denominator)
end

function Encoder(budget::Budget; capacity::Int=256)
    reserve!(budget, bytesbytes(capacity))
    return Encoder(Vector{UInt8}(undef, capacity), 0, budget, 0, 0)
end

Base.length(e::Encoder) = e.pos
capacity(e::Encoder) = length(e.buf)

"""
    ensureroom!(e, n)

Make room for `n` more bytes; the replacement capacity is reserved before allocation and the old
buffer's reservation released after the copy.
"""
function ensureroom!(e::Encoder, n::Int)
    need = checked_add(e.pos, n)
    need <= length(e.buf) && return nothing
    newcap = max(need, min(2 * length(e.buf), length(e.buf) + (64 << 20)))
    newcap <= e.budget.limits.max_datum_bytes + e.budget.limits.max_block_bytes + (1 << 20) ||
        throw(LimitError(:max_datum_bytes, newcap, e.budget.limits.max_datum_bytes, :max_datum_bytes, :encode))
    reserve!(e.budget, bytesbytes(newcap))
    nb = Vector{UInt8}(undef, newcap)
    copyto!(nb, 1, e.buf, 1, e.pos)
    release!(e.budget, bytesbytes(length(e.buf)))
    e.buf = nb
    return nothing
end

@inline function writebyte!(e::Encoder, b::UInt8)
    ensureroom!(e, 1)
    e.pos += 1
    @inbounds e.buf[e.pos] = b
    return nothing
end

"""
    writelong!(e, x::Int64)

Zig-zag varint (up to 10 bytes).
"""
function writelong!(e::Encoder, x::Int64)
    ensureroom!(e, 10)
    v = (reinterpret(UInt64, x) << 1) ⊻ reinterpret(UInt64, x >> 63)
    p = e.pos
    buf = e.buf
    while v >= 0x80
        p += 1
        @inbounds buf[p] = UInt8(v & 0x7f) | 0x80
        v >>= 7
    end
    p += 1
    @inbounds buf[p] = UInt8(v)
    e.pos = p
    return nothing
end

writeint!(e::Encoder, x::Int32) = writelong!(e, Int64(x))
writebool!(e::Encoder, x::Bool) = writebyte!(e, x ? 0x01 : 0x00)

function writefloat!(e::Encoder, x::Float32)
    ensureroom!(e, 4)
    v = reinterpret(UInt32, x)
    p = e.pos
    @inbounds for i in 0:3
        e.buf[p + 1 + i] = UInt8((v >> (8 * i)) & 0xff)
    end
    e.pos = p + 4
    return nothing
end

function writedouble!(e::Encoder, x::Float64)
    ensureroom!(e, 8)
    v = reinterpret(UInt64, x)
    p = e.pos
    @inbounds for i in 0:7
        e.buf[p + 1 + i] = UInt8((v >> (8 * i)) & 0xff)
    end
    e.pos = p + 8
    return nothing
end

function writeraw!(e::Encoder, bytes::AbstractVector{UInt8}, from::Int=1, n::Int=length(bytes) - from + 1)
    ensureroom!(e, n)
    copyto!(e.buf, e.pos + 1, bytes, from, n)
    e.pos += n
    return nothing
end

"""
    writebytes!(e, bytes) / writestring!(e, s)

Length-prefixed bytes/string (the string's UTF-8 validity is the caller's check).
"""
function writebytes!(e::Encoder, bytes::AbstractVector{UInt8})
    n = length(bytes)
    n <= e.budget.limits.max_bytes || throw(LimitError(:max_bytes, n, e.budget.limits.max_bytes, :max_bytes, :encode))
    writelong!(e, Int64(n))
    writeraw!(e, bytes, 1, n)
    return nothing
end

function writestring!(e::Encoder, s::AbstractString)
    n = sizeof(s)
    n <= e.budget.limits.max_bytes || throw(LimitError(:max_bytes, n, e.budget.limits.max_bytes, :max_bytes, :encode))
    writelong!(e, Int64(n))
    writeraw!(e, codeunits(s), 1, n)
    return nothing
end

"""
    take!(e) -> Vector{UInt8}

The written bytes as an exactly sized, caller-owned vector; the encoder is reset.
"""
function Base.take!(e::Encoder)
    out = e.buf[1:e.pos]
    e.pos = 0
    e.credited = 0
    return out
end

function reset!(e::Encoder)
    e.pos = 0
    e.depth = 0
    e.credited = 0
    return e
end

function enter!(e::Encoder)
    e.depth += 1
    checkdepth(e.budget, e.depth)
    return nothing
end

leave!(e::Encoder) = (e.depth -= 1; nothing)
