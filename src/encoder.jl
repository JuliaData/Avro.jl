# Byte-level encoder (plan §4.3): a growable buffer grown only by reserved exact-capacity replacement.

const ENCODE_PATH_FIELD = UInt8(1)
const ENCODE_PATH_INDEX = UInt8(2)
const ENCODE_PATH_KEY = UInt8(3)

"One allocation-free component in the current value path."
struct EncodePathSegment
    kind::UInt8
    name::Union{Nothing,AbstractString,Symbol}
    index::Int
end

const EncodePath = BuildBuf{EncodePathSegment}
const EMPTY_ENCODE_PATH_SEGMENT = EncodePathSegment(0, nothing, 0)

"""
    Encoder(budget; capacity=256)

A growable output buffer with a write position. Growth allocates a replacement `Vector{UInt8}` of exact
capacity after reserving it (old and new storage charged during the copy); never `resize!`/`push!`.
"""
mutable struct Encoder
    buf::Vector{UInt8}
    pos::Int          # number of bytes written
    budget::Budget    # active operation limits and work accounting
    owner::Budget     # owns the retained buffer allocation (differs during prepared append calls)
    depth::Int
    credited::Int     # bytes already credited to the work rule (plan §4.3: the output is the denominator)
    expected::Union{Nothing,Schema}
    path::EncodePath
end

function Encoder(budget::Budget; capacity::Int=256)
    reserve!(budget, bytesbytes(capacity))
    buf = Vector{UInt8}(undef, capacity)
    allocated!(budget, bytesbytes(capacity))
    path = EncodePath(budget, min(budget.limits.max_depth, 16))
    e = Encoder(buf, 0, budget, budget, 0, 0, nothing, path)
    return e
end

function encodepathstorage(e::Encoder)
    return vectorbytes(EncodePathSegment, length(e.path.data)) + shellbytes(EncodePath)
end

function encoderstorage(e::Encoder)
    return checked_add(bytesbytes(length(e.buf)), encodepathstorage(e))
end

function Base.length(e::Encoder)
    return e.pos
end

function capacity(e::Encoder)
    return length(e.buf)
end

"""
    ensureroom!(e, n)

Make room for `n` more bytes; the replacement capacity is reserved before allocation and the old
buffer's reservation released after the copy.
"""
function ensureroom!(e::Encoder, n::Int)
    n >= 0 || throw(ArgumentError("encoder growth must be non-negative"))
    n > typemax(Int) - e.pos &&
        throw(limiterror(e.budget, :max_datum_bytes, typemax(Int),
                         e.budget.limits.max_datum_bytes))
    need = e.pos + n
    need <= length(e.buf) && return nothing
    doubled = satadd(length(e.buf), length(e.buf))
    incremented = satadd(length(e.buf), 64 << 20)
    newcap = max(need, min(doubled, incremented))
    capacitylimit = satadd(satadd(e.budget.limits.max_datum_bytes,
                                  e.budget.limits.max_block_bytes), 1 << 20)
    newcap <= capacitylimit ||
        throw(LimitError(:max_datum_bytes, newcap, e.budget.limits.max_datum_bytes, :max_datum_bytes, :encode))
    active = e.budget
    owner = e.owner
    newbytes = bytesbytes(newcap)
    oldbytes = bytesbytes(length(e.buf))
    activecheckpoint = budgetcheckpoint(active)
    ownercheckpoint = active === owner ? activecheckpoint : budgetcheckpoint(owner)
    try
        active === owner || reserve!(active, newbytes)
        reserve!(owner, newbytes)
    catch
        rollbackreservations!(active, activecheckpoint)
        active === owner || rollbackreservations!(owner, ownercheckpoint)
        rethrow()
    end
    nb = try
        value = Vector{UInt8}(undef, newcap)
        allocated!(owner, newbytes)
        active === owner || allocated!(active, newbytes)
        value
    catch
        rollbackreservations!(active, activecheckpoint)
        active === owner || rollbackreservations!(owner, ownercheckpoint)
        rethrow()
    end
    copyto!(nb, 1, e.buf, 1, e.pos)
    e.buf = nb                                        # the old buffer is unreachable only after the rebind
    release!(owner, oldbytes)
    active === owner || release!(active, oldbytes)
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

function writeint!(e::Encoder, x::Int32)
    return writelong!(e, Int64(x))
end

function writebool!(e::Encoder, x::Bool)
    return writebyte!(e, x ? 0x01 : 0x00)
end

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

function writesymbol!(e::Encoder, symbol::Symbol)
    n = sizeof(symbol)
    n <= e.budget.limits.max_bytes ||
        throw(LimitError(:max_bytes, n, e.budget.limits.max_bytes, :max_bytes, :encode))
    writelong!(e, Int64(n))
    ensureroom!(e, n)
    source = Base.unsafe_convert(Ptr{UInt8}, symbol)
    GC.@preserve symbol unsafe_copyto!(pointer(e.buf, e.pos + 1), source, n)
    e.pos += n
    return nothing
end

function writechar!(e::Encoder, char::Char)
    n = ncodeunits(char)
    n <= e.budget.limits.max_bytes ||
        throw(LimitError(:max_bytes, n, e.budget.limits.max_bytes, :max_bytes, :encode))
    writelong!(e, Int64(n))
    value = UInt32(char)
    if n == 1
        writebyte!(e, UInt8(value))
    elseif n == 2
        writebyte!(e, 0xc0 | UInt8(value >> 6))
        writebyte!(e, 0x80 | UInt8(value & 0x3f))
    elseif n == 3
        writebyte!(e, 0xe0 | UInt8(value >> 12))
        writebyte!(e, 0x80 | UInt8((value >> 6) & 0x3f))
        writebyte!(e, 0x80 | UInt8(value & 0x3f))
    else
        writebyte!(e, 0xf0 | UInt8(value >> 18))
        writebyte!(e, 0x80 | UInt8((value >> 12) & 0x3f))
        writebyte!(e, 0x80 | UInt8((value >> 6) & 0x3f))
        writebyte!(e, 0x80 | UInt8(value & 0x3f))
    end
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
    e.expected = nothing
    emptyencodepath!(e)
    return out
end

function reset!(e::Encoder)
    e.pos = 0
    e.depth = 0
    e.credited = 0
    e.expected = nothing
    emptyencodepath!(e)
    return e
end

function emptyencodepath!(e::Encoder)
    while e.path.len > 0
        e.path.data[e.path.len] = EMPTY_ENCODE_PATH_SEGMENT
        e.path.len -= 1
    end
    return nothing
end

function enter!(e::Encoder)
    e.depth += 1
    checkdepth(e.budget, e.depth)
    return nothing
end

function leave!(e::Encoder)
    return (e.depth -= 1; nothing)
end
