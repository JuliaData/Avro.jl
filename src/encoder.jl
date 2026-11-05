# Byte-level encoder: a growable output buffer with a write position, plus the diagnostic value path
# that `EncodeError`s report.

const ENCODE_PATH_FIELD = UInt8(1)
const ENCODE_PATH_INDEX = UInt8(2)
const ENCODE_PATH_KEY = UInt8(3)

"One component of the value path being encoded (a record field, an array index or a map key)."
struct EncodePathSegment
    kind::UInt8
    name::Union{Nothing,AbstractString,Symbol}
    index::Int
end

"""
    Encoder(limits=Limits(); capacity=256)

A growable output buffer with a write position. `values` counts the values encoded since the last
`reset!`/`take!` (container writers flush blocks on it).
"""
mutable struct Encoder
    buf::Vector{UInt8}
    pos::Int
    depth::Int
    values::Int
    const limits::Limits
    expected::Union{Nothing,Schema}
    const path::Vector{EncodePathSegment}
end

function Encoder(limits::Limits=Limits(); capacity::Int=256)
    return Encoder(Vector{UInt8}(undef, capacity), 0, 0, 0, limits, nothing, EncodePathSegment[])
end

function Base.length(e::Encoder)
    return e.pos
end

"Make room for `n` more bytes (growth doubles, in steps of at most 64 MiB)."
function ensureroom!(e::Encoder, n::Int)
    n >= 0 || throw(ArgumentError("encoder growth must be non-negative"))
    need = checked_add(e.pos, n)
    need <= length(e.buf) && return nothing
    newcap = max(need, min(satadd(length(e.buf), length(e.buf)), satadd(length(e.buf), 64 * MiB)))
    resize!(e.buf, newcap)
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
    copyto!(e.buf, e.pos + 1, bytes, firstindex(bytes) + from - 1, n)
    e.pos += n
    return nothing
end

"""
    writebytes!(e, bytes) / writestring!(e, s)

Length-prefixed bytes/string (the string's UTF-8 validity is the caller's check).
"""
function writebytes!(e::Encoder, bytes::AbstractVector{UInt8})
    n = length(bytes)
    writelong!(e, Int64(n))
    writeraw!(e, bytes, 1, n)
    return nothing
end

function writestring!(e::Encoder, s::AbstractString)
    n = sizeof(s)
    writelong!(e, Int64(n))
    s isa Union{String,SubString{String}} || return writeraw!(e, codeunits(s), 1, n)
    ensureroom!(e, n)
    buf = e.buf
    # `copyto!` from `codeunits(s)` would hash the whole string to rule out aliasing.
    GC.@preserve s buf unsafe_copyto!(pointer(buf, e.pos + 1), pointer(s), n)
    e.pos += n
    return nothing
end

# A `DataString` (from a string column `Avro.Table` read) copies a long string from its buffer in one move, and
# stores a short one's 12 inline payload bytes in two words; the bytes past its length are written over next.
function writestring!(e::Encoder, s::DataString)
    n = ncodeunits(s)
    writelong!(e, Int64(n))
    p = s.p
    if n > DataStrings.INLINE_MAX
        ensureroom!(e, n)
        data = s.data
        buf = e.buf
        GC.@preserve data buf unsafe_copyto!(pointer(buf, e.pos + 1), pointer(data, DataStrings.payloadpos(p)), n)
    else
        ensureroom!(e, DataStrings.INLINE_MAX)
        buf = e.buf
        GC.@preserve buf begin
            q = pointer(buf, e.pos + 1)
            unsafe_store!(Ptr{UInt32}(q), htol((p.a >> 32) % UInt32))
            unsafe_store!(Ptr{UInt64}(q + 4), htol(p.b))
        end
    end
    e.pos += n
    return nothing
end

function writechar!(e::Encoder, char::Char)
    n = ncodeunits(char)
    writelong!(e, Int64(n))
    ensureroom!(e, n)
    e.pos = writecodepoint!(e.buf, e.pos + 1, UInt32(char)) - 1
    return nothing
end

"""
    take!(e) -> Vector{UInt8}

The written bytes as an exactly sized, caller-owned vector; the encoder is reset.
"""
function Base.take!(e::Encoder)
    out = e.buf[1:e.pos]
    reset!(e)
    return out
end

function Base.show(io::IO, e::Encoder)
    return print(io, "Avro.Encoder(", e.pos, " bytes)")
end

function reset!(e::Encoder)
    e.pos = 0
    e.depth = 0
    e.values = 0
    e.expected = nothing
    empty!(e.path)
    return e
end

@inline function countvalue!(e::Encoder)
    e.values += 1
    return nothing
end

function enter!(e::Encoder)
    depth = e.depth + 1
    depth <= e.limits.max_depth || throw(LimitError(:max_depth, depth, e.limits.max_depth))
    e.depth = depth
    return nothing
end

function leave!(e::Encoder)
    e.depth -= 1
    return nothing
end

# ---- the diagnostic value path -----------------------------------------------------------------------

function pushencodepath!(e::Encoder, kind::UInt8, name::Union{Nothing,AbstractString,Symbol}, index::Int=0)
    push!(e.path, EncodePathSegment(kind, name, index))
    return nothing
end

function popencodepath!(e::Encoder)
    pop!(e.path)
    return nothing
end

const DIAGNOSTIC_NAME_PREFIX_BYTES = 96

"Untrusted text for an error message: truncated to a prefix with an ellipsis when long."
function diagnostictext(key::Union{AbstractString,Symbol})
    text = String(key)
    sizeof(text) <= DIAGNOSTIC_NAME_PREFIX_BYTES && return text
    return string(text[1:thisind(text, DIAGNOSTIC_NAME_PREFIX_BYTES)], "…")
end

"Untrusted text for an error message as a quoted, escaped JSON string."
function quoted(s::Union{AbstractString,Symbol})
    return sprint(escapejson, diagnostictext(s))
end

"The JSON-path form (`\$.field[3][\"key\"]`) of the value currently being encoded."
function formatencodepath(e::Encoder)
    io = IOBuffer()
    print(io, '$')
    for segment in e.path
        if segment.kind == ENCODE_PATH_FIELD
            print(io, '.', segment.name::String)
        elseif segment.kind == ENCODE_PATH_INDEX
            print(io, '[', segment.index, ']')
        else
            print(io, '[')
            escapejson(io, diagnostictext(segment.name::Union{AbstractString,Symbol}))
            print(io, ']')
        end
    end
    return String(take!(io))
end
