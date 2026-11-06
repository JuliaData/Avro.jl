# Prepared datum codecs and the one-shot conveniences built on them.

"""
    Avro.DatumReader(writer_schema; reader_schema=nothing, union_resolution=:spec, limits=Limits())
    Avro.DatumReader(writer_schema, T; kw...)

A prepared decoder for datums written with `writer_schema`, optionally resolved against
`reader_schema` and optionally producing the typed target `T`. Configuration is fixed after
construction and safe to share across tasks.

* `reader(bytes) -> value` decodes exactly one datum; trailing bytes are a `DataError`.
* `reader(bytes, pos) -> (value, nextpos)` decodes the datum that starts at byte `pos`.
* `reader(io) -> value` reads the rest of `io` (at most `limits.max_bytes`) as exactly one datum.

Positions are one-based byte offsets from the start of the input, including when its array indices
start elsewhere.
"""
mutable struct DatumReader{T}
    const writer::Schema
    const reader::Schema
    const node::TypedNode
    const limits::Limits
    @atomic scratch::Union{Nothing,Decoder{Vector{UInt8}}}   # one pooled decoder, taken and returned lock-free
end

function Base.show(io::IO, r::DatumReader{T}) where {T}
    return print(io, "Avro.DatumReader(", schemalabel(r.writer), T === Any ? "" : ", $T", ")")
end
function datumreaderplan(@nospecialize(writer::Schema), @nospecialize(reader::Union{Nothing,Schema}), union_resolution::Symbol)
    reader === nothing && return readplan(writer)
    return resolvingplan(writer, reader; union_resolution=union_resolution)
end

function DatumReader(@nospecialize(writer::Schema); kw...)
    return DatumReader(writer, Any; kw...)
end

function DatumReader(@nospecialize(writer::Schema), ::Type{T}; reader_schema::Union{Nothing,Schema}=nothing,
                     union_resolution::Symbol=:spec, limits::Limits=Limits()) where {T}
    reader = something(reader_schema, writer)
    node = typednode(T, reader, datumreaderplan(writer, reader_schema, union_resolution))
    return DatumReader{T}(writer, reader, node, limits, nothing)
end

const EMPTY_BYTES = UInt8[]   # shared and never written: what an idle decoder holds instead of a caller's bytes

function (r::DatumReader)(bytes::AbstractVector{UInt8})
    v, next = decodeat(r, bytes, 1)
    next == length(bytes) + 1 || throw(DataError("trailing bytes after the datum", next))
    return v
end

function (r::DatumReader)(bytes::AbstractVector{UInt8}, pos::Integer)
    return decodeat(r, bytes, Int(pos))
end

function (r::DatumReader)(io::IO)
    return r(sourcebytes(io, r.limits.max_bytes))
end

# The datum at `pos` and the position after it. A `Vector` reuses the pooled decoder; a failed call drops
# the decoder instead of returning it.
function decodeat(r::DatumReader{T}, bytes::Vector{UInt8}, pos::Int) where {T}
    d = @atomicswap r.scratch = nothing
    if d === nothing
        d = Decoder(bytes, r.limits; pos=pos)
    else
        reset!(d, bytes, pos, length(bytes))
    end
    v = decodenode(T, r.node, d)
    next = d.pos
    d.buf = EMPTY_BYTES
    @atomic r.scratch = d
    return (v, next)
end

function decodeat(r::DatumReader{T}, bytes::AbstractVector{UInt8}, pos::Int) where {T}
    d = Decoder(onebased(bytes), r.limits; pos=pos)
    return (decodenode(T, r.node, d), d.pos)
end

"""
    Avro.decode(writer_schema, src; reader_schema=nothing, union_resolution=:spec, limits=Limits())
    Avro.decode(writer_schema, src, T; kw...)
    Avro.decode(writer_schema, bytes, pos::Integer; kw...) -> (value, nextpos)

One-shot decoding of one datum from bytes (trailing bytes rejected), from `bytes` at `pos`, or from an
`IO`; `T` selects a typed target. Use a `DatumReader` for repeated work.
"""
function decode(@nospecialize(writer::Schema), src::Union{AbstractVector{UInt8},IO}; kw...)
    return DatumReader(writer; kw...)(src)
end

function decode(@nospecialize(writer::Schema), src::Union{AbstractVector{UInt8},IO}, ::Type{T}; kw...) where {T}
    return DatumReader(writer, T; kw...)(src)
end

function decode(@nospecialize(writer::Schema), bytes::AbstractVector{UInt8}, pos::Integer; kw...)
    return DatumReader(writer; kw...)(bytes, pos)
end

function decode(@nospecialize(writer::Schema), bytes::AbstractVector{UInt8}, pos::Integer, ::Type{T}; kw...) where {T}
    return DatumReader(writer, T; kw...)(bytes, pos)
end

"""
    Avro.DatumWriter(schema; limits=Limits())
    Avro.DatumWriter(schema, T; limits=Limits())

A prepared encoder that retains its plan and a reusable buffer, so it is single-owner:
`writer(x) -> Vector{UInt8}` returns a fresh owned buffer; `writer(io, x)` writes to `io` without
creating a result vector per call; `writer(e::Avro.Encoder, x)` appends to a caller's encoder. The typed
form takes datums of exactly type `T` (`T = Any` is the untyped form). Either form compiles its encoding
once per datum type, and encodes NamedTuples, structs, vectors and dictionaries of the types Avro encodes
directly through their types; other values encode by their run-time types. Reused buffers may still grow
when a larger value is written.
"""
mutable struct DatumWriter{T}
    const schema::Schema
    const plan::Plan
    const nodes::WriteNodes
    const encoder::Encoder
end

function Base.show(io::IO, w::DatumWriter{T}) where {T}
    return print(io, "Avro.DatumWriter(", schemalabel(w.schema), T === Any ? "" : ", $T", ")")
end

function DatumWriter(@nospecialize(schema::Schema); limits::Limits=Limits())
    return DatumWriter(schema, Any; limits=limits)
end

function DatumWriter(@nospecialize(schema::Schema), ::Type{T}; limits::Limits=Limits()) where {T}
    plan = writeplan(schema)
    nodes = WriteNodes(schema, plan)
    T === Any || writenode!(nodes, T)
    return DatumWriter{T}(schema, plan, nodes, Encoder(limits))
end

function (w::DatumWriter)(x)
    e = w.encoder
    try
        writeprepared!(w, e, x)
    catch
        reset!(e)
        rethrow()
    end
    return take!(e)
end

function (w::DatumWriter)(io::IO, x)
    e = w.encoder
    try
        writeprepared!(w, e, x)
        streamcall(Base.write, io, view(e.buf, 1:e.pos))
    finally
        reset!(e)
    end
    return nothing
end

# Appends from depth zero; a failure leaves the caller's encoder as it was.
function (w::DatumWriter)(e::Encoder, x)
    writeprepared!(w, e, x)
    return nothing
end

function writeprepared!(w::DatumWriter{T}, e::Encoder, x::X) where {T,X}
    T === Any || X === T || preparedtypeerror(w)
    encodetyped!(X, writenode!(w.nodes, X), e, x, w.plan, w.schema)
    return nothing
end

@noinline function preparedtypeerror(w::DatumWriter{T}) where {T}
    throw(EncodeError(string("prepared writer expects a datum of exact type ", typestring(T)), "\$", w.schema))
end

"""
    Avro.encode(schema, x; limits=Limits()) -> Vector{UInt8}
    Avro.encode(x; limits=Limits()) -> Vector{UInt8}          # conventional schema `Avro.schema(x)`

One-shot encoding (builds a plan per call; use `DatumWriter` for repeated use).
"""
function encode(@nospecialize(schema::Schema), x; limits::Limits=Limits())
    e = Encoder(limits)
    encodeonce!(e, schema, x)
    return take!(e)
end

function encode(x; limits::Limits=Limits())
    return encode(schema(x), x; limits=limits)
end

"""
    Avro.encode!(io::IO, schema, x; limits=Limits())
    Avro.encode!(e::Avro.Encoder, schema, x)

One-shot encoding that appends one datum to the caller's stream or encoder; a failed append leaves the
encoder's bytes as they were. Use `DatumWriter` for repeated work.
"""
function encode!(io::IO, @nospecialize(schema::Schema), x; limits::Limits=Limits())
    e = Encoder(limits)
    encodeonce!(e, schema, x)
    streamcall(Base.write, io, view(e.buf, 1:e.pos))
    return nothing
end

function encode!(e::Encoder, @nospecialize(schema::Schema), x)
    encodeonce!(e, schema, x)
    return nothing
end

function encodeonce!(e::Encoder, @nospecialize(schema::Schema), x::T) where {T}
    plan = writeplan(schema)
    encodetyped!(T, writenode(T, schema, plan), e, x, plan, schema)
    return nothing
end
