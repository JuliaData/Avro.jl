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
mutable struct DatumReader{T,P}
    const writer::Schema
    const reader::Schema
    const plan::P                                         # concrete, so prepared calls dispatch statically
    const limits::Limits
    @atomic scratch::Union{Nothing,Decoder{Vector{UInt8}}}   # one pooled decoder, taken and returned lock-free
end

function Base.show(io::IO, r::DatumReader{T}) where {T}
    return print(io, "Avro.DatumReader(", schemalabel(r.writer), T === Any ? "" : ", $T", ")")
end
function datumreaderplan(writer::Schema, reader::Union{Nothing,Schema}, union_resolution::Symbol)
    reader === nothing && return readplan(writer)
    return resolvingplan(writer, reader; union_resolution=union_resolution)
end

function DatumReader(writer::Schema; kw...)
    return DatumReader(writer, Any; kw...)
end

function DatumReader(writer::Schema, ::Type{T}; reader_schema::Union{Nothing,Schema}=nothing,
                     union_resolution::Symbol=:spec, limits::Limits=Limits()) where {T}
    reader = something(reader_schema, writer)
    plan = typedplan(T, reader, datumreaderplan(writer, reader_schema, union_resolution))
    return DatumReader{T,typeof(plan)}(writer, reader, plan, limits, nothing)
end

const EMPTY_BYTES = UInt8[]   # shared and never written: what an idle decoder holds instead of a caller's bytes

function (r::DatumReader)(bytes::AbstractVector{UInt8})
    v, next = decodeat(r, bytes, 1)
    next == length(bytes) + 1 || throw(DataError("trailing bytes after the datum", next))
    return finishtyped(r.plan, v)
end

function (r::DatumReader)(bytes::AbstractVector{UInt8}, pos::Integer)
    v, next = decodeat(r, bytes, Int(pos))
    return (finishtyped(r.plan, v), next)
end

function (r::DatumReader)(io::IO)
    return r(sourcebytes(io, r.limits.max_bytes))
end

# The datum at `pos` and the position after it. A `Vector` reuses the pooled decoder; a failed call drops
# the decoder instead of returning it.
function decodeat(r::DatumReader, bytes::Vector{UInt8}, pos::Int)
    d = @atomicswap r.scratch = nothing
    if d === nothing
        d = Decoder(bytes, r.limits; pos=pos)
    else
        reset!(d, bytes, pos, length(bytes))
    end
    v = decoderoot(r.plan, d)
    next = d.pos
    d.buf = EMPTY_BYTES
    @atomic r.scratch = d
    return (v, next)
end

function decodeat(r::DatumReader, bytes::AbstractVector{UInt8}, pos::Int)
    d = Decoder(onebased(bytes), r.limits; pos=pos)
    return (decoderoot(r.plan, d), d.pos)
end

"""
    Avro.decode(writer_schema, src; reader_schema=nothing, union_resolution=:spec, limits=Limits())
    Avro.decode(writer_schema, src, T; kw...)
    Avro.decode(writer_schema, bytes, pos::Integer; kw...) -> (value, nextpos)

One-shot decoding of one datum from bytes (trailing bytes rejected), from `bytes` at `pos`, or from an
`IO`; `T` selects a typed target. Use a `DatumReader` for repeated work.
"""
function decode(writer::Schema, src::Union{AbstractVector{UInt8},IO}; kw...)
    return DatumReader(writer; kw...)(src)
end

function decode(writer::Schema, src::Union{AbstractVector{UInt8},IO}, ::Type{T}; kw...) where {T}
    return DatumReader(writer, T; kw...)(src)
end

function decode(writer::Schema, bytes::AbstractVector{UInt8}, pos::Integer; kw...)
    return DatumReader(writer; kw...)(bytes, pos)
end

function decode(writer::Schema, bytes::AbstractVector{UInt8}, pos::Integer, ::Type{T}; kw...) where {T}
    return DatumReader(writer, T; kw...)(bytes, pos)
end

# The datum type parameter of a `DatumWriter` built without one.
struct UnfixedDatumWriter end

"""
    Avro.DatumWriter(schema; limits=Limits())
    Avro.DatumWriter(schema, T; limits=Limits())

A prepared encoder that retains its plan and a reusable buffer, so it is single-owner:
`writer(x) -> Vector{UInt8}` returns a fresh owned buffer; `writer(io, x)` writes to `io` without
creating a result vector per call; `writer(e::Avro.Encoder, x)` appends to a caller's encoder. The typed
form fixes the datum type `T`, which is the fastest path when `T` is a NamedTuple or struct whose fields
align with the record schema. Reused buffers may still grow when a larger value is written.
"""
mutable struct DatumWriter{P<:Plan,T,FP}
    const schema::Schema
    const plan::P
    const encoder::Encoder
    const fixedplans::FP          # the typed form's aligned plans (`nothing` when untyped or not aligned)
end

function Base.show(io::IO, w::DatumWriter{P,T}) where {P,T}
    return print(io, "Avro.DatumWriter(", schemalabel(w.schema), T === UnfixedDatumWriter ? "" : ", $T", ")")
end

function DatumWriter(schema::Schema; limits::Limits=Limits())
    plan = writeplan(schema)
    return DatumWriter{typeof(plan),UnfixedDatumWriter,Nothing}(schema, plan, Encoder(limits), nothing)
end

function DatumWriter(schema::Schema, ::Type{T}; limits::Limits=Limits()) where {T}
    plan = writeplan(schema)
    fixed = alignedplans(plan, T)
    return DatumWriter{typeof(plan),T,typeof(fixed)}(schema, plan, Encoder(limits), fixed)
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
        Base.write(io, view(e.buf, 1:e.pos))
    finally
        reset!(e)
    end
    return nothing
end

function (w::DatumWriter)(e::Encoder, x)
    return appenddatum!(() -> writeprepared!(w, e, x), e)
end

function writeprepared!(w::DatumWriter{P,T,FP}, e::Encoder, x) where {P,T,FP}
    T === UnfixedDatumWriter || typeof(x) === T ||
        throw(EncodeError(string("prepared writer expects a datum of exact type ", T), "\$", w.schema))
    FP === Nothing && return encodedatum!(w.plan, e, x, w.schema)
    try
        encodealignedfast!(w.fixedplans, e, x)
    catch err
        err isa EncodeError && replayaligned!(w.plan, Encoder(e.limits), x, w.schema::RecordSchema)
        rethrow()
    end
    return nothing
end

# Encode one datum onto a caller's encoder from depth zero; a failure truncates what it wrote.
function appenddatum!(f::F, e::Encoder) where {F}
    start, depth, values = e.pos, e.depth, e.values
    e.depth = 0
    try
        f()
    catch
        e.pos = start
        e.values = values
        rethrow()
    finally
        e.depth = depth
    end
    return nothing
end

"""
    Avro.encode(schema, x; limits=Limits()) -> Vector{UInt8}
    Avro.encode(x; limits=Limits()) -> Vector{UInt8}          # conventional schema `Avro.schema(x)`

One-shot encoding (builds a plan per call; use `DatumWriter` for repeated use).
"""
function encode(schema::Schema, x; limits::Limits=Limits())
    e = Encoder(limits)
    encodedatum!(writeplan(schema), e, x, schema)
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
function encode!(io::IO, schema::Schema, x; limits::Limits=Limits())
    e = Encoder(limits)
    encodedatum!(writeplan(schema), e, x, schema)
    Base.write(io, view(e.buf, 1:e.pos))
    return nothing
end

function encode!(e::Encoder, schema::Schema, x)
    plan = writeplan(schema)
    return appenddatum!(() -> encodedatum!(plan, e, x, schema), e)
end
