# Prepared codecs and one-shot conveniences (plan §4.5, §5.2).

"""
    Avro.DatumReader(writer_schema; reader_schema=nothing, union_resolution=:spec, limits=Limits(), validate=:strict, names=Avro.DEFAULT_ADMISSION)
    Avro.DatumReader(writer_schema, T; kw...)

A prepared decoder for datums written with `writer_schema`, optionally resolved against
`reader_schema` (plan §4.7) and optionally producing the typed target `T`. Immutable after construction
and safe to share across tasks (every call allocates its own decoder state and budget):
`reader(bytes)`, `reader(bytes, pos) -> (value, nextpos)`, `reader(io)`.
"""
mutable struct DatumReader{T,P}
    const writer::Schema
    const reader::Schema
    const plan::P               # the concrete plan type: prepared calls devirtualize end to end (§10.2)
    const limits::Limits
    const validate::Symbol
    const names::Union{SymbolAdmission,Symbol}
    @atomic scratch::Any        # one pooled per-call Decoder (its budget inside), shared lock-free
end

function DatumReader(writer::Schema; reader_schema::Union{Nothing,Schema}=nothing, union_resolution::Symbol=:spec,
                     limits::Limits=Limits(), validate::Symbol=:strict, names=DEFAULT_ADMISSION)
    validate in (:strict, :fast) || throw(ArgumentError("validate must be :strict or :fast"))
    effective = reader_schema === nothing ? writer : reader_schema
    plan = reader_schema === nothing ? withbudget(limits) do b; readplan(writer; budget=b); end :
           resolvingplan(writer, reader_schema; union_resolution=union_resolution, limits=limits)
    return DatumReader{Nothing,typeof(plan)}(writer, effective, plan, limits, validate, admission(names), nothing)
end

function DatumReader(writer::Schema, ::Type{T}; kw...) where {T}
    r = DatumReader(writer; kw...)
    plan = typedplan(T, r.reader, r.plan, r.limits)
    return DatumReader{T,typeof(plan)}(r.writer, r.reader, plan, r.limits, r.validate, r.names, nothing)
end

"""
    reader(bytes) -> value
    reader(bytes, pos) -> (value, nextpos)
    reader(io) -> value

Decode one datum (the byte form rejects trailing bytes; the `IO` form reads at most
`max_datum_bytes + 1` bytes and errors if bytes remain).
"""
function (r::DatumReader{T,P})(bytes::AbstractVector{UInt8}) where {T,P}
    v, next = r(bytes, 1)
    next == length(bytes) + 1 || throw(DataError("trailing bytes after the datum", next))
    return v
end

function (r::DatumReader{T,P})(bytes::AbstractVector{UInt8}, pos::Integer) where {T,P}
    if bytes isa Vector{UInt8} && length(bytes) <= r.limits.max_datum_bytes
        sc = @atomicswap(r.scratch = nothing)          # pooled per-call state: ≤ 1 allocation per decode (§10.2)
        d = sc isa Decoder{Vector{UInt8}} ? sc : Decoder(UInt8[], Budget(r.limits); validate=r.validate)
        recycle = false
        try
            resetbudget!(d.budget)
            d.buf = bytes
            d.pos = Int(pos)
            d.stop = length(bytes)
            d.depth = 0
            1 <= pos <= length(bytes) + 1 || throw(ArgumentError("position $pos out of range"))
            addinput!(d.budget, length(bytes) - Int(pos) + 1)
            v = decodetyped(T, r.plan, d, r.names)
            next = d.pos
            recycle = true
        finally
            d.buf = UInt8[]                            # never pin the caller's bytes from the pool
            close!(d.budget)
            recycle && @atomicswap(r.scratch = d)     # a failed call drops its scratch instead
        end
        return (finishtyped(r.plan, v, r.names), next)
    end
    v, next = withbudget(r.limits) do budget
        buf = sourcebytes(bytes, r.limits.max_datum_bytes, budget, DataError)
        1 <= pos <= length(buf) + 1 || throw(ArgumentError("position $pos out of range"))
        addinput!(budget, length(buf) - pos + 1)
        d = Decoder(buf, budget; pos=Int(pos), validate=r.validate)
        return (decodetyped(T, r.plan, d, r.names), d.pos)
    end
    return (finishtyped(r.plan, v, r.names), next)     # semantic conversion runs in caller space
end

function (r::DatumReader{T,P})(io::IO) where {T,P}
    v = withbudget(r.limits) do budget
        buf = sourcebytes(io, r.limits.max_datum_bytes, budget, DataError)
        length(buf) <= r.limits.max_datum_bytes || throw(LimitError(:max_datum_bytes, length(buf), r.limits.max_datum_bytes, :max_datum_bytes, :decode))
        addinput!(budget, length(buf))
        d = Decoder(buf, budget; validate=r.validate)
        v = decodetyped(T, r.plan, d, r.names)
        d.pos == length(buf) + 1 || throw(DataError("trailing bytes after the datum", d.pos))
        return v
    end
    return finishtyped(r.plan, v, r.names)
end

decodetyped(::Type{Nothing}, plan::ReadPlan, d::Decoder, names) = decode(plan, d)
decodetyped(::Type{T}, plan::TypedPlan, d::Decoder, names) where {T} = decodetyped(plan, d, names)

"""
    Avro.DatumWriter(schema; limits=Limits())

A prepared encoder with a reusable buffer (single-owner): `writer(x) -> Vector{UInt8}` returns a fresh
owned buffer; `writer(enc_or_io, x)` appends to the caller's encoder or stream.
"""
mutable struct DatumWriter{P<:WritePlan,FP}
    const schema::Schema
    const plan::P               # concrete plan type (§10.2)
    const limits::Limits
    encoder::Union{Nothing,Encoder}
    fasttype::Any               # single-entry aligned-NamedTuple cache (single-owner like `encoder`)
    fastplans::Any
    const fixedplans::FP        # the typed form's aligned tuple, concrete (`nothing` when untyped)
end

"""
    Avro.DatumWriter(schema; limits=Limits())
    Avro.DatumWriter(schema, T; limits=Limits())

The typed form fixes the datum type `T` at construction; when `T` is a NamedTuple aligned with the
record schema, prepared encodes run fully monomorphized (the §10.2 zero-allocation kernel).
"""
function DatumWriter(schema::Schema; limits::Limits=Limits())
    plan = withbudget(limits; direction=:encode) do b
        writeplan(schema; budget=b)
    end
    return DatumWriter{typeof(plan),Nothing}(schema, plan, limits, nothing, nothing, nothing, nothing)
end

function DatumWriter(schema::Schema, ::Type{T}; limits::Limits=Limits()) where {T}
    plan = withbudget(limits; direction=:encode) do b
        writeplan(schema; budget=b)
    end
    fp = alignedplans(plan, T)
    return DatumWriter{typeof(plan),typeof(fp)}(schema, plan, limits, nothing, nothing, nothing, fp)
end

function fastdatumplans(w::DatumWriter, @nospecialize(x))
    x isa NamedTuple || return nothing
    w.fasttype === typeof(x) && return w.fastplans
    fp = alignedplans(w.plan, typeof(x))
    w.fasttype = typeof(x)
    w.fastplans = fp
    return fp
end

function (w::DatumWriter)(x)
    return withbudget(w.limits; direction=:encode) do budget
        e = Encoder(budget)
        encodedatum!(w.plan, e, x)
        take!(e)
    end
end

function (w::DatumWriter)(io::IO, x)
    return withbudget(w.limits; direction=:encode) do budget
        e = Encoder(budget)
        encodedatum!(w.plan, e, x)
        Base.write(io, view(e.buf, 1:e.pos))
        nothing
    end
end

function (w::DatumWriter{P,FP})(e::Encoder, x) where {P,FP}
    if FP !== Nothing && x isa NamedTuple
        encodealigned!(w.fixedplans, e, x)             # fully concrete: the zero-allocation kernel
        return nothing
    end
    fp = fastdatumplans(w, x)
    fp === nothing ? encodedatum!(w.plan, e, x) : encodealigned!(fp, e, x)
    return nothing
end

function encodedatum!(plan::WritePlan, e::Encoder, x)
    start = e.pos
    encode(plan, e, x)
    n = e.pos - start
    n <= e.budget.limits.max_datum_bytes || throw(LimitError(:max_datum_bytes, n, e.budget.limits.max_datum_bytes, :max_datum_bytes, :encode))
    return nothing
end

"""
    Avro.encode(schema, x; limits=Limits()) -> Vector{UInt8}
    Avro.encode(x; limits=Limits()) -> Vector{UInt8}          # conventional schema `Avro.schema(x)`

One-shot encoding (builds a writer per call; use `DatumWriter` for repeated use).
"""
encode(schema::Schema, x; limits::Limits=Limits()) = DatumWriter(schema; limits=limits)(x)
encode(x; limits::Limits=Limits()) = encode(schema(x; limits=limits), x; limits=limits)

"""
    Avro.encode!(enc_or_io, schema, x; limits=Limits())

One-shot encoding that appends one datum to the caller's encoder or stream. Use `DatumWriter` for
repeated work.
"""
encode!(io::IO, schema::Schema, x; limits::Limits=Limits()) = DatumWriter(schema; limits=limits)(io, x)
encode!(e::Encoder, schema::Schema, x; limits::Limits=Limits()) = DatumWriter(schema; limits=limits)(e, x)

"""
    Avro.decode(writer_schema, src; reader_schema=nothing, union_resolution=:spec, limits=Limits(), validate=:strict, names=…)
    Avro.decode(writer_schema, src, T; kw...)
    Avro.decode(writer_schema, bytes, pos::Integer; kw...) -> (value, nextpos)

One-shot decoding of one datum from bytes (trailing bytes rejected), from `bytes` at `pos`, or from an
`IO`.
"""
decode(writer::Schema, src::AbstractVector{UInt8}; kw...) = DatumReader(writer; kw...)(src)
decode(writer::Schema, src::IO; kw...) = DatumReader(writer; kw...)(src)
decode(writer::Schema, src::AbstractVector{UInt8}, pos::Integer; kw...) = DatumReader(writer; kw...)(src, pos)
decode(writer::Schema, src::AbstractVector{UInt8}, ::Type{T}; kw...) where {T} = DatumReader(writer, T; kw...)(src)
decode(writer::Schema, src::IO, ::Type{T}; kw...) where {T} = DatumReader(writer, T; kw...)(src)
decode(writer::Schema, src::AbstractVector{UInt8}, pos::Integer, ::Type{T}; kw...) where {T} = DatumReader(writer, T; kw...)(src, pos)
