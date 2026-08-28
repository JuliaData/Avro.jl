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
    const span::SpanPlan         # exact writer-datum boundary/value plan (allocation-free per call)
    const limits::Limits
    const validate::Symbol
    const names::Union{SymbolAdmission,Symbol}
    const resolved::Bool       # reader defaults need a plan-aware zero-input-value pre-scan
    const staticvalues::Int    # exact writer value count, or -1 when a structural scan is required
    @atomic scratch::Any        # one pooled per-call Decoder (its budget inside), shared lock-free
end

"Internal marker for generic decoding; unlike `Nothing`, it can never be a caller's typed target."
struct GenericDatumTarget end

function datumreaderplan(writer::Schema, reader_schema::Union{Nothing,Schema}, union_resolution::Symbol,
                         limits::Limits, budget::Budget)
    return reader_schema === nothing ? readplan(writer; budget=budget) :
           resolvingplan(writer, reader_schema; union_resolution=union_resolution,
                         limits=limits, budget=budget)
end

function DatumReader(writer::Schema; reader_schema::Union{Nothing,Schema}=nothing, union_resolution::Symbol=:spec,
                     limits::Limits=Limits(), validate::Symbol=:strict, names=DEFAULT_ADMISSION)
    validate in (:strict, :fast) || throw(ArgumentError("validate must be :strict or :fast"))
    effective = reader_schema === nothing ? writer : reader_schema
    plan, span = withbudget(limits) do budget
        return (datumreaderplan(writer, reader_schema, union_resolution, limits, budget),
                spanplan(writer; budget=budget))
    end
    resolved = reader_schema !== nothing
    staticvalues = resolved ? -1 : staticdatumvalues(span, limits)
    return DatumReader{GenericDatumTarget,typeof(plan)}(writer, effective, plan, span, limits,
                                                        validate, admission(names), resolved,
                                                        staticvalues, nothing)
end

function DatumReader(writer::Schema, ::Type{T}; reader_schema::Union{Nothing,Schema}=nothing,
                     union_resolution::Symbol=:spec, limits::Limits=Limits(), validate::Symbol=:strict,
                     names=DEFAULT_ADMISSION) where {T}
    validate in (:strict, :fast) || throw(ArgumentError("validate must be :strict or :fast"))
    effective = reader_schema === nothing ? writer : reader_schema
    plan, span = withbudget(limits) do budget
        generic = datumreaderplan(writer, reader_schema, union_resolution, limits, budget)
        return (typedplan(T, effective, generic, limits; budget=budget),
                spanplan(writer; budget=budget))
    end
    resolved = reader_schema !== nothing
    staticvalues = resolved ? -1 : staticdatumvalues(span, limits)
    return DatumReader{T,typeof(plan)}(writer, effective, plan, span, limits, validate,
                                      admission(names), resolved, staticvalues, nothing)
end

"""
    reader(bytes) -> value
    reader(bytes, pos) -> (value, nextpos)
    reader(io) -> value

Decode one datum (the byte form rejects trailing bytes; the `IO` form reads at most
`max_datum_bytes + 1` bytes and errors if bytes remain).
"""
const EMPTY_BYTES = UInt8[]                            # shared, never written: decoders only read `buf`

@inline function (r::DatumReader{T,P})(bytes::AbstractVector{UInt8}) where {T,P}
    bytes isa Vector{UInt8} && length(bytes) <= r.limits.max_datum_bytes &&
        return first(pooledcall(r, bytes, 1, true))
    return first(slowcall(r, bytes, 1, true))
end

@inline function (r::DatumReader{T,P})(bytes::AbstractVector{UInt8}, pos::Integer) where {T,P}
    bytes isa Vector{UInt8} && return pooledcall(r, bytes, Int(pos), false)
    return slowcall(r, bytes, Int(pos), false)
end

"The pooled per-call path: ≤ 1 payload allocation per decode (§10.2; its own scope, so nothing boxes)."
@inline function pooledcall(r::DatumReader{T,P}, bytes::Vector{UInt8}, pos::Int,
                            whole::Bool) where {T,P}
    sc = @atomicswap(r.scratch = nothing)
    d = sc isa Decoder{Vector{UInt8}} ? sc : Decoder(UInt8[], Budget(r.limits); validate=r.validate)
    recycle = false
    local v, next
    try
        resetbudget!(d.budget)
        d.buf = bytes
        d.pos = pos
        d.stop = length(bytes)
        d.depth = 0
        1 <= pos <= length(bytes) + 1 || throw(ArgumentError("position $pos out of range"))
        if r.staticvalues >= 0
            work = beginstaticdatum!(d, r.staticvalues)
            try
                v = decodetyped(T, r.plan, d, r.names)
                next = d.pos
                whole && next != length(bytes) + 1 &&
                    throw(DataError("trailing bytes after the datum", next))
                finishstaticdatum!(d, work; credit=true)
            catch
                abortdatum!(d, work)
                rethrow()
            end
        else
            datumspan!(r.span, d, pos, d.budget.values,
                       rawspaninputmax(d.budget, bytes, pos, r.limits))
            span = decoderspan(d)
            whole && span.next != length(bytes) + 1 &&
                throw(DataError("trailing bytes after the datum", span.next))
            d.stop = span.next - 1
            addinput!(d.budget, span.bytes)
            planneddatumspan!(r.plan, d, span; resolved=r.resolved)
            span = decoderspan(d)
            work = begindatum!(d, span)
            v = decodetyped(T, r.plan, d, r.names)
            finishdatum!(d, work)
            next = d.pos
        end
        recycle = true
    finally
        d.buf = EMPTY_BYTES                            # never pin the caller's bytes from the pool
        close!(d.budget)
        recycle && @atomicswap(r.scratch = d)          # a failed call drops its scratch instead
    end
    return (finishtyped(r.plan, v, r.names), next)
end

function slowcall(r::DatumReader{T,P}, bytes::AbstractVector{UInt8}, pos::Int,
                  whole::Bool) where {T,P}
    v, next = withbudget(r.limits) do budget
        source = positionalsource(bytes, pos, r.limits, budget)
        d = Decoder(source.buffer, budget; pos=source.start,
                    validate=r.validate)
        datumspan!(r.span, d, source.start, budget.values,
                   rawspaninputmax(budget, source.buffer,
                                   source.start, r.limits))
        span = decoderspan(d)
        next = originalnext(source, span.next)
        whole && next != source.sourceend + 1 &&
            throw(DataError("trailing bytes after the datum", next))
        addinput!(budget, span.bytes)
        d.stop = span.next - 1
        planneddatumspan!(r.plan, d, span; resolved=r.resolved)
        span = decoderspan(d)
        work = begindatum!(d, span)
        value = try
            decodetyped(T, r.plan, d, r.names)
        catch
            abortdatum!(d, work)
            rethrow()
        end
        finishdatum!(d, work)
        return (value, next)
    end
    return (finishtyped(r.plan, v, r.names), next)     # semantic conversion runs in caller space
end

function (r::DatumReader{T,P})(io::IO) where {T,P}
    v = withbudget(r.limits) do budget
        buf = sourcebytes(io, r.limits.max_datum_bytes, budget, DataError)
        length(buf) <= r.limits.max_datum_bytes || throw(LimitError(:max_datum_bytes, length(buf), r.limits.max_datum_bytes, :max_datum_bytes, :decode))
        d = Decoder(buf, budget; validate=r.validate)
        datumspan!(r.span, d, 1, budget.values,
                   rawspaninputmax(budget, buf, 1, r.limits))
        span = decoderspan(d)
        span.next == length(buf) + 1 || throw(DataError("trailing bytes after the datum", span.next))
        addinput!(budget, span.bytes)
        d.stop = span.next - 1
        planneddatumspan!(r.plan, d, span; resolved=r.resolved)
        span = decoderspan(d)
        work = begindatum!(d, span)
        v = try
            decodetyped(T, r.plan, d, r.names)
        catch
            abortdatum!(d, work)
            rethrow()
        end
        finishdatum!(d, work)
        return v
    end
    return finishtyped(r.plan, v, r.names)
end

function decodetyped(::Type{GenericDatumTarget}, plan::ReadPlan, d::Decoder, names)
    return decode(plan, d)
end

function decodetyped(::Type{T}, plan::SemanticTarget{T}, d::Decoder, names) where {T}
    return decode(plan.plan, d)
end

function decodetyped(::Type{T}, plan::TypedPlan, d::Decoder, names) where {T}
    return decodetyped(plan, d, names)
end

"""
    Avro.DatumWriter(schema; limits=Limits())

A prepared encoder with a reusable buffer (single-owner): `writer(x) -> Vector{UInt8}` returns a fresh
owned buffer; `writer(enc_or_io, x)` appends to the caller's encoder or stream.
"""
struct UnfixedDatumWriter end

mutable struct DatumWriter{P<:WritePlan,T,FP}
    const schema::Schema
    const plan::P               # concrete plan type (§10.2)
    const limits::Limits
    const callbudget::Budget      # reset for each call; writer is single-owner
    encoder::Union{Nothing,Encoder}
    fasttype::Any               # single-entry aligned-NamedTuple cache (single-owner like `encoder`)
    fastplans::Any
    fastbytes::Int              # marginal tuple storage retained by the current untyped cache entry
    const fixedplans::FP        # the typed form's aligned tuple, concrete (`nothing` when untyped)
    const fixedbytes::Int       # marginal tuple storage retained by the typed writer
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
    return DatumWriter{typeof(plan),UnfixedDatumWriter,Nothing}(
        schema, plan, limits, Budget(limits; direction=:encode), nothing, nothing, nothing,
        0, nothing, 0)
end

function DatumWriter(schema::Schema, ::Type{T}; limits::Limits=Limits()) where {T}
    plan, fp = withbudget(limits; direction=:encode) do b
        p = writeplan(schema; budget=b)
        return (p, alignedplans(p, T, b))
    end
    return DatumWriter{typeof(plan),T,typeof(fp)}(
        schema, plan, limits, Budget(limits; direction=:encode), nothing, nothing, nothing,
        0, fp, fp === nothing ? 0 : sizeof(fp))
end

function fastdatumplans(w::DatumWriter, @nospecialize(x), budget::Budget)
    x isa NamedTuple || return nothing
    w.fasttype === typeof(x) && return w.fastplans
    fp = alignedplans(w.plan, typeof(x), budget)
    oldbytes = w.fastbytes
    w.fasttype = typeof(x)
    w.fastplans = fp
    w.fastbytes = fp === nothing ? 0 : sizeof(fp)
    release!(budget, oldbytes)                       # the replaced tuple is unreachable after publication
    return fp
end

"Reset the writer's retained encoder and charge its existing buffer to the new operation."
function preparedencoder!(w::DatumWriter, budget::Budget)
    retain!(budget, checked_add(w.fastbytes, w.fixedbytes))
    e = w.encoder
    if e === nothing
        e = Encoder(budget)
        w.encoder = e
        return e
    end
    retain!(budget, encoderstorage(e))
    e.budget = budget
    e.owner = budget
    return reset!(e)
end

"Copy the encoded prefix into caller-owned storage under a reservation for the overlap peak."
function takeowned!(e::Encoder)
    charge = bytesbytes(e.pos)
    checkpoint = budgetcheckpoint(e.budget)
    try
        reserve!(e.budget, charge)
        out = take!(e)
        allocated!(e.budget, charge)
        release!(e.budget, charge)                    # caller ownership starts at the return boundary
        return out
    catch
        rollbackreservations!(e.budget, checkpoint)
        rethrow()
    end
end

function (w::DatumWriter)(x)
    budget = resetbudget!(w.callbudget)
    try
        e = preparedencoder!(w, budget)
        try
            writeprepared!(w, e, x)
            return takeowned!(e)
        catch
            reset!(e)
            rethrow()
        end
    finally
        close!(budget)
    end
end

function (w::DatumWriter)(io::IO, x)
    budget = resetbudget!(w.callbudget)
    try
        e = preparedencoder!(w, budget)
        try
            writeprepared!(w, e, x)
            buf = e.buf
            GC.@preserve buf Base.unsafe_write(io, pointer(buf), UInt(e.pos))
            return nothing
        finally
            reset!(e)
        end
    finally
        close!(budget)
    end
end

function (w::DatumWriter{P,T,FP})(e::Encoder, x) where {P,T,FP}
    e.budget === e.owner || throw(ArgumentError("an Encoder cannot run nested prepared-writer calls"))
    budget = resetbudget!(w.callbudget)
    original = e.budget
    start = e.pos
    olddepth = e.depth
    oldcredited = e.credited
    retain!(budget, checked_add(w.fastbytes, w.fixedbytes))
    retain!(budget, encoderstorage(e))
    e.budget = budget
    e.credited = start
    e.depth = 0
    try
        writeprepared!(w, e, x)
        return nothing
    catch
        e.pos = start
        rethrow()
    finally
        release!(budget, encoderstorage(e))
        e.budget = original
        e.depth = olddepth
        e.credited = oldcredited
        close!(budget)
    end
end

function writeprepared!(w::DatumWriter{P,T,FP}, e::Encoder, x) where {P,T,FP}
    T === UnfixedDatumWriter || typeof(x) === T ||
        throw(EncodeError(typediagnostic(
            e.budget, "prepared writer expects a datum of exact type ", T),
            "\$", w.schema))
    if FP !== Nothing
        encodealigned!(w.fixedplans, e, x, w.schema::RecordSchema) # fully concrete: the zero-allocation kernel
    else
        fp = fastdatumplans(w, x, e.budget)
        fp === nothing ? encodedatum!(w.plan, e, x, w.schema) :
                         encodealigned!(fp, e, x, w.schema::RecordSchema)
    end
    return nothing
end

function withencoderroot!(f::F, e::Encoder, schema::Schema) where {F}
    previous = e.expected
    emptyencodepath!(e)
    e.expected = schema
    try
        return f()
    catch err
        if err isa EncodeError && isempty(err.path)
            throw(EncodeError(err.msg, formatencodepath(e), e.expected))
        end
        rethrow()
    finally
        e.expected = previous
        emptyencodepath!(e)
    end
end

function encodedatum!(plan::WritePlan, e::Encoder, x, schema::Schema)
    return withencoderroot!(e, schema) do
        encodedwork!(e) do
            encode(plan, e, x)
        end
    end
end

"Encode one root datum with density deferred until its exact output span is known."
function encodedwork!(f::F, e::Encoder) where {F}
    budget = e.budget
    e.credited < e.pos && creditoutput!(e)              # settle any earlier caller-owned prefix
    start = e.pos
    values0 = budget.values
    beginworkdefer!(budget)
    complete = false
    try
        f()
        bytes = e.pos - start
        bytes <= budget.limits.max_datum_bytes ||
            throw(LimitError(:max_datum_bytes, bytes, budget.limits.max_datum_bytes,
                             :max_datum_bytes, :encode))
        creditoutput!(e)
        complete = true
    finally
        endworkdefer!(budget)
    end
    complete || return nothing
    values = budget.values - values0
    checkworkscope!(budget, values, e.pos - start)
    if budget.workdeferred == 0
        checkoperationwork!(budget)
        checkcomparisonwork!(budget)
    end
    return nothing
end

"""
    Avro.encode(schema, x; limits=Limits()) -> Vector{UInt8}
    Avro.encode(x; limits=Limits()) -> Vector{UInt8}          # conventional schema `Avro.schema(x)`

One-shot encoding (builds a writer per call; use `DatumWriter` for repeated use).
"""
function encode(schema::Schema, x; limits::Limits=Limits())
    return withbudget(limits; direction=:encode) do budget
        return encodewithbudget(schema, x, budget)
    end
end

function encode(x; limits::Limits=Limits())
    return withbudget(limits; direction=:encode) do budget
        s = withconstructionbudget(budget) do _
            schema(x; limits=limits)
        end
        return encodewithbudget(s, x, budget)
    end
end

function encodewithbudget(schema::Schema, x, budget::Budget)
    plan = writeplan(schema; budget=budget)
    encoder = Encoder(budget)
    encodedatum!(plan, encoder, x, schema)
    return takeowned!(encoder)
end

"""
    Avro.encode!(enc_or_io, schema, x; limits=Limits())

One-shot encoding that appends one datum to the caller's encoder or stream. Use `DatumWriter` for
repeated work.
"""
function encode!(io::IO, schema::Schema, x; limits::Limits=Limits())
    return withbudget(limits; direction=:encode) do budget
        plan = writeplan(schema; budget=budget)
        encoder = Encoder(budget)
        encodedatum!(plan, encoder, x, schema)
        buf = encoder.buf
        GC.@preserve buf Base.unsafe_write(io, pointer(buf), UInt(encoder.pos))
        return nothing
    end
end

function encode!(e::Encoder, schema::Schema, x; limits::Limits=Limits())
    e.budget === e.owner || throw(ArgumentError("an Encoder cannot run nested one-shot writes"))
    return withbudget(limits; direction=:encode) do budget
        original = e.budget
        start = e.pos
        depth = e.depth
        credited = e.credited
        retain!(budget, encoderstorage(e))
        e.budget = budget
        e.depth = 0
        e.credited = start
        try
            plan = writeplan(schema; budget=budget)
            encodedatum!(plan, e, x, schema)
            return nothing
        catch
            e.pos = start
            rethrow()
        finally
            release!(budget, encoderstorage(e))
            e.budget = original
            e.depth = depth
            e.credited = credited
        end
    end
end

"""
    Avro.decode(writer_schema, src; reader_schema=nothing, union_resolution=:spec, limits=Limits(), validate=:strict, names=…)
    Avro.decode(writer_schema, src, T; kw...)
    Avro.decode(writer_schema, bytes, pos::Integer; kw...) -> (value, nextpos)

One-shot decoding of one datum from bytes (trailing bytes rejected), from `bytes` at `pos`, or from an
`IO`.
"""
function decode(writer::Schema, src::AbstractVector{UInt8}; kw...)
    return decodedatum(writer, src, 1, GenericDatumTarget, true; kw...)
end

function decode(writer::Schema, src::IO; kw...)
    return decodedatum(writer, src, 1, GenericDatumTarget, true; kw...)
end

function decode(writer::Schema, src::AbstractVector{UInt8}, pos::Integer; kw...)
    return decodedatum(writer, src, Int(pos), GenericDatumTarget, false; kw...)
end

function decode(writer::Schema, src::AbstractVector{UInt8}, ::Type{T}; kw...) where {T}
    return decodedatum(writer, src, 1, T, true; kw...)
end

function decode(writer::Schema, src::IO, ::Type{T}; kw...) where {T}
    return decodedatum(writer, src, 1, T, true; kw...)
end

function decode(writer::Schema, src::AbstractVector{UInt8}, pos::Integer, ::Type{T}; kw...) where {T}
    return decodedatum(writer, src, Int(pos), T, false; kw...)
end

function decodedatum(writer::Schema, src, pos::Int, ::Type{T}, whole::Bool;
                      reader_schema::Union{Nothing,Schema}=nothing, union_resolution::Symbol=:spec,
                      limits::Limits=Limits(), validate::Symbol=:strict, names=DEFAULT_ADMISSION) where {T}
    validate in (:strict, :fast) || throw(ArgumentError("validate must be :strict or :fast"))
    admitted = admission(names)
    plan, value, next = withbudget(limits) do budget
        generic = datumreaderplan(writer, reader_schema, union_resolution, limits, budget)
        effective = reader_schema === nothing ? writer : reader_schema
        plan = T === GenericDatumTarget ? generic : typedplan(T, effective, generic, limits; budget=budget)
        spans = spanplan(writer; budget=budget)
        source = if src isa IO
            buffer = sourcebytes(src, limits.max_datum_bytes, budget, DataError)
            PositionalDatumSource(buffer, 1, 0, length(buffer))
        else
            positionalsource(src, pos, limits, budget)
        end
        decoder = Decoder(source.buffer, budget; pos=source.start,
                          validate=validate)
        datumspan!(spans, decoder, source.start, budget.values,
                   rawspaninputmax(budget, source.buffer,
                                   source.start, limits))
        span = decoderspan(decoder)
        next = originalnext(source, span.next)
        whole && next != source.sourceend + 1 &&
            throw(DataError("trailing bytes after the datum", next))
        addinput!(budget, span.bytes)
        decoder.stop = span.next - 1
        planneddatumspan!(plan, decoder, span;
                          resolved=reader_schema !== nothing)
        span = decoderspan(decoder)
        work = begindatum!(decoder, span)
        value = try
            decodetyped(T, plan, decoder, admitted)
        catch
            abortdatum!(decoder, work)
            rethrow()
        end
        finishdatum!(decoder, work)
        return (plan, value, next)
    end
    finished = finishtyped(plan, value, admitted)
    return whole ? finished : (finished, next)
end
