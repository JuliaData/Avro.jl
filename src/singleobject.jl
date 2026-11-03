# Single-object encoding and schema stores: marker `C3 01`, the little-endian CRC-64-AVRO
# fingerprint of the writer schema's Parsing Canonical Form, then the binary datum.

const SINGLE_OBJECT_MARKER = (0xC3, 0x01)

"""
    Avro.SchemaStore

The interface of a schema store used by `Avro.decodesingle`: implement
`Avro.lookup(store, fingerprint::UInt64) -> Schema` and throw `Avro.UnknownSchemaError` for an
unknown fingerprint. Writable stores may also implement `Avro.register!(store, schema) -> UInt64`. A
custom store owns its lookup resources, cache bounds and ambiguity policy; the built-in
`Avro.SchemaCache` rejects any second schema under an existing fingerprint that is not structurally
equal to the stored one. Fingerprints are identifiers, not authentication.
"""
abstract type SchemaStore end

"""
    Avro.SchemaCache(; max_entries=10_000)

A lock-protected schema store holding at most `max_entries` schemas. `register!` is idempotent for a
structurally equal schema and raises `Avro.AmbiguousSchemaError` for any other schema under the same
fingerprint — a different Parsing Canonical Form (a CRC collision) or a parsing-equivalent schema that
differs in anything `==` compares (docs, aliases, order, logical types, defaults, props, error flag).
"""
struct SchemaCache <: SchemaStore
    lock::ReentrantLock
    max_entries::Int
    schemas::Dict{UInt64,Schema}
end

function SchemaCache(; max_entries::Integer=10_000)
    max_entries >= 0 || throw(ArgumentError("max_entries must be non-negative"))
    return SchemaCache(ReentrantLock(), Int(max_entries), Dict{UInt64,Schema}())
end

function Base.length(c::SchemaCache)
    return lock(() -> length(c.schemas), c.lock)
end

"""
    Avro.register!(store, schema) -> UInt64

Register `schema` and return its CRC-64-AVRO fingerprint.
"""
function register!(c::SchemaCache, s::Schema)
    fp = fingerprint(s)
    return lock(c.lock) do
        existing = get(c.schemas, fp, nothing)
        if existing === nothing
            n = length(c.schemas) + 1
            n <= c.max_entries || throw(LimitError(:max_entries, n, c.max_entries))
            c.schemas[fp] = s
        else
            schemaequal(existing, s) || throw(AmbiguousSchemaError(fp, existing, s))
        end
        return fp
    end
end

"""
    Avro.lookup(store, fingerprint::UInt64) -> Schema

The schema registered under `fingerprint` (`Avro.UnknownSchemaError` otherwise).
"""
function lookup(c::SchemaCache, fp::UInt64)
    s = lock(() -> get(c.schemas, fp, nothing), c.lock)
    s === nothing && throw(UnknownSchemaError(fp))
    return s
end

"""
    Avro.SingleObjectWriter(schema; limits=Limits())
    Avro.SingleObjectWriter(schema, T; limits=Limits())

A prepared single-object encoder: `writer(x) -> Vector{UInt8}` writes `C3 01`, the little-endian CRC-64-AVRO
fingerprint of `schema`'s Parsing Canonical Form, and the binary datum. It holds a `DatumWriter`, so it is
single-owner, and `T` fixes the datum type in the same way.
"""
struct SingleObjectWriter{W<:DatumWriter}
    header::Vector{UInt8}
    writer::W
end

function Base.show(io::IO, w::SingleObjectWriter)
    return print(io, "Avro.SingleObjectWriter(", schemalabel(w.writer.schema), ")")
end

function singleobjectheader(s::Schema)
    fp = fingerprint(s)
    return UInt8[SINGLE_OBJECT_MARKER..., (UInt8((fp >> (8 * i)) & 0xff) for i in 0:7)...]
end

function SingleObjectWriter(s::Schema; limits::Limits=Limits())
    return SingleObjectWriter(singleobjectheader(s), DatumWriter(s; limits=limits))
end

function SingleObjectWriter(s::Schema, ::Type{T}; limits::Limits=Limits()) where {T}
    return SingleObjectWriter(singleobjectheader(s), DatumWriter(s, T; limits=limits))
end

function (w::SingleObjectWriter)(x)
    writeraw!(w.writer.encoder, w.header)
    return w.writer(x)
end

"""
    Avro.encodesingle(schema, x; limits=Limits()) -> Vector{UInt8}

One-shot single-object encoding of `x` (use a `SingleObjectWriter` for repeated work).
"""
function encodesingle(s::Schema, x; limits::Limits=Limits())
    return SingleObjectWriter(s; limits=limits)(x)
end

"""
    Avro.SingleObjectReader(store; reader_schema=nothing, union_resolution=:spec, limits=Limits())
    Avro.SingleObjectReader(store, T; kw...)

A prepared single-object decoder: `reader(src) -> value` for `src` holding one message, as bytes or an `IO`.
It validates the marker, looks the writer schema up by fingerprint, recomputes the fingerprint of the returned
schema (rejecting a mismatch), resolves against `reader_schema` when given and decodes the payload exactly
(trailing bytes are a `DataError`). The first message of each fingerprint does the schema work once; later
ones only decode. `T` selects a typed target. Safe to share across tasks.
"""
mutable struct SingleObjectReader{T,S<:SchemaStore}
    const store::S
    const reader_schema::Union{Nothing,Schema}
    const union_resolution::Symbol
    const limits::Limits
    const lock::ReentrantLock
    @atomic readers::Dict{UInt64,DatumReader{T}}    # replaced, never mutated, so a lookup takes no lock
end

function Base.show(io::IO, r::SingleObjectReader{T}) where {T}
    return print(io, "Avro.SingleObjectReader(", typeof(r.store), T === Any ? "" : ", $T", ")")
end

function SingleObjectReader(store::SchemaStore; kw...)
    return SingleObjectReader(store, Any; kw...)
end

function SingleObjectReader(store::S, ::Type{T}; reader_schema::Union{Nothing,Schema}=nothing,
                            union_resolution::Symbol=:spec, limits::Limits=Limits()) where {T,S<:SchemaStore}
    return SingleObjectReader{T,S}(store, reader_schema, union_resolution, limits, ReentrantLock(), Dict{UInt64,DatumReader{T}}())
end

function datumreader(r::SingleObjectReader{T}, fp::UInt64) where {T}
    prepared = get(@atomic(r.readers), fp, nothing)
    prepared === nothing || return prepared
    writer = lookup(r.store, fp)
    writer isa Schema || throw(ArgumentError("the schema store returned $(typeof(writer)), not an Avro.Schema"))
    actual = fingerprint(writer)
    actual == fp || throw(DataError("the schema store returned a schema with fingerprint $(string(actual; base=16)) for $(string(fp; base=16))", 3))
    kw = (reader_schema=r.reader_schema, union_resolution=r.union_resolution, limits=r.limits)
    prepared = DatumReader(writer, T; kw...)
    Base.@lock r.lock begin
        readers = copy(@atomic r.readers)
        readers[fp] = prepared
        @atomic r.readers = readers
    end
    return prepared
end

function (r::SingleObjectReader)(src::AbstractVector{UInt8})
    buf = onebased(src)
    n = length(buf)
    n >= 10 || throw(DataError("single-object message shorter than the 10-byte header", n + 1))
    (buf[1], buf[2]) == SINGLE_OBJECT_MARKER || throw(DataError("missing single-object marker C3 01", 1))
    fp = UInt64(0)
    for i in 0:7
        fp |= UInt64(buf[3 + i]) << (8 * i)
    end
    value, next = datumreader(r, fp)(buf, 11)
    next == n + 1 || throw(DataError("trailing bytes after the datum", next))
    return value
end

function (r::SingleObjectReader)(io::IO)
    return r(sourcebytes(io, satadd(r.limits.max_bytes, 10)))
end

"""
    Avro.decodesingle(src, store; reader_schema=nothing, union_resolution=:spec, limits=Limits())
    Avro.decodesingle(src, store, T; kw...)

One-shot single-object decoding of the message in `src`, optionally into the typed target `T` (use a
`SingleObjectReader` for repeated work).
"""
function decodesingle(src::Union{AbstractVector{UInt8},IO}, store::SchemaStore; kw...)
    return SingleObjectReader(store; kw...)(src)
end

function decodesingle(src::Union{AbstractVector{UInt8},IO}, store::SchemaStore, ::Type{T}; kw...) where {T}
    return SingleObjectReader(store, T; kw...)(src)
end
