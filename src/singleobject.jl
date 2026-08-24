# Single-object encoding and schema stores (plan §4.10): marker `C3 01`, the little-endian CRC-64-AVRO
# fingerprint of the writer schema's Parsing Canonical Form, then the binary datum.

const SINGLE_OBJECT_MARKER = (0xC3, 0x01)

"""
    Avro.SchemaStore

The interface of a schema store used by `Avro.decodesingle`: `Avro.lookup(store, fingerprint::UInt64;
limits) -> Schema` (throwing `Avro.UnknownSchemaError`) and `Avro.register!(store, schema; limits) ->
UInt64`. A custom store owns its own table budget and ambiguity policy; the built-in `Avro.SchemaCache`
is bounded and rejects any second schema under an existing fingerprint that is not structurally equal
to the stored one. Fingerprints are identifiers, not authentication.
"""
abstract type SchemaStore end

"""
    Avro.SchemaCache(; max_entries=10_000, max_bytes=64 << 20)

A bounded, lock-protected schema store indexed by a sorted fingerprint vector (binary search; no
hashing). `register!` is idempotent for a structurally equal schema and raises
`Avro.AmbiguousSchemaError` for any other schema under the same fingerprint — a different Parsing
Canonical Form (a CRC collision) or a parsing-equivalent schema with different logical types, defaults,
props or error flag.
"""
mutable struct SchemaCache <: SchemaStore
    const lock::ReentrantLock
    const max_entries::Int
    const max_bytes::Int
    fingerprints::Vector{UInt64}    # exact-capacity replacement on insert (§4.4 growth rule)
    schemas::Vector{Schema}
    bytes::Int
end

function SchemaCache(; max_entries::Integer=10_000, max_bytes::Integer=64 << 20)
    max_entries >= 0 || throw(ArgumentError("max_entries must be non-negative"))
    max_bytes >= 0 || throw(ArgumentError("max_bytes must be non-negative"))
    return SchemaCache(ReentrantLock(), Int(max_entries), Int(max_bytes), UInt64[], Schema[], 0)
end

Base.length(c::SchemaCache) = lock(() -> length(c.fingerprints), c.lock)

"""
    Avro.register!(store, schema; limits=Limits()) -> UInt64

Register `schema` and return its CRC-64-AVRO fingerprint.
"""
function register!(c::SchemaCache, s::Schema; limits::Limits=Limits())
    pcf = canonical(s; limits=limits)
    fp = crc64avro(pcf)
    return lock(c.lock) do
        i = searchsortedfirst(c.fingerprints, fp)
        if i <= length(c.fingerprints) && c.fingerprints[i] == fp
            existing = c.schemas[i]
            same = withbudget(limits) do budget
                schemaequal(existing, s, Vector{Vector{Int32}}(), budget)
            end
            same || throw(AmbiguousSchemaError(fp, existing, s))
            return fp
        end
        n = length(c.fingerprints) + 1
        n <= c.max_entries || throw(LimitError(:max_entries, n, c.max_entries, :max_entries, :decode))
        cost = sizeof(pcf) + 64 + 16                   # PCF text + entry overhead + the two index slots
        retained = checked_add(c.bytes, cost)
        peak = checked_add(retained, checked_mul(16, n - 1))   # old index vectors overlap their replacements
        peak <= c.max_bytes || throw(LimitError(:max_bytes, peak, c.max_bytes, :max_bytes, :decode))
        nf = Vector{UInt64}(undef, n)                  # exact-capacity replacement (§4.4 growth rule):
        ns = Vector{Schema}(undef, n)                  # capacity equals length, and a failure above
        copyto!(nf, 1, c.fingerprints, 1, i - 1)       # leaves the table untouched (transactional)
        copyto!(ns, 1, c.schemas, 1, i - 1)
        nf[i] = fp
        ns[i] = s
        copyto!(nf, i + 1, c.fingerprints, i, n - i)
        copyto!(ns, i + 1, c.schemas, i, n - i)
        c.fingerprints = nf
        c.schemas = ns
        c.bytes = retained
        return fp
    end
end

"""
    Avro.lookup(store, fingerprint::UInt64; limits=Limits()) -> Schema

The schema registered under `fingerprint` (`Avro.UnknownSchemaError` otherwise).
"""
function lookup(c::SchemaCache, fp::UInt64; limits::Limits=Limits())
    lock(c.lock) do
        i = searchsortedfirst(c.fingerprints, fp)
        (i <= length(c.fingerprints) && c.fingerprints[i] == fp) || throw(UnknownSchemaError(fp))
        return c.schemas[i]
    end
end

"""
    Avro.encodesingle(schema, x; limits=Limits()) -> Vector{UInt8}

The single-object encoding of `x`: `C3 01`, the little-endian CRC-64-AVRO fingerprint of `schema`'s
Parsing Canonical Form, and the binary datum.
"""
function encodesingle(s::Schema, x; limits::Limits=Limits())
    graphinfo(s).repaired_names && throw(ArgumentError("a schema with repaired invalid names has no Parsing Canonical Form"))
    return withbudget(limits; direction=:encode) do budget    # one operation budget (plan §4.4, amendment round 1)
        w = BoundedWriter(budget, limits.max_schema_bytes)
        canonicalprint(w, s, FrozenDict{String,Bool}())
        fp = crc64avro(String(take!(w.io)))
        plan = writeplan(s; budget=budget)
        e = Encoder(budget)
        encodedatum!(plan, e, x)
        npayload = e.pos
        reserve!(budget, bytesbytes(10 + npayload))
        out = Vector{UInt8}(undef, 10 + npayload)
        out[1], out[2] = SINGLE_OBJECT_MARKER
        for i in 0:7
            out[3 + i] = UInt8((fp >> (8 * i)) & 0xff)
        end
        copyto!(out, 11, e.buf, 1, npayload)
        return out
    end
end

"""
    Avro.decodesingle(src, store; reader_schema=nothing, union_resolution=:spec, validate=:strict, limits=Limits(), names=Avro.DEFAULT_ADMISSION, T=nothing)

Decode a single-object message: validates the marker, looks the writer schema up by fingerprint,
recomputes the fingerprint of the returned schema (rejecting a mismatch), resolves against
`reader_schema` when given and decodes the payload exactly (trailing bytes are a `DataError`). `T`
selects a typed target.
"""
function decodesingle(src::AbstractVector{UInt8}, store::SchemaStore; reader_schema::Union{Nothing,Schema}=nothing,
                      union_resolution::Symbol=:spec, validate::Symbol=:strict, limits::Limits=Limits(),
                      names=DEFAULT_ADMISSION, T=nothing)
    T === nothing || T isa Type || throw(ArgumentError("T must be a type or nothing"))
    n = length(src)
    n >= 10 || throw(DataError("single-object message shorter than the 10-byte header", n + 1))
    (src[1] == SINGLE_OBJECT_MARKER[1] && src[2] == SINGLE_OBJECT_MARKER[2]) || throw(DataError("missing single-object marker C3 01", 1))
    fp = UInt64(0)
    for i in 0:7
        fp |= UInt64(src[3 + i]) << (8 * i)
    end
    writer = lookup(store, fp; limits=limits)
    writer isa Schema || throw(ArgumentError("the schema store returned $(typeof(writer)), not an Avro.Schema"))
    n - 10 <= limits.max_datum_bytes || throw(LimitError(:max_datum_bytes, n - 10, limits.max_datum_bytes, :max_datum_bytes, :decode))
    v, plan2, adm = withbudget(limits) do budget               # one operation budget (plan §4.4, amendment round 1)
        w = BoundedWriter(budget, limits.max_schema_bytes)
        canonicalprint(w, writer, FrozenDict{String,Bool}())
        actual = crc64avro(String(take!(w.io)))
        actual == fp || throw(DataError("the schema store returned a schema with fingerprint $(string(actual; base=16)) for $(string(fp; base=16))", 3))
        effective = reader_schema === nothing ? writer : reader_schema
        plan = reader_schema === nothing ? readplan(writer; budget=budget) :
               resolvingplan(writer, reader_schema; union_resolution=union_resolution, limits=limits)
        tplan = T === nothing ? plan : typedplan(T, effective, plan, limits)
        adm0 = admission(names)
        payload = view(src, 11:n)
        addinput!(budget, length(payload))
        d = Decoder(payload, budget; validate=validate)
        v0 = decodetyped(T === nothing ? Nothing : T, tplan, d, adm0)
        d.pos == length(payload) + 1 || throw(DataError("trailing bytes after the datum", d.pos + 10))
        return (v0, tplan, adm0)
    end
    return finishtyped(plan2, v, adm)                          # semantic conversion in caller space
end

function decodesingle(io::IO, store::SchemaStore; limits::Limits=Limits(), kw...)
    bytes = withbudget(limits) do budget
        sourcebytes(io, limits.max_datum_bytes + 10, budget, DataError)
    end
    return decodesingle(bytes, store; limits=limits, kw...)
end
