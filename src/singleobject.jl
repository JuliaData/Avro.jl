# Single-object encoding and schema stores (plan §4.10): marker `C3 01`, the little-endian CRC-64-AVRO
# fingerprint of the writer schema's Parsing Canonical Form, then the binary datum.

const SINGLE_OBJECT_MARKER = (0xC3, 0x01)

"""
    Avro.SchemaStore

The interface of a schema store used by `Avro.decodesingle`: `Avro.lookup(store, fingerprint::UInt64;
limits) -> Schema`, `Avro.lookup(store, fingerprint, budget::Avro.Budget) -> Schema`, and
`Avro.register!(store, schema; limits) -> UInt64`. The shared-budget lookup must charge the supplied
budget and throw `Avro.UnknownSchemaError` for an unknown fingerprint. A custom store owns its own
table budget and ambiguity policy; the built-in `Avro.SchemaCache` is bounded and rejects any second
schema under an existing fingerprint that is not structurally equal to the stored one. Fingerprints
are identifiers, not authentication.
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
    const ledger::Budget             # owns every persistent index allocation across register! calls
    fingerprints::Vector{UInt64}    # exact-capacity replacement on insert (§4.4 growth rule)
    schemas::Vector{Schema}
    bytes::Int
end

const SCHEMA_CACHE_LOCK_BYTES = Base.summarysize(ReentrantLock())

"Persistent storage retained by an empty `SchemaCache`, including its ledger and index shells."
function schemacachebasebytes()
    return checked_add(checked_add(sizeof(SchemaCache), sizeof(Budget)),
                       checked_add(SCHEMA_CACHE_LOCK_BYTES,
                                   checked_add(vectorbytes(UInt64, 0), vectorbytes(Schema, 0))))
end

function SchemaCache(; max_entries::Integer=10_000, max_bytes::Integer=64 << 20)
    max_entries >= 0 || throw(ArgumentError("max_entries must be non-negative"))
    max_bytes >= 0 || throw(ArgumentError("max_bytes must be non-negative"))
    entries = Int(max_entries)
    bytes = Int(max_bytes)
    base = schemacachebasebytes()
    bytes >= base || throw(ArgumentError("max_bytes must cover the cache's fixed structure ($base bytes)"))
    ledger = ledgerbudget(bytes)
    fingerprints = UInt64[]
    schemas = Schema[]
    retain!(ledger, base)
    return SchemaCache(ReentrantLock(), entries, bytes, ledger, fingerprints, schemas, base)
end

function cacheindexbytes(n::Int)
    return checked_add(vectorbytes(UInt64, n), vectorbytes(Schema, n))
end

function cachefrozenvectorbytes(vector::FrozenVector{T}) where {T}
    isempty(vector) && return 0
    return checked_add(24, vectorbytes(T, vector.cap))
end

function cachejsonbytes(value)
    value === nothing && return 0
    value isa Union{Bool,Int64,Float64} && return boxbytes(typeof(value))
    value isa String && return stringbytes(sizeof(value))
    value isa JSONNumber && return checked_add(32, stringbytes(sizeof(value.text)))
    if value isa JSONArray
        value === EMPTY_JSON_ARRAY && return 0
        bytes = checked_add(64, cachefrozenvectorbytes(value.items))
        for child in value.items
            bytes = checked_add(bytes, cachejsonbytes(child))
        end
        return bytes
    end
    if value isa JSONObject
        value === EMPTY_JSON_OBJECT && return 0
        bytes = checked_add(64, frozendictbytes(value.members))
        bytes = checked_add(bytes, cachefrozenvectorbytes(value.order))
        bytes = checked_add(bytes, cachefrozenvectorbytes(value.spans))
        for key in value.members.keys
            bytes = checked_add(bytes, stringbytes(sizeof(key)))
        end
        for child in value.members.vals
            bytes = checked_add(bytes, cachejsonbytes(child))
        end
        return bytes
    end
    throw(ArgumentError("internal error: unsupported frozen JSON value $(typeof(value))"))
end

function cacherawmembersbytes(members::FrozenDict{String,RawMember})
    isempty(members) && return 0
    bytes = frozendictbytes(members)
    for (key, member) in members
        bytes = checked_add(bytes, stringbytes(sizeof(key)))
        bytes = checked_add(bytes, stringbytes(sizeof(member.key)))
        isempty(member.value) ||
            (bytes = checked_add(bytes, stringbytes(sizeof(member.value))))
    end
    return bytes
end

function cachelexemesbytes(lexemes::SchemaLexemes)
    bytes = cacherawmembersbytes(lexemes.members)
    isempty(lexemes.fields) && return bytes
    bytes = checked_add(bytes, cachefrozenvectorbytes(lexemes.fields))
    for field in lexemes.fields
        bytes = checked_add(bytes, cacherawmembersbytes(field))
    end
    return bytes
end

function cachepropsbytes(props::Props)
    isempty(props) && return 0
    bytes = frozendictbytes(props)
    for key in props.keys
        bytes = checked_add(bytes, stringbytes(sizeof(key)))
    end
    for value in props.vals
        bytes = checked_add(bytes, cachejsonbytes(value))
    end
    return bytes
end

function cachedefaultbytes(default::Default)
    default isa DefaultValue || return 0
    bytes = cachejsonbytes(default.json)
    isempty(default.span) || (bytes = checked_add(bytes, stringbytes(sizeof(default.span))))
    return bytes
end

function cachestringvectorbytes(values::FrozenVector{String})
    isempty(values) && return 0
    bytes = cachefrozenvectorbytes(values)
    for value in values
        bytes = checked_add(bytes, stringbytes(sizeof(value)))
    end
    return bytes
end

function schemaretainedbytes(s::Schema, seen::Vector{Bool})
    index = Int(nodeid(s)) + 1
    seen[index] && return 0
    seen[index] = true
    bytes = NODE_META_BYTES + NODE_SHELL_BYTES + 192
    bytes = checked_add(bytes, cachelexemesbytes(schemalexemes(s)))
    s isa UnionSchema || (bytes = checked_add(bytes, cachepropsbytes(s.props)))
    if s isa Union{IntSchema,LongSchema,BytesSchema,StringSchema,FixedSchema}
        s.logical isa UnknownLogical &&
            (bytes = checked_add(bytes, stringbytes(sizeof(s.logical.name))))
    end
    if s isa NamedSchema
        bytes = checked_add(bytes, stringbytes(sizeof(s.name.name)))
        bytes = checked_add(bytes, stringbytes(sizeof(s.name.namespace)))
        bytes = checked_add(bytes, cachestringvectorbytes(s.aliases))
        bytes = checked_add(bytes, cachestringvectorbytes(s.rawaliases))
    end
    if s isa ArraySchema
        bytes = checked_add(bytes, schemaretainedbytes(s.items, seen))
    elseif s isa MapSchema
        bytes = checked_add(bytes, schemaretainedbytes(s.values, seen))
    elseif s isa UnionSchema
        bytes = checked_add(bytes, cachefrozenvectorbytes(s.branches))
        for branch in s.branches
            bytes = checked_add(bytes, schemaretainedbytes(branch, seen))
        end
    elseif s isa EnumSchema
        s.doc === nothing || (bytes = checked_add(bytes, stringbytes(sizeof(s.doc))))
        bytes = checked_add(bytes, cachestringvectorbytes(s.symbols))
        bytes = checked_add(bytes, frozendictbytes(s.symbolindex))
        bytes = checked_add(bytes, cachedefaultbytes(s.default))
    elseif s isa RecordSchema
        s.doc === nothing || (bytes = checked_add(bytes, stringbytes(sizeof(s.doc))))
        bytes = checked_add(bytes, cachefrozenvectorbytes(s.fields))
        bytes = checked_add(bytes, frozendictbytes(s.fieldindex))
        for field in s.fields
            bytes = checked_add(bytes, stringbytes(sizeof(field.name)))
            field.doc === nothing ||
                (bytes = checked_add(bytes, stringbytes(sizeof(field.doc))))
            bytes = checked_add(bytes, cachestringvectorbytes(field.aliases))
            bytes = checked_add(bytes, cachepropsbytes(field.props))
            bytes = checked_add(bytes, cachedefaultbytes(field.default))
            bytes = checked_add(bytes, schemaretainedbytes(field.schema, seen))
        end
    end
    return bytes
end

function schemaretainedbytes(s::Schema, budget::Budget)
    charge = vectorbytes(Bool, graphinfo(s).nodes)
    reserve!(budget, charge)
    seen = Vector{Bool}(undef, graphinfo(s).nodes)
    allocated!(budget, charge)
    fill!(seen, false)
    try
        return checked_add(checked_add(144, GRAPH_INFO_BYTES), schemaretainedbytes(s, seen))
    finally
        release!(budget, charge)
    end
end

function Base.length(c::SchemaCache)
    return lock(() -> length(c.fingerprints), c.lock)
end

"""
    Avro.register!(store, schema; limits=Limits()) -> UInt64

Register `schema` and return its CRC-64-AVRO fingerprint.
"""
function register!(c::SchemaCache, s::Schema; limits::Limits=Limits())
    graphinfo(s).repaired_names && throw(ArgumentError("a schema with repaired invalid names has no Parsing Canonical Form"))
    return withbudget(limits) do budget                # one operation: print, hash, compare, insert (D03)
        w = BoundedWriter(budget, limits.max_schema_bytes)
        seen = schemaseen(s, budget)
        canonicalprint(w, s, seen)
        releaseseen!(budget, seen)                     # dead before hashing and comparison (round-3 item 1)
        fp = crc64avro(boundedview(w))
        lock(c.lock) do
        i = cacheindex(c.fingerprints, fp, budget)
        if i <= length(c.fingerprints) && c.fingerprints[i] == fp
            existing = c.schemas[i]
            same = budgetedschemaequal(existing, s, budget)
            same || throw(AmbiguousSchemaError(fp, existing, s))
            return fp
        end
        n = length(c.fingerprints) + 1
        n <= c.max_entries || throw(LimitError(:max_entries, n, c.max_entries, :max_entries, :decode))
        schemacharge = schemaretainedbytes(s, budget)
        addcompare!(budget, checked_mul(16, n - 1))            # both old indexes copied before publication
        checkpoint = budgetcheckpoint(c.ledger)
        nf, ns = try
            retain!(c.ledger, schemacharge)
            fingerprintcharge = vectorbytes(UInt64, n)
            reserve!(c.ledger, fingerprintcharge)
            newfingerprints = Vector{UInt64}(undef, n)     # exact-capacity replacement (§4.4 growth rule)
            allocated!(c.ledger, fingerprintcharge)
            schemacharge = vectorbytes(Schema, n)
            reserve!(c.ledger, schemacharge)
            newschemas = Vector{Schema}(undef, n)
            allocated!(c.ledger, schemacharge)
            copyto!(newfingerprints, 1, c.fingerprints, 1, i - 1)
            copyto!(newschemas, 1, c.schemas, 1, i - 1)
            newfingerprints[i] = fp
            newschemas[i] = s
            copyto!(newfingerprints, i + 1, c.fingerprints, i, n - i)
            copyto!(newschemas, i + 1, c.schemas, i, n - i)
            (newfingerprints, newschemas)
        catch error
            rollbackreservations!(c.ledger, checkpoint)
            if error isa LimitError && error.limit === :max_total_bytes
                throw(LimitError(:max_bytes, error.observed, c.max_bytes, :max_bytes, :decode))
            end
            rethrow()
        end
        oldcharge = cacheindexbytes(length(c.fingerprints))
        c.fingerprints = nf
        c.schemas = ns
        release!(c.ledger, oldcharge)
        c.bytes = c.ledger.reserved
        return fp
        end
    end
end

"""
    Avro.lookup(store, fingerprint::UInt64; limits=Limits()) -> Schema

The schema registered under `fingerprint` (`Avro.UnknownSchemaError` otherwise).
"""
function lookup(c::SchemaCache, fp::UInt64; limits::Limits=Limits())
    return withbudget(limits) do budget                # the binary search is charged work (D03)
        return lookup(c, fp, budget)
    end
end

"""
    Avro.lookup(store, fingerprint, budget::Avro.Budget) -> Schema

The shared-budget route `decodesingle` uses. Custom stores must implement it so lookup work is charged
to the caller's operation budget instead of opening a second budget (D03).
"""
function lookup(::SchemaStore, ::UInt64, ::Budget)
    throw(ArgumentError("a custom Avro.SchemaStore used by decodesingle must implement Avro.lookup(store, fingerprint, budget)"))
end

function lookup(c::SchemaCache, fp::UInt64, budget::Budget)
    return lock(c.lock) do
        n = length(c.fingerprints)
        i = cacheindex(c.fingerprints, fp, budget)
        (i <= n && c.fingerprints[i] == fp) || throw(UnknownSchemaError(fp))
        return c.schemas[i]
    end
end

function cacheindex(fingerprints::Vector{UInt64}, fp::UInt64, budget::Budget)
    lo = 1
    hi = length(fingerprints) + 1
    while lo < hi
        mid = lo + ((hi - lo) >>> 1)
        addcompare!(budget, 8)
        if fingerprints[mid] < fp
            lo = mid + 1
        else
            hi = mid
        end
    end
    lo <= length(fingerprints) && addcompare!(budget, 8)
    return lo
end

"""
    Avro.encodesingle(schema, x; limits=Limits()) -> Vector{UInt8}

The single-object encoding of `x`: `C3 01`, the little-endian CRC-64-AVRO fingerprint of `schema`'s
Parsing Canonical Form, and the binary datum.
"""
function encodesingle(s::Schema, x; limits::Limits=Limits())
    graphinfo(s).repaired_names && throw(ArgumentError("a schema with repaired invalid names has no Parsing Canonical Form"))
    return withbudget(limits; direction=:encode) do budget    # one operation budget (plan §4.4, amendment round 1)
        local fp, e, npayload
        beginworkdefer!(budget)
        try
            w = BoundedWriter(budget, limits.max_schema_bytes; credit=false)
            seen = schemaseen(s, budget)
            canonicalprint(w, s, seen)
            releaseseen!(budget, seen)                 # dead before hashing and the write plan (round-3 item 1)
            fp = crc64avro(boundedview(w))
            plan = writeplan(s; budget=budget)
            e = Encoder(budget)
            encodedatum!(plan, e, x, s)
            npayload = e.pos
        finally
            endworkdefer!(budget)
        end
        checkoperationwork!(budget)
        checkcomparisonwork!(budget)
        reserve!(budget, bytesbytes(10 + npayload))
        out = Vector{UInt8}(undef, 10 + npayload)
        allocated!(budget, bytesbytes(10 + npayload))
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
function decodesingleoperation(src::AbstractVector{UInt8}, store::SchemaStore, budget::Budget;
                               reader_schema::Union{Nothing,Schema}=nothing,
                               union_resolution::Symbol=:spec, validate::Symbol=:strict,
                               limits::Limits=Limits(), names=DEFAULT_ADMISSION, T=nothing)
    T === nothing || T isa Type || throw(ArgumentError("T must be a type or nothing"))
    n = length(src)
    n >= 10 || throw(DataError("single-object message shorter than the 10-byte header", n + 1))
    (src[1] == SINGLE_OBJECT_MARKER[1] && src[2] == SINGLE_OBJECT_MARKER[2]) || throw(DataError("missing single-object marker C3 01", 1))
    fp = UInt64(0)
    for i in 0:7
        fp |= UInt64(src[3 + i]) << (8 * i)
    end
    n - 10 <= limits.max_datum_bytes || throw(LimitError(:max_datum_bytes, n - 10, limits.max_datum_bytes, :max_datum_bytes, :decode))
    local tplan, adm, d, span
    beginworkdefer!(budget)
    try
        writer = lookup(store, fp, budget)             # the shared-budget route (D03); SchemaCache charges this budget
        writer isa Schema || throw(ArgumentError("the schema store returned $(typeof(writer)), not an Avro.Schema"))
        w = BoundedWriter(budget, limits.max_schema_bytes; credit=false)
        seen = schemaseen(writer, budget)
        canonicalprint(w, writer, seen)
        releaseseen!(budget, seen)                     # dead before hashing (round-3 item 1)
        actual = crc64avro(boundedview(w))
        actual == fp || throw(DataError("the schema store returned a schema with fingerprint $(string(actual; base=16)) for $(string(fp; base=16))", 3))
        effective = reader_schema === nothing ? writer : reader_schema
        plan = reader_schema === nothing ? readplan(writer; budget=budget) :
               resolvingplan(writer, reader_schema; union_resolution=union_resolution, limits=limits, budget=budget)
        spans = spanplan(writer; budget=budget)
        tplan = T === nothing ? plan : typedplan(T, effective, plan, limits; budget=budget)
        adm = admission(names)
        d = Decoder(src, budget; pos=11, validate=validate)
        datumspan!(spans, d, 11, budget.values,
                   rawspaninputmax(budget, src, 11, limits))
        span = decoderspan(d)
        span.next == n + 1 || throw(DataError("trailing bytes after the datum", span.next))
        addinput!(budget, span.bytes)
    finally
        endworkdefer!(budget)
    end
    checkoperationwork!(budget)
    checkcomparisonwork!(budget)
    d.stop = span.next - 1
    planneddatumspan!(tplan, d, span; resolved=reader_schema !== nothing)
    span = decoderspan(d)
    work = begindatum!(d, span)
    value = try
        decodetyped(T === nothing ? GenericDatumTarget : T, tplan, d, adm)
    catch
        abortdatum!(d, work)
        rethrow()
    end
    finishdatum!(d, work)
    return (value, tplan, adm)
end

function decodesingle(src::AbstractVector{UInt8}, store::SchemaStore; reader_schema::Union{Nothing,Schema}=nothing,
                      union_resolution::Symbol=:spec, validate::Symbol=:strict, limits::Limits=Limits(),
                      names=DEFAULT_ADMISSION, T=nothing)
    value, plan, adm = withbudget(limits) do budget
        return decodesingleoperation(src, store, budget; reader_schema=reader_schema,
                                     union_resolution=union_resolution, validate=validate,
                                     limits=limits, names=names, T=T)
    end
    return finishtyped(plan, value, adm)
end

function decodesingle(io::IO, store::SchemaStore; limits::Limits=Limits(), kw...)
    value, plan, adm = withbudget(limits) do budget
        maxmessage = limits.max_datum_bytes > typemax(Int) - 10 ? typemax(Int) : limits.max_datum_bytes + 10
        bytes = sourcebytes(io, maxmessage, budget, DataError)
        return decodesingleoperation(bytes, store, budget; limits=limits, kw...)
    end
    return finishtyped(plan, value, adm)
end
