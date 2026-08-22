# Generic value types that carry schema identity (plan §4.6): Record, EnumValue, Fixed, UnionValue and
# the package-owned Avro.Map (sorted permutation; no hashing).

"""
    Avro.Map{V}

The generic representation of an Avro `map`: an `AbstractDict{String,V}` keeping keys and values in
insertion order with an `Int32` permutation sorted by key bytes (built once by a deterministic merge
sort; lookups are binary searches with a fixed per-call bound). No hashing, seeds, rehashing or growth.
Duplicate keys keep the first occurrence's position with the last value. `Dict(m)` converts.
"""
struct Map{V} <: AbstractDict{String,V}
    keys::Vector{String}
    vals::Vector{V}
    perm::Vector{Int32}     # sorted by keys[perm[i]]; length == length(keys)
end

Map{V}() where {V} = Map{V}(String[], V[], Int32[])

"""
    Avro.Map(pairs; limits=Limits())

Build a map from `key => value` pairs (keys `AbstractString` or `Symbol`); copying, validated, charged
to its own budget scope.
"""
function Map(pairs; limits::Limits=Limits())
    return withbudget(limits; direction=:encode) do budget
        ks = String[]
        vs = Any[]
        for (k, v) in pairs
            push!(ks, mapkey(k))
            push!(vs, v)
        end
        V = isempty(vs) ? Any : mapreduce(typeof, typejoin, vs)
        buildmap(V, ks, Vector{V}(vs), budget)
    end
end

Map{V}(pairs; limits::Limits=Limits()) where {V} = withbudget(limits; direction=:encode) do budget
    ks = String[]
    vs = V[]
    for (k, v) in pairs
        push!(ks, mapkey(k))
        push!(vs, convert(V, v))
    end
    buildmap(V, ks, vs, budget)
end

mapkey(k::AbstractString) = String(k)
mapkey(k::Symbol) = String(k)
mapkey(k) = throw(ArgumentError("map keys must be strings or symbols, got $(typeof(k))"))

"""
    buildmap(V, keys, vals, budget) -> Map{V}

The construction used by decoding and the public constructors: reserve the `npairs` permutation plus
the merge-sort scratch, sort the candidate indices by key bytes (every compared byte charged to the
comparison rule), resolve duplicates last-wins into the first occurrence's position, and compact in
place (the vectors keep their `npairs` capacity).
"""
function buildmap(::Type{V}, keys::Vector{String}, vals::Vector{V}, budget::Union{Nothing,Budget}) where {V}
    n = length(keys)
    n <= typemax(Int32) || throw(ArgumentError("a map cannot hold more than $(typemax(Int32)) pairs"))
    budget === nothing || reserve!(budget, 4 * n + 4 * cld(n, 2) + 128)
    perm = Vector{Int32}(undef, n)
    for i in 1:n
        perm[i] = Int32(i)
    end
    scratch = Vector{Int32}(undef, cld(n, 2))
    compared = mergesort!(perm, scratch, keys)
    budget === nothing || addcompare!(budget, compared + 4 * n)
    # duplicates: the sorted run groups equal keys; the earliest index keeps the position, the last value wins
    ndup = 0
    if n > 1
        keep = trues(n)
        i = 1
        while i <= n
            j = i
            while j < n && keys[perm[j + 1]] == keys[perm[i]]
                j += 1
            end
            if j > i
                first = minimum(view(perm, i:j))
                last = maximum(view(perm, i:j))
                vals[first] = vals[last]
                for k in i:j
                    perm[k] == first || (keep[perm[k]] = false; ndup += 1)
                end
            end
            i = j + 1
        end
        if ndup > 0
            newindex = Vector{Int32}(undef, n)
            w = 0
            for i in 1:n
                if keep[i]
                    w += 1
                    keys[w] = keys[i]
                    vals[w] = vals[i]
                    newindex[i] = Int32(w)
                end
            end
            resize!(keys, w)
            resize!(vals, w)
            out = Vector{Int32}(undef, w)
            r = 0
            for k in 1:n
                p = perm[k]
                keep[p] || continue
                r += 1
                out[r] = newindex[p]
            end
            perm = out
        end
    end
    budget === nothing || release!(budget, 4 * cld(n, 2))
    return Map{V}(keys, vals, perm)
end

"""
    mergesort!(perm, scratch, keys) -> compared bytes

Stable merge sort of `perm` by `keys[perm[i]]` (byte order) using `scratch` of length `cld(n, 2)`;
returns the number of key bytes compared.
"""
function mergesort!(perm::Vector{Int32}, scratch::Vector{Int32}, keys::Vector{String})
    compared = Ref(0)
    msort!(perm, 1, length(perm), scratch, keys, compared)
    return compared[]
end

function keyless(a::String, b::String, compared::Base.RefValue{Int})
    compared[] += min(sizeof(a), sizeof(b)) + 1
    return isless(a, b)
end

function msort!(v::Vector{Int32}, lo::Int, hi::Int, t::Vector{Int32}, keys::Vector{String}, compared)
    if hi - lo < 16
        for i in lo + 1:hi
            x = v[i]
            j = i - 1
            while j >= lo && keyless(keys[x], keys[v[j]], compared)
                v[j + 1] = v[j]
                j -= 1
            end
            v[j + 1] = x
        end
        return v
    end
    mid = (lo + hi) >>> 1
    msort!(v, lo, mid, t, keys, compared)
    msort!(v, mid + 1, hi, t, keys, compared)
    keyless(keys[v[mid + 1]], keys[v[mid]], compared) || return v   # already ordered
    nl = mid - lo + 1
    copyto!(t, 1, v, lo, nl)
    i = 1; j = mid + 1; k = lo
    while i <= nl && j <= hi
        if keyless(keys[v[j]], keys[t[i]], compared)
            v[k] = v[j]; j += 1
        else
            v[k] = t[i]; i += 1
        end
        k += 1
    end
    while i <= nl
        v[k] = t[i]; i += 1; k += 1
    end
    return v
end

Base.length(m::Map) = length(m.keys)
Base.isempty(m::Map) = isempty(m.keys)

function Base.iterate(m::Map, i::Int=1)
    i > length(m.keys) && return nothing
    return (m.keys[i] => m.vals[i], i + 1)
end

Base.keys(m::Map) = m.keys
Base.values(m::Map) = m.vals

function keyposition(m::Map, k::AbstractString)
    lo = 1; hi = length(m.perm)
    while lo <= hi
        mid = (lo + hi) >>> 1
        km = m.keys[m.perm[mid]]
        c = cmp(km, k)
        c == 0 && return Int(m.perm[mid])
        c < 0 ? (lo = mid + 1) : (hi = mid - 1)
    end
    return 0
end

Base.haskey(m::Map, k::AbstractString) = keyposition(m, k) != 0
Base.haskey(m::Map, k::Symbol) = haskey(m, String(k))

function Base.getindex(m::Map, k::AbstractString)
    i = keyposition(m, k)
    i == 0 && throw(KeyError(k))
    return m.vals[i]
end
Base.getindex(m::Map, k::Symbol) = m[String(k)]

function Base.get(m::Map, k::AbstractString, default)
    i = keyposition(m, k)
    i == 0 && return default
    return m.vals[i]
end
Base.get(m::Map, k::Symbol, default) = get(m, String(k), default)

Base.:(==)(a::Map, b::Map) = a.keys == b.keys && a.vals == b.vals
Base.hash(a::Map, h::UInt) = hash(a.vals, hash(a.keys, hash(:AvroMap, h)))

function Base.show(io::IO, m::Map{V}) where {V}
    print(io, "Avro.Map{", V, "}(")
    for (i, (k, v)) in enumerate(m)
        i > 1 && print(io, ", ")
        show(io, k); print(io, " => "); show(io, v)
    end
    print(io, ")")
    return nothing
end

# ---- identity-bearing values -------------------------------------------------------------------------

"""
    Avro.Record(schema::RecordSchema, values::Vector{Any}; limits=Limits())

The generic record: field values in schema order. `record[:f]`, `record["f"]` and `record.f` look a
field up by name **without interning** (`keys(record)` returns strings); the Tables row interface is
provided by `Avro.Row`. Equality is structural.
"""
struct Record
    schema::RecordSchema
    values::Vector{Any}
end

function Record(schema::RecordSchema, values; limits::Limits=Limits())
    vs = Any[v for v in values]
    length(vs) == length(schema.fields) || throw(ArgumentError("record \"$(fullname(schema))\" has $(length(schema.fields)) fields, got $(length(vs)) values"))
    return withbudget(limits; direction=:encode) do budget
        reserve!(budget, 56 + 8 * length(vs))
        Record(schema, vs)
    end
end

function fieldposition(r::Record, name::AbstractString)
    i = get(getfield(r, :schema).fieldindex, String(name), 0)
    i == 0 && throw(KeyError(name))
    return i
end

Base.getindex(r::Record, name::AbstractString) = getfield(r, :values)[fieldposition(r, name)]
Base.getindex(r::Record, name::Symbol) = r[String(name)]
Base.getindex(r::Record, i::Integer) = getfield(r, :values)[i]
function Base.getproperty(r::Record, name::Symbol)
    name === :schema && return getfield(r, :schema)
    name === :values && return getfield(r, :values)
    return r[String(name)]
end
Base.propertynames(r::Record, private::Bool=false) = (:schema, :values)
Base.keys(r::Record) = [f.name for f in getfield(r, :schema).fields]
Base.length(r::Record) = length(getfield(r, :values))
Base.haskey(r::Record, name::AbstractString) = haskey(getfield(r, :schema).fieldindex, String(name))
Base.haskey(r::Record, name::Symbol) = haskey(r, String(name))
Base.get(r::Record, name, default) = haskey(r, name) ? r[name] : default
Base.:(==)(a::Record, b::Record) = fullname(getfield(a, :schema)) == fullname(getfield(b, :schema)) && getfield(a, :values) == getfield(b, :values)
Base.hash(a::Record, h::UInt) = hash(getfield(a, :values), hash(fullname(getfield(a, :schema)), hash(:AvroRecord, h)))

function Base.show(io::IO, r::Record)
    print(io, "Avro.Record(", fullname(getfield(r, :schema)), ": ")
    for (i, f) in enumerate(getfield(r, :schema).fields)
        i > 1 && print(io, ", ")
        print(io, f.name, "=")
        show(io, getfield(r, :values)[i])
    end
    print(io, ")")
    return nothing
end

"""
    Avro.EnumValue(schema::EnumSchema, index::Integer)

A generic enum value: the 1-based position into `schema.symbols` (`Avro.ordinal(x)` is the zero-based
wire value). `String(x)` is the symbol; `Symbol(x)` is an explicit caller-owned interning action.
Equality is by fullname and symbol string.
"""
struct EnumValue
    schema::EnumSchema
    index::Int32
end

function EnumValue(schema::EnumSchema, index::Integer; limits::Limits=Limits())
    1 <= index <= length(schema.symbols) || throw(ArgumentError("enum \"$(fullname(schema))\" has $(length(schema.symbols)) symbols; index $index is out of range"))
    return EnumValue(schema, Int32(index))
end

function EnumValue(schema::EnumSchema, symbol::AbstractString; limits::Limits=Limits())
    i = get(schema.symbolindex, String(symbol), 0)
    i == 0 && throw(ArgumentError("\"$symbol\" is not a symbol of enum \"$(fullname(schema))\""))
    return EnumValue(schema, Int32(i))
end

Base.String(x::EnumValue) = x.schema.symbols[x.index]
Base.Symbol(x::EnumValue) = Symbol(String(x))
Base.:(==)(a::EnumValue, b::EnumValue) = fullname(a.schema) == fullname(b.schema) && String(a) == String(b)
Base.hash(a::EnumValue, h::UInt) = hash(String(a), hash(fullname(a.schema), hash(:AvroEnum, h)))
Base.show(io::IO, x::EnumValue) = print(io, "Avro.EnumValue(", fullname(x.schema), ".", String(x), ")")

"""
    Avro.Fixed(schema::FixedSchema, bytes)

A generic fixed value (copied bytes of exactly `schema.size`). Equality is by fullname, size and bytes.
"""
struct Fixed
    schema::FixedSchema
    bytes::Vector{UInt8}
end

function Fixed(schema::FixedSchema, bytes::AbstractVector{UInt8}; limits::Limits=Limits())
    length(bytes) == schema.size || throw(ArgumentError("fixed \"$(fullname(schema))\" has size $(schema.size), got $(length(bytes)) bytes"))
    return withbudget(limits; direction=:encode) do budget
        reserve!(budget, 56 + length(bytes))
        Fixed(schema, Vector{UInt8}(bytes))
    end
end

Base.:(==)(a::Fixed, b::Fixed) = fullname(a.schema) == fullname(b.schema) && a.schema.size == b.schema.size && a.bytes == b.bytes
Base.hash(a::Fixed, h::UInt) = hash(a.bytes, hash(fullname(a.schema), hash(:AvroFixed, h)))
Base.show(io::IO, x::Fixed) = print(io, "Avro.Fixed(", fullname(x.schema), ": 0x", bytes2hex(x.bytes), ")")

"""
    Avro.UnionValue(index::Integer, value)

A generic value of a union that is not the two-branch nullable form: `index` is the 1-based branch
position (`Avro.ordinal(x)` is the wire index); non-copying — `value` is retained. Branch acceptance is
checked at encode time against the enclosing union.
"""
struct UnionValue
    index::Int
    value::Any
    function UnionValue(index::Integer, value)
        index >= 1 || throw(ArgumentError("union branch index must be ≥ 1 (1-based), got $index"))
        return new(Int(index), value)
    end
end

Base.:(==)(a::UnionValue, b::UnionValue) = a.index == b.index && isequal(a.value, b.value)
Base.hash(a::UnionValue, h::UInt) = hash(a.value, hash(a.index, hash(:AvroUnion, h)))
Base.show(io::IO, x::UnionValue) = (print(io, "Avro.UnionValue(", x.index, ", "); show(io, x.value); print(io, ")"))

"""
    Avro.ordinal(x::EnumValue) / Avro.ordinal(x::UnionValue) -> Int

The zero-based wire ordinal of an enum symbol or union branch.
"""
ordinal(x::EnumValue) = Int(x.index) - 1
ordinal(x::UnionValue) = x.index - 1

# ---- the generic Julia type of a schema (plan §4.6) --------------------------------------------------

juliatype(::NullSchema) = Missing
juliatype(::BooleanSchema) = Bool
juliatype(s::IntSchema) = s.logical isa DateLogical ? Date : (s.logical isa TimeMillis ? Time : Int32)
function juliatype(s::LongSchema)
    l = s.logical
    l isa TimeMicros && return Time
    l isa TimestampMillis && return Timestamp{Millisecond}
    l isa TimestampMicros && return Timestamp{Microsecond}
    l isa TimestampNanos && return Timestamp{Nanosecond}
    l isa LocalTimestampMillis && return LocalTimestamp{Millisecond}
    l isa LocalTimestampMicros && return LocalTimestamp{Microsecond}
    l isa LocalTimestampNanos && return LocalTimestamp{Nanosecond}
    return Int64
end
juliatype(::FloatSchema) = Float32
juliatype(::DoubleSchema) = Float64
juliatype(s::BytesSchema) = s.logical isa DecimalLogical ? decimaltype(s.logical) : Vector{UInt8}
juliatype(s::StringSchema) = s.logical isa UUIDLogical ? UUID : String
function juliatype(s::FixedSchema)
    s.logical isa DecimalLogical && return decimaltype(s.logical)
    s.logical isa UUIDLogical && return UUID
    s.logical isa DurationLogical && return Duration
    return Fixed
end
juliatype(::EnumSchema) = EnumValue
juliatype(::RecordSchema) = Record
decimaltype(l::DecimalLogical) = l.precision <= 38 ? Decimal : WideDecimal

function elementtype(s::Schema)
    t = juliatype(s)
    (t === Vector{Any} || t === Map{Any} || t <: Vector || t <: Map) && return Any
    return t
end

juliatype(s::ArraySchema) = Vector{elementtype(s.items)}
juliatype(s::MapSchema) = Map{elementtype(s.values)}

function juliatype(s::UnionSchema)
    nullable = nullablebranch(s)
    nullable == 0 && return UnionValue
    other = s.branches[3 - nullable]
    return Union{Missing,juliatype(other)}
end

"""
    nullablebranch(union) -> Int

For the two-branch nullable form (`["null", T]` / `[T, "null"]`) the position of the `null` branch;
0 otherwise.
"""
function nullablebranch(s::UnionSchema)
    length(s.branches) == 2 || return 0
    s.branches[1] isa NullSchema && !(s.branches[2] isa NullSchema) && return 1
    s.branches[2] isa NullSchema && !(s.branches[1] isa NullSchema) && return 2
    return 0
end
