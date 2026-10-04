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

function Map{V}() where {V}
    return Map{V}(String[], V[], Int32[])
end

"""
    Avro.Map(pairs; limits=Limits())

Build a map from `key => value` pairs (keys `AbstractString` or `Symbol`); copying, validated, charged
to its own budget scope.
"""
struct InferredMapConstructor
    pairs::Any
end

struct TypedMapConstructor{V}
    pairs::Any
end

function (constructor::InferredMapConstructor)(budget::Budget)
    ks, vs, boxes = collectpairs(Any, constructor.pairs, budget)
    return convertmap(valuetype(vs), ks, vs, boxes, budget)
end

function (constructor::TypedMapConstructor{V})(budget::Budget) where {V}
    ks, vs, boxes = collectpairs(V, constructor.pairs, budget)
    return convertmap(V, ks, vs, boxes, budget)
end

function Map(@nospecialize(pairs); limits::Limits=Limits())
    return withbudget(InferredMapConstructor(pairs), limits; direction=:encode)
end

function Map{V}(@nospecialize(pairs); limits::Limits=Limits()) where {V}
    return withbudget(TypedMapConstructor{V}(pairs), limits; direction=:encode)
end

# Copy `key => value` pairs through exact-replacement builders under the constructor's budget.
function collectpairs(::Type{V}, @nospecialize(pairs), budget::Budget) where {V}
    trait = Base.IteratorSize(typeof(pairs))
    capacity = trait isa Union{Base.HasLength,Base.HasShape} ? min(length(pairs), 1024) : 0
    capacity >= 0 || throw(ArgumentError("map pair count must be non-negative"))
    keys = BuildBuf{String}(budget, capacity)
    vals = BuildBuf{V}(budget, capacity)
    boxes = 0
    countvalues!(budget)                               # the map itself
    for (k, v) in pairs
        countvalues!(budget)
        keys.len < typemax(Int32) || throw(ArgumentError("a map cannot hold more than $(typemax(Int32)) pairs"))
        k isa Union{AbstractString,Symbol} || mapkey(k) # public constructors use `ArgumentError`
        key = copymapkey(k, budget)
        value, owned = preparedmapvalue(V, v, budget)
        box = V === Any && isbits(value) ? boxbytes(typeof(value)) : 0
        addinput!(budget, checked_add(checked_add(checked_add(sizeof(key), 8), box), owned))
        push!(keys, budget, key)
        box > 0 && reserve!(budget, box)
        push!(vals, budget, value)
        box > 0 && allocated!(budget, box)
        boxes = checked_add(boxes, box)
    end
    return finishbuild!(keys, budget), finishbuild!(vals, budget), boxes
end

function convertmap(::Type{V}, ks::Vector{String}, vs::Vector{Any}, boxes::Int,
                    budget::Budget) where {V}
    V === Any && return buildmap(Any, ks, vs, budget; ownvalues=false)
    charge = vectorbytes(V, length(vs))
    reserve!(budget, charge)
    converted = Vector{V}(undef, length(vs))
    allocated!(budget, charge)
    for i in eachindex(vs)
        converted[i], owned = preparedmapvalue(V, vs[i], budget)
        addinput!(budget, owned)
    end
    vs = nothing
    release!(budget, vectorbytes(Any, length(converted)) + boxes)
    return buildmap(V, ks, converted, budget; ownvalues=V === String)
end

function convertmap(::Type{V}, ks::Vector{String}, vs::Vector{V}, boxes::Int,
                    budget::Budget) where {V}
    boxes == 0 || V === Any || throw(ArgumentError("concrete map values retained unexpected boxes"))
    return buildmap(V, ks, vs, budget; ownvalues=V === String)
end

function preparedmapvalue(::Type{Any}, @nospecialize(value), ::Budget)
    return (value, 0)
end

function preparedmapvalue(::Type{String}, value, budget::Budget)
    value isa AbstractString ||
        throw(ArgumentError(diagnosticstring(
            budget,
            "map value cannot be converted to String without an allocating user conversion")))
    isstrictutf8(value) || encodeerror("map string value is not valid UTF-8", value)
    text = ownedstringcopy(value, budget)
    return (text, stringbytes(sizeof(text)))
end

function preparedmapvalue(::Type{V}, value, budget::Budget) where {V}
    value isa V && return (value, 0)
    (isbitstype(V) || Base.isbitsunion(V)) && return (convert(V, value), 0)
    throw(ArgumentError(diagnosticstring(
        budget,
        "map value is not an instance of the requested concrete value type and cannot be converted without an allocating user conversion")))
end

# The value type of an inferred map: the promoted join of the value types (`Union{Missing,T}` for
# `missing` and `T`), narrowed to the generic model's element types so the result is a member of `E`.
function valuetype(vs::Vector{Any})
    isempty(vs) && return Any
    return narrowelement(mapreduce(typeof, Base.promote_typejoin, vs))
end

function mapkey(k::AbstractString)
    return String(k)
end

function mapkey(k::Symbol)
    return String(k)
end

function mapkey(k)
    throw(ArgumentError("map keys must be strings or symbols, got $(typeof(k))"))
end

"Copy one map key into operation-owned storage after reserving its exact charge."
function copymapkey(k::Union{AbstractString,Symbol}, budget::Budget)
    n = sizeof(k)
    charge = stringbytes(n)
    checkpoint = budgetcheckpoint(budget)
    key = nothing
    try
        reserve!(budget, charge)
        value = k isa String ? GC.@preserve(k, unsafe_string(pointer(k), n)) : String(k)
        allocated!(budget, charge)
        key = value
        isstrictutf8(key) || encodeerror("map key is not valid UTF-8", k)
        return key
    catch
        key = nothing
        rollbackreservations!(budget, checkpoint)
        rethrow()
    end
end

function copymapkey(k, budget::Budget)
    return encodeerror("map keys must be strings or symbols", k)
end

"""
    buildmap(V, keys, vals, budget) -> Map{V}

The construction used by decoding and the public constructors: reserve the `npairs` permutation plus
the merge-sort scratch, sort the candidate indices by key bytes (every compared byte charged to the
comparison rule), resolve duplicates last-wins into the first occurrence's position, and compact in
place (the vectors keep their `npairs` capacity).
"""
function buildmap(::Type{V}, keys::Vector{String}, vals::Vector{V}, budget::Union{Nothing,Budget};
                  duplicateposition::Union{Nothing,Int}=nothing, ownvalues::Bool=true) where {V}
    n = length(keys)
    valuemovework = isbitstype(V) || Base.isbitsunion(V) ? 0 : 8
    n <= typemax(Int32) || throw(ArgumentError("a map cannot hold more than $(typemax(Int32)) pairs"))
    budget === nothing || reserve!(budget, mapshellbytes(n) + vectorbytes(Int32, cld(n, 2)))   # struct, permutation and the sort scratch
    perm = Vector{Int32}(undef, n)
    scratch = Vector{Int32}(undef, cld(n, 2))
    budget === nothing || allocated!(budget, vectorbytes(Int32, n) + vectorbytes(Int32, cld(n, 2)))   # the Map shell settles at construction
    mergesort!(perm, scratch, keys, budget)
    # duplicates: the sorted run groups equal keys; the earliest index keeps the position, the last value wins
    ndup = 0
    discardbytes = 0
    if n > 1
        budget === nothing || reserve!(budget, vectorbytes(Bool, n))
        keep = fill(true, n)
        budget === nothing || allocated!(budget, vectorbytes(Bool, n))
        i = 1
        while i <= n
            j = i
            while j < n && keyequal(keys[perm[j + 1]], keys[perm[i]], budget)
                j += 1
            end
            if j > i
                if duplicateposition !== nothing
                    keybytes = sizeof(keys[perm[i]])
                    msg = budget === nothing ? "duplicate metadata key" :
                          diagnosticstring(budget, "duplicate metadata key (", keybytes, " bytes)")
                    throw(DataError(msg, duplicateposition))
                end
                first = minimum(view(perm, i:j))
                last = maximum(view(perm, i:j))
                for k in i:j
                    p = perm[k]
                    p == first || (discardbytes = checked_add(discardbytes, stringbytes(sizeof(keys[p]))))
                    p == last || (discardbytes = checked_add(discardbytes, mapvaluebytes(V, vals[p], ownvalues)))
                end
                vals[first] = vals[last]
                budget === nothing || addcompare!(budget, valuemovework)
                for k in i:j
                    perm[k] == first || (keep[perm[k]] = false; ndup += 1)
                end
            end
            i = j + 1
        end
        if ndup > 0
            budget === nothing || reserve!(budget, vectorbytes(Int32, n))
            newindex = Vector{Int32}(undef, n)
            budget === nothing || allocated!(budget, vectorbytes(Int32, n))
            w = 0
            for i in 1:n
                if keep[i]
                    w += 1
                    keys[w] = keys[i]
                    vals[w] = vals[i]
                    newindex[i] = Int32(w)
                end
            end
            budget === nothing || addcompare!(budget, checked_mul(12 + valuemovework, w))
            resize!(keys, w)
            resize!(vals, w)
            budget === nothing || release!(budget, discardbytes)
            r = 0
            for k in 1:n
                p = perm[k]
                keep[p] || continue
                r += 1
                perm[r] = newindex[p]
            end
            budget === nothing || addcompare!(budget, checked_mul(4, r))
            resize!(perm, w)                                                 # keep the original `npairs` capacity
            budget === nothing || release!(budget, vectorbytes(Int32, n))   # the index map dies here
        end
        budget === nothing || release!(budget, vectorbytes(Bool, n))         # the keep mask dies with this block
    end
    budget === nothing || release!(budget, vectorbytes(Int32, cld(n, 2)))
    m = Map{V}(keys, vals, perm)
    budget === nothing || allocated!(budget, STORAGE[].map)
    return m
end

function mapvaluebytes(::Type{V}, @nospecialize(value), ownvalues::Bool) where {V}
    ownvalues && return elementbytes(V, value)
    V === Any && isbits(value) && return boxbytes(typeof(value))
    return 0
end

function keyequal(a::String, b::String, budget::Union{Nothing,Budget})
    budget === nothing || addcompare!(budget, min(sizeof(a), sizeof(b)) + 1)
    return a == b
end

"""
    mergesort!(perm, scratch, keys, budget=nothing)

Stable merge sort of `perm` by `keys[perm[i]]` (byte order) using `scratch` of length `cld(n, 2)`;
initialises `perm` and charges every key comparison and four-byte permutation move before doing it.
"""
function mergesort!(perm::Vector{Int32}, scratch::Vector{Int32}, keys::Vector{String},
                    budget::Union{Nothing,Budget}=nothing)
    for i in eachindex(perm)
        budget === nothing || addcompare!(budget, 4)
        perm[i] = Int32(i)
    end
    msort!(perm, 1, length(perm), scratch, keys, budget)
    return nothing
end

function keyless(a::String, b::String, budget::Union{Nothing,Budget})
    budget === nothing || addcompare!(budget, min(sizeof(a), sizeof(b)) + 1)
    return isless(a, b)
end

function addpermutationmoves!(budget::Union{Nothing,Budget}, n::Int)
    budget === nothing || addcompare!(budget, checked_mul(4, n))
    return nothing
end

function msort!(v::Vector{Int32}, lo::Int, hi::Int, t::Vector{Int32}, keys::Vector{String}, budget)
    if hi - lo < 16
        for i in lo + 1:hi
            x = v[i]
            j = i - 1
            while j >= lo && keyless(keys[x], keys[v[j]], budget)
                addpermutationmoves!(budget, 1)
                v[j + 1] = v[j]
                j -= 1
            end
            addpermutationmoves!(budget, 1)
            v[j + 1] = x
        end
        return v
    end
    mid = (lo + hi) >>> 1
    msort!(v, lo, mid, t, keys, budget)
    msort!(v, mid + 1, hi, t, keys, budget)
    keyless(keys[v[mid + 1]], keys[v[mid]], budget) || return v   # already ordered
    nl = mid - lo + 1
    addpermutationmoves!(budget, nl)
    copyto!(t, 1, v, lo, nl)
    i = 1; j = mid + 1; k = lo
    while i <= nl && j <= hi
        if keyless(keys[v[j]], keys[t[i]], budget)
            addpermutationmoves!(budget, 1)
            v[k] = v[j]; j += 1
        else
            addpermutationmoves!(budget, 1)
            v[k] = t[i]; i += 1
        end
        k += 1
    end
    while i <= nl
        addpermutationmoves!(budget, 1)
        v[k] = t[i]; i += 1; k += 1
    end
    return v
end

function Base.length(m::Map)
    return length(m.keys)
end

function Base.isempty(m::Map)
    return isempty(m.keys)
end

function Base.iterate(m::Map, i::Int=1)
    i > length(m.keys) && return nothing
    return (m.keys[i] => m.vals[i], i + 1)
end

function Base.keys(m::Map)
    return Base.KeySet(m)
end

function Base.values(m::Map)
    return Base.ValueIterator(m)
end

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

"Compare an owned UTF-8 string with a Symbol without materialising the Symbol as a String."
function stringsymbolcompare(text::String, symbol::Symbol)
    nt = sizeof(text)
    ns = sizeof(symbol)
    source = Base.unsafe_convert(Ptr{UInt8}, symbol)
    GC.@preserve text symbol begin
        for i in 1:min(nt, ns)
            a = codeunit(text, i)
            b = unsafe_load(source, i)
            a == b || return a < b ? -1 : 1
        end
    end
    return cmp(nt, ns)
end

function keyposition(m::Map, k::Symbol)
    lo = 1; hi = length(m.perm)
    while lo <= hi
        mid = (lo + hi) >>> 1
        km = m.keys[m.perm[mid]]
        c = stringsymbolcompare(km, k)
        c == 0 && return Int(m.perm[mid])
        c < 0 ? (lo = mid + 1) : (hi = mid - 1)
    end
    return 0
end

function Base.haskey(m::Map, k::AbstractString)
    return keyposition(m, k) != 0
end

function Base.haskey(m::Map, k::Symbol)
    return keyposition(m, k) != 0
end

function Base.getindex(m::Map, k::AbstractString)
    i = keyposition(m, k)
    i == 0 && throw(KeyError(k))
    return m.vals[i]
end

function Base.getindex(m::Map, k::Symbol)
    i = keyposition(m, k)
    i == 0 && throw(KeyError(k))
    return m.vals[i]
end

function Base.get(m::Map, k::AbstractString, default)
    i = keyposition(m, k)
    i == 0 && return default
    return m.vals[i]
end

function Base.get(m::Map, k::Symbol, default)
    i = keyposition(m, k)
    i == 0 && return default
    return m.vals[i]
end

# Equality and hashing follow the sorted key order (insertion order is not part of the value).
function Base.:(==)(a::Map, b::Map)
    length(a.perm) == length(b.perm) || return false
    result = true
    for i in eachindex(a.perm)
        ia, ib = a.perm[i], b.perm[i]
        a.keys[ia] == b.keys[ib] || return false
        eq = a.vals[ia] == b.vals[ib]
        eq === false && return false
        eq === missing && (result = missing)
    end
    return result
end

function Base.isequal(a::Map, b::Map)
    length(a.perm) == length(b.perm) || return false
    for i in eachindex(a.perm)
        ia, ib = a.perm[i], b.perm[i]
        (a.keys[ia] == b.keys[ib] && isequal(a.vals[ia], b.vals[ib])) || return false
    end
    return true
end

function Base.hash(a::Map, h::UInt)
    h = hash(:AvroMap, h)
    for i in a.perm
        h = hash(a.vals[i], hash(a.keys[i], h))
    end
    return h
end

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
    function Record(schema::RecordSchema, values::Vector{Any}, ::Val{:unchecked})
        return new(schema, values)
    end

    function Record(schema::RecordSchema, values; limits::Limits=Limits())
        nf = length(schema.fields)
        return withbudget(limits; direction=:encode) do budget
            countvalues!(budget)                                         # the record itself
            reserve!(budget, recordbytes(nf))                                # before the values copy
            vs = Vector{Any}(undef, nf)
            allocated!(budget, vectorbytes(Any, nf))
            i = 0
            for v in values
                countvalues!(budget)
                i += 1
                i <= nf || throw(ArgumentError("record \"$(fullname(schema))\" has $nf fields, got more values"))
                box = isbits(v) ? boxbytes(typeof(v)) : 0
                box > 0 && reserve!(budget, box)
                vs[i] = v
                box > 0 && allocated!(budget, box)
            end
            i == nf || throw(ArgumentError("record \"$(fullname(schema))\" has $nf fields, got $i values"))
            r = new(schema, vs)
            allocated!(budget, recordbytes(nf) - vectorbytes(Any, nf))
            return r
        end
    end
end

function fieldposition(r::Record, name::AbstractString)
    i = get(getfield(r, :schema).fieldindex, name, 0)
    i == 0 && throw(KeyError(name))
    return i
end

function symbolfieldposition(r::Record, name::Symbol)
    index = getfield(r, :schema).fieldindex
    lo = 1; hi = length(index.keys)
    while lo <= hi
        mid = (lo + hi) >>> 1
        c = stringsymbolcompare(index.keys[mid], name)
        c == 0 && return index.vals[mid]
        c < 0 ? (lo = mid + 1) : (hi = mid - 1)
    end
    return 0
end

function fieldposition(r::Record, name::Symbol)
    i = symbolfieldposition(r, name)
    i != 0 && return i
    throw(KeyError(name))
end

function Base.getindex(r::Record, name::AbstractString)
    return getfield(r, :values)[fieldposition(r, name)]
end

function Base.getindex(r::Record, name::Symbol)
    return getfield(r, :values)[fieldposition(r, name)]
end

function Base.getindex(r::Record, i::Integer)
    return getfield(r, :values)[i]
end

function Base.getproperty(r::Record, name::Symbol)
    name === :schema && return getfield(r, :schema)
    name === :values && return getfield(r, :values)
    return r[name]
end

function Base.propertynames(r::Record, private::Bool=false)
    return (:schema, :values)
end

function Base.keys(r::Record)
    return [f.name for f in getfield(r, :schema).fields]
end

function Base.length(r::Record)
    return length(getfield(r, :values))
end

function Base.haskey(r::Record, name::AbstractString)
    return haskey(getfield(r, :schema).fieldindex, name)
end

function Base.haskey(r::Record, name::Symbol)
    return symbolfieldposition(r, name) != 0
end

function Base.get(r::Record, name, default)
    return haskey(r, name) ? r[name] : default
end

function Base.:(==)(a::Record, b::Record)
    return fullnameequal(getfield(a, :schema).name, getfield(b, :schema).name) && getfield(a, :values) == getfield(b, :values)
end

function Base.isequal(a::Record, b::Record)
    return fullnameequal(getfield(a, :schema).name, getfield(b, :schema).name) && isequal(getfield(a, :values), getfield(b, :values))
end

function Base.hash(a::Record, h::UInt)
    return hash(getfield(a, :values), hash(getfield(a, :schema).name, hash(:AvroRecord, h)))
end

function Base.show(io::IO, r::Record)
    print(io, "Avro.Record(")
    writefullname(io, getfield(r, :schema).name)
    print(io, ": ")
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
    function EnumValue(schema::EnumSchema, index::Int32, ::Val{:unchecked})
        return new(schema, index)
    end

    function EnumValue(schema::EnumSchema, index::Integer; limits::Limits=Limits())
        return withbudget(limits; direction=:encode) do budget
            return publicenumvalue(schema, index, budget)
        end
    end
end

function EnumValue(schema::EnumSchema, symbol::AbstractString; limits::Limits=Limits())
    return withbudget(limits; direction=:encode) do budget
        countvalues!(budget)
        addinput!(budget, sizeof(symbol))
        for (i, candidate) in enumerate(schema.symbols)
            addcompare!(budget, min(sizeof(candidate), sizeof(symbol)) + 1)
            candidate == symbol && return publicenumvalue(schema, i, budget; counted=true)
        end
        throw(ArgumentError(diagnosticstring(
            budget, boundedquoted(symbol), " is not a symbol of enum \"",
            schema.name, "\"")))
    end
end

function publicenumvalue(schema::EnumSchema, index::Integer, budget::Budget; counted::Bool=false)
    counted || countvalues!(budget)
    reserve!(budget, enumvaluebytes())
    1 <= index <= length(schema.symbols) ||
        throw(ArgumentError("enum \"$(fullname(schema))\" has $(length(schema.symbols)) symbols; index $index is out of range"))
    value = EnumValue(schema, Int32(index), Val(:unchecked))
    allocated!(budget, enumvaluebytes())
    return value
end

function Base.String(x::EnumValue)
    return x.schema.symbols[x.index]
end

function Base.Symbol(x::EnumValue)
    return Symbol(String(x))
end

function Base.:(==)(a::EnumValue, b::EnumValue)
    return fullnameequal(a.schema.name, b.schema.name) && String(a) == String(b)
end

function Base.hash(a::EnumValue, h::UInt)
    return hash(String(a), hash(a.schema.name, hash(:AvroEnum, h)))
end

function Base.show(io::IO, x::EnumValue)
    print(io, "Avro.EnumValue(")
    writefullname(io, x.schema.name)
    return print(io, ".", String(x), ")")
end

"""
    Avro.Fixed(schema::FixedSchema, bytes)

A generic fixed value (copied bytes of exactly `schema.size`). Equality is by fullname, size and bytes.
"""
struct Fixed
    schema::FixedSchema
    bytes::Vector{UInt8}
    function Fixed(schema::FixedSchema, bytes::Vector{UInt8}, ::Val{:unchecked})
        return new(schema, bytes)
    end

    function Fixed(schema::FixedSchema, bytes::AbstractVector{UInt8}; limits::Limits=Limits())
        length(bytes) == schema.size || throw(ArgumentError("fixed \"$(fullname(schema))\" has size $(schema.size), got $(length(bytes)) bytes"))
        return withbudget(limits; direction=:encode) do budget
            checkvaluebytes(budget, length(bytes))
            countvalues!(budget)
            reserve!(budget, fixedbytes(length(bytes)))
            f = new(schema, Vector{UInt8}(bytes))
            allocated!(budget, fixedbytes(length(bytes)))
            return f
        end
    end
end

function Base.:(==)(a::Fixed, b::Fixed)
    return fullnameequal(a.schema.name, b.schema.name) && a.schema.size == b.schema.size && a.bytes == b.bytes
end

function Base.hash(a::Fixed, h::UInt)
    return hash(a.bytes, hash(a.schema.name, hash(:AvroFixed, h)))
end

function Base.show(io::IO, x::Fixed)
    print(io, "Avro.Fixed(")
    writefullname(io, x.schema.name)
    return print(io, ": 0x", bytes2hex(x.bytes), ")")
end

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

function Base.:(==)(a::UnionValue, b::UnionValue)
    return a.index == b.index && a.value == b.value
end

function Base.isequal(a::UnionValue, b::UnionValue)
    return a.index == b.index && isequal(a.value, b.value)
end

function Base.hash(a::UnionValue, h::UInt)
    return hash(a.value, hash(a.index, hash(:AvroUnion, h)))
end

function Base.show(io::IO, x::UnionValue)
    return (print(io, "Avro.UnionValue(", x.index, ", "); show(io, x.value); print(io, ")"))
end

"""
    Avro.ordinal(x::EnumValue) / Avro.ordinal(x::UnionValue) -> Int

The zero-based wire ordinal of an enum symbol or union branch.
"""
function ordinal(x::EnumValue)
    return Int(x.index) - 1
end

function ordinal(x::UnionValue)
    return x.index - 1
end

# ---- the generic Julia type of a schema (plan §4.6) --------------------------------------------------

function juliatype(::NullSchema)
    return Missing
end

function juliatype(::BooleanSchema)
    return Bool
end

function juliatype(s::IntSchema)
    return s.logical isa DateLogical ? Date : (s.logical isa TimeMillis ? Time : Int32)
end

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

function juliatype(::FloatSchema)
    return Float32
end

function juliatype(::DoubleSchema)
    return Float64
end

function juliatype(s::BytesSchema)
    return s.logical isa DecimalLogical ? decimaltype(s.logical) : Vector{UInt8}
end

function juliatype(s::StringSchema)
    return s.logical isa UUIDLogical ? UUID : String
end

function juliatype(s::FixedSchema)
    s.logical isa DecimalLogical && return decimaltype(s.logical)
    s.logical isa UUIDLogical && return UUID
    s.logical isa DurationLogical && return Duration
    return Fixed
end

function juliatype(::EnumSchema)
    return EnumValue
end

function juliatype(::RecordSchema)
    return Record
end

function decimaltype(l::DecimalLogical)
    return l.precision <= 38 ? Decimal : WideDecimal
end

# ---- the closed value set E (plan §4.6) --------------------------------------------------------------

"""
    Avro.LEAF_TYPES

The leaf members `L` of the generic value model.
"""
const LEAF_TYPES = (Missing, Bool, Int32, Int64, Float32, Float64, Vector{UInt8}, String, Fixed, EnumValue, Decimal, WideDecimal,
                    UUID, Date, Time, Timestamp{Millisecond}, Timestamp{Microsecond}, Timestamp{Nanosecond},
                    LocalTimestamp{Millisecond}, LocalTimestamp{Microsecond}, LocalTimestamp{Nanosecond}, Duration)

"""
    Avro.valuetypes() -> Vector{Type}

The closed set `E` of types a generic decode can produce: the leaves, `Record`, `UnionValue`,
`Vector{x}`/`Map{x}` for `x` a leaf, `Record`, `UnionValue` or `Union{Missing,y}`, `Vector{Any}`,
`Map{Any}`, and `Union{Missing,x}` for every non-missing member.
"""
function valuetypes()
    elements = Any[LEAF_TYPES..., Record, UnionValue]
    nullable = Any[Union{Missing,x} for x in (LEAF_TYPES[2:end]..., Record)]
    composites = Any[Record, UnionValue]
    for x in (elements..., nullable...)
        push!(composites, Vector{x})
        push!(composites, Map{x})
    end
    push!(composites, Vector{Any})
    push!(composites, Map{Any})
    all = Any[LEAF_TYPES..., composites...]
    for x in (LEAF_TYPES[2:end]..., composites...)
        push!(all, Union{Missing,x})
    end
    return unique(all)
end

"""
    narrowelement(t::Type) -> Type

The element type the generic model stores for values of type `t` inside an array or map (plan §4.6, one
level of typed nesting): a leaf, `Record`, `UnionValue` or `Union{Missing,y}` for a non-missing leaf or
`Record` is kept; every other type (nested arrays and maps, abstract joins) is stored as `Any`.
"""
function narrowelement(@nospecialize(t::Type))
    t isa Union || return (t in LEAF_TYPES || t === Record || t === UnionValue) ? t : Any
    u = Base.nonmissingtype(t)
    return (t === Union{Missing,u} && ((u in LEAF_TYPES && u !== Missing) || u === Record)) ? t : Any
end

function elementtype(s::Schema)
    return narrowelement(juliatype(s))
end

function juliatype(s::ArraySchema)
    return Vector{elementtype(s.items)}
end

function juliatype(s::MapSchema)
    return Map{elementtype(s.values)}
end

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
