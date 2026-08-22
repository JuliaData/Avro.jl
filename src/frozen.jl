# Frozen containers (plan §4.2): the parser fills them and calls `freeze!` once; afterwards every
# mutating method throws, so nothing reachable from a frozen schema can be mutated through the public
# API. No hashing: `FrozenDict` is a
# sorted key vector with binary search (the package never performs hash-table lookups of untrusted
# keys, plan decision 35). Schema nodes hold these containers in `const` fields.

struct FrozenError <: Exception
    what::String
end

Base.showerror(io::IO, e::FrozenError) = print(io, "FrozenError: cannot mutate a frozen ", e.what)

mutable struct FrozenVector{T} <: AbstractVector{T}
    const data::Vector{T}
    frozen::Bool
end

FrozenVector{T}() where {T} = FrozenVector{T}(T[], false)
FrozenVector(v::Vector{T}) where {T} = FrozenVector{T}(v, false)

Base.size(v::FrozenVector) = size(v.data)
Base.IndexStyle(::Type{<:FrozenVector}) = IndexLinear()
Base.@propagate_inbounds Base.getindex(v::FrozenVector, i::Int) = v.data[i]

function Base.setindex!(v::FrozenVector, x, i::Int)
    v.frozen && throw(FrozenError("vector"))
    v.data[i] = x
    return v
end

function Base.push!(v::FrozenVector, x)
    v.frozen && throw(FrozenError("vector"))
    push!(v.data, x)
    return v
end

function Base.empty!(v::FrozenVector)
    v.frozen && throw(FrozenError("vector"))
    empty!(v.data)
    return v
end

Base.resize!(v::FrozenVector, n::Integer) = (v.frozen && throw(FrozenError("vector")); resize!(v.data, n); v)
Base.pop!(v::FrozenVector) = (v.frozen && throw(FrozenError("vector")); pop!(v.data))
Base.append!(v::FrozenVector, xs) = (v.frozen && throw(FrozenError("vector")); append!(v.data, xs); v)
Base.insert!(v::FrozenVector, i::Integer, x) = (v.frozen && throw(FrozenError("vector")); insert!(v.data, i, x); v)
Base.deleteat!(v::FrozenVector, i) = (v.frozen && throw(FrozenError("vector")); deleteat!(v.data, i); v)
Base.copy(v::FrozenVector{T}) where {T} = FrozenVector{T}(copy(v.data), false)

isfrozen(v::FrozenVector) = v.frozen

"""
    FrozenDict{K,V}

An insertion-free, sorted-key dictionary (binary search on `keys`); no hashing. Keys are compared with
`isless`/`==` on their `K` values (`String` keys compare by bytes).
"""
mutable struct FrozenDict{K,V} <: AbstractDict{K,V}
    const keys::Vector{K}
    const vals::Vector{V}
    frozen::Bool
end

FrozenDict{K,V}() where {K,V} = FrozenDict{K,V}(K[], V[], false)

function FrozenDict{K,V}(pairs) where {K,V}
    d = FrozenDict{K,V}()
    for (k, v) in pairs
        d[k] = v
    end
    return d
end

Base.length(d::FrozenDict) = length(d.keys)
Base.isempty(d::FrozenDict) = isempty(d.keys)
isfrozen(d::FrozenDict) = d.frozen

function keyindex(d::FrozenDict{K}, k) where {K}
    i = searchsortedfirst(d.keys, k)
    i <= length(d.keys) && d.keys[i] == k && return i
    return 0
end

Base.haskey(d::FrozenDict, k) = keyindex(d, k) != 0

function Base.getindex(d::FrozenDict, k)
    i = keyindex(d, k)
    i == 0 && throw(KeyError(k))
    return d.vals[i]
end

function Base.get(d::FrozenDict, k, default)
    i = keyindex(d, k)
    i == 0 && return default
    return d.vals[i]
end

function Base.setindex!(d::FrozenDict{K,V}, v, k) where {K,V}
    d.frozen && throw(FrozenError("dict"))
    i = searchsortedfirst(d.keys, k)
    if i <= length(d.keys) && d.keys[i] == k
        d.vals[i] = v
    else
        insert!(d.keys, i, convert(K, k))
        insert!(d.vals, i, convert(V, v))
    end
    return d
end

function Base.delete!(d::FrozenDict, k)
    d.frozen && throw(FrozenError("dict"))
    i = keyindex(d, k)
    i == 0 && return d
    deleteat!(d.keys, i)
    deleteat!(d.vals, i)
    return d
end

function Base.empty!(d::FrozenDict)
    d.frozen && throw(FrozenError("dict"))
    empty!(d.keys)
    empty!(d.vals)
    return d
end

function Base.iterate(d::FrozenDict, i::Int=1)
    i > length(d.keys) && return nothing
    return (d.keys[i] => d.vals[i], i + 1)
end

Base.keys(d::FrozenDict) = d.keys
Base.values(d::FrozenDict) = d.vals
Base.copy(d::FrozenDict{K,V}) where {K,V} = FrozenDict{K,V}(copy(d.keys), copy(d.vals), false)

"""
    FrozenRef{T}

A reference that can be filled exactly once (`fill!`) and read with `[]`; reading before the fill throws.
"""
mutable struct FrozenRef{T}
    value::Union{Nothing,Some{T}}
end

FrozenRef{T}() where {T} = FrozenRef{T}(nothing)
FrozenRef(x::T) where {T} = FrozenRef{T}(Some(x))

isfilled(r::FrozenRef) = r.value !== nothing

function Base.getindex(r::FrozenRef)
    v = r.value
    v === nothing && throw(FrozenError("unfilled reference (read before fill)"))
    return something(v)
end

function fillonce!(r::FrozenRef{T}, x) where {T}
    r.value === nothing || throw(FrozenError("reference (already filled)"))
    r.value = Some(convert(T, x))
    return r
end

# ---- frozen JSON trees ----------------------------------------------------------------------------

"""
    JSONNumber(text)

A raw JSON number token kept lexically (plan §4.2): metadata numbers are never materialised as `BigInt`
or `BigFloat`; equality and hashing are by the token bytes (`1`, `1.0` and `1e0` differ).
"""
struct JSONNumber
    text::String
end

Base.:(==)(a::JSONNumber, b::JSONNumber) = a.text == b.text
Base.hash(a::JSONNumber, h::UInt) = hash(a.text, hash(:JSONNumber, h))

"""
    JSONArray(items)

An immutable JSON array of frozen JSON values.
"""
struct JSONArray
    items::FrozenVector{Any}
end

"""
    JSONObject(members)

An immutable JSON object with sorted keys (insertion order is not preserved; the printer re-emits
members in the order recorded in `order`, which the parser fills with the source order).
"""
struct JSONObject
    members::FrozenDict{String,Any}
    order::FrozenVector{String}
    spans::FrozenVector{UnitRange{Int}}   # source byte span of each member value, in source order (empty when unknown)
end

JSONObject(members::FrozenDict{String,Any}, order::FrozenVector{String}) = JSONObject(members, order, FrozenVector{UnitRange{Int}}())

const FrozenJSON = Union{Nothing,Bool,Int64,Float64,String,JSONNumber,JSONArray,JSONObject}

Base.:(==)(a::JSONArray, b::JSONArray) = a.items.data == b.items.data
Base.hash(a::JSONArray, h::UInt) = hash(a.items.data, hash(:JSONArray, h))
Base.:(==)(a::JSONObject, b::JSONObject) = a.members.keys == b.members.keys && a.members.vals == b.members.vals
Base.hash(a::JSONObject, h::UInt) = hash(a.members.vals, hash(a.members.keys, hash(:JSONObject, h)))
Base.length(a::JSONArray) = length(a.items)
Base.getindex(a::JSONArray, i::Int) = a.items[i]
Base.iterate(a::JSONArray, i::Int=1) = i > length(a.items) ? nothing : (a.items[i], i + 1)
Base.length(o::JSONObject) = length(o.members)
Base.haskey(o::JSONObject, k::AbstractString) = haskey(o.members, String(k))
Base.getindex(o::JSONObject, k::AbstractString) = o.members[String(k)]
Base.get(o::JSONObject, k::AbstractString, default) = get(o.members, String(k), default)
Base.keys(o::JSONObject) = o.order.data

"""
    freeze!(x)

Freeze a frozen container and, recursively, every frozen container reachable from it. Returns `x`.
"""
freeze!(x) = x

function freeze!(v::FrozenVector)
    v.frozen && return v
    v.frozen = true
    for x in v.data
        freeze!(x)
    end
    return v
end

function freeze!(d::FrozenDict)
    d.frozen && return d
    d.frozen = true
    for x in d.vals
        freeze!(x)
    end
    return d
end

function freeze!(a::JSONArray)
    freeze!(a.items)
    return a
end

function freeze!(o::JSONObject)
    freeze!(o.members)
    freeze!(o.order)
    freeze!(o.spans)
    return o
end
