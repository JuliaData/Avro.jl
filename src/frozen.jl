# Frozen containers (plan §4.2): the parser fills them and calls `freeze!` once; afterwards every
# mutating method throws, so nothing reachable from a frozen schema can be mutated through the public
# API. No hashing: `FrozenDict` is a
# sorted key vector with binary search (the package never performs hash-table lookups of untrusted
# keys, plan decision 35). Schema nodes hold these containers in `const` fields.

struct FrozenError <: Exception
    what::String
end

function Base.showerror(io::IO, e::FrozenError)
    return print(io, "FrozenError: cannot mutate a frozen ", e.what)
end

mutable struct FrozenVector{T} <: AbstractVector{T}
    const data::Vector{T}
    frozen::Bool
end

function FrozenVector{T}() where {T}
    return FrozenVector{T}(T[], false)
end

function FrozenVector(v::Vector{T}) where {T}
    return FrozenVector{T}(v, false)
end

function Base.size(v::FrozenVector)
    return size(v.data)
end

function Base.IndexStyle(::Type{<:FrozenVector})
    return IndexLinear()
end

Base.@propagate_inbounds function Base.getindex(v::FrozenVector, i::Int)
    return v.data[i]
end

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

function Base.resize!(v::FrozenVector, n::Integer)
    return (v.frozen && throw(FrozenError("vector")); resize!(v.data, n); v)
end

function Base.pop!(v::FrozenVector)
    return (v.frozen && throw(FrozenError("vector")); pop!(v.data))
end

function Base.append!(v::FrozenVector, xs)
    return (v.frozen && throw(FrozenError("vector")); append!(v.data, xs); v)
end

function Base.insert!(v::FrozenVector, i::Integer, x)
    return (v.frozen && throw(FrozenError("vector")); insert!(v.data, i, x); v)
end

function Base.deleteat!(v::FrozenVector, i)
    return (v.frozen && throw(FrozenError("vector")); deleteat!(v.data, i); v)
end

function Base.copy(v::FrozenVector{T}) where {T}
    return FrozenVector{T}(copy(v.data), false)
end

function isfrozen(v::FrozenVector)
    return v.frozen
end

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

function FrozenDict{K,V}() where {K,V}
    return FrozenDict{K,V}(K[], V[], false)
end

function FrozenDict{K,V}(pairs) where {K,V}
    d = FrozenDict{K,V}()
    for (k, v) in pairs
        d[k] = v
    end
    return d
end

function Base.length(d::FrozenDict)
    return length(d.keys)
end

function Base.isempty(d::FrozenDict)
    return isempty(d.keys)
end

function isfrozen(d::FrozenDict)
    return d.frozen
end

function keyindex(d::FrozenDict{K}, k) where {K}
    i = searchsortedfirst(d.keys, k)
    i <= length(d.keys) && d.keys[i] == k && return i
    return 0
end

function Base.haskey(d::FrozenDict, k)
    return keyindex(d, k) != 0
end

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

function Base.keys(d::FrozenDict)
    return d.keys
end

function Base.values(d::FrozenDict)
    return d.vals
end

function Base.copy(d::FrozenDict{K,V}) where {K,V}
    return FrozenDict{K,V}(copy(d.keys), copy(d.vals), false)
end

"""
    FrozenRef{T}

A reference that can be filled exactly once (`fill!`) and read with `[]`; reading before the fill throws.
"""
mutable struct FrozenRef{T}
    value::Union{Nothing,Some{T}}
end

function FrozenRef{T}() where {T}
    return FrozenRef{T}(nothing)
end

function FrozenRef(x::T) where {T}
    return FrozenRef{T}(Some(x))
end

function isfilled(r::FrozenRef)
    return r.value !== nothing
end

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

function Base.:(==)(a::JSONNumber, b::JSONNumber)
    return a.text == b.text
end

function Base.hash(a::JSONNumber, h::UInt)
    return hash(a.text, hash(:JSONNumber, h))
end

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

function JSONObject(members::FrozenDict{String,Any}, order::FrozenVector{String})
    return JSONObject(members, order, FrozenVector{UnitRange{Int}}())
end

const FrozenJSON = Union{Nothing,Bool,Int64,Float64,String,JSONNumber,JSONArray,JSONObject}

function Base.:(==)(a::JSONArray, b::JSONArray)
    return a.items.data == b.items.data
end

function Base.hash(a::JSONArray, h::UInt)
    return hash(a.items.data, hash(:JSONArray, h))
end

function Base.:(==)(a::JSONObject, b::JSONObject)
    return a.members.keys == b.members.keys && a.members.vals == b.members.vals
end

function Base.hash(a::JSONObject, h::UInt)
    return hash(a.members.vals, hash(a.members.keys, hash(:JSONObject, h)))
end

function Base.length(a::JSONArray)
    return length(a.items)
end

function Base.getindex(a::JSONArray, i::Int)
    return a.items[i]
end

function Base.iterate(a::JSONArray, i::Int=1)
    return i > length(a.items) ? nothing : (a.items[i], i + 1)
end

function Base.length(o::JSONObject)
    return length(o.members)
end

function Base.haskey(o::JSONObject, k::AbstractString)
    return haskey(o.members, String(k))
end

function Base.getindex(o::JSONObject, k::AbstractString)
    return o.members[String(k)]
end

function Base.get(o::JSONObject, k::AbstractString, default)
    return get(o.members, String(k), default)
end

function Base.keys(o::JSONObject)
    return o.order.data
end

"""
    freeze!(x)

Freeze a frozen container and, recursively, every frozen container reachable from it. Returns `x`.
"""
function freeze!(x)
    return x
end

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
