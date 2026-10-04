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
    data::Vector{T}          # replaceable: §4.4 exact-replacement growth rebinds it
    cap::Int                 # the prebuilt backing capacity (length(data) may be shorter)
    frozen::Bool
    function FrozenVector{T}(data::Vector{T}, cap::Int, frozen::Bool) where {T}
        return new(data, cap, frozen)
    end
end

function FrozenVector{T}(v::Vector{T}, frozen::Bool) where {T}
    return FrozenVector{T}(v, length(v), frozen)
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

"An empty frozen vector whose backing storage is prebuilt at exact `capacity` (§4.4 growth rule)."
function emptywithcapacity(::Type{FrozenVector{T}}, capacity::Int) where {T}
    data = Vector{T}(undef, max(capacity, 0))
    resize!(data, 0)
    return FrozenVector{T}(data, max(capacity, 0), false)
end

"""
    FrozenDict{K,V}

An insertion-free, sorted-key dictionary (binary search on `keys`); no hashing. Keys are compared with
`isless`/`==` on their `K` values (`String` keys compare by bytes).
"""
mutable struct FrozenDict{K,V} <: AbstractDict{K,V}
    keys::Vector{K}          # replaceable: §4.4 exact-replacement growth rebinds them
    vals::Vector{V}
    cap::Int                 # the prebuilt backing capacity (lengths may be shorter)
    frozen::Bool
    function FrozenDict{K,V}(ks::Vector{K}, vs::Vector{V}, cap::Int, frozen::Bool) where {K,V}
        return new(ks, vs, cap, frozen)
    end
end

function FrozenDict{K,V}(ks::Vector{K}, vs::Vector{V}, frozen::Bool) where {K,V}
    return FrozenDict{K,V}(ks, vs, max(length(ks), length(vs)), frozen)
end

function FrozenDict{K,V}() where {K,V}
    return FrozenDict{K,V}(K[], V[], false)
end

"An empty frozen dictionary whose key and value storage is prebuilt at exact `capacity` (§4.4)."
function emptywithcapacity(::Type{FrozenDict{K,V}}, capacity::Int) where {K,V}
    ks = Vector{K}(undef, max(capacity, 0))
    resize!(ks, 0)
    vs = Vector{V}(undef, max(capacity, 0))
    resize!(vs, 0)
    return FrozenDict{K,V}(ks, vs, max(capacity, 0), false)
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

struct FrozenKeys{K,V} <: AbstractVector{K}
    dict::FrozenDict{K,V}
end

struct FrozenValues{K,V} <: AbstractVector{V}
    dict::FrozenDict{K,V}
end

function Base.size(view::Union{FrozenKeys,FrozenValues})
    return (length(view.dict),)
end

function Base.IndexStyle(::Type{<:Union{FrozenKeys,FrozenValues}})
    return IndexLinear()
end

Base.@propagate_inbounds function Base.getindex(view::FrozenKeys, index::Int)
    return view.dict.keys[index]
end

Base.@propagate_inbounds function Base.getindex(view::FrozenValues, index::Int)
    return view.dict.vals[index]
end

function Base.iterate(view::Union{FrozenKeys,FrozenValues}, index::Int=1)
    index > length(view) && return nothing
    return (view[index], index + 1)
end

function frozenviewerror(view::FrozenKeys)
    throw(FrozenError("dictionary keys"))
end

function frozenviewerror(view::FrozenValues)
    throw(FrozenError("dictionary values"))
end

function Base.setindex!(view::Union{FrozenKeys,FrozenValues}, value, index)
    return frozenviewerror(view)
end

function Base.push!(view::Union{FrozenKeys,FrozenValues}, values...)
    return frozenviewerror(view)
end

function Base.deleteat!(view::Union{FrozenKeys,FrozenValues}, indices)
    return frozenviewerror(view)
end

function Base.empty!(view::Union{FrozenKeys,FrozenValues})
    return frozenviewerror(view)
end

function Base.resize!(view::Union{FrozenKeys,FrozenValues}, size::Integer)
    return frozenviewerror(view)
end

function Base.pop!(view::Union{FrozenKeys,FrozenValues})
    return frozenviewerror(view)
end

function Base.append!(view::Union{FrozenKeys,FrozenValues}, values)
    return frozenviewerror(view)
end

function Base.insert!(view::Union{FrozenKeys,FrozenValues}, index::Integer, value)
    return frozenviewerror(view)
end

function Base.keys(d::FrozenDict)
    return FrozenKeys(d)
end

function Base.values(d::FrozenDict)
    return FrozenValues(d)
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

const EMPTY_JSON_ITEMS = FrozenVector{Any}([], true)
const EMPTY_JSON_ARRAY = JSONArray(EMPTY_JSON_ITEMS)

"""
    JSONObject(members)

An immutable JSON object with sorted keys (insertion order is not preserved; the printer re-emits
members in the order recorded in `order`, which the parser fills with the source order).
"""
struct JSONMemberSpan
    key::UnitRange{Int}
    value::UnitRange{Int}
end

struct JSONObject
    members::FrozenDict{String,Any}
    order::FrozenVector{String}
    spans::FrozenVector{JSONMemberSpan} # key/value source spans in source order (empty when unknown)
end

const EMPTY_JSON_MEMBERS = FrozenDict{String,Any}(String[], [], true)
const EMPTY_JSON_ORDER = FrozenVector{String}(String[], true)
const EMPTY_JSON_SPANS = FrozenVector{JSONMemberSpan}(JSONMemberSpan[], true)
const EMPTY_JSON_OBJECT = JSONObject(EMPTY_JSON_MEMBERS, EMPTY_JSON_ORDER,
                                     EMPTY_JSON_SPANS)

function JSONObject(members::FrozenDict{String,Any}, order::FrozenVector{String})
    isempty(members) && isempty(order) && return EMPTY_JSON_OBJECT
    return JSONObject(members, order, EMPTY_JSON_SPANS)
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
    return o.order
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
