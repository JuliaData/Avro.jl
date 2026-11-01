# Typed decoding. A typed plan specialises a read plan on a caller-supplied target type `T`. The fast
# route builds plain data types straight from their decoded fields through their positional constructors,
# in a function generated for the compile-time `T`. Everything the fast route does not cover decodes
# generically and is converted afterwards through `StructUtils.make(T, generic, AvroStyle())`.

abstract type TypedPlan end

# The target is `Any` or the schema node's generic type.
struct GenericTarget{P<:ReadPlan} <: TypedPlan
    plan::P
end

# Decode generically, then convert through StructUtils (`AvroStyle`).
struct SemanticTarget{T} <: TypedPlan
    plan::ReadPlan
    schema::Schema
end

# A leaf converted from its generic representation (`convertleaf`).
struct LeafTarget{T,P<:ReadPlan} <: TypedPlan
    plan::P
end

# A `string` interned as a `Symbol`.
struct SymbolTarget <: TypedPlan end

# An enum by reader symbol position into `String`, `Symbol` or a `Base.Enum` (`nothing`: no member).
struct EnumTarget{T,P<:ReadPlan} <: TypedPlan
    plan::P                                    # `EnumPlan`, or `EnumRemapPlan` under resolution
    schema::EnumSchema                         # the reader enum
    members::Vector{Union{Nothing,T}}
end

struct ArrayTarget{E,P<:TypedPlan} <: TypedPlan
    items::P
    minsize::Int
end

# A map into `Dict{K,V}` with `K` `String` or `Symbol`.
struct MapTarget{K,V,P<:TypedPlan} <: TypedPlan
    values::P
    minsize::Int
end

# A two-branch nullable union into `Union{N,…}`: `N` is `Missing`, `Nothing`, or `Union{}` when null is rejected.
struct NullableTarget{N,P<:TypedPlan} <: TypedPlan
    inner::P
    nullpos::Int
end

# Any other union into a Julia `Union` whose members map to the branches.
struct UnionTarget{T} <: TypedPlan
    branches::Vector{TypedPlan}
end

# A schema field the target type does not have.
struct SkipTarget{P<:ReadPlan} <: TypedPlan
    plan::P
end

# Recursion through the caller's own recursive types (filled once the record is built; a function barrier).
mutable struct RefTarget{T} <: TypedPlan
    plan::Union{Nothing,TypedPlan}
end

# A non-union writer `null` resolved to the target's nullable convention.
struct ResolvedNullTarget{N,P<:ReadPlan} <: TypedPlan
    plan::P
end

# A writer union resolved into a two-branch nullable reader union, typed per writer branch.
struct ResolvedNullableTarget{N,BS<:Tuple} <: TypedPlan
    branches::BS
    isnull::Vector{Bool}
end

# A record decoded in wire order straight into a `NamedTuple` or plain struct: `SLOTMAP[j]` is the target
# field of step `j` (0: skipped); `defaults[k]` fills target fields no step fills. Heap identity: an inline
# immutable would re-box on every `plan` field load.
mutable struct RecordTarget{T,PS<:Tuple,SLOTMAP} <: TypedPlan
    const plans::PS
    const defaults::Vector{Any}                # a reader `DefaultPlan` (fresh per record), `missing` or `nothing`
end

# ---- construction -----------------------------------------------------------------------------------

# The record targets built so far by (read plan, target type); a reader record resolved against two
# writer records has two read plans. Each entry stays a complete plan for its key, including one built
# inside a union-member or record attempt that later fell back to the semantic route.
const TypedMemo = IdDict{Any,TypedPlan}

function memolookup(memo::TypedMemo, p::ReadPlan, ::Type{T}) where {T}
    return get(memo, (p, T), nothing)
end

function memostore!(memo::TypedMemo, p::ReadPlan, ::Type{T}, t::TypedPlan) where {T}
    memo[(p, T)] = t
    return t
end

# The typed plan decoding `plan` (the generic or resolving plan of `reader`) into `T`.
function typedplan(::Type{T}, reader::Schema, plan::ReadPlan) where {T}
    return buildtyped(T, reader, plan, TypedMemo())
end

function buildtyped(::Type{T}, s::Schema, p::ReadPlan, memo::TypedMemo) where {T}
    (T === Any || T === juliatype(s)) && return GenericTarget(p)
    customhooks(T) && return SemanticTarget{T}(p, s)
    if isresolving(p)
        t = buildresolvedtyped(T, s, p, memo)
        return t === nothing ? SemanticTarget{T}(p, s) : t
    end
    s isa UnionSchema && return builduniontarget(T, s, p::UnionPlan, memo)
    if T isa Union
        nulltype, inner = splitoptional(T)
        s isa NullSchema && nulltype !== Union{} && return buildtyped(nulltype, s, p, memo)
        inner === nothing && return SemanticTarget{T}(p, s)
        return buildtyped(inner, s, p, memo)      # a non-null schema never produces the null member
    end
    s isa RecordSchema && return buildrecordtarget(T, s, p::RecordPlan, memo)
    s isa ArraySchema && return buildarraytarget(T, s, p::ArrayPlan, memo)
    s isa MapSchema && return builddicttarget(T, s, p::MapPlan, memo)
    return buildleaftarget(T, s, p)
end

# `Union{Missing,X}` → `(Missing, X)`, `Union{Nothing,X}` → `(Nothing, X)`; `inner === nothing` when `T`
# is not of that shape (several members, both null kinds, or not a union).
function splitoptional(::Type{T}) where {T}
    if Missing <: T
        inner = Base.nonmissingtype(T)
        (inner === Union{} || inner isa Union) && return (Missing, nothing)
        return (Missing, inner)
    elseif Nothing <: T
        inner = Base.nonnothingtype(T)
        (inner === Union{} || inner isa Union) && return (Nothing, nothing)
        return (Nothing, inner)
    end
    return (Union{}, nothing)
end

const HOOK_MODULES = (StructUtils, @__MODULE__)

# Answers about a type's methods for the method tables of one world (a method query costs microseconds).
mutable struct HookCache
    const lock::ReentrantLock
    world::UInt
    const answers::IdDict{Any,Bool}
end

# Process-wide; at most `HOOK_CACHE_SIZE` types each, emptied when full or when any method is defined.
const HOOK_CACHE = HookCache(ReentrantLock(), UInt(0), IdDict{Any,Bool}())
const PROPERTY_CACHE = HookCache(ReentrantLock(), UInt(0), IdDict{Any,Bool}())
const HOOK_CACHE_SIZE = 1024

function cachedanswer(f, cache::HookCache, @nospecialize(T::Type))
    world = Base.get_world_counter()
    @lock cache.lock begin
        if cache.world != world
            empty!(cache.answers)
            cache.world = world
        end
        answer = get(cache.answers, T, nothing)
        answer === nothing || return answer
    end
    answer = f(T)::Bool                                # outside the lock: the query may compile
    @lock cache.lock begin
        if cache.world == world                        # unless a newer world emptied the cache meanwhile
            length(cache.answers) >= HOOK_CACHE_SIZE && empty!(cache.answers)
            cache.answers[T] = answer
        end
    end
    return answer
end

# Whether a `StructUtils.make` or `StructUtils.lift` method outside StructUtils and Avro applies to `T`
# under `AvroStyle` (such targets always take the semantic route).
function customhooks(@nospecialize(T::Type))
    return cachedanswer(methodhooks, HOOK_CACHE, T)
end

# Whether `T` overrides `propertynames` (a DataFrame row, say), so that its properties are its columns.
function usesproperties(@nospecialize(T::Type))
    T <: NamedTuple && return false
    return cachedanswer(S -> which(propertynames, Tuple{S}).module !== Base, PROPERTY_CACHE, T)
end

function methodhooks(@nospecialize(T::Type))
    for sig in (Tuple{AvroStyle,Type{T},Any}, Tuple{AvroStyle,Type{T},Any,Any})
        for m in methods(StructUtils.make, sig)
            m.module in HOOK_MODULES || return true
        end
    end
    for sig in (Tuple{Type{T},Any}, Tuple{AvroStyle,Type{T},Any}, Tuple{AvroStyle,Type{T},Any,Any})
        for m in methods(StructUtils.lift, sig)
            m.module in HOOK_MODULES || return true
        end
    end
    return false
end

function builduniontarget(::Type{T}, s::UnionSchema, p::UnionPlan, memo::TypedMemo) where {T}
    nb = nullablebranch(s)
    target = nb == 0 ? branchtargets(T, s, p, memo) : nullabletarget(T, s, p, nb, memo)
    return target === nothing ? SemanticTarget{T}(p, s) : target
end

function nullabletarget(::Type{T}, s::UnionSchema, p::UnionPlan, nb::Int, memo::TypedMemo) where {T}
    N, inner = T isa Union ? splitoptional(T) : (Union{}, T)
    inner === nothing && return nothing
    ip = buildtyped(inner, s.branches[3 - nb], p.branches[3 - nb], memo)
    ip isa SemanticTarget && return nothing
    return NullableTarget{N,typeof(ip)}(ip, nb)
end

function branchtargets(::Type{T}, s::UnionSchema, p::UnionPlan, memo::TypedMemo) where {T}
    members = T isa Union ? Base.uniontypes(T) : Any[T]
    branches = TypedPlan[]
    for (b, bp) in zip(s.branches, p.branches)
        tp = membertarget(members, b, bp, memo)
        tp === nothing && return nothing
        push!(branches, tp)
    end
    return UnionTarget{T}(branches)
end

# The first member that decodes branch `b` directly: the branch's own Julia type first, then `Base.uniontypes` order.
function membertarget(members::Vector{Any}, b::Schema, bp::ReadPlan, memo::TypedMemo)
    natural = juliatype(b)
    for m in (natural in members ? Any[natural; filter(!=(natural), members)] : members)
        tp = buildtyped(m, b, bp, memo)
        isdirect(tp) && return tp
    end
    return nothing
end

# Whether a target decodes with no StructUtils conversion anywhere below it.
function isdirect(tp::TypedPlan, seen::Base.IdSet{Any}=Base.IdSet{Any}())
    tp isa SemanticTarget && return false
    tp in seen && return true
    push!(seen, tp)
    children = tp isa RecordTarget ? tp.plans :
               tp isa ArrayTarget ? (tp.items,) :
               tp isa MapTarget ? (tp.values,) :
               tp isa NullableTarget ? (tp.inner,) :
               tp isa Union{UnionTarget,ResolvedNullableTarget} ? tp.branches :
               tp isa RefTarget && tp.plan !== nothing ? (tp.plan,) : ()
    return all(c -> isdirect(c, seen), children)
end

function buildleaftarget(::Type{T}, s::Schema, p::ReadPlan) where {T}
    T === Symbol && p isa StringPlan && return SymbolTarget()
    p isa EnumPlan && s isa EnumSchema && isenumtarget(T) && return EnumTarget(T, s, p)
    leafcompatible(T, p) && return LeafTarget{T,typeof(p)}(p)
    return SemanticTarget{T}(p, s)
end

function isenumtarget(::Type{T}) where {T}
    return T === String || T === Symbol || T <: Base.Enum
end

function EnumTarget(::Type{T}, s::EnumSchema, p::ReadPlan) where {T}
    return EnumTarget{T,typeof(p)}(p, s, enummembers(T, s))
end

function enummembers(::Type{T}, s::EnumSchema) where {T}
    T === String && return Vector{Union{Nothing,T}}(s.symbols)
    T === Symbol && return Vector{Union{Nothing,T}}(Symbol.(s.symbols))
    members = Vector{Union{Nothing,T}}(nothing, length(s.symbols))
    for e in instances(T)
        i = get(s.symbolindex, avrosymbol(T, e), 0)
        i == 0 || (members[i] = e)
    end
    return members
end

function leafcompatible(::Type{T}, ::NullPlan) where {T}
    return T === Nothing
end

function leafcompatible(::Type{T}, ::Union{IntPlan,LongPlan}) where {T}
    return T <: Integer && T !== Bool && isbitstype(T)
end

function leafcompatible(::Type{T}, ::FloatPlan) where {T}
    return T === Float64 || T === Float16
end

function leafcompatible(::Type{T}, ::StringPlan) where {T}
    return T === Char
end

function leafcompatible(::Type{T}, p::FixedPlan) where {T}
    return T === Vector{UInt8} || isbytetuple(T, p.schema.size)
end

function leafcompatible(::Type{T}, ::Union{TimestampPlan,LocalTimestampPlan}) where {T}
    return T === DateTime
end

function leafcompatible(::Type{T}, ::ReadPlan) where {T}
    return false
end

function isbytetuple(::Type{T}, n::Int) where {T}
    return T <: Tuple && length(T.parameters) == n && all(t -> t === UInt8, T.parameters)
end

function buildarraytarget(::Type{T}, s::ArraySchema, p::ArrayPlan, memo::TypedMemo) where {T}
    T <: Vector || return SemanticTarget{T}(p, s)
    ip = buildtyped(eltype(T), s.items, p.items, memo)
    ip isa SemanticTarget && return SemanticTarget{T}(p, s)
    return ArrayTarget{eltype(T),typeof(ip)}(ip, p.minsize)
end

function builddicttarget(::Type{T}, s::MapSchema, p::MapPlan, memo::TypedMemo) where {T}
    (isconcretetype(T) && T <: Dict && keytype(T) in (String, Symbol)) || return SemanticTarget{T}(p, s)
    vp = buildtyped(valtype(T), s.values, p.values, memo)
    vp isa SemanticTarget && return SemanticTarget{T}(p, s)
    return MapTarget{keytype(T),valtype(T),typeof(vp)}(vp, p.minsize)
end

# Plain and resolved records share the memo and the recursion placeholder; `recordtarget` builds the node.
function buildrecordtarget(::Type{T}, s::RecordSchema, p::Union{RecordPlan,ResolvedRecordPlan},
                           memo::TypedMemo) where {T}
    cached = memolookup(memo, p, T)
    cached === nothing || return cached
    fastroute(T) || return memostore!(memo, p, T, SemanticTarget{T}(p, s))
    ref = RefTarget{T}(nothing)
    memostore!(memo, p, T, ref)
    built = recordtarget(T, s, p, memo)
    built === nothing && (built = SemanticTarget{T}(p, s))   # recursive references through `ref` convert where they occur
    ref.plan = built
    return memostore!(memo, p, T, built)
end

# Eligibility for building `T` straight from decoded fields through its positional constructor: a concrete
# `NamedTuple` or struct that has one, no custom StructUtils hooks under `AvroStyle`, and no field tags other
# than `name` / `avro=(name, default)`.
function fastroute(::Type{T}) where {T}
    isconcretetype(T) || return false
    (T <: NamedTuple || (isstructtype(T) && !(T <: NOT_RECORDLIKE) && hasmethod(T, Tuple{fieldtypes(T)...}))) || return false
    customhooks(T) && return false
    tags = StructUtils.fieldtags(AvroStyle(), T)
    for t in values(tags)
        for (k, v) in pairs(t)
            k === :name && continue
            k === :avro && v isa NamedTuple && all(kk -> kk === :name || kk === :default, keys(v)) && continue
            return false
        end
    end
    return true
end

# Default factories run per value on the semantic route, including defaults that depend on other fields.
function customfielddefaults(@nospecialize(T::Type))
    for signature in (Tuple{AvroStyle,Type{T}}, Tuple{AvroStyle,Type{T},Any})
        for method in methods(StructUtils.fielddefaults, signature)
            method.module in HOOK_MODULES || return true
        end
    end
    return false
end

# Fill each target field nothing else covers with `missing` or `nothing`; `false` sends a type with
# StructUtils defaults to the semantic route.
function implicitdefaults!(defaults::Vector{Any}, ::Type{T}, covered) where {T}
    all(covered) && return true
    customfielddefaults(T) && return false
    for k in 1:fieldcount(T)
        covered[k] && continue
        ft = fieldtype(T, k)
        (Missing <: ft || Nothing <: ft) || throw(ConversionError(
            "target type $T has field $(fieldname(T, k)) with no field in the reader record and no default"))
        defaults[k] = Missing <: ft ? missing : nothing
    end
    return true
end

# Promotions, enum remaps and resolved records decode straight into `T`; a writer resolved into a
# two-branch nullable reader union goes through a wrapped or per-writer-branch target. `nothing` when
# the semantic route must convert.
function buildresolvedtyped(::Type{T}, s::Schema, p::ReadPlan, memo::TypedMemo) where {T}
    p isa PromotePlan && return buildleaftarget(T, s, p)
    p isa EnumRemapPlan && return s isa EnumSchema && isenumtarget(T) ? EnumTarget(T, s, p) : nothing
    p isa ResolvedRecordPlan && s isa RecordSchema && return buildrecordtarget(T, s, p, memo)
    (s isa UnionSchema && nullablebranch(s) != 0) || return nothing
    N, inner = T isa Union ? splitoptional(T) : (Union{}, T)
    inner === nothing && return nothing
    p isa WrapPlan && return wrappedtarget(N, inner, s, p, memo)
    p isa UnionResolvePlan && N !== Union{} && return resolvednullable(N, inner, s, p, memo)
    return nothing
end

function wrappedtarget(::Type{N}, ::Type{X}, s::UnionSchema, p::WrapPlan, memo::TypedMemo) where {N,X}
    p.readerindex == nullablebranch(s) || return buildtyped(X, s.branches[p.readerindex], p.inner, memo)
    N === Union{} && return nothing
    return ResolvedNullTarget{N,typeof(p.inner)}(p.inner)
end

function resolvednullable(::Type{N}, ::Type{X}, s::UnionSchema, p::UnionResolvePlan,
                          memo::TypedMemo) where {N,X}
    nb = nullablebranch(s)
    branches = TypedPlan[]
    isnull = Bool[]
    for (i, bp) in enumerate(p.branches)
        j = p.readerindex[i]
        null = j == nb
        bt = null || bp isa UnresolvableBranch ? GenericTarget(bp) : buildtyped(X, s.branches[j], bp, memo)
        bt isa SemanticTarget && return nothing
        push!(branches, bt)
        push!(isnull, null)
    end
    bs = Tuple(branches)
    return ResolvedNullableTarget{N,typeof(bs)}(bs, isnull)
end

function recordsteps(p::RecordPlan)
    return Pair{Int,ReadPlan}[i => f for (i, f) in enumerate(p.fields)]
end

function recordsteps(p::ResolvedRecordPlan)
    return p.steps
end

function recorddefaults(::RecordPlan)
    return ()
end

function recorddefaults(p::ResolvedRecordPlan)
    return p.defaults
end

function recordtarget(::Type{T}, s::RecordSchema, p::Union{RecordPlan,ResolvedRecordPlan}, memo::TypedMemo) where {T}
    fieldof = recordfieldpositions(s, T)
    steps = recordsteps(p)
    plans = Vector{TypedPlan}(undef, length(steps))
    slotmap = zeros(Int, length(steps))
    for (i, (slot, sp)) in enumerate(steps)
        k = slot == 0 ? 0 : fieldof[slot]
        if k == 0
            plans[i] = SkipTarget(sp)
            continue
        end
        plans[i] = buildtyped(fieldtype(T, k), s.fields[slot].schema, sp, memo)
        slotmap[i] = k
    end
    covered = falses(fieldcount(T))
    for k in slotmap
        k == 0 || (covered[k] = true)
    end
    defaults = Vector{Any}(undef, fieldcount(T))
    for (slot, dp) in recorddefaults(p)
        k = fieldof[slot]
        k == 0 && continue
        defaultfits(fieldtype(T, k), dp) || return nothing
        defaults[k] = dp
        covered[k] = true
    end
    implicitdefaults!(defaults, T, covered) || return nothing
    ps = Tuple(plans)
    return RecordTarget{T,typeof(ps),Tuple(slotmap)}(ps, defaults)
end

# Whether the fast route converts this reader default to `T` (otherwise the semantic route converts it).
function defaultfits(::Type{T}, dp::DefaultPlan) where {T}
    dp.value isa T && return true
    try
        return convertleaf(T, dp.value) isa T
    catch e
        e isa Union{MethodError,ConversionError} || rethrow()
        return false
    end
end

# ---- decoding ---------------------------------------------------------------------------------------

# One typed value, counted like `decode`.
@inline function decodetyped(p::TypedPlan, d::Decoder)
    countvalue!(d)
    return typedvalue(p, d)
end

# One root datum; a semantic root decodes generically for `finishtyped` to convert.
function decoderoot(p::SemanticTarget, d::Decoder)
    return decode(p.plan, d)
end

function decoderoot(p::TypedPlan, d::Decoder)
    return decodetyped(p, d)
end

function typedvalue(p::GenericTarget, d::Decoder)
    return decodevalue(p.plan, d)
end

# A field (or recursive reference) StructUtils converts: decoded generically, then converted in place.
function typedvalue(p::SemanticTarget{T}, d::Decoder) where {T}
    return semanticvalue(T, decodevalue(p.plan, d), p.schema)
end

function typedvalue(p::LeafTarget{T}, d::Decoder) where {T}
    return convertleaf(T, decodevalue(p.plan, d))::T
end

function typedvalue(::SymbolTarget, d::Decoder)
    return targetsymbol(readstring(d))
end

function targetsymbol(s::String)
    occursin('\0', s) && throw(ConversionError("a string holding a NUL cannot become a Symbol"))
    return Symbol(s)
end

function typedvalue(p::EnumTarget{T}, d::Decoder) where {T}
    i = Int(decodevalue(p.plan, d).index)
    m = p.members[i]
    m === nothing &&
        throw(ConversionError("target enum $T has no member for $(repr(diagnostictext(p.schema.symbols[i])))"))
    return m
end

function typedvalue(p::RefTarget{T}, d::Decoder) where {T}
    return typedvalue(p.plan::TypedPlan, d)::T
end

function typedvalue(p::NullableTarget{N}, d::Decoder) where {N}
    readindex(d, 2) == p.nullpos || return decodetyped(p.inner, d)
    countvalue!(d)
    N === Union{} && throw(ConversionError("null is not accepted by the target type"))
    return N === Missing ? missing : nothing
end

function typedvalue(p::UnionTarget{T}, d::Decoder) where {T}
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    return decodetyped(p.branches[readindex(d, n)], d)::T
end

function typedvalue(p::ArrayTarget{E}, d::Decoder) where {E}
    enter!(d)
    out = E[]
    while true
        count, size = readblockcount(d)
        count == 0 && break
        checkcount(d, count, p.minsize)
        isempty(out) && sizehint!(out, min(count, 1024))
        stop = d.stop
        size >= 0 && (d.stop = d.pos + size - 1)
        for _ in 1:count
            push!(out, decodetyped(p.items, d))
        end
        size >= 0 && d.pos != d.stop + 1 && dataerror(d, "sized array block not exactly consumed")
        d.stop = stop
    end
    leave!(d)
    return out
end

function typedvalue(p::MapTarget{K,V}, d::Decoder) where {K,V}
    enter!(d)
    out = Dict{K,V}()
    while true
        count, size = readblockcount(d)
        count == 0 && break
        checkcount(d, count, satadd(p.minsize, 1))
        isempty(out) && sizehint!(out, min(count, 1024))
        stop = d.stop
        size >= 0 && (d.stop = d.pos + size - 1)
        for _ in 1:count
            key = readstring(d)
            out[K === Symbol ? targetsymbol(key) : key] = decodetyped(p.values, d)
        end
        size >= 0 && d.pos != d.stop + 1 && dataerror(d, "sized map block not exactly consumed")
        d.stop = stop
    end
    leave!(d)
    return out
end

function typedvalue(p::ResolvedNullTarget{N}, d::Decoder) where {N}
    decodevalue(p.plan, d)
    return N === Missing ? missing : nothing
end

function typedvalue(p::ResolvedNullableTarget{N}, d::Decoder) where {N}
    i = readindex(d, length(p.branches))
    v = decodetyped(p.branches[i], d)                  # a null branch still consumes the writer's value
    p.isnull[i] || return v
    return N === Missing ? missing : nothing
end

function typedvalue(p::RecordTarget, d::Decoder)
    enter!(d)
    v = recordvalue(p, d)
    leave!(d)
    return v
end

@generated function recordvalue(p::RecordTarget{T,PS,SLOTMAP}, d::Decoder) where {T,PS,SLOTMAP}
    body = Expr(:block)
    plantypes = PS.parameters
    for j in 1:length(plantypes)
        if plantypes[j] <: SkipTarget
            push!(body.args, :(skip(p.plans[$j].plan, d)))
        else
            push!(body.args, :($(Symbol("f", SLOTMAP[j])) = decodetyped(p.plans[$j], d)))
        end
    end
    args = []
    for k in 1:fieldcount(T)
        ft = fieldtype(T, k)
        push!(args, k in SLOTMAP ? :($(Symbol("f", k))::$ft) : :(resolveddefault($ft, p.defaults[$k])))
    end
    construct = T <: NamedTuple ? :($T(($(args...),))) : :($T($(args...)))
    push!(body.args, :(return $construct))
    return body
end

function resolveddefault(::Type{T}, dp::DefaultPlan) where {T}
    v = freshvalue(dp.value)
    return v isa T ? v : convertleaf(T, v)::T
end

function resolveddefault(::Type{T}, x) where {T}
    return x::T
end

# ---- leaf conversions (ConversionError on failure) -------------------------------------------------

function convertleaf(::Type{Nothing}, ::Missing)
    return nothing
end

function convertleaf(::Type{Float64}, v::Float32)
    return Float64(v)
end

function convertleaf(::Type{Float16}, v::Float32)
    h = Float16(v)
    isinf(h) && !isinf(v) && throw(ConversionError("target type Float16 cannot represent $v"))
    return h
end

function convertleaf(::Type{DateTime}, v::Union{Timestamp,LocalTimestamp})
    return todatetime(v)
end

function convertleaf(::Type{Vector{UInt8}}, v::Fixed)
    return v.bytes
end

function convertleaf(::Type{T}, v::Union{Int32,Int64}) where {T<:Integer}
    fits = T <: Signed ? (typemin(T) <= Int64(v) <= typemax(T)) : (v >= 0 && UInt64(v) <= typemax(T))
    fits || throw(ConversionError("target integer type $T cannot represent $v"))
    return T(v)
end

function convertleaf(::Type{Char}, s::String)
    n = ncodeunits(s)
    n > 0 && n == ncodeunits(s[1]) ||
        throw(ConversionError("expected a one-character string for Char, got $(repr(diagnostictext(s)))"))
    return s[1]
end

function convertleaf(::Type{T}, v::Fixed) where {T<:Tuple}
    isbytetuple(T, length(v.bytes)) ||
        throw(ConversionError("target type $T cannot hold the $(length(v.bytes)) bytes of fixed $(fullname(v.schema))"))
    return ntuple(i -> @inbounds(v.bytes[i]), Val(length(T.parameters)))
end

# ---- semantic route ----------------------------------------------------------------------------------

# Convert a generic value to `T` through `StructUtils.make` under `AvroStyle`. A failure converting file
# content is a `ConversionError`, whatever Base raised; errors raised by caller-defined hooks propagate.
function semanticvalue(::Type{T}, v, schema::Schema) where {T}
    try
        return StructUtils.make(T, v, AvroStyle())
    catch e
        e isa Union{AvroError,InterruptException,OutOfMemoryError,StackOverflowError} && rethrow()
        structutilshookexception(catch_backtrace()) && rethrow()
        throw(ConversionError("cannot convert the decoded value to $T; StructUtils raised $(typeof(e))"))
    end
end

# Whether a caught conversion error passed through a caller-defined StructUtils make/lift method.
function structutilshookexception(backtrace)
    for frame in stacktrace(backtrace)
        method = frame.linfo
        while method isa Union{Core.CodeInstance,Core.MethodInstance}
            method = method.def
        end
        method isa Method || continue
        method.module in HOOK_MODULES && continue
        method.name in (:make, :lift) && return true
    end
    return false
end

function finishtyped(p::SemanticTarget{T}, v) where {T}
    return semanticvalue(T, v, p.schema)
end

function finishtyped(p, v)
    return v
end

# StructUtils integration: a `Record` reads as its fields.
function StructUtils.applyeach(st::AvroStyle, f, r::Record)
    vals = getfield(r, :values)
    for (i, fld) in enumerate(getfield(r, :schema).fields)
        ret = f(fld.name, vals[i])
        ret isa StructUtils.EarlyReturn && return ret
    end
    return StructUtils.defaultstate(st)
end

# StructUtils would treat a decoded scalar as a struct without fields and fill an array, dict or struct
# target with nothing; refuse instead so the conversion fails.
const SCALAR_VALUES = Union{Missing,Nothing,Bool,Int32,Int64,Float32,Float64,String,Date,Time,UUID,
                            Timestamp,LocalTimestamp,Duration,DataDecimals.AbstractDecimal,WideDecimal}

function StructUtils.applyeach(::AvroStyle, f, x::SCALAR_VALUES)
    throw(ArgumentError("a decoded $(typeof(x)) is not a collection"))
end

# A number converts to a float type that can hold it: an integer exactly, a float up to rounding.
function StructUtils.lift(st::AvroStyle, ::Type{T}, x::Union{Integer,AbstractFloat}) where {T<:AbstractFloat}
    y = T(x)
    fits = x isa Integer ? y == x : (isfinite(y) || !isfinite(x))
    fits || throw(ConversionError("target type $T cannot represent $x"))
    return y, StructUtils.defaultstate(st)
end

function StructUtils.lift(st::AvroStyle, ::Type{DateTime}, x::Union{Timestamp,LocalTimestamp})
    return todatetime(x), StructUtils.defaultstate(st)
end

# Timestamps convert through `lift` (exact rescaling), not field by field.
function StructUtils.structlike(::AvroStyle, ::Type{<:Union{Durations.ZonedTimestamp,LocalTimestamp}})
    return false
end

function StructUtils.lift(st::AvroStyle, ::Type{T}, x::Durations.ZonedTimestamp) where {T<:Durations.ZonedTimestamp}
    return T(x), StructUtils.defaultstate(st)
end

function StructUtils.lift(st::AvroStyle, ::Type{T}, x::LocalTimestamp) where {T<:LocalTimestamp}
    return convert(T, x), StructUtils.defaultstate(st)
end

# An instant and a wall-clock time do not convert into each other.
function StructUtils.lift(::AvroStyle, ::Type{T}, x::Durations.ZonedTimestamp) where {T<:LocalTimestamp}
    throw(ArgumentError("cannot convert $(typeof(x)) to $T"))
end

function StructUtils.lift(::AvroStyle, ::Type{T}, x::LocalTimestamp) where {T<:Durations.ZonedTimestamp}
    throw(ArgumentError("cannot convert $(typeof(x)) to $T"))
end

# A fixed converts only to a byte tuple of its size, as on the direct route; StructUtils' element-wise
# tuple construction would drop the bytes past a shorter tuple.
function StructUtils.make(st::AvroStyle, ::Type{T}, x::Fixed) where {T<:Tuple}
    return convertleaf(T, x), StructUtils.defaultstate(st)
end

function StructUtils.make(st::AvroStyle, ::Type{Durations.Duration}, x::Duration)
    return Durations.Duration(x), StructUtils.defaultstate(st)
end

function StructUtils.make(st::AvroStyle, ::Type{T}, x::DataDecimals.AbstractDecimal) where {T<:DataDecimals.AbstractDecimal}
    return T(x), StructUtils.defaultstate(st)
end

function StructUtils.make(st::AvroStyle, ::Type{T}, x::WideDecimal) where {T<:DataDecimals.AbstractDecimal}
    value = DataDecimals.DecimalValue{DataDecimals.Int256}(x.unscaled, x.scale)
    return T(value), StructUtils.defaultstate(st)
end

function StructUtils.make(st::AvroStyle, ::Type{WideDecimal}, x::DataDecimals.AbstractDecimal)
    return decimalinput(x), StructUtils.defaultstate(st)
end
