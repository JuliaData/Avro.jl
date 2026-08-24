# Typed decoding (plan §4.5, §4.8). A typed plan specialises on a caller-supplied target type `T`.
# The fast route constructs plain data types directly — NamedTuples by construction, structs with
# `Expr(:new)` in a function generated for the compile-time `T` — so no user constructor ever runs
# under the ceiling and the only allocations are the approved representations. Everything the fast route
# does not cover decodes generically under the ceiling and is converted afterwards, in caller space,
# through `StructUtils.make(T, generic, AvroStyle())`.

abstract type TypedPlan end

"Decode generically: `T` is `Any` or the generic representation of the schema node."
struct GenericTarget{P<:ReadPlan} <: TypedPlan
    plan::P
end

"Decode generically, then convert in caller space through StructUtils (`AvroStyle`)."
struct SemanticTarget{T} <: TypedPlan
    plan::ReadPlan
end

"A leaf converted from its generic representation (`convertleaf`)."
struct LeafTarget{T,P<:ReadPlan} <: TypedPlan
    plan::P
end

"A `string` or `enum` decoded to `Symbol` through the operation's admission object."
struct SymbolTarget{P<:Union{StringPlan,EnumPlan}} <: TypedPlan
    plan::P
end

"An `enum` decoded to a `Base.Enum` by symbol (`Avro.avrosymbol`)."
struct EnumTarget{T} <: TypedPlan
    plan::EnumPlan
    members::Vector{Union{Nothing,T}}    # by symbol position; `nothing` → ConversionError
end

struct ArrayTarget{E,P<:TypedPlan} <: TypedPlan
    items::P
    minsize::Int
end

"`Avro.Map{V}`, `Dict{String,V}` or `Dict{Symbol,V}` (keys admitted) targets."
struct MapTarget{K,V,P<:TypedPlan} <: TypedPlan
    values::P
    minsize::Int
    dict::Bool
end

"A two-branch nullable union into `Union{N,…}`: `N` is `Missing`, `Nothing`, or `Union{}` when null is rejected."
struct NullableTarget{N,P<:TypedPlan} <: TypedPlan
    inner::P
    nullpos::Int
end

"A general union into a Julia `Union` whose members map to the branches (bare member values)."
struct UnionTarget{T} <: TypedPlan
    branches::Vector{TypedPlan}
end

"A schema field the target type does not have: skipped under the active validation mode."
struct SkipTarget{P<:ReadPlan} <: TypedPlan
    plan::P
end

"""
A record into a `NamedTuple` or plain struct: `plans` are the per-schema-field plans in schema order and
`MAP[k]` is the schema field feeding target field `k` (0 → `defaults[k]`).
"""
mutable struct RecordTarget{T,PS<:Tuple,MAP} <: TypedPlan   # heap identity: an inline immutable would
    const schema::RecordSchema                                # re-box on every `plan` field load (§10.2)
    const plans::PS
    const defaults::Vector{Any}
    const shell::Int    # measured per `T` from an empty probe (plan §4.4, R10)
end

"Recursion through the user's own recursive types (filled after construction; a function barrier)."
mutable struct RefTarget{T} <: TypedPlan
    plan::Union{Nothing,TypedPlan}
end

# ---- construction -----------------------------------------------------------------------------------

const TypedMemo = Vector{Vector{Pair{Any,TypedPlan}}}   # per node id: (target type => plan), no hashing

"""
    typedplan(T, reader_schema, plan, limits) -> TypedPlan

The typed plan decoding `plan` (the generic or resolving plan of `reader_schema`) into `T`.
"""
function typedplan(::Type{T}, reader::Schema, plan::ReadPlan, limits::Limits) where {T}
    memo = TypedMemo(undef, graphinfo(reader).nodes)
    for i in eachindex(memo)
        memo[i] = Pair{Any,TypedPlan}[]
    end
    return buildtyped(T, reader, plan, memo)
end

function memolookup(memo::TypedMemo, s::Schema, ::Type{T}) where {T}
    for (t, p) in memo[Int(nodeid(s)) + 1]
        t === T && return p
    end
    return nothing
end

function memostore!(memo::TypedMemo, s::Schema, ::Type{T}, p::TypedPlan) where {T}
    entries = memo[Int(nodeid(s)) + 1]
    for i in eachindex(entries)
        entries[i].first === T || continue
        entries[i] = T => p
        return p
    end
    push!(entries, T => p)
    return p
end

function buildtyped(::Type{T}, s::Schema, p::ReadPlan, memo::TypedMemo) where {T}
    T === Any && return GenericTarget(p)
    T === juliatype(s) && return GenericTarget(p)
    customhooks(T) && return SemanticTarget{T}(p)
    if isresolving(p)                                        # the settled eligibility analysis applies to
        t = buildresolvedtyped(T, s, p, memo)                # resolved plans too (plan §4.8, R18); anything
        t === nothing || return t                            # ineligible converts through the semantic route
        return SemanticTarget{T}(p)
    end
    s isa UnionSchema && return builduniontarget(T, s, p::UnionPlan, memo)
    if T isa Union
        _, inner = splitoptional(T)
        inner === nothing && return SemanticTarget{T}(p)
        return buildtyped(inner, s, p, memo)      # a non-union schema never produces the null member
    end
    s isa RecordSchema && return buildrecordtarget(T, s, p::RecordPlan, memo)
    s isa ArraySchema && return buildarraytarget(T, s, p::ArrayPlan, memo)
    s isa MapSchema && return buildmaptarget(T, s, p::MapPlan, memo)
    return buildleaftarget(T, p)
end

"""
    splitoptional(T) -> (N, inner)

For `Union{Missing,X}` → `(Missing, X)`, `Union{Nothing,X}` → `(Nothing, X)`; `inner === nothing` when
`T` is not of that shape (several members, both null kinds, or not a union).
"""
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

"""
    customhooks(T) -> Bool

Whether a `StructUtils.make` or `StructUtils.lift` method outside StructUtils and Avro applies to `T`
under `AvroStyle` (such targets always take the semantic route).
"""
const HOOK_MODULES = (StructUtils, @__MODULE__)

function customhooks(::Type{T}) where {T}
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
    nb = p.nullable
    if nb != 0
        other, op = s.branches[3 - nb], p.branches[3 - nb]
        N, inner = T isa Union ? splitoptional(T) : (Union{}, T)
        inner === nothing && return SemanticTarget{T}(p)
        ip = buildtyped(inner, other, op, memo)
        ip isa SemanticTarget && return SemanticTarget{T}(p)
        return NullableTarget{N,typeof(ip)}(ip, nb)
    end
    members = T isa Union ? Base.uniontypes(T) : Any[T]
    branches = TypedPlan[]
    for (b, bp) in zip(s.branches, p.branches)
        found = nothing
        for m in members
            tp = buildtyped(m, b, bp, memo)
            tp isa SemanticTarget && continue
            found = tp
            break
        end
        found === nothing && return SemanticTarget{T}(p)
        push!(branches, found)
    end
    return UnionTarget{T}(branches)
end

function buildleaftarget(::Type{T}, p::ReadPlan) where {T}
    T === Symbol && p isa Union{StringPlan,EnumPlan} && return SymbolTarget(p)
    T <: Base.Enum && p isa EnumPlan && return enumtarget(T, p)
    leafcompatible(T, p) && return LeafTarget{T,typeof(p)}(p)
    return SemanticTarget{T}(p)
end

function enumtarget(::Type{T}, p::EnumPlan) where {T<:Base.Enum}
    syms = p.schema.symbols
    members = Vector{Union{Nothing,T}}(nothing, length(syms))
    for e in instances(T)
        name = avrosymbol(T, e)
        haskey(p.schema.symbolindex, name) || continue
        members[p.schema.symbolindex[name]] = e
    end
    return EnumTarget{T}(p, members)
end

leafcompatible(::Type{T}, ::NullPlan) where {T} = T === Nothing
leafcompatible(::Type{T}, ::Union{IntPlan,LongPlan}) where {T} = T <: Integer && T !== Bool && isbitstype(T)
leafcompatible(::Type{T}, ::FloatPlan) where {T} = T === Float64 || T === Float16
leafcompatible(::Type{T}, ::StringPlan) where {T} = T === Char
leafcompatible(::Type{T}, ::EnumPlan) where {T} = T === String
leafcompatible(::Type{T}, p::FixedPlan) where {T} = T === Vector{UInt8} || isbytetuple(T, p.schema.size)
leafcompatible(::Type{T}, ::Union{TimestampPlan,LocalTimestampPlan}) where {T} = T === DateTime
leafcompatible(::Type{T}, ::ReadPlan) where {T} = false

isbytetuple(::Type{T}, n::Int) where {T} = T <: Tuple && length(T.parameters) == n && all(t -> t === UInt8, T.parameters)

function buildrecordtarget(::Type{T}, s::RecordSchema, p::RecordPlan, memo::TypedMemo) where {T}
    cached = memolookup(memo, s, T)
    cached === nothing || return cached
    fastroute(T) || return memostore!(memo, s, T, SemanticTarget{T}(p))
    ref = RefTarget{T}(nothing)
    memostore!(memo, s, T, ref)
    built = buildrecordplan(T, s, p, memo)
    if built === nothing
        # Recursive references captured `ref` already: they convert semantically where they occur.
        sem = SemanticTarget{T}(p)
        ref.plan = sem
        return memostore!(memo, s, T, sem)
    end
    ref.plan = built
    return memostore!(memo, s, T, built)
end

"""
    fastroute(T) -> Bool

Eligibility of `T` for constructor-free construction: a concrete `NamedTuple` or plain struct, no custom
StructUtils hooks under `AvroStyle`, and no field tags other than `name` / `avro=(name, default)`.
"""
function fastroute(::Type{T}) where {T}
    isconcretetype(T) || return false
    (T <: NamedTuple || (isstructtype(T) && !(T <: NOT_RECORDLIKE))) || return false
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

function avrofieldname(tags, field::Symbol)
    t = fieldtag(tags, field, :name)
    return t === nothing ? String(field) : String(t)
end

function buildrecordplan(::Type{T}, s::RecordSchema, p::RecordPlan, memo::TypedMemo) where {T}
    names = fieldnames(T)
    tags = StructUtils.fieldtags(AvroStyle(), T)
    avronames = String[avrofieldname(tags, n) for n in names]
    nschema = length(s.fields)
    plans = Vector{TypedPlan}(undef, nschema)
    map = zeros(Int, length(names))
    for (j, f) in enumerate(s.fields)
        k = findfirst(==(f.name), avronames)
        if k === nothing
            plans[j] = SkipTarget(p.fields[j])
            continue
        end
        tp = buildtyped(fieldtype(T, k), f.schema, p.fields[j], memo)
        tp isa SemanticTarget && return nothing
        plans[j] = tp
        map[k] = j
    end
    defaults = Vector{Any}(undef, length(names))
    defs = StructUtils.fielddefaults(AvroStyle(), T)
    for k in eachindex(names)
        map[k] == 0 || continue
        ft = fieldtype(T, k)
        if haskey(defs, names[k])
            v = defs[names[k]]
            v isa ft || return nothing
            defaults[k] = v
        elseif Missing <: ft
            defaults[k] = missing
        elseif Nothing <: ft
            defaults[k] = nothing
        else
            throw(ArgumentError("field $(names[k]) of $T has no field \"$(avronames[k])\" in record $(fullname(s)) and no default"))
        end
    end
    ps = (plans...,)
    return RecordTarget{T,typeof(ps),(map...,)}(s, ps, defaults, measuredshell(T))
end

"""
The direct typed route over resolving plans (plan §4.8, R18): promotions and enum remaps are leaves
(their decode already yields the reader value); a two-branch-nullable reader union resolves through a
wrapped or per-writer-branch target; resolved records decode writer-ordered steps straight into `T`'s
fields with reader-only defaults materialised once at plan time. Returns `nothing` when ineligible.
"""
function buildresolvedtyped(::Type{T}, s::Schema, p::ReadPlan, memo::TypedMemo) where {T}
    p isa Union{PromotePlan,EnumRemapPlan} && return buildleaftarget(T, p)
    if p isa WrapPlan && p.nullable != 0 && s isa UnionSchema
        N, inner = T isa Union ? splitoptional(T) : (Union{}, T)
        inner === nothing && return nothing
        if p.readerindex == p.nullable
            N === Union{} && return nothing
            return ResolvedNullTarget{N,typeof(p.inner)}(p.inner)
        end
        return buildtyped(inner, s.branches[p.readerindex], p.inner, memo)   # the writer never encodes null here
    end
    if p isa UnionResolvePlan && p.nullable != 0 && s isa UnionSchema
        N, inner = T isa Union ? splitoptional(T) : (Union{}, T)
        (inner === nothing || N === Union{}) && return nothing
        branches = TypedPlan[]
        isnull = Bool[]
        for (i, bp) in enumerate(p.branches)
            if p.readerindex[i] == p.nullable
                push!(branches, GenericTarget(bp))                            # decodes missing
                push!(isnull, true)
            else
                bt = bp isa UnresolvableBranch ? GenericTarget(bp) :
                     buildtyped(inner, s.branches[p.readerindex[i]], bp, memo)
                bt isa SemanticTarget && return nothing
                push!(branches, bt)
                push!(isnull, false)
            end
        end
        bs = (branches...,)
        return ResolvedNullableTarget{N === Missing ? Missing : Nothing,typeof(bs)}(bs, isnull)
    end
    p isa ResolvedRecordPlan && s isa RecordSchema && return buildresolvedrecord(T, s, p, memo)
    return nothing
end

"A non-union null writer resolved to the target's nullable convention."
struct ResolvedNullTarget{N,P<:ReadPlan} <: TypedPlan
    plan::P
end

function typedvalue(p::ResolvedNullTarget{N}, d::Decoder, names) where {N}
    decodevalue(p.plan, d)
    return N === Missing ? missing : nothing
end

"A resolved two-branch-nullable union into `Union{Missing|Nothing, X}` typed targets per writer branch."
struct ResolvedNullableTarget{N,BS<:Tuple} <: TypedPlan
    branches::BS
    isnull::Vector{Bool}
end

function typedvalue(p::ResolvedNullableTarget{N}, d::Decoder, names) where {N}
    i = readindex(d, length(p.branches))
    if p.isnull[i]
        skipnothing = typedvalue(p.branches[i], d, names)                     # consumes the writer branch (null: nothing)
        return N === Missing ? missing : nothing
    end
    return typedvalue(p.branches[i], d, names)
end

"A resolved record decoded stepwise (writer order) directly into `T` (plan §4.8, R18)."
mutable struct ResolvedRecordTarget{T,PS<:Tuple,SLOTMAP} <: TypedPlan
    const schema::RecordSchema
    const plans::PS      # one target per writer step, writer order (SkipTarget when unmapped)
    const defaults::Vector{Any}
    const shell::Int
end

"A reader-field default retained as JSON and materialised afresh for each decoded record."
struct ResolvedReaderDefault{T}
    plan::DefaultPlan
end

function buildresolvedrecord(::Type{T}, s::RecordSchema, p::ResolvedRecordPlan, memo::TypedMemo) where {T}
    cached = memolookup(memo, s, T)
    cached === nothing || return cached
    fastroute(T) || return memostore!(memo, s, T, SemanticTarget{T}(p))
    ref = RefTarget{T}(nothing)
    memostore!(memo, s, T, ref)
    built = buildresolvedrecordplan(T, s, p, memo)
    if built === nothing
        sem = SemanticTarget{T}(p)
        ref.plan = sem
        return memostore!(memo, s, T, sem)
    end
    ref.plan = built
    return memostore!(memo, s, T, built)
end

function resolvedslotmap(::Type{T}, s::RecordSchema) where {T}
    names = fieldnames(T)
    tags = StructUtils.fieldtags(AvroStyle(), T)
    avronames = String[avrofieldname(tags, n) for n in names]
    slotfor = zeros(Int, length(s.fields))               # reader slot -> T field
    for (k, an) in enumerate(avronames)
        i = get(s.fieldindex, an, 0)
        i == 0 && continue
        slotfor[i] = k
    end
    return (names, slotfor)
end

function resolvedsteptargets(::Type{T}, s::RecordSchema, p::ResolvedRecordPlan, memo::TypedMemo,
                             slotfor::Vector{Int}) where {T}
    plans = TypedPlan[]
    stepslot = Int[]
    for (slot, sp) in p.steps
        k = slot == 0 ? 0 : slotfor[slot]
        if k == 0
            push!(plans, SkipTarget(sp))
            push!(stepslot, 0)
        else
            tp = buildtyped(fieldtype(T, k), s.fields[slot].schema, sp, memo)
            tp isa SemanticTarget && return nothing
            push!(plans, tp)
            push!(stepslot, k)
        end
    end
    return (plans, stepslot)
end

function checkedreaderdefault(::Type{T}, dp::DefaultPlan, limits::Limits) where {T}
    v = withbudget(limits) do budget
        jsonvalue(dp.schema, dp.json, budget)
    end
    v2 = v isa T ? v : try
        convertleaf(T, v)
    catch
        nothing
    end
    v2 isa T || return nothing
    return ResolvedReaderDefault{T}(dp)
end

function resolvedreaderdefaults!(::Type{T}, defaults::Vector{Any}, covered::BitVector,
                                 slotfor::Vector{Int}, p::ResolvedRecordPlan, s::RecordSchema) where {T}
    for (slot, dp) in p.defaults
        k = slotfor[slot]
        k == 0 && continue
        dflt = checkedreaderdefault(fieldtype(T, k), dp, graphlimits(s))
        dflt === nothing && return false
        defaults[k] = dflt
        covered[k] = true
    end
    return true
end

function resolvedstaticdefaults!(::Type{T}, defaults::Vector{Any}, covered::BitVector, names) where {T}
    defs = StructUtils.fielddefaults(AvroStyle(), T)
    for k in eachindex(names)
        covered[k] && continue
        ft = fieldtype(T, k)
        if haskey(defs, names[k])
            v = defs[names[k]]
            v isa ft || return false
            defaults[k] = v
        elseif Missing <: ft
            defaults[k] = missing
        elseif Nothing <: ft
            defaults[k] = nothing
        else
            throw(ArgumentError("field $(names[k]) of $T has no writer field, no reader default and no static default"))
        end
    end
    return true
end

function buildresolvedrecordplan(::Type{T}, s::RecordSchema, p::ResolvedRecordPlan, memo::TypedMemo) where {T}
    names, slotfor = resolvedslotmap(T, s)
    targets = resolvedsteptargets(T, s, p, memo, slotfor)
    targets === nothing && return nothing
    plans, stepslot = targets
    defaults = Vector{Any}(undef, length(names))
    covered = falses(length(names))
    for k in stepslot
        k == 0 || (covered[k] = true)
    end
    resolvedreaderdefaults!(T, defaults, covered, slotfor, p, s) || return nothing
    resolvedstaticdefaults!(T, defaults, covered, names) || return nothing
    ps = (plans...,)
    return ResolvedRecordTarget{T,typeof(ps),(stepslot...,)}(s, ps, defaults, measuredshell(T))
end

function resolveddefault(::Type{T}, x::ResolvedReaderDefault{T}, d::Decoder) where {T}
    countvalues!(d.budget)
    v = jsonvalue(x.plan.schema, x.plan.json, d.budget)
    return v isa T ? v : convertleaf(T, v)::T
end

function resolveddefault(::Type{T}, x, d::Decoder) where {T}
    return x::T
end

function typedvalue(p::ResolvedRecordTarget{T}, d::Decoder, names) where {T}
    enter!(d)
    reserve!(d.budget, p.shell)
    v = resolvedrecordvalue(p, d, names)
    leave!(d)
    return v
end

@generated function resolvedrecordvalue(p::ResolvedRecordTarget{T,PS,SLOTMAP}, d::Decoder, names) where {T,PS,SLOTMAP}
    body = Expr(:block)
    plantypes = PS.parameters
    for j in 1:length(plantypes)
        if plantypes[j] <: SkipTarget
            push!(body.args, :(skip(p.plans[$j].plan, d)))
        else
            push!(body.args, :($(Symbol("f", SLOTMAP[j])) = decodetyped(p.plans[$j], d, names)))
        end
    end
    args = []
    for k in 1:fieldcount(T)
        ft = fieldtype(T, k)
        push!(args, k in SLOTMAP ? :($(Symbol("f", k))::$ft) : :(resolveddefault($ft, p.defaults[$k], d)))
    end
    construct = T <: NamedTuple ? :($T(($(args...),))) : Expr(:new, T, args...)
    push!(body.args, :(return $construct))
    return body
end

function buildarraytarget(::Type{T}, s::ArraySchema, p::ArrayPlan, memo::TypedMemo) where {T}
    T <: Vector || return SemanticTarget{T}(p)
    E = eltype(T)
    ip = buildtyped(E, s.items, p.items, memo)
    ip isa SemanticTarget && return SemanticTarget{T}(p)
    return ArrayTarget{E,typeof(ip)}(ip, p.minsize)
end

function buildmaptarget(::Type{T}, s::MapSchema, p::MapPlan, memo::TypedMemo) where {T}
    if T <: Map
        K, V, dict = String, eltype(T).parameters[2], false
    elseif T <: Dict && (keytype(T) === String || keytype(T) === Symbol)
        K, V, dict = keytype(T), valtype(T), true
    else
        return SemanticTarget{T}(p)
    end
    vp = buildtyped(V, s.values, p.values, memo)
    vp isa SemanticTarget && return SemanticTarget{T}(p)
    return MapTarget{K,V,typeof(vp)}(vp, p.minsize, dict)
end

# ---- decoding ---------------------------------------------------------------------------------------

"""
    decodetyped(plan::TypedPlan, d::Decoder, names) -> value

Decode one value of `plan` (counted like the generic `decode`); `names` is the admission object.
"""
@inline function decodetyped(p::TypedPlan, d::Decoder, names)
    countvalues!(d.budget)
    return typedvalue(p, d, names)
end

typedvalue(p::GenericTarget, d::Decoder, names) = decodevalue(p.plan, d)
# Reached only through a recursive reference whose record fell back: converts where it occurs.
typedvalue(p::SemanticTarget{T}, d::Decoder, names) where {T} = semanticvalue(T, decodevalue(p.plan, d), names)
typedvalue(p::LeafTarget{T}, d::Decoder, names) where {T} = convertleaf(T, decodevalue(p.plan, d))::T
typedvalue(p::LeafTarget{String,EnumPlan}, d::Decoder, names) = p.plan.schema.symbols[readindex(d, length(p.plan.schema.symbols))]
typedvalue(p::SymbolTarget{StringPlan}, d::Decoder, names) = admit!(names, readstring(d); budget=d.budget)
typedvalue(p::RefTarget{T}, d::Decoder, names) where {T} = typedvalue(p.plan::TypedPlan, d, names)::T

function typedvalue(p::SymbolTarget{EnumPlan}, d::Decoder, names)
    syms = p.plan.schema.symbols
    return admit!(names, syms[readindex(d, length(syms))]; budget=d.budget)
end

function typedvalue(p::EnumTarget{T}, d::Decoder, names) where {T}
    syms = p.plan.schema.symbols
    i = readindex(d, length(syms))
    m = p.members[i]
    m === nothing && throw(ConversionError("enum symbol \"$(syms[i])\" of $(fullname(p.plan.schema)) has no $T member"))
    return m
end

function typedvalue(p::NullableTarget{N}, d::Decoder, names) where {N}
    i = readindex(d, 2)
    if i == p.nullpos
        N === Union{} && throw(ConversionError("null is not accepted by the target type"))
        return N === Missing ? missing : nothing
    end
    return typedvalue(p.inner, d, names)
end

function typedvalue(p::UnionTarget{T}, d::Decoder, names) where {T}
    n = length(p.branches)
    n == 0 && dataerror(d, "empty union has no datum")
    i = readindex(d, n)
    return typedvalue(p.branches[i], d, names)::T
end

function typedvalue(p::ArrayTarget{E}, d::Decoder, names) where {E}
    enter!(d)
    g = nothing
    while true
        count, size = readblockcount(d)
        count == 0 && break
        checkcount(d, count, p.minsize)
        g === nothing && (g = GrowBuf{E}(d, count))
        if size >= 0
            stop = d.pos + size - 1
            filltyped!(g, p.items, d, names, count, stop)
            d.pos == stop + 1 || dataerror(d, "sized array block not exactly consumed")
        else
            filltyped!(g, p.items, d, names, count, -1)
        end
    end
    leave!(d)
    g === nothing && (reserve!(d.budget, STORAGE[].vector); return Vector{E}(undef, 0))
    return finish!(g, d)
end

function filltyped!(g::GrowBuf{E}, items::TypedPlan, d::Decoder, names, count::Int, stop::Int) where {E}
    outerstop = d.stop
    stop >= 0 && (stop <= d.stop || dataerror(d, "sized block exceeds the remaining bytes"); d.stop = stop)
    for _ in 1:count
        push!(g, d, decodetyped(items, d, names))
    end
    d.stop = outerstop
    return g
end

function typedvalue(p::MapTarget{K,V}, d::Decoder, names) where {K,V}
    enter!(d)
    ks = GrowBuf{String}(d, 0)
    vs = nothing
    while true
        count, size = readblockcount(d)
        count == 0 && break
        checkcount(d, count, p.minsize + 1)
        vs === nothing && (vs = GrowBuf{V}(d, count))
        outerstop = d.stop
        if size >= 0
            stop = d.pos + size - 1
            stop <= d.stop || dataerror(d, "sized map block exceeds the remaining bytes")
            d.stop = stop
        end
        for _ in 1:count
            push!(ks, d, readstring(d))
            push!(vs, d, decodetyped(p.values, d, names))
        end
        size >= 0 && (d.pos == d.stop + 1 || dataerror(d, "sized map block not exactly consumed"))
        d.stop = outerstop
    end
    leave!(d)
    keys = finish!(ks, d)
    vals = vs === nothing ? Vector{V}(undef, 0) : finish!(vs, d)
    p.dict || (addinput!(d.budget, 0); return buildmap(V, keys, vals, d.budget))
    return todict(K, V, keys, vals, names)
end

# `Dict` targets are built outside exact accounting (plan §4.6); `Symbol` keys pass admission.
function todict(::Type{K}, ::Type{V}, keys::Vector{String}, vals::Vector{V}, names) where {K,V}
    out = Dict{K,V}()
    sizehint!(out, length(keys))
    for i in eachindex(keys)
        k = K === Symbol ? admit!(names, keys[i]) : keys[i]
        out[k] = vals[i]
    end
    return out
end

function typedvalue(p::RecordTarget{T}, d::Decoder, names) where {T}
    enter!(d)
    reserve!(d.budget, p.shell)
    v = recordvalue(p, d, names)
    leave!(d)
    return v
end

"""
The heap shell of one decoded `T`, measured once per plan from an empty probe (`Expr(:new, T)` with no
fields: reference fields stay undefined, so `Base.summarysize` reports exactly the header plus the
inline layout — including nested inline structs — and no referenced payload; plan §4.4, R10). The
checked fallback bound covers types `:new` cannot probe.
"""
@generated emptyprobe(::Type{T}) where {T} = Expr(:new, T)

function measuredshell(::Type{T}) where {T}
    isbitstype(T) && return 0
    ismutabletype(T) || isstructtype(T) || return 16 + sizeof(T)
    probe = try
        emptyprobe(T)
    catch
        return sizeof(T) + 8 * fieldcount(T) + 64      # the §4.4 checked bound
    end
    return max(Int(Base.summarysize(probe)), 8)
end

shellbytes(::Type{T}) where {T} = isbitstype(T) ? 0 : 16 + sizeof(T)

@generated function recordvalue(p::RecordTarget{T,PS,MAP}, d::Decoder, names) where {T,PS,MAP}
    body = Expr(:block)
    plantypes = PS.parameters
    for j in 1:length(plantypes)
        if plantypes[j] <: SkipTarget
            push!(body.args, :(skip(p.plans[$j].plan, d)))
        else
            push!(body.args, :($(Symbol("f", j)) = decodetyped(p.plans[$j], d, names)))
        end
    end
    args = Any[]
    for k in 1:fieldcount(T)
        ft = fieldtype(T, k)
        j = MAP[k]
        push!(args, j == 0 ? :(p.defaults[$k]::$ft) : :($(Symbol("f", j))::$ft))
    end
    construct = T <: NamedTuple ? :($T(($(args...),))) : Expr(:new, T, args...)
    push!(body.args, :(return $construct))
    return body
end

# ---- leaf conversions (ConversionError on failure) -------------------------------------------------

convertleaf(::Type{Nothing}, ::Missing) = nothing
convertleaf(::Type{Float64}, v::Float32) = Float64(v)
convertleaf(::Type{Float16}, v::Float32) = Float16(v)
convertleaf(::Type{DateTime}, v::Union{Timestamp,LocalTimestamp}) = DateTime(v)
convertleaf(::Type{Vector{UInt8}}, v::Fixed) = v.bytes

function convertleaf(::Type{T}, v::Union{Int32,Int64}) where {T<:Integer}
    fits = T <: Signed ? (typemin(T) <= Int64(v) <= typemax(T)) : (v >= 0 && UInt64(v) <= typemax(T))
    fits || throw(ConversionError("$v does not fit $T"))
    return T(v)
end

function convertleaf(::Type{Char}, s::String)
    n = ncodeunits(s)
    n > 0 && n == ncodeunits(s[1]) || throw(ConversionError("expected a one-character string for Char, got $(repr(s))"))
    return s[1]
end

function convertleaf(::Type{T}, v::Fixed) where {T<:Tuple}
    return ntuple(i -> @inbounds(v.bytes[i]), Val(length(T.parameters)))
end

# ---- semantic route ----------------------------------------------------------------------------------

const ADMISSION_KEY = :avro_symbol_admission

withadmission(f, names) = task_local_storage(f, ADMISSION_KEY, names)
currentadmission() = get(task_local_storage(), ADMISSION_KEY, DEFAULT_ADMISSION)

"""
    semanticvalue(T, generic, names)

Convert a generic value to `T` through `StructUtils.make` under `AvroStyle`, in caller space; conversion
failures are `ConversionError`s (limit errors propagate).
"""
function semanticvalue(::Type{T}, v, names) where {T}
    return withadmission(names) do
        try
            StructUtils.make(T, v, AvroStyle())
        catch e
            e isa Union{ArgumentError,MethodError,InexactError,TypeError,KeyError,BoundsError,DomainError} || rethrow()
            throw(ConversionError("cannot convert the decoded value to $T: $(sprint(showerror, e))"))
        end
    end
end

finishtyped(p::SemanticTarget{T}, v, names) where {T} = semanticvalue(T, v, names)
finishtyped(p, v, names) = v

# StructUtils integration: generic values as sources.
StructUtils.applyeach(st::AvroStyle, f, r::Record) = applyeachrecord(st, f, r)
StructUtils.applyeach(st::AvroStyle, f::StructUtils.StructStyle, r::Record) = applyeachrecord(st, f, r)   # disambiguates the (f, style, x) form

function applyeachrecord(st::AvroStyle, f, r::Record)
    s = getfield(r, :schema)
    vals = getfield(r, :values)
    for (i, fld) in enumerate(s.fields)
        ret = f(fld.name, vals[i])
        ret isa StructUtils.EarlyReturn && return ret
    end
    return StructUtils.defaultstate(st)
end

# StructUtils lowers non-struct-like sources at the root only: the identity-bearing generic values are
# representations, not structs to traverse.
StructUtils.structlike(::AvroStyle, ::Type{<:Union{Fixed,EnumValue,UnionValue}}) = false
StructUtils.lower(::AvroStyle, x::EnumValue) = String(x)
StructUtils.lower(::AvroStyle, x::Fixed) = x.bytes
StructUtils.lower(::AvroStyle, x::UnionValue) = x.value
StructUtils.lift(st::AvroStyle, ::Type{T}, x::UnionValue) where {T} = StructUtils.lift(st, T, x.value)
StructUtils.lift(st::AvroStyle, ::Type{T}, x::EnumValue) where {T} = StructUtils.lift(st, T, String(x))
StructUtils.lift(st::AvroStyle, ::Type{T}, x::Fixed) where {T} = StructUtils.lift(st, T, x.bytes)
# zero-dimensional array targets (StructUtils' own special case) unwrap the same way
StructUtils.lift(st::AvroStyle, ::Type{A}, x::UnionValue) where {A<:AbstractArray{T,0}} where {T} = StructUtils.lift(st, A, x.value)
StructUtils.lift(st::AvroStyle, ::Type{A}, x::EnumValue) where {A<:AbstractArray{T,0}} where {T} = StructUtils.lift(st, A, String(x))
StructUtils.lift(st::AvroStyle, ::Type{A}, x::Fixed) where {A<:AbstractArray{T,0}} where {T} = StructUtils.lift(st, A, x.bytes)
StructUtils.lift(::AvroStyle, ::Type{Symbol}, x::AbstractString) = (admit!(currentadmission(), x), nothing)
StructUtils.lift(::Type{DateTime}, x::Union{Timestamp,LocalTimestamp}) = DateTime(x)
StructUtils.lift(::Type{T}, x::Union{Timestamp,LocalTimestamp}) where {T<:Union{Timestamp,LocalTimestamp}} = T(x.ticks)
